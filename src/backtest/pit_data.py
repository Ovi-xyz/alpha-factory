"""
pit_data.py — GD §12.4 (Backtest PIT-Aware Data Loader)
Point-in-Time data access untuk backtest — prevents lookahead bias.

PIT Principle: hanya menggunakan data yang tersedia pada saat keputusan dibuat.
    - OHLCV:  hanya bar yang sudah closed sebelum trade_date
    - Macro:  vintage_date <= trade_date (GD §4.5)
    - Signals: computed dari data yang tersedia saat itu

Anti-lookahead guards:
    1. Silver OHLCV: timestamp < trade_date (bar belum close = tidak digunakan)
    2. Macro: vintage_date <= trade_date
    3. Earnings: next_earnings_date diketahui jika sudah diumumkan sebelum trade_date
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from loguru import logger

from src.utils.silver_scope import layer1_globs

# FIX ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8, 2026-07-11):
# SILVER_OHLCV_PATH was an unfiltered "market_ohlcv/**/..." glob template —
# the exact defect class RISK-6 fixed in quality_validator.py and
# technical_signals.py (Layer 2 context OHLCV, added in GMI Cycle 3, shares
# the same market_ohlcv/ root and was silently ALSO being scanned). Found
# by Gate G-8's static scanner when it was added; PITDataLoader queries are
# per-symbol/per-universe with an explicit WHERE symbol filter, so this was
# not exploitable the same way quality_validator.py's aggregate
# COUNT/MAX queries were — but it is still a latent risk (a Layer 1 symbol
# name colliding with a Layer 2 one would silently corrupt PIT backtest
# data) and, more simply, an unnecessary widened glob. Fixed via
# silver_scope.layer1_globs() for consistency with the rest of the
# Bronze/Silver Solidification work, ahead of GMI Wave 1 Cycle 4.
SILVER_OHLCV_ROOT  = Path("data/silver/market_ohlcv")
SILVER_MACRO_PATH  = "data/silver/macro_enriched/**/*_silver.parquet"
GOLD_SIGNALS_PATH  = "data/gold/signals/tech_signals_{tf}.parquet"
GOLD_MTF_PATH      = "data/gold/mtf/mtf_alignment_*.parquet"
GOLD_REGIME_PATH   = "data/gold/macro/regime_store.parquet"


class PITDataLoader:
    """
    Point-in-Time aware data loader.
    All queries are bounded by trade_date to prevent lookahead.

    Usage in backtest:
        loader = PITDataLoader()
        ohlcv  = loader.get_ohlcv("AAPL", "1D", trade_date)
        regime = loader.get_regime(trade_date)
        macro  = loader.get_macro_series("T10Y2Y", trade_date)
    """

    def __init__(self) -> None:
        self._con = duckdb.connect()
        self._con.execute("SET memory_limit='3GB'; SET threads=4;")

    def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        trade_date: date,
        lookback_days: int = 252,
    ) -> pl.DataFrame:
        """
        Return OHLCV bars for symbol up to (but not including) trade_date.
        PIT: only bars with timestamp < trade_date are returned.
        """
        start = trade_date - timedelta(days=lookback_days)
        # FIX ADR-022/RISK-6: Layer 1-scoped glob list, not a single
        # unfiltered market_ohlcv/**/... string — see module header.
        paths = layer1_globs(SILVER_OHLCV_ROOT, f"*_{timeframe}_silver.parquet")
        if not paths:
            return pl.DataFrame()

        try:
            # FIX BCK-SQL-001: $name parameterized (GD §17.7) — prevents SQL injection
            return self._con.execute(
                """
                SELECT
                    symbol, timestamp, timeframe,
                    open, high, low, close, volume,
                    log_return, vwap, dollar_volume, spread_hl,
                    is_adjusted, adj_factor, is_clean, data_source
                FROM read_parquet($path, hive_partitioning=true)
                WHERE symbol    = $symbol
                  AND CAST(timestamp AS DATE) >= $start
                  AND CAST(timestamp AS DATE) <  $trade_date   -- PIT guard
                  AND is_clean  = TRUE
                ORDER BY timestamp ASC
                """,
                {"path": paths, "symbol": symbol,
                 "start": start.isoformat(), "trade_date": trade_date.isoformat()},
            ).pl()
        except Exception as e:
            logger.debug(f"[PITLoader] OHLCV not available for {symbol}/{timeframe}: {e}")
            return pl.DataFrame()

    def get_ohlcv_universe(
        self,
        symbols: list[str],
        timeframe: str,
        trade_date: date,
        lookback_days: int = 252,
    ) -> pl.DataFrame:
        """Batch OHLCV fetch for multiple symbols — efficient for backtesting."""
        if not symbols:
            return pl.DataFrame()

        start       = trade_date - timedelta(days=lookback_days)
        # FIX ADR-022/RISK-6: Layer 1-scoped glob list — see module header.
        paths       = layer1_globs(SILVER_OHLCV_ROOT, f"*_{timeframe}_silver.parquet")
        if not paths:
            return pl.DataFrame()

        try:
            # FIX BCK-SQL-001: fully parameterized — DuckDB supports list param
            # via = ANY($param) operator (avoids f-string SQL, GD §17.7)
            return self._con.execute(
                """
                SELECT
                    symbol, timestamp,
                    open, high, low, close, volume,
                    log_return, vwap, is_adjusted, adj_factor, is_clean
                FROM read_parquet($path, hive_partitioning=true)
                WHERE symbol = ANY($symbols)
                  AND CAST(timestamp AS DATE) >= $start
                  AND CAST(timestamp AS DATE) <  $trade_date
                  AND is_clean = TRUE
                ORDER BY symbol, timestamp ASC
                """,
                {"path": paths, "symbols": symbols,
                 "start": start.isoformat(), "trade_date": trade_date.isoformat()},
            ).pl()
        except Exception as e:
            logger.debug(f"[PITLoader] Universe OHLCV failed: {e}")
            return pl.DataFrame()

    def get_macro_series(
        self,
        series_id: str,
        trade_date: date,
        lookback_days: int = 365,
    ) -> pl.DataFrame:
        """
        Return macro series with PIT integrity.
        PIT: vintage_date <= trade_date — no future revisions used.
        """
        start = trade_date - timedelta(days=lookback_days)
        try:
            # FIX BCK-SQL-001: $name parameterized
            return self._con.execute(
                """
                SELECT
                    series_id, observation_date, value,
                    vintage_date, release_date,
                    is_revision, revision_seq
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE series_id   = $series_id
                  AND CAST(observation_date AS DATE) >= $start
                  AND CAST(vintage_date AS DATE)     <= $trade_date  -- PIT guard
                ORDER BY observation_date ASC, vintage_date ASC
                """,
                {"glob": SILVER_MACRO_PATH, "series_id": series_id,
                 "start": start.isoformat(), "trade_date": trade_date.isoformat()},
            ).pl()
        except Exception as e:
            logger.debug(f"[PITLoader] Macro {series_id} not available: {e}")
            return pl.DataFrame()

    def get_regime(self, trade_date: date) -> Optional[dict]:
        """
        Return macro regime as of trade_date.
        PIT: uses regime computed on or before trade_date.
        """
        try:
            # FIX BCK-SQL-001: $name parameterized
            result = self._con.execute(
                """
                SELECT *
                FROM read_parquet($path)
                WHERE CAST(date AS DATE) <= $trade_date
                ORDER BY date DESC
                LIMIT 1
                """,
                {"path": GOLD_REGIME_PATH, "trade_date": trade_date.isoformat()},
            ).pl()

            if not result.is_empty():
                return result.row(0, named=True)
        except Exception as e:
            logger.debug(f"[PITLoader] Regime not available for {trade_date}: {e}")
        return None

    def get_signals(
        self,
        symbol: str,
        timeframe: str,
        trade_date: date,
    ) -> Optional[dict]:
        """
        Return most recent technical signals as of trade_date.
        PIT: signal_date <= trade_date.
        """
        path = GOLD_SIGNALS_PATH.format(tf=timeframe)
        try:
            # FIX BCK-SQL-001: $name parameterized
            result = self._con.execute(
                """
                SELECT *
                FROM read_parquet($path)
                WHERE symbol      = $symbol
                  AND signal_date <= $trade_date
                ORDER BY signal_date DESC, timestamp DESC
                LIMIT 1
                """,
                {"path": path, "symbol": symbol,
                 "trade_date": trade_date.isoformat()},
            ).pl()

            if not result.is_empty():
                return result.row(0, named=True)
        except Exception as e:
            logger.debug(f"[PITLoader] Signals not available for {symbol}: {e}")
        return None

    def get_mtf_score(
        self,
        symbol: str,
        trade_date: date,
    ) -> Optional[dict]:
        """Return most recent MTF alignment score as of trade_date."""
        try:
            # FIX BCK-SQL-001: $name parameterized
            result = self._con.execute(
                """
                SELECT *
                FROM read_parquet($path, hive_partitioning=false)
                WHERE symbol = $symbol
                  AND date   <= $trade_date
                ORDER BY date DESC
                LIMIT 1
                """,
                {"path": GOLD_MTF_PATH, "symbol": symbol,
                 "trade_date": trade_date.isoformat()},
            ).pl()

            if not result.is_empty():
                return result.row(0, named=True)
        except Exception as e:
            logger.debug(f"[PITLoader] MTF not available for {symbol}: {e}")
        return None

    def close(self) -> None:
        """Release DuckDB connection."""
        self._con.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
