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
from src.utils.trade_date import with_trade_date  # FIX TS-DATE-01

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

# FIX GMI-PIT-001 (ADR-050): read paths for every Gold output that was
# previously unreachable from the backtest/PIT path — GMI Wave 1 Cycle 4's
# CrossAssetEngine (global_index_regime.py, correlation_module.py,
# lead_lag_module.py, forecast_module.py), signal_aggregation.py, and this
# ADR's own two "not yet built" producers (context_regime.parquet —
# ADR-051; gate_status.parquet — this ADR's own GMI-GATE-001, wired below).
#
# cross_asset_corr / lead_lag_matrix / cross_asset_forecast: the producers
# OVERWRITE their snapshot files each weekly run (no history), and
# gold_screener joins those snapshot files whole — so they cannot be turned
# into history stores without fanning out the watchlist. Point-in-time reads
# therefore go through the ADDITIVE history store written by
# src/gold/cross_asset/history.py (one dated file per run, written at that
# run's own run_date — PIT-correct by construction). Each method below
# returns the SINGLE latest snapshot with computation_date <= trade_date,
# never several weeks concatenated. History accrues only from the first
# weekly run after this shipped — it is not a backfill (RISK-35).
GOLD_GLOBAL_REGIME_PATH    = "data/gold/cross_asset/global_regime.parquet"
GOLD_CROSS_ASSET_CORR_PATH = "data/gold/cross_asset/history/cross_asset_corr/cross_asset_corr_*.parquet"
GOLD_LEAD_LAG_PATH         = "data/gold/cross_asset/history/lead_lag_matrix/lead_lag_matrix_*.parquet"
GOLD_FORECAST_PATH         = "data/gold/cross_asset/history/cross_asset_forecast/cross_asset_forecast_*.parquet"
GOLD_SIGNAL_AGG_PATH       = "data/gold/signal_aggregation/signal_aggregation_*.parquet"
# ADR-051 (Dual-HMM Regime Engine) — not yet built. get_context_regime()
# naturally returns None until context_regime.py's run() ever writes this
# file; no special-case handling needed beyond the existing try/except.
GOLD_CONTEXT_REGIME_PATH   = "data/gold/hmm/context_regime.parquet"
# ADR-050's own GMI-GATE-001 output (src/utils/gatekeeper.py). Unlike
# context_regime above, this one DOES exist once this ADR ships — wired
# into DAILY_SEQUENCE immediately before health_report.
GATE_STATUS_PATH           = "data/health/gate_status.parquet"


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
                with_trade_date("""
                SELECT
                    symbol, timestamp, timeframe,
                    open, high, low, close, volume,
                    log_return, vwap, dollar_volume, spread_hl,
                    is_adjusted, adj_factor, is_clean, data_source
                FROM read_parquet($path, hive_partitioning=true)
                WHERE symbol    = $symbol
                  AND __TRADE_DATE__ >= $start
                  AND __TRADE_DATE__ <  $trade_date   -- PIT guard
                  AND is_clean  = TRUE
                ORDER BY timestamp ASC
                """),
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
                with_trade_date("""
                SELECT
                    symbol, timestamp,
                    open, high, low, close, volume,
                    log_return, vwap, is_adjusted, adj_factor, is_clean
                FROM read_parquet($path, hive_partitioning=true)
                WHERE symbol = ANY($symbols)
                  AND __TRADE_DATE__ >= $start
                  AND __TRADE_DATE__ <  $trade_date
                  AND is_clean = TRUE
                ORDER BY symbol, timestamp ASC
                """),
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

    # ── FIX GMI-PIT-001 (ADR-050) ────────────────────────────────────────────
    # New read methods for every Gold output previously unreachable from the
    # backtest/PIT path — see module header for the CrossAssetEngine/
    # signal_aggregation/context_regime/gate_status path notes.

    def get_global_regime(self, trade_date: date) -> Optional[dict]:
        """
        Return the global equity-index breadth regime
        (gold_global_regime / global_index_regime.py) as of trade_date.
        Same single-row idiom as get_regime() — 'date' column, most recent
        row on or before trade_date.
        """
        try:
            result = self._con.execute(
                """
                SELECT *
                FROM read_parquet($path)
                WHERE CAST(date AS DATE) <= $trade_date
                ORDER BY date DESC
                LIMIT 1
                """,
                {"path": GOLD_GLOBAL_REGIME_PATH, "trade_date": trade_date.isoformat()},
            ).pl()

            if not result.is_empty():
                return result.row(0, named=True)
        except Exception as e:
            logger.debug(f"[PITLoader] Global regime not available for {trade_date}: {e}")
        return None

    def get_cross_asset_corr(
        self,
        trade_date: date,
        symbol: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Return the latest CrossAssetEngine correlation snapshot
        (CorrelationModule) with computation_date <= trade_date, optionally
        filtered to pairs involving `symbol`. Reads the PIT history store
        (see module header) and returns exactly ONE snapshot — never several
        weeks concatenated. Empty if trade_date predates the earliest
        retained snapshot. Multi-row by nature (a matrix), unlike the
        single-row get_regime()/get_mtf_score().

        Static SQL only (two fixed branches, no string building) — the
        project's no-f-string-SQL rule (GD §17.7) applies to fragments too.
        """
        try:
            if symbol is not None:
                return self._con.execute(
                    """
                    SELECT *
                    FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                    WHERE CAST(computation_date AS DATE) = (
                              SELECT MAX(CAST(computation_date AS DATE))
                              FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                              WHERE CAST(computation_date AS DATE) <= $trade_date
                          )
                      AND (symbol_a = $symbol OR symbol_b = $symbol)
                    """,
                    {"path": GOLD_CROSS_ASSET_CORR_PATH, "symbol": symbol,
                     "trade_date": trade_date.isoformat()},
                ).pl()
            return self._con.execute(
                """
                SELECT *
                FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                WHERE CAST(computation_date AS DATE) = (
                          SELECT MAX(CAST(computation_date AS DATE))
                          FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                          WHERE CAST(computation_date AS DATE) <= $trade_date
                      )
                """,
                {"path": GOLD_CROSS_ASSET_CORR_PATH, "trade_date": trade_date.isoformat()},
            ).pl()
        except Exception as e:
            logger.debug(f"[PITLoader] Cross-asset correlation not available: {e}")
            return pl.DataFrame()

    def get_lead_lag(
        self,
        trade_date: date,
        symbol: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Return the latest CrossAssetEngine lead-lag snapshot (LeadLagModule)
        with computation_date <= trade_date. `symbol`, when given, matches
        either the leader or the follower. One snapshot, same PIT-history
        semantics as get_cross_asset_corr().
        """
        try:
            if symbol is not None:
                return self._con.execute(
                    """
                    SELECT *
                    FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                    WHERE CAST(computation_date AS DATE) = (
                              SELECT MAX(CAST(computation_date AS DATE))
                              FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                              WHERE CAST(computation_date AS DATE) <= $trade_date
                          )
                      AND (leader = $symbol OR follower = $symbol)
                    """,
                    {"path": GOLD_LEAD_LAG_PATH, "symbol": symbol,
                     "trade_date": trade_date.isoformat()},
                ).pl()
            return self._con.execute(
                """
                SELECT *
                FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                WHERE CAST(computation_date AS DATE) = (
                          SELECT MAX(CAST(computation_date AS DATE))
                          FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                          WHERE CAST(computation_date AS DATE) <= $trade_date
                      )
                """,
                {"path": GOLD_LEAD_LAG_PATH, "trade_date": trade_date.isoformat()},
            ).pl()
        except Exception as e:
            logger.debug(f"[PITLoader] Lead-lag not available: {e}")
            return pl.DataFrame()

    def get_forecast(
        self,
        symbol: str,
        trade_date: date,
        horizon_days: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Return the latest CrossAssetEngine forecast snapshot (ForecastModule)
        for `symbol` with computation_date <= trade_date, optionally narrowed
        to one horizon_days (1-5). One snapshot (one row per horizon), same
        PIT-history semantics as get_cross_asset_corr().
        """
        try:
            if horizon_days is not None:
                return self._con.execute(
                    """
                    SELECT *
                    FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                    WHERE CAST(computation_date AS DATE) = (
                              SELECT MAX(CAST(computation_date AS DATE))
                              FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                              WHERE CAST(computation_date AS DATE) <= $trade_date
                          )
                      AND symbol = $symbol
                      AND horizon_days = $horizon_days
                    """,
                    {"path": GOLD_FORECAST_PATH, "symbol": symbol,
                     "horizon_days": horizon_days, "trade_date": trade_date.isoformat()},
                ).pl()
            return self._con.execute(
                """
                SELECT *
                FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                WHERE CAST(computation_date AS DATE) = (
                          SELECT MAX(CAST(computation_date AS DATE))
                          FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
                          WHERE CAST(computation_date AS DATE) <= $trade_date
                      )
                  AND symbol = $symbol
                ORDER BY horizon_days ASC
                """,
                {"path": GOLD_FORECAST_PATH, "symbol": symbol,
                 "trade_date": trade_date.isoformat()},
            ).pl()
        except Exception as e:
            logger.debug(f"[PITLoader] Forecast not available for {symbol}: {e}")
            return pl.DataFrame()

    def get_signal_aggregation(
        self,
        symbol: str,
        trade_date: date,
    ) -> Optional[dict]:
        """
        Return most recent signal_aggregation row (composite_score,
        composite_grade, sector_breadth_pct, breadth_divergence, ...) for
        symbol as of trade_date. Same idiom as get_mtf_score() — the
        producer also writes one dated file per day (signal_aggregation_
        {date}.parquet), globbed the same way mtf_alignment_*.parquet is.
        """
        try:
            result = self._con.execute(
                """
                SELECT *
                FROM read_parquet($path, hive_partitioning=false)
                WHERE symbol = $symbol
                  AND date   <= $trade_date
                ORDER BY date DESC
                LIMIT 1
                """,
                {"path": GOLD_SIGNAL_AGG_PATH, "symbol": symbol,
                 "trade_date": trade_date.isoformat()},
            ).pl()

            if not result.is_empty():
                return result.row(0, named=True)
        except Exception as e:
            logger.debug(f"[PITLoader] Signal aggregation not available for {symbol}: {e}")
        return None

    def get_context_regime(self, trade_date: date) -> Optional[dict]:
        """
        Return the dual-HMM context regime (ADR-051, context_regime.py —
        NOT YET BUILT) as of trade_date. Same single-row idiom as
        get_regime(). Returns None today, and will keep returning None
        until ADR-051 ships and starts writing context_regime.parquet —
        there is nothing else to special-case here: the existing
        try/except around a read_parquet() of a not-yet-existing path
        already degrades exactly the way every other method in this class
        does when its own Gold producer hasn't run yet.
        """
        try:
            result = self._con.execute(
                """
                SELECT *
                FROM read_parquet($path)
                WHERE CAST(date AS DATE) <= $trade_date
                ORDER BY date DESC
                LIMIT 1
                """,
                {"path": GOLD_CONTEXT_REGIME_PATH, "trade_date": trade_date.isoformat()},
            ).pl()

            if not result.is_empty():
                return result.row(0, named=True)
        except Exception as e:
            logger.debug(f"[PITLoader] Context regime not available for {trade_date}: {e}")
        return None

    def get_gate_status(
        self,
        trade_date: date,
        scope: str = "gold",
    ) -> Optional[dict]:
        """
        Return the most recent Gatekeeper gate_status row (ADR-050,
        GMI-GATE-001) for `scope` ('bronze' | 'silver' | 'gold') on or
        before trade_date.

        Defaults to scope='gold': Gatekeeper's cascading design (see
        src/utils/gatekeeper.py) means Gold's own row is already at least
        as severe as Bronze's or Silver's — a caller that just wants "is
        the pipeline healthy as of this date, overall" gets the full
        cascaded picture from this one default call, while a caller that
        wants layer-specific detail can still pass scope='bronze' or
        'silver' explicitly.
        """
        try:
            result = self._con.execute(
                """
                SELECT *
                FROM read_parquet($path)
                WHERE scope = $scope
                  AND CAST(run_date AS DATE) <= $trade_date
                ORDER BY run_date DESC
                LIMIT 1
                """,
                {"path": GATE_STATUS_PATH, "scope": scope,
                 "trade_date": trade_date.isoformat()},
            ).pl()

            if not result.is_empty():
                return result.row(0, named=True)
        except Exception as e:
            logger.debug(f"[PITLoader] Gate status not available for {trade_date}: {e}")
        return None

    def close(self) -> None:
        """Release DuckDB connection."""
        self._con.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
