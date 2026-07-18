"""
fundamental_processor.py — Silver Fundamental Processor (GD §17.4)
Clean Bronze fundamental data (earnings, financials) → Silver.

Responsibilities:
    - Normalize schema across Finnhub sources
    - Tag upcoming earnings (days_to_earnings)
    - Deduplication by (symbol, earnings_date)
    - PIT-aware: only data available before trade_date is usable

Output:
    data/silver/fundamental/earnings_{date}.parquet
    data/silver/fundamental/quotes_{date}.parquet

Consumed by:
    Gold Screener → days_to_earnings, near_earnings_flag
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from loguru import logger

# FIX FP-AIO-001 (found empirically, 2026-07-11, while adding first-ever
# real-data end-to-end test coverage for process_quotes() during the
# Decision Doc v2 §5 high_52w/low_52w rename — see
# tests/unit/test_fundamental_processor.py::
# test_process_quotes_reads_day_high_day_low): atomic_write_parquet() was
# called at two sites in this file (process_earnings, process_quotes) but
# NEVER IMPORTED. Every other module that calls it does
# `from src.utils.atomic_io import atomic_write_parquet` — this file
# omitted it entirely. Confirmed via NameError on first real (non-empty)
# invocation; the "graceful no Bronze data" tests that already existed for
# both methods return early before reaching the write call, which is
# exactly why this went undetected. Same root-cause class as
# FIX GLD-ADX-001 (a real invocation path with zero test coverage until
# now).
from src.utils.atomic_io import atomic_write_parquet

BRONZE_FUNDAMENTAL = Path("data/bronze/market/fundamental")
SILVER_FUNDAMENTAL = Path("data/silver/fundamental")

CURRENT_SILVER_VERSION = "1.2"


class FundamentalProcessor:
    """
    Process Bronze fundamental data into clean Silver tables.
    """

    def process_earnings(self, run_date: date) -> None:
        """
        Process Finnhub earnings calendar → Silver.
        Computes days_to_earnings relative to run_date.
        """
        glob = str(BRONZE_FUNDAMENTAL / "finnhub" / "earnings_calendar" / "**" / "*.parquet")
        try:
            con = duckdb.connect()
            df  = con.execute(
                """
                SELECT
                    symbol,
                    earnings_date,
                    eps_estimate,
                    eps_actual,
                    revenue_estimate,
                    quarter,
                    year,
                    fetched_date
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE symbol IS NOT NULL
                  AND earnings_date IS NOT NULL
                ORDER BY symbol, earnings_date
                """,  # FIX SIL-SQL-003: $name parameterized (GD §17.7)
                {"glob": glob},
            ).pl()
        except Exception as e:
            logger.warning(f"[FundamentalProcessor] No earnings Bronze data: {e}")
            return

        if df.is_empty():
            return

        # FIX S-F02: PIT filter — exclude records fetched after run_date
        # fetched_date > run_date berarti data tidak tersedia pada saat run_date
        if "fetched_date" in df.columns:
            before = len(df)
            df = df.filter(pl.col("fetched_date") <= pl.lit(str(run_date)))
            dropped = before - len(df)
            if dropped > 0:
                logger.info(
                    f"[FundamentalProcessor] earnings PIT filter dropped {dropped} rows "
                    f"(fetched_date > {run_date})"
                )

        if df.is_empty():
            return

        # Dedup: keep latest fetched record per (symbol, earnings_date)
        df = (
            df.sort("fetched_date", descending=True)
              .unique(subset=["symbol", "earnings_date"], keep="first")
        )

        # Compute days_to_earnings
        df = df.with_columns([
            pl.lit(str(run_date)).alias("run_date"),
            pl.lit(CURRENT_SILVER_VERSION).alias("processing_version"),
        ])

        # Cast earnings_date to Date for arithmetic
        try:
            df = df.with_columns(
                pl.col("earnings_date").str.to_date(strict=False).alias("earnings_date")
            )
            df = df.with_columns([
                (
                    pl.col("earnings_date")
                    - pl.lit(run_date)
                ).dt.total_days().cast(pl.Int32).alias("days_to_earnings"),
                (
                    pl.col("eps_actual").is_not_null()
                ).alias("is_reported"),
            ])
        except Exception as e:
            logger.debug(f"[FundamentalProcessor] Date arithmetic failed: {e}")
            df = df.with_columns([
                pl.lit(None).cast(pl.Int32).alias("days_to_earnings"),
                pl.lit(False).alias("is_reported"),
            ])

        SILVER_FUNDAMENTAL.mkdir(parents=True, exist_ok=True)
        out = SILVER_FUNDAMENTAL / f"earnings_{run_date.isoformat()}.parquet"
        # FIX SIL-AIO-004: atomic write — earnings data consumed by Gold Screener
        atomic_write_parquet(
            df, out, compression="zstd", compression_level=3,
        )
        logger.info(
            f"[FundamentalProcessor] Earnings: {len(df)} records → {out.name}"
        )

    def process_quotes(self, run_date: date) -> None:
        """Process Finnhub real-time quotes → Silver.

        UPD Decision Doc v2 §5 (2026-07-11): high_52w/low_52w renamed to
        day_high/day_low at the Bronze source (finnhub_ingester.py) —
        updated here to match. This IS a real, live consumer of that Bronze
        output (confirmed by direct code trace during the rename), contrary
        to GMI_Decision_Document_v2.docx's stated premise that the rename
        had zero consumers/zero migration cost.
        """
        glob = str(BRONZE_FUNDAMENTAL / "finnhub" / "quote" / "**" / "*.parquet")
        try:
            con = duckdb.connect()
            df  = con.execute(
                """
                SELECT
                    _symbol AS symbol,
                    current_price,
                    change,
                    pct_change,
                    day_high,
                    day_low,
                    prev_close,
                    fetched_date
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE _symbol IS NOT NULL
                ORDER BY _symbol, fetched_date DESC
                """,  # FIX SIL-SQL-003: $name parameterized (GD §17.7)
                {"glob": glob},
            ).pl()
        except Exception as e:
            logger.warning(f"[FundamentalProcessor] No quote Bronze data: {e}")
            return

        if df.is_empty():
            return

        # FIX F-FP-01 [P3]: explicit row_number deduplication for latest quote.
        # BEFORE: df.sort('fetched_date', descending=True).unique(subset=['symbol'], keep='first')
        #         — relies on .unique(keep='first') preserving sort order, which is
        #         documented behaviour but rapuh: if Polars changes uniqueness semantics
        #         in a future version the silent breakage would return stale quotes.
        # AFTER: explicit RANK approach via .over() window expression.
        #         rank('ordinal', descending=True) assigns rank 1 to the largest
        #         fetched_date per symbol regardless of DataFrame row order.
        #         Filter rn==1 then drop the working column — deterministic and
        #         future-proof against any internal ordering changes in Polars.
        df = (
            df.with_columns(
                pl.col("fetched_date")
                  .rank("ordinal", descending=True)
                  .over("symbol")
                  .alias("_rn")
            )
            .filter(pl.col("_rn") == 1)
            .drop("_rn")
            .with_columns([
                pl.lit(str(run_date)).alias("run_date"),
                pl.lit(CURRENT_SILVER_VERSION).alias("processing_version"),
            ])
        )

        out = SILVER_FUNDAMENTAL / f"quotes_{run_date.isoformat()}.parquet"
        # FIX SIL-AIO-004: atomic write — crash-safe Silver quotes
        atomic_write_parquet(
            df, out, compression="zstd", compression_level=3,
        )
        logger.info(
            f"[FundamentalProcessor] Quotes: {len(df)} records → {out.name}"
        )

    def get_days_to_earnings(
        self,
        symbol: str,
        run_date: date,
    ) -> Optional[int]:
        """
        Lookup next earnings date for a symbol.
        Returns days until next earnings (>= 0), or None if unknown.

        FIX FF-1 (HIGH): f-string SQL replaced with DuckDB parameterized query
        using ? placeholders. Consistent with FH-2 / IMF-2 pattern.
        Previous: WHERE symbol = '{symbol}' AND run_date = '{run_date}'
                  — SQL injection risk if symbol contains quotes/special chars.
        """
        pattern = str(SILVER_FUNDAMENTAL / "earnings_*.parquet")
        try:
            con = duckdb.connect()
            result = con.execute(
                """
                SELECT MIN(days_to_earnings) AS dte
                FROM read_parquet(?, hive_partitioning=false)
                WHERE symbol           = ?
                  AND days_to_earnings >= 0
                  AND run_date         = ?
                """,
                [pattern, symbol, str(run_date)],
            ).fetchone()
            if result and result[0] is not None:
                return int(result[0])
        except Exception:
            pass
        return None

    def get_upcoming_earnings(
        self,
        run_date: date,
        within_days: int = 14,
    ) -> pl.DataFrame:
        """
        Return all symbols with earnings in the next `within_days` days.
        Used by Gold Screener to populate near_earnings_flag.
        """
        pattern = str(SILVER_FUNDAMENTAL / f"earnings_{run_date.isoformat()}.parquet")
        try:
            if not Path(pattern).exists():
                return pl.DataFrame()
            # FIX SIL-RPQ-001: lazy scan → collect (single small file, policy consistency)
            df = pl.scan_parquet(pattern).collect()
            return (
                df.filter(
                    (pl.col("days_to_earnings") >= 0)
                    & (pl.col("days_to_earnings") <= within_days)
                )
                .select(["symbol", "earnings_date", "days_to_earnings"])
                .sort("days_to_earnings")
            )
        except Exception:
            return pl.DataFrame()


def run(run_date: date) -> None:
    """Job entry point — called by silver pipeline after bronze_finnhub."""
    proc = FundamentalProcessor()
    proc.process_earnings(run_date)
    proc.process_quotes(run_date)
    logger.info(f"[silver_fundamental] Complete for {run_date}")
