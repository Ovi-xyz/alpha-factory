"""
delta_reprocessor.py — GD §13.3 (Silver Delta Reprocessor)
Deteksi Silver records dengan processing_version != current → trigger reprocess.

Diperlukan setelah upgrade Silver formula (e.g. VWAP fix di v1.2):
    - Scan semua Silver files untuk processing_version field
    - Identify stale symbols (version mismatch)
    - Trigger OHLCVProcessor untuk reprocess hanya symbol yang stale
    - Tidak menyentuh Bronze (anti-pattern)

Usage:
    python -m src.utils.delta_reprocessor --dry-run   # List stale symbols
    python -m src.utils.delta_reprocessor              # Reprocess stale symbols
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from loguru import logger

from src.silver.ohlcv_processor import (
    BRONZE_OHLCV_PATH,
    CURRENT_SILVER_VERSION,
    SILVER_OHLCV_PATH,
    OHLCVProcessor,
)
from src.utils.silver_scope import layer1_globs

SILVER_OHLCV_ROOT = Path("data/silver/market_ohlcv")
# FIX ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8,
# 2026-07-11): SILVER_GLOB used to be a hardcoded unfiltered
# "market_ohlcv/**/*_silver.parquet" string — the same RISK-6 defect class
# already fixed elsewhere (quality_validator.py, technical_signals.py,
# screener.py, correlation_matrix.py, pit_data.py, views.py). Concretely
# here: find_stale_symbols()'s aggregate GROUP BY symbol/timeframe could
# report Layer 2 context instruments (VIX, DXY, ETFs — added in GMI
# Cycle 3, same market_ohlcv/ root) as "stale," inflating dry-run reports
# and stale counts. reprocess() itself was NOT silently misprocessing
# Layer 2 symbols — loader.get(symbol) is Layer-1-only and already raises
# KeyError -> logged + skipped for any Layer 2 symbol that slipped through
# — but the diagnostic-accuracy issue (dry-run reporting phantom Layer 2
# "stale" work) was real and worth fixing for consistency.
#
# SILVER_GLOB is kept as a module-level attribute — its EXISTING role as a
# test-only override point (_effective_glob() below) is unchanged and
# fully preserved (tests already monkeypatch it directly to a tmp_path
# string). Its default value is now None (sentinel for "not overridden"),
# not a hardcoded unfiltered string — the actual default glob is computed
# dynamically from the current filesystem state via layer1_globs(),
# exactly like every other fix in this pass.
SILVER_GLOB = None


class DeltaReprocessor:
    """
    Find and reprocess stale Silver OHLCV files.

    Stale = processing_version != CURRENT_SILVER_VERSION
    or processing_version IS NULL (old files without version metadata).
    """

    def __init__(self) -> None:
        self._proc = OHLCVProcessor()

    def find_stale_symbols(self) -> list[dict]:
        """
        Return list of {symbol, timeframe, current_version} for stale records.
        """
        try:
            con  = duckdb.connect()
            con.execute("SET memory_limit='2GB';")
            glob = self._effective_glob()

            result = con.execute(
                """
                SELECT
                    symbol,
                    COALESCE(timeframe, '') AS timeframe,
                    MAX(processing_version) AS current_version,
                    COUNT(*) AS row_count
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE processing_version IS NULL
                   OR processing_version != $current_version
                GROUP BY symbol, timeframe
                ORDER BY symbol, timeframe
                """,  # FIX UTL-SQL-001: $name parameterized (GD §17.7)
                {"glob": glob, "current_version": CURRENT_SILVER_VERSION},
            ).pl()

            stale = result.to_dicts()
            if stale:
                logger.info(
                    f"[DeltaReprocessor] Found {len(stale)} stale symbol/TF"
                    f" combinations (current version: {CURRENT_SILVER_VERSION})"
                )
            else:
                logger.info(
                    f"[DeltaReprocessor] All Silver files are"
                    f" version {CURRENT_SILVER_VERSION} — nothing to reprocess"
                )
            return stale

        except Exception as e:
            logger.warning(f"[DeltaReprocessor] Scan failed: {e}")
            return []

    def reprocess(
        self,
        run_date: date,
        stale: Optional[list[dict]] = None,
        dry_run: bool = False,
    ) -> int:
        """
        Reprocess stale symbols from Bronze → Silver.

        Args:
            run_date:  Pipeline run date for reproducibility
            stale:     Pre-computed stale list; None = auto-detect
            dry_run:   Log only, no actual reprocessing

        Returns:
            Number of symbol/TF combinations reprocessed.
        """
        if stale is None:
            stale = self.find_stale_symbols()

        if not stale:
            return 0

        if dry_run:
            logger.info(
                f"[DeltaReprocessor] DRY RUN — would reprocess {len(stale)} items:"
            )
            for item in stale[:20]:
                logger.info(
                    f"  {item['symbol']}/{item['timeframe']}"
                    f" (was: {item['current_version']})"
                )
            return len(stale)

        reprocessed = 0
        from src.config.instrument_loader import get_loader
        loader = get_loader()

        for item in stale:
            symbol   = item["symbol"]
            timeframe = item["timeframe"]

            try:
                inst = loader.get(symbol)
            except KeyError:
                logger.warning(
                    f"[DeltaReprocessor] {symbol}: not in instrument loader"
                    " — skipping"
                )
                continue

            try:
                # Read Bronze source
                pattern = str(
                    BRONZE_OHLCV_PATH / inst.market
                    / f"source=yfinance/symbol={symbol}/**/*.parquet"
                )
                try:
                    df = pl.read_parquet(pattern)
                except Exception:
                    logger.debug(
                        f"[DeltaReprocessor] No Bronze data for {symbol}"
                    )
                    continue

                # Reprocess through OHLCVProcessor
                silver_df = self._proc.process_symbol(
                    df=df,
                    symbol=symbol,
                    market=inst.market,
                    timeframe=timeframe,
                    tz_hint=inst.timezone,
                )

                if silver_df is not None and len(silver_df) > 0:
                    self._proc.write(
                        silver_df, symbol, inst.market, timeframe
                    )
                    reprocessed += 1
                    logger.debug(
                        f"[DeltaReprocessor] Reprocessed {symbol}/{timeframe}"
                        f" → v{CURRENT_SILVER_VERSION}"
                    )

            except Exception as e:
                logger.error(
                    f"[DeltaReprocessor] Failed {symbol}/{timeframe}: {e}"
                )

        logger.info(
            f"[DeltaReprocessor] Complete: {reprocessed}/{len(stale)}"
            f" symbol/TF pairs reprocessed to v{CURRENT_SILVER_VERSION}"
        )
        return reprocessed

    def get_version_summary(self) -> dict[str, int]:
        """Return {version: count} summary of all Silver files."""
        try:
            con  = duckdb.connect()
            glob = self._effective_glob()
            rows = con.execute(
                """
                SELECT
                    COALESCE(processing_version, 'NULL') AS version,
                    COUNT(*)                              AS row_count
                FROM read_parquet($glob, hive_partitioning=true)
                GROUP BY processing_version
                ORDER BY version
                """,  # FIX UTL-SQL-001: $name parameterized (GD §17.7)
                {"glob": glob},
            ).fetchall()
            return {r[0]: r[1] for r in rows}
        except Exception:
            return {}

    def _effective_glob(self):
        """Return the effective glob pattern(s) — testable override point.

        UPD ADR-022/RISK-6: if SILVER_GLOB has been explicitly monkeypatched
        (tests), honor it exactly as before (string or list, whatever the
        test provides). Otherwise (production default, SILVER_GLOB is the
        None sentinel), compute the CURRENT Layer1-scoped glob list fresh
        via layer1_globs() — never the old hardcoded unfiltered string.
        """
        import src.utils.delta_reprocessor as _self_mod
        override = getattr(_self_mod, "SILVER_GLOB", None)
        if override is not None:
            return override
        return layer1_globs(SILVER_OHLCV_ROOT, "*_silver.parquet")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Silver Delta Reprocessor — reprocess stale Silver files"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List stale symbols without reprocessing",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Override run date YYYY-MM-DD (default: today)",
    )
    args = parser.parse_args()

    run_date  = date.fromisoformat(args.date) if args.date else date.today()
    processor = DeltaReprocessor()

    # Show version summary
    summary = processor.get_version_summary()
    if summary:
        logger.info("[DeltaReprocessor] Current Silver version distribution:")
        for ver, count in summary.items():
            marker = " ✓" if ver == CURRENT_SILVER_VERSION else " ✗ STALE"
            logger.info(f"  v{ver}: {count} symbol/TF combos{marker}")

    stale = processor.find_stale_symbols()
    processor.reprocess(run_date, stale, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
