"""
silver_scope.py — GMI Wave 1 Bronze/Silver Solidification (pre-Cycle 4)
Canonical Layer 1 / Layer 2 Silver OHLCV glob-scoping helpers.

ROOT CAUSE this module fixes: every existing consumer of
data/silver/market_ohlcv/ (quality_validator.py's 5 CRITICAL/WARNING
checks, technical_signals.py's _process_timeframe) built its own recursive
glob — str(SILVER_OHLCV_PATH / "**" / pattern) — with NO market filter.
Since GMI Cycle 3 added Layer 2 context OHLCV under the SAME root
(market_ohlcv/context/symbol=X/...), every one of those unfiltered globs
was silently ALSO scanning Layer 2 rows. Empirically confirmed to cause:

  1. quality_validator.py::_check_coverage — Layer 2 symbols inflate the
     COUNT(DISTINCT symbol) numerator against a Layer-1-only
     get_loader().count() denominator, allowing coverage% to read >100%
     of the true Layer 1 figure and mask a real Layer 1 coverage drop.
  2. quality_validator.py::_check_freshness — a single fresh Layer 2
     anchor (e.g. VIX) hides pipeline-wide Layer 1 staleness, since
     MAX(timestamp) is computed across both layers combined.
  3. technical_signals.py::_process_timeframe — RSI/MACD/ADX/BBands
     computed for VIX, DXY, ETFs, and global indices as if they were
     tradeable candidates, polluting tech_signals_{TF}.parquet (the
     Gold signal store gold_mtf/gold_screener consume) with rows for
     instruments ADR-003 explicitly reclassified BECAUSE indicator
     computation doesn't make sense for them ("RSI pada VIX... tidak
     bermakna karena VIX adalah threshold-based regime indicator").

All three were live in the repository before this module existed — this
is a genuine correctness-bug fix, not a speculative hardening pass.

This module is the single place that knows how to scope a glob to
"Layer 1 only" or "Layer 2 (context) only". Every current and future
consumer — quality_validator, technical_signals, and Cycle 4's
GlobalIndexRegimeModule / CorrelationModule (which will deliberately want
BOTH layers, merged, and should do so explicitly via
layer1_globs() + [context_glob()] rather than reintroducing an
unfiltered recursive glob) — should use these helpers instead of
re-deriving market lists ad hoc.
"""

from __future__ import annotations

from pathlib import Path

# Instrument.market value for ALL Layer 2 instruments (Architecture
# Extension v1.0 §8.2 Decision 3 / GMI checkpoint Decision 3).
CONTEXT_MARKET: str = "context"


def layer1_markets() -> list[str]:
    """
    Return the distinct Layer 1 market values currently in the instrument
    universe (e.g. ['commodity', 'forex', 'idx', 'us_stocks']), derived
    from InstrumentLoader rather than hardcoded — stays correct
    automatically if a Layer 1 market is ever added or removed. 'index'
    never appears here: it has been permanently empty since ADR-003
    reclassified SPX/VIX to Layer 2 (Architecture Extension v1.0 §2.2).
    """
    from src.config.instrument_loader import get_loader
    return sorted({inst.market for inst in get_loader().all_symbols()})


def layer1_globs(silver_root: Path, filename_pattern: str) -> list[str]:
    """
    Build one glob per Layer 1 market subdirectory that actually exists on
    disk, e.g. for silver_root=data/silver/market_ohlcv and
    filename_pattern="*_1D_silver.parquet":
        ["data/silver/market_ohlcv/commodity/**/*_1D_silver.parquet",
         "data/silver/market_ohlcv/forex/**/*_1D_silver.parquet",
         "data/silver/market_ohlcv/idx/**/*_1D_silver.parquet",
         "data/silver/market_ohlcv/us_stocks/**/*_1D_silver.parquet"]

    Markets whose directory does not exist yet are skipped (fresh install
    / pre-backfill state) rather than included and later causing DuckDB's
    read_parquet() to raise on a zero-match glob when passed as part of a
    LIST — empirically confirmed: a single non-matching glob entry raises
    IOException for the WHOLE query, not just that one entry, so silently
    including it would make an otherwise-healthy market's check fail
    entirely rather than degrade gracefully.

    Returns [] if no Layer 1 market has data yet — callers should treat
    that identically to "no data yet" (the pre-fix single-glob behavior:
    return True / skip, never raise).
    """
    return [
        str(silver_root / market / "**" / filename_pattern)
        for market in layer1_markets()
        if (silver_root / market).exists()
    ]


def context_glob(silver_root: Path, filename_pattern: str) -> str | None:
    """
    Build the single Layer 2 (context) glob, or None if the context/
    directory does not exist yet (silver_context_anchors / Bronze Layer 2
    OHLCV not yet run, or pre-backfill). Callers should treat None the
    same way layer1_globs() returning [] is treated — "no data yet".
    """
    root = silver_root / CONTEXT_MARKET
    if not root.exists():
        return None
    return str(root / "**" / filename_pattern)
