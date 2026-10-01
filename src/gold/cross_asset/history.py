"""
src/gold/cross_asset/history.py — GMI-PIT-001 follow-up (ADR-050 finding, RISK-35)

Point-in-time history for the three weekly CrossAssetEngine outputs that
are OVERWRITTEN wholesale on every run:

    cross_asset_corr.parquet       (CorrelationModule)
    lead_lag_matrix.parquet        (LeadLagModule)
    cross_asset_forecast.parquet   (ForecastModule)

Why a SEPARATE store instead of making those files retain history
---------------------------------------------------------------------
gold_screener LEFT JOINs lead_lag_tbl ON symbol = follower and
forecast_tbl ON symbol = symbol against the WHOLE file, with no
computation_date filter (screener.py). If those files accumulated one
snapshot per week, every watchlist row would fan out once per retained
week — silently duplicating the primary Gold output. legacy_correlation_
bridge.py also derives its output from cross_asset_corr.parquet's current
contents. Changing the snapshot files' meaning would be an Interface
Contract change (GD §17.6) rippling into the screener; an additive
history store changes nothing any existing consumer reads.

Layout (same one-dated-file-per-run convention as mtf_alignment_{date}
and signal_aggregation_{date}):

    data/gold/cross_asset/history/<name>/<name>_<YYYY-MM-DD>.parquet

Each file is that run's complete output, written at the run's own
run_date — so every stored snapshot is point-in-time correct by
construction. Re-running the same run_date replaces that date's file
(idempotent). Reads go through PITDataLoader, which selects the latest
snapshot with computation_date <= trade_date.

Scope of what this does NOT fix: history only accrues from the first
weekly run after this ships. It is not a backfill — the modules need a
resolved active-symbol universe for the run_date (ActiveSymbolsResolver /
ContextAnchorsResolver), and those artifacts only exist for dates the
pipeline actually ran.

Persistence failures are logged and swallowed: history is a side-effect
and must never fail the weekly job that produced the real output.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from src.utils.atomic_io import atomic_write_parquet

HISTORY_ROOT = Path("data/gold/cross_asset/history")

CORR_HISTORY_NAME     = "cross_asset_corr"
LEAD_LAG_HISTORY_NAME = "lead_lag_matrix"
FORECAST_HISTORY_NAME = "cross_asset_forecast"


def history_path(name: str, computation_date: date) -> Path:
    return HISTORY_ROOT / name / f"{name}_{computation_date.isoformat()}.parquet"


def write_history_snapshot(df: pl.DataFrame, name: str, computation_date: date) -> None:
    """Write this run's full output as the snapshot for computation_date.
    Idempotent per (name, computation_date); never raises."""
    try:
        path = history_path(name, computation_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_parquet(df, path, compression="zstd", compression_level=3)
    except Exception as e:
        logger.warning(f"[cross_asset history] {name} {computation_date}: write failed (non-blocking): {e}")
