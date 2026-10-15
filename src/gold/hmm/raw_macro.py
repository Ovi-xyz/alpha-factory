"""
src/gold/hmm/raw_macro.py — ADR-051 (GMI-HMM2-001), macro branch, module 1 of 5.

Layer 1 of hmm-gatekeeper-xgboost.md ("Macro Raw Observation Module"): the
cadence-bucketed (daily / weekly / monthly / quarterly), forward-filled-to-daily,
POINT-IN-TIME-GATED FRED panel for five macro domains — monetary, growth,
inflation, stress, liquidity — routed per series by the `hmm:` block of
config/fred_series.yaml (validated by config.macro_config).

Input   data/silver/macro_enriched/fred_<date>_silver.parquet   (latest
        snapshot <= run_date; Silver only — GD §17.2, Gold never reads Bronze)
Output  data/gold/hmm/raw_macro.parquet
            as_of_date                 Date    weekday calendar — "what was known at the
                                               end of this day" (NOT an observation date)
            <feature> x15              Float64 one column per hmm.features entry
            obs_date__<feature>        Date    the period the carried value MEASURES
            available_on__<feature>    Date    first date the system could use it
            avail_basis__<feature>     String  recorded | estimated | mixed — HOW that
                                               date was determined (see fred_pit)
            computation_date           Date    run_date of the build
            processing_version         String  panel-formula version
            source_snapshot            String  Silver snapshot file used (lineage)
            pit_basis                  String  hmm.pit.basis the panel was built with
        Ages are not stored: as_of_date - obs_date__<feature>.

DOMAIN INDEPENDENCE (ADR-051 Amendment 2 §A): FRED/Silver macro only. No Gold
store is read here, and nothing in the CrossAssetEngine or the technical chain
reads this one.

IDEMPOTENCY / HISTORY SEMANTICS: every run rebuilds the whole panel from the
latest Silver snapshot (a few thousand rows x ~30 columns, sub-second) and
atomically replaces the store. Rebuilding is correct because availability is
gated per row, not per run — a row for date D never contains a value that
was not usable on D under fred_pit's rule. What a rebuild cannot recover is
the VALUE vintage: Silver FRED carries current-vintage values only
(is_revision is False throughout). See fred_pit's module docstring.

FAILURE POLICY: fail loudly on a registry/config defect — no snapshot at all,
or a configured series with zero observations in the snapshot (the repo's
recurring registry-gate silent-drop class). A series with merely SHORT history
(BAMLH0A0HYM2, from 2023-08-21) is not an error here: it produces nulls, and
the consumer flags it `short_history`.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import polars as pl
from loguru import logger

from src.gold.hmm import fred_pit
from src.gold.hmm.config import MacroConfig, macro_config
from src.utils.atomic_io import atomic_write_parquet

GOLD_HMM_PATH       = Path("data/gold/hmm")
RAW_MACRO_PATH      = GOLD_HMM_PATH / "raw_macro.parquet"
PROCESSING_VERSION  = "1.1"      # 1.1: obs/available/as-of dates replace age__; pit_basis lineage


class RawMacroError(RuntimeError):
    """Registry/config/data defect that makes the macro panel unbuildable."""


def build(
    run_date: date,
    silver_dir: Path | str | None = None,
    config_path: Path | str | None = None,
    cfg: Optional[MacroConfig] = None,
) -> pl.DataFrame:
    """Build the PIT-gated daily macro panel as of `run_date` (no I/O write)."""
    cfg = cfg or macro_config(config_path)

    snap = fred_pit.latest_snapshot(run_date, silver_dir)
    if snap is None:
        raise RawMacroError(
            f"no FRED Silver snapshot on or before {run_date} — run silver_macro first"
        )

    needed = cfg.required_series()
    obs = fred_pit.load_observations(snap, needed)
    missing = sorted(set(needed) - set(obs["series_id"].unique().to_list()))
    if missing:
        raise RawMacroError(
            f"{snap.name}: configured series with no observations: {missing} "
            f"(registry/ingestion defect — not a short-history case)"
        )

    panel = fred_pit.build_panel(obs, cfg.features, cfg.lag_days, end=run_date, basis=cfg.pit_basis)
    if panel.height == 0:
        raise RawMacroError(f"{snap.name}: PIT-gated panel is empty as of {run_date}")

    return panel.with_columns(
        pl.lit(run_date).alias("computation_date"),
        pl.lit(PROCESSING_VERSION).alias("processing_version"),
        pl.lit(snap.name).alias("source_snapshot"),
        pl.lit(cfg.pit_basis).alias("pit_basis"),
    )


def run(run_date: date) -> None:
    """Job entry point ('gold_raw_macro' in JOB_REGISTRY)."""
    df = build(run_date)
    atomic_write_parquet(df, RAW_MACRO_PATH)
    logger.info(
        f"[raw_macro] {run_date}: {df.height} rows "
        f"{df['as_of_date'].min()}..{df['as_of_date'].max()} from {df['source_snapshot'][0]} "
        f"-> {RAW_MACRO_PATH}"
    )
