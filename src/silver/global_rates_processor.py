"""
global_rates_processor.py — Silver Layer — Global CB Rates Processor
Data Source & Rates Adjustment v1.0 §9

Transform Bronze BIS CBPOL_D data into silver_global_rates.parquet —
a dedicated Silver table with daily forward-filled CB policy rates,
structural break flags, meeting-day detection, and PIT vintage_date.

WHY a separate table (not silver_macro_enriched):
  Data Source & Rates Adjustment v1.0 §9.1:
  CB rates use 'effective_date' semantics (when decision took effect), not
  'observation_date' (when data was published). Mixing both semantics in
  silver_macro_enriched violates PIT integrity (GD §4.5). A dedicated table
  preserves the correct semantic without contaminating the macro enriched table.

Architecture constraints:
  - GD §17.2 Layer 2 (Silver): reads ONLY from data/bronze/ — never from Gold
  - GD §17.4: Silver processor stateless, no external API calls
  - GD §17.7: No f-string SQL (all DuckDB queries use $name binding)
  - Supplementary Design G2 / GD §7.1: atomic write via tempfile + os.replace
  - GD §4.1: Silver data must be UTC-normalized (CB dates already date-typed,
    no tz conversion needed for daily rates — effective_date is calendar date)

Output: data/silver/global_rates/global_rates_policy.parquet
Schema:
  central_bank, observation_date, rate_pct, rate_bps, effective_date,
  is_meeting_day, direction_change, magnitude_bps, has_structural_break,
  structural_break_id, forward_fill_days, is_stale, vintage_date,
  processing_version

Dikonsumsi oleh: CrossAssetEngine (domain score computation),
                 ForecastModule (PCA pre-processing Layer 2 input),
                 gold_regime (global macro regime detection)
"""

from __future__ import annotations

import os
import tempfile
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import yaml
from loguru import logger

# ── Konstanta ─────────────────────────────────────────────────────────────────

PROCESSOR_VERSION = "1.0.0"

_BRONZE_PATH    = Path("data/bronze/macro/bis_cb_rates")
_OUTPUT_PATH    = Path("data/silver/global_rates")
_OUTPUT_FILE    = "global_rates_policy.parquet"
_BIS_CONFIG     = Path("config/bis_cb_rates.yaml")

_FORWARD_FILL_ALERT_DAYS = 90   # is_stale=True jika forward_fill_days > ini

# Structural break registry dari bis_cb_rates.yaml — diparse sekali saat modul load
_STRUCTURAL_BREAKS: list[dict] = []

def _load_structural_breaks() -> list[dict]:
    """Parse structural break registry dari bis_cb_rates.yaml."""
    if not _BIS_CONFIG.exists():
        logger.warning(f"[GlobalRates] bis_cb_rates.yaml not found at {_BIS_CONFIG}")
        return []
    cfg = yaml.safe_load(_BIS_CONFIG.read_text())
    breaks = []
    for break_id, break_cfg in cfg.get("structural_break_registry", {}).items():
        breaks.append({
            "break_id":    break_id,
            "central_bank": break_cfg.get("central_bank"),
            "break_date":  date.fromisoformat(break_cfg["break_date"]),
            "end_date":    (
                date.fromisoformat(break_cfg["end_date"])
                if break_cfg.get("end_date") else None
            ),
            "severity":    break_cfg.get("severity", "MEDIUM"),
            "type":        break_cfg.get("type", "unknown"),
        })
    logger.debug(f"[GlobalRates] Loaded {len(breaks)} structural break entries")
    return breaks


class GlobalRatesProcessor:
    """
    Transform Bronze BIS CB rates into Silver global_rates_policy.parquet.

    Processing steps:
      1. Scan all Bronze BIS Parquet files (append-only, may span multiple runs)
      2. Deduplicate by (central_bank, obs_date) — keep latest _ingested_at
      3. Generate daily calendar from min_date to run_date for all CBs
      4. Forward-fill rate_pct from last known meeting value
      5. Compute derived columns: rate_bps, is_meeting_day, direction_change,
         magnitude_bps, forward_fill_days, is_stale
      6. Apply structural break flags from bis_cb_rates.yaml registry
      7. Add PIT vintage_date (= run_date — when this Silver version was computed)
      8. Atomic write to data/silver/global_rates/global_rates_policy.parquet

    NOTE on forward-fill (GD §17.7 anti-pattern guard):
      DuckDB forward-fill is done using Polars .forward_fill() — NOT via f-string
      SQL interpolation. All DuckDB queries use $name parameterized binding.
    """

    def __init__(self) -> None:
        self._breaks = _load_structural_breaks()

    def run(self, run_date: date | None = None) -> Path | None:
        """
        Job entry point. run_date = PIT vintage_date of this Silver version.
        Returns output parquet path, or None if no Bronze data found.
        """
        run_date = run_date or date.today()
        logger.info(f"[GlobalRates] Processing run_date={run_date}")

        bronze_df = self._load_bronze()
        if bronze_df is None:
            logger.warning(
                "[GlobalRates] No Bronze BIS data found — "
                "run bronze_bis_rates first"
            )
            return None

        silver_df = self._transform(bronze_df, run_date)
        output_path = self._save(silver_df, run_date)
        logger.success(
            f"[GlobalRates] {len(silver_df)} Silver rows written to {output_path}"
        )
        return output_path

    def _load_bronze(self) -> pl.DataFrame | None:
        """
        Scan all Bronze BIS Parquet files lazily, deduplicate by
        (central_bank, obs_date) keeping latest ingestion.
        Polars lazy API (GD §17.7 preference: scan_parquet over read_parquet).
        """
        pattern = str(_BRONZE_PATH / "**" / "*.parquet")
        try:
            df = (
                pl.scan_parquet(pattern, hive_partitioning=False)
                .sort("_ingested_at", descending=True)
                .unique(subset=["central_bank", "obs_date"], keep="first")
                .sort(["central_bank", "obs_date"])
                .collect()
            )
            if df.is_empty():
                return None
            logger.info(
                f"[GlobalRates] Bronze loaded: {len(df)} unique (CB, date) rows"
            )
            return df
        except Exception as exc:
            logger.warning(f"[GlobalRates] Bronze scan failed: {exc}")
            return None

    def _transform(self, bronze_df: pl.DataFrame, run_date: date) -> pl.DataFrame:
        """
        Core Silver transformation pipeline.
        """
        # Step 1: Get all central banks and full date calendar
        cbs = bronze_df["central_bank"].unique().to_list()
        min_date = bronze_df["obs_date"].min()
        all_dates = [
            min_date + timedelta(days=i)
            for i in range((run_date - min_date).days + 1)
        ]

        # Step 2: Cross-join calendar x CBs -> full grid
        calendar_df = pl.DataFrame({
            "observation_date": all_dates * len(cbs),
            "central_bank": [cb for cb in cbs for _ in all_dates],
        }).sort(["central_bank", "observation_date"])

        # Step 3: Left join Bronze onto calendar
        merged = (
            calendar_df
            .join(
                bronze_df.rename({"obs_date": "observation_date"}),
                on=["central_bank", "observation_date"],
                how="left",
            )
            .sort(["central_bank", "observation_date"])
        )

        # Step 4: Detect meeting days (rate changed) before forward-fill
        merged = merged.with_columns([
            # is_meeting_day: rate_pct was explicitly published for this date
            pl.col("rate_pct").is_not_null().alias("is_meeting_day"),
        ])

        # Step 5: Forward-fill rate_pct per CB
        merged = merged.with_columns(
            pl.col("rate_pct")
              .forward_fill()
              .over("central_bank")
              .alias("rate_pct_filled")
        ).drop("rate_pct").rename({"rate_pct_filled": "rate_pct"})

        # Step 6: rate_bps = rate_pct * 100 (per GD §17.7: Bronze is as-is,
        # Silver adds derived columns)
        merged = merged.with_columns(
            (pl.col("rate_pct") * 100).alias("rate_bps")
        )

        # Step 7: direction_change and magnitude_bps (only meaningful on meeting days)
        merged = merged.with_columns([
            pl.col("rate_bps")
              .diff()
              .over("central_bank")
              .alias("_rate_bps_diff"),
        ]).with_columns([
            pl.when(pl.col("is_meeting_day") & (pl.col("_rate_bps_diff") > 0)).then(1)
              .when(pl.col("is_meeting_day") & (pl.col("_rate_bps_diff") < 0)).then(-1)
              .when(pl.col("is_meeting_day")).then(0)
              .otherwise(0)
              .cast(pl.Int8)
              .alias("direction_change"),
            pl.when(pl.col("is_meeting_day"))
              .then(pl.col("_rate_bps_diff").abs())
              .otherwise(None)
              .cast(pl.Float64)
              .alias("magnitude_bps"),
        ]).drop("_rate_bps_diff")

        # Step 8: effective_date (last meeting date — derived via argmax trick)
        # For each (CB, date), effective_date = last meeting date <= observation_date
        # We compute this by forward-filling the observation_date where is_meeting_day
        merged = merged.with_columns([
            pl.when(pl.col("is_meeting_day"))
              .then(pl.col("observation_date"))
              .otherwise(None)
              .forward_fill()
              .over("central_bank")
              .alias("effective_date"),
        ])

        # Step 9: forward_fill_days and is_stale
        merged = merged.with_columns([
            (pl.col("observation_date") - pl.col("effective_date"))
              .dt.total_days()
              .cast(pl.Int32)
              .alias("forward_fill_days"),
        ]).with_columns([
            (pl.col("forward_fill_days") > _FORWARD_FILL_ALERT_DAYS)
              .alias("is_stale"),
        ])

        # Step 10: Structural break flags
        merged = self._apply_structural_breaks(merged)

        # Step 11: PIT vintage_date + processing_version
        merged = merged.with_columns([
            pl.lit(run_date).cast(pl.Date).alias("vintage_date"),
            pl.lit(PROCESSOR_VERSION).alias("processing_version"),
        ])

        # Step 12: Select and order final schema columns
        final_cols = [
            "central_bank", "observation_date", "rate_pct", "rate_bps",
            "effective_date", "is_meeting_day", "direction_change",
            "magnitude_bps", "has_structural_break", "structural_break_id",
            "forward_fill_days", "is_stale", "vintage_date", "processing_version",
        ]
        return merged.select(final_cols)

    def _apply_structural_breaks(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply structural break flags from bis_cb_rates.yaml registry.
        Conservative pattern: flag any (CB, date) within a registered break window.
        """
        # Initialize columns
        df = df.with_columns([
            pl.lit(False).alias("has_structural_break"),
            pl.lit(None).cast(pl.Utf8).alias("structural_break_id"),
        ])

        for brk in self._breaks:
            cb         = brk["central_bank"]
            break_id   = brk["break_id"]
            break_date = brk["break_date"]
            end_date   = brk["end_date"]

            if end_date is None:
                # Ongoing break — flag everything from break_date onward for this CB
                mask = (
                    (pl.col("central_bank") == cb)
                    & (pl.col("observation_date") >= break_date)
                )
            else:
                mask = (
                    (pl.col("central_bank") == cb)
                    & (pl.col("observation_date") >= break_date)
                    & (pl.col("observation_date") <= end_date)
                )

            df = df.with_columns([
                pl.when(mask)
                  .then(True)
                  .otherwise(pl.col("has_structural_break"))
                  .alias("has_structural_break"),
                pl.when(mask)
                  .then(pl.lit(break_id))
                  .otherwise(pl.col("structural_break_id"))
                  .alias("structural_break_id"),
            ])

        n_flagged = df.filter(pl.col("has_structural_break"))["central_bank"].len()
        logger.debug(f"[GlobalRates] {n_flagged} rows flagged with structural breaks")
        return df

    def _save(self, df: pl.DataFrame, run_date: date) -> Path:
        """
        Atomic write to Silver (Supplementary Design G2 / GD §7.1 pattern).
        zstd compression level 3 (Silver standard — GD §7.1).
        os.replace is POSIX-atomic — no partial Parquet on crash.
        """
        _OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
        final_path = _OUTPUT_PATH / _OUTPUT_FILE

        with tempfile.NamedTemporaryFile(
            dir=_OUTPUT_PATH, suffix=".parquet.tmp", delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)

        try:
            df.write_parquet(
                tmp_path,
                compression="zstd",
                compression_level=3,
                row_group_size=50_000,
            )
            os.replace(tmp_path, final_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

        return final_path


def run(run_date: date | None = None) -> None:
    """Job entry point — dikonsumsi oleh job_registry.py."""
    GlobalRatesProcessor().run(run_date=run_date)
