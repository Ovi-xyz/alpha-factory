"""
legacy_correlation_bridge.py — GMI-CORR-RETIRE-01 (KNOWN_RISKS.md RISK-31,
gold_correlation retirement, 20 Sep 2026).

gold_correlation (src/gold/correlation_matrix.py, pre-Cycle-4: Layer
1-only, raw Pearson via Polars .corr(), sklearn AgglomerativeClustering)
is retired as of this fix — deregistered from JOB_REGISTRY/WEEKLY_SEQUENCE,
module archived to archive/gold_correlation_retirement_2026_09/. It never
produced live output on this repo (data/gold/correlation/ did not exist
at retirement time) — plausibly the same XAE-CAL-01 threshold-bug shape
already found and fixed in the three other Cycle 4 modules on 16 Sep
2026 (correlation_matrix.py's own history filter was
`n_days >= min_history_days * 0.8` = 52 calendar-day rows against a
65-day window, unreachable for any 5-day-week market) — unconfirmed
against real logs, noted here for the record rather than investigated
further, since the module is being retired regardless.

Two real consumers read gold_correlation's output path
(data/gold/correlation/correlation_clusters.parquet), both expecting the
legacy per-symbol schema (symbol, cluster_id, correlation_avg,
n_cluster_members, computed_date):

    screener.py::_deduplicate_by_cluster() — reads (symbol, cluster_id)
        directly for the GD §15.1 max-2-per-cluster concentration guard.
    views.py::v_correlation — the documented Trading Engine Interface
        Contract view (GD §0.4). An external, un-auditable consumer this
        pipeline cannot coordinate with — changing its promised column
        shape is a materially different risk than changing an internal
        caller.

Rather than rewrite both consumers against CorrelationModule's pairwise
schema (symbol_a, symbol_b, correlation, regime, computation_date,
cluster_id_a, cluster_id_b) — a hard cutover that touches two files and
changes the Interface Contract's promised shape for a system this project
can't coordinate with — this module derives the legacy shape FROM the new
pairwise output and writes it to the SAME path. screener.py and views.py
need zero code changes; they simply start receiving real data for the
first time, sourced from Ledoit-Wolf shrinkage over the merged Layer 1 +
Layer 2 universe instead of raw Pearson over Layer 1 only.

correlation_avg matches the old semantics exactly (mean(abs(correlation))
across every pair involving that symbol) — now computed over the richer
merged universe rather than Layer 1 alone. Layer 2 symbols appearing in
the derived file (VIX, DXY, global indices, ETFs, ...) are harmless to
screener.py: its dedup join is keyed off the MTF table, which only ever
contains Layer 1 active_ohlcv candidates (gold_signals is filtered to
active_ohlcv per Architecture v2.0 §5.2) — a Layer 2 row in this file
simply never matches anything and is never joined.

Called from correlation_module.py::run() immediately after
cross_asset_corr.parquet is written — best-effort: a failure here must
never fail the parent gold_cross_asset_correlation job, since
cross_asset_corr.parquet (not this legacy file) is that job's actual
deliverable.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import polars as pl
from loguru import logger

from src.utils.atomic_io import atomic_write_parquet

LEGACY_CORR_CLUSTERS_PATH = Path("data/gold/correlation/correlation_clusters.parquet")

_EMPTY_SCHEMA = {
    "symbol":            pl.Utf8,
    "cluster_id":        pl.Int64,
    "correlation_avg":   pl.Float64,
    "n_cluster_members": pl.Int64,
    "computed_date":     pl.Utf8,
}


def _empty_legacy_df() -> pl.DataFrame:
    return pl.DataFrame({name: pl.Series([], dtype=dtype) for name, dtype in _EMPTY_SCHEMA.items()})


def derive_legacy_correlation_clusters(pairwise: pl.DataFrame) -> pl.DataFrame:
    """
    Project CorrelationModule's pairwise long-format output into the
    legacy per-symbol schema gold_correlation used to produce.

    Each symbol appears in the pairwise table once per pair it's a member
    of (as symbol_a in some rows, symbol_b in others) — both sides are
    unioned here so every symbol's full set of pairwise correlations is
    seen regardless of which column it originally sat in. cluster_id is
    identical across every occurrence of a given symbol within one run
    (assigned once by CorrelationModule._cluster()), so grouping by
    (symbol, cluster_id) is a safe no-op on that dimension, not a
    collapse of genuinely different values.
    """
    required = {"symbol_a", "symbol_b", "correlation", "computation_date",
                "cluster_id_a", "cluster_id_b"}
    if pairwise.is_empty() or not required.issubset(set(pairwise.columns)):
        return _empty_legacy_df()

    side_a = pairwise.select([
        pl.col("symbol_a").alias("symbol"),
        pl.col("cluster_id_a").alias("cluster_id"),
        pl.col("correlation").abs().alias("abs_correlation"),
        pl.col("computation_date").cast(pl.Utf8).alias("computed_date"),
    ])
    side_b = pairwise.select([
        pl.col("symbol_b").alias("symbol"),
        pl.col("cluster_id_b").alias("cluster_id"),
        pl.col("correlation").abs().alias("abs_correlation"),
        pl.col("computation_date").cast(pl.Utf8).alias("computed_date"),
    ])
    long = pl.concat([side_a, side_b], how="vertical")

    per_symbol = (
        long.group_by(["symbol", "cluster_id", "computed_date"])
        .agg(pl.col("abs_correlation").mean().alias("correlation_avg"))
    )

    cluster_sizes = (
        per_symbol.group_by("cluster_id")
        .agg(pl.col("symbol").n_unique().alias("n_cluster_members"))
    )

    return (
        per_symbol.join(cluster_sizes, on="cluster_id", how="left")
        .select(["symbol", "cluster_id", "correlation_avg", "n_cluster_members", "computed_date"])
    )


def write_legacy_correlation_clusters(pairwise: pl.DataFrame) -> Optional[Path]:
    """
    Derive + atomically write correlation_clusters.parquet at
    gold_correlation's old output path. Returns the path written, or None
    if there was nothing to derive (empty/malformed input) — matches
    every other Gold module's "no data, skip, don't crash" convention.

    Deliberately best-effort: exceptions are caught and logged, never
    raised — this file is a backward-compatibility convenience for two
    downstream readers, not CorrelationModule's own deliverable.
    """
    try:
        derived = derive_legacy_correlation_clusters(pairwise)
        if derived.is_empty():
            logger.debug(
                "[legacy_correlation_bridge] Nothing to derive"
                " — pairwise input empty or malformed"
            )
            return None

        LEGACY_CORR_CLUSTERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_parquet(
            derived,
            LEGACY_CORR_CLUSTERS_PATH,
            compression="zstd",
            compression_level=3,
        )
        logger.info(
            f"[legacy_correlation_bridge] {derived['symbol'].n_unique()} symbols"
            f" -> {LEGACY_CORR_CLUSTERS_PATH.name}"
            " (screener.py / views.py:v_correlation compatibility bridge — RISK-31)"
        )
        return LEGACY_CORR_CLUSTERS_PATH
    except Exception as e:
        logger.warning(f"[legacy_correlation_bridge] Derivation/write failed (non-fatal): {e}")
        return None
