"""
correlation_module.py — Architecture v2.0 §6.2 (CrossAssetEngine —
CorrelationModule). GMI Wave 1 Cycle 4 — second module (after
GlobalIndexRegimeModule).

Weekly (Sunday) module computing a Ledoit-Wolf-shrunk correlation matrix
across the MERGED Layer 1 (active_ohlcv) + Layer 2 (correlation_context)
universe, plus hierarchical-clustering cluster assignment.

Deliberately independent of the pre-Cycle-4 gold_correlation job
(src/gold/correlation_matrix.py — Layer 1-only, raw Pearson via Polars
.corr(), sklearn AgglomerativeClustering). Architecture v2.0 §5.1 marks
that module "REPLACED", but actually retiring it — deregistering
gold_correlation, redirecting anything that reads
correlation_clusters.parquet — is a separate, larger decision than "build
Cycle 4's CorrelationModule", out of scope here. Both coexist under
different job names (gold_correlation vs gold_cross_asset_correlation)
until that separate decision is made.

Universe: layer1_globs() + [context_glob()] merged explicitly, per
silver_scope.py's own module docstring instruction — NOT an unfiltered
recursive glob (that bug class is exactly what silver_scope.py exists to
prevent). Symbol list = ActiveSymbolsResolver.load_ohlcv(run_date)
(Layer 1) ∪ ContextAnchorsResolver.load(run_date) (Layer 2 — identical
symbol set to InstrumentLoader.correlation_context(), since both are
simply "context_available=True" with no further filter). Both resolvers
raise FileNotFoundError if not yet resolved for run_date — caught here
and treated as "skip, log warning", matching every other Gold module's
convention for missing upstream data (see e.g. active_symbols.py's own
docstring on context_anchors.py: "cannot fail, block, or be blocked by").

Estimator: sklearn.covariance.LedoitWolf — mandatory per Architecture
v2.0 §6.2.1 for a ~240×240 matrix on a ~60-day window (n_features >>
n_samples; raw Pearson is unstable at this ratio). This single choice
also satisfies §6.2.1's separate "Shanghai handling: robust estimator"
requirement — Ledoit-Wolf is explicitly named there as one of the two
acceptable choices (the other, MinCovDet, is not used here) — SSEC gets
no separate special-casing.

Regime conditioning: each weekly run is tagged with the CURRENT regime
label (most recent row in gold/macro/regime_store.parquet with
date <= run_date). This is NOT "N separate matrices, one per possible
regime label, computed simultaneously" — Architecture v2.0's §6.2.1 text
("separate matrices computed per active regime... stored with regime
label") only becomes meaningful once enough weekly snapshots accumulate
across different regimes to compare against each other; a single fresh
run has exactly one current regime to tag itself with. Documented here
explicitly rather than silently implementing a narrower reading of the
spec without saying so.

Clustering: scipy.cluster.hierarchy.linkage + fcluster on the
1 - |correlation| distance (Architecture v2.0 §6.2.1 names scipy
specifically for this module, unlike the old correlation_matrix.py's
sklearn.cluster.AgglomerativeClustering — a deliberate, spec-driven
difference, not an inconsistency).

Output: data/gold/cross_asset/cross_asset_corr.parquet
Schema (Architecture v2.0 §6.2.2, with one clarification): symbol_a,
symbol_b, correlation, regime, computation_date, cluster_id_a,
cluster_id_b. The doc's own schema lists a single "cluster_id" column on
a PAIR-level row, which is ambiguous — a pair can span two different
clusters. Resolved here as cluster_id_a/cluster_id_b (each symbol's own
assignment) rather than silently picking one and calling it "cluster_id".
One row per unordered pair (symbol_a < symbol_b lexicographically) — no
self-pairs, no mirrored duplicates.

Job registry: 'gold_cross_asset_correlation', depends_on=
['silver_active_symbols', 'silver_context_anchors'] (weekly). NOT yet
wired into gold_screener or LeadLagModule's own eventual consumption of
this output — out of scope for this pass.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import polars as pl
from loguru import logger

from src.silver.active_symbols import ActiveSymbolsResolver
from src.silver.context_anchors import ContextAnchorsResolver
from src.utils.atomic_io import atomic_write_parquet
from src.utils.silver_scope import context_glob, layer1_globs

SILVER_OHLCV_PATH     = Path("data/silver/market_ohlcv")
REGIME_STORE_PATH     = Path("data/gold/macro/regime_store.parquet")
GOLD_CROSS_ASSET_PATH = Path("data/gold/cross_asset")
CROSS_ASSET_CORR_PATH = GOLD_CROSS_ASSET_PATH / "cross_asset_corr.parquet"

LOOKBACK_DAYS      = 65   # Calendar days — bounds the Silver scan window only (still
                           # needed so correlation/pivot has a common date index across
                           # symbols). Rolling window, matching gold_correlation's own
                           # convention.
MIN_OBSERVATIONS   = 40   # FIX XAE-CAL-01 (16 Sep 2026): absolute count of real
                           # (is_clean, non-null) daily bars required within
                           # LOOKBACK_DAYS — NOT a % of calendar days. The prior
                           # MIN_HISTORY_RATIO=0.8 required int(65*0.8)=52 calendar-day
                           # rows, but a 65-calendar-day window contains at most 48
                           # weekdays (empirically verified against live Silver data,
                           # 16 Sep 2026 session) — unreachable for any 5-day-week
                           # market (equities, IDX, forex, commodities), so this
                           # silently zeroed out CorrelationModule every week
                           # regardless of data quality. "Observation-count native":
                           # the threshold is a literal row count, not a calendar-
                           # density ratio. "Calendar-aware": LOOKBACK_DAYS still
                           # bounds recency, and no per-market trading calendar is
                           # modeled — the real Silver row count already reflects
                           # each market's actual calendar (US holidays, IDX closures
                           # incl. Eid al-Fitr, FX weekend gaps), so counting
                           # observations directly is calendar-aware by construction.
                           # Calibrated against live Silver data across every market
                           # type in the universe (valid obs in the current 65-day
                           # window: AAPL 45, BBCA/idx 44, EUR_USD/forex 47,
                           # CL/commodity 47, SSEC/ctx 45, JKSE/ctx 43, DXY/ctx 47,
                           # IDR/ctx 48) — 40 clears all of them with margin for a
                           # worse week (e.g. an Eid al-Fitr closure landing inside
                           # the window) while still requiring ~83% of the 48-weekday
                           # ceiling.
N_CLUSTERS_TARGET  = 10
MAX_PAIRS_RAM_WARN = 40_000  # ~283 symbols — informational only, Ledoit-Wolf does not
                              # share old correlation_matrix.py's RAM/instability
                              # concern at this symbol count; this just flags Parquet
                              # row-count growth for awareness.


class CorrelationModule:
    """Architecture v2.0 §6.2 — Ledoit-Wolf shrinkage correlation +
    hierarchical clustering over the merged Layer 1 + Layer 2 universe."""

    def compute(self, run_date: date) -> Optional[pl.DataFrame]:
        symbols = self._resolve_merged_universe(run_date)
        if symbols is None:
            return None

        returns = self._load_returns(symbols, run_date)
        if returns is None or returns.is_empty():
            logger.warning(
                f"[gold_cross_asset_correlation] No return data in window ending {run_date}"
            )
            return None

        pivot, valid_symbols = self._pivot_returns(returns)
        if len(valid_symbols) < 2:
            logger.warning(
                f"[gold_cross_asset_correlation] Only {len(valid_symbols)} symbols"
                " have enough history — need at least 2"
            )
            return None

        corr_matrix = self._ledoit_wolf_correlation(pivot, valid_symbols)
        cluster_ids = self._cluster(corr_matrix, valid_symbols)
        regime = self._get_current_regime(run_date)

        n_pairs = len(valid_symbols) * (len(valid_symbols) - 1) // 2
        if n_pairs > MAX_PAIRS_RAM_WARN:
            logger.warning(
                f"[gold_cross_asset_correlation] {len(valid_symbols)} symbols -> "
                f"{n_pairs:,} pairs — above the {MAX_PAIRS_RAM_WARN:,}-pair informational "
                "threshold. Ledoit-Wolf itself remains numerically stable at this size; "
                "this is a Parquet-row-count note, not a correctness warning."
            )

        return self._to_long_format(corr_matrix, valid_symbols, cluster_ids, regime, run_date)

    # ── Universe resolution ───────────────────────────────────────────────

    @staticmethod
    def _resolve_merged_universe(run_date: date) -> Optional[list[str]]:
        try:
            layer1 = ActiveSymbolsResolver().load_ohlcv(run_date)
        except FileNotFoundError as e:
            logger.warning(f"[gold_cross_asset_correlation] {e} — skipping")
            return None
        try:
            layer2 = ContextAnchorsResolver().load(run_date)
        except FileNotFoundError as e:
            logger.warning(f"[gold_cross_asset_correlation] {e} — skipping")
            return None
        merged = sorted(set(layer1) | set(layer2))
        logger.info(
            f"[gold_cross_asset_correlation] Universe: {len(layer1)} Layer 1 + "
            f"{len(layer2)} Layer 2 = {len(merged)} merged (after de-dup)"
        )
        return merged

    # ── Silver read ───────────────────────────────────────────────────────

    @staticmethod
    def _load_returns(symbols: list[str], run_date: date) -> Optional[pl.DataFrame]:
        globs = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        ctx_glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        if ctx_glob is not None:
            globs = globs + [ctx_glob]
        if not globs:
            logger.warning("[gold_cross_asset_correlation] No Silver 1D data found — skipping")
            return None

        start = run_date - timedelta(days=LOOKBACK_DAYS)
        con = duckdb.connect()
        con.execute("SET memory_limit='3GB'; SET threads=4;")
        active_df = pl.DataFrame({"symbol": symbols})
        con.register("active_universe_tbl", active_df.to_arrow())
        try:
            return con.execute(
                """
                SELECT
                    symbol,
                    CAST(timestamp AS DATE) AS date,
                    log_return
                FROM read_parquet($globs, hive_partitioning=true)
                WHERE CAST(timestamp AS DATE) >= $start
                  AND CAST(timestamp AS DATE) <= $run_date
                  AND symbol IN (SELECT symbol FROM active_universe_tbl)
                  AND log_return IS NOT NULL
                  AND is_clean = TRUE
                ORDER BY date, symbol
                """,
                {"globs": globs, "start": start, "run_date": run_date},
            ).pl()
        except Exception as e:
            logger.error(f"[gold_cross_asset_correlation] Silver read failed: {e}")
            return None

    @staticmethod
    def _pivot_returns(returns: pl.DataFrame) -> tuple[pl.DataFrame, list[str]]:
        # FIX XAE-CAL-01: observation-count native — see MIN_OBSERVATIONS above.
        sym_counts = (
            returns.group_by("symbol").agg(pl.len().alias("n_obs"))
            .filter(pl.col("n_obs") >= MIN_OBSERVATIONS)
        )
        valid_symbols = sorted(sym_counts["symbol"].to_list())
        if len(valid_symbols) < 2:
            return pl.DataFrame(), valid_symbols

        pivot = (
            returns
            .filter(pl.col("symbol").is_in(valid_symbols))
            .pivot(values="log_return", index="date", on="symbol", aggregate_function="first")
            .sort("date")
        )
        # Re-derive valid_symbols from actual pivot columns (pivot can still
        # produce all-null columns for a symbol with sparse-but-sufficient
        # row count if its dates don't align with the common date index).
        valid_symbols = [c for c in pivot.columns if c != "date"]
        return pivot, valid_symbols

    # ── Ledoit-Wolf correlation ───────────────────────────────────────────

    @staticmethod
    def _ledoit_wolf_correlation(pivot: pl.DataFrame, symbols: list[str]) -> np.ndarray:
        from sklearn.covariance import LedoitWolf

        # Column-wise fill: a symbol missing a handful of days (holiday,
        # not a real gap — GAP-PEER-01's own reasoning applies) gets its
        # own mean substituted rather than dropping the whole date row,
        # which would shrink the common window for EVERY symbol just
        # because one had a market-specific closure.
        data = pivot.select(symbols).to_numpy().copy()
        col_means = np.nanmean(data, axis=0)
        inds = np.where(np.isnan(data))
        data[inds] = np.take(col_means, inds[1])
        # Any symbol with zero non-null observations (all-NaN column,
        # col_mean itself NaN) becomes a zero column post-fill — harmless
        # for LedoitWolf (constant column -> zero variance -> zero
        # correlation with everything), rather than propagating NaN.
        data = np.nan_to_num(data, nan=0.0)

        lw = LedoitWolf().fit(data)
        cov = lw.covariance_
        std = np.sqrt(np.clip(np.diag(cov), a_min=1e-12, a_max=None))
        corr = cov / np.outer(std, std)
        corr = np.clip(corr, -1.0, 1.0)
        np.fill_diagonal(corr, 1.0)
        return corr

    # ── Clustering ────────────────────────────────────────────────────────

    @staticmethod
    def _cluster(corr_matrix: np.ndarray, symbols: list[str]) -> dict[str, int]:
        from scipy.cluster.hierarchy import fcluster, linkage
        from scipy.spatial.distance import squareform

        n = len(symbols)
        if n < 2:
            return {s: 0 for s in symbols}

        dist = 1.0 - np.abs(corr_matrix)
        np.fill_diagonal(dist, 0.0)
        # Symmetrize defensively — floating-point clipping upstream can
        # leave sub-1e-10 asymmetry that squareform() rejects outright.
        dist = (dist + dist.T) / 2.0

        condensed = squareform(dist, checks=False)
        n_clusters = min(N_CLUSTERS_TARGET, max(2, n // 2))
        try:
            z = linkage(condensed, method="average")
            labels = fcluster(z, t=n_clusters, criterion="maxclust")
        except Exception as e:
            logger.warning(
                f"[gold_cross_asset_correlation] Clustering failed ({e})"
                " — assigning all symbols to cluster 0"
            )
            labels = np.zeros(n, dtype=int)

        return {sym: int(label) for sym, label in zip(symbols, labels)}

    # ── Regime tagging ────────────────────────────────────────────────────

    @staticmethod
    def _get_current_regime(run_date: date) -> str:
        if not REGIME_STORE_PATH.exists():
            return "UNKNOWN"
        try:
            df = pl.read_parquet(REGIME_STORE_PATH)
            prior = df.filter(pl.col("date") <= str(run_date)).sort("date", descending=True)
            if prior.is_empty():
                return "UNKNOWN"
            return prior.row(0, named=True)["regime"]
        except Exception as e:
            logger.debug(f"[gold_cross_asset_correlation] Could not read regime: {e}")
            return "UNKNOWN"

    # ── Output assembly ───────────────────────────────────────────────────

    @staticmethod
    def _to_long_format(
        corr_matrix: np.ndarray,
        symbols: list[str],
        cluster_ids: dict[str, int],
        regime: str,
        run_date: date,
    ) -> pl.DataFrame:
        n = len(symbols)
        rows_a, rows_b, rows_corr, rows_ca, rows_cb = [], [], [], [], []
        for i in range(n):
            for j in range(i + 1, n):
                rows_a.append(symbols[i])
                rows_b.append(symbols[j])
                rows_corr.append(float(corr_matrix[i, j]))
                rows_ca.append(cluster_ids[symbols[i]])
                rows_cb.append(cluster_ids[symbols[j]])
        return pl.DataFrame({
            "symbol_a":        rows_a,
            "symbol_b":        rows_b,
            "correlation":     rows_corr,
            "regime":          [regime] * len(rows_a),
            "computation_date": [str(run_date)] * len(rows_a),
            "cluster_id_a":    rows_ca,
            "cluster_id_b":    rows_cb,
        })


def run(run_date: date) -> None:
    """Job entry point for gold_cross_asset_correlation (weekly Sunday)."""
    logger.info(f"[gold_cross_asset_correlation] Starting | run_date={run_date}")

    module = CorrelationModule()
    result = module.compute(run_date)
    if result is None or result.is_empty():
        logger.warning("[gold_cross_asset_correlation] Nothing written (no data)")
        return

    GOLD_CROSS_ASSET_PATH.mkdir(parents=True, exist_ok=True)
    atomic_write_parquet(
        result,
        CROSS_ASSET_CORR_PATH,
        compression="zstd",
        compression_level=3,
    )
    n_symbols = len(set(result["symbol_a"]) | set(result["symbol_b"]))
    logger.info(
        f"[gold_cross_asset_correlation] {n_symbols} symbols, {result.height:,} pairs, "
        f"regime={result['regime'][0]} -> {CROSS_ASSET_CORR_PATH.name}"
    )
