"""
lead_lag_module.py — Architecture v2.0 §6.3 (CrossAssetEngine —
LeadLagModule), updated by Architecture Extension v1.0 ADR-001: BH-FDR
threshold is q=0.03 (NOT the original Architecture v2.0 doc's q=0.10 —
ADR-001 revised this after the Layer 2 universe grew from ~20 to ~58/61
anchors, raising the total test count from ~19,000 to ~57,950 and making
the looser q=0.10 threshold produce thousands of non-actionable
"significant" pairs). GMI Wave 1 Cycle 4 — third module (after
GlobalIndexRegimeModule, CorrelationModule).

Design (Architecture v2.0 §6.3.1 + Architecture Extension §6.2 / ADR-001):
    leaders   = InstrumentLoader.correlation_context(), MINUS any instrument
                with exclude_from_lead_lag_leader=True (currently SSEC only
                — Architecture v2.0 §3.5: circuit-breaker outliers would
                otherwise produce spurious Granger "significance").
    followers = ActiveSymbolsResolver.load_ohlcv(run_date) (Layer 1).
    Test: Granger causality (statsmodels grangercausalitytests), testing
          "leader Granger-causes follower" at lag 1..MAX_LAG (5).
    Correction: Benjamini-Hochberg FDR, applied GLOBALLY across every
          (leader, follower, lag) raw p-value in one run — not per-pair,
          not per-lag in isolation. This is a specific, deliberate
          reading of ADR-001's own "Total tests: 57.950 = anchors ×
          equities × lags" framing: the lag dimension IS part of the
          multiple-testing universe, not something resolved before
          correction. See _run() for where this global collection
          happens.
    optimal_lag: for each (leader, follower) pair, the lag (1..5) with
          the smallest RAW p-value among the up-to-5 tested — the BH-
          adjusted p-value and R² reported for the pair are this same
          lag's values, looked up post-correction. This resolves an
          ambiguity in Architecture v2.0 §6.3.2's schema, which has one
          "optimal_lag" field per pair-row despite testing 5 lags per
          pair — stated explicitly here rather than silently picked.
    max_cross_corr: the maximum |Pearson correlation| between the leader
          series (shifted back by k days) and the follower series,
          across k=1..MAX_LAG — a magnitude sanity-check independent of
          Granger significance. Architecture Extension §6.2's "actionable
          filter" (>= 0.15) is exposed as a column here, NOT used to drop
          rows — every tested pair is written, so a consumer can apply
          its own combination of bh_significant AND max_cross_corr rather
          than have rows silently disappear from the store.

Statsmodels version compatibility (real, not hypothetical — see
_run_granger_quiet()): production's poetry.lock pins statsmodels==0.14.6,
where grangercausalitytests(..., verbose=False) still PRINTS the full
per-lag test summary to stdout unless stdout is separately redirected,
and emits a FutureWarning regardless. Newer statsmodels (0.15+) removed
the verbose parameter entirely. At ~11,000 (leader, follower) pairs per
weekly run this is not a cosmetic concern — both cases are handled.

Output: data/gold/cross_asset/lead_lag_matrix.parquet
Schema (Architecture v2.0 §6.3.2): leader, follower, optimal_lag,
    p_value_raw, p_value_adjusted, r_squared, regime, computation_date,
    bh_significant, max_cross_corr, optimal_lag_days (== optimal_lag,
    both names kept: Architecture v2.0's own table and its §6.3.2 code
    block schema disagree on the column name — "optimal_lag" in the
    table, "optimal_lag_days" in the described output columns of the
    Architecture Extension addendum — kept both rather than guessing
    which consumer expects which).

Job registry: 'gold_lead_lag', depends_on=['silver_active_symbols',
'silver_context_anchors', 'gold_cross_asset_correlation'] (Architecture
v2.0 §6.6 pipeline table lists gold_correlation as a dependency — pointed
here at the new gold_cross_asset_correlation job instead, consistent with
this module living entirely in gold/cross_asset/ alongside it; the
dependency is soft in the sense that this module does not actually read
cross_asset_corr.parquet's contents, only enforces the documented run
order). NOT yet wired into gold_screener — out of scope for this pass.
"""

from __future__ import annotations

import contextlib
import io
import warnings
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import polars as pl
from loguru import logger

from src.config.instrument_loader import get_loader
from src.silver.active_symbols import ActiveSymbolsResolver
from src.silver.context_anchors import ContextAnchorsResolver
from src.utils.atomic_io import atomic_write_parquet
from src.utils.silver_scope import context_glob, layer1_globs

SILVER_OHLCV_PATH     = Path("data/silver/market_ohlcv")
REGIME_STORE_PATH     = Path("data/gold/macro/regime_store.parquet")
GOLD_CROSS_ASSET_PATH = Path("data/gold/cross_asset")
LEAD_LAG_STORE_PATH   = GOLD_CROSS_ASSET_PATH / "lead_lag_matrix.parquet"

LOOKBACK_DAYS     = 65    # Calendar days — bounds the Silver scan window only.
                          # Matches CorrelationModule's window.
MIN_OBSERVATIONS  = 40    # FIX XAE-CAL-01 (16 Sep 2026): observation-count native,
                          # calendar-aware — replaces the old MIN_HISTORY_RATIO=0.8
                          # (int(65*0.8)=52), which was unreachable for any 5-day-week
                          # market (max 48 weekdays in a 65-day window, empirically
                          # verified against live Silver data, 16 Sep 2026 session).
                          # See correlation_module.py's MIN_OBSERVATIONS comment for
                          # the full rationale and calibration data — both modules
                          # share this threshold by convention, not by import, per
                          # each CrossAssetEngine module's independence (Architecture
                          # v2.0 §6.1).
MAX_LAG           = 5     # Architecture v2.0 §6.3.1
BH_FDR_Q          = 0.03  # ADR-001 (Architecture Extension v1.0 §6.2), supersedes v2.0's q=0.10
MIN_PAIR_OBS      = MAX_LAG + 10  # Minimum overlapping obs to attempt Granger at all


class LeadLagModule:
    """Architecture v2.0 §6.3 — Granger causality lead-lag detection,
    Layer 2 anchors (leaders) -> Layer 1 equities (followers), globally
    BH-FDR corrected at q=0.03 (ADR-001)."""

    def compute(self, run_date: date) -> Optional[pl.DataFrame]:
        leaders = self._resolve_leaders()
        if not leaders:
            logger.warning(
                "[gold_lead_lag] No eligible leader anchors "
                "(correlation_context() empty or all exclude_from_lead_lag_leader)"
            )
            return None

        followers = self._resolve_followers(run_date)
        if followers is None:
            return None

        returns = self._load_returns(sorted(set(leaders) | set(followers)), run_date)
        if returns is None or returns.is_empty():
            logger.warning(f"[gold_lead_lag] No return data in window ending {run_date}")
            return None

        pivot, valid = self._pivot_returns(returns)
        valid_leaders   = [s for s in leaders if s in valid]
        valid_followers = [s for s in followers if s in valid]
        if not valid_leaders or not valid_followers:
            logger.warning(
                "[gold_lead_lag] No leader/follower with sufficient history "
                f"(valid_leaders={len(valid_leaders)}, valid_followers={len(valid_followers)})"
            )
            return None

        raw_tests, pair_cross_corr = self._run_all_pairs(pivot, valid_leaders, valid_followers)
        if not raw_tests:
            logger.warning("[gold_lead_lag] No (leader, follower) pair had enough overlap to test")
            return None

        adjusted = self._apply_global_bh_correction(raw_tests)
        result   = self._select_optimal_per_pair(adjusted, pair_cross_corr)

        regime = self._get_current_regime(run_date)
        result = result.with_columns([
            pl.lit(regime).alias("regime"),
            pl.lit(str(run_date)).alias("computation_date"),
        ])
        return result

    # ── Universe resolution ───────────────────────────────────────────────

    @staticmethod
    def _resolve_leaders() -> list[str]:
        return [
            inst.symbol for inst in get_loader().correlation_context()
            if not inst.exclude_from_lead_lag_leader
        ]

    @staticmethod
    def _resolve_followers(run_date: date) -> Optional[list[str]]:
        try:
            return ActiveSymbolsResolver().load_ohlcv(run_date)
        except FileNotFoundError as e:
            logger.warning(f"[gold_lead_lag] {e} — skipping")
            return None

    # ── Silver read (mirrors correlation_module.py's own query shape) ──────

    @staticmethod
    def _load_returns(symbols: list[str], run_date: date) -> Optional[pl.DataFrame]:
        globs = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        ctx_glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        if ctx_glob is not None:
            globs = globs + [ctx_glob]
        if not globs:
            logger.warning("[gold_lead_lag] No Silver 1D data found — skipping")
            return None

        start = run_date - timedelta(days=LOOKBACK_DAYS)
        con = duckdb.connect()
        con.execute("SET memory_limit='3GB'; SET threads=4;")
        universe_df = pl.DataFrame({"symbol": symbols})
        con.register("lead_lag_universe_tbl", universe_df.to_arrow())
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
                  AND symbol IN (SELECT symbol FROM lead_lag_universe_tbl)
                  AND log_return IS NOT NULL
                  AND is_clean = TRUE
                ORDER BY date, symbol
                """,
                {"globs": globs, "start": start, "run_date": run_date},
            ).pl()
        except Exception as e:
            logger.error(f"[gold_lead_lag] Silver read failed: {e}")
            return None

    @staticmethod
    def _pivot_returns(returns: pl.DataFrame) -> tuple[pl.DataFrame, set[str]]:
        # FIX XAE-CAL-01: observation-count native — see MIN_OBSERVATIONS above.
        sym_counts = (
            returns.group_by("symbol").agg(pl.len().alias("n_obs"))
            .filter(pl.col("n_obs") >= MIN_OBSERVATIONS)
        )
        candidate_symbols = sym_counts["symbol"].to_list()
        if len(candidate_symbols) < 2:
            return pl.DataFrame(), set()

        pivot = (
            returns
            .filter(pl.col("symbol").is_in(candidate_symbols))
            .pivot(values="log_return", index="date", on="symbol", aggregate_function="first")
            .sort("date")
        )
        valid = {c for c in pivot.columns if c != "date"}
        return pivot, valid

    # ── Per-pair Granger + cross-correlation ─────────────────────────────

    def _run_all_pairs(
        self, pivot: pl.DataFrame, leaders: list[str], followers: list[str]
    ) -> tuple[list[dict], dict[tuple[str, str], float]]:
        raw_tests: list[dict] = []
        cross_corr: dict[tuple[str, str], float] = {}

        for leader in leaders:
            leader_full = pivot[leader].to_numpy()
            for follower in followers:
                if follower == leader:
                    continue
                pair_df = pivot.select(["date", follower, leader]).drop_nulls()
                if pair_df.height < MIN_PAIR_OBS:
                    continue

                data = pair_df.select([follower, leader]).to_numpy()
                lag_results = self._run_granger_quiet_per_lag(data, MAX_LAG)
                if not lag_results:
                    continue
                for lag, (p_raw, r_squared) in lag_results.items():
                    raw_tests.append({
                        "leader": leader, "follower": follower,
                        "lag": lag, "p_raw": p_raw, "r_squared": r_squared,
                    })

                follower_aligned = pair_df[follower].to_numpy()
                leader_aligned   = pair_df[leader].to_numpy()
                cross_corr[(leader, follower)] = self._max_abs_lagged_corr(
                    leader_aligned, follower_aligned, MAX_LAG
                )

        return raw_tests, cross_corr

    @staticmethod
    def _run_granger_quiet_per_lag(data: np.ndarray, maxlag: int) -> dict[int, tuple[float, float]]:
        """Return {lag: (p_value_raw, r_squared)} for lag in 1..maxlag,
        using the ssr-F-test p-value (standard Granger test statistic)
        and the unrestricted model's R². See module docstring for the
        statsmodels version-compatibility handling."""
        from statsmodels.tsa.stattools import grangercausalitytests

        buf = io.StringIO()
        try:
            with warnings.catch_warnings(), contextlib.redirect_stdout(buf):
                warnings.simplefilter("ignore")
                try:
                    res = grangercausalitytests(data, maxlag=maxlag, verbose=False)
                except TypeError:
                    res = grangercausalitytests(data, maxlag=maxlag)
        except Exception as e:
            logger.debug(f"[gold_lead_lag] Granger test failed for a pair: {e}")
            return {}

        out: dict[int, tuple[float, float]] = {}
        for lag, (test_dict, models) in res.items():
            try:
                _, p_raw, _, _ = test_dict["ssr_ftest"]
                r_squared = models[1].rsquared
                out[lag] = (float(p_raw), float(r_squared))
            except Exception:
                continue
        return out

    @staticmethod
    def _max_abs_lagged_corr(leader: np.ndarray, follower: np.ndarray, max_lag: int) -> float:
        """Max |Pearson correlation| between leader[:-k] and follower[k:]
        for k=1..max_lag — leader's value k days ago vs follower today."""
        best = 0.0
        n = len(leader)
        for k in range(1, max_lag + 1):
            if n - k < 3:
                continue
            a = leader[: n - k]
            b = follower[k:]
            if np.std(a) < 1e-12 or np.std(b) < 1e-12:
                continue
            c = float(np.corrcoef(a, b)[0, 1])
            if np.isfinite(c):
                best = max(best, abs(c))
        return best

    # ── Global BH-FDR correction ──────────────────────────────────────────

    @staticmethod
    def _apply_global_bh_correction(raw_tests: list[dict]) -> list[dict]:
        from statsmodels.stats.multitest import multipletests

        p_raw = [t["p_raw"] for t in raw_tests]
        _, p_adj, _, _ = multipletests(p_raw, alpha=BH_FDR_Q, method="fdr_bh")
        for t, p in zip(raw_tests, p_adj):
            t["p_adjusted"] = float(p)
        return raw_tests

    @staticmethod
    def _select_optimal_per_pair(
        adjusted: list[dict], pair_cross_corr: dict[tuple[str, str], float]
    ) -> pl.DataFrame:
        """Per (leader, follower) pair, keep the lag with the smallest
        RAW p-value (strongest evidence) — see module docstring."""
        best: dict[tuple[str, str], dict] = {}
        for t in adjusted:
            key = (t["leader"], t["follower"])
            if key not in best or t["p_raw"] < best[key]["p_raw"]:
                best[key] = t

        rows = []
        for (leader, follower), t in best.items():
            max_cc = pair_cross_corr.get((leader, follower), 0.0)
            rows.append({
                "leader":            leader,
                "follower":          follower,
                "optimal_lag":       t["lag"],
                "optimal_lag_days":  t["lag"],
                "p_value_raw":       round(t["p_raw"], 8),
                "p_value_adjusted":  round(t["p_adjusted"], 8),
                "r_squared":         round(t["r_squared"], 6),
                "bh_significant":    t["p_adjusted"] < BH_FDR_Q,
                "max_cross_corr":    round(max_cc, 6),
            })
        return pl.DataFrame(rows)

    # ── Regime tagging (same convention as correlation_module.py) ─────────

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
            logger.debug(f"[gold_lead_lag] Could not read regime: {e}")
            return "UNKNOWN"


def run(run_date: date) -> None:
    """Job entry point for gold_lead_lag (weekly Sunday)."""
    logger.info(f"[gold_lead_lag] Starting | run_date={run_date}")

    module = LeadLagModule()
    result = module.compute(run_date)
    if result is None or result.is_empty():
        logger.warning("[gold_lead_lag] Nothing written (no data)")
        return

    GOLD_CROSS_ASSET_PATH.mkdir(parents=True, exist_ok=True)
    atomic_write_parquet(
        result,
        LEAD_LAG_STORE_PATH,
        compression="zstd",
        compression_level=3,
    )
    n_sig = int(result["bh_significant"].sum())
    logger.info(
        f"[gold_lead_lag] {result.height:,} pairs tested, {n_sig:,} bh_significant "
        f"(q={BH_FDR_Q}) -> {LEAD_LAG_STORE_PATH.name}"
    )
