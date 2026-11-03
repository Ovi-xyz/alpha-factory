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
    XAE-R08 additive columns: bh_significant_raw, p_value_calibrated, p_value_calibrated_adjusted,
    p_calibration, is_tautology, n_tests, n_obs, estimator, n_dropped_bars, n_null_draws, corr_null_p95,
    actionable, lag_basis.

XAE-R08 (cross_asset_recalibration_guide L1-L7, section 1.7) — what changed and what did NOT:

  Evidence (16 Oct 2026 store): 10,920 pairs (56 leaders x 195 followers; 54,600 pair-lag tests — the
  "57,950" in ADR-001 is the earlier universe, 61 anchors x 190 equities x 5 lags). BH-significant: 5 pairs,
  all DXY -> FX majors (a basket identity); 0 of 9,912 non-FX pairs. The ADR validation pairs fail (TWSE ->
  NVDA p_adj 0.94, AXJO -> AU 0.99, EIDO -> BBCA 0.96, ...). With ~44 observations a BH threshold near 1e-5
  needs |r| ~ 0.6, so only identities can survive. The "actionable" max_cross_corr >= 0.15 filter passed 89.5%
  of pairs. The Granger test is mis-sized even on a placebo (12.3% rejections at nominal 5%, 2.8% at 1%); no
  estimator fixes size.

  Changes (processing label: estimator "winsorised_mad3", additive columns):
    1. ESTIMATOR: each symbol's window returns are winsorised at +/-3 robust sigma (median/MAD, within the
       window; robust.py) before any test. The study found this keeps the clean-only estimator's out-of-sample
       persistence and is safe on crash bars. It is applied to bars that already passed `is_clean = TRUE`;
       it matters most once the Silver flag split (XAE-R06, NOT yet landed) releases the extreme bars.
    2. PLACEBO CALIBRATION: for every non-tautology pair the leader is circularly shifted (PLACEBO_SHIFTS time(s))
       and the same 5-lag Granger test is run; the pooled placebo minimum-p's are the null against which each
       pair's minimum p is calibrated (robust.calibrated_p, power-law tail below the null's smallest value).
       `bh_significant` is BH at q = 0.03 over those calibrated p's (the family is the pairs). The legacy
       raw-p BH over pair-lag tests is kept as `bh_significant_raw` / `p_value_adjusted`. BH q is NOT loosened
       (the guide: not before the power problem is fixed). If the pooled null has fewer than MIN_NULL_DRAWS
       draws (a tiny universe) calibration is skipped, `p_calibration` = "none", and `bh_significant` is the
       legacy raw-p BH — never a calibration from a handful of draws.
    3. TAUTOLOGIES: pairs listed in config/cross_asset_relations.yaml (DXY -> its basket members) get
       `is_tautology = true`, sit outside both families and are never significant/actionable.
    4. ACTIONABLE: the fixed 0.15 cut is replaced by the n-aware null quantile — `corr_null_p95` is the 95th
       percentile of max_{k=1..5}|corr| for independent series of THIS pair's length (0.40 at n = 44);
       `actionable` = bh_significant AND max_cross_corr >= corr_null_p95 AND NOT is_tautology. gold_screener's
       lead_lag_signal uses `actionable` when the column exists.
    5. REPORTING: n_tests (size of the family behind bh_significant), n_obs (aligned observations of the pair),
       estimator, n_dropped_bars (is_clean = FALSE bars of the pair's two symbols in the window, i.e. what the
       clean-only read discarded), n_null_draws, lag_basis.

  NOT DONE (needs live data or a decision): information-set lags — `lag_basis` is always "date_index"; the
  Asian-close -> US same-date channel the guide found (L7) is outside lags 1-5 and stays untested until the
  decision-timestamp question (D1) and a PIT accessor (proposed GMI-PIT-003) exist. 250-500 observation
  windows (conflicts with regime conditioning; the 65-day window stays). Leader -> follower-group tests. The
  out-of-sample persistence acceptance and the pre-registered-pairs report (TWSE -> US tech, AXJO -> AU/AG,
  EIDO -> IDX30, COAL_NEWC -> ADRO/PTBA/ITMG) need live Silver — run them before trusting `actionable`.

  COST: one Granger run (5 lags) is ~13 ms single-core, so the unmodified module is ~2.4 min per weekly run
  and each placebo shift adds the same again (PLACEBO_SHIFTS = 1 -> ~5 min). The guide's "about a minute
  with a placebo" estimate [I] is optimistic by roughly 5x.

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
import yaml
from loguru import logger

from src.config.instrument_loader import get_loader
from src.silver.active_symbols import ActiveSymbolsResolver
from src.silver.context_anchors import ContextAnchorsResolver
from src.utils.atomic_io import atomic_write_parquet
from src.gold.cross_asset.regime_tag import current_regime_tag
from src.gold.cross_asset.robust import calibrated_p, null_corr_quantile, winsorise_columns
from src.gold.cross_asset.history import LEAD_LAG_HISTORY_NAME, write_history_snapshot
from src.utils.silver_scope import context_glob, layer1_globs

SILVER_OHLCV_PATH     = Path("data/silver/market_ohlcv")
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

# XAE-R08 — estimator / calibration parameters. WINSOR_K = 3 and a single placebo shift follow the
# guide's estimator study; MIN_NULL_DRAWS and the 95% actionable quantile are untuned starting values [U].
ESTIMATOR                = "winsorised_mad3"
WINSOR_K                 = 3.0
PLACEBO_SHIFTS           = 1      # placebo runs per pair; each adds one more Granger pass (~2.4 min total)
PLACEBO_BASE_SHIFT       = 23     # the study's shift; later shifts step by 11, always wrapped into range
MIN_NULL_DRAWS           = 500    # below this the pooled null is too thin to calibrate against
ACTIONABLE_NULL_QUANTILE = 0.95
LAG_BASIS                = "date_index"   # "info_set" lags need the PIT accessor of decision D1 (not built)
RELATIONS_CONFIG_PATH    = Path("config/cross_asset_relations.yaml")


def load_tautologies(path: Optional[Path] = None) -> set[tuple[str, str]]:
    """{(leader, follower)} pairs that are accounting identities (config/cross_asset_relations.yaml).
    A missing or unreadable file yields an EMPTY set with a WARNING — the module then behaves as before
    for those pairs rather than refusing to run, but it is never silent."""
    path = Path(path) if path is not None else RELATIONS_CONFIG_PATH
    try:
        cfg = yaml.safe_load(path.read_text()) or {}
    except Exception as e:
        logger.warning(f"[gold_lead_lag] cannot read {path} ({e}) — NO pairs are marked tautological")
        return set()
    out: set[tuple[str, str]] = set()
    for entry in cfg.get("tautologies") or []:
        leader = str(entry["leader"])
        for follower in entry.get("followers") or []:
            out.add((leader, str(follower)))
    return out


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

        # XAE-R08: robust estimator — winsorise every series within the window, before any test.
        pivot = winsorise_columns(pivot, sorted(valid), WINSOR_K)

        tautologies = load_tautologies()
        raw_tests, pair_cross_corr, pair_info = self._run_all_pairs(
            pivot, valid_leaders, valid_followers, tautologies
        )
        if not raw_tests:
            logger.warning("[gold_lead_lag] No (leader, follower) pair had enough overlap to test")
            return None

        dropped = self._load_dropped_counts(
            sorted(set(valid_leaders) | set(valid_followers)), run_date
        )
        result = self._finalize(raw_tests, pair_cross_corr, pair_info, tautologies, dropped)

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
        self, pivot: pl.DataFrame, leaders: list[str], followers: list[str],
        tautologies: Optional[set[tuple[str, str]]] = None,
    ) -> tuple[list[dict], dict[tuple[str, str], float], dict[tuple[str, str], dict]]:
        """Per-pair Granger (lags 1..MAX_LAG), cross-correlation and — for non-tautology pairs — the placebo
        null draw(s). Returns (raw_tests, cross_corr, pair_info) where pair_info[(leader, follower)] holds
        {"n_obs": int, "null_p": [min-p over lags of each placebo run]}."""
        tautologies = tautologies or set()
        raw_tests: list[dict] = []
        cross_corr: dict[tuple[str, str], float] = {}
        pair_info: dict[tuple[str, str], dict] = {}

        for leader in leaders:
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
                is_taut = (leader, follower) in tautologies
                for lag, (p_raw, r_squared) in lag_results.items():
                    raw_tests.append({
                        "leader": leader, "follower": follower,
                        "lag": lag, "p_raw": p_raw, "r_squared": r_squared,
                        "is_tautology": is_taut,
                    })

                follower_aligned = pair_df[follower].to_numpy()
                leader_aligned   = pair_df[leader].to_numpy()
                cross_corr[(leader, follower)] = self._max_abs_lagged_corr(
                    leader_aligned, follower_aligned, MAX_LAG
                )
                null_p: list[float] = []
                if not is_taut:
                    null_p = self._placebo_min_p(follower_aligned, leader_aligned)
                pair_info[(leader, follower)] = {"n_obs": int(pair_df.height), "null_p": null_p}

        return raw_tests, cross_corr, pair_info

    def _placebo_min_p(self, follower: np.ndarray, leader: np.ndarray) -> list[float]:
        """Minimum Granger p over lags 1..MAX_LAG with the leader circularly shifted by a lag-safe amount
        — the lead-lag relation is destroyed, each series keeps its own autocorrelation and fat tails
        (XAE-R08, guide section 1.7 placebo). One value per shift; shifts that cannot be placed at least
        MAX_LAG+1 positions from alignment are skipped."""
        n = len(leader)
        lo, hi = MAX_LAG + 1, n - MAX_LAG - 1
        if hi < lo:
            return []
        out: list[float] = []
        for i in range(PLACEBO_SHIFTS):
            shift = lo + ((PLACEBO_BASE_SHIFT + 11 * i) % (hi - lo + 1))
            res = self._run_granger_quiet_per_lag(
                np.column_stack([follower, np.roll(leader, shift)]), MAX_LAG
            )
            if res:
                out.append(min(p for p, _ in res.values()))
        return out

    @staticmethod
    def _load_dropped_counts(symbols: list[str], run_date: date) -> dict[str, int]:
        """is_clean = FALSE bars per symbol inside the lookback window — what the clean-only read dropped
        (XAE-R08 `n_dropped_bars`). Informational: a failure returns {} (counts report 0), logged."""
        globs = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        ctx_glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        if ctx_glob is not None:
            globs = globs + [ctx_glob]
        if not globs:
            return {}
        start = run_date - timedelta(days=LOOKBACK_DAYS)
        con = duckdb.connect()
        con.execute("SET memory_limit='3GB'; SET threads=4;")
        # NOTE (review): the two CAST(timestamp AS DATE) below follow _load_returns, which defines the window the
        # estimator actually uses, so the dropped-bar count covers the SAME window. They are session-timezone
        # dependent (RISK-37 D1: cross-market day alignment is a pending methodology decision, which is why this
        # file is on test_trade_date.PENDING_DECISION_D1). Convert BOTH queries together in the D1 change.
        con.register("lead_lag_dropped_universe_tbl", pl.DataFrame({"symbol": symbols}).to_arrow())
        try:
            rows = con.execute(
                """
                SELECT symbol, COUNT(*) AS n
                FROM read_parquet($globs, hive_partitioning=true)
                WHERE CAST(timestamp AS DATE) >= $start
                  AND CAST(timestamp AS DATE) <= $run_date
                  AND symbol IN (SELECT symbol FROM lead_lag_dropped_universe_tbl)
                  AND is_clean = FALSE
                GROUP BY symbol
                """,
                {"globs": globs, "start": start, "run_date": run_date},
            ).fetchall()
            return {r[0]: int(r[1]) for r in rows}
        except Exception as e:
            logger.warning(f"[gold_lead_lag] could not count dropped bars ({e}) — n_dropped_bars reports 0")
            return {}

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
        """Legacy raw-p BH over every (leader, follower, lag) test. Tests flagged is_tautology are NOT in
        the family (XAE-R08): their p_adjusted is None."""
        from statsmodels.stats.multitest import multipletests

        family = [t for t in raw_tests if not t.get("is_tautology")]
        if family:
            _, p_adj, _, _ = multipletests([t["p_raw"] for t in family], alpha=BH_FDR_Q, method="fdr_bh")
            for t, p in zip(family, p_adj):
                t["p_adjusted"] = float(p)
        for t in raw_tests:
            t.setdefault("p_adjusted", None)
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
            p_adj = t.get("p_adjusted")
            rows.append({
                "leader":            leader,
                "follower":          follower,
                "optimal_lag":       t["lag"],
                "optimal_lag_days":  t["lag"],
                "p_value_raw":       round(t["p_raw"], 8),
                "_p_exact":          float(t["p_raw"]),   # internal: calibration must see the unrounded p
                "p_value_adjusted":  None if p_adj is None else round(p_adj, 8),
                "r_squared":         round(t["r_squared"], 6),
                "bh_significant":    p_adj is not None and p_adj < BH_FDR_Q,
                "max_cross_corr":    round(max_cc, 6),
            })
        return pl.DataFrame(rows, schema_overrides={"p_value_adjusted": pl.Float64})

    def _finalize(
        self,
        raw_tests: list[dict],
        pair_cross_corr: dict[tuple[str, str], float],
        pair_info: dict[tuple[str, str], dict],
        tautologies: set[tuple[str, str]],
        dropped: dict[str, int],
    ) -> pl.DataFrame:
        """Legacy BH, placebo calibration, tautology/actionable flags and the reporting columns."""
        from statsmodels.stats.multitest import multipletests

        adjusted = self._apply_global_bh_correction(raw_tests)
        base = self._select_optimal_per_pair(adjusted, pair_cross_corr)
        n_family_legacy = sum(1 for t in adjusted if not t.get("is_tautology"))

        keys = list(zip(base["leader"].to_list(), base["follower"].to_list()))
        base = base.with_columns(
            pl.col("bh_significant").alias("bh_significant_raw"),
            pl.Series("is_tautology", [k in tautologies for k in keys], dtype=pl.Boolean),
        )
        n_obs = [pair_info.get(k, {}).get("n_obs", 0) for k in keys]
        taut = base["is_tautology"].to_list()
        # Calibrate on the UNROUNDED p. The stored p_value_raw is rounded to 8 decimals, which sends every p
        # below 5e-9 to exactly 0 — precisely the strongest pairs, whose order (and calibrated p) would collapse.
        p_min = base["_p_exact"].to_numpy()

        # ---- placebo calibration (non-tautology pairs only) ----
        null_pool = np.array([p for k in keys for p in pair_info.get(k, {}).get("null_p", [])], dtype=float)
        null_pool = null_pool[np.isfinite(null_pool)]
        test_idx = [i for i, t in enumerate(taut) if not t]
        p_cal = [None] * len(keys)
        p_cal_adj = [None] * len(keys)
        method = ["none"] * len(keys)
        sig = base["bh_significant_raw"].to_list()
        n_tests = [n_family_legacy] * len(keys)

        if null_pool.size >= MIN_NULL_DRAWS and test_idx:
            cal, ext = calibrated_p(p_min[test_idx], null_pool)
            _, cal_adj, _, _ = multipletests(cal, alpha=BH_FDR_Q, method="fdr_bh")
            for j, i in enumerate(test_idx):
                p_cal[i] = float(cal[j])
                p_cal_adj[i] = float(cal_adj[j])
                method[i] = "tail_extrapolated" if ext[j] else "empirical"
                sig[i] = bool(cal_adj[j] < BH_FDR_Q)
                n_tests[i] = len(test_idx)
        else:
            logger.warning(
                f"[gold_lead_lag] placebo null has {null_pool.size} draws (< MIN_NULL_DRAWS="
                f"{MIN_NULL_DRAWS}) — calibration skipped, bh_significant is the legacy raw-p BH"
            )
        for i, t in enumerate(taut):
            if t:
                sig[i] = False
                n_tests[i] = n_family_legacy

        corr_q = [float(null_corr_quantile(int(n), MAX_LAG, ACTIONABLE_NULL_QUANTILE)) if n else 1.0
                  for n in n_obs]
        max_cc = base["max_cross_corr"].to_list()
        actionable = [bool(s_ and not t_ and (c >= q_))
                      for s_, t_, c, q_ in zip(sig, taut, max_cc, corr_q)]

        return base.with_columns([
            pl.Series("bh_significant", sig, dtype=pl.Boolean),
            pl.Series("p_value_calibrated", p_cal, dtype=pl.Float64),
            pl.Series("p_value_calibrated_adjusted", p_cal_adj, dtype=pl.Float64),
            pl.Series("p_calibration", method, dtype=pl.Utf8),
            pl.Series("n_tests", n_tests, dtype=pl.Int64),
            pl.Series("n_obs", n_obs, dtype=pl.Int64),
            pl.lit(ESTIMATOR).alias("estimator"),
            pl.Series("n_dropped_bars",
                      [dropped.get(l, 0) + dropped.get(f, 0) for l, f in keys], dtype=pl.Int64),
            pl.lit(int(null_pool.size)).cast(pl.Int64).alias("n_null_draws"),
            pl.Series("corr_null_p95", corr_q, dtype=pl.Float64),
            pl.Series("actionable", actionable, dtype=pl.Boolean),
            pl.lit(LAG_BASIS).alias("lag_basis"),
        ]).drop("_p_exact")

    # ── Regime tagging (same convention as correlation_module.py) ─────────

    @staticmethod
    def _get_current_regime(run_date: date) -> str:
        """The engine's own regime tag — see regime_tag.py (ADR-055 v2, R3)."""
        return current_regime_tag(run_date)


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
    write_history_snapshot(result, LEAD_LAG_HISTORY_NAME, run_date)   # ADD GMI-PIT-001 follow-up
    n_sig = int(result["bh_significant"].sum())
    logger.info(
        f"[gold_lead_lag] {result.height:,} pairs tested, {n_sig:,} bh_significant "
        f"(q={BH_FDR_Q}) -> {LEAD_LAG_STORE_PATH.name}"
    )
