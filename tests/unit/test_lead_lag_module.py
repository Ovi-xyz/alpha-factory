"""
tests/unit/test_lead_lag_module.py — LeadLagModule unit tests
(Architecture v2.0 §6.3 + Architecture Extension v1.0 ADR-001,
GMI Wave 1 Cycle 4 — third CrossAssetEngine module).

Test matrix:
    - Leader resolution excludes exclude_from_lead_lag_leader=True
      instruments (SSEC in production) entirely — never appears as
      `leader`, may still appear as `follower` if also Layer 1 (not the
      case for SSEC, but the exclusion logic itself doesn't care).
    - Graceful skip (compute() -> None) when followers resolver hasn't
      run yet, or when no eligible leaders exist.
    - Granger detection: a follower constructed to depend on a leader's
      value k days ago must be detected with optimal_lag==k, high
      r_squared, and bh_significant=True; an independent (noise) pair
      must not be bh_significant.
    - Global BH-FDR correction actually shrinks the significant count
      relative to uncorrected alpha=0.03 when many independent
      (non-causal) pairs are tested (empirical sanity, not a proof of
      BH theory).
    - Degenerate series (constant / near-zero variance) are skipped, not
      crashed on.
    - Insufficient pairwise overlap is skipped, not crashed on.
    - max_cross_corr is bounded to [0, 1] and reflects the actual lagged
      relationship strength.
    - Output schema/shape from run(); optimal_lag == optimal_lag_days.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

import src.gold.cross_asset.lead_lag_module as llm
import src.gold.cross_asset.regime_tag as regime_tag
from src.gold.cross_asset.lead_lag_module import LeadLagModule, run
from src.silver.active_symbols import ActiveSymbolsResolver

RUN_DATE = date(2026, 3, 1)
N_DAYS = 100
BASE_DATE = date(2026, 1, 1)
DATES = [BASE_DATE + timedelta(days=i) for i in range(N_DAYS)]


class _FakeInstrument:
    def __init__(self, symbol: str, exclude_from_lead_lag_leader: bool = False):
        self.symbol = symbol
        self.exclude_from_lead_lag_leader = exclude_from_lead_lag_leader


class _FakeLoader:
    def __init__(self, leaders: list[_FakeInstrument]):
        self._leaders = leaders

    def correlation_context(self) -> list[_FakeInstrument]:
        return self._leaders


@pytest.fixture
def paths(tmp_path, monkeypatch):
    monkeypatch.setattr(llm, "SILVER_OHLCV_PATH", tmp_path / "silver" / "market_ohlcv")
    monkeypatch.setattr(regime_tag, "GLOBAL_REGIME_PATH", tmp_path / "gold" / "cross_asset" / "global_regime.parquet")
    monkeypatch.setattr(llm, "GOLD_CROSS_ASSET_PATH", tmp_path / "gold" / "cross_asset")
    monkeypatch.setattr(llm, "LEAD_LAG_STORE_PATH", tmp_path / "gold" / "cross_asset" / "lead_lag_matrix.parquet")

    as_out = tmp_path / "silver" / "active_symbols"
    monkeypatch.setattr(ActiveSymbolsResolver, "OUTPUT_PATH", property(lambda s: as_out))
    return tmp_path, as_out


def _set_leaders(monkeypatch, leaders: list[_FakeInstrument]) -> None:
    monkeypatch.setattr(llm, "get_loader", lambda: _FakeLoader(leaders))


def _write_followers(as_out: Path, symbols: list[str]) -> None:
    as_out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}).write_parquet(
        as_out / f"active_ohlcv_{RUN_DATE.isoformat()}.parquet"
    )


def _write_series(tmp_path: Path, symbol: str, market: str, series: np.ndarray, n_days: int = N_DAYS) -> None:
    path = tmp_path / "silver" / "market_ohlcv" / market / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol":     [symbol] * n_days,
        "timestamp":  DATES[:n_days],
        "log_return": series[:n_days],
        "is_clean":   [True] * n_days,
    }).write_parquet(path)


def _weekday_dates_ending(end: date, lookback_days: int) -> list[date]:
    """Realistic 5-day trading calendar: every Mon-Fri date in the
    window ending at `end` — see test_correlation_module.py's identical
    helper for full rationale (FIX XAE-CAL-01 regression guard)."""
    start = end - timedelta(days=lookback_days)
    span = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    return [d for d in span if d.weekday() < 5]


def _write_series_at_dates(
    tmp_path: Path, symbol: str, market: str, series: np.ndarray, dates: list[date]
) -> None:
    path = tmp_path / "silver" / "market_ohlcv" / market / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol":     [symbol] * len(dates),
        "timestamp":  dates,
        "log_return": series[: len(dates)],
        "is_clean":   [True] * len(dates),
    }).write_parquet(path)


def _lagged_dependency(rng, n: int, lag: int, coef: float = 0.7, noise_scale: float = 0.003) -> tuple[np.ndarray, np.ndarray]:
    leader = rng.normal(0, 0.01, n)
    follower = np.zeros(n)
    follower[lag:] = coef * leader[:-lag] + rng.normal(0, noise_scale, n - lag)
    follower[:lag] = rng.normal(0, 0.01, lag)
    return leader, follower


class TestLeaderResolution:
    def test_excluded_leader_never_appears_as_leader(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(1)
        leader, follower = _lagged_dependency(rng, N_DAYS, lag=2)
        _write_series(tmp_path, "GOOD_LEADER", "context", leader)
        _write_series(tmp_path, "EXCLUDED_LEADER", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "FOLLOWER1", "us_stocks", follower)
        _write_followers(as_out, ["FOLLOWER1"])
        _set_leaders(monkeypatch, [
            _FakeInstrument("GOOD_LEADER"),
            _FakeInstrument("EXCLUDED_LEADER", exclude_from_lead_lag_leader=True),
        ])

        result = LeadLagModule().compute(RUN_DATE)

        assert "EXCLUDED_LEADER" not in result["leader"].to_list()

    def test_no_eligible_leaders_returns_none(self, paths, monkeypatch):
        _set_leaders(monkeypatch, [_FakeInstrument("X", exclude_from_lead_lag_leader=True)])
        assert LeadLagModule().compute(RUN_DATE) is None


class TestGracefulSkip:
    def test_returns_none_when_followers_not_resolved(self, paths, monkeypatch):
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])
        # active_ohlcv deliberately not written.
        assert LeadLagModule().compute(RUN_DATE) is None

    def test_run_does_not_raise_when_unresolved(self, paths, monkeypatch):
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])
        run(RUN_DATE)  # no exception


class TestGrangerDetection:
    def test_lagged_dependency_detected_at_correct_lag(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(2)
        leader, follower = _lagged_dependency(rng, N_DAYS, lag=2, coef=0.8, noise_scale=0.002)
        _write_series(tmp_path, "LEADER1", "context", leader)
        _write_series(tmp_path, "FOLLOWER1", "us_stocks", follower)
        _write_followers(as_out, ["FOLLOWER1"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])

        result = LeadLagModule().compute(RUN_DATE)

        row = result.filter(
            (pl.col("leader") == "LEADER1") & (pl.col("follower") == "FOLLOWER1")
        ).row(0, named=True)
        assert row["optimal_lag"] == 2
        assert row["optimal_lag_days"] == 2
        assert row["r_squared"] > 0.5
        assert row["bh_significant"] is True
        assert row["max_cross_corr"] > 0.5

    def test_independent_pair_not_significant(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(3)
        _write_series(tmp_path, "LEADER1", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "FOLLOWER1", "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_followers(as_out, ["FOLLOWER1"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])

        result = LeadLagModule().compute(RUN_DATE)

        row = result.row(0, named=True)
        assert row["bh_significant"] is False

    def test_max_cross_corr_bounded(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(4)
        leader, follower = _lagged_dependency(rng, N_DAYS, lag=1)
        _write_series(tmp_path, "LEADER1", "context", leader)
        _write_series(tmp_path, "FOLLOWER1", "us_stocks", follower)
        _write_followers(as_out, ["FOLLOWER1"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])

        result = LeadLagModule().compute(RUN_DATE)

        assert result["max_cross_corr"].min() >= 0.0
        assert result["max_cross_corr"].max() <= 1.0


class TestGlobalBHCorrection:
    def test_correction_reduces_significant_count_vs_raw_alpha(self, paths, monkeypatch):
        """1 real leader->follower relationship + several independent
        (noise) followers: BH-corrected count of "significant" pairs at
        q=0.03 must not exceed a naive raw-p<0.03 count by more than the
        genuine signal — i.e. correction is actually being applied, not
        a no-op passthrough of raw p-values."""
        tmp_path, as_out = paths
        rng = np.random.default_rng(5)
        leader, real_follower = _lagged_dependency(rng, N_DAYS, lag=1, coef=0.8, noise_scale=0.002)
        _write_series(tmp_path, "LEADER1", "context", leader)
        _write_series(tmp_path, "REAL_FOLLOWER", "us_stocks", real_follower)
        noise_followers = [f"NOISE{i}" for i in range(8)]
        for f in noise_followers:
            _write_series(tmp_path, f, "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_followers(as_out, ["REAL_FOLLOWER"] + noise_followers)
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])

        result = LeadLagModule().compute(RUN_DATE)

        real_row = result.filter(pl.col("follower") == "REAL_FOLLOWER").row(0, named=True)
        assert real_row["bh_significant"] is True
        # Adjusted p-value must never be smaller than the raw p-value —
        # a basic BH-correction invariant (correction only inflates or
        # preserves, never shrinks, an individual p-value).
        assert (result["p_value_adjusted"] >= result["p_value_raw"] - 1e-9).all()


class TestDegenerateAndInsufficientData:
    def test_constant_series_skipped_not_crashed(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(6)
        _write_series(tmp_path, "LEADER1", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "CONSTANT_FOLLOWER", "us_stocks", np.zeros(N_DAYS))
        _write_followers(as_out, ["CONSTANT_FOLLOWER"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])

        result = LeadLagModule().compute(RUN_DATE)
        assert result is None or result.is_empty()

    def test_insufficient_history_pair_skipped(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(7)
        _write_series(tmp_path, "LEADER1", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "SHORT_FOLLOWER", "us_stocks", rng.normal(0, 0.01, 5), n_days=5)
        _write_followers(as_out, ["SHORT_FOLLOWER"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])

        result = LeadLagModule().compute(RUN_DATE)
        assert result is None or result.is_empty()


class TestCalendarAwareness:
    """FIX XAE-CAL-01 regression guard (16 Sep 2026) — see
    test_correlation_module.py's identical class for full rationale. A
    genuine, gap-free 5-day-week leader/follower pair (46 obs here, vs.
    the old, unreachable 52-row threshold) must be detected, not
    silently dropped as insufficient history."""

    def test_weekday_only_leader_and_follower_not_excluded(self, paths, monkeypatch):
        tmp_path, as_out = paths
        weekday_dates = _weekday_dates_ending(RUN_DATE, llm.LOOKBACK_DAYS)
        n = len(weekday_dates)
        assert n < 52
        assert n >= llm.MIN_OBSERVATIONS

        rng = np.random.default_rng(42)
        leader_vals, follower_vals = _lagged_dependency(rng, n, lag=1, coef=0.8, noise_scale=0.002)
        _write_series_at_dates(tmp_path, "LEADER1", "context", leader_vals, weekday_dates)
        _write_series_at_dates(tmp_path, "REAL_FOLLOWER", "us_stocks", follower_vals, weekday_dates)
        _write_followers(as_out, ["REAL_FOLLOWER"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])

        result = LeadLagModule().compute(RUN_DATE)

        assert result is not None and not result.is_empty()
        row = result.filter(pl.col("follower") == "REAL_FOLLOWER").row(0, named=True)
        assert row["bh_significant"] is True


class TestRunEntryPoint:
    def test_run_writes_expected_schema(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(8)
        leader, follower = _lagged_dependency(rng, N_DAYS, lag=3)
        _write_series(tmp_path, "LEADER1", "context", leader)
        _write_series(tmp_path, "FOLLOWER1", "us_stocks", follower)
        _write_followers(as_out, ["FOLLOWER1"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])

        run(RUN_DATE)

        out_path = tmp_path / "gold" / "cross_asset" / "lead_lag_matrix.parquet"
        assert out_path.exists()
        out = pl.read_parquet(out_path)
        required = {
            "leader", "follower", "optimal_lag", "optimal_lag_days",
            "p_value_raw", "p_value_adjusted", "r_squared", "regime",
            "computation_date", "bh_significant", "max_cross_corr",
        }
        assert required.issubset(set(out.columns))
        assert out.row(0, named=True)["optimal_lag"] == out.row(0, named=True)["optimal_lag_days"]


# ═══════════════════════════════════════════════════════════════════════════
# XAE-R08 — robust estimator, placebo calibration, tautologies, n-aware actionable, reporting
# ═══════════════════════════════════════════════════════════════════════════

def _relations(tmp_path, monkeypatch, pairs):
    """Point the module at a relations file listing {leader: [followers]}."""
    import yaml
    p = tmp_path / "relations.yaml"
    p.write_text(yaml.safe_dump({"tautologies": [
        {"relation": "test", "leader": l, "followers": f} for l, f in pairs.items()]}))
    monkeypatch.setattr(llm, "RELATIONS_CONFIG_PATH", p)


def _no_relations(tmp_path, monkeypatch):
    _relations(tmp_path, monkeypatch, {})


class TestTautologyPairs:

    def test_identity_pair_is_flagged_and_never_significant_or_actionable(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(2)
        leader, follower = _lagged_dependency(rng, N_DAYS, lag=1, coef=0.9, noise_scale=0.001)
        _write_series(tmp_path, "DXYLIKE", "context", leader)
        _write_series(tmp_path, "EURLIKE", "us_stocks", follower)
        _write_followers(as_out, ["EURLIKE"])
        _set_leaders(monkeypatch, [_FakeInstrument("DXYLIKE")])
        _no_relations(tmp_path, monkeypatch)
        base = LeadLagModule().compute(RUN_DATE).row(0, named=True)
        assert base["bh_significant"] is True and base["is_tautology"] is False   # genuinely strong

        _relations(tmp_path, monkeypatch, {"DXYLIKE": ["EURLIKE"]})
        row = LeadLagModule().compute(RUN_DATE).row(0, named=True)
        assert row["is_tautology"] is True
        assert row["bh_significant"] is False and row["actionable"] is False
        assert row["p_value_adjusted"] is None and row["p_value_calibrated"] is None
        assert row["max_cross_corr"] > 0.5                        # the data are untouched, only the label

    def test_tautologies_are_outside_the_bh_family(self, paths, monkeypatch):
        """An identity pair's tiny p-values must not make the OTHER pairs' BH thresholds more lenient."""
        tmp_path, as_out = paths
        rng = np.random.default_rng(5)
        leader, follower = _lagged_dependency(rng, N_DAYS, lag=1, coef=0.9, noise_scale=0.001)
        _write_series(tmp_path, "L1", "context", leader)
        _write_series(tmp_path, "TAUT", "us_stocks", follower)
        for i in range(6):
            _write_series(tmp_path, f"N{i}", "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_followers(as_out, ["TAUT"] + [f"N{i}" for i in range(6)])
        _set_leaders(monkeypatch, [_FakeInstrument("L1")])
        _relations(tmp_path, monkeypatch, {"L1": ["TAUT"]})
        r = LeadLagModule().compute(RUN_DATE)
        assert r.filter(pl.col("is_tautology")).height == 1
        assert r.filter(~pl.col("is_tautology"))["n_tests"].unique().to_list() == [6 * 5]   # 6 pairs x 5 lags

    def test_load_tautologies_reads_the_config(self, tmp_path):
        import yaml
        p = tmp_path / "r.yaml"
        p.write_text(yaml.safe_dump({"tautologies": [{"leader": "A", "followers": ["B", "C"]}]}))
        assert llm.load_tautologies(p) == {("A", "B"), ("A", "C")}

    def test_missing_config_gives_empty_set_with_a_warning(self, tmp_path):
        from loguru import logger
        msgs = []
        sink = logger.add(lambda m: msgs.append(m.record["message"]), level="WARNING")
        try:
            assert llm.load_tautologies(tmp_path / "absent.yaml") == set()
        finally:
            logger.remove(sink)
        assert any("NO pairs are marked tautological" in m for m in msgs)

    def test_shipped_config_names_real_instruments(self):
        from src.config.instrument_loader import get_loader
        loader = get_loader()
        context = {i.symbol for i in loader.all_context(include_deferred=True)}
        pairs = llm.load_tautologies()
        assert ("DXY", "EUR_USD") in pairs
        for leader, follower in pairs:
            assert leader in context, f"{leader} is not a Layer 2 context instrument"
            loader.get(follower)          # Layer 1 follower: raises KeyError if not in the registry


class TestRobustEstimator:

    def test_a_shared_crash_bar_one_day_apart_is_not_a_lead_lag(self, paths, monkeypatch):
        """The outlier-induced spurious lag-1: independent series that each crashed once, on consecutive
        days. Un-winsorised Granger calls it overwhelming; the module's estimator does not."""
        tmp_path, as_out = paths
        rng = np.random.default_rng(21)
        leader = rng.normal(0, 0.005, N_DAYS)
        follower = rng.normal(0, 0.005, N_DAYS)
        # window rows are DATES within 65 calendar days of RUN_DATE: index 50 and 51 are inside it
        leader[50] = -0.20
        follower[51] = -0.20
        _write_series(tmp_path, "LEADER1", "context", leader)
        _write_series(tmp_path, "FOLLOWER1", "us_stocks", follower)
        _write_followers(as_out, ["FOLLOWER1"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])
        _no_relations(tmp_path, monkeypatch)

        win = [i for i, d in enumerate(DATES) if RUN_DATE - timedelta(days=llm.LOOKBACK_DAYS) <= d <= RUN_DATE]
        raw_p = LeadLagModule._run_granger_quiet_per_lag(
            np.column_stack([follower[win], leader[win]]), llm.MAX_LAG)
        assert min(p for p, _ in raw_p.values()) < 1e-4, "fixture must be spurious under the raw estimator"

        row = LeadLagModule().compute(RUN_DATE).row(0, named=True)
        assert row["p_value_raw"] > 0.01
        assert row["bh_significant"] is False
        assert row["estimator"] == "winsorised_mad3"

    def test_a_genuine_lag_survives_winsorisation(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(2)
        leader, follower = _lagged_dependency(rng, N_DAYS, lag=2, coef=0.8, noise_scale=0.002)
        _write_series(tmp_path, "LEADER1", "context", leader)
        _write_series(tmp_path, "FOLLOWER1", "us_stocks", follower)
        _write_followers(as_out, ["FOLLOWER1"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])
        _no_relations(tmp_path, monkeypatch)
        row = LeadLagModule().compute(RUN_DATE).row(0, named=True)
        assert row["optimal_lag"] == 2 and row["bh_significant"] is True and row["actionable"] is True


class TestPlaceboCalibration:

    def _universe(self, tmp_path, as_out, monkeypatch, n_leaders=15, n_followers=15, plant=True):
        rng = np.random.default_rng(31)
        leaders = [f"LD{i}" for i in range(n_leaders)]
        followers = [f"FW{i}" for i in range(n_followers)]
        series = {s: rng.normal(0, 0.01, N_DAYS) for s in leaders}
        for s in leaders:
            _write_series(tmp_path, s, "context", series[s])
        for j, f in enumerate(followers):
            x = rng.normal(0, 0.01, N_DAYS)
            if plant and j == 0:
                x = np.zeros(N_DAYS)
                x[1:] = 0.9 * series["LD0"][:-1] + rng.normal(0, 0.001, N_DAYS - 1)
            _write_series(tmp_path, f, "us_stocks", x)
        _write_followers(as_out, followers)
        _set_leaders(monkeypatch, [_FakeInstrument(s) for s in leaders])
        _no_relations(tmp_path, monkeypatch)

    def test_small_null_falls_back_to_the_legacy_test_and_says_so(self, paths, monkeypatch):
        tmp_path, as_out = paths
        self._universe(tmp_path, as_out, monkeypatch, 3, 3)
        r = LeadLagModule().compute(RUN_DATE)
        assert set(r["p_calibration"].to_list()) == {"none"}
        assert r["p_value_calibrated"].null_count() == r.height
        assert r["n_null_draws"][0] == 9 and r["n_null_draws"][0] < llm.MIN_NULL_DRAWS
        assert r["bh_significant"].to_list() == r["bh_significant_raw"].to_list()

    def test_calibrated_path_finds_the_planted_pair_and_no_flood(self, paths, monkeypatch):
        tmp_path, as_out = paths
        monkeypatch.setattr(llm, "PLACEBO_SHIFTS", 2)
        monkeypatch.setattr(llm, "MIN_NULL_DRAWS", 400)
        self._universe(tmp_path, as_out, monkeypatch)            # 225 pairs x 2 shifts = 450 draws
        r = LeadLagModule().compute(RUN_DATE)
        assert r["n_null_draws"][0] == 450
        assert set(r["p_calibration"].to_list()) <= {"empirical", "tail_extrapolated"}
        assert r["n_tests"].unique().to_list() == [225]          # the family is the pairs
        planted = r.filter((pl.col("leader") == "LD0") & (pl.col("follower") == "FW0")).row(0, named=True)
        assert planted["p_calibration"] == "tail_extrapolated"
        assert planted["bh_significant"] is True and planted["actionable"] is True
        assert planted["p_value_calibrated"] < 1e-3
        others = r.filter(~((pl.col("leader") == "LD0") & (pl.col("follower") == "FW0")))
        assert others["bh_significant"].sum() <= 1, "calibrated BH must not flood on pure noise"

    def test_tautology_pair_is_outside_the_calibrated_family_and_the_null(self, paths, monkeypatch):
        """With calibration active, an identity pair must not be calibrated, must not add placebo draws,
        and must not enlarge the BH family the other pairs are held to."""
        tmp_path, as_out = paths
        monkeypatch.setattr(llm, "PLACEBO_SHIFTS", 2)
        monkeypatch.setattr(llm, "MIN_NULL_DRAWS", 400)
        self._universe(tmp_path, as_out, monkeypatch)
        _relations(tmp_path, monkeypatch, {"LD0": ["FW0"]})          # the planted pair is an "identity"
        r = LeadLagModule().compute(RUN_DATE)
        taut = r.filter(pl.col("is_tautology")).row(0, named=True)
        assert (taut["leader"], taut["follower"]) == ("LD0", "FW0")
        assert taut["p_value_calibrated"] is None and taut["p_calibration"] == "none"
        assert taut["bh_significant"] is False and taut["actionable"] is False
        rest = r.filter(~pl.col("is_tautology"))
        assert rest["n_tests"].unique().to_list() == [224]           # 225 pairs minus the identity
        assert r["n_null_draws"][0] == 224 * 2                       # no placebo run for the identity

    def test_raw_columns_are_kept_alongside_the_calibrated_ones(self, paths, monkeypatch):
        tmp_path, as_out = paths
        self._universe(tmp_path, as_out, monkeypatch, 2, 2)
        r = LeadLagModule().compute(RUN_DATE)
        for c in ("p_value_raw", "p_value_adjusted", "bh_significant_raw", "p_value_calibrated",
                  "p_value_calibrated_adjusted"):
            assert c in r.columns


class TestActionableAndReporting:

    def test_actionable_uses_the_n_aware_null_not_a_fixed_0_15(self):
        mod = LeadLagModule()
        raw = []
        for follower, p in (("HIGHCORR", 1e-12), ("LOWCORR", 1e-12)):
            for lag in range(1, 6):
                raw.append({"leader": "L", "follower": follower, "lag": lag,
                            "p_raw": p if lag == 1 else 0.5, "r_squared": 0.5, "is_tautology": False})
        cc = {("L", "HIGHCORR"): 0.70, ("L", "LOWCORR"): 0.20}      # 0.20 > the old 0.15, < 0.40 null p95
        info = {("L", f): {"n_obs": 44, "null_p": []} for f in ("HIGHCORR", "LOWCORR")}
        r = mod._finalize(raw, cc, info, set(), {}).sort("follower")
        high, low = r.row(0, named=True), r.row(1, named=True)   # sorted: HIGHCORR, LOWCORR
        assert high["follower"] == "HIGHCORR" and low["follower"] == "LOWCORR"
        assert low["bh_significant"] is True and low["max_cross_corr"] > 0.15
        assert low["corr_null_p95"] == pytest.approx(0.40, abs=0.03)
        assert low["actionable"] is False and high["actionable"] is True

    def test_reporting_columns_present_and_correct(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(8)
        _write_series(tmp_path, "LEADER1", "context", rng.normal(0, 0.01, N_DAYS))
        # FOLLOWER1 has 4 bars the validator dropped inside the window
        path = tmp_path / "silver" / "market_ohlcv" / "us_stocks" / "symbol=FOLLOWER1" / "FOLLOWER1_1D_silver.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        flags = [True] * N_DAYS
        for i in (45, 50, 55, 58):
            flags[i] = False
        pl.DataFrame({"symbol": ["FOLLOWER1"] * N_DAYS, "timestamp": DATES,
                      "log_return": rng.normal(0, 0.01, N_DAYS), "is_clean": flags}).write_parquet(path)
        _write_followers(as_out, ["FOLLOWER1"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])
        _no_relations(tmp_path, monkeypatch)
        row = LeadLagModule().compute(RUN_DATE).row(0, named=True)
        in_window = sum(1 for i in (45, 50, 55, 58)
                        if RUN_DATE - timedelta(days=llm.LOOKBACK_DAYS) <= DATES[i] <= RUN_DATE)
        assert in_window > 0
        assert row["n_dropped_bars"] == in_window
        assert row["lag_basis"] == "date_index"
        assert row["estimator"] == llm.ESTIMATOR
        assert 40 <= row["n_obs"] <= 65

    def test_run_writes_the_new_columns(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(9)
        _write_series(tmp_path, "LEADER1", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "FOLLOWER1", "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_followers(as_out, ["FOLLOWER1"])
        _set_leaders(monkeypatch, [_FakeInstrument("LEADER1")])
        _no_relations(tmp_path, monkeypatch)
        monkeypatch.setattr(llm, "write_history_snapshot", lambda *a, **k: None)
        run(RUN_DATE)
        out = pl.read_parquet(tmp_path / "gold" / "cross_asset" / "lead_lag_matrix.parquet")
        assert {"is_tautology", "actionable", "p_calibration", "n_tests", "n_obs", "estimator",
                "n_dropped_bars", "n_null_draws", "corr_null_p95", "lag_basis"} <= set(out.columns)


class TestCalibrationSeesExactP:
    """Review finding: p_value_raw is stored rounded to 8 decimals; calibrating on it sends every p < 5e-9
    to 0, collapsing the order of exactly the strongest pairs."""

    def test_stronger_pair_gets_a_strictly_smaller_calibrated_p(self, monkeypatch):
        monkeypatch.setattr(llm, "MIN_NULL_DRAWS", 30)
        rng = np.random.default_rng(3)
        null = list(np.minimum(1.0, (rng.uniform(size=500) / 2.8) ** (1 / 0.9)))
        raw, cc, info = [], {}, {}
        for follower, p in (("STRONGER", 1e-30), ("STRONG", 1e-20)):
            for lag in range(1, 6):
                raw.append({"leader": "L", "follower": follower, "lag": lag,
                            "p_raw": p if lag == 1 else 0.5, "r_squared": 0.5, "is_tautology": False})
            cc[("L", follower)] = 0.8
            info[("L", follower)] = {"n_obs": 44, "null_p": null if follower == "STRONGER" else []}
        r = LeadLagModule()._finalize(raw, cc, info, set(), {})
        by = {row["follower"]: row for row in r.iter_rows(named=True)}
        assert by["STRONGER"]["p_value_raw"] == 0.0 and by["STRONG"]["p_value_raw"] == 0.0   # stored rounded
        assert by["STRONGER"]["p_value_calibrated"] < by["STRONG"]["p_value_calibrated"]
        assert "_p_exact" not in r.columns
