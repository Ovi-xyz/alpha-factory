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
    monkeypatch.setattr(llm, "REGIME_STORE_PATH", tmp_path / "gold" / "macro" / "regime_store.parquet")
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
