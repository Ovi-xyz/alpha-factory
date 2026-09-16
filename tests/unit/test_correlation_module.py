"""
tests/unit/test_correlation_module.py — CorrelationModule unit tests
(Architecture v2.0 §6.2, GMI Wave 1 Cycle 4 — second CrossAssetEngine
module).

Test matrix:
    - Universe merge: Layer 1 (active_ohlcv) ∪ Layer 2 (context_anchors),
      de-duplicated.
    - Graceful skip (compute() -> None) when either resolver has not
      been run yet for run_date (FileNotFoundError caught, not raised).
    - Ledoit-Wolf correlation math: two independent groups of symbols
      driven by a shared latent factor within-group must show high
      within-group correlation and near-zero cross-group correlation.
    - Clustering: with a small, evenly-split universe (n=4, forcing
      exactly 2 clusters via the max(2, n//2) heuristic), the two
      natural correlated groups land in different clusters and every
      member of a group shares its group's cluster_id.
    - Regime tagging: current row of regime_store.parquet (most recent
      date <= run_date) is attached to every output row; "UNKNOWN" if
      the store is absent.
    - Output schema/shape: symbol_a < symbol_b (lexicographic, no
      self-pairs, no mirrored duplicates), row count == C(n, 2).
    - Insufficient history: a symbol with too few clean rows is dropped,
      not crashed on.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

import src.gold.cross_asset.correlation_module as cmod
from src.gold.cross_asset.correlation_module import CorrelationModule, run
from src.silver.active_symbols import ActiveSymbolsResolver
from src.silver.context_anchors import ContextAnchorsResolver

RUN_DATE = date(2026, 3, 1)
N_DAYS = 70
BASE_DATE = date(2026, 1, 1)
DATES = [BASE_DATE + timedelta(days=i) for i in range(N_DAYS)]


@pytest.fixture
def paths(tmp_path, monkeypatch):
    monkeypatch.setattr(cmod, "SILVER_OHLCV_PATH", tmp_path / "silver" / "market_ohlcv")
    monkeypatch.setattr(cmod, "REGIME_STORE_PATH", tmp_path / "gold" / "macro" / "regime_store.parquet")
    monkeypatch.setattr(cmod, "GOLD_CROSS_ASSET_PATH", tmp_path / "gold" / "cross_asset")
    monkeypatch.setattr(cmod, "CROSS_ASSET_CORR_PATH", tmp_path / "gold" / "cross_asset" / "cross_asset_corr.parquet")

    as_out = tmp_path / "silver" / "active_symbols"
    monkeypatch.setattr(ActiveSymbolsResolver, "OUTPUT_PATH", property(lambda s: as_out))
    ca_out = tmp_path / "silver" / "context_anchors"
    monkeypatch.setattr(ContextAnchorsResolver, "OUTPUT_PATH", property(lambda s: ca_out))

    return tmp_path, as_out, ca_out


def _write_active_ohlcv(as_out: Path, symbols: list[str]) -> None:
    as_out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}).write_parquet(
        as_out / f"active_ohlcv_{RUN_DATE.isoformat()}.parquet"
    )


def _write_context_anchors(ca_out: Path, symbols: list[str]) -> None:
    ca_out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}, schema={"symbol": pl.Utf8}).write_parquet(
        ca_out / f"context_anchors_{RUN_DATE.isoformat()}.parquet"
    )


def _write_l1_returns(
    tmp_path: Path, symbol: str, log_returns: np.ndarray, is_clean: bool = True, n_days: int = N_DAYS
) -> None:
    path = tmp_path / "silver" / "market_ohlcv" / "us_stocks" / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol":     [symbol] * n_days,
        "timestamp":  DATES[:n_days],
        "log_return": log_returns[:n_days],
        "is_clean":   [is_clean] * n_days,
    }).write_parquet(path)


def _weekday_dates_ending(end: date, lookback_days: int) -> list[date]:
    """Realistic 5-day trading calendar: every Mon-Fri date in the
    window ending at `end`, going back `lookback_days` calendar days —
    what a genuine, gap-free equity/IDX/forex feed actually looks like
    (never a row on Saturday/Sunday). Used by FIX XAE-CAL-01's
    regression guard below; every other fixture in this file uses
    artificial 7-day-a-week consecutive dates, which is exactly why the
    original MIN_HISTORY_RATIO bug escaped this test suite."""
    start = end - timedelta(days=lookback_days)
    span = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    return [d for d in span if d.weekday() < 5]


def _write_l1_returns_at_dates(
    tmp_path: Path, symbol: str, log_returns: np.ndarray, dates: list[date]
) -> None:
    path = tmp_path / "silver" / "market_ohlcv" / "us_stocks" / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol":     [symbol] * len(dates),
        "timestamp":  dates,
        "log_return": log_returns[: len(dates)],
        "is_clean":   [True] * len(dates),
    }).write_parquet(path)


def _write_l2_returns(tmp_path: Path, symbol: str, log_returns: np.ndarray) -> None:
    path = tmp_path / "silver" / "market_ohlcv" / "context" / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol":     [symbol] * N_DAYS,
        "timestamp":  DATES,
        "log_return": log_returns,
        "is_clean":   [True] * N_DAYS,
    }).write_parquet(path)


class TestUniverseMerge:
    def test_layer1_and_layer2_merged_and_deduped(self, paths):
        tmp_path, as_out, ca_out = paths
        _write_active_ohlcv(as_out, ["AAPL", "MSFT", "SHARED"])
        _write_context_anchors(ca_out, ["DXY", "VIX", "SHARED"])

        merged = CorrelationModule._resolve_merged_universe(RUN_DATE)

        assert merged == ["AAPL", "DXY", "MSFT", "SHARED", "VIX"]

    def test_returns_none_when_layer1_resolver_not_run(self, paths):
        _, as_out, ca_out = paths
        _write_context_anchors(ca_out, ["DXY"])
        # active_ohlcv deliberately not written for RUN_DATE.
        assert CorrelationModule._resolve_merged_universe(RUN_DATE) is None

    def test_returns_none_when_layer2_resolver_not_run(self, paths):
        _, as_out, ca_out = paths
        _write_active_ohlcv(as_out, ["AAPL"])
        assert CorrelationModule._resolve_merged_universe(RUN_DATE) is None

    def test_compute_returns_none_gracefully_not_raises(self, paths):
        module = CorrelationModule()
        assert module.compute(RUN_DATE) is None  # neither resolver run


class TestLedoitWolfCorrelation:
    def test_within_group_high_between_group_near_zero(self, paths):
        """Two independent latent-factor-driven groups: within-group
        correlation must be materially higher than cross-group."""
        tmp_path, as_out, ca_out = paths
        rng = np.random.default_rng(7)
        common_a = rng.normal(0, 0.01, N_DAYS)
        common_b = rng.normal(0, 0.01, N_DAYS)
        group_a = ["A1", "A2", "A3"]
        group_b = ["B1", "B2", "B3"]

        _write_active_ohlcv(as_out, group_a + group_b)
        _write_context_anchors(ca_out, [])
        for s in group_a:
            _write_l1_returns(tmp_path, s, common_a + rng.normal(0, 0.001, N_DAYS))
        for s in group_b:
            _write_l1_returns(tmp_path, s, common_b + rng.normal(0, 0.001, N_DAYS))

        result = CorrelationModule().compute(RUN_DATE)

        within = result.filter(
            (pl.col("symbol_a").is_in(group_a) & pl.col("symbol_b").is_in(group_a))
            | (pl.col("symbol_a").is_in(group_b) & pl.col("symbol_b").is_in(group_b))
        )["correlation"]
        cross = result.filter(
            pl.col("symbol_a").is_in(group_a) & pl.col("symbol_b").is_in(group_b)
        )["correlation"]

        assert within.min() > 0.7, f"within-group correlation too low: {within.to_list()}"
        assert cross.abs().max() < 0.3, f"cross-group correlation too high: {cross.to_list()}"

    def test_correlation_bounded_and_self_correlation_excluded(self, paths):
        tmp_path, as_out, ca_out = paths
        rng = np.random.default_rng(1)
        _write_active_ohlcv(as_out, ["X", "Y", "Z"])
        _write_context_anchors(ca_out, [])
        for s in ["X", "Y", "Z"]:
            _write_l1_returns(tmp_path, s, rng.normal(0, 0.01, N_DAYS))

        result = CorrelationModule().compute(RUN_DATE)

        assert result["correlation"].abs().max() <= 1.0
        assert set(zip(result["symbol_a"], result["symbol_b"])) == {
            ("X", "Y"), ("X", "Z"), ("Y", "Z")
        }
        assert result.height == 3  # C(3, 2)


class TestClustering:
    def test_two_natural_groups_land_in_different_clusters(self, paths):
        """n=4 (2+2) forces exactly max(2, 4//2)=2 clusters — aligns
        cleanly with the 2 synthetic correlated groups."""
        tmp_path, as_out, ca_out = paths
        rng = np.random.default_rng(3)
        common_a = rng.normal(0, 0.01, N_DAYS)
        common_b = rng.normal(0, 0.01, N_DAYS)
        _write_active_ohlcv(as_out, ["A1", "A2", "B1", "B2"])
        _write_context_anchors(ca_out, [])
        _write_l1_returns(tmp_path, "A1", common_a + rng.normal(0, 0.0005, N_DAYS))
        _write_l1_returns(tmp_path, "A2", common_a + rng.normal(0, 0.0005, N_DAYS))
        _write_l1_returns(tmp_path, "B1", common_b + rng.normal(0, 0.0005, N_DAYS))
        _write_l1_returns(tmp_path, "B2", common_b + rng.normal(0, 0.0005, N_DAYS))

        result = CorrelationModule().compute(RUN_DATE)

        def cluster_of(sym):
            row = result.filter(pl.col("symbol_a") == sym)
            if row.height:
                return row.row(0, named=True)["cluster_id_a"]
            return result.filter(pl.col("symbol_b") == sym).row(0, named=True)["cluster_id_b"]

        assert cluster_of("A1") == cluster_of("A2")
        assert cluster_of("B1") == cluster_of("B2")
        assert cluster_of("A1") != cluster_of("B1")

    def test_every_symbol_gets_an_integer_cluster_id(self, paths):
        tmp_path, as_out, ca_out = paths
        rng = np.random.default_rng(9)
        symbols = [f"S{i}" for i in range(6)]
        _write_active_ohlcv(as_out, symbols)
        _write_context_anchors(ca_out, [])
        for s in symbols:
            _write_l1_returns(tmp_path, s, rng.normal(0, 0.01, N_DAYS))

        result = CorrelationModule().compute(RUN_DATE)

        all_ids = set(result["cluster_id_a"].to_list()) | set(result["cluster_id_b"].to_list())
        assert all(isinstance(c, int) for c in all_ids)


class TestRegimeTagging:
    def test_current_regime_attached_to_all_rows(self, paths):
        tmp_path, as_out, ca_out = paths
        rng = np.random.default_rng(2)
        _write_active_ohlcv(as_out, ["X", "Y"])
        _write_context_anchors(ca_out, [])
        for s in ["X", "Y"]:
            _write_l1_returns(tmp_path, s, rng.normal(0, 0.01, N_DAYS))

        regime_path = tmp_path / "gold" / "macro" / "regime_store.parquet"
        regime_path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "date":   [str(RUN_DATE - timedelta(days=5)), str(RUN_DATE)],
            "regime": ["RISK_OFF", "RISK_ON"],
        }).write_parquet(regime_path)

        result = CorrelationModule().compute(RUN_DATE)

        assert set(result["regime"].to_list()) == {"RISK_ON"}

    def test_unknown_regime_when_store_absent(self, paths):
        tmp_path, as_out, ca_out = paths
        rng = np.random.default_rng(4)
        _write_active_ohlcv(as_out, ["X", "Y"])
        _write_context_anchors(ca_out, [])
        for s in ["X", "Y"]:
            _write_l1_returns(tmp_path, s, rng.normal(0, 0.01, N_DAYS))

        result = CorrelationModule().compute(RUN_DATE)

        assert set(result["regime"].to_list()) == {"UNKNOWN"}


class TestInsufficientHistory:
    def test_symbol_with_too_little_history_dropped_not_crashed(self, paths):
        tmp_path, as_out, ca_out = paths
        rng = np.random.default_rng(5)
        _write_active_ohlcv(as_out, ["FULL_A", "FULL_B", "SHORT"])
        _write_context_anchors(ca_out, [])
        _write_l1_returns(tmp_path, "FULL_A", rng.normal(0, 0.01, N_DAYS))
        _write_l1_returns(tmp_path, "FULL_B", rng.normal(0, 0.01, N_DAYS))
        # Far below the MIN_OBSERVATIONS=40 floor.
        _write_l1_returns(tmp_path, "SHORT", rng.normal(0, 0.01, 5), n_days=5)

        result = CorrelationModule().compute(RUN_DATE)

        assert result is not None
        all_syms = set(result["symbol_a"].to_list()) | set(result["symbol_b"].to_list())
        assert "SHORT" not in all_syms
        assert all_syms == {"FULL_A", "FULL_B"}

    def test_fewer_than_two_valid_symbols_returns_none(self, paths):
        tmp_path, as_out, ca_out = paths
        rng = np.random.default_rng(6)
        _write_active_ohlcv(as_out, ["ONLY"])
        _write_context_anchors(ca_out, [])
        _write_l1_returns(tmp_path, "ONLY", rng.normal(0, 0.01, N_DAYS))

        assert CorrelationModule().compute(RUN_DATE) is None


class TestIsCleanFiltering:
    def test_dirty_rows_excluded_from_returns(self, paths):
        tmp_path, as_out, ca_out = paths
        rng = np.random.default_rng(8)
        _write_active_ohlcv(as_out, ["X", "Y"])
        _write_context_anchors(ca_out, [])
        _write_l1_returns(tmp_path, "X", rng.normal(0, 0.01, N_DAYS), is_clean=False)
        _write_l1_returns(tmp_path, "Y", rng.normal(0, 0.01, N_DAYS))

        # X is entirely dirty -> excluded entirely -> fewer than 2 valid symbols.
        assert CorrelationModule().compute(RUN_DATE) is None


class TestCalendarAwareness:
    """FIX XAE-CAL-01 regression guard (16 Sep 2026). A 65-calendar-day
    window contains at most 48 weekdays — a genuine, gap-free 5-day-week
    symbol must NOT be excluded as insufficient history. The prior
    MIN_HISTORY_RATIO=0.8 threshold (int(65*0.8)=52) was unreachable for
    any such symbol and silently zeroed out CorrelationModule every
    week, regardless of data quality — see MIN_OBSERVATIONS' comment in
    correlation_module.py for the empirical calibration."""

    def test_weekday_only_symbols_not_excluded(self, paths):
        tmp_path, as_out, ca_out = paths
        weekday_dates = _weekday_dates_ending(RUN_DATE, cmod.LOOKBACK_DAYS)
        n = len(weekday_dates)
        # Fixture sanity, not a module assertion: confirms this test
        # actually exercises the failure mode (below the old, unreachable
        # threshold) and clears the new one.
        assert n < 52
        assert n >= cmod.MIN_OBSERVATIONS

        rng = np.random.default_rng(21)
        _write_active_ohlcv(as_out, ["WD_A", "WD_B"])
        _write_context_anchors(ca_out, [])
        _write_l1_returns_at_dates(tmp_path, "WD_A", rng.normal(0, 0.01, n), weekday_dates)
        _write_l1_returns_at_dates(tmp_path, "WD_B", rng.normal(0, 0.01, n), weekday_dates)

        result = CorrelationModule().compute(RUN_DATE)

        assert result is not None
        all_syms = set(result["symbol_a"].to_list()) | set(result["symbol_b"].to_list())
        assert {"WD_A", "WD_B"}.issubset(all_syms)


class TestRunEntryPoint:
    def test_run_writes_expected_schema(self, paths):
        tmp_path, as_out, ca_out = paths
        rng = np.random.default_rng(11)
        _write_active_ohlcv(as_out, ["A", "B", "C"])
        _write_context_anchors(ca_out, [])
        for s in ["A", "B", "C"]:
            _write_l1_returns(tmp_path, s, rng.normal(0, 0.01, N_DAYS))

        run(RUN_DATE)

        out_path = tmp_path / "gold" / "cross_asset" / "cross_asset_corr.parquet"
        assert out_path.exists()
        out = pl.read_parquet(out_path)
        required = {
            "symbol_a", "symbol_b", "correlation", "regime",
            "computation_date", "cluster_id_a", "cluster_id_b",
        }
        assert required.issubset(set(out.columns))
        assert out.height == 3

    def test_run_no_crash_when_universe_unresolved(self, paths):
        """run() must log and return, never raise, when upstream
        resolvers haven't produced a file for run_date yet."""
        run(RUN_DATE)  # no exception
