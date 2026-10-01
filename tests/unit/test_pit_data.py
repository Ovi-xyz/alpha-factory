"""
tests/unit/test_pit_data.py

FIX GMI-PIT-001 (ADR-050): PITDataLoader read methods for Gold outputs
previously unreachable from the backtest/PIT path — get_global_regime,
get_cross_asset_corr, get_lead_lag, get_forecast, get_signal_aggregation,
get_context_regime, get_gate_status.

pit_data.py had no dedicated unit test file before this (only indirect
coverage via test_backtest_engine.py) — this file covers the new methods
and their PIT guards specifically; it does not retrofit tests for the
pre-existing methods.

Isolation: every path constant is monkeypatched to a tmp_path fixture
file. Date-column types match what each real producer writes:
global_regime / signal_aggregation / gate_status use ISO-string dates;
cross_asset_corr / lead_lag / forecast use real Date computation_date.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import src.backtest.pit_data as pit
from src.backtest.pit_data import PITDataLoader


@pytest.fixture
def loader():
    ldr = PITDataLoader()
    yield ldr
    ldr.close()


def _patch(monkeypatch, name: str, path) -> None:
    monkeypatch.setattr(pit, name, str(path))


class TestGetGlobalRegime:

    def _write(self, path):
        pl.DataFrame({
            "date": ["2026-09-24", "2026-09-25", "2026-09-26"],
            "global_risk_score": [40.0, 55.0, 70.0],
            "global_regime_label": ["MIXED", "MIXED", "RISK_ON"],
        }).write_parquet(path)

    def test_returns_most_recent_on_or_before(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "global_regime.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_GLOBAL_REGIME_PATH", p)
        row = loader.get_global_regime(date(2026, 9, 25))
        assert row["date"] == "2026-09-25"
        assert row["global_risk_score"] == 55.0

    def test_never_returns_a_future_row(self, loader, monkeypatch, tmp_path):
        """PIT guard: a trade_date before the file's first row sees nothing,
        and one mid-file never sees the later rows."""
        p = tmp_path / "global_regime.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_GLOBAL_REGIME_PATH", p)
        assert loader.get_global_regime(date(2026, 9, 23)) is None
        assert loader.get_global_regime(date(2026, 9, 24))["date"] == "2026-09-24"

    def test_missing_file_returns_none(self, loader, monkeypatch, tmp_path):
        _patch(monkeypatch, "GOLD_GLOBAL_REGIME_PATH", tmp_path / "absent.parquet")
        assert loader.get_global_regime(date(2026, 9, 25)) is None


class TestGetCrossAssetCorr:

    def _write(self, path, computation_date=date(2026, 9, 20)):
        pl.DataFrame({
            "symbol_a": ["AAPL", "AAPL", "MSFT"],
            "symbol_b": ["MSFT", "GOOGL", "GOOGL"],
            "correlation": [0.8, 0.6, 0.7],
            "regime": ["RISK_ON"] * 3,
            "computation_date": [computation_date] * 3,
            "cluster_id_a": [1, 1, 1],
            "cluster_id_b": [1, 2, 2],
        }).write_parquet(path)

    def test_returns_full_snapshot_when_on_or_after(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "corr.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_CROSS_ASSET_CORR_PATH", p)
        assert loader.get_cross_asset_corr(date(2026, 9, 25)).height == 3

    def test_empty_when_trade_date_predates_computation(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "corr.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_CROSS_ASSET_CORR_PATH", p)
        assert loader.get_cross_asset_corr(date(2026, 9, 15)).is_empty()

    def test_symbol_filter_matches_either_side(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "corr.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_CROSS_ASSET_CORR_PATH", p)
        df = loader.get_cross_asset_corr(date(2026, 9, 25), symbol="MSFT")
        assert df.height == 2   # MSFT as symbol_b once, symbol_a once

    def test_missing_file_returns_empty_frame(self, loader, monkeypatch, tmp_path):
        _patch(monkeypatch, "GOLD_CROSS_ASSET_CORR_PATH", tmp_path / "absent.parquet")
        assert loader.get_cross_asset_corr(date(2026, 9, 25)).is_empty()


class TestGetLeadLag:

    def _write(self, path):
        pl.DataFrame({
            "leader": ["TWSE", "AXJO"],
            "follower": ["NVDA", "AU"],
            "optimal_lag_days": [1, 1],
            "bh_significant": [True, True],
            "max_cross_corr": [0.31, 0.22],
            "computation_date": [date(2026, 9, 20)] * 2,
        }).write_parquet(path)

    def test_symbol_filter_matches_leader(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "ll.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_LEAD_LAG_PATH", p)
        df = loader.get_lead_lag(date(2026, 9, 25), symbol="TWSE")
        assert df.height == 1 and df["follower"][0] == "NVDA"

    def test_symbol_filter_matches_follower(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "ll.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_LEAD_LAG_PATH", p)
        df = loader.get_lead_lag(date(2026, 9, 25), symbol="AU")
        assert df.height == 1 and df["leader"][0] == "AXJO"

    def test_pit_guard(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "ll.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_LEAD_LAG_PATH", p)
        assert loader.get_lead_lag(date(2026, 9, 19)).is_empty()
        assert loader.get_lead_lag(date(2026, 9, 20)).height == 2


class TestGetForecast:

    def _write(self, path):
        pl.DataFrame({
            "symbol": ["AAPL"] * 3 + ["MSFT"],
            "computation_date": [date(2026, 9, 20)] * 4,
            "horizon_days": [1, 2, 3, 1],
            "forecast_return": [0.001, 0.002, 0.003, 0.004],
        }).write_parquet(path)

    def test_returns_all_horizons_ordered(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "fc.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_FORECAST_PATH", p)
        df = loader.get_forecast("AAPL", date(2026, 9, 25))
        assert df["horizon_days"].to_list() == [1, 2, 3]

    def test_horizon_filter(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "fc.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_FORECAST_PATH", p)
        df = loader.get_forecast("AAPL", date(2026, 9, 25), horizon_days=2)
        assert df.height == 1 and df["forecast_return"][0] == pytest.approx(0.002)

    def test_unknown_symbol_empty(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "fc.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_FORECAST_PATH", p)
        assert loader.get_forecast("ZZZZ", date(2026, 9, 25)).is_empty()

    def test_pit_guard(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "fc.parquet"
        self._write(p)
        _patch(monkeypatch, "GOLD_FORECAST_PATH", p)
        assert loader.get_forecast("AAPL", date(2026, 9, 19)).is_empty()


class TestGetSignalAggregation:

    def _write_days(self, tmp_path):
        for d, score in [("2026-09-24", 0.1), ("2026-09-25", 0.2), ("2026-09-26", 0.3)]:
            pl.DataFrame({
                "symbol": ["AAPL", "MSFT"],
                "date": [d, d],
                "composite_score": [score, -score],
            }).write_parquet(tmp_path / f"signal_aggregation_{d}.parquet")
        return str(tmp_path / "signal_aggregation_*.parquet")

    def test_returns_most_recent_on_or_before(self, loader, monkeypatch, tmp_path):
        _patch(monkeypatch, "GOLD_SIGNAL_AGG_PATH", self._write_days(tmp_path))
        row = loader.get_signal_aggregation("AAPL", date(2026, 9, 25))
        assert row["date"] == "2026-09-25"
        assert row["composite_score"] == pytest.approx(0.2)

    def test_never_returns_a_future_day(self, loader, monkeypatch, tmp_path):
        _patch(monkeypatch, "GOLD_SIGNAL_AGG_PATH", self._write_days(tmp_path))
        assert loader.get_signal_aggregation("AAPL", date(2026, 9, 23)) is None

    def test_symbol_isolation(self, loader, monkeypatch, tmp_path):
        _patch(monkeypatch, "GOLD_SIGNAL_AGG_PATH", self._write_days(tmp_path))
        row = loader.get_signal_aggregation("MSFT", date(2026, 9, 26))
        assert row["composite_score"] == pytest.approx(-0.3)

    def test_no_files_returns_none(self, loader, monkeypatch, tmp_path):
        _patch(monkeypatch, "GOLD_SIGNAL_AGG_PATH", str(tmp_path / "nothing_*.parquet"))
        assert loader.get_signal_aggregation("AAPL", date(2026, 9, 25)) is None


class TestGetContextRegime:

    def test_returns_none_while_adr051_unbuilt(self, loader, monkeypatch, tmp_path):
        """ADR-051's context_regime.parquet doesn't exist yet — the method
        must degrade to None, not raise."""
        _patch(monkeypatch, "GOLD_CONTEXT_REGIME_PATH", tmp_path / "context_regime.parquet")
        assert loader.get_context_regime(date(2026, 9, 25)) is None

    def test_reads_once_producer_exists(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "context_regime.parquet"
        pl.DataFrame({
            "date": ["2026-09-24", "2026-09-25"],
            "context_regime": ["A", "B"],
        }).write_parquet(p)
        _patch(monkeypatch, "GOLD_CONTEXT_REGIME_PATH", p)
        assert loader.get_context_regime(date(2026, 9, 25))["context_regime"] == "B"
        assert loader.get_context_regime(date(2026, 9, 23)) is None


class TestGetGateStatus:

    def _write(self, path):
        rows = []
        for d, gold_status in [("2026-09-24", "warn"), ("2026-09-25", "pass"), ("2026-09-26", "block")]:
            for scope in ("bronze", "silver", "gold"):
                rows.append({
                    "run_date": d, "scope": scope, "layer_health": "pass",
                    "gate_status": gold_status if scope == "gold" else "pass",
                    "streak_days": 1,
                })
        pl.DataFrame(rows).write_parquet(path)

    def test_defaults_to_gold_scope(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "gate_status.parquet"
        self._write(p)
        _patch(monkeypatch, "GATE_STATUS_PATH", p)
        row = loader.get_gate_status(date(2026, 9, 25))
        assert row["scope"] == "gold"
        assert row["gate_status"] == "pass"

    def test_explicit_scope(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "gate_status.parquet"
        self._write(p)
        _patch(monkeypatch, "GATE_STATUS_PATH", p)
        row = loader.get_gate_status(date(2026, 9, 26), scope="bronze")
        assert row["scope"] == "bronze"

    def test_pit_guard_never_sees_future_status(self, loader, monkeypatch, tmp_path):
        p = tmp_path / "gate_status.parquet"
        self._write(p)
        _patch(monkeypatch, "GATE_STATUS_PATH", p)
        # 09-25 is 'pass'; the later 09-26 'block' must not leak backward.
        assert loader.get_gate_status(date(2026, 9, 25))["gate_status"] == "pass"
        assert loader.get_gate_status(date(2026, 9, 23)) is None

    def test_missing_file_returns_none(self, loader, monkeypatch, tmp_path):
        _patch(monkeypatch, "GATE_STATUS_PATH", tmp_path / "absent.parquet")
        assert loader.get_gate_status(date(2026, 9, 25)) is None

    def test_roundtrip_with_real_gatekeeper_output(self, loader, monkeypatch, tmp_path):
        """Schema-contract check: what Gatekeeper.compute() actually emits
        is readable by get_gate_status() — guards against the two drifting."""
        import src.utils.gatekeeper as gk
        from src.scheduler.dependency_guard import DependencyGuard
        # Isolate from the real data/.sentinels + data/health/ locations.
        guard = DependencyGuard(sentinel_dir=tmp_path / ".sentinels")
        monkeypatch.setattr(gk, "DependencyGuard", lambda: guard)
        monkeypatch.setattr(gk, "QUALITY_GATE_LOG_PATH", tmp_path / "qgl.parquet")
        out = gk.compute(date(2026, 9, 27))
        p = tmp_path / "gate_status.parquet"
        out.write_parquet(p)
        _patch(monkeypatch, "GATE_STATUS_PATH", p)
        row = loader.get_gate_status(date(2026, 9, 27), scope="silver")
        assert row is not None
        assert set(row) == {"run_date", "scope", "layer_health", "gate_status", "streak_days"}


class TestCrossAssetPITHistorySelection:
    """
    With the additive history store, the three weekly CrossAssetEngine reads
    must return exactly ONE snapshot: the latest with computation_date <=
    trade_date. Regression target: a naive `WHERE computation_date <=
    trade_date` returns every earlier week concatenated once history exists.
    """

    WEEKS = [date(2026, 9, 6), date(2026, 9, 13), date(2026, 9, 20)]

    def _corr(self, d, value):
        return pl.DataFrame({
            "symbol_a": ["AAPL", "AAPL"], "symbol_b": ["MSFT", "GOOGL"],
            "correlation": [value, value + 0.1], "regime": ["R"] * 2,
            "computation_date": [d] * 2, "cluster_id_a": [1, 1], "cluster_id_b": [1, 2],
        })

    def _setup(self, monkeypatch, tmp_path):
        import src.gold.cross_asset.history as hist
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path)
        for i, d in enumerate(self.WEEKS):
            hist.write_history_snapshot(self._corr(d, 0.1 * (i + 1)), "cross_asset_corr", d)
        _patch(monkeypatch, "GOLD_CROSS_ASSET_CORR_PATH",
               tmp_path / "cross_asset_corr" / "cross_asset_corr_*.parquet")

    def test_returns_latest_snapshot_not_concatenation(self, loader, monkeypatch, tmp_path):
        self._setup(monkeypatch, tmp_path)
        df = loader.get_cross_asset_corr(date(2026, 9, 30))
        assert df["computation_date"].unique().to_list() == [date(2026, 9, 20)]
        assert df.height == 2   # one snapshot's rows, not 6

    def test_mid_history_trade_date_picks_the_snapshot_in_force(self, loader, monkeypatch, tmp_path):
        self._setup(monkeypatch, tmp_path)
        df = loader.get_cross_asset_corr(date(2026, 9, 15))
        assert df["computation_date"].unique().to_list() == [date(2026, 9, 13)]
        assert df.filter(pl.col("symbol_b") == "MSFT")["correlation"][0] == pytest.approx(0.2)

    def test_later_snapshot_never_leaks_backward(self, loader, monkeypatch, tmp_path):
        self._setup(monkeypatch, tmp_path)
        # Day before the 20th: the 20th's values (0.3) must not be visible.
        df = loader.get_cross_asset_corr(date(2026, 9, 19))
        assert df["computation_date"].unique().to_list() == [date(2026, 9, 13)]

    def test_trade_date_on_computation_date_includes_it(self, loader, monkeypatch, tmp_path):
        self._setup(monkeypatch, tmp_path)
        df = loader.get_cross_asset_corr(date(2026, 9, 13))
        assert df["computation_date"].unique().to_list() == [date(2026, 9, 13)]

    def test_before_earliest_snapshot_is_empty(self, loader, monkeypatch, tmp_path):
        self._setup(monkeypatch, tmp_path)
        assert loader.get_cross_asset_corr(date(2026, 9, 5)).is_empty()

    def test_symbol_filter_applies_within_selected_snapshot(self, loader, monkeypatch, tmp_path):
        self._setup(monkeypatch, tmp_path)
        df = loader.get_cross_asset_corr(date(2026, 9, 15), symbol="GOOGL")
        assert df.height == 1
        assert df["computation_date"][0] == date(2026, 9, 13)

    def test_schema_drift_between_snapshots_does_not_break_reads(self, loader, monkeypatch, tmp_path):
        """A later run adding a column must not make older/newer snapshots
        unreadable together (union_by_name)."""
        import src.gold.cross_asset.history as hist
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path)
        old = self._corr(date(2026, 9, 6), 0.1)
        new = self._corr(date(2026, 9, 13), 0.2).with_columns(pl.lit("x").alias("new_col"))
        hist.write_history_snapshot(old, "cross_asset_corr", date(2026, 9, 6))
        hist.write_history_snapshot(new, "cross_asset_corr", date(2026, 9, 13))
        _patch(monkeypatch, "GOLD_CROSS_ASSET_CORR_PATH",
               tmp_path / "cross_asset_corr" / "cross_asset_corr_*.parquet")
        assert loader.get_cross_asset_corr(date(2026, 9, 14))["new_col"][0] == "x"
        assert loader.get_cross_asset_corr(date(2026, 9, 8)).height == 2   # old snapshot, new_col null

    def test_lead_lag_and_forecast_select_single_snapshot_too(self, loader, monkeypatch, tmp_path):
        import src.gold.cross_asset.history as hist
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path)
        for i, d in enumerate(self.WEEKS):
            hist.write_history_snapshot(pl.DataFrame({
                "leader": ["TWSE"], "follower": ["NVDA"], "optimal_lag_days": [1],
                "max_cross_corr": [0.1 * (i + 1)], "computation_date": [d],
            }), "lead_lag_matrix", d)
            hist.write_history_snapshot(pl.DataFrame({
                "symbol": ["AAPL", "AAPL"], "computation_date": [d, d],
                "horizon_days": [1, 2], "forecast_return": [0.001 * (i + 1)] * 2,
            }), "cross_asset_forecast", d)
        _patch(monkeypatch, "GOLD_LEAD_LAG_PATH", tmp_path / "lead_lag_matrix" / "lead_lag_matrix_*.parquet")
        _patch(monkeypatch, "GOLD_FORECAST_PATH", tmp_path / "cross_asset_forecast" / "cross_asset_forecast_*.parquet")

        ll = loader.get_lead_lag(date(2026, 9, 15))
        assert ll.height == 1 and ll["computation_date"][0] == date(2026, 9, 13)
        assert ll["max_cross_corr"][0] == pytest.approx(0.2)

        fc = loader.get_forecast("AAPL", date(2026, 9, 15))
        assert fc["horizon_days"].to_list() == [1, 2]            # one snapshot, ordered
        assert fc["computation_date"].unique().to_list() == [date(2026, 9, 13)]
        assert loader.get_forecast("AAPL", date(2026, 9, 15), horizon_days=2).height == 1
