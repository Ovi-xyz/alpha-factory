"""
tests/unit/test_cross_asset_history_adr050.py

GMI-PIT-001 follow-up (ADR-050 finding / RISK-35): additive point-in-time
history for the three weekly CrossAssetEngine outputs that are otherwise
OVERWRITTEN each run (src/gold/cross_asset/history.py).

Covers: the writer (layout, idempotence, non-blocking), the hook in each
module's run(), the property that motivated the whole design (the snapshot
files gold_screener joins stay single-snapshot while history accumulates),
and the writer<->PITDataLoader path contract.

tests/conftest.py already redirects HISTORY_ROOT to a tmp dir for every test;
tests here that need a specific root override it explicitly.
"""

from __future__ import annotations

from datetime import date
from fnmatch import fnmatch
from pathlib import Path

import polars as pl
import pytest

import src.backtest.pit_data as pit
import src.gold.cross_asset.correlation_module as corr_mod
import src.gold.cross_asset.forecast_module as fc_mod
import src.gold.cross_asset.history as hist
import src.gold.cross_asset.lead_lag_module as ll_mod


def _corr_frame(d: date, corr: float = 0.5) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol_a": ["AAPL"], "symbol_b": ["MSFT"], "correlation": [corr],
        "regime": ["RISK_ON"], "computation_date": [d],
        "cluster_id_a": [1], "cluster_id_b": [1],
    })


def _ll_frame(d: date) -> pl.DataFrame:
    return pl.DataFrame({
        "leader": ["TWSE"], "follower": ["NVDA"], "optimal_lag_days": [1],
        "bh_significant": [True], "max_cross_corr": [0.3], "computation_date": [d],
    })


def _fc_frame(d: date) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": ["AAPL"], "computation_date": [d], "horizon_days": [1],
        "forecast_return": [0.001], "stable": [True],
    })


class TestWriter:

    def test_layout(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path)
        assert hist.history_path("cross_asset_corr", date(2026, 9, 27)) == (
            tmp_path / "cross_asset_corr" / "cross_asset_corr_2026-09-27.parquet"
        )

    def test_writes_file_and_creates_parents(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path / "does" / "not" / "exist")
        hist.write_history_snapshot(_corr_frame(date(2026, 9, 27)), "cross_asset_corr", date(2026, 9, 27))
        assert hist.history_path("cross_asset_corr", date(2026, 9, 27)).exists()

    def test_same_date_replaces(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path)
        d = date(2026, 9, 27)
        hist.write_history_snapshot(_corr_frame(d, 0.1), "cross_asset_corr", d)
        hist.write_history_snapshot(_corr_frame(d, 0.9), "cross_asset_corr", d)
        df = pl.read_parquet(hist.history_path("cross_asset_corr", d))
        assert df.height == 1 and df["correlation"][0] == pytest.approx(0.9)

    def test_distinct_dates_are_distinct_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path)
        for d in (date(2026, 9, 20), date(2026, 9, 27)):
            hist.write_history_snapshot(_corr_frame(d), "cross_asset_corr", d)
        assert len(list((tmp_path / "cross_asset_corr").glob("*.parquet"))) == 2

    def test_write_failure_never_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path)
        monkeypatch.setattr(hist, "atomic_write_parquet",
                            lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
        hist.write_history_snapshot(_corr_frame(date(2026, 9, 27)), "cross_asset_corr", date(2026, 9, 27))


class TestModuleHooks:
    """run() of each weekly module must write BOTH its snapshot and a
    history file for that run_date."""

    def test_correlation(self, tmp_path, monkeypatch):
        d = date(2026, 9, 27)
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path / "h")
        monkeypatch.setattr(corr_mod, "GOLD_CROSS_ASSET_PATH", tmp_path / "s")
        monkeypatch.setattr(corr_mod, "CROSS_ASSET_CORR_PATH", tmp_path / "s" / "cross_asset_corr.parquet")
        monkeypatch.setattr(corr_mod, "write_legacy_correlation_clusters", lambda df: None)
        monkeypatch.setattr(corr_mod.CorrelationModule, "compute", lambda self, rd: _corr_frame(rd))
        corr_mod.run(d)
        assert (tmp_path / "s" / "cross_asset_corr.parquet").exists()
        assert hist.history_path(hist.CORR_HISTORY_NAME, d).exists()

    def test_lead_lag(self, tmp_path, monkeypatch):
        d = date(2026, 9, 27)
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path / "h")
        monkeypatch.setattr(ll_mod, "GOLD_CROSS_ASSET_PATH", tmp_path / "s")
        monkeypatch.setattr(ll_mod, "LEAD_LAG_STORE_PATH", tmp_path / "s" / "lead_lag_matrix.parquet")
        monkeypatch.setattr(ll_mod.LeadLagModule, "compute", lambda self, rd: _ll_frame(rd))
        ll_mod.run(d)
        assert (tmp_path / "s" / "lead_lag_matrix.parquet").exists()
        assert hist.history_path(hist.LEAD_LAG_HISTORY_NAME, d).exists()

    def test_forecast(self, tmp_path, monkeypatch):
        d = date(2026, 9, 27)
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path / "h")
        monkeypatch.setattr(fc_mod, "GOLD_CROSS_ASSET_PATH", tmp_path / "s")
        monkeypatch.setattr(fc_mod, "FORECAST_STORE_PATH", tmp_path / "s" / "cross_asset_forecast.parquet")
        monkeypatch.setattr(fc_mod.ForecastModule, "compute", lambda self, rd: _fc_frame(rd))
        fc_mod.run(d)
        assert (tmp_path / "s" / "cross_asset_forecast.parquet").exists()
        assert hist.history_path(hist.FORECAST_HISTORY_NAME, d).exists()

    def test_no_history_written_when_nothing_computed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path / "h")
        monkeypatch.setattr(ll_mod.LeadLagModule, "compute", lambda self, rd: None)
        ll_mod.run(date(2026, 9, 27))
        assert not (tmp_path / "h").exists()

    def test_history_failure_does_not_fail_the_job_or_lose_the_snapshot(self, tmp_path, monkeypatch):
        d = date(2026, 9, 27)
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path / "h")
        monkeypatch.setattr(hist, "atomic_write_parquet",
                            lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
        monkeypatch.setattr(ll_mod, "GOLD_CROSS_ASSET_PATH", tmp_path / "s")
        monkeypatch.setattr(ll_mod, "LEAD_LAG_STORE_PATH", tmp_path / "s" / "lead_lag_matrix.parquet")
        monkeypatch.setattr(ll_mod.LeadLagModule, "compute", lambda self, rd: _ll_frame(rd))
        # snapshot write goes through ll_mod's OWN atomic_write_parquet reference
        # (untouched) — only the history module's is broken.
        ll_mod.run(d)
        assert (tmp_path / "s" / "lead_lag_matrix.parquet").exists()


class TestSnapshotFilesStayScreenerSafe:
    """The reason history is a SEPARATE store: gold_screener LEFT JOINs the
    snapshot file whole (ON symbol = follower / symbol = symbol, no
    computation_date filter) — it must never see more than one week."""

    def test_two_weekly_runs_snapshot_keeps_latest_only_history_keeps_both(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path / "h")
        monkeypatch.setattr(ll_mod, "GOLD_CROSS_ASSET_PATH", tmp_path / "s")
        snap = tmp_path / "s" / "lead_lag_matrix.parquet"
        monkeypatch.setattr(ll_mod, "LEAD_LAG_STORE_PATH", snap)
        monkeypatch.setattr(ll_mod.LeadLagModule, "compute", lambda self, rd: _ll_frame(rd))
        w1, w2 = date(2026, 9, 20), date(2026, 9, 27)
        ll_mod.run(w1)
        ll_mod.run(w2)
        snapshot = pl.read_parquet(snap)
        assert snapshot["computation_date"].unique().to_list() == [w2]   # unchanged contract
        assert len(list((tmp_path / "h" / "lead_lag_matrix").glob("*.parquet"))) == 2


class TestWriterReaderPathContract:
    """Guards against the writer and PITDataLoader drifting apart."""

    @pytest.mark.parametrize("name,pit_const", [
        ("cross_asset_corr",     "GOLD_CROSS_ASSET_CORR_PATH"),
        ("lead_lag_matrix",      "GOLD_LEAD_LAG_PATH"),
        ("cross_asset_forecast", "GOLD_FORECAST_PATH"),
    ])
    def test_reader_glob_matches_writer_output(self, name, pit_const, monkeypatch):
        monkeypatch.setattr(hist, "HISTORY_ROOT", Path("data/gold/cross_asset/history"))
        written = str(hist.history_path(name, date(2026, 9, 27)))
        assert fnmatch(written, getattr(pit, pit_const)), (written, getattr(pit, pit_const))
