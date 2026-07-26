"""
tests/unit/test_pipeline_dashboard.py — Pipeline health dashboard
real-function coverage. Decision C (GMI_Decision_Document_v5.docx §3,
tranche item #7 — "monitoring/reporting, not data-correctness-critical").
Previously zero test coverage for this module.

This module builds every path as a hardcoded, CWD-relative glob string
(no injectable base-path constants) — a display-only diagnostic, lower
individual stakes per Decision C's own sequencing rationale. Isolation
here uses monkeypatch.chdir(tmp_path) rather than a source refactor, since
promoting ~15 report-only globs to constants is a larger, separately-
scoped change for marginal benefit on a tool whose failure mode is "shows
no data," not data corruption. Flagged for a future pass, not done here.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from unittest.mock import patch

import polars as pl
import pytest

import src.utils.pipeline_dashboard as dash


def _warm_instrument_loader_cache() -> None:
    """get_loader() is an lru_cache singleton reading config/instruments_*
    relative to CWD (the same class of hardcode noted in the module
    docstring above). Populating the cache once, before any test chdirs
    into an isolated tmp_path, means later calls return the cached
    instance without touching disk again."""
    from src.config.instrument_loader import get_loader
    get_loader()


class TestPureHelpers:

    def test_bar_empty(self):
        assert dash._bar(0.0, width=10) == "░" * 10

    def test_bar_full(self):
        assert dash._bar(1.0, width=10) == "█" * 10

    def test_bar_half(self):
        assert dash._bar(0.5, width=10) == "█" * 5 + "░" * 5

    def test_c_plain_when_not_tty(self):
        with patch("sys.stdout.isatty", return_value=False):
            assert dash._c("hello", dash.RED) == "hello"

    def test_c_wrapped_when_tty(self):
        with patch("sys.stdout.isatty", return_value=True):
            result = dash._c("hello", dash.RED)
        assert result.startswith(dash.RED)
        assert "hello" in result


class TestSectionJobStatus:

    def _registry(self):
        return {
            "bronze_a": {"layer": "bronze", "est_minutes": 5},
            "silver_b": {"layer": "silver", "est_minutes": 10},
        }

    def test_no_sentinels_all_pending(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        with patch("src.scheduler.job_registry.JOB_REGISTRY", self._registry()), \
             patch("src.scheduler.job_registry.PIPELINE_SEQUENCE", ["bronze_a", "silver_b"]):
            dash._section_job_status(date(2026, 6, 1))
        out = capsys.readouterr().out
        assert "0/2 completed" in out
        assert "PENDING" in out

    def test_some_sentinels_done(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        run_date = date(2026, 6, 1)
        sentinel_dir = tmp_path / "data" / ".sentinels"
        sentinel_dir.mkdir(parents=True)
        (sentinel_dir / f"bronze_a_{run_date.isoformat()}.done").write_text("done")
        with patch("src.scheduler.job_registry.JOB_REGISTRY", self._registry()), \
             patch("src.scheduler.job_registry.PIPELINE_SEQUENCE", ["bronze_a", "silver_b"]):
            dash._section_job_status(run_date)
        out = capsys.readouterr().out
        assert "1/2 completed" in out
        assert "DONE" in out

    def test_all_sentinels_done_green_bar(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        run_date = date(2026, 6, 1)
        sentinel_dir = tmp_path / "data" / ".sentinels"
        sentinel_dir.mkdir(parents=True)
        for job in ["bronze_a", "silver_b"]:
            (sentinel_dir / f"{job}_{run_date.isoformat()}.done").write_text("done")
        with patch("src.scheduler.job_registry.JOB_REGISTRY", self._registry()), \
             patch("src.scheduler.job_registry.PIPELINE_SEQUENCE", ["bronze_a", "silver_b"]):
            dash._section_job_status(run_date)
        out = capsys.readouterr().out
        assert "2/2 completed (100%)" in out

    def test_job_outside_pipeline_sequence_still_listed(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        registry = {**self._registry(), "extra_job": {"layer": "util", "est_minutes": 1}}
        with patch("src.scheduler.job_registry.JOB_REGISTRY", registry), \
             patch("src.scheduler.job_registry.PIPELINE_SEQUENCE", ["bronze_a", "silver_b"]):
            dash._section_job_status(date(2026, 6, 1))
        out = capsys.readouterr().out
        assert "extra_job" in out


class TestSectionLayerCoverage:

    def test_no_data_anywhere(self, tmp_path, monkeypatch, capsys):
        _warm_instrument_loader_cache()
        monkeypatch.chdir(tmp_path)
        dash._section_layer_coverage()
        out = capsys.readouterr().out
        assert "no data yet" in out

    def test_bronze_files_present_counted(self, tmp_path, monkeypatch, capsys):
        _warm_instrument_loader_cache()
        monkeypatch.chdir(tmp_path)
        d = tmp_path / "data" / "bronze" / "market" / "ohlcv" / "us_stocks" / "symbol=AAPL"
        d.mkdir(parents=True)
        (d / "AAPL_raw.parquet").write_bytes(b"x" * 1024)
        dash._section_layer_coverage()
        out = capsys.readouterr().out
        assert "1 files" in out


class TestSectionStorage:

    def test_disk_usage_ok_green(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        fake_usage = type("U", (), {
            "free": 300 * 1024**3, "total": 500 * 1024**3, "used": 200 * 1024**3,
        })()
        with patch("shutil.disk_usage", return_value=fake_usage):
            dash._section_storage()
        out = capsys.readouterr().out
        assert "free" in out
        assert "ALERT" not in out

    def test_disk_usage_red_alert(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        fake_usage = type("U", (), {
            "free": 50 * 1024**3, "total": 500 * 1024**3, "used": 450 * 1024**3,
        })()
        with patch("shutil.disk_usage", return_value=fake_usage):
            dash._section_storage()
        out = capsys.readouterr().out
        assert "ALERT" in out

    def test_disk_usage_amber_warning(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        fake_usage = type("U", (), {
            "free": 100 * 1024**3, "total": 500 * 1024**3, "used": 400 * 1024**3,
        })()
        with patch("shutil.disk_usage", return_value=fake_usage):
            dash._section_storage()
        out = capsys.readouterr().out
        assert "Warning" in out

    def test_per_layer_directory_sizes_summed(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        d = tmp_path / "data" / "bronze"
        d.mkdir(parents=True)
        (d / "f.parquet").write_bytes(b"x" * 2048)
        fake_usage = type("U", (), {
            "free": 300 * 1024**3, "total": 500 * 1024**3, "used": 200 * 1024**3,
        })()
        with patch("shutil.disk_usage", return_value=fake_usage):
            dash._section_storage()
        out = capsys.readouterr().out
        assert "data/bronze" in out

    def test_exception_caught_and_reported(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        with patch("shutil.disk_usage", side_effect=OSError("no such mount")):
            dash._section_storage()
        out = capsys.readouterr().out
        assert "Storage check failed" in out


class TestSectionRecentFailures:

    def test_no_progress_db_yet(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        dash._section_recent_failures(date(2026, 6, 1))
        out = capsys.readouterr().out
        assert "No progress database yet" in out

    def test_no_failures_in_window(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        db_dir = tmp_path / "data" / "health"
        db_dir.mkdir(parents=True)
        con = sqlite3.connect(db_dir / "progress.db")
        con.execute(
            "CREATE TABLE symbol_progress (job_name TEXT, run_date TEXT, symbol TEXT, "
            "timeframe TEXT, status TEXT, error_msg TEXT, ts TEXT)"
        )
        con.commit(); con.close()
        dash._section_recent_failures(date(2026, 6, 1))
        out = capsys.readouterr().out
        assert "No failures in last 7 days" in out

    def test_failures_within_window_listed(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        db_dir = tmp_path / "data" / "health"
        db_dir.mkdir(parents=True)
        con = sqlite3.connect(db_dir / "progress.db")
        con.execute(
            "CREATE TABLE symbol_progress (job_name TEXT, run_date TEXT, symbol TEXT, "
            "timeframe TEXT, status TEXT, error_msg TEXT, ts TEXT)"
        )
        con.execute(
            "INSERT INTO symbol_progress VALUES (?,?,?,?,?,?,?)",
            ("gold_signals", "2026-05-30", "AAPL", "1D", "failed", "boom", "2026-05-30T10:00:00"),
        )
        con.commit(); con.close()
        dash._section_recent_failures(date(2026, 6, 1))
        out = capsys.readouterr().out
        assert "1 failures in last 7 days" in out
        assert "AAPL" in out
        assert "boom" in out

    def test_corrupt_db_exception_caught(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        db_dir = tmp_path / "data" / "health"
        db_dir.mkdir(parents=True)
        (db_dir / "progress.db").write_text("not a sqlite file")
        dash._section_recent_failures(date(2026, 6, 1))
        out = capsys.readouterr().out
        assert "Progress DB read failed" in out


class TestSectionDataFreshness:

    def test_no_files_found(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        dash._section_data_freshness()
        out = capsys.readouterr().out
        assert "not found" in out

    def test_recent_file_green(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        d = tmp_path / "data" / "gold" / "macro"
        d.mkdir(parents=True)
        (d / "regime_store.parquet").write_bytes(b"x")
        dash._section_data_freshness()
        out = capsys.readouterr().out
        assert "today" in out


class TestRenderDashboardSmoke:
    def test_full_render_does_not_raise(self, tmp_path, monkeypatch, capsys):
        _warm_instrument_loader_cache()
        monkeypatch.chdir(tmp_path)
        with patch("src.scheduler.job_registry.JOB_REGISTRY", {}), \
             patch("src.scheduler.job_registry.PIPELINE_SEQUENCE", []):
            dash.render_dashboard(date(2026, 6, 1))
        out = capsys.readouterr().out
        assert "PIPELINE HEALTH DASHBOARD" in out


class TestMainEntryPoint:
    def test_main_defaults_to_today(self, monkeypatch):
        with patch("sys.argv", ["pipeline_dashboard"]), \
             patch.object(dash, "render_dashboard") as mock_render:
            dash.main()
            mock_render.assert_called_once_with(date.today())

    def test_main_uses_date_override(self, monkeypatch):
        with patch("sys.argv", ["pipeline_dashboard", "--date", "2026-05-22"]), \
             patch.object(dash, "render_dashboard") as mock_render:
            dash.main()
            mock_render.assert_called_once_with(date(2026, 5, 22))
