"""tests/unit/test_runner.py — Runner CLI unit tests"""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestRunnerImports:

    def test_runner_importable(self):
        from src import runner
        assert runner is not None

    def test_run_job_importable(self):
        from src.runner import run_job
        assert callable(run_job)

    def test_run_all_importable(self):
        from src.runner import run_all
        assert callable(run_all)

    def test_parse_args_importable(self):
        from src.runner import parse_args
        assert callable(parse_args)


class TestParseArgs:

    def _parse(self, args: list[str]):
        from src.runner import parse_args
        with patch("sys.argv", ["runner.py"] + args):
            return parse_args()

    def test_job_flag(self):
        args = self._parse(["--job", "bronze_ohlcv_daily"])
        assert args.job == "bronze_ohlcv_daily"

    def test_force_flag(self):
        args = self._parse(["--job", "gold_regime", "--force"])
        assert args.force is True

    def test_date_flag(self):
        args = self._parse(["--job", "all", "--date", "2026-05-01"])
        assert args.date == "2026-05-01"

    def test_list_flag(self):
        args = self._parse(["--list"])
        assert args.list is True

    def test_status_flag(self):
        args = self._parse(["--status"])
        assert args.status is True

    def test_reset_flag(self):
        args = self._parse(["--reset", "gold_regime"])
        assert args.reset == "gold_regime"

    def test_reset_all_flag(self):
        args = self._parse(["--reset-all"])
        assert args.reset_all is True

    def test_dashboard_flag(self):
        args = self._parse(["--dashboard"])
        assert args.dashboard is True

    def test_views_flag(self):
        args = self._parse(["--views"])
        assert args.views is True

    def test_default_no_force(self):
        args = self._parse(["--job", "bronze_ohlcv_daily"])
        assert args.force is False

    def test_default_no_date(self):
        args = self._parse(["--job", "bronze_ohlcv_daily"])
        assert args.date is None


class TestRunJob:

    def test_unknown_job_raises_system_exit(self, monkeypatch, tmp_path):
        """Unknown job name → sys.exit(1)."""
        from src.runner import run_job
        monkeypatch.setattr("src.runner.guard.sentinel_dir", tmp_path / "s")
        (tmp_path / "s").mkdir()

        with pytest.raises(SystemExit) as exc:
            run_job("nonexistent_job_xyz", force=True)
        assert exc.value.code == 1

    def test_dependency_not_met_raises_system_exit(self, monkeypatch, tmp_path):
        """Missing dependency without --force → sys.exit(1)."""
        from src.runner import run_job
        from src.scheduler.dependency_guard import DependencyGuard

        sentinel_dir = tmp_path / ".sentinels"
        sentinel_dir.mkdir()
        monkeypatch.setattr("src.runner.guard",
                            DependencyGuard(sentinel_dir=sentinel_dir))

        # silver_ohlcv depends on bronze_ohlcv_daily — sentinel not written
        with pytest.raises(SystemExit) as exc:
            run_job("silver_ohlcv", force=False,
                    run_date=date(2025, 1, 2))
        assert exc.value.code == 1

    def test_force_bypasses_dependency(self, monkeypatch, tmp_path):
        """--force skips dependency check."""
        from src.runner import run_job
        from src.scheduler.dependency_guard import DependencyGuard
        from src.scheduler.job_registry import JOB_REGISTRY

        sentinel_dir = tmp_path / ".sentinels"
        sentinel_dir.mkdir()
        monkeypatch.setattr("src.runner.guard",
                            DependencyGuard(sentinel_dir=sentinel_dir))

        # Mock the job function so it doesn't actually run
        called = []
        original_fn = JOB_REGISTRY["silver_ohlcv"]["fn"]
        JOB_REGISTRY["silver_ohlcv"]["fn"] = lambda d: called.append(d)
        try:
            run_job("silver_ohlcv", force=True, run_date=date(2025, 1, 2))
        finally:
            JOB_REGISTRY["silver_ohlcv"]["fn"] = original_fn

        assert len(called) == 1

    def test_successful_job_writes_sentinel(self, monkeypatch, tmp_path):
        """Successful job execution writes .done sentinel."""
        from src.runner import run_job
        from src.scheduler.dependency_guard import DependencyGuard
        from src.scheduler.job_registry import JOB_REGISTRY

        sentinel_dir = tmp_path / ".sentinels"
        sentinel_dir.mkdir()
        guard = DependencyGuard(sentinel_dir=sentinel_dir)
        monkeypatch.setattr("src.runner.guard", guard)

        JOB_REGISTRY["health_report"]["fn"] = lambda d: None   # Mock fn
        run_date = date(2025, 3, 15)

        # health_report has gold_screener as dep — force it
        run_job("health_report", force=True, run_date=run_date)
        assert guard.is_done("health_report", run_date)


class TestScheduleGuardInRunner:

    def test_schedule_constrained_job_skipped_on_wrong_day(self, monkeypatch, tmp_path):
        """EIA job skipped when it's not Wednesday."""
        from src.runner import run_job
        from src.scheduler.dependency_guard import DependencyGuard

        sentinel_dir = tmp_path / ".sentinels"
        sentinel_dir.mkdir()
        monkeypatch.setattr("src.runner.guard",
                            DependencyGuard(sentinel_dir=sentinel_dir))

        # Monday = weekday 0, EIA needs Wednesday = 2
        monday = date(2025, 1, 6)
        assert monday.weekday() == 0

        # Should skip without error (log warning, return)
        run_job("bronze_eia", force=False, run_date=monday)

        # Sentinel should NOT be written (job was skipped, not completed)
        guard = DependencyGuard(sentinel_dir=sentinel_dir)
        assert not guard.is_done("bronze_eia", monday)
