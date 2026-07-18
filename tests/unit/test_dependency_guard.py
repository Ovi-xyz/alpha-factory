"""tests/unit/test_dependency_guard.py — DependencyGuard file sentinel tests"""

from datetime import date, timedelta
from pathlib import Path

import pytest

from src.scheduler.dependency_guard import DependencyGuard


@pytest.fixture
def guard(tmp_path):
    return DependencyGuard(sentinel_dir=tmp_path / "sentinels")


class TestDependencyGuard:

    def test_is_done_initially_false(self, guard):
        assert not guard.is_done("bronze_ohlcv_daily", date(2025, 1, 15))

    def test_mark_done_creates_sentinel(self, guard, tmp_path):
        run_date = date(2025, 1, 15)
        guard.mark_done("test_job", run_date)
        assert guard.is_done("test_job", run_date)

    def test_different_dates_are_independent(self, guard):
        d1 = date(2025, 1, 15)
        d2 = date(2025, 1, 16)
        guard.mark_done("job_a", d1)
        assert     guard.is_done("job_a", d1)
        assert not guard.is_done("job_a", d2)

    def test_check_dependencies_empty_when_all_done(self, guard):
        run_date = date(2025, 1, 15)
        guard.mark_done("dep_1", run_date)
        guard.mark_done("dep_2", run_date)
        missing = guard.check_dependencies(["dep_1", "dep_2"], run_date)
        assert missing == []

    def test_check_dependencies_returns_missing(self, guard):
        run_date = date(2025, 1, 15)
        guard.mark_done("dep_1", run_date)
        missing = guard.check_dependencies(["dep_1", "dep_2"], run_date)
        assert "dep_2" in missing
        assert "dep_1" not in missing

    def test_reset_job_removes_sentinel(self, guard):
        run_date = date(2025, 1, 15)
        guard.mark_done("job_x", run_date)
        assert guard.is_done("job_x", run_date)
        guard.reset_job("job_x", run_date)
        assert not guard.is_done("job_x", run_date)

    def test_reset_all_clears_date(self, guard):
        run_date = date(2025, 1, 15)
        guard.mark_done("job_a", run_date)
        guard.mark_done("job_b", run_date)
        count = guard.reset_all(run_date)
        assert count == 2
        assert not guard.is_done("job_a", run_date)
        assert not guard.is_done("job_b", run_date)

    def test_reset_all_does_not_affect_other_dates(self, guard):
        d1 = date(2025, 1, 15)
        d2 = date(2025, 1, 16)
        guard.mark_done("job_a", d1)
        guard.mark_done("job_a", d2)
        guard.reset_all(d1)
        # d2 sentinel should remain
        assert guard.is_done("job_a", d2)

    def test_get_all_statuses(self, guard):
        run_date = date(2025, 1, 15)
        guard.mark_done("job_a", run_date)
        statuses = guard.get_all_statuses(["job_a", "job_b"], run_date)
        assert statuses["job_a"] is True
        assert statuses["job_b"] is False

    def test_sentinel_content_has_job_name(self, guard):
        """Sentinel file should contain job name for debugging."""
        run_date = date(2025, 1, 15)
        guard.mark_done("bronze_ohlcv_daily", run_date)
        sentinel = guard._sentinel_path("bronze_ohlcv_daily", run_date)
        content  = sentinel.read_text()
        assert "bronze_ohlcv_daily" in content


class TestIsDoneWithin:
    """
    FIX NEW-1 [BLOCKING] (audit_v1_7_3_uncovered_findings.docx §2, Opsi A):
    staleness-window lookup for cross-cadence dependencies.
    """

    def test_zero_max_age_equals_is_done(self, guard):
        """max_age_days=0 must behave identically to exact-date is_done()."""
        run_date = date(2025, 6, 23)   # Tuesday
        guard.mark_done("silver_macro", date(2025, 6, 22))   # Sunday — 1 day prior
        assert not guard.is_done_within("silver_macro", run_date, 0)
        assert guard.is_done_within("silver_macro", run_date, 1)

    def test_finds_sentinel_within_window(self, guard):
        """Sentinel from N days ago is found within an N+ day window."""
        sunday  = date(2025, 6, 22)
        guard.mark_done("silver_macro", sunday)
        for offset, weekday_name in enumerate(
            ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        ):
            run_date = sunday + timedelta(days=offset)
            assert guard.is_done_within("silver_macro", run_date, 7), (
                f"Expected sentinel from {sunday} to be found on {run_date}"
                f" ({weekday_name}, offset={offset}) with max_age_days=7"
            )

    def test_does_not_find_sentinel_outside_window(self, guard):
        """Sentinel older than max_age_days must NOT be considered done."""
        sunday   = date(2025, 6, 15)   # Two Sundays ago relative to run_date
        run_date = date(2025, 6, 23)   # 8 days after sunday
        guard.mark_done("silver_macro", sunday)
        assert not guard.is_done_within("silver_macro", run_date, 7)

    def test_does_not_look_forward(self, guard):
        """is_done_within must only search backward, never forward in time."""
        run_date = date(2025, 6, 17)
        guard.mark_done("silver_macro", date(2025, 6, 22))   # 5 days in the future
        assert not guard.is_done_within("silver_macro", run_date, 7)

    def test_negative_max_age_raises(self, guard):
        with pytest.raises(ValueError):
            guard.is_done_within("silver_macro", date(2025, 6, 23), -1)

    def test_no_sentinel_anywhere_returns_false(self, guard):
        assert not guard.is_done_within("silver_macro", date(2025, 6, 23), 30)


class TestCheckDependenciesStaleTolerance:
    """
    FIX NEW-1: check_dependencies() with the optional stale_tolerance parameter.
    """

    def test_no_stale_tolerance_preserves_exact_match_behavior(self, guard):
        """Backward compat: omitting stale_tolerance behaves exactly like before."""
        sunday   = date(2025, 6, 22)
        tuesday  = date(2025, 6, 24)
        guard.mark_done("silver_macro", sunday)
        missing = guard.check_dependencies(["silver_macro"], tuesday)
        assert "silver_macro" in missing

    def test_stale_tolerance_allows_carried_forward_sentinel(self, guard):
        """A dependency configured with stale_tolerance accepts an older sentinel."""
        sunday  = date(2025, 6, 22)
        guard.mark_done("silver_macro", sunday)
        guard.mark_done("silver_ohlcv", date(2025, 6, 24))   # same-day dep
        missing = guard.check_dependencies(
            ["silver_ohlcv", "silver_macro"],
            date(2025, 6, 24),   # Tuesday
            stale_tolerance={"silver_macro": 7},
        )
        assert missing == []

    def test_stale_tolerance_does_not_relax_other_deps(self, guard):
        """stale_tolerance only applies to the dependency it names — other deps
        in the same call still require exact-date match."""
        guard.mark_done("silver_macro", date(2025, 6, 22))
        # silver_ohlcv intentionally NOT marked done for run_date
        missing = guard.check_dependencies(
            ["silver_ohlcv", "silver_macro"],
            date(2025, 6, 24),
            stale_tolerance={"silver_macro": 7},
        )
        assert missing == ["silver_ohlcv"]

    def test_stale_tolerance_exceeded_still_reports_missing(self, guard):
        """A dependency older than its configured tolerance is still missing."""
        guard.mark_done("silver_macro", date(2025, 6, 1))   # 23 days prior
        missing = guard.check_dependencies(
            ["silver_macro"],
            date(2025, 6, 24),
            stale_tolerance={"silver_macro": 7},
        )
        assert missing == ["silver_macro"]
