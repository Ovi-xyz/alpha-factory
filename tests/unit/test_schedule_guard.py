"""tests/unit/test_schedule_guard.py — G5 schedule framework test suite"""

from datetime import date

import pytest

from src.scheduler.job_registry import _passes_schedule


class TestPassesSchedule:

    def test_no_constraints_always_passes(self):
        """Daily job — no schedule constraint → always True."""
        job = {"description": "daily job"}
        assert _passes_schedule(job, date(2025, 1, 15))
        assert _passes_schedule(job, date(2025, 6, 30))

    def test_eia_wednesday_only(self):
        """IDD §10.2: EIA on Wednesday → True; Friday → False."""
        job = {"run_on_weekdays": [2]}  # 2=Wednesday
        wednesday = date(2025, 1, 15)  # Wednesday
        friday    = date(2025, 1, 17)  # Friday
        assert wednesday.weekday() == 2
        assert _passes_schedule(job, wednesday) is True
        assert _passes_schedule(job, friday) is False

    def test_bls_cpi_day_of_month(self):
        """BLS CPI: run on day 10-15."""
        job = {"run_on_day_of_month": list(range(10, 16))}
        assert _passes_schedule(job, date(2025, 2, 10)) is True
        assert _passes_schedule(job, date(2025, 2, 15)) is True
        assert _passes_schedule(job, date(2025, 2, 9))  is False
        assert _passes_schedule(job, date(2025, 2, 16)) is False

    def test_bea_gdp_quarterly_months(self):
        """BEA GDP: months 1,4,7,10 AND last week of month."""
        job = {
            "run_on_months":       [1, 4, 7, 10],
            "run_on_day_of_month": list(range(25, 32)),
        }
        # January 28 → should pass
        assert _passes_schedule(job, date(2025, 1, 28)) is True
        # February 28 → wrong month
        assert _passes_schedule(job, date(2025, 2, 28)) is False
        # April 10 → right month, wrong day
        assert _passes_schedule(job, date(2025, 4, 10)) is False

    def test_nfp_first_friday(self):
        """BLS NFP: first Friday of month."""
        job = {"run_on_nth_weekday": {"n": 1, "weekday": 4}}  # 4=Friday

        # Find first Friday of February 2025 = Feb 7
        first_friday = date(2025, 2, 7)
        assert first_friday.weekday() == 4
        assert (first_friday.day - 1) // 7 + 1 == 1  # First occurrence

        second_friday = date(2025, 2, 14)
        assert _passes_schedule(job, first_friday) is True
        assert _passes_schedule(job, second_friday) is False

    def test_nfp_tolerance_day(self):
        """NFP with tolerance_days=1: also runs on Saturday after first Friday."""
        job = {"run_on_nth_weekday": {"n": 1, "weekday": 4, "tolerance_days": 1}}
        first_friday  = date(2025, 2, 7)
        first_saturday = date(2025, 2, 8)  # tolerance window
        second_friday = date(2025, 2, 14)

        assert _passes_schedule(job, first_friday)  is True
        assert _passes_schedule(job, first_saturday) is True
        assert _passes_schedule(job, second_friday) is False
