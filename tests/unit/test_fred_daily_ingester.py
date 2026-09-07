"""
tests/unit/test_fred_daily_ingester.py

NEW (FIX GMI-FRED-DAILY-01, Ovi, 7 Sep 2026) — src/bronze/fred_daily_ingester.py
delegates 9 orphaned cadence: daily FRED series (VIXCLS, DEXUSEU,
BAMLH0A0HYM2, BAMLC0A0CM, DCOILWTICO, DEXJPUS, DFF, T5YIE, T10YIE) to
FREDIngester with an explicit series_filter — the same delegate-wrapper
shape test_treasury_ingester.py already covers for TREASURY_FRED_SERIES.

Tests validate:
    1. Delegation to FREDIngester with the correct series_filter
    2. The filter's exact contents — all 9 expected series, incl. the 3
       regime inputs by name (regression guard against silent list drift)
    3. No overlap with TREASURY_FRED_SERIES (the two modules must not
       double-fetch the same series via two different daily jobs)
    4. Exception from the delegate is caught and logged, not propagated
    5. Module-level run() is the entry point job_registry.py's
       _bronze_fred_daily wrapper calls
    6. Architectural invariant: no write()/write_macro() call anywhere in
       this module's body — it only ever delegates (GD §17.3), same
       invariant test_treasury_ingester.py enforces for TreasuryIngester
"""
from __future__ import annotations

import ast
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.bronze.fred_daily_ingester import FRED_DAILY_SERIES, run
from src.bronze.treasury_ingester import TREASURY_FRED_SERIES


@pytest.fixture
def run_date() -> date:
    return date(2026, 9, 7)  # a Monday — incidental, no weekday logic in this module itself


# ── Successful Delegation ────────────────────────────────────────────────────

class TestRunDelegatesToFredIngester:

    def test_delegates_with_correct_series_filter(self, run_date):
        mock_instance = MagicMock()
        with patch(
            "src.bronze.fred_ingester.FREDIngester", return_value=mock_instance
        ) as mock_fred_cls:
            run(run_date)
            mock_fred_cls.assert_called_once_with()
            mock_instance.run.assert_called_once_with(
                run_date, series_filter=FRED_DAILY_SERIES
            )

    def test_series_filter_passed_is_the_canonical_list_object_contents(self, run_date):
        """Regression guard: must pass the exact 9-series list, not a subset
        or a re-derived one that could silently drift from FRED_DAILY_SERIES."""
        mock_instance = MagicMock()
        with patch(
            "src.bronze.fred_ingester.FREDIngester", return_value=mock_instance
        ):
            run(run_date)
            passed_series = mock_instance.run.call_args.kwargs["series_filter"]
            assert passed_series == FRED_DAILY_SERIES


# ── Series List Contents — Regression Guards ─────────────────────────────────

class TestFredDailySeriesContents:

    def test_contains_exactly_nine_series(self):
        assert len(FRED_DAILY_SERIES) == 9

    def test_contains_all_three_regime_inputs(self):
        """VIXCLS (vix_proxy), DEXUSEU (dxy_proxy), BAMLH0A0HYM2
        (credit_spread) are the 3 of gold_regime's 7 macro regime inputs
        that were silently starved by the bug this module fixes."""
        for regime_input_series in ("VIXCLS", "DEXUSEU", "BAMLH0A0HYM2"):
            assert regime_input_series in FRED_DAILY_SERIES

    def test_contains_all_nine_expected_series(self):
        assert set(FRED_DAILY_SERIES) == {
            "VIXCLS", "DEXUSEU", "BAMLH0A0HYM2", "BAMLC0A0CM",
            "DCOILWTICO", "DEXJPUS", "DFF", "T5YIE", "T10YIE",
        }

    def test_no_overlap_with_treasury_series(self):
        """These two daily-cadence modules must own disjoint series sets —
        an overlap would mean the same series gets fetched (and idempotency-
        checked) twice per day via two independent jobs."""
        overlap = set(FRED_DAILY_SERIES) & set(TREASURY_FRED_SERIES)
        assert overlap == set(), f"Series double-owned by both daily jobs: {overlap}"


# ── Exception Handling ────────────────────────────────────────────────────────

class TestRunHandlesDelegateException:

    def test_delegate_exception_is_caught_not_propagated(self, run_date):
        mock_instance = MagicMock()
        mock_instance.run.side_effect = ConnectionError("FRED API timeout")
        with patch(
            "src.bronze.fred_ingester.FREDIngester", return_value=mock_instance
        ):
            run(run_date)  # must not raise despite delegate failure


# ── Module-Level run() Entry Point (job_registry.py wiring) ──────────────────

class TestJobRegistryWiring:

    def test_job_registry_entry_delegates_to_module_run(self, run_date):
        from src.scheduler.job_registry import JOB_REGISTRY
        assert "bronze_fred_daily" in JOB_REGISTRY
        with patch("src.bronze.fred_daily_ingester.run") as mock_run:
            JOB_REGISTRY["bronze_fred_daily"]["fn"](run_date)
            mock_run.assert_called_once_with(run_date)

    def test_job_registry_entry_has_no_dependencies(self):
        """GD §17.3.1: Bronze ingesters are independent of one another."""
        from src.scheduler.job_registry import JOB_REGISTRY
        assert JOB_REGISTRY["bronze_fred_daily"]["depends_on"] == []

    def test_job_registry_entry_has_no_schedule_guard(self):
        """Must run every day — fred_ingester.py's own per-series weekday
        check is the correct gate here, not a job-level run_on_weekdays
        constraint (that was the bug's root cause for bronze_macro_weekly)."""
        from src.scheduler.job_registry import JOB_REGISTRY
        assert "run_on_weekdays" not in JOB_REGISTRY["bronze_fred_daily"]

    def test_in_daily_sequence(self):
        from src.scheduler.job_registry import DAILY_SEQUENCE
        assert "bronze_fred_daily" in DAILY_SEQUENCE

    def test_not_in_weekly_only_portion(self):
        """Must not be double-scheduled: WEEKLY_SEQUENCE = weekly-only jobs
        + DAILY_SEQUENCE, so appearing once (via DAILY_SEQUENCE) is correct;
        it must not also appear in the weekly-only prefix."""
        from src.scheduler.job_registry import WEEKLY_SEQUENCE, DAILY_SEQUENCE
        weekly_only_prefix = WEEKLY_SEQUENCE[: len(WEEKLY_SEQUENCE) - len(DAILY_SEQUENCE)]
        assert "bronze_fred_daily" not in weekly_only_prefix

    def test_included_in_bronze_layer_scope(self):
        """`--job bronze` (GMI-JR-003) must pick this job up."""
        from src.scheduler.job_registry import LAYER_JOB_NAMES
        assert "bronze_fred_daily" in LAYER_JOB_NAMES["bronze"]


# ── Architectural Invariants (GD §17.3 — delegation-only) ────────────────────

class TestArchitecturalInvariants:

    def test_never_calls_write_or_write_macro(self):
        """No write()/write_macro() call anywhere in the module body — all
        writes are performed by the delegated FREDIngester (GD §17.3), same
        invariant TI-1 already enforces for TreasuryIngester."""
        import src.bronze.fred_daily_ingester as module
        source = Path(module.__file__).read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in ("write", "write_macro"), (
                    "fred_daily_ingester calls a write method directly — "
                    "violates delegation-only design, GD §17.3"
                )

    def test_syntax_valid(self):
        import src.bronze.fred_daily_ingester as module
        ast.parse(Path(module.__file__).read_text())
