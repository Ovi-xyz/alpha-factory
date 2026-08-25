"""
tests/integration/test_runner_weekly_cadence.py — NEW-1 / NEW-2 empirical reproduction

FIX NEW-1 + NEW-2 [BLOCKING] (audit_v1_7_3_uncovered_findings.docx, Sections 2-3):
    This file reproduces the EXACT empirical scenario described in the audit
    (Section 2.3 "Reproduksi Empiris") — `runner.py --job all` stubbed across
    every job function, run across multiple consecutive days — to prove the
    fix resolves the documented crash and validates the two new Production
    Readiness gates introduced by the audit:

        GATE-N1: `python runner.py --job all` completes 13/13 jobs without
                 --force, on ANY run_date (not only Sunday).
        GATE-N2: gold_screener produces output via `--job all` without --force,
                 i.e. it is not permanently locked by an orphaned dependency.

    Methodology mirrors audit §0.1: every job['fn'] is stubbed to a no-op
    (isolating DependencyGuard/JOB_REGISTRY orchestration behavior from actual
    job content — same approach as the audit's own reproduction), run against
    a sandboxed sentinel directory.

Before the fix, this reproduction would raise SystemExit(1) at silver_validate
(job #5 of 13) on every day except the exact date silver_macro was last run —
see CHANGELOG.md v1.7.4 NEW-1/NEW-2 for the root cause.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from src.scheduler.dependency_guard import DependencyGuard
from src.scheduler.job_registry import JOB_REGISTRY, PIPELINE_SEQUENCE


@pytest.fixture
def stubbed_registry():
    """
    Replace every job['fn'] in JOB_REGISTRY with a no-op recorder, and restore
    the originals afterward (JOB_REGISTRY is a shared module-level singleton —
    mutating it without restoring would leak into other test modules).
    """
    calls: list[str] = []
    originals: dict[str, object] = {}

    for name, job in JOB_REGISTRY.items():
        originals[name] = job["fn"]
        # Default-arg trick to bind `name` at each loop iteration correctly.
        job["fn"] = (lambda run_date, _name=name: calls.append(_name))

    try:
        yield calls
    finally:
        for name, fn in originals.items():
            JOB_REGISTRY[name]["fn"] = fn


@pytest.fixture
def sandboxed_guard(monkeypatch, tmp_path):
    """Point src.runner.guard at an isolated sentinel directory."""
    sentinel_dir = tmp_path / ".sentinels"
    sentinel_dir.mkdir()
    guard = DependencyGuard(sentinel_dir=sentinel_dir)
    monkeypatch.setattr("src.runner.guard", guard)
    # health_report and other jobs may instantiate PipelineLogger which writes
    # to the real data/health path — not sandboxed here, consistent with the
    # existing convention in tests/unit/test_runner.py.
    return guard


class TestJobAllAcrossWeek:
    """
    GATE-N1: `--job all` must complete 13/13 DAILY_SEQUENCE jobs without
    --force on any day of the week, given silver_macro/bronze_macro_weekly
    were run once on the preceding Sunday (normal weekly SOP, GD §14.4.2) —
    NOT on every single day, since their cadence is weekly per GD §3.3.1.
    """

    def test_daily_sequence_completes_every_day_of_the_week(
        self, stubbed_registry, sandboxed_guard
    ):
        from src.runner import run_all, run_job

        sunday = date(2026, 6, 21)
        assert sunday.weekday() == 6   # Sunday

        # Step 1: simulate the weekly SOP run ONCE, on Sunday (GD §14.4.2) —
        # this is the only place silver_macro's sentinel gets written all week.
        run_job("bronze_macro_weekly", force=False, run_date=sunday)
        run_job("silver_macro",        force=False, run_date=sunday)

        # Step 2: run `--job all` (DAILY_SEQUENCE only) for every day of the
        # SAME week, Sunday through Saturday — must NOT raise SystemExit on
        # any of them, including the 6 days silver_macro does NOT re-run.
        for offset in range(7):
            run_date = sunday + timedelta(days=offset)
            try:
                run_all(force=False, run_date=run_date)
            except SystemExit as e:   # pragma: no cover - failure diagnostic
                pytest.fail(
                    f"--job all crashed via sys.exit({e.code}) on "
                    f"{run_date} ({run_date.strftime('%A')}), "
                    f"{offset} day(s) after the weekly SOP ran on {sunday}."
                )

            for job_name in PIPELINE_SEQUENCE:
                assert sandboxed_guard.is_done(job_name, run_date), (
                    f"{job_name} did not complete on {run_date}"
                    f" ({run_date.strftime('%A')})"
                )

    def test_no_prior_weekly_run_still_reports_missing_not_crash_signature(
        self, stubbed_registry, sandboxed_guard
    ):
        """
        Sanity check on the OTHER side of the fix: if silver_macro has NEVER
        run (not even once, e.g. brand-new install before the Pre-Coding
        Checklist's weekly bootstrap), --job all must still correctly report
        the dependency as missing — the staleness window must not become
        unconditionally permissive. This intentionally still raises
        SystemExit; it documents that the fix narrows the false-negative
        window without disabling the dependency guard outright.
        """
        from src.runner import run_all

        tuesday = date(2026, 6, 23)
        with pytest.raises(SystemExit):
            run_all(force=False, run_date=tuesday)

        # silver_macro itself was never run, so silver_validate must be the
        # job that reports the missing dependency.
        assert not sandboxed_guard.is_done("silver_validate", tuesday)


class TestGoldScreenerNotLocked:
    """GATE-N2: gold_screener must not be permanently locked by an orphaned
    Finnhub-derived dependency. Originally written against silver_fundamental
    (FIX NEW-2); updated per ADR-043 (GMI_Decision_Document_v10.docx), which
    retired Finnhub in full — silver_sentiment no longer exists either."""

    def test_gold_screener_completes_with_only_its_current_dependencies(
        self, stubbed_registry, sandboxed_guard
    ):
        from src.runner import run_job

        run_date = date(2026, 6, 23)

        # Satisfy gold_screener's actual (post-ADR-043) dependencies directly.
        # Neither silver_fundamental nor silver_sentiment exist any more —
        # gold_screener does not wait on either.
        for dep in ("gold_mtf", "gold_regime", "gold_sector"):
            run_job(dep, force=True, run_date=run_date)

        run_job("gold_screener", force=False, run_date=run_date)
        assert sandboxed_guard.is_done("gold_screener", run_date)

    def test_bronze_finnhub_and_silver_fundamental_no_longer_runnable(
        self, stubbed_registry, sandboxed_guard
    ):
        """FIX ADR-043: bronze_finnhub and silver_fundamental were retired in
        full, not merely deprioritized — attempting to run either by name
        must fail the same clean way as any other unknown job name
        (SystemExit(1) via runner.py's own JOB_REGISTRY membership check),
        not a KeyError or a silent no-op."""
        from src.runner import run_job

        run_date = date(2026, 6, 23)
        for retired_job in ("bronze_finnhub", "silver_fundamental", "silver_sentiment"):
            with pytest.raises(SystemExit):
                run_job(retired_job, force=True, run_date=run_date)
            assert not sandboxed_guard.is_done(retired_job, run_date)


class TestLayerCommands:
    """
    GMI-JR-003 — `--job bronze/silver/gold`: staged per-layer live testing.
    Uses the same stubbed_registry/sandboxed_guard pattern as the rest of
    this file so job orchestration (not job content) is what's exercised.
    """

    def test_bronze_layer_completes_standalone(self, stubbed_registry, sandboxed_guard):
        from src.runner import run_layer
        from src.scheduler.job_registry import LAYER_JOB_NAMES

        # Wednesday — satisfies bronze_eia's run_on_weekdays=[2] guard too,
        # so this exercises the real schedule guard, not force=True.
        run_date = date(2026, 8, 19)
        assert run_date.weekday() == 2

        run_layer("bronze", force=False, run_date=run_date)

        for job_name in LAYER_JOB_NAMES["bronze"]:
            assert sandboxed_guard.is_done(job_name, run_date), (
                f"{job_name} did not complete during the bronze layer run"
            )

    def test_silver_without_bronze_fails_dependency_check(
        self, stubbed_registry, sandboxed_guard
    ):
        """`--job silver` on a fresh sentinel dir (bronze never ran today)
        must sys.exit(1) — not silently skip or partially complete."""
        from src.runner import run_layer

        with pytest.raises(SystemExit):
            run_layer("silver", force=False, run_date=date(2026, 8, 20))

    def test_bronze_then_silver_then_gold_completes_full_chain(
        self, stubbed_registry, sandboxed_guard
    ):
        """The staged live-testing workflow this feature was built for:
        run each layer separately, in order, no --force needed once the
        upstream layer's sentinels exist for the same run_date."""
        from src.runner import run_layer
        from src.scheduler.job_registry import LAYER_JOB_NAMES

        run_date = date(2026, 8, 19)   # Wednesday, see test above

        run_layer("bronze", force=False, run_date=run_date)
        run_layer("silver", force=False, run_date=run_date)
        run_layer("gold",   force=False, run_date=run_date)

        for layer in ("bronze", "silver", "gold"):
            for job_name in LAYER_JOB_NAMES[layer]:
                assert sandboxed_guard.is_done(job_name, run_date), (
                    f"{job_name} ({layer}) did not complete in the staged chain"
                )
