"""
tests/unit/test_run_all_run_on_failure_adr050.py

ADD GMI-GATE-001 (ADR-050) — run_on_failure.

Before: run_all() stopped at the first failed job (SystemExit from the
dependency guard, or the job's own exception), so the Gatekeeper — a job
whose whole purpose is to observe a partly-failed pipeline — never ran on
exactly the days it mattered.

After: once a job has failed, remaining jobs are skipped EXCEPT those
flagged run_on_failure in JOB_REGISTRY; the FIRST failure is still
re-raised at the end (same exception type / SystemExit code as before),
and a later run_on_failure job failing never masks it.

Methodology mirrors tests/integration/test_runner_weekly_cadence.py: every
job['fn'] stubbed to a recorder, sandboxed sentinel directory, originals
restored afterwards (JOB_REGISTRY is a shared module-level singleton).
"""

from __future__ import annotations

from datetime import date

import pytest

from src.runner import run_all, run_job
from src.scheduler.dependency_guard import DependencyGuard
from src.scheduler.job_registry import DAILY_SEQUENCE, JOB_REGISTRY

RUN_DATE = date(2026, 9, 29)   # a Tuesday


@pytest.fixture
def stubbed_registry():
    calls: list[str] = []
    originals = {}
    for name, job in JOB_REGISTRY.items():
        originals[name] = job["fn"]
        job["fn"] = (lambda run_date, _name=name: calls.append(_name))
    try:
        yield calls
    finally:
        for name, fn in originals.items():
            JOB_REGISTRY[name]["fn"] = fn


@pytest.fixture
def sandboxed_guard(monkeypatch, tmp_path):
    guard = DependencyGuard(sentinel_dir=tmp_path / ".sentinels")
    monkeypatch.setattr("src.runner.guard", guard)
    # silver_validate / gold_regime hard-depend on silver_macro (weekly, 7d
    # stale tolerance) — same precondition the weekly SOP satisfies in prod.
    guard.mark_done("silver_macro", RUN_DATE)
    return guard


def _fail_with(name: str, exc: BaseException):
    def _boom(run_date):
        raise exc
    JOB_REGISTRY[name]["fn"] = _boom


class TestRegistryContract:

    def test_only_gatekeeper_carries_the_flag(self):
        """Pinned on purpose: run_on_failure changes failure semantics for a
        job, so adding it anywhere else should be a deliberate decision that
        has to touch this test."""
        flagged = {n for n, j in JOB_REGISTRY.items() if j.get("run_on_failure")}
        assert flagged == {"gatekeeper"}

    def test_flagged_job_is_in_daily_sequence(self):
        assert "gatekeeper" in DAILY_SEQUENCE


class TestNoFailureUnchanged:

    def test_full_sequence_runs_in_order(self, stubbed_registry, sandboxed_guard):
        run_all(force=False, run_date=RUN_DATE)
        assert stubbed_registry == list(DAILY_SEQUENCE)


class TestFailureStillRunsGatekeeper:

    def test_job_exception_skips_rest_but_runs_gatekeeper(self, stubbed_registry, sandboxed_guard):
        _fail_with("silver_ohlcv", RuntimeError("boom"))
        with pytest.raises(RuntimeError, match="boom"):
            run_all(force=False, run_date=RUN_DATE)
        assert "gatekeeper" in stubbed_registry
        # everything after the failure that is NOT flagged was skipped
        for skipped in ("silver_validate", "gold_signals", "gold_screener", "health_report"):
            assert skipped not in stubbed_registry

    def test_jobs_before_failure_still_ran(self, stubbed_registry, sandboxed_guard):
        _fail_with("silver_ohlcv", RuntimeError("boom"))
        with pytest.raises(RuntimeError):
            run_all(force=False, run_date=RUN_DATE)
        assert stubbed_registry[:4] == list(DAILY_SEQUENCE[:4])   # the 4 bronze jobs

    def test_dependency_systemexit_also_triggers_gatekeeper(self, stubbed_registry, tmp_path, monkeypatch):
        """No silver_macro sentinel -> silver_validate's dependency check
        does sys.exit(1). Gatekeeper must still run, and the SystemExit
        (same exit code as before this change) must still be raised."""
        guard = DependencyGuard(sentinel_dir=tmp_path / ".sentinels")
        monkeypatch.setattr("src.runner.guard", guard)
        with pytest.raises(SystemExit) as ei:
            run_all(force=False, run_date=RUN_DATE)
        assert ei.value.code == 1
        assert "gatekeeper" in stubbed_registry
        assert "gold_signals" not in stubbed_registry

    def test_failing_job_has_no_sentinel_gatekeeper_does(self, stubbed_registry, sandboxed_guard):
        _fail_with("silver_ohlcv", RuntimeError("boom"))
        with pytest.raises(RuntimeError):
            run_all(force=False, run_date=RUN_DATE)
        assert not sandboxed_guard.is_done("silver_ohlcv", RUN_DATE)
        assert sandboxed_guard.is_done("gatekeeper", RUN_DATE)


class TestOriginalFailurePreserved:

    def test_gatekeeper_failure_does_not_mask_original(self, stubbed_registry, sandboxed_guard):
        _fail_with("silver_ohlcv", RuntimeError("original"))
        _fail_with("gatekeeper", ValueError("secondary"))
        with pytest.raises(RuntimeError, match="original"):
            run_all(force=False, run_date=RUN_DATE)

    def test_gatekeeper_failure_alone_still_raises(self, stubbed_registry, sandboxed_guard):
        """A failure with no earlier failure is just a failure."""
        _fail_with("gatekeeper", ValueError("solo"))
        with pytest.raises(ValueError, match="solo"):
            run_all(force=False, run_date=RUN_DATE)

    def test_first_failure_wins_over_later_failures(self, stubbed_registry, sandboxed_guard):
        _fail_with("bronze_treasury", RuntimeError("first"))
        _fail_with("silver_ohlcv", KeyError("second"))   # skipped anyway
        with pytest.raises(RuntimeError, match="first"):
            run_all(force=False, run_date=RUN_DATE)


class TestKeyboardInterrupt:

    def test_interrupt_is_not_swallowed_and_gatekeeper_does_not_run(self, stubbed_registry, sandboxed_guard):
        """An operator who aborts must not trigger further jobs."""
        _fail_with("silver_ohlcv", KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt):
            run_all(force=False, run_date=RUN_DATE)
        assert "gatekeeper" not in stubbed_registry


class TestStandaloneGatekeeper:

    def test_runs_with_zero_sentinels(self, stubbed_registry, tmp_path, monkeypatch):
        """After a failed LAYER run (util jobs are not in layer lists) the
        operator invokes `--job gatekeeper`; with depends_on=[] it must run
        on a day with no sentinels at all instead of exiting on a missing
        dependency."""
        guard = DependencyGuard(sentinel_dir=tmp_path / ".sentinels")
        monkeypatch.setattr("src.runner.guard", guard)
        run_job("gatekeeper", run_date=RUN_DATE)      # must not SystemExit
        assert stubbed_registry == ["gatekeeper"]
        assert guard.is_done("gatekeeper", RUN_DATE)
