"""
tests/unit/test_gatekeeper_adr050.py

FIX GMI-GATE-001 (ADR-050): src/utils/gatekeeper.py — cascading Bronze ->
Silver -> Gold health gate with time-windowed hysteresis, built on
DependencyGuard sentinels + QualityValidator's persisted
quality_gate_log.parquet.

Isolation: every test points DependencyGuard at a tmp_path sentinel dir
and QUALITY_GATE_LOG_PATH / GATE_STATUS_PATH at tmp_path files —
never the real data/.sentinels or data/health/*.parquet. Job scheduling
(LAYER_JOB_NAMES, JOB_REGISTRY, _passes_schedule, DAILY_SEQUENCE) is used
for real (not mocked) so tests exercise the actual weekday-gating logic
(bronze_eia Wednesday-only, weekly-only jobs with no explicit
run_on_weekdays guard, etc.) rather than a hand-simplified stand-in.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

import src.utils.gatekeeper as gk
from src.scheduler.dependency_guard import DependencyGuard
from src.scheduler.job_registry import (
    DAILY_SEQUENCE,
    GATE_DOMAINS,
    JOB_REGISTRY,
    LAYER_JOB_NAMES,
    _passes_schedule,
)

ANCHOR = date(2026, 9, 27)  # a Sunday — exercises the weekly-only job set too


N_SCOPES = 3 + len(GATE_DOMAINS)      # bronze/silver/gold + one scope per intelligence domain (ADR-053 D4)


def _mark_all_expected(guard: DependencyGuard, day: date) -> None:
    for layer in ("bronze", "silver", "gold"):
        for job_name in LAYER_JOB_NAMES[layer]:
            if _passes_schedule(JOB_REGISTRY[job_name], day):
                guard.mark_done(job_name, day)


def _write_quality_log(path: Path, days: list[date], passed: bool = True) -> None:
    rows = []
    for day in days:
        for check, tier in [
            ("null_check", "critical"),
            ("price_sanity", "critical"),
            ("gap_detection", "warning"),
        ]:
            rows.append({
                "run_date": day.isoformat(), "check_name": check, "tier": tier,
                "passed": passed, "detail": None, "logged_at": day.isoformat(),
            })
    pl.DataFrame(rows).write_parquet(path)


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Isolated DependencyGuard + quality/gate log paths, plus a helper
    to build N consecutive fully-healthy days ending at ANCHOR."""
    sentinel_dir = tmp_path / ".sentinels"
    guard = DependencyGuard(sentinel_dir=sentinel_dir)
    monkeypatch.setattr(gk, "DependencyGuard", lambda: guard)

    quality_log_path = tmp_path / "quality_gate_log.parquet"
    gate_status_path = tmp_path / "gate_status.parquet"
    monkeypatch.setattr(gk, "QUALITY_GATE_LOG_PATH", quality_log_path)
    monkeypatch.setattr(gk, "GATE_STATUS_PATH", gate_status_path)

    class Sandbox:
        def __init__(self):
            self.guard = guard
            self.quality_log_path = quality_log_path
            self.gate_status_path = gate_status_path

        def healthy_days(self, anchor: date, n: int) -> list[date]:
            days = [anchor - timedelta(days=i) for i in range(n)]
            for d in days:
                _mark_all_expected(self.guard, d)
            _write_quality_log(self.quality_log_path, days, passed=True)
            return days

    return Sandbox()


class TestWorseOrdering:

    def test_pass_pass_is_pass(self):
        assert gk._worse("pass", "pass") == "pass"

    def test_pass_warn_is_warn(self):
        assert gk._worse("pass", "warn") == "warn"
        assert gk._worse("warn", "pass") == "warn"

    def test_warn_block_is_block(self):
        assert gk._worse("warn", "block") == "block"
        assert gk._worse("block", "warn") == "block"

    def test_pass_block_is_block(self):
        assert gk._worse("pass", "block") == "block"


class TestEmitStatusHysteresis:

    def test_single_pass_day_emits_warn_not_pass(self):
        """RECOVERY_CONFIRM_DAYS=2 -- one good day alone isn't enough."""
        emitted, streak = gk._emit_status(["pass"])
        assert emitted == "warn"
        assert streak == 1

    def test_two_consecutive_pass_days_emit_pass(self):
        emitted, streak = gk._emit_status(["pass", "pass"])
        assert emitted == "pass"
        assert streak == 2

    def test_pass_after_block_history_needs_fresh_confirmation(self):
        emitted, streak = gk._emit_status(["pass", "block", "block"])
        assert emitted == "warn"   # only 1-day streak of 'pass' so far
        assert streak == 1

    def test_single_block_day_emits_warn_not_block(self):
        """BLOCK_CONFIRM_DAYS=4 -- one bad day surfaces as warn, not a hard stop."""
        emitted, streak = gk._emit_status(["block", "pass", "pass"])
        assert emitted == "warn"
        assert streak == 1

    def test_three_consecutive_block_days_still_warn(self):
        emitted, streak = gk._emit_status(["block", "block", "block", "pass"])
        assert emitted == "warn"
        assert streak == 3

    def test_four_consecutive_block_days_emits_block(self):
        emitted, streak = gk._emit_status(["block", "block", "block", "block", "pass"])
        assert emitted == "block"
        assert streak == 4

    def test_warn_always_surfaces_immediately(self):
        emitted, streak = gk._emit_status(["warn", "pass", "pass"])
        assert emitted == "warn"
        assert streak == 1

    def test_streak_counts_only_leading_run(self):
        emitted, streak = gk._emit_status(["pass", "pass", "pass", "block", "block"])
        assert streak == 3
        assert emitted == "pass"


class TestExpectedJobsForLayer:

    def test_bronze_eia_only_expected_on_wednesday(self):
        wednesday = date(2026, 9, 30)
        sunday    = date(2026, 9, 27)
        assert "bronze_eia" in gk._expected_jobs_for_layer("bronze", wednesday)
        assert "bronze_eia" not in gk._expected_jobs_for_layer("bronze", sunday)

    def test_weekly_only_jobs_with_no_explicit_guard_appear_every_day(self):
        """silver_macro/silver_global_rates carry no run_on_weekdays guard
        of their own -- _passes_schedule() alone can't exclude them on a
        non-Sunday. This is exactly why _job_completed() applies a
        staleness window to non-DAILY_SEQUENCE jobs instead of trusting
        _passes_schedule() to have already handled cadence."""
        wednesday = date(2026, 9, 30)
        assert "silver_macro" in gk._expected_jobs_for_layer("silver", wednesday)

    def test_daily_jobs_expected_every_day(self):
        for d in (date(2026, 9, 27), date(2026, 9, 30)):
            assert "bronze_ohlcv_daily" in gk._expected_jobs_for_layer("bronze", d)


class TestJobCompleted:

    def test_daily_job_requires_exact_date(self, sandbox):
        sandbox.guard.mark_done("bronze_ohlcv_daily", ANCHOR - timedelta(days=1))
        assert not gk._job_completed(
            sandbox.guard, "bronze_ohlcv_daily", ANCHOR, set(DAILY_SEQUENCE)
        )

    def test_weekly_job_tolerates_staleness_within_window(self, sandbox):
        sandbox.guard.mark_done("silver_macro", ANCHOR - timedelta(days=6))
        assert gk._job_completed(
            sandbox.guard, "silver_macro", ANCHOR, set(DAILY_SEQUENCE)
        )

    def test_weekly_job_fails_beyond_staleness_window(self, sandbox):
        sandbox.guard.mark_done("silver_macro", ANCHOR - timedelta(days=8))
        assert not gk._job_completed(
            sandbox.guard, "silver_macro", ANCHOR, set(DAILY_SEQUENCE)
        )


class TestBronzeRawStatus:

    def test_all_jobs_missing_is_block(self, sandbox):
        status = gk._bronze_raw_status(sandbox.guard, ANCHOR, set(DAILY_SEQUENCE))
        assert status == "block"

    def test_all_jobs_done_no_quarantine_is_pass(self, sandbox):
        _mark_all_expected(sandbox.guard, ANCHOR)
        status = gk._bronze_raw_status(sandbox.guard, ANCHOR, set(DAILY_SEQUENCE))
        assert status == "pass"

    def test_one_missing_job_is_block(self, sandbox):
        _mark_all_expected(sandbox.guard, ANCHOR)
        # Undo one required job by pointing at a guard with no sentinel for it.
        missing_dir = sandbox.gate_status_path.parent / "empty_sentinels"
        empty_guard = DependencyGuard(sentinel_dir=missing_dir)
        status = gk._bronze_raw_status(empty_guard, ANCHOR, set(DAILY_SEQUENCE))
        assert status == "block"

    def test_quarantine_file_today_is_warn(self, sandbox, monkeypatch, tmp_path):
        _mark_all_expected(sandbox.guard, ANCHOR)
        quarantine_dir = tmp_path / "quarantine"
        quarantine_dir.mkdir()
        f = quarantine_dir / "AAPL_yfinance_20260927_120000.parquet"
        f.write_bytes(b"fake")
        monkeypatch.setattr(gk, "QUARANTINE_GLOB", str(quarantine_dir / "*.parquet"))
        # Force mtime to ANCHOR's date at noon local time.
        import os, time
        ts = time.mktime(ANCHOR.timetuple())
        os.utime(f, (ts, ts))
        status = gk._bronze_raw_status(sandbox.guard, ANCHOR, set(DAILY_SEQUENCE))
        assert status == "warn"


class TestSilverRawStatus:

    def test_cascades_bronze_block(self, sandbox):
        _mark_all_expected(sandbox.guard, ANCHOR)
        _write_quality_log(sandbox.quality_log_path, [ANCHOR], passed=True)
        status = gk._silver_raw_status(
            sandbox.guard, ANCHOR, set(DAILY_SEQUENCE),
            pl.read_parquet(sandbox.quality_log_path), bronze_status="block",
        )
        assert status == "block"

    def test_all_healthy_is_pass(self, sandbox):
        _mark_all_expected(sandbox.guard, ANCHOR)
        _write_quality_log(sandbox.quality_log_path, [ANCHOR], passed=True)
        status = gk._silver_raw_status(
            sandbox.guard, ANCHOR, set(DAILY_SEQUENCE),
            pl.read_parquet(sandbox.quality_log_path), bronze_status="pass",
        )
        assert status == "pass"

    def test_critical_check_failure_is_block(self, sandbox):
        _mark_all_expected(sandbox.guard, ANCHOR)
        _write_quality_log(sandbox.quality_log_path, [ANCHOR], passed=False)
        status = gk._silver_raw_status(
            sandbox.guard, ANCHOR, set(DAILY_SEQUENCE),
            pl.read_parquet(sandbox.quality_log_path), bronze_status="pass",
        )
        assert status == "block"

    def test_warning_only_failure_is_warn(self, sandbox):
        _mark_all_expected(sandbox.guard, ANCHOR)
        rows = [
            {"run_date": ANCHOR.isoformat(), "check_name": "null_check",
             "tier": "critical", "passed": True, "detail": None, "logged_at": ""},
            {"run_date": ANCHOR.isoformat(), "check_name": "gap_detection",
             "tier": "warning", "passed": False, "detail": "gap found", "logged_at": ""},
        ]
        pl.DataFrame(rows).write_parquet(sandbox.quality_log_path)
        status = gk._silver_raw_status(
            sandbox.guard, ANCHOR, set(DAILY_SEQUENCE),
            pl.read_parquet(sandbox.quality_log_path), bronze_status="pass",
        )
        assert status == "warn"

    def test_missing_log_data_is_warn_not_pass(self, sandbox):
        _mark_all_expected(sandbox.guard, ANCHOR)
        empty = pl.DataFrame(schema={
            "run_date": pl.Utf8, "check_name": pl.Utf8, "tier": pl.Utf8,
            "passed": pl.Boolean, "detail": pl.Utf8, "logged_at": pl.Utf8,
        })
        status = gk._silver_raw_status(
            sandbox.guard, ANCHOR, set(DAILY_SEQUENCE), empty, bronze_status="pass",
        )
        assert status == "warn"

    def test_missing_silver_job_is_block_even_with_clean_log(self, sandbox):
        _write_quality_log(sandbox.quality_log_path, [ANCHOR], passed=True)
        # No sentinels marked at all -> every expected Silver job missing.
        status = gk._silver_raw_status(
            sandbox.guard, ANCHOR, set(DAILY_SEQUENCE),
            pl.read_parquet(sandbox.quality_log_path), bronze_status="pass",
        )
        assert status == "block"


class TestGoldRawStatus:

    def test_cascades_silver_warn(self, sandbox):
        _mark_all_expected(sandbox.guard, ANCHOR)
        status = gk._gold_raw_status(sandbox.guard, ANCHOR, set(DAILY_SEQUENCE), silver_status="warn")
        assert status == "warn"

    def test_all_gold_jobs_done_and_silver_pass_is_pass(self, sandbox):
        _mark_all_expected(sandbox.guard, ANCHOR)
        status = gk._gold_raw_status(sandbox.guard, ANCHOR, set(DAILY_SEQUENCE), silver_status="pass")
        assert status == "pass"

    def test_missing_gold_job_is_block(self, sandbox):
        empty_guard = DependencyGuard(sentinel_dir=sandbox.gate_status_path.parent / "empty2")
        status = gk._gold_raw_status(empty_guard, ANCHOR, set(DAILY_SEQUENCE), silver_status="pass")
        assert status == "block"


class TestComputeIntegration:

    def test_empty_environment_blocks_all_layers(self, sandbox):
        df = gk.compute(ANCHOR)
        assert df.height == N_SCOPES                      # 3 layers + ADR-053 domain scopes
        assert set(df["gate_status"].to_list()) == {"block"}
        assert set(df["streak_days"].to_list()) == {gk.LOOKBACK_DAYS}

    def test_healthy_history_passes_after_recovery_window(self, sandbox):
        sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)
        df = gk.compute(ANCHOR)
        for row in df.to_dicts():
            assert row["gate_status"] == "pass", row
            assert row["layer_health"] == "pass"

    def test_single_bad_day_surfaces_as_warn_not_block(self, sandbox):
        days = sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)
        # Break ONLY today's bronze completion — knock out one job's sentinel
        # for ANCHOR specifically by rebuilding a fresh empty guard for just
        # that date's check via a targeted second sandbox is overkill; instead
        # remove today's sentinel files entirely (bronze layer) to simulate a
        # single bad day sitting inside an otherwise-healthy window.
        sentinel_dir = sandbox.guard.sentinel_dir
        for job_name in LAYER_JOB_NAMES["bronze"]:
            if _passes_schedule(JOB_REGISTRY[job_name], ANCHOR):
                p = sentinel_dir / f"{job_name}_{ANCHOR.isoformat()}.done"
                if p.exists():
                    p.unlink()
        df = gk.compute(ANCHOR)
        bronze_row = df.filter(pl.col("scope") == "bronze").to_dicts()[0]
        assert bronze_row["layer_health"] == "block"
        assert bronze_row["gate_status"] == "warn"
        assert bronze_row["streak_days"] == 1

    def test_scopes_present(self, sandbox):
        df = gk.compute(ANCHOR)
        assert set(df["scope"].to_list()) == {"bronze", "silver", "gold", *GATE_DOMAINS}


class TestRunPersistence:

    def test_run_writes_gate_status_file(self, sandbox):
        sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)
        gk.run(ANCHOR)
        assert sandbox.gate_status_path.exists()
        df = pl.read_parquet(sandbox.gate_status_path)
        assert df.height == N_SCOPES

    def test_rerun_same_date_replaces_not_duplicates(self, sandbox):
        sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)
        gk.run(ANCHOR)
        gk.run(ANCHOR)
        df = pl.read_parquet(sandbox.gate_status_path)
        assert df.height == N_SCOPES   # not 2 * N_SCOPES

    def test_history_accumulates_across_dates(self, sandbox):
        sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)
        gk.run(ANCHOR)
        sandbox.healthy_days(ANCHOR + timedelta(days=1), gk.LOOKBACK_DAYS)
        gk.run(ANCHOR + timedelta(days=1))
        df = pl.read_parquet(sandbox.gate_status_path)
        assert df.height == 2 * N_SCOPES
        assert set(df["run_date"].unique().to_list()) == {
            ANCHOR.isoformat(), (ANCHOR + timedelta(days=1)).isoformat(),
        }

    def test_persistence_failure_does_not_raise(self, sandbox, monkeypatch):
        sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)

        def _boom(*a, **kw):
            raise OSError("disk full")

        monkeypatch.setattr(gk, "atomic_write_parquet", _boom)
        gk.run(ANCHOR)   # must not raise


class TestDueExactlyOnGuardDays:
    """_due_exactly_on_guard_days(): only a run_on_weekdays-ONLY guard means
    'due (and required) on exactly the days _passes_schedule lets it through'."""

    def test_real_registry_classification(self):
        d = gk._due_exactly_on_guard_days
        assert d(JOB_REGISTRY["bronze_macro_weekly"])            # Sunday
        assert d(JOB_REGISTRY["bronze_bis_rates"])               # Sunday
        assert d(JOB_REGISTRY["bronze_eia"])                     # Wednesday
        assert d(JOB_REGISTRY["gold_lead_lag"])                  # Sunday
        assert not d(JOB_REGISTRY["silver_macro"])               # no guard at all
        assert not d(JOB_REGISTRY["silver_global_rates"])        # no guard at all
        assert not d(JOB_REGISTRY["bronze_bls_cpi"])             # day-of-month window
        assert not d(JOB_REGISTRY["bronze_bea_gdp"])             # months + day window

    def test_synthetic_cases(self):
        d = gk._due_exactly_on_guard_days
        assert d({"run_on_weekdays": [6]})
        assert d({"run_on_weekdays": [0, 2, 4]})
        assert not d({})
        assert not d({"run_on_weekdays": [6], "run_on_day_of_month": [1]})
        assert not d({"run_on_nth_weekday": {"n": 1, "weekday": 4}})


class TestDetectionLag:
    """
    Measured against the real registry (not assumed): how many days after a
    weekly job's last success does Gatekeeper first flag it as missing?

        guarded weekly (run_on_weekdays) — flagged ON the next due day it is
            missed (day+7). Under the earlier blanket 7-day window it was
            first flagged at day+14 (a full week late), because the window
            still covered the previous run on the due day itself and the job
            is not "expected" on any other day.
        unguarded weekly (silver_macro) — flagged the day after its window
            lapses (day+8).
    """

    def _first_flag(self, sandbox, job, layer, last_success):
        sandbox.guard.mark_done(job, last_success)
        daily = set(DAILY_SEQUENCE)
        for k in range(0, 30):
            day = last_success + timedelta(days=k)
            if job in gk._expected_jobs_for_layer(layer, day) and \
                    not gk._job_completed(sandbox.guard, job, day, daily):
                return k
        return None

    def test_guarded_sunday_job_flagged_on_the_missed_due_day(self, sandbox):
        assert self._first_flag(sandbox, "bronze_macro_weekly", "bronze", date(2026, 9, 6)) == 7

    def test_guarded_wednesday_job_flagged_on_the_missed_due_day(self, sandbox):
        assert self._first_flag(sandbox, "bronze_eia", "bronze", date(2026, 9, 2)) == 7

    def test_unguarded_weekly_job_flagged_day_after_window_lapses(self, sandbox):
        assert self._first_flag(sandbox, "silver_macro", "silver", date(2026, 9, 6)) == 8

    def test_guarded_job_completed_on_due_day_is_ok(self, sandbox):
        sunday = date(2026, 9, 13)
        sandbox.guard.mark_done("bronze_macro_weekly", sunday)
        assert gk._job_completed(sandbox.guard, "bronze_macro_weekly", sunday, set(DAILY_SEQUENCE))

    def test_guarded_job_done_the_day_before_does_not_satisfy_the_due_day(self, sandbox):
        """Ran Saturday instead of Sunday: Sunday's due run is still missing."""
        sandbox.guard.mark_done("bronze_macro_weekly", date(2026, 9, 12))
        assert not gk._job_completed(
            sandbox.guard, "bronze_macro_weekly", date(2026, 9, 13), set(DAILY_SEQUENCE)
        )

    def test_unguarded_job_run_daily_is_never_flagged(self, sandbox):
        """Real live pattern (your sentinel history): silver_macro runs
        every day via `--job silver` — a window must not make that look bad."""
        for i in range(20):
            sandbox.guard.mark_done("silver_macro", date(2026, 9, 1) + timedelta(days=i))
        daily = set(DAILY_SEQUENCE)
        for i in range(20):
            assert gk._job_completed(sandbox.guard, "silver_macro", date(2026, 9, 1) + timedelta(days=i), daily)


class TestDomainScopes:
    """ADR-053 D4: one scope per intelligence domain, built from that domain's own job chain only."""

    def test_members_are_the_domain_chain_and_nothing_else(self):
        sunday = date(2026, 10, 25)
        wednesday = date(2026, 10, 21)
        assert sunday.weekday() == 6 and wednesday.weekday() == 2
        hmm_sun = set(gk.domain_members("hmm", sunday))
        assert {"gold_raw_macro", "gold_context_regime", "gold_hmm_walk_forward", "silver_macro"} <= hmm_sun
        assert hmm_sun.isdisjoint({"gold_signals", "gold_mtf", "gold_global_regime", "gold_lead_lag"})
        assert "gold_hmm_walk_forward" not in gk.domain_members("hmm", wednesday)     # Sunday-gated
        assert set(gk.domain_members("technical", wednesday)).isdisjoint(
            {"gold_raw_macro", "gold_context_regime", "gold_global_regime"})

    def test_unknown_domain_is_a_key_error(self):
        with pytest.raises(KeyError):
            gk.domain_members("nope", ANCHOR)

    def test_healthy_history_passes_every_domain(self, sandbox):
        sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)
        for dom in GATE_DOMAINS:
            st = gk.domain_status(dom, ANCHOR)
            assert st["gate_status"] == "pass" and st["layer_health"] == "pass", (dom, st)

    def test_a_cross_asset_outage_does_not_touch_the_other_domains(self, sandbox):
        """The point of the domain scopes: gold_global_regime missing today blocks cross_asset's raw
        status and the gold LAYER, but the hmm and technical chains are unaffected."""
        sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)
        sandbox.guard.reset_job("gold_global_regime", ANCHOR)
        assert gk.domain_status("cross_asset", ANCHOR)["layer_health"] == "block"
        assert gk.domain_status("hmm", ANCHOR)["layer_health"] == "pass"
        assert gk.domain_status("technical", ANCHOR)["layer_health"] == "pass"
        df = gk.compute(ANCHOR)
        assert df.filter(pl.col("scope") == "gold")["layer_health"][0] == "block"

    def test_an_hmm_outage_does_not_touch_the_technical_or_cross_asset_domains(self, sandbox):
        sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)
        sandbox.guard.reset_job("gold_context_regime", ANCHOR)
        assert gk.domain_status("hmm", ANCHOR)["layer_health"] == "block"
        assert gk.domain_status("technical", ANCHOR)["layer_health"] == "pass"
        assert gk.domain_status("cross_asset", ANCHOR)["layer_health"] == "pass"

    def test_a_failed_silver_quality_check_reaches_only_domains_that_depend_on_silver_validate(self, sandbox):
        from src.scheduler.job_registry import JOB_REGISTRY
        sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)
        _write_quality_log(sandbox.quality_log_path, [ANCHOR], passed=False)
        chains = {d: gk._upstream_closure(GATE_DOMAINS[d], JOB_REGISTRY) for d in GATE_DOMAINS}
        for dom, chain in chains.items():
            expected = "block" if "silver_validate" in chain else "pass"
            assert gk.domain_status(dom, ANCHOR)["layer_health"] == expected, dom
        assert "silver_validate" not in chains["hmm"]            # the HMM chain never rests on it

    def test_domain_status_uses_the_same_hysteresis_as_the_layers(self, sandbox):
        days = sandbox.healthy_days(ANCHOR, gk.LOOKBACK_DAYS)
        sandbox.guard.reset_job("gold_context_regime", ANCHOR)
        st = gk.domain_status("hmm", ANCHOR)
        assert st["layer_health"] == "block" and st["gate_status"] == "warn" and st["streak_days"] == 1  # one bad day: warn, not block

    def test_domain_gate_status_never_raises(self, monkeypatch):
        monkeypatch.setattr(gk, "domain_status", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
        assert gk.domain_gate_status("hmm", ANCHOR) == "unknown"
