"""
tests/integration/test_gatekeeper_ghr_adr059.py — ADR-059 GHR-04 / GHR-05 / GHR-09.

GK3  Sunday ordering: the Gatekeeper (a daily job) ran 35 minutes BEFORE the weekly sentinels were
     written, read the due weekly jobs as missing and emitted raw `block` on all three scopes.
GK5  Resume trap: days paused on purpose carry no Silver/Gold sentinels, so they recompute as `block`;
     a failing job on the first day after the pause then emitted `block` immediately, with no 4-day
     confirmation.
GK4  History is recomputed from mutable sentinel files, so a sentinel cleanup rewrote it; only 6
     `gate_status` rows were ever persisted.
"""

from __future__ import annotations

from datetime import date, timedelta

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
    due_weekly_head_jobs,
)
from src.utils import data_paths, pause_markers

SUNDAY = date(2026, 9, 27)
TUESDAY = date(2026, 9, 29)
WEDNESDAY = date(2026, 9, 30)
DAILY = set(DAILY_SEQUENCE)
N_SCOPES = 3 + len(GATE_DOMAINS)


def _mark_layer(guard: DependencyGuard, layer: str, day: date, *, skip=()) -> None:
    for job_name in LAYER_JOB_NAMES[layer]:
        if job_name not in skip and _passes_schedule(JOB_REGISTRY[job_name], day):
            guard.mark_done(job_name, day)


def _quality(path, days, passed=True) -> None:
    rows = [{"run_date": d.isoformat(), "check_name": c, "tier": t, "passed": passed,
             "detail": None, "logged_at": d.isoformat()}
            for d in days for c, t in (("null_check", "critical"), ("price_sanity", "critical"),
                                       ("gap_detection", "warning"))]
    pl.DataFrame(rows).write_parquet(path)


@pytest.fixture
def sb(tmp_path, monkeypatch):
    """Isolated guard + every gate store under tmp_path (incl. pause markers and the evidence log)."""
    monkeypatch.setenv(data_paths.ENV_VAR, str(tmp_path))
    guard = DependencyGuard(sentinel_dir=tmp_path / ".sentinels")
    monkeypatch.setattr(gk, "DependencyGuard", lambda: guard)
    monkeypatch.setattr(gk, "QUALITY_GATE_LOG_PATH", tmp_path / "quality_gate_log.parquet")
    monkeypatch.setattr(gk, "GATE_STATUS_PATH", tmp_path / "gate_status.parquet")
    monkeypatch.setattr(gk, "GATE_LOG_DIR", tmp_path / "gate_log")

    class SB:
        pass

    o = SB()
    o.guard, o.tmp = guard, tmp_path
    o.quality_path, o.status_path, o.log_dir = tmp_path / "quality_gate_log.parquet", \
        tmp_path / "gate_status.parquet", tmp_path / "gate_log"

    def healthy(anchor: date, n: int, *, layers=("bronze", "silver", "gold")):
        days = [anchor - timedelta(days=i) for i in range(n)]
        for d in days:
            for layer in layers:
                _mark_layer(guard, layer, d)
        _quality(o.quality_path, days)
        return days

    o.healthy = healthy
    return o


def _weekly_only(day: date) -> list[str]:
    return [j for layer in ("bronze", "silver", "gold") for j in gk._expected_jobs_for_layer(layer, day)
            if gk._is_weekly_only_guard_job(j, DAILY)]


# ═════════════════════════════════════════════ GHR-04 ═══════════════════════════════════════════

class TestPendingWeeklyJobs:

    def _daily_first_sunday(self, sb):
        """The observed 18 Oct order: every daily job done, no weekly-only job started yet."""
        sb.healthy(SUNDAY, 14)
        for j in _weekly_only(SUNDAY):
            sb.guard.reset_job(j, SUNDAY)
        assert _weekly_only(SUNDAY), "fixture must contain due weekly-only jobs"

    def test_weekly_jobs_are_pending_not_missing_on_the_evaluation_day(self, sb):
        self._daily_first_sunday(sb)
        ev: list[dict] = []
        raw = gk._bronze_raw_status(sb.guard, SUNDAY, DAILY, allow_pending=True, evidence=ev)
        assert raw != "block"
        states = {e["job"]: e["job_state"] for e in ev if e["job"]}
        assert states["bronze_macro_weekly"] == gk.JOB_PENDING
        assert states["bronze_bis_rates"] == gk.JOB_PENDING

    def test_without_allow_pending_the_same_day_is_missing_and_blocks(self, sb):
        """Past days are final: the unchanged behaviour, so a wrong `pending` self-corrects."""
        self._daily_first_sunday(sb)
        assert gk._bronze_raw_status(sb.guard, SUNDAY, DAILY) == "block"

    def test_compute_daily_first_sunday_is_not_a_raw_block(self, sb):
        self._daily_first_sunday(sb)
        rows = {r["scope"]: r for r in gk.compute(SUNDAY).to_dicts()}
        for scope in ("bronze", "silver", "gold", "cross_asset"):
            assert rows[scope]["layer_health"] != "block", scope

    def test_weekly_first_order_is_a_raw_pass(self, sb):
        sb.healthy(SUNDAY, 14)
        rows = {r["scope"]: r for r in gk.compute(SUNDAY).to_dicts()}
        assert all(rows[s]["layer_health"] == "pass" for s in ("bronze", "silver", "gold"))

    def test_once_the_weekly_sequence_started_an_unfinished_job_is_missing(self, sb):
        self._daily_first_sunday(sb)
        sb.guard.mark_done("bronze_macro_weekly", SUNDAY)          # started ...
        ev: list[dict] = []
        raw = gk._bronze_raw_status(sb.guard, SUNDAY, DAILY, allow_pending=True, evidence=ev)
        assert raw == "block"                                       # ... bronze_bis_rates never finished
        states = {e["job"]: e["job_state"] for e in ev if e["job"]}
        assert states["bronze_bis_rates"] == gk.JOB_MISSING

    def test_next_day_recomputation_corrects_a_pending_that_never_finished(self, sb):
        """Sunday looked fine (pending); the weekly jobs never ran; Monday sees Sunday as final."""
        self._daily_first_sunday(sb)
        monday = SUNDAY + timedelta(days=1)
        sb.healthy(monday, 1)
        raw = gk._bronze_raw_status(sb.guard, SUNDAY, DAILY)        # offset >= 1 -> allow_pending False
        assert raw == "block"

    def test_compute_confines_pending_to_the_evaluation_day(self, sb):
        """Through compute()'s own history loop: Sunday (offset 1 from Monday) without its weekly
        sentinels is FINAL -> block, in the layer scopes and in the domain scope alike."""
        monday = SUNDAY + timedelta(days=1)
        sb.healthy(monday, 14)
        for j in _weekly_only(SUNDAY):
            sb.guard.reset_job(j, SUNDAY)
        _, ev = gk.compute_with_evidence(monday)
        heads = ev.filter(pl.col("job").is_null())

        def raw(scope, offset):
            return heads.filter((pl.col("scope") == scope) & (pl.col("day_offset") == offset))["raw_status"].item()

        for scope in ("bronze", "cross_asset"):
            assert raw(scope, 1) == "block", scope
            assert raw(scope, 0) != "block", scope

    def test_domain_scope_weekly_members_are_pending_too(self, sb):
        self._daily_first_sunday(sb)
        members = gk.domain_members("cross_asset", SUNDAY)
        assert any(gk._is_weekly_only_guard_job(j, DAILY) for j in members)
        assert gk._domain_raw_status("cross_asset", sb.guard, SUNDAY, DAILY, gk._read_quality_log(),
                                     allow_pending=True) != "block"
        assert gk._domain_raw_status("cross_asset", sb.guard, SUNDAY, DAILY, gk._read_quality_log()) == "block"


class TestWeeklyHeadJobs:
    """The runner side of GHR-04: only jobs that can never fail a dependency run ahead of the chain."""

    def test_sunday_head_is_the_bronze_weekly_set(self):
        assert due_weekly_head_jobs(SUNDAY) == ["bronze_macro_weekly", "bronze_bis_rates"]

    def test_wednesday_head_is_eia(self):
        assert due_weekly_head_jobs(WEDNESDAY) == ["bronze_eia"]

    def test_tuesday_has_no_head(self):
        assert due_weekly_head_jobs(TUESDAY) == []

    def test_every_head_job_is_dependency_free_and_guard_only(self):
        for day in (SUNDAY, WEDNESDAY):
            for name in due_weekly_head_jobs(day):
                job = JOB_REGISTRY[name]
                assert not job.get("depends_on")
                assert {k for k in job if k.startswith("run_on_")} == {"run_on_weekdays"}
                assert name not in DAILY_SEQUENCE

    def test_weekly_gold_jobs_are_not_in_the_head(self):
        """They hard-depend on same-day DAILY Silver jobs; ahead of the chain they would stop it."""
        head = set(due_weekly_head_jobs(SUNDAY))
        for name in ("gold_cross_asset_correlation", "gold_lead_lag", "gold_forecast",
                     "gold_hmm_macro_fit", "silver_macro"):
            assert name not in head

    def test_run_all_runs_the_head_before_the_daily_chain(self, monkeypatch):
        import src.runner as runner

        calls: list[str] = []
        monkeypatch.setattr(runner, "run_job", lambda name, force=False, run_date=None: calls.append(name))
        runner.run_all(run_date=SUNDAY)
        assert calls[:2] == ["bronze_macro_weekly", "bronze_bis_rates"]
        assert calls[2:] == list(DAILY_SEQUENCE)
        assert calls.index("bronze_bis_rates") < calls.index("gatekeeper")

    def test_run_all_on_a_non_weekly_day_is_unchanged(self, monkeypatch):
        import src.runner as runner

        calls: list[str] = []
        monkeypatch.setattr(runner, "run_job", lambda name, force=False, run_date=None: calls.append(name))
        runner.run_all(run_date=TUESDAY)
        assert calls == list(DAILY_SEQUENCE)


# ═════════════════════════════════════════════ GHR-09 ═══════════════════════════════════════════

class TestPauseMarkers:

    def test_round_trip_and_boundaries(self, tmp_path):
        pause_markers.add_pause(["silver"], date(2026, 10, 19), "alignment", "ovi", date(2026, 11, 5), root=tmp_path)
        p = pause_markers.load_pauses(tmp_path)
        assert pause_markers.is_paused(p, "silver", date(2026, 10, 19))
        assert pause_markers.is_paused(p, "silver", date(2027, 1, 1))           # open-ended
        assert not pause_markers.is_paused(p, "silver", date(2026, 10, 18))
        assert not pause_markers.is_paused(p, "gold", date(2026, 10, 20))
        pause_markers.end_pause(["silver"], date(2026, 11, 2), root=tmp_path)
        p = pause_markers.load_pauses(tmp_path)
        assert pause_markers.is_paused(p, "silver", date(2026, 11, 1))
        assert not pause_markers.is_paused(p, "silver", date(2026, 11, 2))      # resumed_on exclusive
        assert p["silver"][0].reason == "alignment" and p["silver"][0].expected_resume == date(2026, 11, 5)

    def test_resume_keeps_the_paused_days_in_history(self, tmp_path):
        pause_markers.add_pause(["gold"], date(2026, 10, 1), "x", root=tmp_path)
        pause_markers.end_pause(["gold"], date(2026, 10, 10), root=tmp_path)
        assert (tmp_path / "gold.json").exists()
        pause_markers.add_pause(["gold"], date(2026, 10, 20), "second", root=tmp_path)   # a second period
        p = pause_markers.load_pauses(tmp_path)
        assert len(p["gold"]) == 2 and pause_markers.is_paused(p, "gold", date(2026, 10, 5))

    def test_refuses_a_second_open_pause_and_a_resume_without_one(self, tmp_path):
        pause_markers.add_pause(["silver"], date(2026, 10, 1), "x", root=tmp_path)
        with pytest.raises(ValueError):
            pause_markers.add_pause(["silver"], date(2026, 10, 2), "y", root=tmp_path)
        with pytest.raises(ValueError):
            pause_markers.end_pause(["gold"], date(2026, 10, 2), root=tmp_path)
        with pytest.raises(ValueError):
            pause_markers.end_pause(["silver"], date(2026, 9, 30), root=tmp_path)   # before the start

    def test_only_layer_scopes_are_pausable(self, tmp_path):
        for bad in (["hmm"], ["all"], []):
            with pytest.raises(ValueError):
                pause_markers.add_pause(bad, date(2026, 10, 1), "x", root=tmp_path)

    def test_unreadable_marker_is_not_paused_and_does_not_raise(self, tmp_path):
        (tmp_path / "silver.json").write_text("{ not json")
        assert pause_markers.load_pauses(tmp_path) == {}


class TestSkippedIsTransparent:

    def test_worse(self):
        assert gk._worse(gk.SKIPPED, "pass") == "pass"
        assert gk._worse("block", gk.SKIPPED) == "block"
        assert gk._worse(gk.SKIPPED, gk.SKIPPED) == gk.SKIPPED

    def test_streak_neither_extends_nor_breaks(self):
        assert gk._emit_status(["pass", "skipped", "skipped", "pass"]) == ("pass", 2)
        assert gk._emit_status(["block", "skipped", "block", "block", "block"]) == ("block", 4)
        assert gk._emit_status(["pass", "skipped", "block"]) == ("warn", 1)       # a real break still breaks

    def test_skipped_today_emits_skipped_with_the_run_length(self):
        assert gk._emit_status(["skipped", "skipped", "pass"]) == ("skipped", 2)

    def test_existing_hysteresis_is_unchanged_without_skipped(self):
        assert gk._emit_status(["block"] * 4) == ("block", 4)
        assert gk._emit_status(["block"] * 3) == ("warn", 3)
        assert gk._emit_status(["pass", "pass"]) == ("pass", 2)


class TestResumeTrap:
    """14 paused days (13 paused + the resume day), then a failing job on the first day back."""

    def _scenario(self, sb, *, with_marker: bool):
        day0 = SUNDAY
        for i in range(14):
            _mark_layer(sb.guard, "bronze", day0 - timedelta(days=i))
        if with_marker:
            pause_markers.add_pause(["silver", "gold"], day0 - timedelta(days=13), "chain alignment")
            pause_markers.end_pause(["silver", "gold"], day0)
        return day0

    def test_failing_first_day_after_a_marked_pause_is_warn_not_block(self, sb):
        day0 = self._scenario(sb, with_marker=True)
        _mark_layer(sb.guard, "silver", day0, skip=("silver_ohlcv",))     # day 1: a Silver job failed
        _mark_layer(sb.guard, "gold", day0)
        _quality(sb.quality_path, [day0])
        rows = {r["scope"]: r for r in gk.compute(day0).to_dicts()}
        assert rows["silver"]["layer_health"] == "block"
        assert rows["silver"]["gate_status"] == "warn"                      # 1 evaluated day < 4
        assert rows["silver"]["streak_days"] == 1
        assert rows["gold"]["gate_status"] == "warn"

    def test_control_without_a_marker_the_same_day_is_an_immediate_block(self, sb):
        """The defect: the paused days recompute as block, so the streak is 14 and block is emitted."""
        day0 = self._scenario(sb, with_marker=False)
        _mark_layer(sb.guard, "silver", day0, skip=("silver_ohlcv",))
        _mark_layer(sb.guard, "gold", day0)
        _quality(sb.quality_path, [day0])
        rows = {r["scope"]: r for r in gk.compute(day0).to_dicts()}
        assert rows["silver"]["gate_status"] == "block"
        assert rows["silver"]["streak_days"] >= gk.BLOCK_CONFIRM_DAYS

    def test_healthy_day_one_is_warn_and_day_two_is_pass(self, sb):
        day0 = self._scenario(sb, with_marker=True)
        for layer in ("silver", "gold"):
            _mark_layer(sb.guard, layer, day0)
        _quality(sb.quality_path, [day0, day0 + timedelta(days=1)])
        rows = {r["scope"]: r for r in gk.compute(day0).to_dicts()}
        assert rows["silver"]["gate_status"] == "warn" and rows["silver"]["streak_days"] == 1
        day1 = day0 + timedelta(days=1)
        for layer in ("bronze", "silver", "gold"):
            _mark_layer(sb.guard, layer, day1)
        rows = {r["scope"]: r for r in gk.compute(day1).to_dicts()}
        assert rows["silver"]["gate_status"] == "pass" and rows["silver"]["streak_days"] == 2

    def test_the_paused_days_are_skipped_in_the_history(self, sb):
        day0 = self._scenario(sb, with_marker=True)
        for layer in ("silver", "gold"):
            _mark_layer(sb.guard, layer, day0)
        _quality(sb.quality_path, [day0])
        _, ev = gk.compute_with_evidence(day0)
        heads = ev.filter(pl.col("job").is_null() & (pl.col("scope") == "silver")).sort("day_offset")
        assert heads["raw_status"].to_list() == ["pass"] + [gk.SKIPPED] * 13

    def test_paused_today_emits_skipped_and_domains_through_the_layer_follow(self, sb):
        sb.healthy(SUNDAY, 14, layers=("bronze",))
        pause_markers.add_pause(["silver"], SUNDAY - timedelta(days=2), "x")
        rows = {r["scope"]: r for r in gk.compute(SUNDAY).to_dicts()}
        assert rows["silver"]["gate_status"] == gk.SKIPPED
        assert rows["bronze"]["gate_status"] != gk.SKIPPED                  # bronze is not paused
        for domain in GATE_DOMAINS:                                         # every chain runs through Silver
            assert rows[domain]["gate_status"] == gk.SKIPPED, domain

    def test_pause_in_another_layer_does_not_touch_bronze(self, sb):
        sb.healthy(SUNDAY, 14, layers=("bronze",))
        pause_markers.add_pause(["gold"], SUNDAY - timedelta(days=5), "x")
        rows = {r["scope"]: r for r in gk.compute(SUNDAY).to_dicts()}
        assert rows["bronze"]["layer_health"] == "pass"


# ═════════════════════════════════════════════ GHR-05 ═══════════════════════════════════════════

class TestEvidenceLog:

    def _run(self, sb, day=SUNDAY):
        sb.healthy(day, 14)
        gk.run(day)

    def test_run_appends_a_part_and_never_rewrites_an_earlier_one(self, sb):
        self._run(sb)
        parts = sorted(sb.log_dir.glob("evaluated_at=*.parquet"))
        assert len(parts) == 1
        first_bytes = parts[0].read_bytes()
        gk.run(SUNDAY)
        parts2 = sorted(sb.log_dir.glob("evaluated_at=*.parquet"))
        assert len(parts2) == 2
        assert parts2[0].read_bytes() == first_bytes

    def test_every_scope_and_day_has_a_header_and_jobs_carry_state_and_mtime(self, sb):
        self._run(sb)
        log = gk.read_gate_log()
        heads = log.filter(pl.col("job").is_null())
        assert heads["scope"].n_unique() == N_SCOPES
        assert heads.group_by("scope").len()["len"].unique().to_list() == [gk.LOOKBACK_DAYS]
        jobs = log.filter(pl.col("job").is_not_null() & (pl.col("day_offset") == 0) & (pl.col("scope") == "gold"))
        assert jobs.height > 0
        assert set(jobs["job_state"].to_list()) <= {gk.JOB_DONE, gk.JOB_PENDING}
        done = jobs.filter(pl.col("job_state") == gk.JOB_DONE)
        assert done["sentinel_mtime"].null_count() == 0

    def test_gate_status_is_derivable_from_the_log_alone(self, sb):
        self._run(sb)
        written = pl.read_parquet(sb.status_path).sort("scope")
        derived = gk.derive_gate_status(gk.read_gate_log(), SUNDAY).sort("scope")
        assert derived.select("run_date", "scope", "layer_health", "gate_status").to_dicts() == \
            written.select("run_date", "scope", "layer_health", "gate_status").to_dicts()
        assert derived["streak_days"].to_list() == written["streak_days"].to_list()

    def test_derive_uses_the_latest_evaluation_of_the_date(self, sb):
        self._run(sb)                                                       # evaluation 1: healthy
        sb.guard.reset_job("silver_ohlcv", SUNDAY)                          # something broke afterwards
        gk.run(SUNDAY)                                                      # evaluation 2
        written = pl.read_parquet(sb.status_path).filter(pl.col("scope") == "silver")
        derived = gk.derive_gate_status(gk.read_gate_log(), SUNDAY).filter(pl.col("scope") == "silver")
        assert written["layer_health"].item() == "block"
        assert derived["layer_health"].item() == "block"
        assert derived["gate_status"].item() == written["gate_status"].item()

    def test_the_emitted_verdict_rides_on_the_offset_zero_header_rows(self, sb):
        self._run(sb)
        log = gk.read_gate_log()
        h0 = log.filter(pl.col("job").is_null() & (pl.col("day_offset") == 0))
        assert h0["gate_status"].null_count() == 0
        assert h0.filter(pl.col("day_offset") > 0).height == 0

    def test_sentinel_cleanup_does_not_rewrite_earlier_evidence(self, sb):
        """GK4: after the sentinels are deleted the next evaluation says block, the log still holds
        what the first evaluation saw."""
        self._run(sb)
        first = sorted(sb.log_dir.glob("evaluated_at=*.parquet"))[0]
        before = pl.read_parquet(first)
        for f in (sb.tmp / ".sentinels").glob("*"):
            f.unlink()
        gk.run(SUNDAY)
        assert pl.read_parquet(first).equals(before)
        old = before.filter(pl.col("job").is_null() & (pl.col("scope") == "bronze") & (pl.col("day_offset") == 3))
        assert old["raw_status"].to_list() == ["pass"]
        now = gk.read_gate_log()
        latest = now["evaluated_at"].max()
        new = now.filter((pl.col("evaluated_at") == latest) & pl.col("job").is_null()
                         & (pl.col("scope") == "bronze") & (pl.col("day_offset") == 3))
        assert new["raw_status"].to_list() == ["block"]

    def test_legacy_gate_status_is_seeded_once(self, sb):
        legacy = pl.DataFrame([
            {"run_date": "2026-10-17", "scope": s, "layer_health": "pass", "gate_status": "warn", "streak_days": 1}
            for s in ("bronze", "silver", "gold")
        ])
        legacy.write_parquet(sb.status_path)
        self._run(sb, date(2026, 10, 18))
        seed = sb.log_dir / "legacy_seed.parquet"
        assert seed.exists()
        log = gk.read_gate_log()
        assert log.filter(pl.col("evidence") == "legacy").height == 3
        got = gk.derive_gate_status(log, date(2026, 10, 17))
        assert got.height == 3 and set(got["gate_status"].to_list()) == {"warn"}
        gk.run(date(2026, 10, 18))                                         # second run: no re-seed
        assert gk.read_gate_log().filter(pl.col("evidence") == "legacy").height == 3

    def test_a_log_write_failure_never_fails_the_gate(self, sb, monkeypatch):
        sb.healthy(SUNDAY, 14)
        monkeypatch.setattr(gk, "_write_gate_log", lambda f: (_ for _ in ()).throw(OSError("disk full")))
        gk.run(SUNDAY)
        assert sb.status_path.exists()

    def test_compute_is_unchanged_in_shape(self, sb):
        sb.healthy(SUNDAY, 14)
        out = gk.compute(SUNDAY)
        assert out.columns == ["run_date", "scope", "layer_health", "gate_status", "streak_days"]
        assert out.height == N_SCOPES
