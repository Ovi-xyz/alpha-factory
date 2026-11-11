"""
tests/integration/test_job_alignment_adr060.py

ADR-060 (GMI-JOB-ALIGN-001) — `runner.py --job all|bronze|silver|gold` are ONE chain.

Measured before the change (job_registry imported, week of 9-15 Nov 2026):

    Mon-Sat   all = 24   bronze+silver+gold = 24   (different members: silver_macro / silver_global_rates
                                                    only under the layer runs; gatekeeper / health_report
                                                    only under `all`)
    Sunday    all = 26   bronze+silver+gold = 34   (10 jobs never run by `all`: the two Silver macro jobs
                                                    and the 8 weekly Gold jobs)

Consequences pinned here: an `all`-only operator hit a 7-day cliff (silver_macro never refreshed ->
silver_validate / gold_raw_* refuse on day 8), the Gatekeeper expected the 8 weekly Gold jobs every Sunday
and read them as missing, health_report could not run after a failure, and `--job gold` on a Sunday put
~100 min of weekly work in front of the daily chain.

Methodology mirrors test_runner_weekly_cadence.py: every job fn stubbed to a recorder (orchestration only),
a sandboxed sentinel directory, originals restored afterwards (JOB_REGISTRY is a shared singleton).
"""

from __future__ import annotations

import sys
from datetime import date, timedelta

import polars as pl
import pytest

import src.runner as runner
import src.utils.gatekeeper as gk
from src.scheduler.dependency_guard import DependencyGuard
from src.scheduler.job_registry import (
    DAILY_SEQUENCE,
    JOB_REGISTRY,
    LAYER_JOB_NAMES,
    WEEKLY_HEAD,
    WEEKLY_ONLY_JOBS,
    WEEKLY_SEQUENCE,
    WEEKLY_TAIL,
    _passes_schedule,
    _split_weekly,
    due_weekly_head_jobs,
    due_weekly_tail_jobs,
    jobs_for,
    layer_jobs_for,
)
from src.utils import data_paths

WEEK = [date(2026, 11, 9) + timedelta(days=i) for i in range(7)]   # Mon .. Sun
TUESDAY, WEDNESDAY, SUNDAY = WEEK[1], WEEK[2], WEEK[6]
UTIL = ["gatekeeper", "health_report"]
LAYERS = ("bronze", "silver", "gold")
DAILY = set(DAILY_SEQUENCE)


def _is_subsequence(part: list[str], whole: list[str]) -> bool:
    it = iter(whole)
    return all(any(x == y for y in it) for x in part)


@pytest.fixture
def stubbed_registry():
    calls: list[str] = []
    originals = {name: job["fn"] for name, job in JOB_REGISTRY.items()}
    for name, job in JOB_REGISTRY.items():
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
    return guard


def _fail_with(name: str, exc: BaseException):
    def _boom(run_date):
        raise exc
    JOB_REGISTRY[name]["fn"] = _boom


# ═════════════════════════════════════ the canonical chain, statically ═══════════════════════════

class TestChainStructure:

    def test_every_sequenced_job_appears_exactly_once(self):
        assert len(WEEKLY_SEQUENCE) == len(set(WEEKLY_SEQUENCE))
        assert set(WEEKLY_SEQUENCE) == set(WEEKLY_ONLY_JOBS) | DAILY
        assert not set(WEEKLY_ONLY_JOBS) & DAILY

    def test_daily_sequence_ends_with_the_util_jobs_in_order(self):
        """The head/daily/tail/util split loses nothing only if util is contiguous at the end."""
        assert DAILY_SEQUENCE[-2:] == UTIL
        assert all(JOB_REGISTRY[j]["layer"] != "util" for j in DAILY_SEQUENCE[:-2])

    def test_chain_is_weekly_head_then_daily_then_weekly_tail_then_util(self):
        assert WEEKLY_SEQUENCE == [*WEEKLY_HEAD, *DAILY_SEQUENCE[:-2], *WEEKLY_TAIL, *UTIL]

    def test_head_and_tail_partition_the_weekly_only_jobs_in_registry_order(self):
        assert not set(WEEKLY_HEAD) & set(WEEKLY_TAIL)
        assert set(WEEKLY_HEAD) | set(WEEKLY_TAIL) == set(WEEKLY_ONLY_JOBS)
        assert [j for j in WEEKLY_ONLY_JOBS if j in WEEKLY_HEAD] == WEEKLY_HEAD
        assert [j for j in WEEKLY_ONLY_JOBS if j in WEEKLY_TAIL] == WEEKLY_TAIL

    def test_the_two_sets_are_exactly_these(self):
        assert WEEKLY_HEAD == ["bronze_macro_weekly", "bronze_bis_rates", "bronze_eia",
                               "silver_macro", "silver_global_rates"]
        assert WEEKLY_TAIL == ["gold_cross_asset_correlation", "gold_lead_lag", "gold_forecast",
                               "gold_hmm_macro_fit", "gold_hmm_market_fit", "gold_hmm_walk_forward",
                               "gold_hmm_xgboost_fit", "gold_technical_xgboost_fit"]

    def test_head_jobs_are_guard_only_and_depend_only_on_head_jobs(self):
        for name in WEEKLY_HEAD:
            job = JOB_REGISTRY[name]
            assert {k for k in job if k.startswith("run_on_")} == {"run_on_weekdays"}, name
            assert set(job.get("depends_on", [])) <= set(WEEKLY_HEAD), name

    def test_every_tail_job_comes_after_the_whole_daily_chain(self):
        last_daily = max(WEEKLY_SEQUENCE.index(j) for j in DAILY_SEQUENCE[:-2])
        assert all(WEEKLY_SEQUENCE.index(j) > last_daily for j in WEEKLY_TAIL)
        assert WEEKLY_SEQUENCE.index("gold_screener") < min(WEEKLY_SEQUENCE.index(j) for j in WEEKLY_TAIL)

    def test_every_dependency_precedes_its_job_in_the_chain(self):
        pos = {j: i for i, j in enumerate(WEEKLY_SEQUENCE)}
        for name in WEEKLY_SEQUENCE:
            for dep in JOB_REGISTRY[name].get("depends_on", []):
                assert pos[dep] < pos[name], f"{name} runs before its dependency {dep}"

    def test_weekly_dependencies_that_are_not_daily_are_covered_by_a_stale_tolerance(self):
        """A job in the chain whose upstream is NOT scheduled that day must carry a stale_tolerance
        for it, otherwise `all` stops at the dependency guard on that day."""
        for day in WEEK:
            chain = set(jobs_for(day))
            for name in chain:
                job = JOB_REGISTRY[name]
                for dep in job.get("depends_on", []):
                    if dep not in chain:
                        assert dep in job.get("stale_tolerance", {}), f"{day:%a}: {name} -> {dep}"

    def test_silver_weekly_jobs_declare_their_sunday_cadence(self):
        for name in ("silver_macro", "silver_global_rates"):
            assert JOB_REGISTRY[name]["run_on_weekdays"] == [6]
            assert name in due_weekly_head_jobs(SUNDAY)
            assert name not in jobs_for(TUESDAY) and name not in jobs_for(WEDNESDAY)
        # their dependents keep the window that makes the off-days work
        assert JOB_REGISTRY["silver_validate"]["stale_tolerance"]["silver_macro"] == 7


# ═══════════════════════════ `all` and the three layer runs are the same list ═══════════════════

class TestAllEqualsTheLayerRuns:

    @pytest.mark.parametrize("day", WEEK, ids=lambda d: d.strftime("%a"))
    def test_all_minus_util_is_exactly_the_union_of_the_layer_lists(self, day):
        chain = jobs_for(day)
        layered = [j for layer in LAYERS for j in layer_jobs_for(layer, day)]
        assert sorted(j for j in chain if JOB_REGISTRY[j]["layer"] != "util") == sorted(layered)
        assert len(layered) == len(set(layered))                         # no job in two layers

    @pytest.mark.parametrize("day", WEEK, ids=lambda d: d.strftime("%a"))
    def test_each_layer_list_is_an_ordered_subsequence_of_all(self, day):
        chain = jobs_for(day)
        for layer in LAYERS:
            assert _is_subsequence(layer_jobs_for(layer, day), chain), (day, layer)

    @pytest.mark.parametrize("day", WEEK, ids=lambda d: d.strftime("%a"))
    def test_the_chain_contains_the_daily_sequence_in_order_and_ends_with_util(self, day):
        chain = jobs_for(day)
        assert _is_subsequence(DAILY_SEQUENCE, chain)
        assert chain[-2:] == UTIL
        assert len(chain) == len(set(chain))

    def test_job_counts_per_weekday(self):
        """The measured defect, now fixed: Sunday all == layers + util, and the other days match too."""
        for day in WEEK:
            layered = sum(len(layer_jobs_for(layer, day)) for layer in LAYERS)
            assert len(jobs_for(day)) == layered + len(UTIL), day
        assert len(jobs_for(SUNDAY)) == 36 and len(jobs_for(TUESDAY)) == 24 and len(jobs_for(WEDNESDAY)) == 25

    def test_a_sunday_all_run_includes_what_it_never_ran_before(self):
        chain = set(jobs_for(SUNDAY))
        assert {"silver_macro", "silver_global_rates", *WEEKLY_TAIL} <= chain

    def test_every_weekly_job_is_reachable_on_some_weekday(self):
        reachable = set().union(*(set(jobs_for(d)) for d in WEEK))
        assert set(WEEKLY_ONLY_JOBS) <= reachable

    def test_due_helpers_agree_with_the_chain(self):
        for day in WEEK:
            chain = jobs_for(day)
            assert due_weekly_head_jobs(day) == [j for j in chain if j in WEEKLY_HEAD]
            assert due_weekly_tail_jobs(day) == [j for j in chain if j in WEEKLY_TAIL]

    def test_gold_layer_on_sunday_runs_the_weekly_tail_after_the_daily_gold_jobs(self):
        gold = layer_jobs_for("gold", SUNDAY)
        assert gold[:len(gold) - len(WEEKLY_TAIL)] == [j for j in DAILY_SEQUENCE if JOB_REGISTRY[j]["layer"] == "gold"]
        assert gold[-len(WEEKLY_TAIL):] == WEEKLY_TAIL

    def test_layer_membership_is_unchanged_by_the_reorder(self):
        for layer in LAYERS:
            assert set(LAYER_JOB_NAMES[layer]) == {j for j in WEEKLY_SEQUENCE if JOB_REGISTRY[j]["layer"] == layer}


# ═════════════════════ an `all`-only operator never hits the 7-day cliff ════════════════════════

class TestAllOnlyOperatorIsSelfSufficient:

    def test_four_weeks_of_all_without_a_single_layer_run(self, stubbed_registry, sandboxed_guard):
        """From an empty sentinel directory on a Sunday, run only `--job all`, every day, for 28 days.
        Before ADR-060 silver_macro was never refreshed, so silver_validate refused on day 8."""
        start = date(2026, 11, 8)
        assert start.weekday() == 6
        for i in range(28):
            day = start + timedelta(days=i)
            runner.run_all(run_date=day)                                   # must not raise
            assert sandboxed_guard.is_done("health_report", day)
        for k in range(4):
            sunday = start + timedelta(days=7 * k)
            for name in ("silver_macro", "silver_global_rates", *WEEKLY_TAIL):
                assert sandboxed_guard.is_done(name, sunday), (sunday, name)
            for off in range(1, 7):
                assert not sandboxed_guard.is_done("silver_macro", sunday + timedelta(days=off))

    def test_the_old_chain_would_have_failed_on_day_eight(self, stubbed_registry, sandboxed_guard):
        """Control for the test above: run only the DAILY_SEQUENCE (what `all` used to run) for the same
        28 days and watch it die at silver_validate once the seeded silver_macro sentinel ages out."""
        sandboxed_guard.mark_done("silver_macro", date(2026, 11, 8))
        for name in ("bronze_macro_weekly", "bronze_bis_rates"):
            sandboxed_guard.mark_done(name, date(2026, 11, 8))
        failed_on = None
        for i in range(1, 28):
            day = date(2026, 11, 8) + timedelta(days=i)
            try:
                for name in DAILY_SEQUENCE:
                    runner.run_job(name, run_date=day)
            except SystemExit:
                failed_on = i
                break
        assert failed_on == 8


# ════════════════════ the head/tail rule itself, on a synthetic registry ════════════════════════
# (today every weekly job is guard-only, so the guard clause of the rule is invisible in the real
#  registry — these cases pin each clause on its own)

class TestSplitRule:

    @staticmethod
    def _reg(**jobs):
        return {name: dict(spec) for name, spec in jobs.items()}

    def test_a_guard_only_job_with_no_dependencies_is_head(self):
        reg = self._reg(a={"run_on_weekdays": [6]})
        assert _split_weekly(["a"], reg) == (["a"], [])

    def test_a_job_whose_parent_is_head_is_head(self):
        reg = self._reg(a={"run_on_weekdays": [6]}, b={"run_on_weekdays": [6], "depends_on": ["a"]})
        assert _split_weekly(["a", "b"], reg) == (["a", "b"], [])

    def test_a_job_with_a_daily_or_unknown_parent_is_tail(self):
        reg = self._reg(a={"run_on_weekdays": [6], "depends_on": ["some_daily_job"]})
        assert _split_weekly(["a"], reg) == ([], ["a"])

    def test_a_job_whose_parent_is_tail_is_tail(self):
        reg = self._reg(a={"run_on_weekdays": [6], "depends_on": ["daily"]},
                        b={"run_on_weekdays": [6], "depends_on": ["a"]})
        assert _split_weekly(["a", "b"], reg) == ([], ["a", "b"])

    def test_a_parent_listed_after_its_child_does_not_make_the_child_head(self):
        """The head is built in list order: a dependency that has not been placed yet cannot satisfy it."""
        reg = self._reg(b={"run_on_weekdays": [6], "depends_on": ["a"]}, a={"run_on_weekdays": [6]})
        assert _split_weekly(["b", "a"], reg) == (["a"], ["b"])

    @pytest.mark.parametrize("guard", [
        {"run_on_day_of_month": [10, 11]},
        {"run_on_weekdays": [6], "run_on_months": [1, 4]},
        {},
    ], ids=["day-of-month only", "weekday plus months", "no guard at all"])
    def test_anything_other_than_a_weekday_only_guard_is_tail(self, guard):
        reg = self._reg(a=guard)
        assert _split_weekly(["a"], reg) == ([], ["a"])

    def test_order_is_preserved_inside_each_side(self):
        reg = self._reg(h1={"run_on_weekdays": [6]}, t1={"run_on_weekdays": [6], "depends_on": ["d"]},
                        h2={"run_on_weekdays": [2]}, t2={})
        head, tail = _split_weekly(["h1", "t1", "h2", "t2"], reg)
        assert head == ["h1", "h2"] and tail == ["t1", "t2"]


# ═════════════════════════════ failure semantics of the shared chain ═════════════════════════════

class TestChainFailureSemantics:

    def test_a_failed_tail_job_does_not_skip_the_other_tail_jobs(self, stubbed_registry, sandboxed_guard):
        _fail_with("gold_hmm_macro_fit", RuntimeError("fit exploded"))
        with pytest.raises(RuntimeError, match="fit exploded"):
            runner.run_all(run_date=SUNDAY)
        for name in ("gold_hmm_market_fit", "gold_hmm_walk_forward", "gold_hmm_xgboost_fit",
                     "gold_technical_xgboost_fit", "gold_cross_asset_correlation", "gold_forecast"):
            assert name in stubbed_registry, name
        assert stubbed_registry[-2:] == UTIL
        assert not sandboxed_guard.is_done("gold_hmm_macro_fit", SUNDAY)

    def test_a_tail_job_whose_own_dependency_failed_is_still_refused(self, stubbed_registry, sandboxed_guard):
        _fail_with("gold_cross_asset_correlation", RuntimeError("corr exploded"))
        with pytest.raises(RuntimeError, match="corr exploded"):
            runner.run_all(run_date=SUNDAY)
        assert "gold_lead_lag" not in stubbed_registry and "gold_forecast" not in stubbed_registry
        assert "gold_hmm_macro_fit" in stubbed_registry and "gold_technical_xgboost_fit" in stubbed_registry

    def test_the_first_failure_is_the_one_raised(self, stubbed_registry, sandboxed_guard):
        _fail_with("gold_hmm_macro_fit", RuntimeError("first"))
        _fail_with("gold_technical_xgboost_fit", ValueError("second"))
        with pytest.raises(RuntimeError, match="first"):
            runner.run_all(run_date=SUNDAY)

    def test_a_daily_failure_skips_the_whole_tail_but_not_util(self, stubbed_registry, sandboxed_guard):
        _fail_with("gold_screener", RuntimeError("screener exploded"))
        with pytest.raises(RuntimeError, match="screener exploded"):
            runner.run_all(run_date=SUNDAY)
        assert not set(WEEKLY_TAIL) & set(stubbed_registry)
        assert stubbed_registry[-2:] == UTIL

    def test_a_head_failure_skips_everything_but_util(self, stubbed_registry, sandboxed_guard):
        _fail_with("bronze_macro_weekly", RuntimeError("macro exploded"))
        with pytest.raises(RuntimeError, match="macro exploded"):
            runner.run_all(run_date=SUNDAY)
        assert stubbed_registry == UTIL

    def test_health_report_runs_after_a_failure(self, stubbed_registry, sandboxed_guard):
        sandboxed_guard.mark_done("silver_macro", TUESDAY)
        _fail_with("silver_ohlcv", RuntimeError("silver exploded"))
        with pytest.raises(RuntimeError):
            runner.run_all(run_date=TUESDAY)
        assert stubbed_registry[-2:] == UTIL

    def test_keyboard_interrupt_in_the_tail_stops_everything(self, stubbed_registry, sandboxed_guard):
        _fail_with("gold_hmm_market_fit", KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt):
            runner.run_all(run_date=SUNDAY)
        assert "gold_hmm_walk_forward" not in stubbed_registry and not (set(UTIL) & set(stubbed_registry))


class TestLayerRunFollowsTheSameChain:

    @pytest.fixture
    def recorded(self, monkeypatch):
        calls: list[str] = []
        monkeypatch.setattr(runner, "run_job", lambda name, force=False, run_date=None: calls.append(name))
        return calls

    def test_without_force_a_layer_run_is_the_scheduled_slice_of_all(self, recorded):
        for day in (TUESDAY, WEDNESDAY, SUNDAY):
            for layer in LAYERS:
                recorded.clear()
                runner.run_layer(layer, run_date=day)
                assert recorded == layer_jobs_for(layer, day), (day, layer)

    def test_with_force_a_layer_run_lists_every_member(self, recorded):
        for layer in LAYERS:
            recorded.clear()
            runner.run_layer(layer, force=True, run_date=TUESDAY)
            assert recorded == LAYER_JOB_NAMES[layer]

    def test_the_three_layer_runs_cover_what_all_runs_in_the_same_order(self, recorded):
        runner.run_all(run_date=SUNDAY)
        all_calls = list(recorded)
        layered: list[str] = []
        for layer in LAYERS:
            recorded.clear()
            runner.run_layer(layer, run_date=SUNDAY)
            layered += recorded
        assert [j for j in all_calls if j not in UTIL] and sorted(layered) == sorted(j for j in all_calls if j not in UTIL)
        for layer in LAYERS:
            assert _is_subsequence([j for j in layered if JOB_REGISTRY[j]["layer"] == layer], all_calls)

    def test_a_gold_tail_failure_does_not_skip_the_other_tail_jobs_in_a_layer_run(self, stubbed_registry, sandboxed_guard):
        # every upstream of the gold layer on a Sunday, as the earlier layers would have left them
        for layer in ("bronze", "silver"):
            for name in layer_jobs_for(layer, SUNDAY):
                sandboxed_guard.mark_done(name, SUNDAY)
        _fail_with("gold_hmm_macro_fit", RuntimeError("fit exploded"))
        with pytest.raises(RuntimeError, match="fit exploded"):
            runner.run_layer("gold", run_date=SUNDAY)
        assert "gold_technical_xgboost_fit" in stubbed_registry

    def test_a_daily_gold_failure_skips_the_tail_in_a_layer_run(self, stubbed_registry, sandboxed_guard):
        for layer in ("bronze", "silver"):
            for name in layer_jobs_for(layer, SUNDAY):
                sandboxed_guard.mark_done(name, SUNDAY)
        _fail_with("gold_mtf", RuntimeError("mtf exploded"))
        with pytest.raises(RuntimeError, match="mtf exploded"):
            runner.run_layer("gold", run_date=SUNDAY)
        assert not set(WEEKLY_TAIL) & set(stubbed_registry)


# ═════════════════════════ post-steps `all` always had, now for `--job <layer>` ═════════════════

class TestLayerCommandPostSteps:

    @pytest.fixture
    def rig(self, monkeypatch):
        class Rig:
            jobs: list[str] = []
            views: list[date] = []
            layer_error: BaseException | None = None
            gate_error: BaseException | None = None

        r = Rig()
        r.jobs, r.views = [], []

        def fake_layer(layer, force=False, run_date=None):
            if r.layer_error is not None:
                raise r.layer_error

        def fake_job(name, force=False, run_date=None):
            r.jobs.append(name)
            if name == "gatekeeper" and r.gate_error is not None:
                raise r.gate_error

        monkeypatch.setattr(runner, "run_layer", fake_layer)
        monkeypatch.setattr(runner, "run_job", fake_job)
        monkeypatch.setattr(runner, "_register_views", lambda d: r.views.append(d))
        return r

    def test_completed_gold_run_gates_and_registers_views(self, rig):
        runner.run_layer_command("gold", force=False, run_date=TUESDAY)
        assert rig.jobs == ["gatekeeper"] and rig.views == [TUESDAY]

    @pytest.mark.parametrize("layer", ["bronze", "silver"])
    def test_completed_earlier_layer_does_neither(self, rig, layer):
        """A half-finished day must not be recorded as missing/block for the layers not yet run."""
        runner.run_layer_command(layer, force=False, run_date=TUESDAY)
        assert rig.jobs == [] and rig.views == []

    @pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
    def test_failed_layer_run_records_the_bad_day_and_reraises_unchanged(self, rig, layer):
        rig.layer_error = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_layer_command(layer, force=False, run_date=TUESDAY)
        assert rig.jobs == ["gatekeeper"] and rig.views == []

    def test_a_dependency_system_exit_is_reraised_with_its_code(self, rig):
        rig.layer_error = SystemExit(1)
        with pytest.raises(SystemExit) as ei:
            runner.run_layer_command("silver", force=False, run_date=TUESDAY)
        assert ei.value.code == 1 and rig.jobs == ["gatekeeper"]

    def test_no_gate_skips_the_gatekeeper_only(self, rig):
        runner.run_layer_command("gold", force=False, run_date=TUESDAY, gate=False)
        assert rig.jobs == [] and rig.views == [TUESDAY]
        rig.layer_error = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            runner.run_layer_command("silver", force=False, run_date=TUESDAY, gate=False)
        assert rig.jobs == []

    def test_a_failing_gatekeeper_never_masks_the_layer_outcome(self, rig):
        rig.gate_error = RuntimeError("gate exploded")
        runner.run_layer_command("gold", force=False, run_date=TUESDAY)       # success stays success
        rig.layer_error = ValueError("layer failed")
        with pytest.raises(ValueError, match="layer failed"):                # failure keeps ITS error
            runner.run_layer_command("gold", force=False, run_date=TUESDAY)

    def test_keyboard_interrupt_triggers_no_post_step(self, rig):
        rig.layer_error = KeyboardInterrupt()
        with pytest.raises(KeyboardInterrupt):
            runner.run_layer_command("gold", force=False, run_date=TUESDAY)
        assert rig.jobs == [] and rig.views == []


class TestCliWiring:

    def _argv(self, monkeypatch, *args):
        monkeypatch.setattr(sys, "argv", ["runner.py", *args])

    def test_no_gate_flag_defaults_to_off(self, monkeypatch):
        self._argv(monkeypatch, "--job", "gold")
        assert runner.parse_args().no_gate is False
        self._argv(monkeypatch, "--job", "gold", "--no-gate")
        assert runner.parse_args().no_gate is True

    @pytest.mark.parametrize("extra,gate", [([], True), (["--no-gate"], False)])
    def test_main_passes_the_gate_choice_to_the_layer_command(self, monkeypatch, extra, gate):
        seen: dict = {}
        monkeypatch.setattr(runner, "run_layer_command", lambda layer, **kw: seen.update(layer=layer, **kw))
        self._argv(monkeypatch, "--job", "silver", "--date", "2026-11-10", *extra)
        runner.main()
        assert seen == {"layer": "silver", "force": False, "run_date": date(2026, 11, 10), "gate": gate}

    def test_main_all_still_registers_views_after_a_full_run(self, monkeypatch):
        order: list[str] = []
        monkeypatch.setattr(runner, "run_all", lambda **kw: order.append("all"))
        monkeypatch.setattr(runner, "_register_views", lambda d: order.append("views"))
        self._argv(monkeypatch, "--job", "all", "--date", "2026-11-10")
        runner.main()
        assert order == ["all", "views"]


# ═══════════════════ the specialists' quality tag is read before the Sunday tail ════════════════

def _quality(path, days) -> None:
    rows = [{"run_date": d.isoformat(), "check_name": c, "tier": t, "passed": True,
             "detail": None, "logged_at": d.isoformat()}
            for d in days for c, t in (("null_check", "critical"), ("price_sanity", "critical"),
                                       ("gap_detection", "warning"))]
    pl.DataFrame(rows).write_parquet(path)


@pytest.fixture
def gate_sb(tmp_path, monkeypatch):
    monkeypatch.setenv(data_paths.ENV_VAR, str(tmp_path))
    guard = DependencyGuard(sentinel_dir=tmp_path / ".sentinels")
    monkeypatch.setattr(gk, "DependencyGuard", lambda: guard)
    monkeypatch.setattr(gk, "QUALITY_GATE_LOG_PATH", tmp_path / "quality_gate_log.parquet")
    monkeypatch.setattr(gk, "GATE_STATUS_PATH", tmp_path / "gate_status.parquet")
    monkeypatch.setattr(gk, "GATE_LOG_DIR", tmp_path / "gate_log")
    days = [SUNDAY - timedelta(days=i) for i in range(14)]
    for d in days:
        for layer in LAYERS:
            for name in LAYER_JOB_NAMES[layer]:
                if _passes_schedule(JOB_REGISTRY[name], d):
                    guard.mark_done(name, d)
    _quality(tmp_path / "quality_gate_log.parquet", days)

    class SB:
        pass

    o = SB()
    o.guard = guard
    return o


def _tail_not_yet_run(sb) -> None:
    """Sunday as the daily chain sees it: head and daily jobs done, the weekly tail still to come."""
    for name in WEEKLY_TAIL:
        sb.guard.reset_job(name, SUNDAY)


class TestPredictTimeGateBeforeTheTail:

    def test_tail_members_are_pending_at_predict_time_not_missing(self, gate_sb):
        _tail_not_yet_run(gate_sb)
        ql = gk._read_quality_log()
        assert set(WEEKLY_TAIL) & set(gk.domain_members("hmm", SUNDAY))
        assert gk._domain_raw_status("hmm", gate_sb.guard, SUNDAY, DAILY, ql) == "block"          # as before
        assert gk._domain_raw_status("hmm", gate_sb.guard, SUNDAY, DAILY, ql, before_weekly_tail=True) != "block"

    def test_only_the_evaluation_day_is_softened(self, gate_sb):
        """Past days are final: the same missing tail job on an earlier day still blocks."""
        _tail_not_yet_run(gate_sb)
        ql = gk._read_quality_log()
        assert gk._domain_raw_status("hmm", gate_sb.guard, SUNDAY, DAILY, ql,
                                     before_weekly_tail=True, day_offset=1) == "block"

    def test_it_does_not_hide_a_missing_head_or_daily_member(self, gate_sb):
        _tail_not_yet_run(gate_sb)
        ql = gk._read_quality_log()
        for name in ("bronze_macro_weekly", "silver_macro", "gold_raw_macro"):
            gate_sb.guard.reset_job(name, SUNDAY)
            assert gk._domain_raw_status("hmm", gate_sb.guard, SUNDAY, DAILY, ql,
                                         before_weekly_tail=True) == "block", name
            gate_sb.guard.mark_done(name, SUNDAY)

    def test_domain_gate_status_is_what_the_specialists_stamp(self, gate_sb):
        _tail_not_yet_run(gate_sb)
        assert gk.domain_gate_status("hmm", SUNDAY) == "pass"
        assert gk.domain_status("hmm", SUNDAY)["gate_status"] != "pass"      # the Gatekeeper's own reading

    def test_the_gatekeeper_itself_still_sees_a_missing_tail_job_after_the_tail(self, gate_sb):
        """The Gatekeeper runs AFTER the tail in the chain, so a tail job that did not finish is missing."""
        gate_sb.guard.reset_job("gold_technical_xgboost_fit", SUNDAY)         # the others finished
        gate_sb.guard.reset_job("gold_hmm_walk_forward", SUNDAY)
        ql = gk._read_quality_log()
        assert gk._domain_raw_status("hmm", gate_sb.guard, SUNDAY, DAILY, ql, allow_pending=True) == "block"
