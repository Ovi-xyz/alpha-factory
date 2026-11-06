"""
tests/unit/test_gap_baseline_ghr06_adr059.py — GHR-06 (ADR-059): baseline-aware severity.

GK1: `context_gap_detection` was the only failing check on 16/17/18 Oct (same detail every day), is
warning-tier, so Silver raw = warn and Gold inherited it — the gate could not reach `pass`, and a chronic
red hides the day a NEW gap appears. A stable known count is `info`; anything that moves is `warn`.
"""

from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest
from loguru import logger

import src.silver.quality_validator as qv_mod
import src.utils.gatekeeper as gk
import src.utils.quality_baselines as qb
from src.utils.gap_analysis import GapClassification, GapEvent
from src.utils.quality_baselines import Baseline, gap_severity, load_baseline

TODAY = date(2026, 11, 5)


@pytest.fixture
def warnings_seen():
    seen: list[str] = []
    hid = logger.add(lambda m: seen.append(str(m)), level="WARNING")
    yield seen
    logger.remove(hid)


def _yaml(tmp_path, body: str):
    p = tmp_path / "quality_baselines.yaml"
    p.write_text(body)
    return p


VALID = """baselines:
  context_gap_detection:
    count: 72
    date: 2026-10-31
    justification: "JKSE/TWSE holiday calendars, reviewed"
    expires: 2027-01-31
"""


class TestLoadBaseline:

    def test_absent_file_and_empty_baselines_mean_none(self, tmp_path):
        assert load_baseline("context_gap_detection", TODAY, tmp_path / "nope.yaml") is None
        assert load_baseline("context_gap_detection", TODAY, _yaml(tmp_path, "baselines: {}\n")) is None

    def test_valid_entry(self, tmp_path):
        b = load_baseline("context_gap_detection", TODAY, _yaml(tmp_path, VALID))
        assert b == Baseline("context_gap_detection", 72, date(2026, 10, 31),
                             "JKSE/TWSE holiday calendars, reviewed", date(2027, 1, 31))

    def test_other_checks_are_not_affected(self, tmp_path):
        assert load_baseline("price_sanity", TODAY, _yaml(tmp_path, VALID)) is None

    def test_expiry_boundary_and_warning(self, tmp_path, warnings_seen):
        p = _yaml(tmp_path, VALID)
        assert load_baseline("context_gap_detection", date(2027, 1, 31), p) is not None    # last active day
        assert not warnings_seen
        assert load_baseline("context_gap_detection", date(2027, 2, 1), p) is None         # expired
        assert any("EXPIRED" in m for m in warnings_seen)

    @pytest.mark.parametrize("field", ["count", "date", "justification", "expires"])
    def test_a_missing_field_means_not_active_and_says_so(self, tmp_path, warnings_seen, field):
        lines = [l for l in VALID.splitlines() if f"    {field}:" not in l]
        assert load_baseline("context_gap_detection", TODAY, _yaml(tmp_path, "\n".join(lines) + "\n")) is None
        assert any("incomplete or invalid" in m for m in warnings_seen)

    @pytest.mark.parametrize("bad", ["count: -1", "count: true", 'count: "72"', "count: 7.5"])
    def test_invalid_counts(self, tmp_path, bad):
        body = VALID.replace("count: 72", bad)
        assert load_baseline("context_gap_detection", TODAY, _yaml(tmp_path, body)) is None

    def test_expires_before_date_is_invalid_even_when_not_yet_past(self, tmp_path):
        """as_of is BEFORE the (nonsensical) expiry, so the expiry branch cannot be what rejects it."""
        body = VALID.replace("expires: 2027-01-31", "expires: 2026-10-01")      # date: 2026-10-31
        assert load_baseline("context_gap_detection", date(2026, 9, 15), _yaml(tmp_path, body)) is None

    def test_garbage_yaml_does_not_raise(self, tmp_path):
        assert load_baseline("context_gap_detection", TODAY, _yaml(tmp_path, "baselines: [unclosed")) is None

    def test_the_shipped_config_activates_nothing(self):
        assert load_baseline("context_gap_detection", TODAY, qb.BASELINES_PATH) is None


B72 = Baseline("context_gap_detection", 72, date(2026, 10, 31), "j", date(2027, 1, 31))
K = [f"S{i}@2026-01-0{(i % 9) + 1}" for i in range(72)]


class TestGapSeverity:

    def test_no_baseline_is_exactly_the_legacy_rule(self):
        assert gap_severity(50, None, None, None)[0] == "ok"
        assert gap_severity(51, None, None, None)[0] == "warn"
        # new gaps alone never escalate without a baseline (nothing is "known" to be new against)
        assert gap_severity(3, ["a@x", "b@x", "c@x"], ["a@x"], None)[0] == "ok"

    def test_stable_known_count_is_info(self):
        assert gap_severity(72, K, K, B72) == ("info", [])

    def test_a_smaller_subset_is_still_info(self):
        assert gap_severity(10, K[:10], K, B72)[0] == "info"

    def test_larger_count_warns(self):
        assert gap_severity(73, K + ["X@2026-02-01"], K, B72)[0] == "warn"

    def test_one_new_gap_warns_even_when_the_count_is_unchanged(self):
        cur = K[:-1] + ["NEW@2026-05-05"]
        sev, new = gap_severity(72, cur, K, B72)
        assert sev == "warn" and new == ["NEW@2026-05-05"]

    def test_zero_is_ok_not_info(self):
        assert gap_severity(0, [], K, B72)[0] == "ok"

    def test_no_previous_run_means_nothing_is_new(self):
        assert gap_severity(72, K, None, B72) == ("info", [])

    def test_unavailable_identities_fall_back_to_the_count_only(self):
        assert gap_severity(72, None, K, B72)[0] == "info"
        assert gap_severity(73, None, K, B72)[0] == "warn"

    def test_a_stable_count_above_the_legacy_threshold_is_info_under_a_baseline(self):
        assert gap_severity(72, K, K, B72)[0] == "info"           # 72 > 50 would be warn without one


def _ev(sym: str, end: str) -> GapEvent:
    return GapEvent(symbol=sym, prev_ts=date(2026, 1, 1), ts=date.fromisoformat(end), day_gap=6, peer_group="g")


@pytest.fixture
def validator(tmp_path, monkeypatch):
    monkeypatch.setattr(qv_mod, "QUALITY_GATE_LOG_PATH", tmp_path / "quality_gate_log.parquet")
    monkeypatch.setattr(qv_mod, "context_glob", lambda root, pat: "x")
    monkeypatch.setattr(qv_mod, "context_peer_groups", lambda: ({}, {}))
    monkeypatch.setattr(qv_mod, "CalendarOracle", SimpleNamespace(from_config=lambda: SimpleNamespace(
        status=lambda: {"failed_calendars": [], "unknown_symbols": set()})))

    class H:
        def __init__(self):
            self.log = tmp_path / "quality_gate_log.parquet"
            self.cfg = tmp_path / "quality_baselines.yaml"
            monkeypatch.setattr(qb, "BASELINES_PATH", self.cfg)

        def run(self, run_date: date, events: list[GapEvent]):
            qv = qv_mod.QualityValidator()

            def fake(con, glob, peer_map, peer_sizes, threshold_days=5, oracle=None):
                qv._last_gap_classification = GapClassification(isolated=list(events))
                return len(events), 0, {}

            qv._classified_gap_counts = fake
            ok = qv._check_context_gap_detection(run_date)
            qv_mod._persist_quality_gate_log(run_date, {"context_gap_detection": ok}, qv._issues, qv._gap_info)
            return ok, qv

    return H()


class TestValidatorIntegration:

    def test_without_a_baseline_behaviour_is_unchanged(self, validator):
        ok, qv = validator.run(date(2026, 11, 2), [_ev(f"S{i}", "2026-01-05") for i in range(3)])
        assert ok is True and qv._gap_info["severity_effective"] == "ok"
        ok, qv = validator.run(date(2026, 11, 3), [_ev(f"S{i}", "2026-01-05") for i in range(51)])
        assert ok is False and qv._issues[0]["check"] == "context_gap_detection"

    def test_stable_known_count_passes_as_info_and_a_swapped_gap_warns(self, validator):
        validator.cfg.write_text(VALID)
        base = [_ev(f"S{i}", "2026-01-05") for i in range(72)]
        ok, qv = validator.run(date(2026, 11, 2), base)                 # first run: nothing to compare with
        assert ok is True and qv._gap_info["severity_effective"] == "info"
        assert qv._gap_info["detail"].startswith("INFO")
        ok, qv = validator.run(date(2026, 11, 3), base)                 # stable
        assert ok is True and qv._gap_info["new_isolated_gaps"] == 0
        swapped = base[:-1] + [_ev("NEWSYM", "2026-03-03")]             # same count, one NEW gap
        ok, qv = validator.run(date(2026, 11, 4), swapped)
        assert ok is False and qv._gap_info["new_isolated_gaps"] == 1
        assert "1 new vs the previous run" in qv._issues[0]["detail"] and "baseline 72" in qv._issues[0]["detail"]

    def test_a_count_above_the_baseline_warns(self, validator):
        validator.cfg.write_text(VALID)
        ok, _ = validator.run(date(2026, 11, 2), [_ev(f"S{i}", "2026-01-05") for i in range(73)])
        assert ok is False

    def test_an_expired_baseline_falls_back_to_the_legacy_threshold(self, validator, warnings_seen):
        validator.cfg.write_text(VALID.replace("expires: 2027-01-31", "expires: 2026-11-01"))
        ok, qv = validator.run(date(2026, 11, 2), [_ev(f"S{i}", "2026-01-05") for i in range(72)])
        assert ok is False and qv._gap_info["baseline_count"] is None      # 72 > 50 again
        assert any("EXPIRED" in m for m in warnings_seen)

    def test_the_persisted_row_carries_the_new_columns_and_nothing_leaks_to_other_checks(self, validator):
        validator.cfg.write_text(VALID)
        validator.run(date(2026, 11, 2), [_ev(f"S{i}", "2026-01-05") for i in range(72)])
        log = pl.read_parquet(validator.log)
        row = log.filter(pl.col("check_name") == "context_gap_detection").to_dicts()[0]
        assert (row["isolated_count"], row["baseline_count"], row["severity_effective"]) == (72, 72, "info")
        assert len(json.loads(row["gap_keys"])) == 72 and row["passed"] is True
        assert row["detail"].startswith("INFO")

    def test_previous_keys_ignore_the_same_day_and_logs_that_predate_the_columns(self, validator, tmp_path):
        qv = qv_mod.QualityValidator()
        assert qv._previous_gap_keys(date(2026, 11, 3)) is None                       # no log at all
        pl.DataFrame([{"run_date": "2026-11-02", "check_name": "context_gap_detection", "tier": "warning",
                       "passed": True, "detail": None, "logged_at": "x"}]).write_parquet(validator.log)
        assert qv._previous_gap_keys(date(2026, 11, 3)) is None                       # old schema: no gap_keys
        validator.cfg.write_text(VALID)
        validator.run(date(2026, 11, 3), [_ev("A", "2026-01-05")])
        assert qv._previous_gap_keys(date(2026, 11, 3)) is None                       # same day never compares to itself
        assert qv._previous_gap_keys(date(2026, 11, 4)) == ["A@2026-01-05"]

    def test_info_is_not_a_gate_warning_but_warn_is(self, validator):
        """_quality_local_status maps only a FAILED warning-tier check to warn; `info` passes."""
        validator.cfg.write_text(VALID)
        base = [_ev(f"S{i}", "2026-01-05") for i in range(72)]
        validator.run(date(2026, 11, 2), base)
        log = pl.read_parquet(validator.log)
        assert gk._quality_local_status(log, date(2026, 11, 2)) == "pass"
        validator.run(date(2026, 11, 3), base + [_ev("EXTRA", "2026-02-02")])
        log = pl.read_parquet(validator.log)
        assert gk._quality_local_status(log, date(2026, 11, 3)) == "warn"

    def test_a_stubbed_classification_without_identities_still_works(self, validator, monkeypatch):
        qv = qv_mod.QualityValidator()
        qv._classified_gap_counts = lambda *a, **k: (3, 0, {})            # leaves _last_gap_classification None
        assert qv._check_context_gap_detection(date(2026, 11, 2)) is True
        assert qv._gap_info["gap_keys"] is None and qv._gap_info["severity_effective"] == "ok"
