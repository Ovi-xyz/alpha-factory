"""
tests/unit/test_gatekeeper_wiring_adr050.py

FIX GMI-GATE-001 (ADR-050): wiring of the Gatekeeper into the pipeline —
JOB_REGISTRY / DAILY_SEQUENCE placement, and health_report's soft
surfacing of gate status. The Gatekeeper's own logic is covered in
test_gatekeeper_adr050.py; this file covers only how it is hooked in.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import src.utils.health_reporter as hr
from src.scheduler.job_registry import (
    DAILY_SEQUENCE,
    JOB_REGISTRY,
    LAYER_JOB_NAMES,
    WEEKLY_SEQUENCE,
)


class TestJobRegistryWiring:

    def test_gatekeeper_registered(self):
        assert "gatekeeper" in JOB_REGISTRY

    def test_registry_entry_has_required_fields(self):
        job = JOB_REGISTRY["gatekeeper"]
        for field in ("description", "fn", "depends_on", "layer", "est_minutes"):
            assert field in job
        assert callable(job["fn"])

    def test_layer_is_util_so_it_never_grades_itself(self):
        """layer='util' keeps it out of LAYER_JOB_NAMES — Gatekeeper must
        never count its own sentinel as an 'expected job' in the layers it
        is grading (and `--job bronze|silver|gold` must not run it)."""
        assert JOB_REGISTRY["gatekeeper"]["layer"] == "util"
        for names in LAYER_JOB_NAMES.values():
            assert "gatekeeper" not in names

    def test_no_hard_dependency(self):
        """Deliberate (see the registry-entry comment): a hard dependency on
        gold_screener would make it unrunnable on the failed days its
        output matters most."""
        assert JOB_REGISTRY["gatekeeper"]["depends_on"] == []

    def test_sequenced_immediately_before_health_report(self):
        i = DAILY_SEQUENCE.index("gatekeeper")
        assert DAILY_SEQUENCE[i + 1] == "health_report"

    def test_gatekeeper_after_gold_screener(self):
        assert DAILY_SEQUENCE.index("gatekeeper") > DAILY_SEQUENCE.index("gold_screener")

    def test_present_exactly_once_in_weekly_sequence(self):
        assert WEEKLY_SEQUENCE.count("gatekeeper") == 1

    def test_health_report_dependency_unchanged(self):
        """health_report reads gate status softly — it must not hard-depend
        on the Gatekeeper (a reporting/alerting job shouldn't be blocked by
        a companion job's failure)."""
        assert JOB_REGISTRY["health_report"]["depends_on"] == ["gold_screener"]


@pytest.fixture
def gate_file(tmp_path, monkeypatch):
    p = tmp_path / "gate_status.parquet"
    monkeypatch.setattr(hr, "GATE_STATUS_PATH", p)
    return p


class TestHealthReporterSurfacing:

    def test_absent_file_returns_empty(self, gate_file):
        assert hr._read_gate_status(date(2026, 9, 27)) == {}

    def test_reads_rows_for_run_date_only(self, gate_file):
        rows = []
        for d, status in [("2026-09-26", "block"), ("2026-09-27", "pass")]:
            for scope in ("bronze", "silver", "gold"):
                rows.append({"run_date": d, "scope": scope, "layer_health": "pass",
                             "gate_status": status, "streak_days": 3})
        pl.DataFrame(rows).write_parquet(gate_file)
        out = hr._read_gate_status(date(2026, 9, 27))
        assert set(out) == {"bronze", "silver", "gold"}
        assert all(v["gate_status"] == "pass" for v in out.values())
        assert out["gold"]["streak_days"] == 3

    def test_no_row_for_date_returns_empty(self, gate_file):
        pl.DataFrame([{"run_date": "2026-09-20", "scope": "gold", "layer_health": "pass",
                       "gate_status": "pass", "streak_days": 1}]).write_parquet(gate_file)
        assert hr._read_gate_status(date(2026, 9, 27)) == {}

    def test_corrupt_file_degrades_to_empty_not_raise(self, gate_file):
        gate_file.write_bytes(b"not a parquet file")
        assert hr._read_gate_status(date(2026, 9, 27)) == {}

    def test_report_carries_gate_status_key(self, gate_file):
        report = hr.generate_daily_report(date(2026, 9, 27))
        assert report["gate_status"] == {}

    def test_report_populates_gate_status(self, gate_file):
        pl.DataFrame([{"run_date": "2026-09-27", "scope": "gold", "layer_health": "warn",
                       "gate_status": "warn", "streak_days": 1}]).write_parquet(gate_file)
        report = hr.generate_daily_report(date(2026, 9, 27))
        assert report["gate_status"]["gold"]["gate_status"] == "warn"

    def test_print_report_handles_both_cases(self, gate_file):
        hr._print_report(hr.generate_daily_report(date(2026, 9, 27)))   # empty
        pl.DataFrame([{"run_date": "2026-09-27", "scope": "gold", "layer_health": "pass",
                       "gate_status": "pass", "streak_days": 2}]).write_parquet(gate_file)
        hr._print_report(hr.generate_daily_report(date(2026, 9, 27)))   # populated
