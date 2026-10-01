"""
tests/unit/test_quality_gate_log_persistence_adr050.py

FIX GMI-GATE-001 (ADR-050): quality_validator.py::run() now persists its
full per-check result set to data/health/quality_gate_log.parquet
(previously in-memory only, discarded when the job function returned).

Isolates QUALITY_GATE_LOG_PATH to a tmp_path per test (never touches the
real data/health/ directory) via monkeypatch, matching the isolation
pattern already established for DependencyGuard/PipelineLogger tests in
this codebase (sandboxed path, not the production location).
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from src.silver import quality_validator as qv
from src.silver.quality_validator import (
    CRITICAL_CHECKS,
    WARNING_CHECKS,
    QualityValidator,
    _persist_quality_gate_log,
)


@pytest.fixture(autouse=True)
def _isolate_log_path(tmp_path, monkeypatch):
    """Every test in this file writes to an isolated tmp_path, never the
    real data/health/quality_gate_log.parquet."""
    monkeypatch.setattr(qv, "QUALITY_GATE_LOG_PATH", tmp_path / "quality_gate_log.parquet")
    return tmp_path


class TestPersistSchema:

    def test_writes_one_row_per_check(self):
        results = {"null_check": True, "gap_detection": False, "vix_circuit": True}
        _persist_quality_gate_log(date(2025, 5, 1), results, issues=[])
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        assert df.height == 3

    def test_schema_has_expected_columns(self):
        results = {"null_check": True}
        _persist_quality_gate_log(date(2025, 5, 1), results, issues=[])
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        expected_cols = {"run_date", "check_name", "tier", "passed", "detail", "logged_at"}
        assert expected_cols.issubset(set(df.columns))

    def test_tier_classification_matches_check_sets(self):
        results = {
            "null_check": True,          # CRITICAL_CHECKS
            "gap_detection": True,       # WARNING_CHECKS
            "vix_circuit": True,         # neither -> 'other'
        }
        _persist_quality_gate_log(date(2025, 5, 1), results, issues=[])
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        tiers = dict(zip(df["check_name"].to_list(), df["tier"].to_list()))
        assert tiers["null_check"] == "critical"
        assert tiers["gap_detection"] == "warning"
        assert tiers["vix_circuit"] == "other"

    def test_every_critical_and_warning_check_name_classified_correctly(self):
        """No hardcoded second copy of the tier lists — derive expected
        tiers from the real CRITICAL_CHECKS/WARNING_CHECKS sets and check
        every name round-trips through _persist_quality_gate_log."""
        results = {name: True for name in CRITICAL_CHECKS | WARNING_CHECKS}
        _persist_quality_gate_log(date(2025, 5, 1), results, issues=[])
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        tiers = dict(zip(df["check_name"].to_list(), df["tier"].to_list()))
        for name in CRITICAL_CHECKS:
            assert tiers[name] == "critical"
        for name in WARNING_CHECKS:
            assert tiers[name] == "warning"

    def test_passed_column_is_boolean(self):
        results = {"null_check": True, "gap_detection": False}
        _persist_quality_gate_log(date(2025, 5, 1), results, issues=[])
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        assert df["passed"].dtype == pl.Boolean

    def test_detail_populated_from_issues_for_failed_check(self):
        results = {"null_check": False}
        issues  = [{"check": "null_check", "detail": "null_rate=5.00% > 0.10%"}]
        _persist_quality_gate_log(date(2025, 5, 1), results, issues)
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        assert df.filter(pl.col("check_name") == "null_check")["detail"][0] == \
            "null_rate=5.00% > 0.10%"

    def test_detail_none_for_passing_check_with_no_issue(self):
        results = {"null_check": True}
        _persist_quality_gate_log(date(2025, 5, 1), results, issues=[])
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        assert df.filter(pl.col("check_name") == "null_check")["detail"][0] is None

    def test_first_matching_issue_wins_not_concatenated(self):
        """If a check somehow logs two issues in one run (none currently
        do), the first is kept — never silently concatenated or the last
        one silently overwriting the first."""
        results = {"null_check": False}
        issues = [
            {"check": "null_check", "detail": "first"},
            {"check": "null_check", "detail": "second"},
        ]
        _persist_quality_gate_log(date(2025, 5, 1), results, issues)
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        assert df.filter(pl.col("check_name") == "null_check")["detail"][0] == "first"

    def test_no_rows_written_when_results_empty(self):
        _persist_quality_gate_log(date(2025, 5, 1), {}, issues=[])
        assert not qv.QUALITY_GATE_LOG_PATH.exists()


class TestIdempotentRerunAndHistory:

    def test_rerun_same_date_replaces_not_duplicates(self):
        _persist_quality_gate_log(date(2025, 5, 1), {"null_check": True}, issues=[])
        _persist_quality_gate_log(date(2025, 5, 1), {"null_check": False}, issues=[])
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        assert df.height == 1
        assert df["passed"][0] is False

    def test_history_accumulates_across_distinct_dates(self):
        _persist_quality_gate_log(date(2025, 5, 1), {"null_check": True}, issues=[])
        _persist_quality_gate_log(date(2025, 5, 2), {"null_check": True}, issues=[])
        _persist_quality_gate_log(date(2025, 5, 3), {"null_check": False}, issues=[])
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        assert df.height == 3
        assert sorted(df["run_date"].to_list()) == ["2025-05-01", "2025-05-02", "2025-05-03"]

    def test_other_dates_untouched_by_a_rerun(self):
        _persist_quality_gate_log(date(2025, 5, 1), {"null_check": True}, issues=[])
        _persist_quality_gate_log(date(2025, 5, 2), {"null_check": True}, issues=[])
        _persist_quality_gate_log(date(2025, 5, 1), {"null_check": False}, issues=[])  # rerun day 1
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        day2 = df.filter(pl.col("run_date") == "2025-05-02")
        assert day2.height == 1
        assert day2["passed"][0] is True


class TestPersistenceNeverBlocksTheGate:

    def test_persistence_failure_does_not_raise(self, monkeypatch):
        """A write failure in the logging side-effect must never propagate
        — silver_validate's own CRITICAL-check gate is the only thing
        allowed to raise QualityGateError."""
        def _boom(*a, **kw):
            raise OSError("disk full")
        monkeypatch.setattr(qv, "atomic_write_parquet", _boom)
        # Should not raise, even though the underlying write blows up.
        _persist_quality_gate_log(date(2025, 5, 1), {"null_check": True}, issues=[])


class TestIntegrationWithModuleRun:
    """End-to-end through the real job entry point qv.run() — confirms
    the wiring (not just the helper function in isolation)."""

    def test_run_persists_log_for_the_run_date(self):
        qv.run(date(2025, 5, 1))
        assert qv.QUALITY_GATE_LOG_PATH.exists()
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        assert set(df["run_date"].unique().to_list()) == {"2025-05-01"}

    def test_run_persists_even_when_critical_check_fails(self, monkeypatch):
        """The log must capture a blocked run too — Gatekeeper's history
        needs the failure, not just the healthy days."""
        monkeypatch.setattr(
            QualityValidator, "_check_null", lambda self, run_date: False
        )
        with pytest.raises(qv.QualityGateError):
            qv.run(date(2025, 5, 1))
        df = pl.read_parquet(qv.QUALITY_GATE_LOG_PATH)
        row = df.filter(pl.col("check_name") == "null_check")
        assert row["passed"][0] is False
        assert row["tier"][0] == "critical"
