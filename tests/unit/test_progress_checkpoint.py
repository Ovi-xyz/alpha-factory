"""tests/unit/test_progress_checkpoint.py — G6 ProgressCheckpoint test suite"""

from datetime import date

import pytest

from src.utils.progress_checkpoint import ProgressCheckpoint


@pytest.fixture
def checkpoint(tmp_db_path, monkeypatch):
    """ProgressCheckpoint dengan temp DB path."""
    monkeypatch.setattr(ProgressCheckpoint, "DB_PATH", tmp_db_path)
    return ProgressCheckpoint("test_job", date(2025, 1, 15))


class TestProgressCheckpoint:

    def test_is_done_initially_false(self, checkpoint):
        assert not checkpoint.is_done("AAPL")

    def test_mark_done_sets_done(self, checkpoint):
        checkpoint.mark_done("AAPL")
        assert checkpoint.is_done("AAPL")

    def test_mark_failed_does_not_set_done(self, checkpoint):
        checkpoint.mark_failed("MSFT", ValueError("network error"))
        assert not checkpoint.is_done("MSFT")

    def test_failed_report_has_error_msg(self, checkpoint):
        checkpoint.mark_failed("TSLA", RuntimeError("timeout"))
        report = checkpoint.failed_report()
        assert len(report) == 1
        assert "timeout" in report[0]["error_msg"]
        assert report[0]["symbol"] == "TSLA"

    def test_timeframe_isolation(self, checkpoint):
        """IDD §10.2: Done (AAPL, 1D) must NOT affect (AAPL, 1H)."""
        checkpoint.mark_done("AAPL", timeframe="1D")
        assert checkpoint.is_done("AAPL", timeframe="1D")
        assert not checkpoint.is_done("AAPL", timeframe="1H")

    def test_pending_symbols_excludes_done(self, checkpoint):
        checkpoint.mark_done("AAPL")
        pending = checkpoint.pending_symbols(["AAPL", "MSFT", "GOOGL"])
        assert "AAPL" not in pending
        assert "MSFT" in pending

    def test_clear_run_date_isolation(self, tmp_db_path, monkeypatch):
        """IDD §10.2: Clear T-1 must NOT delete checkpoint for T."""
        monkeypatch.setattr(ProgressCheckpoint, "DB_PATH", tmp_db_path)

        date_t1 = date(2025, 1, 14)
        date_t  = date(2025, 1, 15)

        cp_t1 = ProgressCheckpoint("test_job", date_t1)
        cp_t  = ProgressCheckpoint("test_job", date_t)

        cp_t1.mark_done("AAPL")
        cp_t.mark_done("MSFT")

        # Clear T-1
        cp_t1.clear(date_t1)

        # T-1 should be gone
        assert not cp_t1.is_done("AAPL")
        # T should remain intact
        assert cp_t.is_done("MSFT")

    def test_summary_counts(self, checkpoint):
        checkpoint.mark_done("A")
        checkpoint.mark_done("B")
        checkpoint.mark_failed("C", Exception("err"))
        s = checkpoint.summary()
        assert s.get("done", 0) == 2
        assert s.get("failed", 0) == 1

    def test_coverage_pct(self, checkpoint):
        checkpoint.mark_done("A")
        checkpoint.mark_done("B")
        assert checkpoint.coverage_pct(4) == 50.0
        assert checkpoint.coverage_pct(2) == 100.0

    def test_clear_all_run_dates_when_no_run_date_given(self, checkpoint):
        """Coverage tranche (17 Aug 2026) — clear(run_date=None) branch:
        deletes every checkpoint for this job across ALL run_dates, not
        just one (distinct from test_clear_run_date_isolation, which only
        exercises the run_date-specific DELETE)."""
        checkpoint.mark_done("AAPL")
        other_date_checkpoint = type(checkpoint)(checkpoint.job_name, date(2025, 2, 1))
        other_date_checkpoint.mark_done("MSFT")

        checkpoint.clear(None)

        assert not checkpoint.is_done("AAPL")
        assert not other_date_checkpoint.is_done("MSFT")

    def test_coverage_pct_zero_total_expected_returns_100(self, checkpoint):
        """Coverage tranche (17 Aug 2026) — total_expected=0 guard, avoiding
        a ZeroDivisionError and treating 'nothing expected' as fully covered."""
        assert checkpoint.coverage_pct(0) == 100.0
