"""
tests/unit/test_health_reporter_ghr02_adr059.py — GHR-02 (ADR-059).

HR1: the IDX coverage check compared CAST(_ingested_at AS DATE) (naive UTC) with run_date (the WIB
     calendar day). A chain starting 21:16 UTC ingests on the PREVIOUS UTC date, so the check read
     0/30 although all 30 files existed. The window is now the WIB calendar day expressed in UTC.
HR2: send_telegram_alert was an if/elif chain, so a permanent IDX alert hid the failed-jobs and the
     success messages. One message now carries every active alert.
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

import src.utils.health_reporter as hr

RUN_DATE = date(2026, 10, 18)   # a Sunday; WIB day = [17 Oct 17:00 UTC, 18 Oct 17:00 UTC)


def _idx_symbols() -> list[str]:
    from src.config.instrument_loader import get_loader
    return [i.symbol for i in get_loader().by_market("idx")]


def _write_idx(base, rows) -> None:
    out = base / "data" / "bronze" / "market" / "ohlcv" / "idx" / "run" / "symbol=X"
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(out / "f.parquet")


class TestWindow:

    def test_wib_day_in_utc(self):
        assert hr.local_day_window_utc(RUN_DATE) == (datetime(2026, 10, 17, 17, 0), datetime(2026, 10, 18, 17, 0))

    def test_window_is_exactly_24_hours_and_naive(self):
        a, b = hr.local_day_window_utc(date(2026, 3, 1))
        assert (b - a).total_seconds() == 86400
        assert a.tzinfo is None and b.tzinfo is None


class TestIdxCoverageWindow:

    def test_previous_utc_day_evening_ingest_counts_for_the_wib_run_date(self, tmp_path, monkeypatch):
        """The observed 18 Oct case: all 30 files ingested 21:22 UTC on 17 Oct, run_date 18 Oct (WIB)."""
        monkeypatch.chdir(tmp_path)
        syms = _idx_symbols()
        _write_idx(tmp_path, [{"_symbol": s, "_ingested_at": "2026-10-17T21:22:11", "close": 1.0} for s in syms])
        r = hr._check_idx_coverage(RUN_DATE)
        assert r["idx_present_count"] == len(syms)
        assert r["idx_missing_count"] == 0
        assert r["idx_coverage_alert"] is False

    @pytest.mark.parametrize("ingested, inside", [
        ("2026-10-17T16:59:59", False),   # one second before WIB midnight -> belongs to the 17th
        ("2026-10-17T17:00:00", True),    # WIB midnight
        ("2026-10-17T17:00:01", True),
        ("2026-10-18T16:59:59", True),    # last second of the WIB day
        ("2026-10-18T17:00:00", False),   # WIB midnight of the 19th -> belongs to the next run_date
    ])
    def test_boundaries(self, tmp_path, monkeypatch, ingested, inside):
        monkeypatch.chdir(tmp_path)
        sym = _idx_symbols()[0]
        _write_idx(tmp_path, [{"_symbol": sym, "_ingested_at": ingested, "close": 1.0}])
        assert (hr._check_idx_coverage(RUN_DATE)["idx_present_count"] == 1) is inside

    def test_fractional_seconds_and_unparseable_values(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        syms = _idx_symbols()
        _write_idx(tmp_path, [
            {"_symbol": syms[0], "_ingested_at": "2026-10-17T21:22:11.123456", "close": 1.0},
            {"_symbol": syms[1], "_ingested_at": "not-a-timestamp", "close": 1.0},   # TRY_CAST -> NULL -> excluded
        ])
        r = hr._check_idx_coverage(RUN_DATE)
        assert r["idx_present_count"] == 1


class TestAlertComposition:

    def _report(self, **kw):
        base = {
            "date": "2026-10-18", "total_runs": 10, "total_failed": 0, "total_rows": 5000,
            "storage_alert": False, "storage_free_gb": 200.0,
            "idx_coverage_alert": False, "idx_present_count": 30, "idx_missing_count": 0,
            "idx_total": 30, "idx_coverage_pct": 100.0,
        }
        base.update(kw)
        return base

    def test_idx_and_failed_jobs_are_both_reported(self):
        msg = hr.compose_alert_message(self._report(
            idx_coverage_alert=True, idx_present_count=0, idx_missing_count=30, idx_coverage_pct=0.0,
            total_failed=4))
        assert "IDX_PARTIAL_FAILURE" in msg and "4 jobs FAILED" in msg
        assert "✅" not in msg

    def test_all_three_alerts_in_fixed_order(self):
        msg = hr.compose_alert_message(self._report(
            storage_alert=True, storage_free_gb=40.0, idx_coverage_alert=True,
            idx_missing_count=9, idx_coverage_pct=70.0, total_failed=3))
        assert msg.index("Storage ALERT") < msg.index("IDX_PARTIAL_FAILURE") < msg.index("jobs FAILED")
        assert "3 alerts" in msg

    def test_success_line_only_when_no_alert(self):
        assert hr.collect_alerts(self._report()) == []
        assert hr.compose_alert_message(self._report()).startswith("✅ Pipeline 2026-10-18")

    def test_single_alert_keeps_the_legacy_one_line_shape(self):
        msg = hr.compose_alert_message(self._report(storage_alert=True, storage_free_gb=40.0))
        assert msg == "⚠️ Pipeline 2026-10-18 | Storage ALERT: 40.0GB free"
        failed = hr.compose_alert_message(self._report(total_failed=3))
        assert failed == "🔴 Pipeline 2026-10-18 | 3 jobs FAILED"

    def test_failed_threshold_boundary(self):
        assert hr.collect_alerts(self._report(total_failed=hr.FAILED_ALERT_COUNT - 1)) == []
        assert len(hr.collect_alerts(self._report(total_failed=hr.FAILED_ALERT_COUNT))) == 1
