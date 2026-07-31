"""tests/unit/test_health_reporter.py — Health reporter unit tests"""

import sqlite3
from datetime import date
from pathlib import Path

import pytest

from src.utils.health_reporter import generate_daily_report, FAILED_ALERT_COUNT


class TestHealthReporter:

    @pytest.fixture
    def tmp_db(self, tmp_path) -> Path:
        """Create a temp pipeline_runs.db with sample data."""
        db_path = tmp_path / "pipeline_runs.db"
        con = sqlite3.connect(db_path)
        con.execute("""
            CREATE TABLE pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT, run_date TEXT, layer TEXT, source TEXT,
                symbol TEXT, timeframe TEXT, status TEXT,
                rows_written INTEGER DEFAULT 0,
                duration_sec REAL, error_msg TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        today = date(2025, 5, 22).isoformat()
        rows  = [
            ("bronze_ohlcv_daily", today, "bronze", "yfinance", None, None, "success", 1200, 42.5, None),
            ("silver_ohlcv",       today, "silver", None,       None, None, "success", 800,  61.0, None),
            ("gold_signals",       today, "gold",   None,       None, None, "failed",  0,    0.0,  "timeout"),
            ("health_report",      today, "util",   None,       None, None, "success", 0,    1.2,  None),
        ]
        con.executemany(
            "INSERT INTO pipeline_runs (run_id, run_date, layer, source, symbol, "
            "timeframe, status, rows_written, duration_sec, error_msg) VALUES (?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        con.commit()
        con.close()
        return db_path

    def test_report_structure(self, tmp_db):
        """Report must have required keys."""
        report = generate_daily_report.__wrapped__(date(2025, 5, 22)) \
            if hasattr(generate_daily_report, "__wrapped__") \
            else _generate_with_custom_db(tmp_db, date(2025, 5, 22))
        assert isinstance(report, dict)

    def test_report_with_db(self, tmp_db, monkeypatch):
        """generate_daily_report reads correctly from SQLite."""
        import src.utils.health_reporter as hr
        monkeypatch.setattr(hr, "DB_PATH", tmp_db)
        report = hr.generate_daily_report(date(2025, 5, 22))
        assert "date"             in report
        assert "pipeline_summary" in report
        assert "total_runs"       in report
        assert "total_failed"     in report
        assert "storage_free_gb"  in report

    def test_failed_count_is_correct(self, tmp_db, monkeypatch):
        """total_failed must equal number of failed rows."""
        import src.utils.health_reporter as hr
        monkeypatch.setattr(hr, "DB_PATH", tmp_db)
        report = hr.generate_daily_report(date(2025, 5, 22))
        assert report["total_failed"] == 1   # Only gold_signals failed

    def test_storage_alert_field_present(self, tmp_db, monkeypatch):
        """storage_alert field must be a bool."""
        import src.utils.health_reporter as hr
        monkeypatch.setattr(hr, "DB_PATH", tmp_db)
        report = hr.generate_daily_report(date(2025, 5, 22))
        assert isinstance(report["storage_alert"], bool)

    def test_empty_db_returns_zeros(self, tmp_path, monkeypatch):
        """Empty DB produces zeroed totals (not an exception)."""
        import src.utils.health_reporter as hr
        empty_db = tmp_path / "empty.db"
        con = sqlite3.connect(empty_db)
        con.execute("""CREATE TABLE pipeline_runs (
            id INTEGER PRIMARY KEY, run_id TEXT, run_date TEXT,
            layer TEXT, source TEXT, symbol TEXT, timeframe TEXT,
            status TEXT, rows_written INTEGER, duration_sec REAL,
            error_msg TEXT, created_at TEXT)""")
        con.commit(); con.close()
        monkeypatch.setattr(hr, "DB_PATH", empty_db)
        report = hr.generate_daily_report(date(2025, 5, 22))
        assert report["total_runs"]   == 0
        assert report["total_failed"] == 0

    def test_failed_alert_threshold_constant(self):
        """FAILED_ALERT_COUNT must be a positive integer."""
        assert isinstance(FAILED_ALERT_COUNT, int)
        assert FAILED_ALERT_COUNT > 0

    def test_run_executes_without_error(self, tmp_path, monkeypatch):
        """run() with no Telegram env vars set must complete without raising
        (generate_daily_report + _print_report path, Telegram path skipped)."""
        import src.utils.health_reporter as hr

        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        hr.run(date(2025, 6, 1))   # must not raise

    def test_print_report_no_crash(self, capsys):
        """_print_report() must handle a fully-populated report dict without error."""
        import src.utils.health_reporter as hr

        report = {
            "date": "2025-06-01",
            "pipeline_summary": [
                {"job": "bronze_ohlcv_daily", "layer": "bronze", "total_runs": 1,
                 "success": 1, "failed": 0, "avg_duration_sec": 12.3, "total_rows": 1000},
            ],
            "total_runs": 1, "total_failed": 0, "total_rows": 1000,
            "storage_free_gb": 120.5, "storage_alert": False, "storage_warn": True,
            # FIX ADR-029: idx_tvdatafeed_count/idx_fallback_count removed --
            # reworked to presence-vs-missing (tvdatafeed retired).
            "idx_total": 30, "idx_present_count": 28,
            "idx_missing_count": 2, "idx_coverage_pct": 93.3, "idx_coverage_alert": False,
        }
        hr._print_report(report)   # must not raise


class TestSendTelegramAlert:
    """send_telegram_alert() message priority: storage > IDX coverage > failed-count > success."""

    def _base_report(self, **overrides):
        # FIX ADR-029: idx_fallback_count removed -- presence-vs-missing schema.
        report = {
            "date": "2025-06-01", "total_runs": 10, "total_failed": 0, "total_rows": 5000,
            "storage_alert": False, "storage_free_gb": 200.0,
            "idx_coverage_alert": False, "idx_present_count": 30,
            "idx_missing_count": 0, "idx_total": 30, "idx_coverage_pct": 100.0,
        }
        report.update(overrides)
        return report

    def test_success_message_when_all_healthy(self, monkeypatch):
        import src.utils.health_reporter as hr
        captured = {}
        monkeypatch.setattr(
            "httpx.post",
            lambda url, json, timeout: captured.update(json) or _FakeResponse(),
        )
        hr.send_telegram_alert(self._base_report(), "tok", "chat")
        assert "✅" in captured["text"]

    def test_idx_alert_takes_priority_over_success(self, monkeypatch):
        import src.utils.health_reporter as hr
        captured = {}
        monkeypatch.setattr(
            "httpx.post",
            lambda url, json, timeout: captured.update(json) or _FakeResponse(),
        )
        report = self._base_report(
            idx_coverage_alert=True, idx_present_count=23, idx_missing_count=7,
            idx_coverage_pct=76.7,
        )
        hr.send_telegram_alert(report, "tok", "chat")
        assert "IDX_PARTIAL_FAILURE" in captured["text"]

    def test_storage_alert_takes_priority_over_idx(self, monkeypatch):
        """Storage alert must win even if IDX is also degraded — storage is checked first."""
        import src.utils.health_reporter as hr
        captured = {}
        monkeypatch.setattr(
            "httpx.post",
            lambda url, json, timeout: captured.update(json) or _FakeResponse(),
        )
        report = self._base_report(
            storage_alert=True, storage_free_gb=40.0,
            idx_coverage_alert=True, idx_present_count=15, idx_missing_count=15,
        )
        hr.send_telegram_alert(report, "tok", "chat")
        assert "Storage ALERT" in captured["text"]
        assert "IDX_PARTIAL_FAILURE" not in captured["text"]


class _FakeResponse:
    def raise_for_status(self):
        pass


class TestIDXCoverageAlert:
    """
    FIX ADR-029 (GMI_Decision_Document_v7.docx, 30 Jul 2026): reworked from
    tvdatafeed-vs-fallback to presence-vs-missing after tvdatafeed's
    retirement (KNOWN_RISKS.md RISK-1 -> RESOLVED; originally FIX GAP-10
    [P3], Production Readiness Assessment v1.7.2, GD §9.1). yfinance .JK is
    now IDX30's SOLE source, so a source-of-origin distinction is no longer
    meaningful -- under the OLD schema every present symbol would show as
    "fallback", permanently over-tripping the alert on every healthy run.
    These tests build a synthetic Bronze IDX fixture (real Parquet on disk,
    via tmp_path) to exercise the full DuckDB read -> presence resolution ->
    alert threshold path.
    """

    @staticmethod
    def _write_idx_fixture(base_dir, rows, subdir="mixed"):
        import polars as pl
        out_dir = Path(base_dir) / "data" / "bronze" / "market" / "ohlcv" / "idx" / subdir / "symbol=X"
        out_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(rows).write_parquet(out_dir / "fixture.parquet")

    def test_full_coverage_no_alert(self, tmp_path, monkeypatch):
        """All 30 IDX symbols present -> 100% coverage, no alert."""
        import src.utils.health_reporter as hr
        monkeypatch.chdir(tmp_path)

        from src.config.instrument_loader import get_loader
        idx_symbols = [i.symbol for i in get_loader().by_market("idx")]
        run_date = date(2025, 6, 15)
        ingested_at = "2025-06-15T03:00:00"

        rows = [
            {"_symbol": s, "_ingested_at": ingested_at, "close": 100.0}
            for s in idx_symbols
        ]
        self._write_idx_fixture(tmp_path, rows)

        result = hr._check_idx_coverage(run_date)
        assert result["idx_total"] == len(idx_symbols)
        assert result["idx_present_count"] == len(idx_symbols)
        assert result["idx_missing_count"] == 0
        assert result["idx_coverage_pct"] == 100.0
        assert result["idx_coverage_alert"] is False

    def test_degraded_coverage_triggers_alert(self, tmp_path, monkeypatch):
        """> 5 symbols missing -> alert fires."""
        import src.utils.health_reporter as hr
        monkeypatch.chdir(tmp_path)

        from src.config.instrument_loader import get_loader
        idx_symbols = [i.symbol for i in get_loader().by_market("idx")]
        run_date = date(2025, 6, 15)
        ingested_at = "2025-06-15T03:00:00"

        rows = []
        for i, sym in enumerate(idx_symbols):
            if i < 22:
                rows.append({"_symbol": sym, "_ingested_at": ingested_at, "close": 100.0})
            else:
                continue   # missing entirely (8 symbols)
        self._write_idx_fixture(tmp_path, rows)

        result = hr._check_idx_coverage(run_date)
        assert result["idx_present_count"] == 22
        assert result["idx_missing_count"] == 8
        assert result["idx_coverage_alert"] is True   # 8 > threshold (5)

    def test_below_threshold_no_alert(self, tmp_path, monkeypatch):
        """<= 5 symbols missing -> alert must NOT fire (boundary case)."""
        import src.utils.health_reporter as hr
        monkeypatch.chdir(tmp_path)

        from src.config.instrument_loader import get_loader
        idx_symbols = [i.symbol for i in get_loader().by_market("idx")]
        run_date = date(2025, 6, 15)
        ingested_at = "2025-06-15T03:00:00"

        rows = []
        for i, sym in enumerate(idx_symbols):
            if i < 5:
                continue   # exactly 5 missing
            rows.append({"_symbol": sym, "_ingested_at": ingested_at, "close": 100.0})
        self._write_idx_fixture(tmp_path, rows)

        result = hr._check_idx_coverage(run_date)
        assert result["idx_missing_count"] == 5
        assert result["idx_coverage_alert"] is False   # exactly at threshold, not over

    def test_no_bronze_data_graceful(self, tmp_path, monkeypatch):
        """No Bronze IDX data at all must not raise — zeroed fields returned."""
        import src.utils.health_reporter as hr
        monkeypatch.chdir(tmp_path)

        result = hr._check_idx_coverage(date(2025, 6, 15))
        assert result["idx_coverage_alert"] is False
        assert result["idx_coverage_pct"] is None or result["idx_coverage_pct"] == 0.0

    def test_idx_fields_present_in_full_report(self, tmp_path, monkeypatch):
        """generate_daily_report() must always include the new IDX keys."""
        import sqlite3
        import src.utils.health_reporter as hr

        db_path = tmp_path / "pipeline_runs.db"
        con = sqlite3.connect(db_path)
        con.execute("""
            CREATE TABLE pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT, run_date TEXT, layer TEXT, source TEXT,
                symbol TEXT, timeframe TEXT, status TEXT,
                rows_written INTEGER DEFAULT 0,
                duration_sec REAL, error_msg TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()
        con.close()
        monkeypatch.setattr(hr, "DB_PATH", db_path)

        report = hr.generate_daily_report(date(2025, 5, 22))
        for key in (
            "idx_total", "idx_present_count",
            "idx_missing_count", "idx_coverage_pct", "idx_coverage_alert",
        ):
            assert key in report, f"Missing IDX coverage key: {key}"


def _generate_with_custom_db(db_path: Path, run_date: date) -> dict:
    """Helper: run generate_daily_report with a custom db path."""
    import src.utils.health_reporter as hr
    orig = hr.DB_PATH
    hr.DB_PATH = db_path
    try:
        return hr.generate_daily_report(run_date)
    finally:
        hr.DB_PATH = orig
