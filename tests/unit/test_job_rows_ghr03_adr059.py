"""
tests/unit/test_job_rows_ghr03_adr059.py — GHR-03 (ADR-059): real row counts, and "unknown" is never 0.

HR3: `rows_written` was 0 on 3,428 of 3,428 pipeline_runs rows (run_job never passed it), and
`source` / `symbol` / `timeframe` were empty too. The Telegram success line always said "0 rows" and
an empty run looked identical to a full one.
"""

from __future__ import annotations

import sqlite3
import types
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.utils.job_result import JobResult
from src.utils.pipeline_logger import PipelineLogger

LEGACY_SCHEMA = """CREATE TABLE pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, run_date TEXT NOT NULL,
    layer TEXT, source TEXT, symbol TEXT, timeframe TEXT, status TEXT,
    rows_written INTEGER DEFAULT 0, duration_sec REAL, error_msg TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP)"""


def _rows(db: Path, job: str):
    with sqlite3.connect(db) as con:
        return con.execute(
            "SELECT status, rows_written, rows_known FROM pipeline_runs WHERE run_id = ? ORDER BY id", (job,)
        ).fetchall()


class TestJobResult:

    @pytest.mark.parametrize("bad", [-1, True, 1.5, "7"])
    def test_rejects_invalid_row_counts(self, bad):
        with pytest.raises(ValueError):
            JobResult(rows_written=bad)

    def test_none_and_zero_are_both_valid_and_distinct(self):
        assert JobResult().rows_written is None
        assert JobResult(rows_written=0).rows_written == 0

    def test_only_a_jobresult_is_honoured(self):
        assert JobResult.rows_from(JobResult(rows_written=5)) == 5
        assert JobResult.rows_from(None) is None
        # a bare int could be any counter (symbols processed, an exit code) — never a row count
        assert JobResult.rows_from(7) is None
        assert JobResult.rows_from({"rows_written": 7}) is None
        assert JobResult.rows_from(pl.DataFrame({"a": [1, 2]})) is None

    def test_warnings(self):
        assert JobResult.warnings_from(JobResult(warnings=("w",))) == ("w",)
        assert JobResult.warnings_from(None) == ()


class TestLoggerRowsKnown:

    def test_success_without_a_count_is_unknown_not_zero(self, tmp_path):
        db = tmp_path / "r.db"
        PipelineLogger(db).success("j_unknown", layer="util")
        assert _rows(db, "j_unknown") == [("success", None, 0)]

    def test_a_reported_zero_is_known(self, tmp_path):
        db = tmp_path / "r.db"
        PipelineLogger(db).success("j_zero", rows_written=0)
        assert _rows(db, "j_zero") == [("success", 0, 1)]

    def test_a_reported_count_is_stored(self, tmp_path):
        db = tmp_path / "r.db"
        PipelineLogger(db).success("j_n", rows_written=12345)
        assert _rows(db, "j_n") == [("success", 12345, 1)]

    def test_failure_is_unknown(self, tmp_path):
        db = tmp_path / "r.db"
        PipelineLogger(db).failure("j_fail", "boom")
        assert _rows(db, "j_fail") == [("failed", None, 0)]

    def test_legacy_db_is_migrated_and_legacy_zeros_are_flagged_unknown(self, tmp_path):
        db = tmp_path / "legacy.db"
        with sqlite3.connect(db) as con:
            con.execute(LEGACY_SCHEMA)
            con.executemany("INSERT INTO pipeline_runs (run_id, run_date, status, rows_written) VALUES (?,?,?,?)",
                            [("old_a", "2026-10-15", "success", 0), ("old_b", "2026-10-16", "success", 0)])
        PipelineLogger(db)
        assert _rows(db, "old_a") == [("success", 0, 0)]       # history NOT rewritten, flagged unknown
        assert _rows(db, "old_b") == [("success", 0, 0)]

    def test_migration_does_not_reset_new_rows_on_the_next_start(self, tmp_path):
        """The UPDATE that flags legacy rows must run ONLY when the column is first added."""
        db = tmp_path / "legacy.db"
        with sqlite3.connect(db) as con:
            con.execute(LEGACY_SCHEMA)
        PipelineLogger(db).success("fresh", rows_written=50)
        PipelineLogger(db)                                       # a second process start
        assert _rows(db, "fresh") == [("success", 50, 1)]


class TestRunJobRecordsRows:

    @pytest.fixture
    def runner_env(self, monkeypatch, tmp_path):
        import src.runner as runner
        from src.scheduler.dependency_guard import DependencyGuard
        from src.scheduler.job_registry import JOB_REGISTRY

        monkeypatch.setattr(runner, "guard", DependencyGuard(sentinel_dir=tmp_path / ".s"))
        db = tmp_path / "runs.db"
        monkeypatch.setattr(runner, "pipeline_logger", PipelineLogger(db))

        def register(name, fn):
            monkeypatch.setitem(JOB_REGISTRY, name, {"description": "probe", "fn": fn,
                                                     "depends_on": [], "layer": "util"})
        return runner, db, register

    def test_jobresult_rows_are_recorded(self, runner_env):
        runner, db, register = runner_env
        register("ghr03_a", lambda d: JobResult(rows_written=42))
        runner.run_job("ghr03_a", run_date=date(2026, 9, 29))
        assert _rows(db, "ghr03_a") == [("success", 42, 1)]

    def test_none_return_is_unknown(self, runner_env):
        runner, db, register = runner_env
        register("ghr03_b", lambda d: None)
        runner.run_job("ghr03_b", run_date=date(2026, 9, 29))
        assert _rows(db, "ghr03_b") == [("success", None, 0)]

    def test_a_bare_int_return_is_not_mistaken_for_rows(self, runner_env):
        runner, db, register = runner_env
        register("ghr03_c", lambda d: 7)
        runner.run_job("ghr03_c", run_date=date(2026, 9, 29))
        assert _rows(db, "ghr03_c") == [("success", None, 0)]

    def test_reported_zero_is_recorded_as_a_known_zero(self, runner_env):
        runner, db, register = runner_env
        register("ghr03_d", lambda d: JobResult(rows_written=0))
        runner.run_job("ghr03_d", run_date=date(2026, 9, 29))
        assert _rows(db, "ghr03_d") == [("success", 0, 1)]

    def test_warnings_are_surfaced(self, runner_env, monkeypatch):
        runner, db, register = runner_env
        seen: list[str] = []
        monkeypatch.setattr(runner.pipeline_logger, "warn", seen.append)
        register("ghr03_e", lambda d: JobResult(rows_written=1, warnings=("thin day",)))
        runner.run_job("ghr03_e", run_date=date(2026, 9, 29))
        assert any("thin day" in m for m in seen)


class TestProducers:

    def test_base_ingester_counts_written_rows_not_idempotent_skips(self, tmp_path):
        from src.bronze.base_ingester import BronzeIngester

        ing = BronzeIngester()
        ing.BASE_PATH = tmp_path
        df = pl.DataFrame({"close": [1.0, 2.0, 3.0]})
        assert ing.write(df, "t", "market/x", "SYM") is not None
        assert ing.rows_written_total == 3
        assert ing.write(df, "t", "market/x", "SYM") is None          # same day: idempotent skip
        assert ing.rows_written_total == 3
        ing.write(df, "t", "market/x", "SYM2")
        assert ing.rows_written_total == 6

    def test_instances_do_not_share_the_counter(self, tmp_path):
        from src.bronze.base_ingester import BronzeIngester

        a, b = BronzeIngester(), BronzeIngester()
        a.BASE_PATH = tmp_path
        a.write(pl.DataFrame({"c": [1.0]}), "t", "market/x", "S")
        assert a.rows_written_total == 1 and b.rows_written_total == 0
        assert BronzeIngester.rows_written_total == 0

    def test_market_ingester_run_returns_this_calls_rows(self, tmp_path, monkeypatch):
        import src.bronze.market_ingester as mi

        insts = [types.SimpleNamespace(symbol=f"GHR3{c}", market="us_stocks", timezone="UTC") for c in "AB"]
        monkeypatch.setattr(mi, "get_loader", lambda: types.SimpleNamespace(all_symbols=lambda: insts))
        monkeypatch.setattr(mi, "YFINANCE_THROTTLE_SECONDS", 0)
        ing = mi.MarketOHLCVIngester(timeframes=["1D", "1W"])
        ing.BASE_PATH = tmp_path

        def fake_run_symbol(inst, tf, run_date):
            ing.write(pl.DataFrame({"close": [1.0, 2.0]}), "t", f"market/ohlcv/x/timeframe={tf}", inst.symbol)

        monkeypatch.setattr(ing, "_run_symbol", fake_run_symbol)
        first = ing.run(date(2031, 3, 3))
        assert isinstance(first, JobResult) and first.rows_written == 2 * 2 * 2
        second = ing.run(date(2031, 3, 3))                             # checkpoint: nothing pending
        assert second.rows_written == 0                                # a delta, not the running total

    def test_silver_ohlcv_run_returns_rows_written(self, monkeypatch):
        import src.config.instrument_loader as il
        import src.silver.ohlcv_processor as op

        insts = [types.SimpleNamespace(symbol=f"GHR3S{c}", market="us_stocks", timezone="UTC") for c in "AB"]
        loader = types.SimpleNamespace(symbol_list=lambda: [i.symbol for i in insts], all_symbols=lambda: insts)
        monkeypatch.setattr(il, "get_loader", lambda: loader)

        class FakeProc:
            def process_symbol(self, **kw):
                return pl.DataFrame({"close": [1.0, 2.0, 3.0]})

            def write(self, df, symbol, market, tf):
                pass

            def synthesize_4h(self, **kw):
                return None

        monkeypatch.setattr(op, "OHLCVProcessor", FakeProc)
        monkeypatch.setattr(op, "_read_bronze_symbol", lambda pattern, tz_hint=None: pl.DataFrame({"x": [1]}))
        out = op.run(date(2031, 3, 4))
        assert isinstance(out, JobResult)
        assert out.rows_written == 3 * len(op._RUN_BRONZE_TFS) * len(insts)

    def test_registry_wrappers_pass_the_result_through(self, monkeypatch):
        import src.scheduler.job_registry as jr
        import src.silver.ohlcv_processor as op

        sentinel = JobResult(rows_written=99)
        monkeypatch.setattr(op, "run", lambda d: sentinel)
        assert jr._silver_ohlcv(date(2026, 9, 29)) is sentinel


class TestHealthReportRows:

    @staticmethod
    def _db(tmp_path, monkeypatch, rows):
        import src.utils.health_reporter as hr

        db = tmp_path / "runs.db"
        PipelineLogger(db)                                                # creates the new schema
        with sqlite3.connect(db) as con:
            con.executemany(
                "INSERT INTO pipeline_runs (run_id, run_date, layer, status, rows_written, rows_known, duration_sec)"
                " VALUES (?,?,?,?,?,?,?)", rows)
        monkeypatch.setattr(hr, "DB_PATH", db)
        monkeypatch.setattr(hr, "_check_idx_coverage", lambda d: {})
        monkeypatch.setattr(hr, "_read_gate_status", lambda d: {})
        # The report also checks the HOST's free disk; pin it so these tests do not depend on it.
        DiskUsage = types.SimpleNamespace
        monkeypatch.setattr(hr.shutil, "disk_usage", lambda p: DiskUsage(total=10**13, used=0, free=5 * 10**12))
        return hr

    MON = date(2026, 9, 28)                                              # a weekday
    SAT = date(2026, 9, 26)

    def test_only_reported_counts_are_summed(self, tmp_path, monkeypatch):
        hr = self._db(tmp_path, monkeypatch, [
            ("bronze_ohlcv_daily", "2026-09-28", "bronze", "success", 1000, 1, 5.0),
            ("silver_validate",    "2026-09-28", "silver", "success", 0,    0, 5.0),   # legacy/unknown 0
            ("gold_mtf",           "2026-09-28", "gold",   "success", None, 0, 5.0),
            # an UNKNOWN row that nevertheless carries a non-zero number must not be summed
            ("gold_signals_old",   "2026-09-28", "gold",   "success", 999,  0, 5.0),
        ])
        rep = hr.generate_daily_report(self.MON)
        assert rep["total_rows"] == 1000
        assert rep["rows_known_runs"] == 1

    def test_no_reported_count_reads_as_na_not_zero(self, tmp_path, monkeypatch):
        hr = self._db(tmp_path, monkeypatch, [("gold_mtf", "2026-09-28", "gold", "success", None, 0, 5.0)])
        rep = hr.generate_daily_report(self.MON)
        assert rep["rows_known_runs"] == 0
        msg = hr.compose_alert_message(rep)
        assert "rows n/a" in msg and "0 rows" not in msg

    def test_a_known_total_is_shown(self, tmp_path, monkeypatch):
        hr = self._db(tmp_path, monkeypatch, [("silver_ohlcv", "2026-09-28", "silver", "success", 2500, 1, 5.0)])
        assert "2,500 rows" in hr.compose_alert_message(hr.generate_daily_report(self.MON))

    def test_zero_row_anomaly_is_flagged_on_a_weekday(self, tmp_path, monkeypatch):
        hr = self._db(tmp_path, monkeypatch, [("silver_ohlcv", "2026-09-28", "silver", "success", 0, 1, 5.0)])
        rep = hr.generate_daily_report(self.MON)
        assert rep["zero_row_jobs"] == ["silver_ohlcv"]
        assert "ZERO_ROWS" in hr.compose_alert_message(rep)

    def test_not_flagged_when_the_count_is_unknown(self, tmp_path, monkeypatch):
        hr = self._db(tmp_path, monkeypatch, [("silver_ohlcv", "2026-09-28", "silver", "success", 0, 0, 5.0)])
        assert hr.generate_daily_report(self.MON)["zero_row_jobs"] == []

    def test_not_flagged_when_another_run_that_day_wrote_rows(self, tmp_path, monkeypatch):
        """An idempotent re-run reports 0; the job's day total is what counts."""
        hr = self._db(tmp_path, monkeypatch, [
            ("bronze_ohlcv_daily", "2026-09-28", "bronze", "success", 5000, 1, 5.0),
            ("bronze_ohlcv_daily", "2026-09-28", "bronze", "success", 0,    1, 5.0),
        ])
        assert hr.generate_daily_report(self.MON)["zero_row_jobs"] == []

    def test_not_flagged_on_a_weekend(self, tmp_path, monkeypatch):
        hr = self._db(tmp_path, monkeypatch, [("silver_ohlcv", "2026-09-26", "silver", "success", 0, 1, 5.0)])
        assert hr.generate_daily_report(self.SAT)["zero_row_jobs"] == []

    def test_a_job_without_the_flag_is_never_flagged(self, tmp_path, monkeypatch):
        hr = self._db(tmp_path, monkeypatch, [("gold_mtf", "2026-09-28", "gold", "success", 0, 1, 5.0)])
        assert hr.generate_daily_report(self.MON)["zero_row_jobs"] == []

    def test_legacy_schema_db_still_reads(self, tmp_path, monkeypatch):
        import src.utils.health_reporter as hr

        db = tmp_path / "old.db"
        with sqlite3.connect(db) as con:
            con.execute(LEGACY_SCHEMA)
            con.execute("INSERT INTO pipeline_runs (run_id, run_date, layer, status, rows_written)"
                        " VALUES ('x','2026-09-28','gold','success',777)")
        monkeypatch.setattr(hr, "DB_PATH", db)
        monkeypatch.setattr(hr, "_check_idx_coverage", lambda d: {})
        monkeypatch.setattr(hr, "_read_gate_status", lambda d: {})
        rep = hr.generate_daily_report(self.MON)
        assert rep["total_rows"] == 777 and "rows_known_runs" not in rep and rep["zero_row_jobs"] == []

    def test_a_pause_is_reported_not_stale(self, tmp_path, monkeypatch):
        from src.utils import data_paths, pause_markers

        monkeypatch.setenv(data_paths.ENV_VAR, str(tmp_path))
        pause_markers.add_pause(["silver", "gold"], date(2026, 9, 20), "chain alignment",
                                expected_resume=date(2026, 10, 5))
        hr = self._db(tmp_path, monkeypatch, [])
        rep = hr.generate_daily_report(self.MON)
        assert set(rep["paused"]) == {"silver", "gold"}
        assert rep["paused"]["silver"]["since"] == "2026-09-20"
        assert rep["paused"]["silver"]["reason"] == "chain alignment"
