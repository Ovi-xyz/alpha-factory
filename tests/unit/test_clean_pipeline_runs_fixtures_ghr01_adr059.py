"""
tests/unit/test_clean_pipeline_runs_fixtures_ghr01_adr059.py — GHR-01 offline cleanup utility.
Archive FIRST, delete only the two explicit predicates, never touch genuine failures, dry-run default.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import polars as pl
import pytest

from scripts.maintenance import clean_pipeline_runs_fixtures as clean


def _make_db(path: Path) -> None:
    con = sqlite3.connect(path)
    con.execute("""CREATE TABLE pipeline_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, run_date TEXT NOT NULL,
        layer TEXT, source TEXT, symbol TEXT, timeframe TEXT, status TEXT,
        rows_written INTEGER DEFAULT 0, duration_sec REAL, error_msg TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    ins = ("INSERT INTO pipeline_runs (run_id, run_date, layer, status, duration_sec, error_msg, created_at)"
           " VALUES (?,?,?,?,?,?,?)")
    rows = []
    # a burst: 21 instant rows in one second (fixture rows, mixed status)
    for i in range(21):
        rows.append(("silver_ohlcv", "2026-10-15", "silver", "success" if i % 3 else "failed", 0.0,
                     None if i % 3 else "something not in the fixture list", "2026-10-15 19:30:00"))
    # a burst second that ALSO holds one real-duration row: must survive
    rows.append(("gold_signals", "2026-10-15", "gold", "success", 12.8, None, "2026-10-15 19:30:00"))
    # fixture error strings outside any burst
    for msg in ("boom", "first", "original", "secondary", "solo"):
        rows.append(("x", "2026-10-16", "util", "failed", 0.0, msg, f"2026-10-16 10:00:0{len(msg) % 9}"))
    # genuine failures
    rows.append(("silver_validate", "2026-10-17", "silver", "failed", 41.0, "CRITICAL: nulls", "2026-10-17 21:50:00"))
    rows.append(("gold_screener", "2026-10-18", "gold", "failed", 9.0, "freshness 0/593", "2026-10-18 04:50:00"))
    # normal successes
    for j in range(3):
        rows.append(("bronze_ohlcv_daily", "2026-10-18", "bronze", "success", 120.5 + j, None, f"2026-10-18 04:1{j}:00"))
    con.executemany(ins, rows)
    con.commit()
    con.close()


class TestPlan:

    def test_predicates_select_exactly_the_fixture_rows(self, tmp_path):
        db = tmp_path / "runs.db"
        _make_db(db)
        plan = clean.plan_runs(db)
        assert plan.total_rows == 21 + 1 + 5 + 2 + 3
        assert len(plan.fixture_error_ids) == 5
        assert len(plan.burst_ids) == 21                 # the 21 instant rows, NOT the 12.8 s row
        assert len(plan.burst_timestamps) == 1
        assert len(plan.delete_ids) == 26

    def test_genuine_failures_are_reported_and_kept(self, tmp_path):
        db = tmp_path / "runs.db"
        _make_db(db)
        plan = clean.plan_runs(db)
        kept = {r[1] for r in plan.kept_failures}
        assert {"silver_validate", "gold_screener"} <= kept
        # the burst's 'failed' rows are deleted, so they are not listed as kept
        assert all(r[3] != "something not in the fixture list" for r in plan.kept_failures)

    def test_dry_run_changes_nothing(self, tmp_path, capsys):
        db = tmp_path / "runs.db"
        _make_db(db)
        before = db.read_bytes()
        rc = clean.main(["--db", str(db), "--archive-db", str(tmp_path / "a.db"),
                         "--quarantine-dir", str(tmp_path / "q"), "--archive-stubs", str(tmp_path / "s")])
        assert rc == 0
        assert db.read_bytes() == before
        assert not (tmp_path / "a.db").exists()
        assert "DRY RUN" in capsys.readouterr().out


class TestApply:

    def test_archives_first_then_deletes_only_the_plan(self, tmp_path):
        db, arch = tmp_path / "runs.db", tmp_path / "arch" / "runs_pre.db"
        _make_db(db)
        plan = clean.plan_runs(db)
        n = clean.apply_runs(db, arch, plan)
        assert n == 26
        with sqlite3.connect(arch) as a, sqlite3.connect(db) as d:
            assert a.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 32     # untouched copy
            assert d.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 6
            left = {r[0] for r in d.execute("SELECT run_id FROM pipeline_runs")}
        assert left == {"gold_signals", "silver_validate", "gold_screener", "bronze_ohlcv_daily"}

    def test_refuses_to_overwrite_an_existing_archive(self, tmp_path):
        db, arch = tmp_path / "runs.db", tmp_path / "runs_pre.db"
        _make_db(db)
        arch.write_bytes(b"precious")
        with pytest.raises(FileExistsError):
            clean.apply_runs(db, arch, clean.plan_runs(db))
        assert arch.read_bytes() == b"precious"
        with sqlite3.connect(db) as d:                           # and the DB was not touched
            assert d.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 32


class TestQuarantineStubs:

    def _write(self, d: Path, name: str, df: pl.DataFrame) -> Path:
        d.mkdir(parents=True, exist_ok=True)
        p = d / name
        df.write_parquet(p)
        return p

    def test_only_one_row_series_id_stubs_are_selected_and_moved(self, tmp_path):
        q = tmp_path / "q"
        stub = self._write(q, "stub.parquet", pl.DataFrame({"series_id": ["X"], "reason": ["missing column"]}))
        self._write(q, "real.parquet", pl.DataFrame({"series_id": ["X"] * 5, "value": [1.0] * 5}))
        self._write(q, "wide.parquet", pl.DataFrame({"series_id": ["X"], "a": [1], "b": [2], "c": [3]}))
        self._write(q, "other.parquet", pl.DataFrame({"symbol": ["A"]}))
        assert clean.find_quarantine_stubs(q) == [stub]
        arch = tmp_path / "arch"
        assert clean.move_stubs([stub], arch) == 1
        assert (arch / "stub.parquet").exists() and not stub.exists()
        assert len(list(q.glob("*.parquet"))) == 3

    def test_move_refuses_to_overwrite(self, tmp_path):
        q, arch = tmp_path / "q", tmp_path / "arch"
        stub = self._write(q, "stub.parquet", pl.DataFrame({"series_id": ["X"]}))
        arch.mkdir()
        (arch / "stub.parquet").write_bytes(b"keep")
        with pytest.raises(FileExistsError):
            clean.move_stubs([stub], arch)
        assert stub.exists() and (arch / "stub.parquet").read_bytes() == b"keep"
