"""
scripts/maintenance/clean_pipeline_runs_fixtures.py — GHR-01 (ADR-059) one-off, OFFLINE log cleanup.

Removes the TEST-SUITE pollution from the production audit stores, AFTER archiving them:

  1. ``pipeline_runs.db`` rows written by the test suite (before the suite was isolated from the
     live ``data/`` tree). Two explicit predicates, nothing else:
       a. ``error_msg`` is exactly one of the known fixture strings (boom / first / original /
          secondary / solo);
       b. the row sits in a BURST — one identical ``created_at`` second holding >= ``--burst-min``
          rows — AND is an instant row (``duration_sec`` <= 0.01). Real runs log one row per job at
          different seconds with real durations.
     Genuine failures (any other ``error_msg``) are never touched and are printed so the operator
     can see what stays.
  2. ``data/quarantine`` stubs: one-row Parquet files that carry only a ``series_id`` (and at most
     two more columns). Verified source (ADR-059 GHR-07): they are written by the FRED / IMF / EIA
     ``TestSchemaValidatorGate`` tests, not by production ingesters. They are MOVED to an archive
     directory, never deleted.

Safety contract:
  * DRY-RUN by default: prints the plan and changes nothing. ``--apply`` is required to write.
  * ``--apply`` first copies the whole DB with sqlite's backup API to ``--archive-db`` and REFUSES
    to run if that file already exists (an archive is never overwritten). Stubs are moved with
    ``shutil.move`` and refuse to overwrite an existing destination.
  * Run it with the pipeline stopped (a concurrent write is possible otherwise).
  * Offline utility: not a pipeline job, not imported from ``src/``.

    python scripts/maintenance/clean_pipeline_runs_fixtures.py                 # dry run
    python scripts/maintenance/clean_pipeline_runs_fixtures.py --apply
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

from src.utils.data_paths import data_dir, health_dir, quarantine_dir

FIXTURE_ERROR_MSGS: tuple[str, ...] = ("boom", "first", "original", "secondary", "solo")
DEFAULT_BURST_MIN_ROWS = 20
INSTANT_ROW_MAX_DURATION_SEC = 0.01
STUB_MAX_COLUMNS = 3


@dataclass
class RunsPlan:
    total_rows: int
    fixture_error_ids: list[int] = field(default_factory=list)
    burst_ids: list[int] = field(default_factory=list)
    burst_timestamps: list[str] = field(default_factory=list)
    kept_failures: list[tuple] = field(default_factory=list)   # (id, run_id, run_date, error_msg)

    @property
    def delete_ids(self) -> list[int]:
        return sorted(set(self.fixture_error_ids) | set(self.burst_ids))


def plan_runs(db_path: Path, burst_min: int = DEFAULT_BURST_MIN_ROWS) -> RunsPlan:
    """Read-only: which rows the two predicates select, and which failures stay."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        total = con.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
        fixture_ids: list[int] = []
        for msg in FIXTURE_ERROR_MSGS:              # one parameterised query per string (no SQL building)
            fixture_ids += [r[0] for r in con.execute(
                "SELECT id FROM pipeline_runs WHERE error_msg = ?", (msg,))]
        burst_ts = [r[0] for r in con.execute(
            "SELECT created_at FROM pipeline_runs GROUP BY created_at HAVING COUNT(*) >= ?", (burst_min,))]
        burst_ids: list[int] = []
        for ts in burst_ts:
            burst_ids += [r[0] for r in con.execute(
                "SELECT id FROM pipeline_runs WHERE created_at = ? AND COALESCE(duration_sec, 0) <= ?",
                (ts, INSTANT_ROW_MAX_DURATION_SEC))]
        selected = set(fixture_ids) | set(burst_ids)
        kept = [
            (r[0], r[1], r[2], r[3]) for r in con.execute(
                "SELECT id, run_id, run_date, error_msg FROM pipeline_runs WHERE status = 'failed' ORDER BY id")
            if r[0] not in selected
        ]
    finally:
        con.close()
    return RunsPlan(total, sorted(fixture_ids), sorted(set(burst_ids)), sorted(burst_ts), kept)


def apply_runs(db_path: Path, archive_db: Path, plan: RunsPlan) -> int:
    """Archive the whole DB (sqlite backup API), then delete the planned rows in one transaction."""
    if archive_db.exists():
        raise FileExistsError(f"archive already exists, refusing to overwrite: {archive_db}")
    archive_db.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(db_path)
    try:
        dst = sqlite3.connect(archive_db)
        try:
            src.backup(dst)
        finally:
            dst.close()
        ids = plan.delete_ids
        with src:                                   # one transaction
            src.executemany("DELETE FROM pipeline_runs WHERE id = ?", [(i,) for i in ids])
    finally:
        src.close()
    return len(ids)


def find_quarantine_stubs(qdir: Path) -> list[Path]:
    """One-row Parquet files holding only a series_id (and at most STUB_MAX_COLUMNS columns)."""
    import polars as pl

    stubs: list[Path] = []
    for f in sorted(qdir.glob("*.parquet")) if qdir.exists() else []:
        try:
            df = pl.read_parquet(f)
        except Exception:                           # unreadable: not ours to judge — leave it
            continue
        if df.height == 1 and "series_id" in df.columns and len(df.columns) <= STUB_MAX_COLUMNS:
            stubs.append(f)
    return stubs


def move_stubs(stubs: list[Path], archive_dir: Path) -> int:
    archive_dir.mkdir(parents=True, exist_ok=True)
    for f in stubs:                                  # check ALL destinations first: all or nothing
        if (archive_dir / f.name).exists():
            raise FileExistsError(f"stub archive already holds {f.name}, refusing to overwrite")
    for f in stubs:
        shutil.move(str(f), str(archive_dir / f.name))
    return len(stubs)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", type=Path, default=health_dir() / "pipeline_runs.db")
    p.add_argument("--archive-db", type=Path,
                   default=data_dir() / "archive" / "pipeline_runs_pre_isolation_2026_10.db")
    p.add_argument("--quarantine-dir", type=Path, default=quarantine_dir())
    p.add_argument("--archive-stubs", type=Path,
                   default=data_dir() / "archive" / "quarantine_test_stubs_2026_10")
    p.add_argument("--burst-min", type=int, default=DEFAULT_BURST_MIN_ROWS)
    p.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    args = p.parse_args(argv)

    rc = 0
    if args.db.exists():
        plan = plan_runs(args.db, args.burst_min)
        print(f"pipeline_runs: {plan.total_rows} rows | fixture error_msg rows: {len(plan.fixture_error_ids)} | "
              f"burst instant rows: {len(plan.burst_ids)} in {len(plan.burst_timestamps)} bursts | "
              f"to delete: {len(plan.delete_ids)}")
        print(f"genuine failures kept ({len(plan.kept_failures)}):")
        for row in plan.kept_failures:
            print(f"  id={row[0]} job={row[1]} run_date={row[2]} error={str(row[3])[:80]!r}")
        if args.apply:
            n = apply_runs(args.db, args.archive_db, plan)
            print(f"APPLIED: archived to {args.archive_db}; deleted {n} rows")
    else:
        print(f"no DB at {args.db} — nothing to clean")
        rc = 1

    stubs = find_quarantine_stubs(args.quarantine_dir)
    print(f"quarantine stubs (1 row, series_id only): {len(stubs)}")
    for f in stubs:
        print(f"  {f}")
    if args.apply and stubs:
        print(f"APPLIED: moved {move_stubs(stubs, args.archive_stubs)} stub(s) to {args.archive_stubs}")

    if not args.apply:
        print("DRY RUN — nothing changed. Re-run with --apply (pipeline stopped) to archive and clean.")
    return rc


if __name__ == "__main__":
    sys.exit(main())
