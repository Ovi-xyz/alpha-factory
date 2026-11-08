"""
scripts/maintenance/archive_legacy_active_symbols.py — GMI-AS-002 one-off, OFFLINE archive.

``ActiveSymbolsResolver`` no longer writes the legacy ``active_{date}.parquet`` (it was a byte-for-byte
twin of ``active_ohlcv_{date}.parquet``). The files already on disk are now inert; this script MOVES
them out of ``data/silver/active_symbols/`` so the directory holds one dated artifact per day.

A legacy file is archived ONLY when its canonical twin exists and holds the same data (same schema,
same rows, same order). Everything else is left exactly where it is and reported:

  * ``legacy-only``  — no ``active_ohlcv_{date}`` twin: this may be the only record of that day;
  * ``differs``      — the twin exists but the content is not identical: a human decides;
  * ``unreadable``   — the file cannot be read as Parquet.

Only names that match ``active_YYYY-MM-DD.parquet`` exactly are ever considered, so
``active_ohlcv_*``, ``membership_log.parquet`` and any other file are never touched.

Safety contract:
  * DRY-RUN by default: prints the plan and changes nothing. ``--apply`` is required to move files.
  * Files are MOVED (``shutil.move``), never deleted, and a move refuses to overwrite an existing
    destination.
  * Exit code 1 when anything was left behind for a human (a clean run exits 0).
  * Offline utility: not a pipeline job, not imported from ``src/``.

    python scripts/maintenance/archive_legacy_active_symbols.py                  # dry run
    python scripts/maintenance/archive_legacy_active_symbols.py --apply
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

from src.utils.data_paths import data_dir

LEGACY_RE = re.compile(r"^active_(\d{4}-\d{2}-\d{2})\.parquet$")
DEFAULT_ARCHIVE_NAME = "active_symbols_legacy_2026_11"


@dataclass
class ArchivePlan:
    archive: list[Path] = field(default_factory=list)                    # identical to the canonical twin
    legacy_only: list[Path] = field(default_factory=list)
    differs: list[Path] = field(default_factory=list)
    unreadable: list[Path] = field(default_factory=list)

    @property
    def left_behind(self) -> list[Path]:
        return sorted(self.legacy_only + self.differs + self.unreadable)


def twin_of(legacy: Path) -> Path:
    m = LEGACY_RE.match(legacy.name)
    assert m, f"not a legacy active file: {legacy.name}"
    return legacy.with_name(f"active_ohlcv_{m.group(1)}.parquet")


def plan_archive(active_dir: Path) -> ArchivePlan:
    """Read-only: classify every legacy file against its canonical twin."""
    import polars as pl

    plan = ArchivePlan()
    for f in sorted(active_dir.glob("active_*.parquet")) if active_dir.exists() else []:
        if not LEGACY_RE.match(f.name):
            continue                                                    # active_ohlcv_*, anything else: not ours
        twin = twin_of(f)
        if not twin.exists():
            plan.legacy_only.append(f)
            continue
        try:
            a, b = pl.read_parquet(f), pl.read_parquet(twin)
        except Exception:                                               # noqa: BLE001 — unreadable: leave it
            plan.unreadable.append(f)
            continue
        (plan.archive if (a.schema == b.schema and a.equals(b)) else plan.differs).append(f)
    return plan


def move_files(files: list[Path], archive_dir: Path) -> int:
    """Move (never delete); refuse to overwrite. Checks every destination BEFORE moving anything."""
    clashes = [archive_dir / f.name for f in files if (archive_dir / f.name).exists()]
    if clashes:
        raise FileExistsError(f"archive destination exists, refusing to overwrite: {clashes[0]}")
    archive_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        shutil.move(str(f), str(archive_dir / f.name))
    return len(files)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", type=Path, default=data_dir() / "silver" / "active_symbols",
                    help="active_symbols directory (default: <data>/silver/active_symbols)")
    ap.add_argument("--archive-dir", type=Path, default=data_dir() / "archive" / DEFAULT_ARCHIVE_NAME,
                    help="where identical legacy files are moved")
    ap.add_argument("--apply", action="store_true", help="actually move the files (default: dry run)")
    args = ap.parse_args(argv)

    plan = plan_archive(args.dir)
    print(f"directory: {args.dir}")
    print(f"identical to the canonical twin (will be archived): {len(plan.archive)}")
    for f in plan.archive:
        print(f"  {f.name}")
    for label, files in (("legacy-only (no active_ohlcv twin)", plan.legacy_only),
                         ("differs from the canonical twin", plan.differs),
                         ("unreadable", plan.unreadable)):
        print(f"LEFT IN PLACE — {label}: {len(files)}")
        for f in files:
            print(f"  {f.name}")

    if args.apply and plan.archive:
        n = move_files(plan.archive, args.archive_dir)
        print(f"APPLIED: moved {n} file(s) to {args.archive_dir}")
    if not args.apply:
        print("DRY RUN — nothing changed. Re-run with --apply to archive.")
    return 1 if plan.left_behind else 0


if __name__ == "__main__":
    sys.exit(main())
