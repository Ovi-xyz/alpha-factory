"""GMI-AS-002 — scripts/maintenance/archive_legacy_active_symbols.py."""

from __future__ import annotations

import ast
from pathlib import Path

import polars as pl
import pytest

from scripts.maintenance import archive_legacy_active_symbols as arc

REPO_ROOT = Path(__file__).resolve().parents[2]


def _frame(syms=("AAPL", "BBCA", "EUR_USD")) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": list(syms),
        "dollar_volume_20d": [float(i + 1) for i in range(len(syms))],
        "clean_days": [20] * len(syms),
    })


@pytest.fixture
def dirs(tmp_path):
    active = tmp_path / "silver" / "active_symbols"
    active.mkdir(parents=True)
    return active, tmp_path / "archive" / "legacy"


def _pair(active: Path, day: str, legacy: pl.DataFrame | None = None, canonical: pl.DataFrame | None = None) -> None:
    (legacy if legacy is not None else _frame()).write_parquet(active / f"active_{day}.parquet")
    (canonical if canonical is not None else _frame()).write_parquet(active / f"active_ohlcv_{day}.parquet")


def _run(active: Path, archive: Path, *flags: str) -> int:
    return arc.main(["--dir", str(active), "--archive-dir", str(archive), *flags])


def _names(d: Path) -> list[str]:
    return sorted(p.name for p in d.iterdir()) if d.exists() else []


class TestPlan:

    def test_identical_twin_is_planned_for_archive(self, dirs):
        active, _ = dirs
        _pair(active, "2026-10-16")
        plan = arc.plan_archive(active)
        assert [p.name for p in plan.archive] == ["active_2026-10-16.parquet"]
        assert plan.left_behind == []

    def test_legacy_without_a_twin_is_never_archived(self, dirs):
        active, _ = dirs
        _frame().write_parquet(active / "active_2026-10-15.parquet")
        plan = arc.plan_archive(active)
        assert plan.archive == [] and [p.name for p in plan.legacy_only] == ["active_2026-10-15.parquet"]

    def test_different_content_is_never_archived(self, dirs):
        active, _ = dirs
        _pair(active, "2026-10-16", canonical=_frame(("AAPL", "BBCA")))
        plan = arc.plan_archive(active)
        assert plan.archive == [] and len(plan.differs) == 1

    def test_same_values_in_a_different_row_order_differ(self, dirs):
        active, _ = dirs
        _pair(active, "2026-10-16", canonical=_frame(("EUR_USD", "BBCA", "AAPL")))
        assert len(arc.plan_archive(active).differs) == 1

    def test_same_values_with_a_different_dtype_differ(self, dirs):
        active, _ = dirs
        _pair(active, "2026-10-16", canonical=_frame().with_columns(pl.col("clean_days").cast(pl.Int32)))
        assert len(arc.plan_archive(active).differs) == 1

    def test_unreadable_legacy_is_left_alone(self, dirs):
        active, _ = dirs
        (active / "active_2026-10-16.parquet").write_text("not a parquet file")
        _frame().write_parquet(active / "active_ohlcv_2026-10-16.parquet")
        plan = arc.plan_archive(active)
        assert plan.archive == [] and len(plan.unreadable) == 1

    def test_only_exact_legacy_names_are_considered(self, dirs):
        active, _ = dirs
        for name in ("active_ohlcv_2026-10-16.parquet", "membership_log.parquet",
                     "active_context_2026-10-16.parquet", "active_2026-10-16.parquet.tmp",
                     "active_2026-10-16.parquet.bak", "active_latest.parquet"):
            _frame().write_parquet(active / name)
        plan = arc.plan_archive(active)
        assert plan.archive == plan.legacy_only == plan.differs == plan.unreadable == []

    def test_missing_directory_is_an_empty_plan(self, tmp_path):
        plan = arc.plan_archive(tmp_path / "nope")
        assert plan.archive == [] and plan.left_behind == []


class TestMain:

    def test_dry_run_changes_nothing(self, dirs):
        active, archive = dirs
        _pair(active, "2026-10-16")
        before = _names(active)
        assert _run(active, archive) == 0
        assert _names(active) == before and not archive.exists()

    def test_apply_moves_only_the_legacy_twin_and_keeps_the_canonical_file(self, dirs):
        active, archive = dirs
        for day in ("2026-10-16", "2026-10-17", "2026-10-18"):
            _pair(active, day)
        assert _run(active, archive, "--apply") == 0
        assert _names(active) == [f"active_ohlcv_{d}.parquet" for d in ("2026-10-16", "2026-10-17", "2026-10-18")]
        assert _names(archive) == [f"active_{d}.parquet" for d in ("2026-10-16", "2026-10-17", "2026-10-18")]
        assert pl.read_parquet(archive / "active_2026-10-16.parquet").equals(_frame())    # moved, not rewritten

    def test_apply_leaves_the_unverifiable_behind_and_exits_1(self, dirs):
        active, archive = dirs
        _pair(active, "2026-10-16")                                           # verified -> archived
        _frame().write_parquet(active / "active_2026-10-10.parquet")          # legacy-only -> stays
        _pair(active, "2026-10-17", canonical=_frame(("AAPL",)))              # differs -> stays
        assert _run(active, archive, "--apply") == 1
        assert _names(archive) == ["active_2026-10-16.parquet"]
        assert "active_2026-10-10.parquet" in _names(active) and "active_2026-10-17.parquet" in _names(active)

    def test_dry_run_exit_code_reports_left_behind_files(self, dirs):
        active, archive = dirs
        _frame().write_parquet(active / "active_2026-10-10.parquet")
        assert _run(active, archive) == 1

    def test_apply_is_idempotent(self, dirs):
        active, archive = dirs
        _pair(active, "2026-10-16")
        assert _run(active, archive, "--apply") == 0
        assert _run(active, archive, "--apply") == 0                          # nothing left to move
        assert _names(archive) == ["active_2026-10-16.parquet"]


class TestMoveSafety:

    def test_refuses_to_overwrite_and_moves_nothing(self, dirs):
        active, archive = dirs
        _pair(active, "2026-10-16")
        _pair(active, "2026-10-17")
        archive.mkdir(parents=True)
        _frame(("OLD",)).write_parquet(archive / "active_2026-10-17.parquet")  # only the SECOND clashes
        plan = arc.plan_archive(active)
        with pytest.raises(FileExistsError):
            arc.move_files(plan.archive, archive)
        assert "active_2026-10-16.parquet" in _names(active)                  # all-or-nothing: the first did not move
        assert pl.read_parquet(archive / "active_2026-10-17.parquet")["symbol"].to_list() == ["OLD"]


class TestIsolation:

    def test_nothing_in_src_imports_the_script(self):
        """Offline utility: not a pipeline job and never imported from src/. (A docstring pointer for the
        operator, as in active_symbols.py, is documentation, not coupling — so the scan is on IMPORTS.)"""
        offenders = []
        for p in sorted((REPO_ROOT / "src").rglob("*.py")):
            for node in ast.walk(ast.parse(p.read_text(encoding="utf-8"))):
                names = ([a.name for a in node.names] if isinstance(node, ast.Import) else
                         [node.module or ""] + [a.name for a in node.names] if isinstance(node, ast.ImportFrom) else [])
                if any(n == "scripts" or n.startswith("scripts.") or "archive_legacy_active_symbols" in n for n in names):
                    offenders.append(str(p.relative_to(REPO_ROOT)))
        assert offenders == []

    def test_the_import_scan_catches_what_it_exists_to_catch(self):
        for bad in ("from scripts.maintenance import archive_legacy_active_symbols",
                    "import scripts.maintenance.archive_legacy_active_symbols",
                    "from scripts.maintenance.archive_legacy_active_symbols import main"):
            node = ast.parse(bad).body[0]
            names = ([a.name for a in node.names] if isinstance(node, ast.Import) else
                     [node.module or ""] + [a.name for a in node.names])
            assert any(n == "scripts" or n.startswith("scripts.") or "archive_legacy_active_symbols" in n for n in names), bad
