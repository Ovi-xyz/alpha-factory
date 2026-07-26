"""
tests/unit/test_archived_migration_scripts.py

NEW (v1.11.2) — GMI_Decision_Document_v3.docx Priority 3 / Checkpoint v6 §8
item 3: scripts/migrate_instruments.py and scripts/build_instruments_v14.py
were archived after confirming empirically that BOTH execute a destructive
write to config/instruments.yaml at MODULE IMPORT TIME (no `__main__` gate
ever existed in either file) and BOTH target a schema several versions
behind the current v1.5 instruments.yaml (see scripts/archive/README.md for
the full root-cause writeup).

These tests are a permanent regression guard: they confirm the hard
`raise SystemExit(...)` guard fires — on both direct execution AND a plain
`import` — and that config/instruments.yaml is never touched, in an
isolated subprocess (not the test-runner's own process, since the whole
point of the original bug was import-time side effects that a same-process
`import` in a test would not faithfully reproduce).
"""
from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
ARCHIVE_DIR = REPO_ROOT / "scripts" / "archive"
# UPD Decision B Step 2 (GMI_Decision_Document_v5.docx, 2026-07-22):
# config/instruments.yaml no longer exists as a single file — split into
# 3. The regression this test guards against (destructive import-time
# write) applies equally to all 3 new files, and there's more surface
# area to guard now than before, so all 3 are checked rather than picking
# just one as a stand-in.
GUARDED_CONFIG_FILES = [
    REPO_ROOT / "config" / "instruments_identity.yaml",
    REPO_ROOT / "config" / "instruments_taxonomy.yaml",
    REPO_ROOT / "config" / "regime_sector_weights.yaml",
]

ARCHIVED_SCRIPTS = [
    "migrate_instruments.py",
    "build_instruments_v14.py",
]


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        args, cwd=REPO_ROOT, capture_output=True, text=True, timeout=30,
    )


class TestArchivedScriptsAreDisabled:

    @pytest.mark.parametrize("script_name", ARCHIVED_SCRIPTS)
    def test_direct_execution_exits_nonzero(self, script_name):
        """Running the archived script directly must fail loudly, not migrate."""
        result = _run([sys.executable, str(ARCHIVE_DIR / script_name)])
        assert result.returncode != 0, (
            f"{script_name} exited 0 — archival guard is not firing"
        )
        assert "ARCHIVED" in (result.stdout + result.stderr)

    @pytest.mark.parametrize("script_name", ARCHIVED_SCRIPTS)
    def test_import_alone_exits_nonzero(self, script_name):
        """Even a bare `import` (no direct execution) must trigger the guard —
        this was the actual bug: neither script ever had a `__main__` gate,
        so the destructive write ran at import time."""
        module_name = script_name.removesuffix(".py")
        result = _run(
            [sys.executable, "-c", f"import scripts.archive.{module_name}"]
        )
        assert result.returncode != 0
        assert "ARCHIVED" in (result.stdout + result.stderr)

    @pytest.mark.parametrize("script_name", ARCHIVED_SCRIPTS)
    def test_instruments_yaml_untouched_by_either_invocation(self, script_name):
        """The regression this guards against: none of the 3 instrument
        config files' content or mtime may change from either attempted
        invocation."""
        before = [(f, f.read_text(), f.stat().st_mtime_ns) for f in GUARDED_CONFIG_FILES]

        _run([sys.executable, str(ARCHIVE_DIR / script_name)])
        module_name = script_name.removesuffix(".py")
        _run([sys.executable, "-c", f"import scripts.archive.{module_name}"])

        for f, before_text, before_mtime in before:
            assert f.read_text() == before_text, (
                f"{script_name} modified {f.name} content — guard failed"
            )
            assert f.stat().st_mtime_ns == before_mtime, (
                f"{script_name} touched {f.name} on disk — guard failed"
            )


class TestArchiveHousekeeping:

    def test_archive_readme_exists(self):
        assert (ARCHIVE_DIR / "README.md").exists()

    @pytest.mark.parametrize("script_name", ARCHIVED_SCRIPTS)
    def test_archived_script_still_syntactically_valid(self, script_name):
        """Guard must not have broken the file — kept as valid Python for
        historical reference, per scripts/archive/README.md."""
        ast.parse((ARCHIVE_DIR / script_name).read_text())

    def test_no_longer_present_at_original_scripts_path(self):
        """Confirms the git-mv actually happened, not a copy-and-forget that
        leaves the dangerous, unguarded originals reachable at the old path."""
        assert not (REPO_ROOT / "scripts" / "migrate_instruments.py").exists()
        assert not (REPO_ROOT / "scripts" / "build_instruments_v14.py").exists()


class TestMakefileMigrateTargetFailsLoudly:
    """
    `make migrate` is kept (not deleted from the Makefile) so anyone running
    it from muscle memory gets a clear explanation instead of either
    "No rule to make target" or, worse, silent data loss.
    """

    def test_make_migrate_exits_nonzero_and_explains(self):
        result = _run(["make", "migrate"])
        assert result.returncode != 0
        assert "archive" in (result.stdout + result.stderr).lower()
