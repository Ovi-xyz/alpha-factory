"""
tests/unit/test_makefile_safety_nets.py

Extracted from tests/unit/test_archived_migration_scripts.py (retired
2026-08-07) when scripts/archive/ was deleted entirely from the repo.
That file's regression guard against migrate_instruments.py and
build_instruments_v14.py being dangerously importable no longer applies
-- those files don't exist anywhere now, archived or otherwise, so the
bug they guarded against (destructive import-time writes) is
structurally impossible, not just disabled. Removing the whole file
would also have silently dropped this one still-valid check: the
Makefile's `migrate` target is deliberately kept (not deleted) so
anyone running it from muscle memory gets a clear explanation instead
of a raw "No rule to make target" or, worse, silent data loss -- see
the Makefile's own `migrate:` target comment. That's still true
regardless of what happened to the archive, so it moved here rather
than being lost along with the rest of the file.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        args, cwd=REPO_ROOT, capture_output=True, text=True, timeout=30,
    )


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
