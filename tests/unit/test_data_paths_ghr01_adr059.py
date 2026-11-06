"""
tests/unit/test_data_paths_ghr01_adr059.py — GHR-01 (ADR-059): the test suite must not write to the
LIVE operational tree (data/health, data/quarantine, data/.sentinels, data/.pause).

Measured on a pristine clone: the UNMODIFIED suite created data/health/pipeline_runs.db,
data/health/progress.db and 18 one-row quarantine stubs in the live tree. Verified cause: runner tests
sandbox the sentinel guard but not the PipelineLogger, so every run_job() call wrote a row into the
production audit DB; the FRED / IMF / EIA TestSchemaValidatorGate tests quarantine into the real path.

Pinned at three levels: the resolver contract (default byte-identical, env override), the wiring (every
operational path constant derives from it) and the sandbox (the session runs from a symlink-mirrored cwd
so every cwd-relative `data/...` default resolves into scratch).
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

from src.utils import data_paths
from tests.conftest import snapshot_tree

REPO_ROOT = Path(__file__).resolve().parents[2]

_PROBE = """
import json
from src.utils import data_paths
import src.utils.pipeline_logger as pl
import src.utils.health_reporter as hr
import src.utils.gatekeeper as gk
import src.utils.pipeline_dashboard as pd_
import src.utils.progress_checkpoint as pc
import src.silver.quality_validator as qv
import src.bronze.schema_validator as sv
import src.scheduler.pipeline_scheduler as ps
import src.backtest.pit_data as pit
import src.runner as runner
from src.scheduler.dependency_guard import DependencyGuard
print(json.dumps({
    "data_dir": str(data_paths.data_dir()),
    "logger_db": str(pl.PipelineLogger().db_path),
    "reporter_db": str(hr.DB_PATH), "reporter_gate": str(hr.GATE_STATUS_PATH),
    "gate_status": str(gk.GATE_STATUS_PATH), "gate_quality": str(gk.QUALITY_GATE_LOG_PATH),
    "gate_quarantine": gk.QUARANTINE_GLOB,
    "dash_db": str(pd_.DB_PATH), "dash_progress": str(pd_.PROGRESS_DB_PATH),
    "progress_db": str(pc.ProgressCheckpoint.DB_PATH),
    "quality_log": str(qv.QUALITY_GATE_LOG_PATH),
    "quarantine": str(sv.SchemaValidator.QUARANTINE_PATH),
    "scheduler_sentinels": str(ps.SENTINEL_DIR),
    "pit_gate": pit.GATE_STATUS_PATH,
    "runner_sentinels": str(runner.guard.sentinel_dir),
    "runner_db": str(runner.pipeline_logger.db_path),
    "guard_default": str(DependencyGuard().sentinel_dir),
}))
"""


def _probe(env_value: str | None, cwd: Path) -> dict:
    import json

    env = {k: v for k, v in os.environ.items() if k != data_paths.ENV_VAR}
    if env_value is not None:
        env[data_paths.ENV_VAR] = env_value
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    out = subprocess.run([sys.executable, "-B", "-c", _PROBE], cwd=cwd, env=env,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-2000:]
    return json.loads(out.stdout.strip().splitlines()[-1])


class TestResolverContract:

    def test_unset_resolves_to_the_previous_hardcoded_relative_paths(self, tmp_path):
        """Production behaviour is byte-identical: with the variable unset every constant is
        exactly the old literal relative path."""
        p = _probe(None, tmp_path)           # cwd = scratch dir, so nothing lands in the repo
        assert p["data_dir"] == "data"
        assert p["logger_db"] == "data/health/pipeline_runs.db"
        assert p["reporter_db"] == "data/health/pipeline_runs.db"
        assert p["reporter_gate"] == "data/health/gate_status.parquet"
        assert p["gate_status"] == "data/health/gate_status.parquet"
        assert p["gate_quality"] == "data/health/quality_gate_log.parquet"
        assert p["gate_quarantine"] == "data/quarantine/*.parquet"
        assert p["dash_db"] == "data/health/pipeline_runs.db"
        assert p["dash_progress"] == "data/health/progress.db"
        assert p["progress_db"] == "data/health/progress.db"
        assert p["quality_log"] == "data/health/quality_gate_log.parquet"
        assert p["quarantine"] == "data/quarantine"
        assert p["scheduler_sentinels"] == "data/.sentinels"
        assert p["pit_gate"] == "data/health/gate_status.parquet"
        assert p["runner_sentinels"] == "data/.sentinels"
        assert p["runner_db"] == "data/health/pipeline_runs.db"
        assert p["guard_default"] == "data/.sentinels"

    def test_env_redirects_every_operational_path(self, tmp_path):
        root = str(tmp_path / "scratch")
        p = _probe(root, tmp_path)
        for key, val in p.items():
            assert val.startswith(root), f"{key} escaped ALPHA_FACTORY_DATA_DIR: {val}"

    def test_empty_env_is_treated_as_unset(self, tmp_path):
        assert _probe("", tmp_path)["data_dir"] == "data"

    def test_pause_dir_follows_the_same_root(self, monkeypatch, tmp_path):
        monkeypatch.setenv(data_paths.ENV_VAR, str(tmp_path))
        assert data_paths.pause_dir() == tmp_path / ".pause"


class TestSuiteRunsInTheSandbox:
    """In-process: conftest moved the whole session into a symlink-mirrored sandbox cwd, so every
    cwd-relative `data/...` default resolves into scratch."""

    def test_the_session_does_not_run_from_the_repo_root(self):
        assert Path.cwd().resolve() != REPO_ROOT
        assert Path("data").resolve().parent != REPO_ROOT          # `data/` is NOT the repo's

    def test_the_sandbox_mirrors_everything_except_data(self):
        for name in ("src", "config", "scripts", "tests", "pyproject.toml", "KNOWN_RISKS.md"):
            assert Path(name).exists(), name
            assert Path(name).resolve() == REPO_ROOT / name
        assert not (Path.cwd() / "data").is_symlink()

    def test_the_env_override_is_not_set_so_cwd_relative_isolation_still_works(self):
        """An absolute process-wide override defeats the many tests that chdir(tmp_path)."""
        assert os.environ.get(data_paths.ENV_VAR) is None

    def test_runner_logger_and_guard_are_in_the_sandbox_not_the_live_tree(self):
        import src.runner as runner

        live = (REPO_ROOT / "data").resolve()
        for p in (runner.pipeline_logger.db_path, runner.guard.sentinel_dir):
            resolved = (Path.cwd() / p).resolve()
            assert live not in resolved.parents, resolved
            assert Path.cwd().resolve() in resolved.parents, resolved

    def test_failing_job_through_run_job_logs_to_the_sandbox_db(self, monkeypatch, tmp_path):
        """The leak regression: a job that raises 'boom' is recorded in the runner's logger DB — which
        must be the sandbox one, so the live audit DB never sees the fixture row."""
        import src.runner as runner
        from src.scheduler.dependency_guard import DependencyGuard
        from src.scheduler.job_registry import JOB_REGISTRY

        monkeypatch.setattr(runner, "guard", DependencyGuard(sentinel_dir=tmp_path / ".s"))
        original = JOB_REGISTRY["bronze_ohlcv_daily"]["fn"]
        JOB_REGISTRY["bronze_ohlcv_daily"]["fn"] = lambda d: (_ for _ in ()).throw(RuntimeError("boom"))
        try:
            with pytest.raises(RuntimeError, match="boom"):
                runner.run_job("bronze_ohlcv_daily", force=True, run_date=date(2026, 9, 29))
        finally:
            JOB_REGISTRY["bronze_ohlcv_daily"]["fn"] = original

        db = (Path.cwd() / runner.pipeline_logger.db_path).resolve()
        assert (REPO_ROOT / "data").resolve() not in db.parents
        with sqlite3.connect(db) as con:
            n = con.execute("SELECT COUNT(*) FROM pipeline_runs WHERE error_msg = 'boom'").fetchone()[0]
        assert n >= 1

    def test_a_quarantine_written_with_the_default_path_lands_in_the_sandbox(self):
        """What the FRED / IMF / EIA TestSchemaValidatorGate tests used to do to the live tree."""
        from src.bronze.schema_validator import SchemaValidator

        resolved = (Path.cwd() / SchemaValidator.QUARANTINE_PATH).resolve()
        assert (REPO_ROOT / "data").resolve() not in resolved.parents


class TestSandboxConstruction:

    def test_data_is_never_mirrored_even_when_the_real_repo_has_one(self, tmp_path, monkeypatch):
        """On the real machine the repo root HAS a data/ tree (GBs of live Bronze/Silver/Gold). Mirroring it
        would put that tree inside the sandbox and let any test write straight into it."""
        import tests.conftest as cf

        fake_repo = tmp_path / "repo"
        (fake_repo / "data" / "health").mkdir(parents=True)
        (fake_repo / "data" / "health" / "pipeline_runs.db").write_text("LIVE")
        (fake_repo / "src").mkdir()
        (fake_repo / "config").mkdir()
        (fake_repo / ".env").write_text("K=V")
        monkeypatch.setattr(cf, "_REPO_ROOT", fake_repo)
        sandbox = cf._build_sandbox_cwd()
        try:
            names = {e.name for e in sandbox.iterdir()}
            assert "data" not in names
            assert {"src", "config", ".env"} <= names             # everything else, hidden files included
            assert all((sandbox / n).is_symlink() for n in names)
            assert (fake_repo / "data" / "health" / "pipeline_runs.db").read_text() == "LIVE"
        finally:
            cf._remove_sandbox(sandbox)
        assert (fake_repo / "data" / "health" / "pipeline_runs.db").read_text() == "LIVE"


class TestSandboxCleanup:

    def test_cleanup_never_follows_a_symlink_into_the_real_tree(self, tmp_path):
        """If the sandbox teardown followed the mirror symlinks it would delete real repo files.
        (This is the exact failure that destroyed a working clone on 2026-11-05 when a MUTANT of this
        function ran against the real tree — mutants of this function must only ever run in a copy.)"""
        from tests.conftest import _remove_sandbox

        real = tmp_path / "real_repo"
        (real / "src").mkdir(parents=True)
        (real / "src" / "keep.py").write_text("x = 1\n")
        (real / "README.md").write_text("keep")
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        (sandbox / "src").symlink_to(real / "src", target_is_directory=True)
        (sandbox / "README.md").symlink_to(real / "README.md")
        (sandbox / "data" / "health").mkdir(parents=True)
        (sandbox / "data" / "health" / "scratch.db").write_text("junk")

        _remove_sandbox(sandbox)

        assert not sandbox.exists()
        assert (real / "src" / "keep.py").read_text() == "x = 1\n"
        assert (real / "README.md").read_text() == "keep"


class TestSnapshotHelper:
    """The session guard's detector: added / removed / changed files all show up."""

    def test_detects_added_removed_and_changed(self, tmp_path):
        a = tmp_path / "health"
        a.mkdir()
        (a / "keep.txt").write_text("x")
        (a / "gone.txt").write_text("x")
        (a / "edit.txt").write_text("x")
        before = snapshot_tree([a])
        (a / "gone.txt").unlink()
        (a / "new.txt").write_text("y")
        (a / "edit.txt").write_text("longer content")
        after = snapshot_tree([a])
        assert set(after) - set(before) == {str(a / "new.txt")}
        assert set(before) - set(after) == {str(a / "gone.txt")}
        assert {p for p in set(before) & set(after) if before[p] != after[p]} == {str(a / "edit.txt")}

    def test_missing_root_is_empty_not_an_error(self, tmp_path):
        assert snapshot_tree([tmp_path / "nope"]) == {}

    def test_unchanged_tree_compares_equal(self, tmp_path):
        (tmp_path / "f").write_text("x")
        assert snapshot_tree([tmp_path]) == snapshot_tree([tmp_path])
