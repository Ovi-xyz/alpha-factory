"""
tests/unit/test_check_poetry_env.py

NEW — ADR-026 (poetry_conda_environment, 2026-07-19).
Tests for scripts/check_poetry_env.py: the pre/post diagnostic that
asserts `poetry install --with dev` lands inside the active conda env
rather than a separate Poetry-managed virtualenv.

Includes a permanent regression guard (TestArgparseSurface) for the
exact bug found empirically while implementing this ADR: the script's
argparse only declared `--post`, while the Makefile (setup/install/
doctor targets, same ADR) calls it with `--pre` explicitly. Running
`make setup` would have failed with "unrecognized arguments: --pre" on
the very first real use — caught by executing the command, not by
re-reading the document.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import check_poetry_env as cpe  # noqa: E402


@pytest.fixture
def fake_environment_yml(tmp_path, monkeypatch):
    """Point the module's ENV_YAML at a throwaway environment.yml."""
    env_file = tmp_path / "environment.yml"
    env_file.write_text("name: alpha-factory\nchannels:\n  - conda-forge\n")
    monkeypatch.setattr(cpe, "ENV_YAML", env_file)
    return env_file


class TestExpectedEnvName:

    def test_parses_name_field(self, fake_environment_yml):
        assert cpe._expected_env_name() == "alpha-factory"

    def test_missing_name_field_raises(self, tmp_path, monkeypatch):
        env_file = tmp_path / "environment.yml"
        env_file.write_text("channels:\n  - conda-forge\n")
        monkeypatch.setattr(cpe, "ENV_YAML", env_file)
        with pytest.raises(RuntimeError, match="no 'name:' field"):
            cpe._expected_env_name()


class TestCheckPre:

    def test_no_conda_prefix_active_fails(self, monkeypatch):
        monkeypatch.delenv("CONDA_PREFIX", raising=False)
        assert cpe._check_pre("alpha-factory") == 1

    def test_wrong_env_name_active_fails(self, monkeypatch):
        monkeypatch.setenv("CONDA_PREFIX", "/opt/conda/envs/some-other-env")
        assert cpe._check_pre("alpha-factory") == 1

    def test_correct_env_name_active_passes(self, monkeypatch):
        monkeypatch.setenv("CONDA_PREFIX", "/opt/conda/envs/alpha-factory")
        assert cpe._check_pre("alpha-factory") == 0


class TestCheckPost:

    def test_poetry_reused_active_env_passes(self, monkeypatch):
        monkeypatch.setenv("CONDA_PREFIX", "/opt/conda/envs/alpha-factory")

        def fake_run(cmd, capture_output, text):
            return subprocess.CompletedProcess(
                cmd, 0, stdout="/opt/conda/envs/alpha-factory\n"
            )
        monkeypatch.setattr(subprocess, "run", fake_run)
        assert cpe._check_post("alpha-factory") == 0

    def test_poetry_created_separate_venv_fails(self, monkeypatch):
        # The exact regression this script exists to catch: no active
        # conda env, Poetry silently falls back to its own cache dir.
        monkeypatch.delenv("CONDA_PREFIX", raising=False)

        def fake_run(cmd, capture_output, text):
            return subprocess.CompletedProcess(
                cmd, 0,
                stdout="/root/.cache/pypoetry/virtualenvs/alpha-factory-abc123-py3.12\n",
            )
        monkeypatch.setattr(subprocess, "run", fake_run)
        assert cpe._check_post("alpha-factory") == 1


class TestArgparseSurface:
    """Permanent regression guard for the --pre/--post CLI mismatch found
    empirically during ADR-026 implementation (see module docstring)."""

    def test_pre_flag_is_declared(self, fake_environment_yml, monkeypatch):
        monkeypatch.delenv("CONDA_PREFIX", raising=False)
        monkeypatch.setattr(sys, "argv", ["check_poetry_env.py", "--pre"])
        # Must not raise SystemExit(2) ("unrecognized arguments").
        assert cpe.main() == 1  # no CONDA_PREFIX -> real check still fails, but CLI parses

    def test_post_flag_is_declared(self, fake_environment_yml, monkeypatch):
        monkeypatch.setenv("CONDA_PREFIX", "/opt/conda/envs/alpha-factory")
        monkeypatch.setattr(sys, "argv", ["check_poetry_env.py", "--post"])

        def fake_run(cmd, capture_output, text):
            return subprocess.CompletedProcess(
                cmd, 0, stdout="/opt/conda/envs/alpha-factory\n"
            )
        monkeypatch.setattr(subprocess, "run", fake_run)
        assert cpe.main() == 0

    def test_no_flags_defaults_to_pre_check(self, fake_environment_yml, monkeypatch):
        monkeypatch.setenv("CONDA_PREFIX", "/opt/conda/envs/alpha-factory")
        monkeypatch.setattr(sys, "argv", ["check_poetry_env.py"])
        assert cpe.main() == 0
