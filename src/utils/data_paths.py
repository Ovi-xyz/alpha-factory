"""
src/utils/data_paths.py — GHR-01 (ADR-059, GMI-GHR-REC-001)

One resolver for the OPERATIONAL state directories — health (``pipeline_runs.db``, ``gate_status``,
``quality_gate_log``, ``progress.db``), Bronze quarantine, DependencyGuard sentinels and pause
markers — so a process can be pointed at a scratch tree instead of the live ``data/`` tree.

Why this exists (ADR-059 §2.5, verified in source and measured): ``src/runner.py`` builds its
``PipelineLogger`` at import time on the relative path ``data/health/pipeline_runs.db``, and the runner
tests sandbox only the sentinel guard, not the logger. Running the UNMODIFIED suite on a pristine clone
created ``data/health/pipeline_runs.db``, ``data/health/progress.db`` and 18 one-row quarantine stubs in
the live tree. Calibrating the Gatekeeper on those logs would encode the pollution into thresholds, so
the isolation comes first.

Contract:
    * ``ALPHA_FACTORY_DATA_DIR`` unset (production, every operator command, AND the test suite) ->
      ``Path("data")``, i.e. byte-identical to the previous hard-coded relative paths.
    * set -> that directory (an operator can point a one-off command at another tree).
    * The test suite does NOT use the override: ``tests/conftest.py`` runs the whole session from a
      sandbox working directory that mirrors the repo except ``data/``, so the cwd-relative default
      resolves into scratch. An absolute process-wide override was tried first and rejected: it defeats
      the many tests that isolate themselves with ``monkeypatch.chdir(tmp_path)`` and leaks checkpoint/log
      state from one test into the next.
    * Only the operational state directories above go through this module. Bronze / Silver / Gold DATA
      paths are not redirected here.

Module-level constants that call these functions are bound at import time; call-time defaults
(``DependencyGuard()``, ``PipelineLogger()``) resolve again on every construction.
"""

from __future__ import annotations

import os
from pathlib import Path

ENV_VAR = "ALPHA_FACTORY_DATA_DIR"


def data_dir() -> Path:
    """Root of the operational state tree: ``$ALPHA_FACTORY_DATA_DIR`` or ``data``."""
    raw = os.environ.get(ENV_VAR)
    return Path(raw) if raw else Path("data")


def health_dir() -> Path:
    return data_dir() / "health"


def quarantine_dir() -> Path:
    return data_dir() / "quarantine"


def sentinels_dir() -> Path:
    return data_dir() / ".sentinels"


def pause_dir() -> Path:
    """Pause markers (GHR-09): one ``<scope>.json`` per paused scope."""
    return data_dir() / ".pause"
