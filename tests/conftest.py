"""
conftest.py — IDD §10.1 (Testing Infrastructure v1.1)
Pytest fixtures untuk semua komponen pipeline.

Fixtures:
    tmp_bronze_path     — temporary Bronze directory dengan sample OHLCV
    sample_ohlcv_df     — minimal OHLCV DataFrame
    sample_silver_1d    — multi-symbol Silver 1D Parquet
    tmp_db_path         — temporary SQLite untuk ProgressCheckpoint tests
"""

from __future__ import annotations

import atexit
import os
import shutil
import tempfile
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

# ── GHR-01 (ADR-059): test isolation from the LIVE operational tree ────────────────────────────
# Measured on a pristine clone: running the UNMODIFIED suite created data/health/pipeline_runs.db,
# data/health/progress.db and 18 one-row quarantine stubs (EIA / IMF / FRED series) in the live tree.
# Cause, verified in source: src/runner.py builds its PipelineLogger and DependencyGuard at import time on
# cwd-relative `data/...` paths and the runner tests sandbox the guard only; the FRED / IMF / EIA
# TestSchemaValidatorGate tests push a one-row frame through a real SchemaValidator into the real
# quarantine path; ProgressCheckpoint defaults to data/health/progress.db. The Gatekeeper and health_report
# read exactly those stores.
#
# Mechanism: run the whole session from a SANDBOX WORKING DIRECTORY that mirrors the repo through symlinks
# — everything except `data/`. Every cwd-relative `data/...` default (health, quarantine, sentinels, pause,
# and the Bronze/Silver/Gold roots) then resolves into scratch, with no source change and no change to the
# many existing tests that already isolate themselves with `monkeypatch.chdir(tmp_path)`. A process-wide
# ALPHA_FACTORY_DATA_DIR was tried first and rejected: an absolute path shared by the whole session defeats
# cwd-relative isolation and leaks checkpoint/log state from one test into the next (15 failures).
_INVOCATION_DIR = Path.cwd()
_REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.pop("ALPHA_FACTORY_DATA_DIR", None)      # tests use the relative default, inside the sandbox


def _build_sandbox_cwd() -> Path:
    sandbox = Path(tempfile.mkdtemp(prefix="af_test_cwd_"))
    for entry in _REPO_ROOT.iterdir():
        if entry.name == "data":                     # the one thing the sandbox must NOT mirror
            continue
        (sandbox / entry.name).symlink_to(entry, target_is_directory=entry.is_dir())
    return sandbox


def _remove_sandbox(sandbox: Path) -> None:
    """Unlink the mirror symlinks (NEVER follow them into the repo), then delete the scratch data/."""
    for entry in sandbox.iterdir():
        if entry.is_symlink():
            entry.unlink()
        else:
            shutil.rmtree(entry, ignore_errors=True)
    try:
        sandbox.rmdir()
    except OSError:
        pass


_SANDBOX_CWD = _build_sandbox_cwd()
os.chdir(_SANDBOX_CWD)
atexit.register(_remove_sandbox, _SANDBOX_CWD)

# The live trees the guard watches (resolved from the real locations, not from the sandbox cwd): the
# invocation directory's `data/` (what every operator command uses) and the repo-root one.
_LIVE_OPERATIONAL_DIRS = sorted({
    (base / "data" / sub).resolve()
    for base in (_INVOCATION_DIR, _REPO_ROOT)
    for sub in ("health", "quarantine", ".sentinels", ".pause")
})


def snapshot_tree(roots) -> dict[str, tuple[int, int]]:
    """{path: (size, mtime_ns)} for every file under `roots` (missing roots are empty)."""
    snap: dict[str, tuple[int, int]] = {}
    for root in roots:
        root = Path(root)
        if not root.exists():
            continue
        for f in root.rglob("*"):
            if f.is_file():
                st = f.stat()
                snap[str(f)] = (st.st_size, st.st_mtime_ns)
    return snap


@pytest.fixture(scope="session", autouse=True)
def _live_operational_tree_untouched():
    """Fail the session if anything under the live data/health, data/quarantine, data/.sentinels or
    data/.pause changed while the suite ran (GHR-01 guard). Do not run the suite while the daily
    chain is running on the same checkout: a concurrent pipeline write is indistinguishable from a
    leaking test and will trip this guard."""
    before = snapshot_tree(_LIVE_OPERATIONAL_DIRS)
    yield
    after = snapshot_tree(_LIVE_OPERATIONAL_DIRS)
    added   = sorted(set(after) - set(before))
    removed = sorted(set(before) - set(after))
    changed = sorted(p for p in set(before) & set(after) if before[p] != after[p])
    assert not (added or removed or changed), (
        "the test suite modified the LIVE operational tree "
        f"(added={added[:5]}, removed={removed[:5]}, changed={changed[:5]}) — "
        "a test is writing to data/health, data/quarantine, data/.sentinels or data/.pause "
        "instead of a tmp_path (GHR-01)"
    )


@pytest.fixture
def tmp_bronze_path(tmp_path: Path) -> Path:
    """Temporary Bronze directory dengan structure Hive partition."""
    path = tmp_path / "bronze" / "market" / "ohlcv"
    path.mkdir(parents=True)
    return path


@pytest.fixture
def sample_ohlcv_df() -> pl.DataFrame:
    """Minimal OHLCV DataFrame — 20 trading days untuk AAPL."""
    base_date = date(2025, 1, 2)
    return pl.DataFrame({
        "symbol":    ["AAPL"] * 20,
        "timestamp": [
            base_date + timedelta(days=i) for i in range(20)
        ],
        "open":      [150.0 + i for i in range(20)],
        "high":      [155.0 + i for i in range(20)],
        "low":       [145.0 + i for i in range(20)],
        "close":     [152.0 + i for i in range(20)],
        "volume":    [1_000_000] * 20,
        "is_clean":  [True] * 20,
    })


@pytest.fixture
def sample_silver_1d(tmp_path: Path, sample_ohlcv_df: pl.DataFrame) -> str:
    """
    Sample Silver 1D Parquet — multiple symbols, 20 hari.
    Return glob pattern string.
    """
    symbols = ["AAPL", "MSFT", "BBCA", "EUR_USD"]
    markets  = ["us_stocks", "us_stocks", "idx", "forex"]
    dfs: list[pl.DataFrame] = []

    for sym, mkt in zip(symbols, markets):
        df = sample_ohlcv_df.with_columns([
            pl.lit(sym).alias("symbol"),
            pl.lit(mkt).alias("market"),
            (pl.col("close") * pl.col("volume")).alias("dollar_volume"),
        ])
        dfs.append(df)

    combined = pl.concat(dfs)
    out_dir = tmp_path / "silver" / "ohlcv_1d"
    out_dir.mkdir(parents=True)
    out_path = out_dir / "data.parquet"
    combined.write_parquet(out_path)

    return str(out_dir / "*.parquet")


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Temporary SQLite path untuk ProgressCheckpoint tests."""
    db_dir = tmp_path / "health"
    db_dir.mkdir(parents=True)
    return db_dir / "progress.db"


@pytest.fixture
def run_date() -> date:
    """Default test run_date."""
    return date(2025, 1, 22)


@pytest.fixture(autouse=True)
def _isolate_cross_asset_history(tmp_path_factory, monkeypatch):
    """
    ADD GMI-PIT-001 follow-up (ADR-050): the three weekly CrossAssetEngine
    run() functions now also write a PIT history snapshot
    (src/gold/cross_asset/history.py). Existing module tests already
    redirect each module's own snapshot path, but know nothing about the
    history root — without this they would write
    data/gold/cross_asset/history/... into the real repo tree (observed:
    *_2026-03-01.parquet files leaking after a run). Redirect it for every
    test; tests that exercise the history store itself override this.
    """
    import src.gold.cross_asset.history as history_mod
    monkeypatch.setattr(
        history_mod, "HISTORY_ROOT", tmp_path_factory.mktemp("cross_asset_history")
    )

    # Same reasoning for Gatekeeper's Bronze quarantine check: other tests in
    # this suite (bronze ingester tests) leave real files in data/quarantine/
    # with today's mtime, which would make Gatekeeper's "quarantine today"
    # signal depend on ambient repo state. Point it at an empty dir; the one
    # test that exercises the quarantine path overrides this explicitly.
    import src.utils.gatekeeper as gatekeeper_mod
    monkeypatch.setattr(
        gatekeeper_mod, "QUARANTINE_GLOB",
        str(tmp_path_factory.mktemp("no_quarantine") / "*.parquet"),
    )
