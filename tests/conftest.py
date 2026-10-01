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

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest


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
