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
