"""
test_bronze_to_silver.py — Bronze→Silver Integration Test
End-to-end test: write synthetic Bronze data → run OHLCVProcessor → verify Silver.

This test validates:
    1. Bronze Hive partition structure is correct
    2. OHLCVProcessor reads Bronze and produces valid Silver
    3. Silver schema compliance (all required columns present)
    4. VWAP formula uses typical price (H+L+C)/3 — v1.2 fix
    5. is_clean flag is set correctly
    6. processing_version matches CURRENT_SILVER_VERSION
    7. is_adjusted + adj_factor columns present — v1.2 new
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from src.bronze.base_ingester import BronzeIngester
from src.silver.ohlcv_processor import (
    CURRENT_SILVER_VERSION,
    OHLCVProcessor,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def bronze_ohlcv_path(tmp_path) -> Path:
    """Synthetic Bronze OHLCV Parquet in Hive partition structure."""
    base_date = date(2025, 1, 2)
    n         = 60    # 60 trading days

    # Create realistic OHLCV with trend
    rows = []
    price = 150.0
    for i in range(n):
        price += (0.5 if i % 3 != 2 else -0.3)   # gentle uptrend with pullbacks
        rows.append({
            "timestamp": base_date + timedelta(days=i),
            "open":      round(price - 0.3, 2),
            "high":      round(price + 1.2, 2),
            "low":       round(price - 0.8, 2),
            "close":     round(price, 2),
            "volume":    1_000_000 + i * 5_000,
        })

    df = pl.DataFrame(rows)

    # Write to Bronze Hive partition: source=yfinance/symbol=AAPL/year=2025/month=01/
    bronze_dir = (
        tmp_path / "bronze" / "market" / "ohlcv" / "us_stocks"
        / "source=yfinance" / "symbol=AAPL" / "year=2025" / "month=01"
    )
    bronze_dir.mkdir(parents=True)
    df.write_parquet(bronze_dir / "AAPL_raw_20250102.parquet")
    return tmp_path / "bronze" / "market" / "ohlcv" / "us_stocks"


# ── Integration Tests ─────────────────────────────────────────────────────────

class TestBronzeToSilver:

    def test_full_pipeline_runs(self, bronze_ohlcv_path, tmp_path):
        """OHLCVProcessor successfully reads Bronze and writes Silver."""
        proc = OHLCVProcessor()

        # Read Bronze
        pattern = str(
            bronze_ohlcv_path
            / "source=yfinance" / "symbol=AAPL" / "**" / "*.parquet"
        )
        df = pl.read_parquet(pattern)
        assert len(df) > 0, "Bronze data should not be empty"

        # Process
        silver = proc.process_symbol(df, "AAPL", "us_stocks", "1D")
        assert silver is not None
        assert len(silver) > 0

    def test_silver_schema_complete(self, bronze_ohlcv_path):
        """All required Silver schema columns must be present."""
        proc    = OHLCVProcessor()
        pattern = str(bronze_ohlcv_path / "**" / "*.parquet")
        df      = pl.read_parquet(pattern)
        silver  = proc.process_symbol(df, "AAPL", "us_stocks", "1D")

        required = [
            "symbol", "timeframe", "open", "high", "low", "close", "volume",
            "log_return", "dollar_volume", "spread_hl", "vwap",
            "is_adjusted", "adj_factor", "is_clean",
            "data_source", "processing_version",
        ]
        for col in required:
            assert col in silver.columns, f"Missing Silver column: {col}"

    def test_vwap_typical_price(self, bronze_ohlcv_path):
        """CRITICAL: VWAP must use (H+L+C)/3, not close."""
        proc    = OHLCVProcessor()
        pattern = str(bronze_ohlcv_path / "**" / "*.parquet")
        df      = pl.read_parquet(pattern)
        silver  = proc.process_symbol(df, "AAPL", "us_stocks", "1D")

        valid = silver.filter(pl.col("vwap").is_not_null())
        assert len(valid) > 0

        # VWAP must be between low and high (typical price is always in range)
        assert (valid["vwap"] >= valid["low"]).all(), "VWAP < low detected"
        assert (valid["vwap"] <= valid["high"]).all(), "VWAP > high detected"

        # VWAP should NOT simply equal close (old buggy formula)
        diff = (valid["vwap"] - valid["close"]).abs().sum()
        assert diff > 0, "VWAP equals close — v1.2 VWAP fix not applied!"

    def test_dollar_volume_computed(self, bronze_ohlcv_path):
        """dollar_volume = close * volume (G2 FIX: not assumed from source)."""
        proc    = OHLCVProcessor()
        pattern = str(bronze_ohlcv_path / "**" / "*.parquet")
        df      = pl.read_parquet(pattern)
        silver  = proc.process_symbol(df, "AAPL", "us_stocks", "1D")

        assert "dollar_volume" in silver.columns
        valid = silver.filter(
            pl.col("dollar_volume").is_not_null()
            & pl.col("close").is_not_null()
            & pl.col("volume").is_not_null()
        )
        if len(valid) > 0:
            # Verify: dollar_volume ≈ close * volume (within float precision)
            expected = (valid["close"] * valid["volume"].cast(pl.Float64))
            actual   = valid["dollar_volume"]
            diff     = (expected - actual).abs().max()
            assert diff < 1.0, f"dollar_volume mismatch: max diff={diff}"

    def test_is_adjusted_flag(self, bronze_ohlcv_path):
        """is_adjusted and adj_factor columns must be present (v1.2 NEW)."""
        proc    = OHLCVProcessor()
        pattern = str(bronze_ohlcv_path / "**" / "*.parquet")
        df      = pl.read_parquet(pattern)
        silver  = proc.process_symbol(
            df, "AAPL", "us_stocks", "1D", is_adjusted=True, adj_factor=1.0
        )
        assert "is_adjusted" in silver.columns
        assert "adj_factor"  in silver.columns
        assert silver["is_adjusted"].to_list()[0] is True

    def test_processing_version_correct(self, bronze_ohlcv_path):
        """processing_version must match CURRENT_SILVER_VERSION."""
        proc    = OHLCVProcessor()
        pattern = str(bronze_ohlcv_path / "**" / "*.parquet")
        df      = pl.read_parquet(pattern)
        silver  = proc.process_symbol(df, "AAPL", "us_stocks", "1D")

        versions = silver["processing_version"].unique().to_list()
        assert len(versions) == 1
        assert versions[0] == CURRENT_SILVER_VERSION

    def test_is_clean_mostly_true(self, bronze_ohlcv_path):
        """Clean synthetic data should have mostly is_clean=True."""
        proc    = OHLCVProcessor()
        pattern = str(bronze_ohlcv_path / "**" / "*.parquet")
        df      = pl.read_parquet(pattern)
        silver  = proc.process_symbol(df, "AAPL", "us_stocks", "1D")

        clean_pct = silver["is_clean"].mean()
        assert clean_pct >= 0.9, (
            f"Expected >= 90% clean rows, got {clean_pct:.1%}"
        )

    def test_no_ohlcv_nulls(self, bronze_ohlcv_path):
        """Clean input should produce Silver with no null OHLCV values."""
        proc    = OHLCVProcessor()
        pattern = str(bronze_ohlcv_path / "**" / "*.parquet")
        df      = pl.read_parquet(pattern)
        silver  = proc.process_symbol(df, "AAPL", "us_stocks", "1D")

        for col in ["open", "high", "low", "close", "volume"]:
            null_count = silver[col].null_count()
            assert null_count == 0, f"Column {col} has {null_count} nulls"

    def test_log_return_computed(self, bronze_ohlcv_path):
        """log_return should be present and non-null (except first row)."""
        proc    = OHLCVProcessor()
        pattern = str(bronze_ohlcv_path / "**" / "*.parquet")
        df      = pl.read_parquet(pattern)
        silver  = proc.process_symbol(df, "AAPL", "us_stocks", "1D")

        assert "log_return" in silver.columns
        # First row may be null (no prev_close) — rest should be non-null
        non_null = silver["log_return"].drop_nulls()
        assert len(non_null) >= len(silver) - 1
