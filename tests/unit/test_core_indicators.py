"""tests/unit/test_core_indicators.py — Gold core indicators test suite"""

from datetime import date, timedelta

import polars as pl
import pytest

from src.gold.indicators.core_indicators import (
    add_atr,
    add_ema,
    add_macd,
    add_momentum_features,
    add_rsi,
)


@pytest.fixture
def sample_signal_df():
    """20-row OHLCV DataFrame with symbol column for indicator testing."""
    n = 50
    base = date(2025, 1, 2)
    return pl.DataFrame({
        "symbol":    ["AAPL"] * n,
        "timestamp": [base + timedelta(days=i) for i in range(n)],
        "open":      [150.0 + i * 0.3 for i in range(n)],
        "high":      [155.0 + i * 0.3 for i in range(n)],
        "low":       [148.0 + i * 0.3 for i in range(n)],
        "close":     [152.0 + i * 0.3 for i in range(n)],
        "volume":    [1_000_000 + i * 5000 for i in range(n)],
    })


class TestCoreIndicators:

    def test_add_ema_columns_present(self, sample_signal_df):
        result = add_ema(sample_signal_df, periods=[9, 21])
        assert "ema_9"  in result.columns
        assert "ema_21" in result.columns

    def test_ema_all_periods(self, sample_signal_df):
        result = add_ema(sample_signal_df, periods=[9, 21, 50, 200])
        for p in [9, 21, 50, 200]:
            assert f"ema_{p}" in result.columns

    def test_ema_values_are_float(self, sample_signal_df):
        result = add_ema(sample_signal_df, periods=[9])
        assert result["ema_9"].dtype in (pl.Float32, pl.Float64)

    def test_add_rsi_range(self, sample_signal_df):
        """RSI values must be in [0, 100]."""
        result = add_rsi(sample_signal_df, periods=[14])
        valid = result["rsi_14"].drop_nulls()
        assert (valid >= 0).all()
        assert (valid <= 100).all()

    def test_rsi_both_periods(self, sample_signal_df):
        result = add_rsi(sample_signal_df, periods=[14, 28])
        assert "rsi_14" in result.columns
        assert "rsi_28" in result.columns

    def test_add_macd_columns(self, sample_signal_df):
        result = add_macd(sample_signal_df)
        assert "macd"        in result.columns
        assert "macd_signal" in result.columns
        assert "macd_hist"   in result.columns

    def test_macd_hist_is_diff(self, sample_signal_df):
        """macd_hist = macd - macd_signal."""
        result = add_macd(sample_signal_df)
        valid = result.filter(
            pl.col("macd").is_not_null()
            & pl.col("macd_signal").is_not_null()
            & pl.col("macd_hist").is_not_null()
        )
        expected = (valid["macd"] - valid["macd_signal"]).round(8)
        actual   = valid["macd_hist"].round(8)
        assert (expected - actual).abs().max() < 1e-6

    def test_add_atr_columns(self, sample_signal_df):
        result = add_atr(sample_signal_df, period=14)
        assert "atr_14"  in result.columns
        assert "atr_pct" in result.columns
        assert "tr"      in result.columns

    def test_atr_non_negative(self, sample_signal_df):
        """ATR must always be >= 0."""
        result = add_atr(sample_signal_df)
        valid = result["atr_14"].drop_nulls()
        assert (valid >= 0).all()

    def test_momentum_features_after_ema_rsi(self, sample_signal_df):
        """add_momentum_features needs ema_50 and rsi_14 present."""
        result = (
            sample_signal_df
            .pipe(add_ema,  periods=[50])
            .pipe(add_rsi,  periods=[14])
            .pipe(add_momentum_features)
        )
        assert "volume_sma_20"  in result.columns
        assert "relative_volume" in result.columns
        assert "trend_strength"  in result.columns
        assert "momentum_score"  in result.columns

    def test_trend_strength_binary(self, sample_signal_df):
        """trend_strength must be 0 or 1."""
        result = (
            sample_signal_df
            .pipe(add_ema, periods=[50])
            .pipe(add_rsi, periods=[14])
            .pipe(add_momentum_features)
        )
        valid = result["trend_strength"].drop_nulls()
        unique_vals = set(valid.to_list())
        assert unique_vals.issubset({0, 1})

    def test_pipeline_chaining(self, sample_signal_df):
        """Full pipeline chaining must not raise."""
        result = (
            sample_signal_df
            .pipe(add_ema,  periods=[9, 21, 50, 200])
            .pipe(add_rsi,  periods=[14, 28])
            .pipe(add_macd)
            .pipe(add_atr,  period=14)
            .pipe(add_momentum_features)
        )
        assert len(result) == len(sample_signal_df)
        assert len(result.columns) > len(sample_signal_df.columns)
