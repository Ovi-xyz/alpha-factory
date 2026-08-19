"""
tests/unit/test_base_ingester.py — BronzeIngester.write()/write_macro()
idempotency-skip coverage. Coverage tranche (17 Aug 2026) — previously zero
test coverage for this module (only exercised indirectly via concrete
ingester subclasses' happy paths, which never triggered the same-day skip).
"""

from __future__ import annotations

import polars as pl
import pytest

from src.bronze.base_ingester import BronzeIngester


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(BronzeIngester, "BASE_PATH", tmp_path / "bronze")
    return tmp_path


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({"close": [100.0, 101.0]})


class TestWriteIdempotency:
    def test_first_write_returns_path(self, sample_df):
        result = BronzeIngester().write(
            sample_df, source="yfinance", asset_class="market/ohlcv/us_stocks", symbol="AAPL"
        )
        assert result is not None
        assert result.exists()

    def test_second_write_same_day_skips(self, sample_df):
        ingester = BronzeIngester()
        first = ingester.write(
            sample_df, source="yfinance", asset_class="market/ohlcv/us_stocks", symbol="AAPL"
        )
        second = ingester.write(
            sample_df, source="yfinance", asset_class="market/ohlcv/us_stocks", symbol="AAPL"
        )
        assert first is not None
        assert second is None   # FIX GD-F08: idempotent skip

    def test_extra_metadata_columns_added(self, sample_df):
        result = BronzeIngester().write(
            sample_df, source="yfinance", asset_class="market/ohlcv/us_stocks",
            symbol="AAPL", extra_metadata={"_tz_hint": "America/New_York"},
        )
        written = pl.read_parquet(result)
        assert written["_tz_hint"].to_list() == ["America/New_York", "America/New_York"]


class TestWriteMacroIdempotency:
    def test_first_write_macro_returns_path(self, sample_df):
        result = BronzeIngester().write_macro(
            sample_df, source="fred", domain="monetary_policy", series_id="FEDFUNDS"
        )
        assert result is not None
        assert result.exists()

    def test_second_write_macro_same_day_skips(self, sample_df):
        ingester = BronzeIngester()
        first = ingester.write_macro(
            sample_df, source="fred", domain="monetary_policy", series_id="FEDFUNDS"
        )
        second = ingester.write_macro(
            sample_df, source="fred", domain="monetary_policy", series_id="FEDFUNDS"
        )
        assert first is not None
        assert second is None   # FIX BI-1: idempotent skip

    def test_different_series_id_not_skipped(self, sample_df):
        ingester = BronzeIngester()
        ingester.write_macro(
            sample_df, source="fred", domain="monetary_policy", series_id="FEDFUNDS"
        )
        other = ingester.write_macro(
            sample_df, source="fred", domain="monetary_policy", series_id="DGS10"
        )
        assert other is not None
