"""tests/unit/test_inc_fetch.py — G1 IncFetchProtocol test suite"""

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from src.bronze.inc_fetch import FALLBACK_YEARS, IncFetchProtocol


class TestIncFetchProtocol:
    def setup_method(self):
        self.inc = IncFetchProtocol()
        self.run_date = date(2025, 6, 1)

    def test_no_prior_data_uses_fallback(self, tmp_path):
        """IDD §10.2: resolve_start_date — no data → run_date - fallback_years * 365"""
        result = self.inc.resolve_start_date(
            bronze_path=tmp_path / "ohlcv",
            symbol="AAPL",
            source="yfinance",
            run_date=self.run_date,
            fallback_years=10,
        )
        expected = self.run_date - timedelta(days=365 * 10)
        assert result == expected

    def test_reproducibility_same_run_date(self, tmp_path):
        """Same run_date must return same start_date — no date.today() dependency."""
        r1 = self.inc.resolve_start_date(
            tmp_path / "ohlcv", "AAPL", "yfinance",
            run_date=date(2025, 3, 1), fallback_years=5
        )
        r2 = self.inc.resolve_start_date(
            tmp_path / "ohlcv", "AAPL", "yfinance",
            run_date=date(2025, 3, 1), fallback_years=5
        )
        assert r1 == r2

    def test_different_run_dates_give_different_start(self, tmp_path):
        """Different run_dates → different start_dates."""
        r1 = self.inc.resolve_start_date(
            tmp_path / "ohlcv", "AAPL", "yfinance",
            run_date=date(2025, 3, 1), fallback_years=5
        )
        r2 = self.inc.resolve_start_date(
            tmp_path / "ohlcv", "AAPL", "yfinance",
            run_date=date(2025, 4, 1), fallback_years=5
        )
        assert r1 != r2

    def test_with_existing_data_uses_lookback(self, tmp_path):
        """IDD §10.2: existing data → last_date - DEFAULT_LOOKBACK_DAYS"""
        # Create a Bronze-like Parquet with timestamp
        last_date = date(2025, 5, 20)
        bronze_path = tmp_path / "ohlcv"
        source_dir = bronze_path / "source=yfinance" / "symbol=AAPL" / "year=2025" / "month=05"
        source_dir.mkdir(parents=True)

        df = pl.DataFrame({
            "timestamp": [last_date],
            "close": [150.0],
        })
        df.write_parquet(source_dir / "AAPL_raw_test.parquet")

        result = self.inc.resolve_start_date(
            bronze_path=bronze_path,
            symbol="AAPL",
            source="yfinance",
            run_date=self.run_date,
            fallback_years=10,
        )
        expected = last_date - timedelta(days=self.inc.DEFAULT_LOOKBACK_DAYS)
        assert result == expected

    def test_fallback_years_per_timeframe(self):
        """
        FALLBACK_YEARS dict harus memiliki semua Bronze TFs (6 TF setelah v1.5).
        v1.5: '4H' DIHAPUS dari FALLBACK_YEARS — Bronze tidak lagi menyimpan 4H.
        Silver mensintesis 4H dari Silver 1H (GD §4.1, §17.7).
        """
        required = {"5m", "15m", "1H", "1D", "1W", "1M"}
        assert required.issubset(set(FALLBACK_YEARS.keys()))
        # Verifikasi eksplisit: 4H TIDAK boleh ada
        assert "4H" not in FALLBACK_YEARS, (
            "4H dihapus dari FALLBACK_YEARS v1.5: Bronze tidak fetch/store 4H"
        )

    def test_1w_1m_fallback_is_15(self):
        """FIX v1.1: 1W/1M fallback buffer = 15 (not 10)."""
        assert FALLBACK_YEARS["1W"] == 15
        assert FALLBACK_YEARS["1M"] == 15
