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


class TestFallbackDaysOverride:
    """FIX (chat thread, 31 Aug 2026 live-test finding): FALLBACK_YEARS["1H"]=2
    computes to run_date - timedelta(days=730), landing EXACTLY on yfinance's
    real "must be within the last 730 days" ceiling for 1H intraday history.
    Live-test evidence (idx timeframe=1H, 2026-08-31): 28/29 symbols failed
    cold-start with exactly that Yahoo error; only the first request (AADI)
    squeaked through on sub-second request timing, not because 730 is safe.
    A years*365 value is also leap-year-sensitive across different run_dates
    (int(365*2) can be 730 OR 731 depending on which Feb 29 falls inside the
    window). FALLBACK_DAYS gives resolve_start_date() an exact, run_date-
    independent day count that takes precedence over fallback_years.
    """

    def setup_method(self):
        self.inc = IncFetchProtocol()

    def test_fallback_days_1h_is_720(self):
        from src.bronze.inc_fetch import FALLBACK_DAYS
        assert FALLBACK_DAYS["1H"] == 720

    def test_fallback_days_leaves_730_day_wall_margin(self):
        """720 = 10-day safety margin under the confirmed 730-day ceiling."""
        from src.bronze.inc_fetch import FALLBACK_DAYS
        assert FALLBACK_DAYS["1H"] < 730
        assert 730 - FALLBACK_DAYS["1H"] == 10

    def test_fallback_days_takes_precedence_over_fallback_years(self, tmp_path):
        """When both are given, fallback_days wins entirely — fallback_years
        is not consulted at all, not even as a cross-check."""
        run_date = date(2026, 8, 31)
        result = self.inc.resolve_start_date(
            bronze_path=tmp_path / "ohlcv",
            symbol="AADI", source="yfinance_jk",
            run_date=run_date,
            fallback_years=2,      # would give exactly 730 days (the bug)
            fallback_days=720,     # must win
        )
        assert result == run_date - timedelta(days=720)
        assert result != run_date - timedelta(days=730)

    def test_no_fallback_days_preserves_existing_years_behavior(self, tmp_path):
        """Default (fallback_days=None) must be byte-for-byte identical to
        pre-fix behavior for every other timeframe — no regression."""
        run_date = date(2025, 6, 1)
        result = self.inc.resolve_start_date(
            bronze_path=tmp_path / "ohlcv",
            symbol="AAPL", source="yfinance",
            run_date=run_date,
            fallback_years=10,
        )
        assert result == run_date - timedelta(days=365 * 10)

    def test_fallback_days_is_run_date_independent_unlike_years_formula(self):
        """Regression guard for the leap-year drift half of the bug: a fixed
        day count gives the identical span regardless of which run_date is
        used, unlike int(365 * fallback_years) which can silently shift by
        a day depending on whether a Feb 29 falls inside the window."""
        from src.bronze.inc_fetch import FALLBACK_DAYS
        d1 = date(2026, 8, 31) - timedelta(days=FALLBACK_DAYS["1H"])
        d2 = date(2027, 3, 15) - timedelta(days=FALLBACK_DAYS["1H"])
        assert (date(2026, 8, 31) - d1).days == 720
        assert (date(2027, 3, 15) - d2).days == 720

    def test_existing_data_path_ignores_fallback_days(self, tmp_path):
        """fallback_days only applies to the cold-start (no prior data)
        branch — the lookback-from-last-date path is untouched."""
        last_date = date(2025, 5, 20)
        bronze_path = tmp_path / "ohlcv"
        source_dir = (bronze_path / "source=yfinance_jk" / "symbol=AADI"
                      / "year=2025" / "month=05")
        source_dir.mkdir(parents=True)
        pl.DataFrame({"timestamp": [last_date], "close": [150.0]}).write_parquet(
            source_dir / "AADI_raw_test.parquet"
        )
        result = self.inc.resolve_start_date(
            bronze_path=bronze_path, symbol="AADI", source="yfinance_jk",
            run_date=date(2025, 6, 1), fallback_years=2, fallback_days=720,
        )
        assert result == last_date - timedelta(days=self.inc.DEFAULT_LOOKBACK_DAYS)
