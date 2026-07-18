"""tests/unit/test_forex_cache.py — G4 ForexDayCache unit tests"""

from datetime import date, timedelta

import polars as pl
import pytest

from src.bronze.forex_cache import ForexDayCache


@pytest.fixture
def cache(tmp_path, monkeypatch):
    monkeypatch.setattr(ForexDayCache, "CACHE_PATH", tmp_path / "forex_cache")
    return ForexDayCache()


@pytest.fixture
def sample_forex_df():
    return pl.DataFrame({
        "timestamp": [date(2025, 1, 2)],
        "open":      [1.085],
        "high":      [1.090],
        "low":       [1.082],
        "close":     [1.087],
        "volume":    [0],
    })


class TestForexDayCache:

    def test_save_and_load(self, cache, sample_forex_df):
        """Saved data must be loadable next day with staleness=True."""
        run_date = date(2025, 1, 3)
        cache.save("EUR_USD", sample_forex_df, run_date - timedelta(days=1))
        loaded = cache.load("EUR_USD", run_date)
        assert loaded is not None
        assert "staleness" in loaded.columns
        assert loaded["staleness"].to_list()[0] is True

    def test_load_nonexistent_returns_none(self, cache):
        """load() returns None if no cache file exists."""
        result = cache.load("GBP_USD", date(2025, 5, 1))
        assert result is None

    def test_save_marks_staleness_false(self, cache, sample_forex_df):
        """save() marks staleness=False on the saved record."""
        run_date = date(2025, 1, 2)
        cache.save("EUR_USD", sample_forex_df, run_date)
        # Load as today (not tomorrow) — same day file
        path = cache.CACHE_PATH / f"EUR_USD_{run_date.isoformat()}.parquet"
        assert path.exists()
        saved = pl.read_parquet(path)
        assert "staleness" in saved.columns
        assert saved["staleness"].to_list()[0] is False

    def test_load_yesterday_correctly(self, cache, sample_forex_df):
        """load(run_date) looks for file dated run_date - 1 day."""
        yesterday = date(2025, 3, 14)
        today     = date(2025, 3, 15)
        cache.save("USD_JPY", sample_forex_df, yesterday)
        loaded = cache.load("USD_JPY", today)
        assert loaded is not None

    def test_is_stale_too_old_no_cache(self, cache):
        """is_stale_too_old returns True when no cache files exist."""
        result = cache.is_stale_too_old(date(2025, 5, 1))
        assert result is True

    def test_is_stale_too_old_with_recent_cache(self, cache, sample_forex_df):
        """is_stale_too_old returns False if recent cache exists."""
        run_date = date(2025, 3, 15)
        cache.save("EUR_USD", sample_forex_df, run_date - timedelta(days=1))
        result = cache.is_stale_too_old(run_date)
        assert result is False

    def test_cleanup_removes_old_files(self, cache, sample_forex_df):
        """cleanup_old_cache removes files older than keep_days."""
        old_date  = date(2025, 1, 2)
        new_date  = date(2025, 3, 15)
        cache.save("EUR_USD", sample_forex_df, old_date)
        cache.save("EUR_USD", sample_forex_df, new_date)
        removed = cache.cleanup_old_cache(keep_days=30)
        assert removed >= 1   # old_date file should be removed

    def test_multiple_pairs_independent(self, cache, sample_forex_df):
        """Different currency pairs are stored independently."""
        run_date = date(2025, 2, 10)
        yesterday = run_date - timedelta(days=1)
        cache.save("EUR_USD", sample_forex_df, yesterday)
        cache.save("GBP_USD", sample_forex_df, yesterday)

        loaded_eur = cache.load("EUR_USD", run_date)
        loaded_gbp = cache.load("GBP_USD", run_date)
        assert loaded_eur is not None
        assert loaded_gbp is not None
