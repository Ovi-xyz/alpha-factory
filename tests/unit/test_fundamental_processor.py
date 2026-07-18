"""tests/unit/test_fundamental_processor.py — FundamentalProcessor unit tests"""

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from src.silver.fundamental_processor import FundamentalProcessor, CURRENT_SILVER_VERSION


@pytest.fixture
def proc():
    return FundamentalProcessor()


@pytest.fixture
def sample_earnings_silver(tmp_path, monkeypatch) -> Path:
    """Write sample earnings Silver Parquet for testing."""
    import src.silver.fundamental_processor as fp_mod
    run_date = date(2025, 3, 15)

    df = pl.DataFrame({
        "symbol":           ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN"],
        "earnings_date":    [
            date(2025, 3, 17),   # 2 days away
            date(2025, 3, 20),   # 5 days away
            date(2025, 4, 10),   # 26 days away
            date(2025, 3, 14),   # 1 day ago (past)
            date(2025, 5, 1),    # far future
        ],
        "days_to_earnings": [2, 5, 26, -1, 47],
        "eps_estimate":     [1.5, 2.8, 1.2, 3.5, 0.9],
        "eps_actual":       [None, None, None, 3.6, None],
        "quarter":          [1, 1, 1, 4, 1],
        "year":             [2025, 2025, 2025, 2024, 2025],
        "run_date":         [str(run_date)] * 5,
        "processing_version": [CURRENT_SILVER_VERSION] * 5,
        "is_reported":      [False, False, False, True, False],
    })

    silver_dir = tmp_path / "fundamental"
    silver_dir.mkdir()
    out_path = silver_dir / f"earnings_{run_date.isoformat()}.parquet"
    df.write_parquet(out_path)

    monkeypatch.setattr(fp_mod, "SILVER_FUNDAMENTAL", silver_dir)
    return silver_dir


class TestFundamentalProcessor:

    def test_get_days_to_earnings_known_symbol(self, proc, sample_earnings_silver):
        """Returns correct days_to_earnings for known symbol."""
        result = proc.get_days_to_earnings("AAPL", date(2025, 3, 15))
        assert result == 2

    def test_get_days_to_earnings_unknown_symbol(self, proc, sample_earnings_silver):
        """Returns None for unknown symbol."""
        result = proc.get_days_to_earnings("UNKNOWN_TICKER", date(2025, 3, 15))
        assert result is None

    def test_get_days_to_earnings_excludes_past(self, proc, sample_earnings_silver):
        """Past earnings (negative days_to_earnings) are not returned."""
        # NVDA had earnings yesterday (days=-1)
        result = proc.get_days_to_earnings("NVDA", date(2025, 3, 15))
        assert result is None   # Negative days filtered out

    def test_get_upcoming_earnings_within_7_days(self, proc, sample_earnings_silver):
        """get_upcoming_earnings returns only symbols within window."""
        df = proc.get_upcoming_earnings(date(2025, 3, 15), within_days=7)
        assert not df.is_empty()
        symbols = df["symbol"].to_list()
        assert "AAPL"  in symbols   # 2 days
        assert "MSFT"  in symbols   # 5 days
        assert "GOOGL" not in symbols  # 26 days — outside window

    def test_get_upcoming_earnings_excludes_past(self, proc, sample_earnings_silver):
        """Past earnings excluded from upcoming."""
        df = proc.get_upcoming_earnings(date(2025, 3, 15), within_days=30)
        symbols = df["symbol"].to_list()
        assert "NVDA" not in symbols   # Past earnings

    def test_get_upcoming_earnings_sorted_ascending(self, proc, sample_earnings_silver):
        """Results sorted by days_to_earnings ascending."""
        df = proc.get_upcoming_earnings(date(2025, 3, 15), within_days=90)
        if not df.is_empty():
            dtes = df["days_to_earnings"].to_list()
            assert dtes == sorted(dtes)

    def test_get_upcoming_earnings_empty_on_no_file(self, proc, tmp_path, monkeypatch):
        """Returns empty DataFrame when no Silver fundamental file exists."""
        import src.silver.fundamental_processor as fp_mod
        monkeypatch.setattr(fp_mod, "SILVER_FUNDAMENTAL", tmp_path / "nonexistent")
        df = proc.get_upcoming_earnings(date(2025, 1, 1), within_days=30)
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0

    def test_near_earnings_flag_logic(self):
        """near_earnings_flag should be True when days_to_earnings <= 3."""
        df = pl.DataFrame({
            "symbol":           ["AAPL", "MSFT", "GOOGL"],
            "days_to_earnings": [2, 5, None],
        }).with_columns([
            (
                pl.col("days_to_earnings").is_not_null()
                & (pl.col("days_to_earnings") <= 3)
            ).alias("near_earnings_flag")
        ])
        flags = dict(zip(df["symbol"].to_list(), df["near_earnings_flag"].to_list()))
        assert flags["AAPL"]  is True    # 2 days ≤ 3
        assert flags["MSFT"]  is False   # 5 days > 3
        assert flags["GOOGL"] is False   # None → False

    def test_processing_version_correct(self, sample_earnings_silver, proc):
        """Processed Silver has correct version."""
        assert CURRENT_SILVER_VERSION == "1.2"


class TestFundamentalProcessorNoData:

    def test_process_earnings_graceful_no_bronze(self, tmp_path, monkeypatch):
        """process_earnings gracefully handles missing Bronze data."""
        import src.silver.fundamental_processor as fp_mod
        monkeypatch.setattr(fp_mod, "BRONZE_FUNDAMENTAL", tmp_path / "no_bronze")
        monkeypatch.setattr(fp_mod, "SILVER_FUNDAMENTAL", tmp_path / "silver")
        proc = FundamentalProcessor()
        # Should not raise
        proc.process_earnings(date(2025, 3, 15))

    def test_process_earnings_writes_real_data(self, tmp_path, monkeypatch):
        """NEW — FIX FP-AIO-001 regression guard. process_earnings() shares
        fundamental_processor.py's missing atomic_write_parquet import with
        process_quotes() (see module import comment and
        test_process_quotes_reads_day_high_day_low) — this is the matching
        first-ever real (non-empty) invocation test for process_earnings(),
        closing the identical test gap that let the same bug hide here too.
        """
        import src.silver.fundamental_processor as fp_mod
        bronze_root = tmp_path / "bronze"
        silver_root = tmp_path / "silver"
        monkeypatch.setattr(fp_mod, "BRONZE_FUNDAMENTAL", bronze_root)
        monkeypatch.setattr(fp_mod, "SILVER_FUNDAMENTAL", silver_root)

        earnings_dir = bronze_root / "finnhub" / "earnings_calendar" / "symbol=AAPL"
        earnings_dir.mkdir(parents=True)
        bronze_df = pl.DataFrame({
            "symbol":           ["AAPL"],
            "earnings_date":    ["2025-03-17"],
            "eps_estimate":     [1.5],
            "eps_actual":       [None],
            "revenue_estimate": [90000000000.0],
            "quarter":          [1],
            "year":             [2025],
            "fetched_date":     ["2025-03-14"],
        })
        bronze_df.write_parquet(earnings_dir / "data.parquet")

        proc = FundamentalProcessor()
        proc.process_earnings(date(2025, 3, 15))  # must not raise (FP-AIO-001)

        out_path = silver_root / "earnings_2025-03-15.parquet"
        assert out_path.exists()
        result = pl.read_parquet(out_path)
        assert result["symbol"].to_list() == ["AAPL"]
        assert result["days_to_earnings"].to_list() == [2]

    def test_process_quotes_graceful_no_bronze(self, tmp_path, monkeypatch):
        """process_quotes gracefully handles missing Bronze data."""
        import src.silver.fundamental_processor as fp_mod
        monkeypatch.setattr(fp_mod, "BRONZE_FUNDAMENTAL", tmp_path / "no_bronze")
        monkeypatch.setattr(fp_mod, "SILVER_FUNDAMENTAL", tmp_path / "silver")
        proc = FundamentalProcessor()
        # Should not raise
        proc.process_quotes(date(2025, 3, 15))

    def test_process_quotes_reads_day_high_day_low(self, tmp_path, monkeypatch):
        """NEW — GMI_Decision_Document_v2.docx §5: locks in the Silver
        (consumer) side of the high_52w/low_52w -> day_high/day_low rename.

        This is a real, live consumer of finnhub_ingester.py's Bronze quote
        output — found by direct code trace while implementing the rename,
        contradicting the decision document's stated premise that the
        rename had zero consumers. Exercises process_quotes() end-to-end
        against a Bronze fixture shaped like the ACTUAL renamed producer
        output (day_high/day_low, plus the _symbol metadata column
        BronzeIngester.write() adds), not just the graceful-no-data path
        test_process_quotes_graceful_no_bronze already covers.
        """
        import src.silver.fundamental_processor as fp_mod
        bronze_root = tmp_path / "bronze"
        silver_root = tmp_path / "silver"
        monkeypatch.setattr(fp_mod, "BRONZE_FUNDAMENTAL", bronze_root)
        monkeypatch.setattr(fp_mod, "SILVER_FUNDAMENTAL", silver_root)

        quote_dir = bronze_root / "finnhub" / "quote" / "symbol=AAPL"
        quote_dir.mkdir(parents=True)
        bronze_df = pl.DataFrame({
            "_symbol":        ["AAPL"],
            "current_price":  [152.3],
            "change":         [1.2],
            "pct_change":     [0.8],
            "day_high":       [155.0],
            "day_low":        [149.0],
            "prev_close":     [151.1],
            "fetched_date":   ["2025-03-15"],
        })
        bronze_df.write_parquet(quote_dir / "data.parquet")

        proc = FundamentalProcessor()
        proc.process_quotes(date(2025, 3, 15))

        out_path = silver_root / "quotes_2025-03-15.parquet"
        assert out_path.exists()
        result = pl.read_parquet(out_path)
        assert "day_high" in result.columns
        assert "day_low" in result.columns
        assert "high_52w" not in result.columns
        assert "low_52w" not in result.columns
        assert result["day_high"].to_list() == [155.0]
        assert result["day_low"].to_list() == [149.0]
