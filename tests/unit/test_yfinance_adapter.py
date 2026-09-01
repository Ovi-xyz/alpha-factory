"""tests/unit/test_yfinance_adapter.py — yfinance adapter unit tests"""

from datetime import date

import pytest

from src.bronze.yfinance_adapter import (
    ForexDayCacheAdapter,
    YFinanceForexAdapter,
    YFinanceJKAdapter,
)


class TestYFinanceForexAdapter:

    def test_name(self):
        assert YFinanceForexAdapter().name == "yfinance_forex"

    def test_parse_eur_usd_slash(self):
        assert YFinanceForexAdapter._to_yf_symbol("EUR/USD") == "EURUSD=X"

    def test_parse_eur_usd_underscore(self):
        assert YFinanceForexAdapter._to_yf_symbol("EUR_USD") == "EURUSD=X"

    def test_parse_eurusd_no_sep(self):
        assert YFinanceForexAdapter._to_yf_symbol("EURUSD") == "EURUSD=X"

    def test_parse_already_api_format(self):
        assert YFinanceForexAdapter._to_yf_symbol("EURUSD=X") == "EURUSD=X"

    def test_parse_dxy(self):
        result = YFinanceForexAdapter._to_yf_symbol("DXY")
        assert result == "DX-Y.NYB"

    def test_parse_gbp_usd(self):
        result = YFinanceForexAdapter._to_yf_symbol("GBP/USD")
        assert result == "GBPUSD=X"


class TestYFinanceJKAdapter:

    def test_name(self):
        assert YFinanceJKAdapter().name == "yfinance_jk"

    def test_adds_jk_suffix(self):
        """Adapter maps IDX symbol to .JK format for yfinance."""
        adapter = YFinanceJKAdapter()
        # We test the symbol conversion indirectly
        # BBCA → should call with BBCA.JK
        assert adapter.name == "yfinance_jk"


class TestForexDayCacheAdapter:

    def test_name(self):
        assert ForexDayCacheAdapter().name == "forex_day_cache"

    def test_intraday_tf_returns_none(self):
        """Cache only works for daily+ granularity — intraday returns None."""
        adapter = ForexDayCacheAdapter()
        result  = adapter.fetch(
            "EUR_USD", "1H",
            date(2025, 1, 2), date(2025, 1, 3)
        )
        assert result is None

    def test_5m_tf_returns_none(self):
        adapter = ForexDayCacheAdapter()
        result  = adapter.fetch(
            "EUR_USD", "5m",
            date(2025, 1, 2), date(2025, 1, 3)
        )
        assert result is None


class TestNormalizeDf:
    """Coverage tranche (17 Aug 2026) — _normalize_df() standalone, and the
    entire YFinanceAdapter.fetch() HTTP-equivalent flow (mocking yfinance's
    Ticker.history), which had zero coverage: YFinanceAdapter wasn't even
    imported by the existing test suite."""

    def test_none_input_returns_none(self):
        from src.bronze.yfinance_adapter import _normalize_df
        assert _normalize_df(None) is None

    def test_empty_df_returns_none(self):
        import pandas as pd
        from src.bronze.yfinance_adapter import _normalize_df
        assert _normalize_df(pd.DataFrame()) is None

    def test_valid_df_normalizes_columns(self):
        import pandas as pd
        from src.bronze.yfinance_adapter import _normalize_df
        idx = pd.DatetimeIndex(["2025-01-02", "2025-01-03"], name="Date")
        df = pd.DataFrame({
            "Open": [100.0, 101.0], "High": [102.0, 103.0],
            "Low": [99.0, 100.0], "Close": [101.0, 102.0],
            "Volume": [1_000_000, 1_100_000],
        }, index=idx)
        result = _normalize_df(df)
        assert result is not None
        assert set(result.columns) == {"timestamp", "open", "high", "low", "close", "volume"}
        assert result["open"].to_list() == [100.0, 101.0]


class TestYFinanceAdapterFetch:

    def test_name(self):
        from src.bronze.yfinance_adapter import YFinanceAdapter
        assert YFinanceAdapter().name == "yfinance"

    def test_4h_disabled_raises_internally_and_returns_none(self, monkeypatch):
        """FIX YF-1: 4H is explicitly disabled and raises ValueError, which
        is caught by fetch()'s own outer except and surfaces as None."""
        from src.bronze.yfinance_adapter import YFinanceAdapter
        result = YFinanceAdapter().fetch("AAPL", "4H", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None

    def test_unsupported_tf_returns_none(self):
        from src.bronze.yfinance_adapter import YFinanceAdapter
        result = YFinanceAdapter().fetch("AAPL", "3D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None

    def test_successful_fetch_returns_normalized_df(self, monkeypatch):
        import pandas as pd
        from unittest.mock import MagicMock, patch
        from src.bronze.yfinance_adapter import YFinanceAdapter
        from src.utils.rate_limiter import SourceLimiters

        monkeypatch.setattr(SourceLimiters.yfinance, "wait", lambda: None)
        idx = pd.DatetimeIndex(["2025-01-02"], name="Date")
        hist_df = pd.DataFrame({
            "Open": [100.0], "High": [102.0], "Low": [99.0],
            "Close": [101.0], "Volume": [1_000_000],
        }, index=idx)
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = hist_df
        with patch("yfinance.Ticker", return_value=mock_ticker):
            result = YFinanceAdapter().fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is not None
        assert result["close"].to_list() == [101.0]

    def test_ticker_exception_caught_returns_none(self, monkeypatch):
        from unittest.mock import patch
        from src.bronze.yfinance_adapter import YFinanceAdapter
        from src.utils.rate_limiter import SourceLimiters

        monkeypatch.setattr(SourceLimiters.yfinance, "wait", lambda: None)
        with patch("yfinance.Ticker", side_effect=RuntimeError("network down")):
            result = YFinanceAdapter().fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None


class TestYFinanceForexAdapterFetchDelegation:
    """Coverage tranche (17 Aug 2026) — the delegation body (lines 133-134),
    distinct from the already-tested _to_yf_symbol() static conversions."""

    def test_delegates_to_yfinance_adapter_with_converted_symbol(self, monkeypatch):
        from unittest.mock import patch
        from src.bronze.yfinance_adapter import YFinanceAdapter, YFinanceForexAdapter
        with patch.object(YFinanceAdapter, "fetch", return_value="SENTINEL") as mock_fetch:
            result = YFinanceForexAdapter().fetch(
                "EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31)
            )
        assert result == "SENTINEL"
        mock_fetch.assert_called_once_with(
            "EURUSD=X", "1D", date(2025, 1, 2), date(2025, 1, 31)
        )


class TestYFinanceJKAdapterFetchDelegation:
    """Coverage tranche (17 Aug 2026) — the .JK-suffix delegation body
    (lines 165-166); prior test only asserted .name, never called .fetch()."""

    def test_appends_jk_suffix_when_absent(self, monkeypatch):
        from unittest.mock import patch
        from src.bronze.yfinance_adapter import YFinanceAdapter, YFinanceJKAdapter
        with patch.object(YFinanceAdapter, "fetch", return_value="SENTINEL") as mock_fetch:
            result = YFinanceJKAdapter().fetch(
                "BBCA", "1D", date(2025, 1, 2), date(2025, 1, 31)
            )
        assert result == "SENTINEL"
        mock_fetch.assert_called_once_with(
            "BBCA.JK", "1D", date(2025, 1, 2), date(2025, 1, 31)
        )

    def test_does_not_double_append_jk_suffix(self, monkeypatch):
        from unittest.mock import patch
        from src.bronze.yfinance_adapter import YFinanceAdapter, YFinanceJKAdapter
        with patch.object(YFinanceAdapter, "fetch", return_value="SENTINEL") as mock_fetch:
            YFinanceJKAdapter().fetch("BBCA.JK", "1D", date(2025, 1, 2), date(2025, 1, 31))
        mock_fetch.assert_called_once_with(
            "BBCA.JK", "1D", date(2025, 1, 2), date(2025, 1, 31)
        )


class TestForexDayCacheAdapterSuccessPath:
    """Coverage tranche (17 Aug 2026) — the daily+ cache-lookup success path
    (lines 188-190); prior tests only covered the intraday early-return."""

    def test_daily_tf_delegates_to_forex_day_cache(self, monkeypatch, tmp_path):
        import polars as pl
        from unittest.mock import patch
        from src.bronze.forex_cache import ForexDayCache
        from src.bronze.yfinance_adapter import ForexDayCacheAdapter

        sentinel_df = pl.DataFrame({"timestamp": ["2025-01-02"], "close": [1.10]})
        with patch.object(ForexDayCache, "load", return_value=sentinel_df) as mock_load:
            result = ForexDayCacheAdapter().fetch(
                "EUR_USD", "1D", date(2025, 1, 2), date(2025, 1, 3)
            )
        assert result is sentinel_df
        mock_load.assert_called_once_with("EUR_USD", date(2025, 1, 3))


class TestDropTrailingNullOhlc:
    """FIX (chat thread, 31 Aug 2026 live-test finding): market_ingester.py
    passes end=run_date to Ticker.history(). When the fetch executes before
    run_date's session has actually occurred (e.g. an early-WIB-morning
    Bronze run), yfinance can return a trailing placeholder row for that
    not-yet-traded day with null OHLC — SchemaValidator's not-nullable gate
    (config/schemas/yfinance_ohlcv.yaml) then quarantines the ENTIRE
    DataFrame over that one artifact row, discarding legitimate history too.
    Confirmed live: AAPL and many other us_stocks/context 1D symbols,
    live-test 2026-08-31 ("Column 'open': not nullable but has 1 nulls").
    """

    def test_single_trailing_null_row_dropped(self):
        import polars as pl
        from src.bronze.yfinance_adapter import _drop_trailing_null_ohlc

        df = pl.DataFrame({
            "timestamp": [date(2025, 1, 2), date(2025, 1, 3)],
            "open":  [100.0, None], "high": [102.0, None],
            "low":   [99.0, None],  "close": [101.0, None],
            "volume": [1_000_000, None],
        })
        result = _drop_trailing_null_ohlc(df)
        assert len(result) == 1
        assert result["timestamp"].to_list() == [date(2025, 1, 2)]

    def test_multiple_trailing_null_rows_dropped(self):
        import polars as pl
        from src.bronze.yfinance_adapter import _drop_trailing_null_ohlc

        df = pl.DataFrame({
            "timestamp": [date(2025, 1, i) for i in (2, 3, 4, 5)],
            "open":  [100.0, 101.0, None, None],
            "high":  [102.0, 103.0, None, None],
            "low":   [99.0, 100.0, None, None],
            "close": [101.0, 102.0, None, None],
        })
        result = _drop_trailing_null_ohlc(df)
        assert len(result) == 2
        assert result["timestamp"].to_list() == [date(2025, 1, 2), date(2025, 1, 3)]

    def test_no_null_rows_returns_unchanged(self):
        import polars as pl
        from src.bronze.yfinance_adapter import _drop_trailing_null_ohlc

        df = pl.DataFrame({
            "timestamp": [date(2025, 1, 2), date(2025, 1, 3)],
            "open": [100.0, 101.0], "high": [102.0, 103.0],
            "low": [99.0, 100.0], "close": [101.0, 102.0],
        })
        result = _drop_trailing_null_ohlc(df)
        assert len(result) == 2

    def test_mid_series_null_row_is_not_dropped(self):
        """A null row NOT at the tail is a genuine data-quality issue and
        must still reach SchemaValidator — only trailing artifacts are
        stripped, never a mid-series gap."""
        import polars as pl
        from src.bronze.yfinance_adapter import _drop_trailing_null_ohlc

        df = pl.DataFrame({
            "timestamp": [date(2025, 1, i) for i in (2, 3, 4)],
            "open":  [100.0, None, 102.0],
            "high":  [102.0, None, 104.0],
            "low":   [99.0, None, 101.0],
            "close": [101.0, None, 103.0],
        })
        result = _drop_trailing_null_ohlc(df)
        assert len(result) == 3   # unchanged — null row is mid-series, not trailing

    def test_partial_null_row_not_dropped(self):
        """Only a row where ALL of open/high/low/close are null counts as
        the trailing artifact — a row with just one null OHLC field is a
        different (and real) data-quality issue, left for SchemaValidator."""
        import polars as pl
        from src.bronze.yfinance_adapter import _drop_trailing_null_ohlc

        df = pl.DataFrame({
            "timestamp": [date(2025, 1, 2), date(2025, 1, 3)],
            "open":  [100.0, None],
            "high":  [102.0, 103.0],
            "low":   [99.0, 100.0],
            "close": [101.0, 102.0],
        })
        result = _drop_trailing_null_ohlc(df)
        assert len(result) == 2   # unchanged — not ALL of OHLC are null

    def test_all_rows_null_returns_empty(self):
        import polars as pl
        from src.bronze.yfinance_adapter import _drop_trailing_null_ohlc

        df = pl.DataFrame({
            "timestamp": [date(2025, 1, 2), date(2025, 1, 3)],
            "open": [None, None], "high": [None, None],
            "low": [None, None], "close": [None, None],
        })
        result = _drop_trailing_null_ohlc(df)
        assert len(result) == 0

    def test_normalize_df_end_to_end_strips_trailing_placeholder(self):
        """Full _normalize_df() flow: raw pandas history with a trailing
        not-yet-traded placeholder row → clean, all-valid Polars output."""
        import pandas as pd
        from src.bronze.yfinance_adapter import _normalize_df

        idx = pd.DatetimeIndex(["2025-01-02", "2025-01-03"], name="Date")
        df = pd.DataFrame({
            "Open": [100.0, None], "High": [102.0, None],
            "Low": [99.0, None], "Close": [101.0, None],
            "Volume": [1_000_000, None],
        }, index=idx)
        result = _normalize_df(df)
        assert result is not None
        assert len(result) == 1
        assert result["open"].null_count() == 0

    def test_normalize_df_returns_none_if_everything_is_placeholder(self):
        """If the ENTIRE fetch is nothing but placeholder rows (e.g. a
        single-row cold-start request landing before the session opens),
        _normalize_df() must return None — same contract as an empty
        DataFrame — rather than an empty-but-non-None result downstream
        code isn't expecting."""
        import pandas as pd
        from src.bronze.yfinance_adapter import _normalize_df

        idx = pd.DatetimeIndex(["2025-01-02"], name="Date")
        df = pd.DataFrame({
            "Open": [None], "High": [None],
            "Low": [None], "Close": [None], "Volume": [None],
        }, index=idx)
        assert _normalize_df(df) is None
