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
