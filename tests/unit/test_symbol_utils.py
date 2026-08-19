"""tests/unit/test_symbol_utils.py — G3 symbol_utils test suite"""

import pytest
from src.utils.symbol_utils import normalize_symbol, symbol_to_polygon_forex, to_api_symbol


class TestNormalizeSymbol:
    def test_brk_b_dot_replaced(self):
        assert normalize_symbol("BRK.B", "us_stocks") == "BRK-B"

    def test_eur_usd_slash_replaced(self):
        assert normalize_symbol("EUR/USD", "forex") == "EUR_USD"

    def test_mobileye_override(self):
        assert normalize_symbol("MOBILEYE", "us_stocks") == "MBLY"

    def test_uppercase_output(self):
        assert normalize_symbol("aapl", "us_stocks") == "AAPL"

    def test_no_dots_in_output(self):
        result = normalize_symbol("BRK.B", "us_stocks")
        assert "." not in result

    def test_no_slashes_in_output(self):
        result = normalize_symbol("EUR/USD", "forex")
        assert "/" not in result


class TestToApiSymbol:
    """G3 test cases dari Supplementary Design §4.6 checklist."""

    def test_spx_index_yfinance(self):
        assert to_api_symbol("SPX", "index", "yfinance") == "^GSPC"

    def test_vix_index_yfinance(self):
        assert to_api_symbol("VIX", "index", "yfinance") == "^VIX"

    def test_dxy_index_yfinance(self):
        assert to_api_symbol("DXY", "index", "yfinance") == "DX-Y.NYB"

    def test_eur_usd_forex_yfinance(self):
        assert to_api_symbol("EUR/USD", "forex", "yfinance") == "EURUSD=X"

    def test_bbca_idx_yfinance(self):
        assert to_api_symbol("BBCA", "idx", "yfinance") == "BBCA.JK"

    def test_aapl_us_stocks_yfinance(self):
        assert to_api_symbol("AAPL", "us_stocks", "yfinance") == "AAPL"

    def test_brk_b_us_stocks_polygon(self):
        # BRK.B is raw symbol for Polygon (no override needed, Polygon uses dot)
        assert to_api_symbol("BRK.B", "us_stocks", "polygon") == "BRK.B"

    def test_unknown_index_fallback(self):
        # Unknown index: prefix with ^
        result = to_api_symbol("FTSE", "index", "yfinance")
        assert result == "^FTSE"

    def test_commodity_yfinance_suffix(self):
        result = to_api_symbol("AU", "commodity", "yfinance")
        assert result.endswith("=F") or result == "AU=F"


class TestToApiSymbolAdditionalSources:
    """Coverage tranche (17 Aug 2026) — remaining source/market branches."""

    def test_dxy_forex_yfinance_uses_index_map(self):
        # DXY can arrive tagged market='forex' (pre-ADR-003 callers) as well
        # as market='index' — both must resolve to the same yfinance symbol.
        assert to_api_symbol("DXY", "forex", "yfinance") == "DX-Y.NYB"

    def test_idx_tvdatafeed_returns_raw_symbol(self):
        assert to_api_symbol("BBCA", "idx", "tvdatafeed") == "BBCA"

    def test_non_idx_tvdatafeed_returns_raw_symbol(self):
        assert to_api_symbol("CL", "commodity", "tvdatafeed") == "CL"

    def test_forex_alphavantage_strips_slash_no_suffix(self):
        assert to_api_symbol("EUR/USD", "forex", "alphavantage") == "EURUSD"

    def test_non_forex_alphavantage_returns_raw_symbol(self):
        assert to_api_symbol("AAPL", "us_stocks", "alphavantage") == "AAPL"

    def test_unrecognized_source_falls_back_to_raw_symbol(self):
        assert to_api_symbol("AAPL", "us_stocks", "some_future_source") == "AAPL"


class TestSymbolToPolygonForex:
    def test_eur_usd_converted_to_polygon_format(self):
        assert symbol_to_polygon_forex("EUR/USD") == "C:EURUSD"

    def test_no_slash_pair_still_prefixed(self):
        assert symbol_to_polygon_forex("USDJPY") == "C:USDJPY"
