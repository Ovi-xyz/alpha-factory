"""tests/unit/test_alphavantage_adapter.py — AlphaVantage adapter unit tests"""

from datetime import date

import pytest

from src.bronze.alphavantage_adapter import AlphaVantageForexAdapter


class TestAlphaVantageForexAdapter:

    def test_name(self):
        assert AlphaVantageForexAdapter().name == "alphavantage"

    def test_no_api_key_returns_none(self, monkeypatch):
        """Returns None when ALPHAVANTAGE_API_KEY not set."""
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "")
        adapter = AlphaVantageForexAdapter()
        result  = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None

    def test_budget_exhausted_returns_none(self, monkeypatch):
        """Returns None when daily budget is exhausted."""
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        from src.utils.rate_limiter import SourceLimiters
        # Exhaust the budget
        for _ in range(SourceLimiters.alphavantage.budget):
            SourceLimiters.alphavantage.record_call()

        adapter = AlphaVantageForexAdapter()
        result  = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None

    def test_parse_pair_eur_usd_slash(self):
        from_sym, to_sym = AlphaVantageForexAdapter._parse_pair("EUR/USD")
        assert from_sym == "EUR"
        assert to_sym   == "USD"

    def test_parse_pair_eurusd_no_sep(self):
        from_sym, to_sym = AlphaVantageForexAdapter._parse_pair("EURUSD")
        assert from_sym == "EUR"
        assert to_sym   == "USD"

    def test_parse_pair_eur_usd_underscore(self):
        from_sym, to_sym = AlphaVantageForexAdapter._parse_pair("EUR_USD")
        assert from_sym == "EUR"
        assert to_sym   == "USD"

    def test_parse_pair_with_x_suffix(self):
        from_sym, to_sym = AlphaVantageForexAdapter._parse_pair("EURUSD=X")
        assert from_sym == "EUR"
        assert to_sym   == "USD"

    def test_parse_dxy_returns_empty_to_signal_skip(self):
        """
        FIX NEW-5 [P3 LOW] (audit_v1_7_3_uncovered_findings.docx Section 6):
        FIX AV-2 (an earlier session) intentionally changed _parse_pair("DXY")
        to return ("", "") rather than proxy DXY via a USD/EUR pair — DXY is a
        weighted basket index, not a single currency pair, so an AV
        FX_DAILY(from=USD,to=EUR) call as a "proxy" silently returned a
        materially different series mislabeled as DXY. The caller
        (AlphaVantageForexAdapter.fetch) checks for this empty-tuple sentinel
        and skips AV entirely for DXY (falls through to the next adapter in
        the chain, e.g. yfinance DX-Y.NYB). This test previously asserted the
        OLD pre-AV-2 proxy behavor ("USD","EUR") and had been failing ever
        since — it tested removed behavior rather than the current contract.
        """
        from_sym, to_sym = AlphaVantageForexAdapter._parse_pair("DXY")
        assert from_sym == ""
        assert to_sym   == ""

    def test_parse_invalid_returns_empty(self):
        from_sym, to_sym = AlphaVantageForexAdapter._parse_pair("INVALID_SYMBOL_XYZ")
        assert from_sym == ""
        assert to_sym   == ""

    def test_unsupported_tf_returns_none(self, monkeypatch):
        """4H is not in AV FX function map → returns None."""
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        result  = adapter.fetch("EUR/USD", "4H", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None
