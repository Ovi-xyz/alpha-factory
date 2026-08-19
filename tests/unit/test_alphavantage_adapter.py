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


class TestFetchHttpFlow:
    """Coverage tranche (17 Aug 2026) — full fetch() HTTP flow. Zero prior
    coverage existed for lines 90-173 (the entire request/response handling
    body) — only _parse_pair() and the two pre-request guard clauses (missing
    key, exhausted budget) had tests.

    Also fixes a latent test-isolation gap: SourceLimiters.alphavantage is a
    module-level singleton whose _used counter is never reset between tests,
    so test_budget_exhausted_returns_none (which deliberately exhausts it)
    could silently poison any test running after it in the same session —
    including, potentially, test_unsupported_tf_returns_none above, which
    would then pass for the wrong reason (budget-exhausted path) rather than
    the one it claims to test (unsupported-timeframe path)."""

    @pytest.fixture(autouse=True)
    def _reset_budget(self):
        from src.utils.rate_limiter import SourceLimiters
        SourceLimiters.alphavantage._reset_date = None
        yield
        SourceLimiters.alphavantage._reset_date = None

    @staticmethod
    def _resp(status_code=200, json_data=None):
        from unittest.mock import MagicMock
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data or {}
        return resp

    def test_unsupported_tf_returns_none_isolated(self, monkeypatch):
        """Same assertion as test_unsupported_tf_returns_none above, but
        under this class's guaranteed budget-reset fixture — confirms the
        original test's PASS is for the intended reason (line 94-97's
        `function is None` branch), not a coincidental budget-exhausted
        short-circuit from test-order leakage into a shared singleton."""
        from unittest.mock import patch
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        with patch("requests.get") as mock_get:
            result = adapter.fetch("EUR/USD", "4H", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None
        mock_get.assert_not_called()   # never reaches the HTTP call at all

    def test_dxy_skipped_before_http_call(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        with patch("requests.get") as mock_get:
            result = adapter.fetch("DXY", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None
        mock_get.assert_not_called()

    def test_http_non_200_returns_none_budget_not_consumed(self, monkeypatch):
        from unittest.mock import patch
        from src.utils.rate_limiter import SourceLimiters
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        before = SourceLimiters.alphavantage.used
        with patch("requests.get", return_value=self._resp(status_code=429)):
            result = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None
        assert SourceLimiters.alphavantage.used == before   # FIX AV-1: not consumed

    def test_api_information_message_returns_none(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        resp = self._resp(json_data={"Information": "rate limit hit"})
        with patch("requests.get", return_value=resp):
            result = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None

    def test_api_note_message_returns_none(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        resp = self._resp(json_data={"Note": "call frequency"})
        with patch("requests.get", return_value=resp):
            result = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None

    def test_missing_time_series_key_returns_none(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        resp = self._resp(json_data={"Meta Data": {"1. Information": "unrelated"}})
        with patch("requests.get", return_value=resp):
            result = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None

    def test_successful_fetch_returns_sorted_filtered_df(self, monkeypatch):
        from unittest.mock import patch
        from src.utils.rate_limiter import SourceLimiters
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        before = SourceLimiters.alphavantage.used
        resp = self._resp(json_data={
            "Time Series FX (Daily)": {
                "2025-01-15": {"1. open": "1.10", "2. high": "1.11", "3. low": "1.09", "4. close": "1.105"},
                "2025-01-10": {"1. open": "1.08", "2. high": "1.09", "3. low": "1.07", "4. close": "1.085"},
                "2024-12-01": {"1. open": "1.00", "2. high": "1.01", "3. low": "0.99", "4. close": "1.005"},
            }
        })
        with patch("requests.get", return_value=resp):
            df = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert df is not None
        assert df["timestamp"].to_list() == ["2025-01-10", "2025-01-15"]
        assert df["volume"].to_list() == [None, None]
        assert SourceLimiters.alphavantage.used == before + 1   # FIX AV-1: consumed on success

    def test_missing_ohlc_key_yields_none_not_zero(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        resp = self._resp(json_data={
            "Time Series FX (Daily)": {
                "2025-01-15": {"2. high": "1.11", "3. low": "1.09", "4. close": "1.105"},
            }
        })
        with patch("requests.get", return_value=resp):
            df = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert df is not None
        assert df["open"].to_list() == [None]   # FIX AV-3: None, not 0.0

    def test_malformed_date_key_row_skipped(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        resp = self._resp(json_data={
            "Time Series FX (Daily)": {
                "not-a-date": {"1. open": "1.10", "2. high": "1.11", "3. low": "1.09", "4. close": "1.105"},
                "2025-01-15": {"1. open": "1.10", "2. high": "1.11", "3. low": "1.09", "4. close": "1.105"},
            }
        })
        with patch("requests.get", return_value=resp):
            df = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert df is not None
        assert df["timestamp"].to_list() == ["2025-01-15"]

    def test_all_rows_out_of_range_returns_none(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        resp = self._resp(json_data={
            "Time Series FX (Daily)": {
                "2024-12-01": {"1. open": "1.00", "2. high": "1.01", "3. low": "0.99", "4. close": "1.005"},
            }
        })
        with patch("requests.get", return_value=resp):
            df = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert df is None

    def test_intraday_interval_param_applied(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        resp = self._resp(json_data={"Time Series FX (60min)": {}})
        with patch("requests.get", return_value=resp) as mock_get:
            adapter.fetch("EUR/USD", "1H", date(2025, 1, 2), date(2025, 1, 31))
        _, kwargs = mock_get.call_args
        assert kwargs["params"]["interval"] == "60min"

    def test_network_exception_returns_none(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_key")
        adapter = AlphaVantageForexAdapter()
        with patch("requests.get", side_effect=ConnectionError("network down")):
            result = adapter.fetch("EUR/USD", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None
