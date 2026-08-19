"""tests/unit/test_polygon_adapter.py — Polygon adapter unit tests"""

from datetime import date

import pytest

from src.bronze.polygon_adapter import PolygonAdapter, _TIMESPAN_MAP


class TestPolygonAdapter:

    def test_name(self):
        assert PolygonAdapter().name == "polygon"

    def test_no_api_key_returns_none(self, monkeypatch):
        """Returns None gracefully when no API key is set."""
        monkeypatch.setenv("POLYGON_API_KEY", "")
        adapter = PolygonAdapter()
        result  = adapter.fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None

    def test_unsupported_tf_returns_none(self, monkeypatch):
        """Unsupported timeframe returns None."""
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        result  = adapter.fetch("AAPL", "2H", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None


class TestPolygonTimespanMap:

    def test_all_standard_tfs_covered(self):
        """All standard timeframes must have a Polygon mapping."""
        required = {"5m", "15m", "1H", "4H", "1D", "1W", "1M"}
        assert required.issubset(set(_TIMESPAN_MAP.keys()))

    def test_1d_maps_to_day(self):
        mult, ts = _TIMESPAN_MAP["1D"]
        assert ts == "day"
        assert mult == 1

    def test_5m_maps_to_minute(self):
        mult, ts = _TIMESPAN_MAP["5m"]
        assert ts == "minute"
        assert mult == 5

    def test_1h_maps_to_hour(self):
        mult, ts = _TIMESPAN_MAP["1H"]
        assert ts == "hour"
        assert mult == 1

    def test_1w_maps_to_week(self):
        mult, ts = _TIMESPAN_MAP["1W"]
        assert ts == "week"

    def test_1m_maps_to_month(self):
        mult, ts = _TIMESPAN_MAP["1M"]
        assert ts == "month"


class TestFetchHttpFlow:
    """Coverage tranche (17 Aug 2026) — full fetch() HTTP flow. Zero prior
    coverage existed for lines 108-200 (request/pagination/response body).

    SourceLimiters.polygon is a real blocking RateLimiter (5 req/min free
    tier -> ~15s between calls). Every test below patches .wait() to a
    no-op so the suite doesn't actually sleep for real wall-clock time."""

    @pytest.fixture(autouse=True)
    def _no_throttle(self, monkeypatch):
        from src.utils.rate_limiter import SourceLimiters
        monkeypatch.setattr(SourceLimiters.polygon, "wait", lambda: None)
        monkeypatch.setattr("src.bronze.polygon_adapter.time.sleep", lambda s: None)

    @staticmethod
    def _resp(status_code=200, json_data=None):
        from unittest.mock import MagicMock
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data or {}
        return resp

    @staticmethod
    def _bar(t=1735689600000, o=150.0, h=152.0, l=149.0, c=151.0, v=1_000_000, vw=150.5):
        row = {}
        if t is not None: row["t"] = t
        if o is not None: row["o"] = o
        if h is not None: row["h"] = h
        if l is not None: row["l"] = l
        if c is not None: row["c"] = c
        if v is not None: row["v"] = v
        if vw is not None: row["vw"] = vw
        return row

    def test_hour_timespan_blocked_on_free_tier(self, monkeypatch):
        """FIX POL-1: 1H/4H ARE in _TIMESPAN_MAP (unlike wholly-unknown TFs
        like '2H'), but map to the paid-tier-only 'hour' timespan and must
        be blocked before any HTTP call is made."""
        from unittest.mock import patch
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        with patch("requests.get") as mock_get:
            result = adapter.fetch("AAPL", "1H", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None
        mock_get.assert_not_called()

    def test_successful_single_page_fetch(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        resp = self._resp(json_data={"results": [self._bar()], "status": "OK"})
        with patch("requests.get", return_value=resp):
            df = adapter.fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert df is not None
        assert len(df) == 1
        assert df["open"].to_list() == [150.0]
        assert df["volume"].to_list() == [1_000_000.0]   # FIX POL-5: float, not int

    def test_rate_limited_429_sleeps_and_returns_none(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        with patch("src.bronze.polygon_adapter.time.sleep") as mock_sleep, \
             patch("requests.get", return_value=self._resp(status_code=429)):
            result = adapter.fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None
        mock_sleep.assert_called_once()   # FIX POL-3: actual sleep occurs

    def test_non_200_non_429_returns_none(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        with patch("requests.get", return_value=self._resp(status_code=500)):
            result = adapter.fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None

    def test_missing_ohlcv_fields_yield_none_not_zero(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        bar = self._bar(o=None, v=None)   # missing 'o' and 'v' keys entirely
        resp = self._resp(json_data={"results": [bar], "status": "OK"})
        with patch("requests.get", return_value=resp):
            df = adapter.fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert df is not None
        assert df["open"].to_list() == [None]     # FIX POL-2
        assert df["volume"].to_list() == [None]    # FIX POL-5

    def test_pagination_follows_next_url_when_full_page(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        page1_results = [self._bar(t=1735689600000 + i) for i in range(50_000)]
        page1 = self._resp(json_data={
            "results": page1_results, "status": "OK",
            "next_url": "https://api.polygon.io/v2/aggs/next",
        })
        page2 = self._resp(json_data={"results": [self._bar(t=99999)], "status": "OK"})
        with patch("requests.get", side_effect=[page1, page2]) as mock_get:
            df = adapter.fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert df is not None
        assert len(df) == 50_001
        assert mock_get.call_count == 2
        # next_url call must have the API key appended, and no params dict
        second_call_args = mock_get.call_args_list[1]
        assert "apiKey=test_key" in second_call_args.args[0]
        assert second_call_args.kwargs["params"] is None

    def test_no_pagination_when_under_full_page(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        resp = self._resp(json_data={
            "results": [self._bar()], "status": "OK",
            "next_url": "https://api.polygon.io/v2/aggs/next",  # present but should be ignored
        })
        with patch("requests.get", return_value=resp) as mock_get:
            adapter.fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert mock_get.call_count == 1   # next_url NOT followed — under 50k rows

    def test_max_pages_cap_logs_warning(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        full_page = self._resp(json_data={
            "results": [self._bar(t=i) for i in range(50_000)], "status": "OK",
            "next_url": "https://api.polygon.io/v2/aggs/next",
        })
        with patch("requests.get", return_value=full_page):
            df = adapter.fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert df is not None   # still returns whatever was accumulated
        assert len(df) == 50_000 * 10   # MAX_PAGES=10 pages fetched

    def test_empty_results_across_all_pages_returns_none(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        resp = self._resp(json_data={"results": [], "status": "OK"})
        with patch("requests.get", return_value=resp):
            result = adapter.fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None

    def test_network_exception_returns_none(self, monkeypatch):
        from unittest.mock import patch
        monkeypatch.setenv("POLYGON_API_KEY", "test_key")
        adapter = PolygonAdapter()
        with patch("requests.get", side_effect=ConnectionError("network down")):
            result = adapter.fetch("AAPL", "1D", date(2025, 1, 2), date(2025, 1, 31))
        assert result is None
