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
