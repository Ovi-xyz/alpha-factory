"""
tests/unit/test_bea_ingester_gld001.py — Test suite untuk GLD-001 fix.

FIX GLD-001: BEA NIPA unit-mixing — LineDescription filter di _fetch_nipa().
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.bronze.bea_ingester import BEAIngester, BEA_SERIES, LINE_FILTER


def _make_row(line_desc: str, time_period: str, value: float) -> dict:
    return {
        "LineDescription": line_desc,
        "TimePeriod":      time_period,
        "DataValue":       str(value),
        "CL_UNIT":         "Billions of chained 2017 dollars",
    }


def _mock_resp(rows: list[dict]) -> MagicMock:
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = {
        "BEAAPI": {"Results": {"Data": rows}}
    }
    return mock


def _fetch(spec: dict, rows: list[dict]) -> pl.DataFrame | None:
    """Helper: run _fetch_nipa with a mocked HTTP response."""
    ingester = BEAIngester.__new__(BEAIngester)
    ingester._api_key = "FAKE"
    ingester._validator = None
    with patch("requests.get", return_value=_mock_resp(rows)):
        return ingester._fetch_nipa(spec, date(2025, 6, 1))


class TestLineFilterConstants:
    """Verify LINE_FILTER dict structure."""

    def test_line_filter_defined_and_non_empty(self):
        assert isinstance(LINE_FILTER, dict)
        assert len(LINE_FILTER) >= 3

    def test_real_gdp_entry(self):
        assert LINE_FILTER.get("real_gdp") == "Gross domestic product"

    def test_pce_deflator_entry(self):
        assert "pce_deflator" in LINE_FILTER
        assert LINE_FILTER["pce_deflator"]

    def test_trade_balance_entry(self):
        assert "trade_balance" in LINE_FILTER
        assert LINE_FILTER["trade_balance"]

    def test_all_bea_series_covered(self):
        """Every BEA_SERIES entry must have a LINE_FILTER entry."""
        for s in BEA_SERIES:
            assert s["name"] in LINE_FILTER, (
                f"GLD-001: BEA series '{s['name']}' not in LINE_FILTER — unit-mixing risk"
            )


class TestBEANIPALineDescriptionFilter:
    """GLD-001: _fetch_nipa() must filter rows by LineDescription."""

    GDP_SPEC = {"name": "real_gdp", "table_name": "T10106",
                "dataset": "NIPA", "frequency": "Q"}

    def test_only_target_row_returned_from_multiple_rows(self):
        """From 5 rows only 1 (the target LineDescription) should survive."""
        rows = [
            _make_row("Gross domestic product",             "2025Q1", 22900.0),  # target
            _make_row("Personal consumption expenditures",  "2025Q1", 16100.0),
            _make_row("Gross private domestic investment",  "2025Q1",  4200.0),
            _make_row("Government consumption expenditures","2025Q1",  3800.0),
            _make_row("Net exports of goods and services",  "2025Q1", -1200.0),
        ]
        df = _fetch(self.GDP_SPEC, rows)

        assert df is not None
        assert len(df) == 1, (
            f"GLD-001: expected 1 row (target only), got {len(df)}. "
            "Unit-mixing if > 1 row per quarter."
        )
        assert df["value"][0] == pytest.approx(22900.0)

    def test_pce_deflator_filters_correct_row(self):
        spec = {"name": "pce_deflator", "table_name": "T20304",
                "dataset": "NIPA", "frequency": "Q"}
        target = LINE_FILTER["pce_deflator"]
        rows = [
            _make_row(target,            "2025Q1", 118.5),
            _make_row("Durable goods",   "2025Q1",  95.3),
            _make_row("Nondurable goods","2025Q1", 110.2),
        ]
        df = _fetch(spec, rows)
        assert df is not None
        assert len(df) == 1
        assert df["value"][0] == pytest.approx(118.5)

    def test_trade_balance_filters_correct_row(self):
        spec = {"name": "trade_balance", "table_name": "T40100",
                "dataset": "NIPA", "frequency": "Q"}
        target = LINE_FILTER["trade_balance"]
        rows = [
            _make_row("Exports of goods and services", "2025Q1",  3500.0),
            _make_row("Imports of goods and services", "2025Q1", -4700.0),
            _make_row(target,                           "2025Q1", -1200.0),
        ]
        df = _fetch(spec, rows)
        assert df is not None
        assert len(df) == 1
        assert df["value"][0] == pytest.approx(-1200.0)

    def test_no_matching_row_returns_none_or_empty(self):
        """If no row matches the LineDescription, return None or empty."""
        rows = [
            _make_row("Personal consumption expenditures", "2025Q1", 16100.0),
            _make_row("Gross private domestic investment", "2025Q1",  4200.0),
        ]
        df = _fetch(self.GDP_SPEC, rows)
        result_empty = (df is None) or (hasattr(df, "__len__") and len(df) == 0)
        assert result_empty, (
            "GLD-001: no matching target row → should return None or empty DataFrame"
        )

    def test_multi_quarter_one_row_each(self):
        """2 quarters → exactly 2 rows (1 per quarter)."""
        rows = [
            _make_row("Gross domestic product",            "2025Q1", 22900.0),
            _make_row("Personal consumption expenditures", "2025Q1", 16100.0),
            _make_row("Gross domestic product",            "2024Q4", 22700.0),
            _make_row("Personal consumption expenditures", "2024Q4", 15900.0),
        ]
        df = _fetch(self.GDP_SPEC, rows)
        assert df is not None
        assert len(df) == 2, f"GLD-001: 2 quarters → 2 rows, got {len(df)}"

    def test_unknown_series_no_filter(self):
        """Series not in LINE_FILTER → all rows pass through (backward-compat)."""
        spec = {"name": "unknown_series", "table_name": "T99999",
                "dataset": "NIPA", "frequency": "Q"}
        rows = [
            _make_row("Component A", "2025Q1", 100.0),
            _make_row("Component B", "2025Q1", 200.0),
        ]
        df = _fetch(spec, rows)
        assert df is not None
        assert len(df) == 2

    def test_exact_match_not_partial(self):
        """Partial match on LineDescription must NOT pass filter."""
        rows = [
            _make_row("Gross domestic product",           "2025Q1", 22900.0),  # exact → pass
            _make_row("Gross domestic product, billions", "2025Q1", 22900.0),  # partial → skip
            _make_row("Gross domestic product (note)",    "2025Q1", 22900.0),  # partial → skip
        ]
        df = _fetch(self.GDP_SPEC, rows)
        assert df is not None
        assert len(df) == 1, "Only exact match should pass filter"
