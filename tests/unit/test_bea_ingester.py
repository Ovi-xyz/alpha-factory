"""
tests/unit/test_bea_ingester.py — BEAIngester coverage tranche (17 Aug 2026)

Complements test_bea_ingester_gld001.py, which covers ONLY the LineDescription/
LineNumber filtering logic inside _fetch_nipa() via a direct-call helper. This
file covers everything else: run() end-to-end (both the has-API-key and
no-API-key/FRED-mirror paths), _fetch_nipa()'s HTTP error branches, the
non-quarterly TimePeriod formats (annual "2025"/"2025A", unknown), the
malformed-row except/pass, and _run_via_fred_mirror() itself.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.bronze.bea_ingester import BEAIngester, BEA_SERIES, run


def _mock_resp(status_code: int = 200, rows: list[dict] | None = None) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = {"BEAAPI": {"Results": {"Data": rows or []}}}
    return mock


def _ingester(api_key: str = "FAKE", validator=None) -> BEAIngester:
    ing = BEAIngester.__new__(BEAIngester)
    ing._api_key = api_key
    ing._validator = validator
    return ing


class TestRunEntryPoint:

    def test_no_api_key_falls_back_to_fred_mirror(self, monkeypatch):
        monkeypatch.delenv("BEA_API_KEY", raising=False)
        ing = BEAIngester()
        with patch.object(ing, "_run_via_fred_mirror") as mock_mirror:
            ing.run(date(2026, 6, 1))
        mock_mirror.assert_called_once_with(date(2026, 6, 1))

    def test_successful_run_writes_all_series(self, monkeypatch):
        ing = _ingester()
        write_calls = []
        monkeypatch.setattr(ing, "write_macro", lambda **kw: write_calls.append(kw))
        df = pl.DataFrame({"value": [1.0]})
        with patch.object(ing, "_fetch_nipa", return_value=df):
            ing.run(date(2026, 6, 1))
        assert len(write_calls) == len(BEA_SERIES)
        assert {c["series_id"] for c in write_calls} == {s["name"] for s in BEA_SERIES}

    def test_schema_mismatch_quarantines_and_continues(self, monkeypatch):
        mock_validator = MagicMock()
        mock_validator.validate.return_value = (False, ["bad schema"])
        ing = _ingester(validator=mock_validator)
        write_calls = []
        monkeypatch.setattr(ing, "write_macro", lambda **kw: write_calls.append(kw))
        df = pl.DataFrame({"value": [1.0]})
        with patch.object(ing, "_fetch_nipa", return_value=df):
            ing.run(date(2026, 6, 1))
        assert write_calls == []
        assert mock_validator.handle_mismatch.call_count == len(BEA_SERIES)

    def test_fetch_exception_isolated_per_series(self, monkeypatch):
        """One series raising must not stop the loop for the others."""
        ing = _ingester()
        write_calls = []
        monkeypatch.setattr(ing, "write_macro", lambda **kw: write_calls.append(kw))

        def flaky(spec, run_date):
            if spec["name"] == "real_gdp":
                raise RuntimeError("simulated failure")
            return pl.DataFrame({"value": [1.0]})

        with patch.object(ing, "_fetch_nipa", side_effect=flaky):
            ing.run(date(2026, 6, 1))   # must not raise
        assert "real_gdp" not in {c["series_id"] for c in write_calls}
        assert len(write_calls) == len(BEA_SERIES) - 1

    def test_none_or_empty_df_skips_write(self, monkeypatch):
        ing = _ingester()
        write_calls = []
        monkeypatch.setattr(ing, "write_macro", lambda **kw: write_calls.append(kw))
        with patch.object(ing, "_fetch_nipa", return_value=None):
            ing.run(date(2026, 6, 1))
        assert write_calls == []


class TestFetchNipaHttpErrors:

    GDP_SPEC = {"name": "real_gdp", "table_name": "T10106", "dataset": "NIPA", "frequency": "Q"}

    def test_non_200_returns_none(self):
        ing = _ingester()
        with patch("requests.get", return_value=_mock_resp(status_code=503)):
            result = ing._fetch_nipa(self.GDP_SPEC, date(2026, 6, 1))
        assert result is None

    def test_empty_rows_returns_none(self):
        ing = _ingester()
        with patch("requests.get", return_value=_mock_resp(rows=[])):
            result = ing._fetch_nipa(self.GDP_SPEC, date(2026, 6, 1))
        assert result is None

    def test_network_exception_returns_none(self):
        ing = _ingester()
        with patch("requests.get", side_effect=ConnectionError("network down")):
            result = ing._fetch_nipa(self.GDP_SPEC, date(2026, 6, 1))
        assert result is None


class TestTimePeriodParsing:
    """Only the 'Q' (quarterly) TimePeriod format is exercised by the
    GLD-001 test file. This covers the annual and unknown-format branches."""

    GDP_SPEC = {"name": "real_gdp", "table_name": "T10106", "dataset": "NIPA", "frequency": "Q"}

    def _row(self, period: str, line_number: str = None) -> dict:
        row = {
            "LineDescription": "Gross domestic product",
            "TimePeriod": period,
            "DataValue": "22900.0",
            "CL_UNIT": "Billions of chained 2017 dollars",
        }
        if line_number:
            row["LineNumber"] = line_number
        return row

    def test_4digit_annual_format(self):
        ing = _ingester()
        with patch("requests.get", return_value=_mock_resp(rows=[self._row("2025")])):
            df = ing._fetch_nipa(self.GDP_SPEC, date(2026, 6, 1))
        assert df is not None
        assert df["observation_date"][0] == "2025-01-01"

    def test_letter_suffixed_annual_format(self):
        """e.g. '2025A' — annual with a type-code suffix."""
        ing = _ingester()
        with patch("requests.get", return_value=_mock_resp(rows=[self._row("2025A")])):
            df = ing._fetch_nipa(self.GDP_SPEC, date(2026, 6, 1))
        assert df is not None
        assert df["observation_date"][0] == "2025-01-01"

    def test_unknown_format_row_skipped(self):
        ing = _ingester()
        rows = [self._row("garbage"), self._row("2025Q1")]
        with patch("requests.get", return_value=_mock_resp(rows=rows)):
            df = ing._fetch_nipa(self.GDP_SPEC, date(2026, 6, 1))
        assert df is not None
        assert len(df) == 1
        assert df["observation_date"][0] == "2025-01-01"


class TestMalformedRowHandling:

    GDP_SPEC = {"name": "real_gdp", "table_name": "T10106", "dataset": "NIPA", "frequency": "Q"}

    def test_unparseable_data_value_row_skipped_others_kept(self):
        rows = [
            {"LineDescription": "Gross domestic product", "TimePeriod": "2025Q1",
             "DataValue": "N/A", "CL_UNIT": "Billions"},
            {"LineDescription": "Gross domestic product", "TimePeriod": "2025Q2",
             "DataValue": "23000.0", "CL_UNIT": "Billions"},
        ]
        ing = _ingester()
        with patch("requests.get", return_value=_mock_resp(rows=rows)):
            df = ing._fetch_nipa(self.GDP_SPEC, date(2026, 6, 1))
        assert df is not None
        assert len(df) == 1
        assert df["observation_date"][0] == "2025-04-01"


class TestRunViaFredMirror:

    def test_fred_api_key_present_delegates_to_fred_ingester(self, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "fake-fred-key")
        ing = BEAIngester.__new__(BEAIngester)
        with patch("src.bronze.fred_ingester.FREDIngester") as mock_cls:
            ing._run_via_fred_mirror(date(2026, 6, 1))
        mock_cls.return_value.run.assert_called_once()
        call_args = mock_cls.return_value.run.call_args
        assert call_args.args[0] == date(2026, 6, 1)
        assert "series_filter" in call_args.kwargs
        assert "GDPC1" in call_args.kwargs["series_filter"]

    def test_no_keys_at_all_logs_warning_no_raise(self, monkeypatch):
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        ing = BEAIngester.__new__(BEAIngester)
        ing._run_via_fred_mirror(date(2026, 6, 1))   # must not raise


class TestModuleLevelRun:

    def test_run_wrapper_delegates_to_class(self, monkeypatch):
        with patch.object(BEAIngester, "run") as mock_run:
            run(date(2026, 6, 1))
        mock_run.assert_called_once_with(date(2026, 6, 1))
