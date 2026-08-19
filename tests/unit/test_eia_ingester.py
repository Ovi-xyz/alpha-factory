"""
tests/unit/test_eia_ingester.py — Bronze EIA ingester real-function
coverage. Decision C (GMI_Decision_Document_v5.docx §3, tranche item #6).
Previously zero test coverage for this module.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.bronze.base_ingester import BronzeIngester
from src.bronze.eia_ingester import EIAIngester, run


def _eia_response(status_code=200, series_data=None):
    """Build a mock APIv2 response. series_data: list of row dicts, e.g.
    [{"period": "2026-05-06", "value": 450000.0}, ...]. None -> no
    'response' envelope at all (mirrors a real v2 error body)."""
    resp = MagicMock()
    resp.status_code = status_code
    if series_data is not None:
        resp.json.return_value = {"response": {"data": series_data}}
    else:
        resp.json.return_value = {"error": "series does not exist."}
    return resp


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(BronzeIngester, "BASE_PATH", tmp_path / "bronze")
    import src.bronze.eia_ingester as mod
    monkeypatch.setattr(mod, "SCHEMA_PATH", tmp_path / "no_schema.yaml")
    monkeypatch.setattr("time.sleep", lambda *_: None)
    return tmp_path


class TestApiKeyHandling:
    def test_no_key_still_attempts_v2_request(self, tmp_path, monkeypatch):
        monkeypatch.delenv("EIA_API_KEY", raising=False)
        with patch("requests.get", return_value=_eia_response(series_data=[])) as mock_get:
            EIAIngester().run(date(2026, 6, 1))
            assert mock_get.call_count == len(__import__(
                "src.bronze.eia_ingester", fromlist=["EIA_SERIES"]
            ).EIA_SERIES)
            assert "api_key" not in mock_get.call_args_list[0].kwargs["params"]
            # FIX ADR-038: series_id is now part of the URL path (v2's
            # /v2/seriesid/{id} route), not a query param.
            first_url = mock_get.call_args_list[0].args[0]
            assert first_url.startswith("https://api.eia.gov/v2/seriesid/")

    def test_key_present_included_in_params(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        with patch("requests.get", return_value=_eia_response(series_data=[])) as mock_get:
            EIAIngester().run(date(2026, 6, 1))
            assert mock_get.call_args_list[0].kwargs["params"]["api_key"] == "fake-key"


class TestSuccessfulFetch:
    def test_iso_period_parsed_and_written(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        resp = _eia_response(series_data=[
            {"period": "2026-05-06", "value": 450000.0},
            {"period": "2026-05-13", "value": 452000.0},
        ])
        with patch("requests.get", return_value=resp):
            EIAIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "eia" / "crude_oil"
        written = pl.read_parquet(next(out_dir.rglob("us_crude_stocks*.parquet")))
        assert set(written["observation_date"].to_list()) == {"2026-05-06", "2026-05-13"}
        assert written["series_name"].to_list()[0] == "us_crude_stocks"
        assert written["unit"].to_list()[0] == "thousand_barrels"
        # FIX EIA-3: release_date is always run_date, no lag proxy
        assert set(written["release_date"].to_list()) == {"2026-06-01"}

    def test_compact_8digit_period_normalized_to_iso(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        resp = _eia_response(series_data=[{"period": "20260506", "value": 450000.0}])
        with patch("requests.get", return_value=resp):
            EIAIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "eia" / "crude_oil"
        written = pl.read_parquet(next(out_dir.rglob("us_crude_stocks*.parquet")))
        assert written["observation_date"].to_list() == ["2026-05-06"]

    def test_null_value_row_skipped(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        resp = _eia_response(series_data=[
            {"period": "2026-05-06", "value": None},
            {"period": "2026-05-13", "value": 452000.0},
        ])
        with patch("requests.get", return_value=resp):
            EIAIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "eia" / "crude_oil"
        written = pl.read_parquet(next(out_dir.rglob("us_crude_stocks*.parquet")))
        assert len(written) == 1

    def test_no_response_envelope_no_write(self, tmp_path, monkeypatch):
        """FIX ADR-038: v2 error bodies omit the 'response' key entirely
        (e.g. {"error": "series does not exist."}) rather than v1's
        {"series": []}."""
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"error": "series does not exist."}
        with patch("requests.get", return_value=resp):
            EIAIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "eia" / "crude_oil"
        assert not out_dir.exists() or not list(out_dir.rglob("*.parquet"))

    def test_http_error_no_write(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        with patch("requests.get", return_value=_eia_response(status_code=503)):
            EIAIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "eia" / "crude_oil"
        assert not out_dir.exists() or not list(out_dir.rglob("*.parquet"))

    def test_request_exception_caught(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        with patch("requests.get", side_effect=ConnectionError("down")):
            EIAIngester().run(date(2026, 6, 1))   # must not raise
        out_dir = tmp_path / "bronze" / "macro" / "eia" / "crude_oil"
        assert not out_dir.exists() or not list(out_dir.rglob("*.parquet"))

    def test_all_four_series_fetched(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        with patch("requests.get", return_value=_eia_response(series_data=[])) as mock_get:
            EIAIngester().run(date(2026, 6, 1))
        assert mock_get.call_count == 4
        # FIX ADR-038: series_id is now embedded in the URL path, not params.
        urls_requested = [c.args[0] for c in mock_get.call_args_list]
        assert "https://api.eia.gov/v2/seriesid/PET.RWTC.W" in urls_requested


class TestIncrementalFetchWindow:
    def test_first_run_uses_five_year_lookback(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        run_date = date(2026, 6, 1)
        with patch("requests.get", return_value=_eia_response(series_data=[])) as mock_get:
            EIAIngester().run(run_date)
        expected_start = (run_date - timedelta(days=365 * 5)).isoformat()
        assert mock_get.call_args_list[0].kwargs["params"]["start"] == expected_start

    def test_incremental_run_uses_14_day_buffer_from_last_known(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        bronze_dir = tmp_path / "bronze" / "macro" / "eia" / "crude_oil"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "series_id": ["PET.WCRSTUS1.W"], "observation_date": ["2026-05-06"],
        }).write_parquet(bronze_dir / "seed.parquet")
        run_date = date(2026, 6, 1)
        with patch("requests.get", return_value=_eia_response(series_data=[])) as mock_get:
            EIAIngester().run(run_date)
        expected_start = (date(2026, 5, 6) - timedelta(days=14)).isoformat()
        first_call = mock_get.call_args_list[0]
        # FIX ADR-038: series_id confirmed via URL path, not params.
        assert first_call.args[0] == "https://api.eia.gov/v2/seriesid/PET.WCRSTUS1.W"
        assert first_call.kwargs["params"]["start"] == expected_start


class TestBuildLastKnownCache:
    def test_empty_when_no_bronze_data(self, tmp_path):
        assert EIAIngester()._build_last_known_cache() == {}

    def test_populated_from_real_bronze_fixture(self, tmp_path, monkeypatch):
        bronze_dir = tmp_path / "bronze" / "macro" / "eia" / "crude_oil"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "series_id": ["PET.RWTC.W", "PET.RWTC.W"],
            "observation_date": ["2026-04-01", "2026-05-06"],
        }).write_parquet(bronze_dir / "seed.parquet")
        cache = EIAIngester()._build_last_known_cache()
        assert cache["PET.RWTC.W"] == date(2026, 5, 6)


class TestSchemaValidatorGate:
    def test_quarantine_on_schema_mismatch(self, tmp_path, monkeypatch):
        import src.bronze.eia_ingester as mod
        monkeypatch.setattr(mod, "SCHEMA_PATH",
                             __import__("pathlib").Path("config/schemas/eia_oil.yaml"))
        ingester = EIAIngester()
        assert ingester._validator is not None
        bad_df = pl.DataFrame({"series_id": ["PET.RWTC.W"]})   # missing required cols
        ok, errors = ingester._validator.validate(bad_df, "PET.RWTC.W")
        assert ok is False
        assert errors

    def test_quarantine_path_exercised_via_run(self, tmp_path, monkeypatch):
        import src.bronze.eia_ingester as mod
        monkeypatch.setattr(mod, "SCHEMA_PATH",
                             __import__("pathlib").Path("config/schemas/eia_oil.yaml"))
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        malformed = pl.DataFrame({"series_id": ["PET.RWTC.W"]})
        with patch.object(EIAIngester, "_fetch_series", return_value=malformed):
            EIAIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "eia" / "crude_oil"
        assert not out_dir.exists() or not list(out_dir.rglob("*.parquet"))


class TestRunEntryPoint:
    def test_module_level_run_delegates_to_class(self, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        with patch.object(EIAIngester, "run") as mock_run:
            run(date(2026, 6, 1))
            mock_run.assert_called_once_with(date(2026, 6, 1))


class TestRunLoopExceptionHandling:
    """Coverage tranche (17 Aug 2026) — outer except in run()'s per-series loop."""

    def test_write_macro_exception_increments_failed_without_raising(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        resp = _eia_response(series_data=[{"period": "2026-05-06", "value": 450000.0}])
        with patch("requests.get", return_value=resp), \
             patch.object(EIAIngester, "write_macro", side_effect=RuntimeError("disk full")):
            EIAIngester().run(date(2026, 6, 1))   # must not raise


class TestFetchSeriesRecordParsingErrors:
    """Coverage tranche (17 Aug 2026) — malformed-row except/pass in _fetch_series."""

    def test_unparseable_value_row_skipped_others_kept(self, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        resp = _eia_response(series_data=[
            {"period": "2026-05-06", "value": "not-a-number"},
            {"period": "2026-05-13", "value": 452000.0},
        ])
        with patch("requests.get", return_value=resp):
            df = EIAIngester()._fetch_series("PET.RWTC.W", date(2026, 6, 1))
        assert df is not None
        assert df["observation_date"].to_list() == ["2026-05-13"]


class TestBuildLastKnownCacheMalformedDate:
    """Coverage tranche (17 Aug 2026) — except/pass around date.fromisoformat."""

    def test_malformed_date_row_excluded_from_cache(self, tmp_path):
        bronze_dir = tmp_path / "bronze" / "macro" / "eia" / "crude_oil"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "series_id": ["PET.RWTC.W"],
            "observation_date": ["not-a-date"],
        }).write_parquet(bronze_dir / "seed.parquet")
        cache = EIAIngester()._build_last_known_cache()
        assert "PET.RWTC.W" not in cache
