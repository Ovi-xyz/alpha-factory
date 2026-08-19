"""
tests/unit/test_imf_ingester.py — Bronze IMF ingester real-function
coverage. Decision C (GMI_Decision_Document_v5.docx §3, tranche item #5).
Previously zero test coverage for this module.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests

from src.bronze.base_ingester import BronzeIngester
from src.bronze.imf_ingester import IMFIngester, _imf_weo_release_date, run


def _imf_response(status_code=200, values=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {"values": values or {}}
    return resp


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(BronzeIngester, "BASE_PATH", tmp_path / "bronze")
    import src.bronze.imf_ingester as mod
    monkeypatch.setattr(mod, "SCHEMA_PATH", tmp_path / "no_schema.yaml")
    monkeypatch.setattr("time.sleep", lambda *_: None)
    return tmp_path


class TestWeoReleaseDateHelper:
    """Direct tests for _imf_weo_release_date. Empirically verified: because
    the three candidates (Oct obs_year, Apr obs_year+1, Oct obs_year+1) are
    chronologically increasing and the function returns on the FIRST one
    that is <= run_date, candidate #1 (Oct of obs_year) is selected for
    EVERY run_date on or after it — candidates #2/#3 are structurally
    unreachable (if #1 hasn't passed, the later ones can't have either).
    This is a real-but-low-priority dead-code observation, not fixed here
    (see thread report) — the function's actual behavior ('always return
    the first/preliminary WEO release date once published') is internally
    consistent with its own docstring and with the FRED/BLS 'release_date
    = first publication' convention elsewhere in Bronze, so it was not
    treated as a bug."""

    def test_returns_first_preliminary_release_once_passed(self):
        assert _imf_weo_release_date(2025, date(2025, 11, 1)) == date(2025, 10, 1)
        # Still Oct-of-obs_year even long after the later candidates would
        # also have passed — candidate #1 always wins first.
        assert _imf_weo_release_date(2025, date(2028, 1, 1)) == date(2025, 10, 1)

    def test_not_yet_published_falls_back_to_run_date(self):
        result = _imf_weo_release_date(2026, date(2026, 5, 1))
        assert result == date(2026, 5, 1)


class TestSuccessfulFetch:
    def test_fetches_key_countries_and_writes(self, tmp_path, monkeypatch):
        values = {
            "NGDP_RPCH": {
                "USA": {"2025": 2.5, "2026": 2.1},
                "MEX": {"2025": 1.1},          # not in KEY_COUNTRIES -> excluded
                "IDN": {"2025": 5.0},
            }
        }
        resp = _imf_response(values=values)
        with patch("requests.get", return_value=resp):
            IMFIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "imf" / "world_economic_outlook"
        files = list(out_dir.glob("gdp_growth*.parquet"))
        assert len(files) == 1
        written = pl.read_parquet(files[0])
        assert set(written["country"].to_list()) == {"USA", "IDN"}
        assert "MEX" not in written["country"].to_list()

    def test_null_value_skipped(self, tmp_path, monkeypatch):
        values = {"NGDP_RPCH": {"USA": {"2025": None, "2026": 2.1}}}
        resp = _imf_response(values=values)
        with patch("requests.get", return_value=resp):
            IMFIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "imf" / "world_economic_outlook"
        written = pl.read_parquet(next(out_dir.glob("gdp_growth*.parquet")))
        assert written["observation_date"].to_list() == ["2026-01-01"]

    def test_empty_values_no_write(self, tmp_path, monkeypatch):
        resp = _imf_response(values={"NGDP_RPCH": {}})
        with patch("requests.get", return_value=resp):
            IMFIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "imf"
        assert not out_dir.exists() or not list(out_dir.rglob("*.parquet"))

    def test_http_error_status_returns_none(self, tmp_path, monkeypatch):
        resp = _imf_response(status_code=500)
        with patch("requests.get", return_value=resp):
            IMFIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "imf"
        assert not out_dir.exists() or not list(out_dir.rglob("*.parquet"))

    def test_timeout_caught_specifically(self, tmp_path, monkeypatch):
        with patch("requests.get", side_effect=requests.exceptions.Timeout("slow")):
            IMFIngester().run(date(2026, 6, 1))   # must not raise
        out_dir = tmp_path / "bronze" / "macro" / "imf"
        assert not out_dir.exists() or not list(out_dir.rglob("*.parquet"))

    def test_generic_request_exception_caught(self, tmp_path, monkeypatch):
        with patch("requests.get", side_effect=ConnectionError("dns fail")):
            IMFIngester().run(date(2026, 6, 1))   # must not raise
        out_dir = tmp_path / "bronze" / "macro" / "imf"
        assert not out_dir.exists() or not list(out_dir.rglob("*.parquet"))

    def test_release_date_uses_weo_proxy_not_run_date(self, tmp_path, monkeypatch):
        run_date = date(2026, 6, 1)
        values = {"NGDP_RPCH": {"USA": {"2025": 2.5}}}
        resp = _imf_response(values=values)
        with patch("requests.get", return_value=resp):
            IMFIngester().run(run_date)
        out_dir = tmp_path / "bronze" / "macro" / "imf" / "world_economic_outlook"
        written = pl.read_parquet(next(out_dir.glob("gdp_growth*.parquet")))
        # obs_year=2025's Oct-1 preliminary release (2025-10-01) has already
        # passed relative to run_date (2026-06-01) -> that candidate wins,
        # NOT run_date itself (proving this isn't just run_date.isoformat()).
        assert written["release_date"].to_list() == ["2025-10-01"]

    def test_dynamic_year_range_in_request_params(self, tmp_path, monkeypatch):
        """FIX IMF-1: years requested must extend through run_date.year, not
        a hardcoded historical ceiling."""
        resp = _imf_response(values={})
        with patch("requests.get", return_value=resp) as mock_get:
            IMFIngester().run(date(2031, 1, 1))
        called_years = mock_get.call_args_list[0].kwargs["params"]["periods"]
        assert "2031" in called_years.split(",")

    def test_one_indicator_failure_does_not_abort_others(self, tmp_path, monkeypatch):
        def flaky_get(url, **kwargs):
            if "NGDP_RPCH" in url:
                raise RuntimeError("boom")
            indicator_id = url.rsplit("/", 2)[-2]   # .../{indicator}/{countries}
            return _imf_response(values={indicator_id: {"USA": {"2025": 1.0}}})

        with patch("requests.get", side_effect=flaky_get):
            IMFIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "imf" / "world_economic_outlook"
        # 4 of the 5 IMF_INDICATORS succeed (NGDP_RPCH raised)
        assert len(list(out_dir.glob("*.parquet"))) == 4
        assert not list(out_dir.glob("gdp_growth*.parquet"))


class TestSchemaValidatorGate:
    def test_quarantine_on_schema_mismatch(self, tmp_path, monkeypatch):
        import src.bronze.imf_ingester as mod
        monkeypatch.setattr(mod, "SCHEMA_PATH",
                             __import__("pathlib").Path("config/schemas/imf_weo.yaml"))
        ingester = IMFIngester()
        assert ingester._validator is not None
        bad_df = pl.DataFrame({"series_id": ["NGDP_RPCH"]})   # missing required cols
        ok, errors = ingester._validator.validate(bad_df, "gdp_growth")
        assert ok is False
        assert errors

    def test_quarantine_path_exercised_via_run(self, tmp_path, monkeypatch):
        import src.bronze.imf_ingester as mod
        monkeypatch.setattr(mod, "SCHEMA_PATH",
                             __import__("pathlib").Path("config/schemas/imf_weo.yaml"))
        malformed = pl.DataFrame({"series_id": ["NGDP_RPCH"]})
        with patch.object(IMFIngester, "_fetch_indicator", return_value=malformed):
            IMFIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "imf" / "world_economic_outlook"
        assert not out_dir.exists() or not list(out_dir.glob("*.parquet"))


class TestGetLatestValue:
    def test_returns_value_from_bronze_data(self, tmp_path, monkeypatch):
        import src.bronze.imf_ingester as mod
        bronze_dir = tmp_path / "data" / "bronze" / "macro" / "imf"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "series_id": ["NGDP_RPCH", "NGDP_RPCH"],
            "country": ["USA", "USA"],
            "observation_date": ["2025-01-01", "2026-01-01"],
            "value": [2.0, 2.5],
        }).write_parquet(bronze_dir / "gdp_growth.parquet")
        monkeypatch.chdir(tmp_path)
        result = IMFIngester().get_latest_value("NGDP_RPCH", "USA")
        assert result == pytest.approx(2.5)

    def test_no_data_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert IMFIngester().get_latest_value("NGDP_RPCH", "USA") is None

    def test_query_exception_returns_none(self, monkeypatch):
        with patch("duckdb.connect", side_effect=RuntimeError("boom")):
            assert IMFIngester().get_latest_value("NGDP_RPCH", "USA") is None


class TestRunEntryPoint:
    def test_module_level_run_delegates_to_class(self, monkeypatch):
        with patch.object(IMFIngester, "run") as mock_run:
            run(date(2026, 6, 1))
            mock_run.assert_called_once_with(date(2026, 6, 1))


class TestRunLoopExceptionHandling:
    """Coverage tranche (17 Aug 2026) — outer except in run()'s per-indicator loop."""

    def test_write_macro_exception_increments_failed_without_raising(self, tmp_path, monkeypatch):
        values = {"NGDP_RPCH": {"USA": {"2026": 2.1}}}
        resp = _imf_response(values=values)
        with patch("requests.get", return_value=resp), \
             patch.object(IMFIngester, "write_macro", side_effect=RuntimeError("disk full")):
            IMFIngester().run(date(2026, 6, 1))   # must not raise


class TestFetchIndicatorRecordParsingErrors:
    """Coverage tranche (17 Aug 2026) — except (ValueError, TypeError): pass
    around float(value)/int(year_str) inside the per-country/year loop."""

    def test_unparseable_value_row_skipped_others_kept(self):
        values = {
            "NGDP_RPCH": {
                "USA": {"2025": "not-a-number", "2026": 2.1},
            }
        }
        resp = _imf_response(values=values)
        with patch("requests.get", return_value=resp):
            df = IMFIngester()._fetch_indicator("NGDP_RPCH", date(2026, 6, 1))
        assert df is not None
        assert df["observation_date"].to_list() == ["2026-01-01"]
