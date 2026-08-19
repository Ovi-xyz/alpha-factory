"""
tests/unit/test_bis_rates_ingester.py — GMI Wave 1
Test suite untuk src/bronze/bis_rates_ingester.py.

Dokumen referensi: Data Source & Rates Adjustment v1.0 §8.1
Pattern mocking mengikuti tests/unit/test_bea_ingester_gld001.py
(patch requests.get, tidak melakukan HTTP call nyata).
"""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.bronze.bis_rates_ingester import (
    BISCBRatesIngester,
    _BIS_ENDPOINT,
    _REF_AREA_MAP,
)


class TestBisEndpoint:
    """Regression guard for FIX BIS-1 (1 Aug 2026) -- the production
    ingester hardcodes its own copy of the endpoint (does not read
    config/bis_cb_rates.yaml), so this must be locked in independently of
    the config-file and preflight-script guards. Root cause: the real BIS
    dataflow ID is WS_CBPOL, not WS_CBPOL_D -- confirmed against
    data.bis.org's own indexed URLs and a live third-party code example
    for the sibling WS_CBTA dataflow. See module-level FIX BIS-1 comment
    for the full evidence trail."""

    def test_uses_correct_dataflow_id(self):
        assert "/data/dataflow/BIS/WS_CBPOL/1.0/" in _BIS_ENDPOINT
        assert "WS_CBPOL_D" not in _BIS_ENDPOINT

    def test_key_wildcards_freq_and_includes_all_ref_areas(self):
        key = _BIS_ENDPOINT.rsplit("/", 1)[-1]
        assert key.startswith(".")
        for ref_area in _REF_AREA_MAP:
            assert ref_area in key


def _mock_resp(csv_text: str) -> MagicMock:
    mock = MagicMock()
    mock.status_code = 200
    mock.text = csv_text
    mock.content = csv_text.encode()
    mock.raise_for_status = MagicMock()
    return mock


_SAMPLE_CSV = (
    "FREQ,REF_AREA,CB_POLICY_RATE,TIME_PERIOD,OBS_VALUE\n"
    "D,XM,Policy Rate,2026-01-15,3.50\n"
    "D,GB,Policy Rate,2026-01-15,4.75\n"
    "D,JP,Policy Rate,2026-01-15,0.50\n"
    "D,ZZ,Policy Rate,2026-01-15,9.99\n"   # unknown REF_AREA — should be dropped
)


class TestRefAreaMap:
    """Verify _REF_AREA_MAP structure — Data Source & Rates Adjustment v1.0 §3.1."""

    def test_has_12_entries(self):
        assert len(_REF_AREA_MAP) == 12

    def test_ecb_maps_to_xm(self):
        """ADR-010: ECB via BIS REF_AREA=XM, not FRED."""
        assert _REF_AREA_MAP["XM"] == "ECB"

    def test_indonesia_maps_to_bi(self):
        assert _REF_AREA_MAP["ID"] == "BI"

    def test_china_maps_to_pboc(self):
        assert _REF_AREA_MAP["CN"] == "PBOC"

    def test_all_expected_codes_present(self):
        expected = {"XM", "GB", "JP", "CA", "AU", "NZ", "CH", "KR", "NO", "SE", "CN", "ID"}
        assert set(_REF_AREA_MAP.keys()) == expected


class TestParseCsv:
    """Test _parse_csv() — parsing logic, isolated from HTTP fetch."""

    def setup_method(self):
        self.ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)

    def test_parses_known_ref_areas(self):
        df = self.ingester._parse_csv(_SAMPLE_CSV)
        assert df is not None
        assert len(df) == 3  # ZZ excluded — unknown REF_AREA

    def test_unknown_ref_area_excluded(self):
        df = self.ingester._parse_csv(_SAMPLE_CSV)
        assert "ZZ" not in df["ref_area"].to_list()

    def test_central_bank_column_mapped_correctly(self):
        df = self.ingester._parse_csv(_SAMPLE_CSV)
        row = df.filter(pl.col("ref_area") == "XM")
        assert row["central_bank"].to_list() == ["ECB"]

    def test_obs_date_parsed_as_date(self):
        df = self.ingester._parse_csv(_SAMPLE_CSV)
        assert df["obs_date"].dtype == pl.Date
        assert df["obs_date"][0] == date(2026, 1, 15)

    def test_rate_pct_parsed_as_float(self):
        df = self.ingester._parse_csv(_SAMPLE_CSV)
        assert df["rate_pct"].dtype == pl.Float64
        ecb_rate = df.filter(pl.col("ref_area") == "XM")["rate_pct"][0]
        assert ecb_rate == 3.50

    def test_missing_value_becomes_null(self):
        csv_with_gap = (
            "FREQ,REF_AREA,CB_POLICY_RATE,TIME_PERIOD,OBS_VALUE\n"
            "D,XM,Policy Rate,2026-01-16,\n"
        )
        df = self.ingester._parse_csv(csv_with_gap)
        assert df is not None
        assert df["rate_pct"][0] is None

    def test_source_and_ingested_at_columns_present(self):
        df = self.ingester._parse_csv(_SAMPLE_CSV)
        assert set(df["_source"].unique().to_list()) == {"bis_cbpol_d"}
        assert df["_ingested_at"][0] is not None

    def test_malformed_date_row_skipped(self):
        csv_bad_date = (
            "FREQ,REF_AREA,CB_POLICY_RATE,TIME_PERIOD,OBS_VALUE\n"
            "D,XM,Policy Rate,NOT-A-DATE,3.50\n"
            "D,GB,Policy Rate,2026-01-15,4.75\n"
        )
        df = self.ingester._parse_csv(csv_bad_date)
        assert len(df) == 1
        assert df["ref_area"][0] == "GB"

    def test_no_time_period_header_returns_none(self):
        garbage = "not,a,valid,csv,at,all\n1,2,3,4,5\n"
        df = self.ingester._parse_csv(garbage)
        assert df is None

    def test_empty_after_filter_returns_none(self):
        """All rows have unknown REF_AREA — should return None, not empty df."""
        csv_all_unknown = (
            "FREQ,REF_AREA,CB_POLICY_RATE,TIME_PERIOD,OBS_VALUE\n"
            "D,ZZ,Policy Rate,2026-01-15,1.00\n"
        )
        df = self.ingester._parse_csv(csv_all_unknown)
        assert df is None

    def test_metadata_preamble_lines_skipped(self):
        """BIS CSV may have comment/metadata lines before the real header."""
        csv_with_preamble = (
            "# BIS Statistics Warehouse Export\n"
            "# Generated 2026-06-30\n"
            "FREQ,REF_AREA,CB_POLICY_RATE,TIME_PERIOD,OBS_VALUE\n"
            "D,XM,Policy Rate,2026-01-15,3.50\n"
        )
        df = self.ingester._parse_csv(csv_with_preamble)
        assert df is not None
        assert len(df) == 1


class TestFetchCsv:
    """Test _fetch_csv() HTTP retry logic — no real network calls."""

    def test_successful_fetch_returns_text(self):
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        with patch("requests.get", return_value=_mock_resp(_SAMPLE_CSV)) as mock_get:
            result = ingester._fetch_csv()
        assert result == _SAMPLE_CSV
        mock_get.assert_called_once()

    def test_no_api_key_required_in_url(self):
        """BIS CBPOL_D requires no API key (GD §3.4)."""
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        with patch("requests.get", return_value=_mock_resp(_SAMPLE_CSV)) as mock_get:
            ingester._fetch_csv()
        call_args = mock_get.call_args
        url = call_args[0][0] if call_args[0] else call_args.kwargs.get("url", "")
        assert "key=" not in url.lower()
        assert "apikey" not in url.lower()

    def test_retries_on_failure_then_succeeds(self):
        import requests as req_module
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        responses = [req_module.RequestException("timeout"), _mock_resp(_SAMPLE_CSV)]
        with patch("requests.get", side_effect=responses):
            with patch("time.sleep"):  # skip real sleep in test
                result = ingester._fetch_csv()
        assert result == _SAMPLE_CSV

    def test_all_retries_exhausted_returns_none(self):
        import requests as req_module
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        with patch("requests.get", side_effect=req_module.RequestException("down")):
            with patch("time.sleep"):
                result = ingester._fetch_csv()
        assert result is None


class TestRunIntegration:
    """Test run() orchestration — fetch, parse, validate, write."""

    def test_run_writes_bronze_on_success(self, tmp_path, monkeypatch):
        from src.bronze import bis_rates_ingester as mod
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        ingester._validator = MagicMock()
        ingester._validator.validate.return_value = (True, [])

        write_calls = []
        ingester.write = lambda df, source, asset_class, symbol: write_calls.append(
            (source, asset_class, symbol, len(df))
        ) or (tmp_path / "fake.parquet")

        with patch("requests.get", return_value=_mock_resp(_SAMPLE_CSV)):
            ingester.run(run_date=date(2026, 6, 30))

        assert len(write_calls) == 1
        source, asset_class, symbol, n_rows = write_calls[0]
        assert source == "bis_cbpol_d"
        assert n_rows == 3

    def test_run_aborts_on_schema_mismatch(self):
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        ingester._validator = MagicMock()
        ingester._validator.validate.return_value = (False, ["fake error"])
        ingester._validator.handle_mismatch = MagicMock()
        ingester.write = MagicMock()

        with patch("requests.get", return_value=_mock_resp(_SAMPLE_CSV)):
            ingester.run(run_date=date(2026, 6, 30))

        ingester._validator.handle_mismatch.assert_called_once()
        ingester.write.assert_not_called()

    def test_run_aborts_gracefully_on_fetch_failure(self):
        import requests as req_module
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        ingester._validator = MagicMock()
        ingester.write = MagicMock()

        with patch("requests.get", side_effect=req_module.RequestException("down")):
            with patch("time.sleep"):
                ingester.run(run_date=date(2026, 6, 30))  # must not raise

        ingester.write.assert_not_called()


class TestModuleRunFunction:
    """Test module-level run() entry point used by job_registry.py."""

    def test_run_function_delegates_to_ingester(self):
        from src.bronze import bis_rates_ingester as mod
        with patch.object(mod.BISCBRatesIngester, "run") as mock_run:
            mod.run(run_date=date(2026, 6, 30))
        mock_run.assert_called_once_with(run_date=date(2026, 6, 30))


class TestRunParseCsvNone:
    """Coverage tranche (17 Aug 2026) — run()'s abort branch (lines 145-146)
    when _parse_csv() itself returns None despite a successful HTTP fetch
    (garbage/unparseable content) — distinct from test_run_aborts_
    gracefully_on_fetch_failure, which covers _fetch_csv() failing instead."""

    def test_run_aborts_when_parse_returns_none(self):
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        ingester._validator = MagicMock()
        ingester.write = MagicMock()

        with patch("requests.get", return_value=_mock_resp("not a valid bis csv at all")):
            ingester.run(run_date=date(2026, 6, 30))   # must not raise

        ingester.write.assert_not_called()
        ingester._validator.validate.assert_not_called()


class TestParseCsvColumnDetection:
    """Coverage tranche (17 Aug 2026) — REF_AREA alternative-column-name
    detection (lines 242-251) and the missing time_period/obs_value guard
    (lines 253-257)."""

    def setup_method(self):
        self.ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)

    def test_alternative_ref_area_column_name_detected(self):
        """'country' is accepted as an alias for 'ref_area'."""
        csv_alt_col = (
            "FREQ,COUNTRY,CB_POLICY_RATE,TIME_PERIOD,OBS_VALUE\n"
            "D,XM,Policy Rate,2026-01-15,3.50\n"
        )
        df = self.ingester._parse_csv(csv_alt_col)
        assert df is not None
        assert df["ref_area"][0] == "XM"

    def test_no_ref_area_column_at_all_returns_none(self):
        csv_no_ref_area = (
            "FREQ,SOMETHING_ELSE,CB_POLICY_RATE,TIME_PERIOD,OBS_VALUE\n"
            "D,XM,Policy Rate,2026-01-15,3.50\n"
        )
        df = self.ingester._parse_csv(csv_no_ref_area)
        assert df is None

    def test_missing_obs_value_column_returns_none(self):
        csv_no_obs_value = (
            "FREQ,REF_AREA,CB_POLICY_RATE,TIME_PERIOD\n"
            "D,XM,Policy Rate,2026-01-15\n"
        )
        df = self.ingester._parse_csv(csv_no_obs_value)
        assert df is None

    def test_missing_time_period_column_returns_none(self):
        csv_no_time_period = (
            "FREQ,REF_AREA,CB_POLICY_RATE,OBS_VALUE\n"
            "D,XM,Policy Rate,3.50\n"
        )
        df = self.ingester._parse_csv(csv_no_time_period)
        assert df is None


class TestParseCsvCbLookupMiss:
    """Coverage tranche (17 Aug 2026) — the `if cb is None: continue` branch
    (line 276). Under normal data flow this is unreachable: the upstream
    filter and this lookup both key off the SAME _REF_AREA_MAP, so any row
    that survives the filter always resolves to a non-None cb. We construct
    the divergent state directly by patching in a dict whose .keys() lists
    the real codes (so the filter still passes them through) but whose
    .get() always misses (so the lookup fails) — matching this branch's own
    defensive intent (a REF_AREA present in the known set at filter-time
    that somehow doesn't resolve at lookup-time)."""

    class _FilterPassesLookupMissesMap(dict):
        def get(self, key, default=None):
            return None   # always miss, regardless of key

    def test_cb_lookup_miss_row_skipped(self, monkeypatch):
        from src.bronze import bis_rates_ingester as mod
        fake_map = self._FilterPassesLookupMissesMap(_REF_AREA_MAP)
        monkeypatch.setattr(mod, "_REF_AREA_MAP", fake_map)
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        csv_text = (
            "FREQ,REF_AREA,CB_POLICY_RATE,TIME_PERIOD,OBS_VALUE\n"
            "D,XM,Policy Rate,2026-01-15,3.50\n"
        )
        df = ingester._parse_csv(csv_text)
        # Row passes the is_in() filter (XM is a real key) but cb resolves
        # to None at lookup time under the patched map -> skipped -> empty
        # rows list -> the "zero valid rows" branch (lines 300-302) fires.
        assert df is None


class TestParseCsvRateValueError:
    """Coverage tranche (17 Aug 2026) — except ValueError: rate_pct = None
    (lines 288-289), distinct from test_missing_value_becomes_null which
    covers the empty-string case handled by the guard clause, not this
    except block."""

    def test_unparseable_rate_value_becomes_null(self):
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        csv_bad_value = (
            "FREQ,REF_AREA,CB_POLICY_RATE,TIME_PERIOD,OBS_VALUE\n"
            "D,XM,Policy Rate,2026-01-15,N/A\n"
        )
        df = ingester._parse_csv(csv_bad_value)
        assert df is not None
        assert df["rate_pct"][0] is None


class TestParseCsvOuterException:
    """Coverage tranche (17 Aug 2026) — the outer except Exception (lines
    316-318) wrapping the whole parse body."""

    def test_read_csv_exception_returns_none(self):
        ingester = BISCBRatesIngester.__new__(BISCBRatesIngester)
        with patch("polars.read_csv", side_effect=RuntimeError("corrupt stream")):
            csv_text = (
                "FREQ,REF_AREA,CB_POLICY_RATE,TIME_PERIOD,OBS_VALUE\n"
                "D,XM,Policy Rate,2026-01-15,3.50\n"
            )
            df = ingester._parse_csv(csv_text)
        assert df is None
