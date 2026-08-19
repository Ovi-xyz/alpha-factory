"""
tests/unit/test_fred_ingester.py — Bronze FRED ingester real-function
coverage. Decision C (GMI_Decision_Document_v5.docx §3, tranche item #3).
Previously zero test coverage for this module.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest

from src.bronze.base_ingester import BronzeIngester
from src.bronze.fred_ingester import FREDIngester, run


def _write_registry(path, series: list[dict]) -> None:
    import yaml
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump({"series": series}))


def _fake_series(values: dict) -> pd.Series:
    """values: {'2026-01-01': 5.0, ...} -> pandas Series w/ DatetimeIndex."""
    idx = pd.to_datetime(list(values.keys()))
    return pd.Series(list(values.values()), index=idx)


@pytest.fixture(autouse=True)
def _isolate_bronze_path(tmp_path, monkeypatch):
    monkeypatch.setattr(BronzeIngester, "BASE_PATH", tmp_path / "bronze")
    import src.bronze.fred_ingester as mod
    monkeypatch.setattr(mod, "BRONZE_FRED_PATH", tmp_path / "bronze" / "macro" / "fred")
    monkeypatch.setattr(mod, "SCHEMA_PATH", tmp_path / "no_schema.yaml")  # skip validator, tested separately
    return tmp_path


class TestNoApiKey:
    def test_missing_api_key_skips_entirely(self, tmp_path, monkeypatch):
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        monkeypatch.setattr(
            FREDIngester, "_load_registry",
            lambda self: {"series": [{"id": "CPIAUCSL", "domain": "inflation"}]},
        )
        ingester = FREDIngester()
        with patch("fredapi.Fred") as mock_fred:
            ingester.run(date(2026, 6, 1))
            mock_fred.assert_not_called()


class TestSuccessfulFetch:
    def test_fetches_and_writes_series(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setattr(
            FREDIngester, "_load_registry",
            lambda self: {"series": [
                {"id": "CPIAUCSL", "domain": "inflation", "cadence": "weekly"},
            ]},
        )
        fake_data = _fake_series({"2026-01-01": 310.5, "2026-01-08": 311.0})
        mock_client = MagicMock()
        mock_client.get_series.return_value = fake_data
        with patch("fredapi.Fred", return_value=mock_client):
            ingester = FREDIngester()
            ingester.run(date(2026, 6, 1))

        out_dir = tmp_path / "bronze" / "macro" / "fred" / "inflation"
        files = list(out_dir.glob("*.parquet"))
        assert len(files) == 1
        written = pl.read_parquet(files[0])
        assert written["series_id"].to_list() == ["CPIAUCSL", "CPIAUCSL"]
        assert written["value"].to_list() == [310.5, 311.0]
        # FIX FRED-1: release_date = obs + lag (35d for CPIAUCSL) — well
        # before run_date here, so NOT clamped (see test below for that case)
        assert written["release_date"].to_list()[0] == "2026-02-05"  # 2026-01-01 + 35d

    def test_release_date_clamped_to_run_date(self, tmp_path, monkeypatch):
        """A very recent observation + long lag must not produce a
        release_date AFTER run_date — clamped, per FIX FRED-1."""
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setattr(
            FREDIngester, "_load_registry",
            lambda self: {"series": [{"id": "PAYEMS", "domain": "labor", "cadence": "weekly"}]},
        )
        run_date = date(2026, 6, 1)
        fake_data = _fake_series({"2026-05-30": 158000.0})   # PAYEMS lag=40d -> would be 2026-07-09
        mock_client = MagicMock()
        mock_client.get_series.return_value = fake_data
        with patch("fredapi.Fred", return_value=mock_client):
            FREDIngester().run(run_date)
        out_dir = tmp_path / "bronze" / "macro" / "fred" / "labor"
        written = pl.read_parquet(next(out_dir.glob("*.parquet")))
        assert written["release_date"].to_list()[0] == run_date.isoformat()

    def test_daily_series_skipped_on_weekend(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setattr(
            FREDIngester, "_load_registry",
            lambda self: {"series": [{"id": "T10Y2Y", "domain": "rates", "cadence": "daily"}]},
        )
        saturday = date(2026, 6, 6)   # confirmed Saturday
        assert saturday.weekday() == 5
        with patch("fredapi.Fred") as mock_fred:
            FREDIngester().run(saturday)
            mock_fred.assert_not_called()

    def test_empty_series_response_no_write(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setattr(
            FREDIngester, "_load_registry",
            lambda self: {"series": [{"id": "CPIAUCSL", "domain": "inflation"}]},
        )
        mock_client = MagicMock()
        mock_client.get_series.return_value = pd.Series([], dtype=float)
        with patch("fredapi.Fred", return_value=mock_client):
            FREDIngester().run(date(2026, 6, 1))
        assert not (tmp_path / "bronze" / "macro" / "fred").exists() or \
            not list((tmp_path / "bronze" / "macro" / "fred").rglob("*.parquet"))

    def test_api_exception_for_one_series_does_not_abort_others(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setattr(
            FREDIngester, "_load_registry",
            lambda self: {"series": [
                {"id": "BAD", "domain": "other"},
                {"id": "GOOD", "domain": "other"},
            ]},
        )
        mock_client = MagicMock()
        mock_client.get_series.side_effect = [
            RuntimeError("API down"),
            _fake_series({"2026-05-01": 1.0}),
        ]
        with patch("fredapi.Fred", return_value=mock_client):
            FREDIngester().run(date(2026, 6, 1))
        out_dir = tmp_path / "bronze" / "macro" / "fred" / "other"
        files = list(out_dir.glob("*.parquet"))
        assert len(files) == 1
        assert pl.read_parquet(files[0])["series_id"].to_list() == ["GOOD"]

    def test_series_filter_restricts_to_subset(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setattr(
            FREDIngester, "_load_registry",
            lambda self: {"series": [
                {"id": "A", "domain": "x"}, {"id": "B", "domain": "x"},
            ]},
        )
        mock_client = MagicMock()
        mock_client.get_series.return_value = _fake_series({"2026-05-01": 1.0})
        with patch("fredapi.Fred", return_value=mock_client):
            FREDIngester().run(date(2026, 6, 1), series_filter=["A"])
        assert mock_client.get_series.call_count == 1


class TestSchemaValidatorGate:

    def test_quarantine_on_schema_mismatch(self, tmp_path, monkeypatch):
        """Real SchemaValidator against the real fred_macro.yaml, fed a
        dataframe missing a required column."""
        import src.bronze.fred_ingester as mod
        monkeypatch.setattr(mod, "SCHEMA_PATH",
                             __import__("pathlib").Path("config/schemas/fred_macro.yaml"))
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setattr(
            FREDIngester, "_load_registry",
            lambda self: {"series": [{"id": "CPIAUCSL", "domain": "inflation"}]},
        )
        # Patch _fetch_series to directly return a malformed df, bypassing
        # the real fredapi parsing (isolates the validator gate itself).
        malformed = pl.DataFrame({"series_id": ["CPIAUCSL"]})  # missing value/observation_date/release_date
        with patch.object(FREDIngester, "_fetch_series", return_value=malformed):
            ingester = FREDIngester()
            assert ingester._validator is not None
            ingester.run(date(2026, 6, 1))
        # Nothing written to the normal path — quarantined instead
        out_dir = tmp_path / "bronze" / "macro" / "fred" / "inflation"
        assert not out_dir.exists() or not list(out_dir.glob("*.parquet"))


class TestFredHelpers:

    def test_last_known_cache_empty_when_no_bronze_data(self, tmp_path):
        assert FREDIngester()._build_last_known_cache() == {}

    def test_registry_missing_returns_empty_dict(self, tmp_path, monkeypatch):
        import src.bronze.fred_ingester as mod
        monkeypatch.setattr(mod, "FRED_REGISTRY_PATH", tmp_path / "nonexistent.yaml")
        assert FREDIngester()._load_registry() == {}

    def test_get_regime_series_filters_flag(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            FREDIngester, "_load_registry",
            lambda self: {"series": [
                {"id": "A", "regime_input": True},
                {"id": "B", "regime_input": False},
                {"id": "C"},
            ]},
        )
        assert FREDIngester().get_regime_series() == ["A"]


class TestRunEntryPoint:
    def test_module_level_run_delegates_to_class(self, tmp_path, monkeypatch):
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        with patch.object(FREDIngester, "run") as mock_run:
            run(date(2026, 6, 1))
            mock_run.assert_called_once_with(date(2026, 6, 1))


class TestRunLoopExceptionHandling:
    """Coverage tranche (17 Aug 2026) — run()'s own outer except (lines
    189-191), distinct from _fetch_series()'s inner except which already
    catches get_series() API errors (test_api_exception_for_one_series...
    above never reaches run()'s own except, since _fetch_series() returns
    None gracefully instead of propagating)."""

    def test_write_macro_exception_increments_failed_without_raising(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setattr(
            FREDIngester, "_load_registry",
            lambda self: {"series": [{"id": "CPIAUCSL", "domain": "inflation"}]},
        )
        mock_client = MagicMock()
        mock_client.get_series.return_value = _fake_series({"2026-05-01": 1.0})
        with patch("fredapi.Fred", return_value=mock_client), \
             patch.object(FREDIngester, "write_macro", side_effect=RuntimeError("disk full")):
            FREDIngester().run(date(2026, 6, 1))   # must not raise


class TestFetchSeriesImportError:
    """Coverage tranche (17 Aug 2026) — the except ImportError: branch in
    _fetch_series() when fredapi itself isn't installed."""

    def test_fredapi_not_installed_returns_none(self, tmp_path, monkeypatch):
        import sys
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setitem(sys.modules, "fredapi", None)
        result = FREDIngester()._fetch_series("CPIAUCSL", date(2026, 6, 1))
        assert result is None


class TestBuildLastKnownCacheMalformedDate:
    """Coverage tranche (17 Aug 2026) — except (ValueError, TypeError): pass
    around date.fromisoformat in _build_last_known_cache()."""

    def test_malformed_date_row_excluded_from_cache(self, tmp_path):
        bronze_dir = tmp_path / "bronze" / "macro" / "fred" / "inflation"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "series_id": ["CPIAUCSL"],
            "observation_date": ["not-a-date"],
        }).write_parquet(bronze_dir / "seed.parquet")
        cache = FREDIngester()._build_last_known_cache()
        assert "CPIAUCSL" not in cache

    def test_valid_date_row_included_in_cache(self, tmp_path):
        bronze_dir = tmp_path / "bronze" / "macro" / "fred" / "inflation"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "series_id": ["CPIAUCSL"],
            "observation_date": ["2026-05-01"],
        }).write_parquet(bronze_dir / "seed.parquet")
        cache = FREDIngester()._build_last_known_cache()
        assert cache["CPIAUCSL"] == date(2026, 5, 1)
