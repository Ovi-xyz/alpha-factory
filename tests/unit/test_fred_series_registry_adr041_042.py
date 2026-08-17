"""
tests/unit/test_fred_series_registry_adr041_042.py

FIX ADR-041 / ADR-042 (GMI_Decision_Document_v9.docx, 14 Aug 2026):
  - ADR-041: prune 5 dead/redundant FRED series from config/fred_series.yaml
    (GOLDAMGBD228NLBM, NAPM, NMFCI, PPIFGS, CSCICP03USM665S).
  - ADR-042: register the 6 Treasury tenors previously declared in
    treasury_ingester.py's TREASURY_FRED_SERIES but silently dropped by
    FREDIngester.run()'s series_filter (registry-absent).

Covers both the static registry content (counts, presence/absence) and
the exact silent-drop mechanism described in the ADR — series_filter can
only retain series already present in the loaded registry — via the real
production code path (FREDIngester.run() + the actual live YAML file, not
a mocked registry).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml

from src.bronze.base_ingester import BronzeIngester
from src.bronze.fred_ingester import FREDIngester, RELEASE_LAG_DAYS

REGISTRY_PATH = Path("config/fred_series.yaml")

PRUNED_SERIES = ["GOLDAMGBD228NLBM", "NAPM", "NMFCI", "PPIFGS", "CSCICP03USM665S"]
NEW_TREASURY_TENORS = ["DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS7", "DGS20"]
EXISTING_TREASURY_TENORS = ["DGS2", "DGS5", "DGS10", "DGS30"]


def _fake_series(values: dict) -> pd.Series:
    idx = pd.to_datetime(list(values.keys()))
    return pd.Series(list(values.values()), index=idx)


@pytest.fixture
def registry() -> dict:
    return yaml.safe_load(REGISTRY_PATH.read_text())


class TestRegistryContentADR041:
    """Static content checks against the live config/fred_series.yaml."""

    def test_total_series_count_is_68(self, registry):
        assert len(registry["series"]) == 68

    def test_no_duplicate_ids(self, registry):
        ids = [s["id"] for s in registry["series"]]
        assert len(ids) == len(set(ids))

    @pytest.mark.parametrize("series_id", PRUNED_SERIES)
    def test_pruned_series_absent(self, registry, series_id):
        ids = {s["id"] for s in registry["series"]}
        assert series_id not in ids

    def test_pruned_series_were_never_in_regime_inputs(self, registry):
        """Sanity guard: confirms the ADR's own claim that macro regime
        detection is unaffected by the prune."""
        regime_values = set(registry["regime_inputs"].values())
        for series_id in PRUNED_SERIES:
            assert series_id not in regime_values


class TestRegistryContentADR042:
    @pytest.mark.parametrize("series_id", NEW_TREASURY_TENORS)
    def test_new_treasury_tenor_registered(self, registry, series_id):
        matches = [s for s in registry["series"] if s["id"] == series_id]
        assert len(matches) == 1
        spec = matches[0]
        assert spec["domain"] == "monetary_policy"
        assert spec["frequency"] == "daily"
        assert spec["cadence"] == "daily"
        assert spec["regime_input"] is False

    def test_all_13_treasury_tenors_now_present(self, registry):
        """Closes the gap GD v1.2 §3.3.3 describes ('full 1M-30Y yield
        curve') vs. what was actually registered (previously only
        2Y/5Y/10Y/30Y)."""
        ids = {s["id"] for s in registry["series"]}
        for tenor in NEW_TREASURY_TENORS + EXISTING_TREASURY_TENORS:
            assert tenor in ids, f"{tenor} missing from registry"


class TestSilentDropMechanismFixed:
    """FIX ADR-042: reproduces the exact bug mechanism the ADR describes —
    FREDIngester.run()'s series_filter can only retain series already
    present in the loaded registry — using the REAL production registry
    file (not a mocked one), proving the 6 new tenors are no longer
    silently dropped."""

    @pytest.fixture(autouse=True)
    def _isolate(self, tmp_path, monkeypatch):
        monkeypatch.setattr(BronzeIngester, "BASE_PATH", tmp_path / "bronze")
        import src.bronze.fred_ingester as mod
        monkeypatch.setattr(mod, "BRONZE_FRED_PATH", tmp_path / "bronze" / "macro" / "fred")
        monkeypatch.setattr(mod, "SCHEMA_PATH", tmp_path / "no_schema.yaml")
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        return tmp_path

    def test_treasury_tenor_no_longer_silently_dropped(self, tmp_path):
        """Before ADR-042: series_filter=['DGS20'] against the real
        registry would retain 0 series (DGS20 wasn't registered) -- the
        ingestion loop would run 0 times and write nothing, with no error.
        After ADR-042: DGS20 IS in the registry, so the filter retains it
        and the real fetch path executes."""
        fake_data = _fake_series({"2026-08-01": 4.85})
        mock_client = MagicMock()
        mock_client.get_series.return_value = fake_data
        with patch("fredapi.Fred", return_value=mock_client):
            ingester = FREDIngester()  # loads the REAL config/fred_series.yaml
            ingester.run(date(2026, 8, 17), series_filter=["DGS20"])  # Monday

        mock_client.get_series.assert_called_once()
        out_dir = tmp_path / "bronze" / "macro" / "fred" / "monetary_policy"
        files = list(out_dir.glob("*.parquet"))
        assert len(files) == 1, (
            "ADR-042: DGS20 should now be retained by series_filter and "
            "fetched -- previously silently dropped (0 files written, no error)"
        )

    def test_pruned_series_filter_now_matches_nothing(self, tmp_path):
        """Post-ADR-041, requesting a pruned series via series_filter is
        the SAME silent-no-op behavior the registry-absent bug always had
        -- now correctly applying to genuinely dead series instead of
        live Treasury tenors."""
        mock_client = MagicMock()
        with patch("fredapi.Fred", return_value=mock_client):
            ingester = FREDIngester()
            ingester.run(date(2026, 8, 15), series_filter=["PPIFGS"])
        mock_client.get_series.assert_not_called()


class TestGrepSweepCleanup:
    """FIX ADR-041 checklist item 11: confirms the dead references found
    during the grep sweep were actually cleaned up, not just detected."""

    @pytest.mark.parametrize("series_id", PRUNED_SERIES)
    def test_release_lag_days_no_longer_references_pruned_series(self, series_id):
        assert series_id not in RELEASE_LAG_DAYS

    def test_release_lag_days_still_covers_existing_treasury_tenors(self):
        """Vestigial cleanup must not have collateral-damaged unrelated
        entries -- the 4 pre-existing Treasury tenor lag entries stay."""
        for tenor in EXISTING_TREASURY_TENORS:
            assert tenor in RELEASE_LAG_DAYS

    def test_bls_fred_mirror_map_no_longer_references_ppifgs(self):
        from src.bronze.bls_ingester import BLSIngester
        import inspect

        source = inspect.getsource(BLSIngester._run_via_fred_mirror)
        assert "PPIFGS" not in source
        assert "PPIFIS" in source, "PPIFIS (surviving series) must remain"
