"""
tests/unit/test_macro_processor.py

FIX GAP-8 [P2] (Production Readiness Assessment v1.7.2, Supplementary
Design §10.3 — Minimum Coverage Target 80%): test_macro_processor.py did
not exist. v1.7.2 shipped two fixes to this module with zero test
coverage:
    F-MP-01 — process_bls() / process_bea() added to run() (previously
              BLS/BEA Bronze data was a dead end, never promoted to Silver)
    F-MP-02 — REVISION_TOLERANCE added to _detect_revisions() (previously
              direct float != comparison caused false-positive revisions
              from Parquet round-trip precision loss)

The 5 test cases below are exactly the cases specified in the assessment's
GAP-8 fix specification.
"""

from datetime import date

import polars as pl
import pytest

from src.silver.macro_processor import MacroProcessor, REVISION_TOLERANCE


def _bronze_macro_df(series_id: str, observation_date: str, value: float,
                      release_date: str) -> pl.DataFrame:
    """Minimal Bronze macro row shape — enough for _process_domain()'s
    SELECT * + PIT filter (release_date) + revision join (series_id,
    observation_date, value) to all succeed."""
    return pl.DataFrame({
        "series_id":        [series_id],
        "observation_date": [observation_date],
        "value":            [value],
        "release_date":     [release_date],
    })


class TestProcessEIACreatesSilverOutput:
    """FIX EIA-6 [chat thread, 8/9 Sep 2026]: process_eia()'s domain_glob
    was a hardcoded literal pointing at data/bronze/commodity/eia/ — a
    directory that has never existed (confirmed empirically against the
    live repo). EIAIngester actually writes to data/bronze/macro/eia/
    crude_oil/ via write_macro(source="eia", domain="crude_oil", ...),
    the same convention process_fred()/process_bls()/process_bea() above
    already glob at the source level. This test uses the REAL path EIA
    data lives at, not the old broken one — the regression this guards
    against is exactly "glob points somewhere no ingester ever writes
    to", which a test asserting against the wrong path would not catch."""

    def test_process_eia_creates_silver_output(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        bronze_dir = tmp_path / "data" / "bronze" / "macro" / "eia" / "crude_oil"
        bronze_dir.mkdir(parents=True)
        _bronze_macro_df(
            "PET.RWTC.W", "2026-08-26", 63.5, "2026-08-27"
        ).write_parquet(bronze_dir / "wti_spot_price_fixture.parquet")

        run_date = date(2026, 9, 1)   # well after release_date -> passes PIT filter
        MacroProcessor().process_eia(run_date)

        silver_dir = tmp_path / "data" / "silver" / "macro_enriched"
        matches = list(silver_dir.glob("eia_*_silver.parquet"))
        assert len(matches) == 1, f"Expected exactly one eia_*_silver.parquet, got {matches}"

        out = pl.read_parquet(matches[0])
        assert out.height == 1
        assert out["series_id"][0] == "PET.RWTC.W"

    def test_process_eia_does_not_look_in_old_broken_path(self, tmp_path, monkeypatch):
        """Data sitting ONLY at the old, never-real 'commodity/eia' path
        must NOT be found — if it were, that would mean the glob had been
        widened rather than corrected, silently reintroducing a source of
        confusion about where EIA data actually lives."""
        monkeypatch.chdir(tmp_path)

        old_wrong_dir = tmp_path / "data" / "bronze" / "commodity" / "eia"
        old_wrong_dir.mkdir(parents=True)
        _bronze_macro_df(
            "PET.RWTC.W", "2026-08-26", 63.5, "2026-08-27"
        ).write_parquet(old_wrong_dir / "wti_spot_price_fixture.parquet")

        MacroProcessor().process_eia(date(2026, 9, 1))

        silver_dir = tmp_path / "data" / "silver" / "macro_enriched"
        assert list(silver_dir.glob("eia_*_silver.parquet")) == []


class TestProcessBLSCreatesSilverOutput:
    """Test Case 1 (GAP-8 spec): patch Bronze BLS fixture -> run process_bls()
    -> assert Silver Parquet exists at SILVER_MACRO_PATH/bls_*."""

    def test_process_bls_creates_silver_output(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        bronze_dir = tmp_path / "data" / "bronze" / "macro" / "bls"
        bronze_dir.mkdir(parents=True)
        _bronze_macro_df(
            "CUUR0000SA0", "2025-01-01", 310.5, "2025-02-05"
        ).write_parquet(bronze_dir / "bls_fixture.parquet")

        run_date = date(2025, 6, 1)   # well after release_date -> passes PIT filter
        MacroProcessor().process_bls(run_date)

        silver_dir = tmp_path / "data" / "silver" / "macro_enriched"
        matches = list(silver_dir.glob("bls_*_silver.parquet"))
        assert len(matches) == 1, f"Expected exactly one bls_*_silver.parquet, got {matches}"

        out = pl.read_parquet(matches[0])
        assert out.height == 1
        assert out["series_id"][0] == "CUUR0000SA0"
        assert "vintage_date" in out.columns
        assert "is_revision" in out.columns


class TestProcessBEACreatesSilverOutput:
    """Test Case 2 (GAP-8 spec): identical for BEA domain."""

    def test_process_bea_creates_silver_output(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        bronze_dir = tmp_path / "data" / "bronze" / "macro" / "bea"
        bronze_dir.mkdir(parents=True)
        _bronze_macro_df(
            "real_gdp", "2025-01-01", 2.8, "2025-05-01"
        ).write_parquet(bronze_dir / "bea_fixture.parquet")

        run_date = date(2025, 6, 1)
        MacroProcessor().process_bea(run_date)

        silver_dir = tmp_path / "data" / "silver" / "macro_enriched"
        matches = list(silver_dir.glob("bea_*_silver.parquet"))
        assert len(matches) == 1, f"Expected exactly one bea_*_silver.parquet, got {matches}"

        out = pl.read_parquet(matches[0])
        assert out.height == 1
        assert out["series_id"][0] == "real_gdp"


class TestRunCallsBLSAndBEA:
    """Test Case 3 (GAP-8 spec): mock process_bls/process_bea, call run()
    -> assert both called exactly once (F-MP-01 regression guard)."""

    def test_run_calls_bls_and_bea(self, monkeypatch):
        import unittest.mock as mock
        import src.silver.macro_processor as mp_mod

        called = {"fred": 0, "bls": 0, "bea": 0, "treasury": 0, "eia": 0}

        def make_tracker(name):
            def _tracked(self, run_date):
                called[name] += 1
            return _tracked

        monkeypatch.setattr(mp_mod.MacroProcessor, "process_fred", make_tracker("fred"))
        monkeypatch.setattr(mp_mod.MacroProcessor, "process_bls", make_tracker("bls"))
        monkeypatch.setattr(mp_mod.MacroProcessor, "process_bea", make_tracker("bea"))
        monkeypatch.setattr(mp_mod.MacroProcessor, "process_treasury", make_tracker("treasury"))
        monkeypatch.setattr(mp_mod.MacroProcessor, "process_eia", make_tracker("eia"))

        mp_mod.run(date(2025, 6, 1))

        assert called["bls"] == 1, "F-MP-01 regression: process_bls() not called by run()"
        assert called["bea"] == 1, "F-MP-01 regression: process_bea() not called by run()"
        assert called["fred"] == 1
        assert called["treasury"] == 1
        assert called["eia"] == 1


class TestRevisionTolerance:
    """Test Cases 4 & 5 (GAP-8 spec): F-MP-02 REVISION_TOLERANCE behavior."""

    def _setup_prior_vintage(self, tmp_path, monkeypatch, series_id, obs_date, prior_value):
        import src.silver.macro_processor as mp_mod
        monkeypatch.setattr(mp_mod, "SILVER_MACRO_PATH", tmp_path)

        prior = pl.DataFrame({
            "series_id":        [series_id],
            "observation_date": [obs_date],
            "value":            [prior_value],
            "revision_seq":     [0],
        })
        tmp_path.mkdir(parents=True, exist_ok=True)
        prior.write_parquet(tmp_path / "fred_2025-05-01_silver.parquet")
        return mp_mod.MacroProcessor()

    def test_revision_tolerance_no_false_positive(self, tmp_path, monkeypatch):
        """value1=0.0025, value2=0.00250000000001 -> is_revision must be False
        (float round-trip noise, not a genuine revision)."""
        proc = self._setup_prior_vintage(
            tmp_path, monkeypatch, "T10Y2Y", "2025-04-01", 0.0025
        )
        new_df = pl.DataFrame({
            "series_id":        ["T10Y2Y"],
            "observation_date": ["2025-04-01"],
            "value":            [0.00250000000001],
        })

        result = proc._detect_revisions(new_df, "fred", date(2025, 6, 1))

        assert result["is_revision"][0] is False
        assert abs(0.00250000000001 - 0.0025) <= REVISION_TOLERANCE, (
            "Test fixture sanity check: difference must be within tolerance"
        )

    def test_revision_tolerance_detects_genuine(self, tmp_path, monkeypatch):
        """value1=0.0025, value2=0.003 -> is_revision must be True
        (genuine BLS/BEA-style revision, well outside tolerance)."""
        proc = self._setup_prior_vintage(
            tmp_path, monkeypatch, "T10Y2Y", "2025-04-01", 0.0025
        )
        new_df = pl.DataFrame({
            "series_id":        ["T10Y2Y"],
            "observation_date": ["2025-04-01"],
            "value":            [0.003],
        })

        result = proc._detect_revisions(new_df, "fred", date(2025, 6, 1))

        assert result["is_revision"][0] is True
        assert result["revision_seq"][0] == 1
        assert abs(0.003 - 0.0025) > REVISION_TOLERANCE, (
            "Test fixture sanity check: difference must exceed tolerance"
        )
