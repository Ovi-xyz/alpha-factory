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


class TestBronzeDedupBeforeRevisionJoin:
    """FIX SIL-MACRO-DEDUP-01 (18 Sep 2026) regression guard. Reproduces
    the actual production mechanism: Bronze's incremental-fetch overlap
    (Supplementary Design G1, 7-day lookback re-fetch) legitimately
    writes the same (series_id, observation_date) key into multiple
    Bronze files, and _detect_revisions()'s join against `prev` used to
    compound that duplication exponentially, day over day -- confirmed
    empirically against production data (fred_2026-09-17_silver.parquet:
    105,813,947 rows, 93% of it in one key). See module docstring for
    the full empirical account."""

    def _bronze_dupes_df(self, series_id: str, observation_date: str,
                          values_and_timestamps: list[tuple[float, str]],
                          release_date: str) -> pl.DataFrame:
        """Simulates Bronze's incremental-overlap duplication directly:
        the SAME (series_id, observation_date) key appearing across
        several Bronze files (rows here), each with its own
        _ingested_at, exactly as read_parquet(glob) would concatenate
        them before any dedup existed."""
        n = len(values_and_timestamps)
        return pl.DataFrame({
            "series_id":        [series_id] * n,
            "observation_date": [observation_date] * n,
            "value":            [v for v, _ in values_and_timestamps],
            "release_date":     [release_date] * n,
            "_ingested_at":     [ts for _, ts in values_and_timestamps],
        })

    def test_duplicate_bronze_rows_collapse_to_one_per_key(self, tmp_path, monkeypatch):
        """3 Bronze rows for the same key (simulating 3 overlapping
        incremental fetches) must collapse to exactly 1 Silver row --
        not 3, and critically not 3 x (whatever prev already had)."""
        monkeypatch.chdir(tmp_path)
        bronze_dir = tmp_path / "data" / "bronze" / "macro" / "fred"
        bronze_dir.mkdir(parents=True)
        self._bronze_dupes_df(
            "MORTGAGE30US", "2026-09-03",
            [(6.10, "2026-09-03T21:00:00"), (6.11, "2026-09-04T21:00:00"),
             (6.12, "2026-09-05T21:00:00")],
            release_date="2026-09-03",
        ).write_parquet(bronze_dir / "mortgage_fixture.parquet")

        MacroProcessor().process_fred(date(2026, 9, 6))

        out = pl.read_parquet(
            list((tmp_path / "data" / "silver" / "macro_enriched").glob("fred_*_silver.parquet"))[0]
        )
        assert out.height == 1, f"Expected 1 deduped row, got {out.height}"
        # Latest _ingested_at (2026-09-05) must win, not the first or a merge.
        assert out["value"][0] == 6.12

    def test_join_does_not_compound_across_two_consecutive_runs(self, tmp_path, monkeypatch):
        """The actual production mechanism: run process_fred() twice in a
        row (day N, then day N+1), each time with Bronze overlap
        duplication present, and confirm day N+1's output does NOT
        multiply day N's row count -- reproducing, at small scale, the
        exact daily-compounding pattern that reached 105.8M rows live."""
        monkeypatch.chdir(tmp_path)
        bronze_dir = tmp_path / "data" / "bronze" / "macro" / "fred"
        bronze_dir.mkdir(parents=True)

        # Day 1: 2 overlapping Bronze rows for the same key (simulates
        # the incremental-fetch overlap that seeds the very first
        # duplication) plus a handful of other clean keys.
        self._bronze_dupes_df(
            "MORTGAGE30US", "2026-09-01",
            [(6.00, "2026-09-01T21:00:00"), (6.00, "2026-09-02T21:00:00")],
            release_date="2026-09-01",
        ).write_parquet(bronze_dir / "day1_dupe.parquet")
        pl.DataFrame({
            "series_id": ["DGS10"], "observation_date": ["2026-09-01"],
            "value": [4.1], "release_date": ["2026-09-01"],
            "_ingested_at": ["2026-09-01T21:00:00"],
        }).write_parquet(bronze_dir / "day1_other.parquet")

        MacroProcessor().process_fred(date(2026, 9, 2))
        day1_out = pl.read_parquet(
            sorted((tmp_path / "data" / "silver" / "macro_enriched").glob("fred_*_silver.parquet"))[-1]
        )
        assert day1_out.height == 2  # MORTGAGE30US (deduped to 1) + DGS10

        # Day 2: Bronze overlap re-fetches the SAME MORTGAGE30US date
        # again (exactly what the 7-day lookback does in production).
        self._bronze_dupes_df(
            "MORTGAGE30US", "2026-09-01",
            [(6.00, "2026-09-01T21:00:00"), (6.00, "2026-09-02T21:00:00"),
             (6.00, "2026-09-03T21:00:00")],
            release_date="2026-09-01",
        ).write_parquet(bronze_dir / "day2_dupe.parquet")

        MacroProcessor().process_fred(date(2026, 9, 3))
        day2_out = pl.read_parquet(
            sorted((tmp_path / "data" / "silver" / "macro_enriched").glob("fred_*_silver.parquet"))[-1]
        )
        # Pre-fix, this join would have multiplied day1's (already
        # duplicate-containing) prev by day2's duplicate-containing df.
        # Post-fix: still exactly 1 row for MORTGAGE30US, not 2 or 6.
        mortgage_rows = day2_out.filter(pl.col("series_id") == "MORTGAGE30US")
        assert mortgage_rows.height == 1, (
            f"Compounding regression: expected 1 row, got {mortgage_rows.height} "
            f"-- the join is multiplying duplicate keys across runs again"
        )

    def test_dedup_keeps_latest_ingested_value_not_arbitrary(self, tmp_path, monkeypatch):
        """When Bronze overlap carries genuinely different values for the
        same key across ingestion runs (e.g. a value corrected between
        fetches), the LATEST _ingested_at must win -- not the first row
        DuckDB happens to read, which is what a naive .unique() without
        an explicit sort would do."""
        monkeypatch.chdir(tmp_path)
        bronze_dir = tmp_path / "data" / "bronze" / "macro" / "fred"
        bronze_dir.mkdir(parents=True)
        # Write with the LATEST _ingested_at row FIRST in the file, to
        # make sure the fix isn't accidentally relying on file/row order.
        self._bronze_dupes_df(
            "DFF", "2026-09-01",
            [(5.50, "2026-09-05T09:00:00"), (5.25, "2026-09-01T09:00:00")],
            release_date="2026-09-01",
        ).write_parquet(bronze_dir / "dff_fixture.parquet")

        MacroProcessor().process_fred(date(2026, 9, 6))

        out = pl.read_parquet(
            list((tmp_path / "data" / "silver" / "macro_enriched").glob("fred_*_silver.parquet"))[0]
        )
        assert out.height == 1
        assert out["value"][0] == 5.50  # latest _ingested_at, not first row read


class TestRevisionJoinCircuitBreaker:
    """FIX SIL-MACRO-DEDUP-01: _join_with_guard() is deliberately
    extracted as its own method (see its docstring) so this circuit
    breaker is unit-testable on its own terms, independent of whether
    the dedup calls around it in _detect_revisions() are currently
    correct -- calling _detect_revisions() end-to-end can no longer
    exercise the trip condition at all once both dedups are in place
    (a LEFT JOIN against a key-unique right side is *structurally*
    guaranteed to never produce more rows than its left input), which
    is exactly the point of the dedups -- but the guard exists for the
    case where that structural guarantee is ever broken by a future
    change to code around it, and that case needs its own direct test."""

    def test_duplicate_key_in_prev_trips_the_breaker(self):
        """A prev with a genuine duplicate key (i.e. the exact
        pre-dedup production shape) must trip the breaker -- called
        directly, deliberately bypassing _detect_revisions()'s own
        prev-dedup to isolate the guard itself."""
        df = pl.DataFrame({
            "series_id": ["DFF"], "observation_date": ["2026-09-01"],
            "value": [5.30],
        })
        undeduped_prev = pl.DataFrame({
            "series_id": ["DFF", "DFF"], "observation_date": ["2026-09-01", "2026-09-01"],
            "value_prev": [5.25, 5.25], "revision_seq_prev": [0, 0],
        })

        result = MacroProcessor._join_with_guard(df, undeduped_prev, "fred")

        assert result is None, "Breaker must trip (return None) when joined > input"

    def test_clean_unique_prev_does_not_trip_the_breaker(self):
        """The normal, correct case: a key-unique prev must produce a
        normal joined DataFrame, not a tripped breaker."""
        df = pl.DataFrame({
            "series_id": ["DFF"], "observation_date": ["2026-09-01"],
            "value": [5.30],
        })
        clean_prev = pl.DataFrame({
            "series_id": ["DFF"], "observation_date": ["2026-09-01"],
            "value_prev": [5.25], "revision_seq_prev": [0],
        })

        result = MacroProcessor._join_with_guard(df, clean_prev, "fred")

        assert result is not None
        assert result.height == 1

    def test_end_to_end_pre_dedup_shaped_prev_no_longer_reaches_the_join_duplicated(self, tmp_path, monkeypatch):
        """Full _detect_revisions() path: even when the on-disk prev
        file has the exact pre-fix duplicate shape, _detect_revisions()'s
        own defensive dedup absorbs it before the join runs -- the
        breaker doesn't need to trip here because the dedup already
        did its job, and the genuine revision (5.30 vs 5.25) is still
        correctly detected rather than being masked."""
        monkeypatch.setattr("src.silver.macro_processor.SILVER_MACRO_PATH", tmp_path)
        prior = pl.DataFrame({
            "series_id": ["DFF", "DFF"], "observation_date": ["2026-09-01", "2026-09-01"],
            "value": [5.25, 5.25], "revision_seq": [0, 0],
        })
        tmp_path.mkdir(parents=True, exist_ok=True)
        prior.write_parquet(tmp_path / "fred_2026-09-05_silver.parquet")

        df = pl.DataFrame({
            "series_id": ["DFF"], "observation_date": ["2026-09-01"],
            "value": [5.30],
        })

        result = MacroProcessor()._detect_revisions(df, "fred", date(2026, 9, 6))

        assert result.height == 1  # not multiplied by prev's duplicate rows
        assert result["is_revision"][0] == True  # genuine revision, correctly detected
