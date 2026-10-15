"""
tests/unit/test_hmm_raw_macro.py — ADR-051 (GMI-HMM2-001): src/gold/hmm/raw_macro.py.

Fixtures write a tiny synthetic registry + Silver FRED snapshot into tmp_path;
build() takes explicit paths so nothing touches data/ or config/. One test
runs the REAL fred_series.yaml against a synthetic snapshot covering all 13
required series to pin the production column contract.
"""
from datetime import date, timedelta

import polars as pl
import pytest
import yaml

from src.gold.hmm import raw_macro as rm
from src.gold.hmm.config import macro_config


def _registry(extra_features=None, extra_series=None, basis="recorded"):
    series = [
        {"id": "AAA", "frequency": "daily"},
        {"id": "BBB", "frequency": "daily"},
        {"id": "CPI", "frequency": "monthly"},
    ] + (extra_series or [])
    return {
        "series": series,
        "hmm": {
            "version": "t", "domains": ["d1", "d2"],
            "pit": {"basis": basis, "frequency_floor_days": {"daily": 1, "weekly": 7, "monthly": 45, "quarterly": 120},
                    "series_lag_days": {}},
            "features": [
                {"feature": "AAA", "series": "AAA", "domain": "d1", "transform": "level"},
                {"feature": "SPREAD", "domain": "d1", "derived": {"op": "subtract", "left": "AAA", "right": "BBB"}},
                {"feature": "CPI_YOY", "series": "CPI", "domain": "d2", "transform": "pct_change_12m"},
            ] + (extra_features or []),
        },
    }


def _write_cfg(tmp_path, reg=None):
    p = tmp_path / "fred_series.yaml"
    p.write_text(yaml.safe_dump(reg or _registry()))
    return p


def _snapshot(tmp_path, snap_date="2026-09-25", series=("AAA", "BBB", "CPI")):
    rows = []
    d = date(2025, 1, 1)
    while d <= date(2026, 9, 24):
        if d.weekday() < 5:
            if "AAA" in series:
                rows.append(("AAA", d, (d + timedelta(days=1)).isoformat(), 5.0 + (d.toordinal() % 7) * 0.01, "t"))
            if "BBB" in series:
                rows.append(("BBB", d, (d + timedelta(days=1)).isoformat(), 2.0, "t"))
        d += timedelta(days=1)
    if "CPI" in series:
        for k in range(21):                                   # Jan-2025 .. Sep-2026
            y, m = 2025 + k // 12, k % 12 + 1
            rows.append(("CPI", date(y, m, 1), (date(y, m, 1) + timedelta(days=35)).isoformat(),
                         300.0 * (1.003 ** k), "t"))
    df = pl.DataFrame(rows, orient="row", schema={
        "series_id": pl.String, "observation_date": pl.Date, "release_date": pl.String,
        "value": pl.Float64, "_ingested_at": pl.String})
    silver = tmp_path / "silver"
    silver.mkdir(exist_ok=True)
    df.write_parquet(silver / f"fred_{snap_date}_silver.parquet")
    return silver


class TestBuild:
    def test_schema_order_and_lineage(self, tmp_path):
        cfg = _write_cfg(tmp_path)
        df = rm.build(date(2026, 9, 25), _snapshot(tmp_path), cfg)
        feats = ["AAA", "SPREAD", "CPI_YOY"]
        assert df.columns == ["as_of_date"] + feats + [f"obs_date__{f}" for f in feats] + \
            [f"available_on__{f}" for f in feats] + [f"avail_basis__{f}" for f in feats] + \
            ["computation_date", "processing_version", "source_snapshot", "pit_basis"]
        assert df.schema["as_of_date"] == pl.Date
        assert all(df.schema[f] == pl.Float64 for f in feats)
        assert all(df.schema[f"obs_date__{f}"] == pl.Date and df.schema[f"available_on__{f}"] == pl.Date
                   for f in feats)
        assert all(df.schema[f"avail_basis__{f}"] == pl.String for f in feats)
        assert df["pit_basis"].unique().to_list() == ["recorded"]
        assert df["computation_date"].unique().to_list() == [date(2026, 9, 25)]
        assert df["processing_version"].unique().to_list() == [rm.PROCESSING_VERSION]
        assert df["source_snapshot"].unique().to_list() == ["fred_2026-09-25_silver.parquet"]

    def test_derived_and_transform_values(self, tmp_path):
        cfg = _write_cfg(tmp_path)
        df = rm.build(date(2026, 9, 25), _snapshot(tmp_path), cfg)
        last = df.filter(pl.col("as_of_date") == df["as_of_date"].max()).to_dicts()[0]
        assert last["SPREAD"] == pytest.approx(last["AAA"] - 2.0)
        assert last["CPI_YOY"] == pytest.approx((1.003 ** 12 - 1) * 100, rel=1e-6)

    def test_no_lookahead_against_run_date(self, tmp_path):
        """Even if the chosen snapshot physically contains observations after
        run_date (here a snapshot dated 03-10 holding rows through 09-24), the
        panel ends at run_date and gates every value on availability."""
        cfg = _write_cfg(tmp_path)
        silver = _snapshot(tmp_path, snap_date="2026-03-10")
        df = rm.build(date(2026, 3, 10), silver, cfg)
        assert df["as_of_date"].max() <= date(2026, 3, 10)
        # AAA obs of 2026-03-10 is available 2026-03-11 -> must not be in the 03-10 row
        last = df.filter(pl.col("as_of_date") == date(2026, 3, 10)).to_dicts()[0]
        assert (last["as_of_date"] - last["obs_date__AAA"]).days >= 1
        assert last["available_on__AAA"] <= last["as_of_date"]

    def test_snapshot_after_run_date_is_not_used(self, tmp_path):
        cfg = _write_cfg(tmp_path)
        silver = _snapshot(tmp_path, snap_date="2026-09-25")
        with pytest.raises(rm.RawMacroError, match="no FRED Silver snapshot"):
            rm.build(date(2026, 9, 1), silver, cfg)

    def test_missing_series_raises_and_names_it(self, tmp_path):
        cfg = _write_cfg(tmp_path)
        silver = _snapshot(tmp_path, series=("AAA", "CPI"))        # BBB absent
        with pytest.raises(rm.RawMacroError, match=r"\['BBB'\]"):
            rm.build(date(2026, 9, 25), silver, cfg)

    def test_short_history_is_not_an_error(self, tmp_path):
        """BAMLH0A0HYM2-style: a series starting late yields leading nulls only."""
        reg = _registry()
        reg["series"].append({"id": "LATE", "frequency": "daily"})
        reg["hmm"]["features"].append({"feature": "LATE", "series": "LATE", "domain": "d2", "transform": "level"})
        cfg = _write_cfg(tmp_path, reg)
        silver = _snapshot(tmp_path)
        snap = silver / "fred_2026-09-25_silver.parquet"
        base = pl.read_parquet(snap)
        late = pl.DataFrame([("LATE", date(2026, 6, 1), "2026-06-02", 1.0, "t")], orient="row", schema=base.schema)
        pl.concat([base, late]).write_parquet(snap)
        df = rm.build(date(2026, 9, 25), silver, cfg)
        assert 0 < df["LATE"].null_count() < df.height
        assert df["LATE"][-1] == 1.0

    @staticmethod
    def _stamp_late_rows(silver):
        """Backfill stamp (2026-08-20) on every row, except AAA observations from 2026-09-01 on,
        which were 'recorded' on 2026-09-23."""
        snap = silver / "fred_2026-09-25_silver.parquet"
        df = pl.read_parquet(snap).with_columns(
            pl.when((pl.col("series_id") == "AAA") & (pl.col("observation_date") >= date(2026, 9, 1)))
            .then(pl.lit("2026-09-23T06:00:00")).otherwise(pl.lit("2026-08-20T06:00:00")).alias("_ingested_at"))
        df.write_parquet(snap)

    def test_recorded_basis_withholds_a_value_until_the_day_the_system_recorded_it(self, tmp_path):
        silver = _snapshot(tmp_path)
        self._stamp_late_rows(silver)
        rec = rm.build(date(2026, 9, 25), silver, _write_cfg(tmp_path, _registry(basis="recorded")))
        est = rm.build(date(2026, 9, 25), silver, _write_cfg(tmp_path, _registry(basis="estimated")))
        r_rec = rec.filter(pl.col("as_of_date") == date(2026, 9, 10)).to_dicts()[0]
        r_est = est.filter(pl.col("as_of_date") == date(2026, 9, 10)).to_dicts()[0]
        assert r_est["obs_date__AAA"] == date(2026, 9, 9) and r_est["avail_basis__AAA"] == "estimated"
        # recorded: the 09-01.. rows only existed in this system from 09-23, so 09-10 still carries 08-31
        assert r_rec["obs_date__AAA"] == date(2026, 8, 31) and r_rec["avail_basis__AAA"] == "estimated"
        day = rec.filter(pl.col("as_of_date") == date(2026, 9, 23)).to_dicts()[0]       # the recording day
        assert day["avail_basis__AAA"] == "recorded" and day["available_on__AAA"] == date(2026, 9, 23)
        assert day["obs_date__AAA"] == date(2026, 9, 23)
        # an observation dated AFTER its stamp is clamped up to its own date (never usable before it exists)
        nxt = rec.filter(pl.col("as_of_date") == date(2026, 9, 24)).to_dicts()[0]
        assert nxt["obs_date__AAA"] == date(2026, 9, 24) and nxt["available_on__AAA"] == date(2026, 9, 24)
        assert rec["pit_basis"].unique().to_list() == ["recorded"] and est["pit_basis"].unique().to_list() == ["estimated"]

    def test_run_date_before_any_availability_raises(self, tmp_path):
        cfg = _write_cfg(tmp_path)
        silver = _snapshot(tmp_path, snap_date="2024-01-01")        # snapshot exists, rows are all later
        with pytest.raises(rm.RawMacroError, match="empty"):
            rm.build(date(2024, 6, 1), silver, cfg)


class TestRun:
    def _patch(self, monkeypatch, tmp_path, silver, cfg):
        monkeypatch.setattr(rm, "RAW_MACRO_PATH", tmp_path / "out" / "hmm" / "raw_macro.parquet")
        monkeypatch.setattr(rm.fred_pit, "SILVER_MACRO_PATH", silver)
        monkeypatch.setattr("src.gold.hmm.config.FRED_SERIES_PATH", cfg)

    def test_writes_store_and_is_idempotent(self, tmp_path, monkeypatch):
        cfg = _write_cfg(tmp_path)
        self._patch(monkeypatch, tmp_path, _snapshot(tmp_path), cfg)
        rm.run(date(2026, 9, 25))
        first = pl.read_parquet(rm.RAW_MACRO_PATH)
        rm.run(date(2026, 9, 25))
        second = pl.read_parquet(rm.RAW_MACRO_PATH)
        assert first.height > 0
        assert first.equals(second)

    def test_atomic_write_leaves_no_tmp_files(self, tmp_path, monkeypatch):
        cfg = _write_cfg(tmp_path)
        self._patch(monkeypatch, tmp_path, _snapshot(tmp_path), cfg)
        rm.run(date(2026, 9, 25))
        assert [p.name for p in rm.RAW_MACRO_PATH.parent.iterdir()] == ["raw_macro.parquet"]

    def test_failure_does_not_clobber_existing_store(self, tmp_path, monkeypatch):
        cfg = _write_cfg(tmp_path)
        silver = _snapshot(tmp_path)
        self._patch(monkeypatch, tmp_path, silver, cfg)
        rm.run(date(2026, 9, 25))
        before = rm.RAW_MACRO_PATH.read_bytes()
        monkeypatch.setattr(rm.fred_pit, "SILVER_MACRO_PATH", tmp_path / "empty_dir")
        with pytest.raises(rm.RawMacroError):
            rm.run(date(2026, 9, 25))
        assert rm.RAW_MACRO_PATH.read_bytes() == before


class TestRealRegistryContract:
    """The production column contract, against a synthetic snapshot that has
    every one of the 13 required series."""

    def test_columns_match_real_config(self, tmp_path):
        cfg = macro_config()
        rows = []
        for sid in cfg.required_series():
            for k in range(0, 800):
                d = date(2024, 1, 1) + timedelta(days=k)
                if d.weekday() < 5 or sid in ("CPIAUCSL", "UNRATE", "A191RL1Q225SBEA"):
                    rows.append((sid, d, (d + timedelta(days=1)).isoformat(), 1.0 + k * 0.001, "t"))
        df = pl.DataFrame(rows, orient="row", schema={
            "series_id": pl.String, "observation_date": pl.Date, "release_date": pl.String,
            "value": pl.Float64, "_ingested_at": pl.String})
        silver = tmp_path / "s"
        silver.mkdir()
        df.write_parquet(silver / "fred_2026-03-31_silver.parquet")
        out = rm.build(date(2026, 3, 31), silver)
        names = cfg.feature_names()
        assert out.columns == ["as_of_date"] + names + [f"obs_date__{n}" for n in names] + \
            [f"available_on__{n}" for n in names] + [f"avail_basis__{n}" for n in names] + \
            ["computation_date", "processing_version", "source_snapshot", "pit_basis"]
        assert len(names) == 15
