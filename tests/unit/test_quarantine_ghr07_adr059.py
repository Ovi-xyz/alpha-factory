"""
tests/unit/test_quarantine_ghr07_adr059.py — GHR-07 (ADR-059).

GK2/HR5: the Gatekeeper read "a quarantine file with today's mtime" as Bronze `warn`. The files it found
were one-row stubs holding only `series_id` — written by the test suite (TestSchemaValidatorGate in the
FRED / IMF / EIA ingester tests push `pl.DataFrame({"series_id": [...]})` through a REAL validator into the
LIVE quarantine). Production ingesters never emit such a frame: each guards `len(df) > 0` before
validating, which is pinned below. What remained fragile is inferring "quarantined today" from mtime, which
a copy / restore / touch changes without any quarantine event. Files now record `_quarantined_at` and
`_quarantine_kind`; mtime is only the fallback for files written before that.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

import src.utils.gatekeeper as gk
from src.bronze.schema_validator import QUARANTINE_KIND_SCHEMA_MISMATCH, SchemaValidator

DAY = date(2026, 10, 18)


def _touch_day(path: Path, day: date) -> None:
    ts = datetime.combine(day, datetime.min.time()).replace(hour=12).timestamp()
    os.utime(path, (ts, ts))


@pytest.fixture
def qdir(tmp_path, monkeypatch):
    d = tmp_path / "q"
    d.mkdir()
    monkeypatch.setattr(gk, "QUARANTINE_GLOB", str(d / "*.parquet"))
    return d


def _write(d: Path, name: str, *, quarantined_at=None, kind=None, mtime: date | None = None, extra=None) -> Path:
    cols = {"series_id": ["X"], "value": [1.0]}
    if quarantined_at is not None:
        cols["_quarantined_at"] = [quarantined_at]
    if kind is not None:
        cols["_quarantine_kind"] = [kind]
    p = d / name
    pl.DataFrame(cols).write_parquet(p)
    if mtime is not None:
        _touch_day(p, mtime)
    return p


class TestHandleMismatchMarker:

    def test_file_records_when_and_why(self, tmp_path, monkeypatch):
        monkeypatch.setattr(SchemaValidator, "QUARANTINE_PATH", tmp_path / "q")
        schema = tmp_path / "s.yaml"
        schema.write_text("source: t\nexpected_columns:\n  - {name: a, type: Float64}\n")
        v = SchemaValidator(schema)
        v.handle_mismatch(pl.DataFrame({"b": [1]}), ["Missing column: 'a'"], "SYM", on_mismatch="quarantine")
        out = pl.read_parquet(next((tmp_path / "q").glob("*.parquet")))
        assert out["_quarantine_kind"][0] == QUARANTINE_KIND_SCHEMA_MISMATCH == "schema_mismatch"
        stamp = datetime.fromisoformat(out["_quarantined_at"][0])
        assert abs((datetime.now() - stamp).total_seconds()) < 60
        assert "Missing column" in out["_quarantine_reason"][0]
        assert out["b"].to_list() == [1]                                  # the original payload is preserved


class TestGatekeeperReadsTheMarker:

    def test_a_new_file_counts_by_its_own_timestamp_even_with_an_old_mtime(self, qdir):
        _write(qdir, "a.parquet", quarantined_at=f"{DAY}T04:00:00", kind="schema_mismatch",
               mtime=DAY - timedelta(days=30))
        assert len(gk._quarantine_files(DAY)) == 1

    def test_a_copied_or_restored_file_is_not_quarantined_today(self, qdir):
        """mtime says today, the file itself says it was quarantined weeks ago."""
        _write(qdir, "a.parquet", quarantined_at="2026-09-01T04:00:00", kind="schema_mismatch", mtime=DAY)
        assert gk._quarantine_files(DAY) == []

    def test_only_schema_mismatch_files_count(self, qdir):
        _write(qdir, "a.parquet", quarantined_at=f"{DAY}T04:00:00", kind="manual_hold")
        assert gk._quarantine_files(DAY) == []

    def test_legacy_file_without_the_marker_falls_back_to_mtime(self, qdir):
        _write(qdir, "legacy.parquet", mtime=DAY)
        assert len(gk._quarantine_files(DAY)) == 1
        assert gk._quarantine_files(DAY + timedelta(days=1)) == []

    def test_unreadable_file_falls_back_to_mtime_and_does_not_raise(self, qdir):
        p = qdir / "garbage.parquet"
        p.write_bytes(b"not parquet")
        _touch_day(p, DAY)
        assert len(gk._quarantine_files(DAY)) == 1

    def test_malformed_timestamp_falls_back_to_mtime(self, qdir):
        _write(qdir, "a.parquet", quarantined_at="not-a-date", kind="schema_mismatch", mtime=DAY)
        assert len(gk._quarantine_files(DAY)) == 1

    def test_bronze_raw_status_follows_the_marker(self, qdir, tmp_path, monkeypatch):
        from src.scheduler.dependency_guard import DependencyGuard
        from src.scheduler.job_registry import DAILY_SEQUENCE, LAYER_JOB_NAMES, _passes_schedule, JOB_REGISTRY

        guard = DependencyGuard(sentinel_dir=tmp_path / ".s")
        for j in LAYER_JOB_NAMES["bronze"]:
            if _passes_schedule(JOB_REGISTRY[j], DAY):
                guard.mark_done(j, DAY)
        seq = set(DAILY_SEQUENCE)
        assert gk._bronze_raw_status(guard, DAY, seq) == "pass"
        _write(qdir, "a.parquet", quarantined_at=f"{DAY}T04:00:00", kind="schema_mismatch")
        assert gk._bronze_raw_status(guard, DAY, seq) == "warn"
        for f in qdir.glob("*.parquet"):
            f.unlink()
        _write(qdir, "b.parquet", quarantined_at="2026-01-01T00:00:00", kind="schema_mismatch", mtime=DAY)
        assert gk._bronze_raw_status(guard, DAY, seq) == "pass"           # restored file: no warn


class TestNoNewDataQuarantinesNothing:
    """The guide's hypothesis was that an empty fetch yields a column-less stub frame. It does not: every
    macro ingester guards `len(df) > 0` before the validator. Pinned so it stays true."""

    @pytest.fixture
    def spy(self, tmp_path, monkeypatch):
        monkeypatch.setattr(SchemaValidator, "QUARANTINE_PATH", tmp_path / "quarantine")
        with patch.object(SchemaValidator, "handle_mismatch") as m:
            yield m

    @pytest.mark.parametrize("fetched", [None, pl.DataFrame()])
    def test_fred(self, spy, tmp_path, monkeypatch, fetched):
        import src.bronze.fred_ingester as mod
        from src.bronze.fred_ingester import FREDIngester

        monkeypatch.setattr(mod, "SCHEMA_PATH", Path("config/schemas/fred_macro.yaml"))
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setattr(FREDIngester, "_load_registry",
                            lambda self: {"series": [{"id": "CPIAUCSL", "domain": "inflation"}]})
        with patch.object(FREDIngester, "_fetch_series", return_value=fetched):
            ing = FREDIngester()
            assert ing._validator is not None
            ing.run(date(2026, 6, 1))
        assert spy.call_count == 0 and not list((tmp_path / "quarantine").glob("*.parquet"))

    @pytest.mark.parametrize("fetched", [None, pl.DataFrame()])
    def test_imf(self, spy, tmp_path, monkeypatch, fetched):
        import src.bronze.imf_ingester as mod
        from src.bronze.imf_ingester import IMFIngester

        monkeypatch.setattr(mod, "SCHEMA_PATH", Path("config/schemas/imf_weo.yaml"))
        with patch.object(IMFIngester, "_fetch_indicator", return_value=fetched):
            IMFIngester().run(date(2026, 6, 1))
        assert spy.call_count == 0 and not list((tmp_path / "quarantine").glob("*.parquet"))

    @pytest.mark.parametrize("fetched", [None, pl.DataFrame()])
    def test_eia(self, spy, tmp_path, monkeypatch, fetched):
        import src.bronze.eia_ingester as mod
        from src.bronze.eia_ingester import EIAIngester

        monkeypatch.setattr(mod, "SCHEMA_PATH", Path("config/schemas/eia_oil.yaml"))
        monkeypatch.setenv("EIA_API_KEY", "fake-key")
        with patch.object(EIAIngester, "_fetch_series", return_value=fetched):
            EIAIngester().run(date(2026, 6, 1))
        assert spy.call_count == 0 and not list((tmp_path / "quarantine").glob("*.parquet"))

    def test_a_genuinely_mismatched_frame_is_still_quarantined(self, tmp_path, monkeypatch):
        import src.bronze.fred_ingester as mod
        from src.bronze.fred_ingester import FREDIngester

        monkeypatch.setattr(SchemaValidator, "QUARANTINE_PATH", tmp_path / "quarantine")
        monkeypatch.setattr(mod, "SCHEMA_PATH", Path("config/schemas/fred_macro.yaml"))
        monkeypatch.setenv("FRED_API_KEY", "fake-key")
        monkeypatch.setattr(FREDIngester, "_load_registry",
                            lambda self: {"series": [{"id": "CPIAUCSL", "domain": "inflation"}]})
        with patch.object(FREDIngester, "_fetch_series", return_value=pl.DataFrame({"series_id": ["CPIAUCSL"]})):
            FREDIngester().run(date(2026, 6, 1))
        files = list((tmp_path / "quarantine").glob("*.parquet"))
        assert len(files) == 1
        assert pl.read_parquet(files[0])["_quarantine_kind"][0] == "schema_mismatch"
