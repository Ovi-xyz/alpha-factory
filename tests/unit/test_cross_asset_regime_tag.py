"""tests/unit/test_cross_asset_regime_tag.py — the CrossAssetEngine's own regime tag (ADR-055 v2, R3).

The three weekly modules tag each output row with the engine's OWN global_regime_label instead of
the retired rule-based regime_store. One shared reader (regime_tag.current_regime_tag).
"""

import re
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import src.gold.cross_asset.regime_tag as regime_tag
from src.gold.cross_asset.regime_tag import UNKNOWN, current_regime_tag

SRC = Path(__file__).resolve().parents[2] / "src"


def _write(path: Path, rows: list[tuple[str, str | None]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"date": [r[0] for r in rows], "global_regime_label": [r[1] for r in rows]},
                 schema={"date": pl.Utf8, "global_regime_label": pl.Utf8}).write_parquet(path)
    return path


class TestCurrentRegimeTag:

    def test_latest_row_on_or_before_run_date(self, tmp_path):
        p = _write(tmp_path / "g.parquet", [("2026-10-14", "MIXED"), ("2026-10-16", "RISK_ON"),
                                           ("2026-10-19", "RISK_OFF")])
        assert current_regime_tag(date(2026, 10, 18), p) == "RISK_ON"      # Sunday -> Friday row
        assert current_regime_tag(date(2026, 10, 16), p) == "RISK_ON"      # same day counts
        assert current_regime_tag(date(2026, 10, 19), p) == "RISK_OFF"

    def test_never_reads_a_future_row(self, tmp_path):
        p = _write(tmp_path / "g.parquet", [("2026-10-19", "RISK_OFF")])
        assert current_regime_tag(date(2026, 10, 18), p) == UNKNOWN

    def test_unknown_when_store_missing(self, tmp_path):
        assert current_regime_tag(date(2026, 10, 18), tmp_path / "absent.parquet") == UNKNOWN

    def test_unknown_when_latest_label_is_null(self, tmp_path):
        p = _write(tmp_path / "g.parquet", [("2026-10-16", None)])
        assert current_regime_tag(date(2026, 10, 18), p) == UNKNOWN

    def test_unknown_when_file_corrupt(self, tmp_path):
        p = tmp_path / "g.parquet"
        p.write_text("not a parquet file")
        assert current_regime_tag(date(2026, 10, 18), p) == UNKNOWN

    def test_default_path_is_looked_up_at_call_time(self, tmp_path, monkeypatch):
        p = _write(tmp_path / "g.parquet", [("2026-10-16", "MIXED")])
        monkeypatch.setattr(regime_tag, "GLOBAL_REGIME_PATH", p)
        assert current_regime_tag(date(2026, 10, 18)) == "MIXED"

    def test_accepts_date_typed_store(self, tmp_path):
        p = tmp_path / "g.parquet"
        pl.DataFrame({"date": [date(2026, 10, 16)], "global_regime_label": ["RISK_ON"]}).write_parquet(p)
        assert current_regime_tag(date(2026, 10, 18), p) == "RISK_ON"


class TestModulesShareTheOneImplementation:

    @pytest.mark.parametrize("mod", ["correlation_module", "lead_lag_module", "forecast_module"])
    def test_module_delegates_and_carries_no_regime_store(self, mod):
        text = (SRC / "gold" / "cross_asset" / f"{mod}.py").read_text(encoding="utf-8")
        assert "REGIME_STORE_PATH" not in text
        assert "from src.gold.cross_asset.regime_tag import current_regime_tag" in text
        assert "return current_regime_tag(run_date)" in text

    def test_cross_asset_engine_does_not_read_the_hmm_engine(self):
        """Tagging with the engine's own label is the whole point (domain independence)."""
        for f in (SRC / "gold" / "cross_asset").glob("*.py"):
            text = f.read_text(encoding="utf-8")
            assert "data/gold/hmm" not in text, f.name          # no path into the HMM stores
            assert "src.gold.hmm" not in text, f.name           # no import from the HMM package


class TestNoConsumerFiltersOnLegacyTags:
    """ADR-055 v1 R3 precondition: before the tag vocabulary changed, check that no consumer
    partitions/filters on the old 5-state tags (STAGFLATION / REFLATION / DISINFLATION)."""

    LEGACY_ONLY = ("STAGFLATION", "REFLATION", "DISINFLATION")

    def test_no_src_consumer_filters_on_legacy_regime_tags(self):
        offenders = []
        for f in SRC.rglob("*.py"):
            text = f.read_text(encoding="utf-8")
            for tag in self.LEGACY_ONLY:
                if re.search(rf"""["']{tag}["']""", text):
                    offenders.append(f"{f.relative_to(SRC)}:{tag}")
        assert offenders == [], f"legacy 5-state tags still referenced in src: {offenders}"


# ═══════════════════════════════════════════════════════════════════════════
# XAE-R01 — staleness guard (cross_asset_recalibration_guide G7)
# ═══════════════════════════════════════════════════════════════════════════

def _write_bar(path: Path, rows: list[tuple[str, str, str | None]]) -> Path:
    """rows: (date, as_of_bar_date, label)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": [r[0] for r in rows],
        "as_of_bar_date": [date.fromisoformat(r[1]) if r[1] else None for r in rows],
        "global_regime_label": [r[2] for r in rows],
    }, schema={"date": pl.Utf8, "as_of_bar_date": pl.Date, "global_regime_label": pl.Utf8}).write_parquet(path)
    return path


class TestStalenessGuard:

    def test_a_label_older_than_the_limit_is_unknown(self, tmp_path):
        p = _write(tmp_path / "g.parquet", [("2026-10-18", "RISK_OFF")])
        # the old reader returned RISK_OFF for any later run date
        assert current_regime_tag(date(2026, 11, 1), p) == UNKNOWN

    def test_limit_boundary_is_inclusive(self, tmp_path):
        p = _write(tmp_path / "g.parquet", [("2026-10-16", "MIXED")])
        assert current_regime_tag(date(2026, 10, 21), p) == "MIXED"      # exactly 5 days
        assert current_regime_tag(date(2026, 10, 22), p) == UNKNOWN      # 6 days

    def test_staleness_is_measured_from_the_bar_date_when_present(self, tmp_path):
        """First-seen run date 21 Oct but the bar is 16 Oct: the label is 6 days old, not 1."""
        p = _write_bar(tmp_path / "g.parquet", [("2026-10-21", "2026-10-16", "RISK_OFF")])
        assert current_regime_tag(date(2026, 10, 22), p) == UNKNOWN

    def test_weekend_run_after_a_friday_bar_is_fresh(self, tmp_path):
        p = _write_bar(tmp_path / "g.parquet", [("2026-10-17", "2026-10-16", "RISK_OFF")])
        assert current_regime_tag(date(2026, 10, 18), p) == "RISK_OFF"

    def test_guard_can_be_disabled_or_tightened(self, tmp_path):
        p = _write(tmp_path / "g.parquet", [("2026-10-01", "MIXED")])
        assert current_regime_tag(date(2026, 11, 1), p, max_age_days=-1) == "MIXED"
        p2 = _write(tmp_path / "g2.parquet", [("2026-10-16", "MIXED")])
        assert current_regime_tag(date(2026, 10, 18), p2, max_age_days=1) == UNKNOWN

    def test_staleness_is_logged_at_warning(self, tmp_path):
        from loguru import logger
        msgs = []
        sink = logger.add(lambda m: msgs.append(m.record["message"]), level="WARNING")
        try:
            p = _write(tmp_path / "g.parquet", [("2026-10-01", "MIXED")])
            assert current_regime_tag(date(2026, 11, 1), p) == UNKNOWN
        finally:
            logger.remove(sink)
        assert any("stale" in m for m in msgs)

    def test_same_date_rows_resolve_to_the_newest_bar(self, tmp_path):
        p = _write_bar(tmp_path / "g.parquet", [("2026-10-20", "2026-10-16", "MIXED"),
                                               ("2026-10-20", "2026-10-19", "RISK_OFF")])
        assert current_regime_tag(date(2026, 10, 20), p) == "RISK_OFF"

    def test_legacy_store_without_the_column_still_reads(self, tmp_path):
        p = _write(tmp_path / "g.parquet", [("2026-10-16", "RISK_ON")])
        assert current_regime_tag(date(2026, 10, 18), p) == "RISK_ON"


# ═══════════════════════════════════════════════════════════════════════════
# XAE-R02 — weekly outputs carry the engine's own tag (never a legacy one)
# ═══════════════════════════════════════════════════════════════════════════

class TestWeeklyTagParity:
    """guide G8: the weekly files on disk carried the pre-ADR-055 tag RISK_ON while the engine's own label
    said RISK_OFF. Every weekly module must stamp exactly current_regime_tag(computation_date)."""

    @pytest.mark.parametrize("modpath, cls", [
        ("src.gold.cross_asset.correlation_module", "CorrelationModule"),
        ("src.gold.cross_asset.lead_lag_module", "LeadLagModule"),
        ("src.gold.cross_asset.forecast_module", "ForecastModule"),
    ])
    def test_module_tag_equals_shared_reader(self, tmp_path, monkeypatch, modpath, cls):
        import importlib
        p = _write_bar(tmp_path / "g.parquet", [("2026-10-17", "2026-10-16", "RISK_OFF")])
        monkeypatch.setattr(regime_tag, "GLOBAL_REGIME_PATH", p)
        module_cls = getattr(importlib.import_module(modpath), cls)
        for d in (date(2026, 10, 18), date(2026, 11, 1)):          # fresh, then stale
            assert module_cls._get_current_regime(d) == current_regime_tag(d)
        assert module_cls._get_current_regime(date(2026, 10, 18)) == "RISK_OFF"
        assert module_cls._get_current_regime(date(2026, 11, 1)) == UNKNOWN

    def test_no_weekly_module_reads_a_regime_store_of_its_own(self):
        """One reader (regime_tag.py): a weekly module that opened global_regime.parquet itself would be a
        second copy of the staleness rule."""
        for name in ("correlation_module.py", "lead_lag_module.py", "forecast_module.py"):
            text = (SRC / "gold" / "cross_asset" / name).read_text()
            assert "global_regime.parquet" not in text.split('"""', 2)[-1], name
            assert "current_regime_tag" in text, name
