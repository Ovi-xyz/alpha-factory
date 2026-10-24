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
