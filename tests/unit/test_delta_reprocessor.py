"""tests/unit/test_delta_reprocessor.py — DeltaReprocessor unit tests"""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.utils.delta_reprocessor import DeltaReprocessor
from src.silver.ohlcv_processor import CURRENT_SILVER_VERSION


class TestDeltaReprocessor:

    def test_dry_run_returns_stale_count(self, tmp_path, monkeypatch):
        """dry_run=True returns count without writing."""
        stale = [
            {"symbol": "AAPL", "timeframe": "1D", "current_version": "1.1", "row_count": 100},
            {"symbol": "MSFT", "timeframe": "1D", "current_version": "1.0", "row_count": 80},
        ]
        proc = DeltaReprocessor()
        result = proc.reprocess(date(2025, 5, 1), stale=stale, dry_run=True)
        assert result == 2

    def test_empty_stale_list_returns_zero(self):
        """No stale symbols → reprocess returns 0."""
        proc   = DeltaReprocessor()
        result = proc.reprocess(date(2025, 5, 1), stale=[], dry_run=False)
        assert result == 0

    def test_find_stale_graceful_on_no_data(self, tmp_path, monkeypatch):
        """find_stale_symbols returns [] when no Silver data exists."""
        import src.utils.delta_reprocessor as dr
        monkeypatch.setattr(dr, "SILVER_GLOB", str(tmp_path / "nonexistent/**/*.parquet"))
        proc  = DeltaReprocessor()
        stale = proc.find_stale_symbols()
        assert stale == []

    def test_version_summary_empty_on_no_data(self, tmp_path, monkeypatch):
        """get_version_summary returns {} when no Silver data."""
        import src.utils.delta_reprocessor as dr
        monkeypatch.setattr(dr, "SILVER_GLOB", str(tmp_path / "nonexistent/**/*.parquet"))
        proc    = DeltaReprocessor()
        summary = proc.get_version_summary()
        assert summary == {}

    def test_version_summary_detects_versions(self, tmp_path, monkeypatch):
        """get_version_summary correctly reads processing_version from Silver."""
        import src.utils.delta_reprocessor as dr

        # Write Silver Parquet with mixed versions
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        old_df = pl.DataFrame({"symbol": ["AAPL"], "processing_version": ["1.0"]})
        new_df = pl.DataFrame({"symbol": ["MSFT"], "processing_version": [CURRENT_SILVER_VERSION]})
        old_df.write_parquet(silver_dir / "old_silver.parquet")
        new_df.write_parquet(silver_dir / "new_silver.parquet")

        monkeypatch.setattr(dr, "SILVER_GLOB", str(silver_dir / "**/*.parquet"))

        proc    = DeltaReprocessor()
        summary = proc.get_version_summary()

        # Should detect both versions
        assert len(summary) > 0

    def test_current_version_constant_format(self):
        """CURRENT_SILVER_VERSION must follow semantic version format."""
        import re
        pattern = r"^\d+\.\d+$"
        assert re.match(pattern, CURRENT_SILVER_VERSION), (
            f"CURRENT_SILVER_VERSION '{CURRENT_SILVER_VERSION}' "
            f"doesn't match expected format X.Y"
        )


class TestDeltaReprocessorGlobScope:
    """
    NEW — ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8,
    2026-07-11). SILVER_GLOB's DEFAULT (non-test-overridden) value used to
    be a hardcoded unfiltered 'market_ohlcv/**/*_silver.parquet' string —
    the same defect class already fixed elsewhere. These tests cover the
    DEFAULT path specifically; every other test in this file already
    covers the (unchanged, preserved) test-override path via
    monkeypatch.setattr(dr, "SILVER_GLOB", ...).
    """

    def test_default_silver_glob_is_none_sentinel(self):
        """SILVER_GLOB's module-level default must be the None sentinel
        (meaning 'not overridden — compute dynamically'), never a
        hardcoded unfiltered market_ohlcv/**/ string."""
        import src.utils.delta_reprocessor as dr
        assert dr.SILVER_GLOB is None

    def test_effective_glob_uses_layer1_globs_when_not_overridden(self, tmp_path, monkeypatch):
        """With SILVER_GLOB left at its default (None), _effective_glob()
        must compute a Layer1-scoped glob list via layer1_globs(), not an
        unfiltered market_ohlcv/**/ string."""
        import src.utils.delta_reprocessor as dr
        from src.config.instrument_loader import get_loader

        get_loader()  # pre-warm before chdir (relative instruments.yaml path)
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data" / "silver" / "market_ohlcv" / "us_stocks").mkdir(parents=True)
        (tmp_path / "data" / "silver" / "market_ohlcv" / "idx").mkdir(parents=True)

        proc = DeltaReprocessor()
        glob = proc._effective_glob()

        assert isinstance(glob, list)
        assert len(glob) == 2
        for g in glob:
            assert g.count("**") == 1
            assert g.endswith("*_silver.parquet")
        assert any("us_stocks" in g for g in glob)
        assert any("idx" in g for g in glob)

    def test_find_stale_symbols_excludes_layer2_context_data(self, tmp_path, monkeypatch):
        """find_stale_symbols() must not report a Layer 2 context
        instrument (e.g. VIX, living under market_ohlcv/context/) as
        stale — proves the actual correctness property, not just that the
        glob shape looks right."""
        from src.config.instrument_loader import get_loader

        get_loader()
        monkeypatch.chdir(tmp_path)

        l1_dir = tmp_path / "data" / "silver" / "market_ohlcv" / "us_stocks"
        l1_dir.mkdir(parents=True)
        pl.DataFrame({
            "symbol": ["AAPL"], "timeframe": ["1D"], "processing_version": ["0.9-old"],
        }).write_parquet(l1_dir / "AAPL_1D_silver.parquet")

        l2_dir = tmp_path / "data" / "silver" / "market_ohlcv" / "context"
        l2_dir.mkdir(parents=True)
        pl.DataFrame({
            "symbol": ["VIX"], "timeframe": ["1D"], "processing_version": ["0.9-old"],
        }).write_parquet(l2_dir / "VIX_1D_silver.parquet")

        proc = DeltaReprocessor()
        stale = proc.find_stale_symbols()

        stale_symbols = {s["symbol"] for s in stale}
        assert "AAPL" in stale_symbols
        assert "VIX" not in stale_symbols, (
            "VIX (Layer 2) must not appear in stale-symbol report — "
            "exactly the RISK-6 defect class this fix guards against"
        )
