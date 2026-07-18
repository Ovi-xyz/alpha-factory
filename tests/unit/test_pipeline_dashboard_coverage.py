"""
tests/unit/test_pipeline_dashboard_coverage.py

NEW — ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8,
2026-07-11). pipeline_dashboard.py had ZERO test coverage anywhere in the
repo before this file. Narrowly focused on _section_layer_coverage()'s
glob-scope fix: "Silver OHLCV" (a single unfiltered
market_ohlcv/**/*.parquet glob) was split into "Silver OHLCV (Layer 1)"
and "Silver OHLCV (Layer 2 context)" via silver_scope's layer1_globs()/
context_glob() helpers, matching the fix pattern applied to
quality_validator.py, technical_signals.py, screener.py,
correlation_matrix.py, pit_data.py, views.py, and delta_reprocessor.py in
the same pass.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from src.utils.pipeline_dashboard import _section_layer_coverage


class TestLayerCoverageGlobScope:

    def test_runs_without_error_on_empty_filesystem(self, tmp_path, monkeypatch):
        """No data anywhere — must print 'no data yet' rows, never raise."""
        from src.config.instrument_loader import get_loader
        get_loader()  # pre-warm before chdir (relative instruments.yaml path)
        monkeypatch.chdir(tmp_path)
        _section_layer_coverage()  # must not raise

    def test_layer1_and_layer2_counted_separately(self, tmp_path, monkeypatch, capsys):
        """Layer 1 (us_stocks) and Layer 2 (context) Silver OHLCV files must
        be reported as separate rows, each with the correct file count —
        proving the split, not just that it runs."""
        from src.config.instrument_loader import get_loader
        get_loader()
        monkeypatch.chdir(tmp_path)

        l1_dir = tmp_path / "data" / "silver" / "market_ohlcv" / "us_stocks" / "symbol=AAPL"
        l1_dir.mkdir(parents=True)
        pl.DataFrame({"symbol": ["AAPL"], "timestamp": [date(2026, 7, 1)]}).write_parquet(
            l1_dir / "AAPL_1D_silver.parquet"
        )
        pl.DataFrame({"symbol": ["AAPL"], "timestamp": [date(2026, 7, 1)]}).write_parquet(
            l1_dir / "AAPL_1H_silver.parquet"
        )

        l2_dir = tmp_path / "data" / "silver" / "market_ohlcv" / "context" / "symbol=VIX"
        l2_dir.mkdir(parents=True)
        pl.DataFrame({"symbol": ["VIX"], "timestamp": [date(2026, 7, 1)]}).write_parquet(
            l2_dir / "VIX_1D_silver.parquet"
        )

        _section_layer_coverage()
        output = capsys.readouterr().out

        assert "Silver OHLCV (Layer 1)" in output
        assert "Silver OHLCV (Layer 2 context)" in output
        # Layer 1 row must show 2 files (AAPL 1D + 1H), not 3 (which would
        # mean VIX leaked into the Layer 1 count)
        for line in output.splitlines():
            if "Silver OHLCV (Layer 1)" in line:
                assert "2 files" in line, f"Expected 2 Layer 1 files, got: {line}"
            if "Silver OHLCV (Layer 2 context)" in line:
                assert "1 files" in line, f"Expected 1 Layer 2 file, got: {line}"
