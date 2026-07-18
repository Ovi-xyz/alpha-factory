"""
tests/unit/test_correlation_matrix_glob_scope.py

NEW — ADR-022 (GMI_Decision_Document_v2.docx CI Gate G-8, 2026-07-11).

correlation_matrix.py had ZERO dedicated unit test coverage before this
file (confirmed: no tests/unit/test_correlation_matrix.py exists anywhere
in the repo's history; only incidental coverage via
test_gold_audit_integration.py and test_fstring_sql_absence.py). This file
does not attempt comprehensive module coverage — that is a separate,
larger undertaking outside this fix's scope — it is narrowly focused on
locking in the Gate G-8 fix: run()'s SILVER_1D_PATH (an unfiltered
"market_ohlcv/**/*_1D_silver.parquet" glob string, the same RISK-6 defect
class fixed in quality_validator.py/technical_signals.py) was replaced
with a Layer 1-scoped glob LIST via src.utils.silver_scope.layer1_globs().
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from src.gold.correlation_matrix import run, SILVER_OHLCV_ROOT


class TestCorrelationMatrixGlobScope:

    def test_module_no_longer_defines_unfiltered_silver_1d_path(self):
        """SILVER_1D_PATH (unfiltered market_ohlcv/**/ glob string constant)
        must not exist — replaced by SILVER_OHLCV_ROOT + layer1_globs()."""
        import src.gold.correlation_matrix as cm_mod
        assert not hasattr(cm_mod, "SILVER_1D_PATH")
        assert SILVER_OHLCV_ROOT == __import__("pathlib").Path(
            "data/silver/market_ohlcv"
        )

    def test_run_passes_layer1_scoped_glob_list_not_unfiltered_string(self, tmp_path, monkeypatch):
        """run() must call compute_correlation_matrix with a LIST of
        per-market globs (silver_scope.layer1_globs() output), never the
        old single unfiltered 'market_ohlcv/**/*_1D_silver.parquet' string.
        """
        import src.gold.correlation_matrix as cm_mod

        monkeypatch.setattr(cm_mod, "SILVER_OHLCV_ROOT", tmp_path)
        (tmp_path / "us_stocks").mkdir()
        (tmp_path / "idx").mkdir()

        with patch.object(cm_mod, "_load_active_symbols", return_value=["AAPL", "BBCA"]), \
             patch.object(cm_mod, "compute_correlation_matrix", return_value=None) as mock_compute:
            cm_mod.run(date(2026, 7, 12))

        mock_compute.assert_called_once()
        passed_path = mock_compute.call_args.kwargs["silver_1d_path"]
        assert isinstance(passed_path, list), (
            "silver_1d_path must be a list of Layer1-scoped globs, not a "
            "single unfiltered string"
        )
        assert len(passed_path) == 2
        for p in passed_path:
            assert p.count("**") == 1, "each glob must have exactly one '**'"
            assert p.endswith("*_1D_silver.parquet")
        assert any("us_stocks" in p for p in passed_path)
        assert any("idx" in p for p in passed_path)

    def test_run_skips_gracefully_when_no_layer1_silver_data(self, tmp_path, monkeypatch):
        """layer1_globs() returning [] (no market subdirectory exists yet)
        must short-circuit run() gracefully, not crash or pass an empty
        list into DuckDB read_parquet (which raises on an empty list)."""
        import src.gold.correlation_matrix as cm_mod

        monkeypatch.setattr(cm_mod, "SILVER_OHLCV_ROOT", tmp_path / "does_not_exist")

        with patch.object(cm_mod, "_load_active_symbols", return_value=["AAPL"]), \
             patch.object(cm_mod, "compute_correlation_matrix") as mock_compute:
            cm_mod.run(date(2026, 7, 12))  # must not raise

        mock_compute.assert_not_called()

    def test_compute_correlation_matrix_accepts_list_of_paths(self):
        """compute_correlation_matrix's silver_1d_path parameter must accept
        a list (not only a single string) — DuckDB's read_parquet() natively
        supports a list of glob patterns as its first argument."""
        from src.gold.correlation_matrix import compute_correlation_matrix

        # Empty active_symbols short-circuits before any path is touched —
        # this just confirms the call signature accepts a list without a
        # TypeError, independent of any real Parquet data.
        result = compute_correlation_matrix(
            silver_1d_path=["data/silver/market_ohlcv/us_stocks/**/*_1D_silver.parquet"],
            active_symbols=[],
        )
        assert result is None
