"""
tests/unit/test_check_glob_scope.py

NEW — ADR-022 (GMI_Decision_Document_v2.docx CI Gate G-8, 2026-07-11).
Tests for scripts/check_glob_scope.py itself: the AST-based scanner that
detects (a) double-'**' glob literals and (b) unscoped market_ohlcv globs
not routed through silver_scope.py's helpers.
"""

from __future__ import annotations

import ast
import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import check_glob_scope as gs  # noqa: E402


def _scan_source(tmp_path: Path, source: str) -> list[str]:
    """Write `source` as a fake src/ file and scan just that tree."""
    fake_src = tmp_path / "src"
    fake_src.mkdir()
    (fake_src / "fake_module.py").write_text(textwrap.dedent(source))

    import os
    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        return gs.scan()
    finally:
        os.chdir(old_cwd)


class TestCheckGlobScopeScanner:

    def test_clean_file_has_no_violations(self, tmp_path):
        source = """
            import polars as pl
            SOME_PATH = "data/silver/macro_enriched/**/*.parquet"
            df = pl.read_parquet(SOME_PATH)
        """
        assert _scan_source(tmp_path, source) == []

    def test_double_globstar_literal_detected(self, tmp_path):
        source = """
            BAD_PATH = "data/silver/market_ohlcv/**/symbol=*/**/*.parquet"
        """
        violations = _scan_source(tmp_path, source)
        assert len(violations) == 1
        assert "double-'**'" in violations[0]

    def test_unscoped_market_ohlcv_glob_detected(self, tmp_path):
        source = """
            BAD_PATH = "data/silver/market_ohlcv/**/*_1D_silver.parquet"
        """
        violations = _scan_source(tmp_path, source)
        assert len(violations) == 1
        assert "unscoped market_ohlcv glob" in violations[0]

    def test_scoped_per_market_glob_not_flagged(self, tmp_path):
        """A glob correctly scoped to ONE market subdirectory (the
        legitimate output shape of layer1_globs()) must NOT be flagged —
        only a bare market_ohlcv/**/ root glob is a violation."""
        source = """
            GOOD_PATH = "data/silver/market_ohlcv/us_stocks/**/*_1D_silver.parquet"
        """
        assert _scan_source(tmp_path, source) == []

    def test_docstring_mentioning_market_ohlcv_not_flagged(self, tmp_path):
        """Regression guard for the exact false positive an earlier regex
        draft of this scanner produced: a module docstring that discusses
        the OLD broken glob path as history/documentation must not be
        flagged — only a LIVE construction (assignment, dict/list value,
        call argument) counts."""
        source = '''
            """
            This module used to read data/silver/market_ohlcv/**/*.parquet
            directly but was fixed to use silver_scope.py instead.
            """
            import polars as pl
        '''
        assert _scan_source(tmp_path, source) == []

    def test_violation_inside_dict_value_detected(self, tmp_path):
        """views.py's real violations were embedded inside dict VALUES
        (a DuckDB SQL string stored as a dict entry), not a simple
        top-level assignment — the scanner must catch this shape too."""
        source = """
            VIEW_DEFINITIONS = {
                "v_ohlcv_1D": '''
                    CREATE VIEW v_ohlcv_1D AS
                    SELECT * FROM read_parquet('data/silver/market_ohlcv/**/*_1D_silver.parquet')
                ''',
            }
        """
        violations = _scan_source(tmp_path, source)
        assert len(violations) == 1
        assert "unscoped market_ohlcv glob" in violations[0]

    def test_violation_inside_list_element_detected(self, tmp_path):
        """pipeline_dashboard.py's real violation was a tuple element in a
        list literal — the scanner must catch this shape too."""
        source = """
            LAYERS = [
                ("Silver OHLCV", "data/silver/market_ohlcv/**/*.parquet", "blue"),
            ]
        """
        violations = _scan_source(tmp_path, source)
        assert len(violations) == 1

    def test_fstring_fragment_detected(self, tmp_path):
        source = '''
            root = "data/silver"
            BAD_PATH = f"{root}/market_ohlcv/**/*_1D_silver.parquet"
        '''
        violations = _scan_source(tmp_path, source)
        assert len(violations) == 1

    def test_silver_scope_py_itself_is_exempt(self, tmp_path):
        """silver_scope.py legitimately constructs these globs — it must
        never be flagged by its own enforcement gate."""
        fake_src = tmp_path / "src" / "utils"
        fake_src.mkdir(parents=True)
        (fake_src / "silver_scope.py").write_text(textwrap.dedent("""
            ROOT = "data/silver/market_ohlcv/**/*.parquet"
        """))
        import os
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            violations = gs.scan()
        finally:
            os.chdir(old_cwd)
        assert violations == []

    def test_real_repo_currently_passes(self):
        """The actual repo, after this pass's fixes (pit_data.py,
        correlation_matrix.py, screener.py, views.py,
        delta_reprocessor.py, pipeline_dashboard.py), must have zero
        violations — this is the live regression guard, not just a
        synthetic-fixture test."""
        import os
        # Run from the actual repo root (this test file's grandparent's parent)
        repo_root = Path(__file__).resolve().parents[2]
        old_cwd = os.getcwd()
        os.chdir(repo_root)
        try:
            violations = gs.scan()
        finally:
            os.chdir(old_cwd)
        assert violations == [], f"Gate G-8 violations found in real repo: {violations}"

    def test_main_returns_1_on_violations(self, tmp_path, monkeypatch, capsys):
        fake_src = tmp_path / "src"
        fake_src.mkdir()
        (fake_src / "bad.py").write_text(
            'BAD = "data/silver/market_ohlcv/**/*.parquet"\n'
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["check_glob_scope.py"])
        exit_code = gs.main()
        assert exit_code == 1
        out = capsys.readouterr().out
        assert "Gate G-8 FAILED" in out

    def test_main_returns_0_on_clean(self, tmp_path, monkeypatch, capsys):
        fake_src = tmp_path / "src"
        fake_src.mkdir()
        (fake_src / "good.py").write_text('GOOD = "data/silver/macro_enriched/**/*.parquet"\n')
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["check_glob_scope.py"])
        exit_code = gs.main()
        assert exit_code == 0
        out = capsys.readouterr().out
        assert "Gate G-8 PASSED" in out
