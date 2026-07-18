"""
tests/unit/test_silver_scope.py — silver_scope.py Unit Tests
ADD GMI-SCOPE-001 — see src/utils/silver_scope.py module docstring for the
three empirically-confirmed masking/pollution bugs this utility fixes.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.utils.silver_scope import CONTEXT_MARKET, context_glob, layer1_globs, layer1_markets


def _touch_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["X"], "timestamp": ["2026-01-01"]}).write_parquet(path)


class TestLayer1Markets:
    def test_returns_real_layer1_markets(self):
        """Sanity against the actual instruments.yaml v1.4 universe —
        empirically confirmed set (checkpoint + direct verification)."""
        assert layer1_markets() == ["commodity", "forex", "idx", "us_stocks"]

    def test_context_never_appears(self):
        assert CONTEXT_MARKET not in layer1_markets()

    def test_index_never_appears(self):
        """Permanently empty since ADR-003 (SPX/VIX reclassified to Layer 2)."""
        assert "index" not in layer1_markets()


class TestLayer1Globs:
    def test_skips_nonexistent_market_directories(self, tmp_path):
        """Only us_stocks/ exists on disk — the other 3 known Layer 1
        markets must be silently skipped, not included as dead globs."""
        _touch_parquet(tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet")
        globs = layer1_globs(tmp_path, "*_1D_silver.parquet")
        assert len(globs) == 1
        assert "us_stocks" in globs[0]

    def test_returns_empty_list_when_nothing_exists(self, tmp_path):
        """Fresh install / pre-backfill state — must return [], not raise
        and not silently include a dead glob that would break a DuckDB
        list-bound read_parquet() call downstream."""
        assert layer1_globs(tmp_path, "*_1D_silver.parquet") == []

    def test_never_includes_context_directory(self, tmp_path):
        """The whole point of this helper: context/ must never leak into
        a 'Layer 1' glob list, even when it exists on disk alongside
        Layer 1 markets.

        Uses a subdirectory (not tmp_path directly) for the glob root:
        pytest's tmp_path fixture is itself named after the test function
        ("test_never_includes_context_directory..."), so a naive
        substring check on the full glob string would spuriously match
        the fixture's own path, not the market segment this test actually
        cares about. Checking the constructed market-path prefix directly
        avoids that false positive.
        """
        root = tmp_path / "market_ohlcv"
        _touch_parquet(root / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet")
        _touch_parquet(root / "context" / "symbol=VIX" / "VIX_1D_silver.parquet")
        globs = layer1_globs(root, "*_1D_silver.parquet")
        context_prefix = str(root / "context")
        assert all(not g.startswith(context_prefix) for g in globs)

    def test_glob_has_single_double_star_only(self, tmp_path):
        """Guard against reintroducing the double-'**' DuckDB defect class
        (KNOWN_RISKS.md RISK-2 / checkpoint Section 4.3) — each returned
        glob string must contain exactly one '**' occurrence."""
        _touch_parquet(tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet")
        globs = layer1_globs(tmp_path, "*_1D_silver.parquet")
        for g in globs:
            assert g.count("**") == 1


class TestContextGlob:
    def test_returns_none_when_context_dir_absent(self, tmp_path):
        assert context_glob(tmp_path, "*_1D_silver.parquet") is None

    def test_returns_glob_when_context_dir_present(self, tmp_path):
        _touch_parquet(tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet")
        g = context_glob(tmp_path, "*_1D_silver.parquet")
        assert g is not None
        assert "context" in g
        assert g.count("**") == 1

    def test_never_matches_layer1_markets(self, tmp_path):
        _touch_parquet(tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet")
        _touch_parquet(tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet")
        g = context_glob(tmp_path, "*_1D_silver.parquet")
        assert "us_stocks" not in g


class TestScopingCorrectnessEndToEnd:
    """Reproduces the exact empirical probe used to discover the masking
    bugs (see silver_scope.py module docstring) — now as a permanent
    regression guard rather than an ad hoc sandbox script."""

    def test_context_rows_excluded_from_layer1_scoped_query(self, tmp_path):
        import duckdb
        from datetime import date

        aapl_path = tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet"
        vix_path = tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet"
        aapl_path.parent.mkdir(parents=True)
        vix_path.parent.mkdir(parents=True)

        pl.DataFrame({
            "symbol": ["AAPL"], "timestamp": [date(2026, 6, 1)],
        }).write_parquet(aapl_path)
        pl.DataFrame({
            "symbol": ["VIX"], "timestamp": [date(2026, 6, 20)],  # fresher
        }).write_parquet(vix_path)

        globs = layer1_globs(tmp_path, "*_1D_silver.parquet")
        con = duckdb.connect()
        result = con.execute(
            "SELECT MAX(CAST(timestamp AS DATE)) AS latest, "
            "COUNT(DISTINCT symbol) AS n FROM read_parquet($globs, hive_partitioning=true)",
            {"globs": globs},
        ).fetchone()
        # Must see ONLY AAPL's date, not VIX's fresher one — this is the
        # exact defect that let a fresh Layer 2 anchor mask Layer 1 staleness.
        assert result[0] == date(2026, 6, 1)
        assert result[1] == 1
