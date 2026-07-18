"""
tests/unit/test_context_anchors.py — ContextAnchorsResolver Unit Tests
MOVED GMI-CTX-001: migrated from tests/unit/test_active_symbols.py's
TestGMIAS001DualLayerOutput (resolve_context/load_context/load_context_full
tests) after src/silver/context_anchors.py was extracted from
active_symbols.py — see that module's docstring for the full rationale.

Test matrix:
  - resolve() writes context_anchors_{date}.parquet using only
    InstrumentLoader — no Silver DuckDB query involved (Architecture v2.0
    Table §4.2: "Filter: None").
  - Deferred instruments (TIN, CPO, RUBBER; ADR-007) excluded.
  - Output schema carries all required Layer 2 metadata columns.
  - load() / load_full() success paths (NEW — these had ZERO test coverage
    under either name, before or after the split; load_context_full() in
    particular had no caller and no test anywhere in the pre-split repo).
  - load() / load_full() raise FileNotFoundError when not yet resolved.
  - Module-level run() delegates correctly (mirrors the job_registry.py
    wrapper contract test pattern used throughout this repo).
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from src.silver.context_anchors import RESOLVER_VERSION, ContextAnchorsResolver, run


def _resolver_with_tmp_output(tmp_path, monkeypatch) -> ContextAnchorsResolver:
    resolver = ContextAnchorsResolver()
    output_path = tmp_path / "silver" / "context_anchors"
    monkeypatch.setattr(type(resolver), "OUTPUT_PATH", property(lambda s: output_path))
    return resolver, output_path


class TestContextAnchorsResolve:
    """resolve() — Layer 2, config-driven, zero Silver dependency."""

    def test_resolve_writes_parquet_without_silver(self, tmp_path, monkeypatch):
        """
        MOVED GMI-CTX-001 (was test_resolve_context_writes_parquet_without_silver):
        resolve() must produce context_anchors_{date}.parquet using only
        InstrumentLoader (no Silver DuckDB query, no silver_1d_path argument
        even exists on this method's signature). We do NOT mock get_loader
        here — it exercises the real InstrumentLoader against
        instruments.yaml v1.5 (56 active context instruments post
        ADR-014/024).
        """
        resolver, output_path = _resolver_with_tmp_output(tmp_path, monkeypatch)
        run_date = date(2025, 3, 3)

        symbols = resolver.resolve(run_date)

        context_parquet = output_path / f"context_anchors_{run_date.isoformat()}.parquet"
        assert context_parquet.exists(), "context_anchors_{date}.parquet must exist"
        assert len(symbols) == 56, (
            f"Expected 56 active Layer 2 instruments (59 - 3 deferred), got {len(symbols)}"
        )

    def test_resolve_excludes_deferred(self, tmp_path, monkeypatch):
        """MOVED GMI-CTX-001 / ADR-007: TIN, CPO, RUBBER must not appear —
        context_available=False (Wave 2 pending MYR normalization)."""
        resolver, _ = _resolver_with_tmp_output(tmp_path, monkeypatch)
        symbols = resolver.resolve(date(2025, 3, 4))
        assert "TIN"    not in symbols
        assert "CPO"    not in symbols
        assert "RUBBER" not in symbols

    def test_resolve_parquet_schema(self, tmp_path, monkeypatch):
        """MOVED GMI-CTX-001: output parquet must carry all required
        metadata columns."""
        resolver, output_path = _resolver_with_tmp_output(tmp_path, monkeypatch)
        run_date = date(2025, 3, 5)
        resolver.resolve(run_date)

        df = pl.read_parquet(output_path / f"context_anchors_{run_date.isoformat()}.parquet")
        required_cols = {
            "symbol", "context_category", "context_group", "layer",
            "include_in_forecast", "reliability_flag", "proxy_for",
            "resolved_date", "resolver_version",
        }
        missing = required_cols - set(df.columns)
        assert not missing, f"context_anchors parquet missing columns: {missing}"

    def test_resolve_returns_list_of_strings(self, tmp_path, monkeypatch):
        resolver, _ = _resolver_with_tmp_output(tmp_path, monkeypatch)
        symbols = resolver.resolve(date(2025, 3, 6))
        assert isinstance(symbols, list)
        assert all(isinstance(s, str) for s in symbols)

    def test_resolve_stamps_resolver_version(self, tmp_path, monkeypatch):
        resolver, output_path = _resolver_with_tmp_output(tmp_path, monkeypatch)
        run_date = date(2025, 3, 7)
        resolver.resolve(run_date)
        df = pl.read_parquet(output_path / f"context_anchors_{run_date.isoformat()}.parquet")
        assert set(df["resolver_version"].unique().to_list()) == {RESOLVER_VERSION}

    def test_resolve_is_idempotent(self, tmp_path, monkeypatch):
        """Running resolve() twice for the same run_date must produce
        identical symbol sets (config-driven — no reason to diverge)."""
        resolver, _ = _resolver_with_tmp_output(tmp_path, monkeypatch)
        run_date = date(2025, 3, 8)
        s1 = resolver.resolve(run_date)
        s2 = resolver.resolve(run_date)
        assert sorted(s1) == sorted(s2)


class TestContextAnchorsLoad:
    """
    load() / load_full() — NEW coverage. Neither method's success path
    (nor load_full() at all, under any name) had a single test anywhere in
    the repo before this file was created — an outright gap, not just a
    migration.
    """

    def test_load_returns_list_of_strings(self, tmp_path, monkeypatch):
        resolver, _ = _resolver_with_tmp_output(tmp_path, monkeypatch)
        run_date = date(2025, 3, 9)
        resolver.resolve(run_date)

        symbols = resolver.load(run_date)
        assert isinstance(symbols, list)
        assert all(isinstance(s, str) for s in symbols)
        assert len(symbols) == 56

    def test_load_matches_resolve_output(self, tmp_path, monkeypatch):
        resolver, _ = _resolver_with_tmp_output(tmp_path, monkeypatch)
        run_date = date(2025, 3, 10)
        resolved = resolver.resolve(run_date)
        loaded = resolver.load(run_date)
        assert sorted(resolved) == sorted(loaded)

    def test_load_raises_when_no_parquet(self, tmp_path, monkeypatch):
        """MOVED GMI-CTX-001 (was test_load_context_raises_when_no_parquet)."""
        resolver, _ = _resolver_with_tmp_output(tmp_path, monkeypatch)
        with pytest.raises(FileNotFoundError):
            resolver.load(date(2099, 1, 1))

    def test_load_full_returns_dataframe_with_all_columns(self, tmp_path, monkeypatch):
        """NEW — load_full() had zero test coverage under any name
        (load_context_full() previously) anywhere in the repo."""
        resolver, _ = _resolver_with_tmp_output(tmp_path, monkeypatch)
        run_date = date(2025, 3, 11)
        resolver.resolve(run_date)

        df = resolver.load_full(run_date)
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 56
        required_cols = {
            "symbol", "context_category", "context_group", "layer",
            "include_in_forecast", "reliability_flag", "proxy_for",
            "resolved_date", "resolver_version",
        }
        missing = required_cols - set(df.columns)
        assert not missing, f"load_full() DataFrame missing columns: {missing}"

    def test_load_full_raises_when_no_parquet(self, tmp_path, monkeypatch):
        """NEW — load_full()'s FileNotFoundError path was never tested."""
        resolver, _ = _resolver_with_tmp_output(tmp_path, monkeypatch)
        with pytest.raises(FileNotFoundError):
            resolver.load_full(date(2099, 1, 1))


class TestContextAnchorsRunEntryPoint:
    """Module-level run() — job_registry.py wrapper contract."""

    def test_run_delegates_to_resolver_resolve(self, tmp_path, monkeypatch):
        from unittest.mock import patch
        run_date = date(2025, 3, 12)
        with patch(
            "src.silver.context_anchors.ContextAnchorsResolver.resolve"
        ) as mock_resolve:
            run(run_date)
        mock_resolve.assert_called_once_with(run_date)

    def test_run_produces_parquet_end_to_end(self, tmp_path, monkeypatch):
        """No mocking — full run() call against real InstrumentLoader,
        confirming the wiring (not just the delegation) works."""
        resolver = ContextAnchorsResolver()
        output_path = tmp_path / "silver" / "context_anchors"
        monkeypatch.setattr(
            "src.silver.context_anchors.ContextAnchorsResolver.OUTPUT_PATH",
            property(lambda s: output_path),
        )
        run_date = date(2025, 3, 13)
        run(run_date)
        assert (output_path / f"context_anchors_{run_date.isoformat()}.parquet").exists()
