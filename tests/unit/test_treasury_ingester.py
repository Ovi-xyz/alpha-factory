"""
tests/unit/test_treasury_ingester.py

NEW (v1.11.2) — Coverage gap closure (GMI_Decision_Document_v3.docx Priority
2 / Checkpoint v6 §8 item 2). src/bronze/treasury_ingester.py had 0%
coverage (27 statements, 0 covered) prior to this file, despite being the
delegate that ingests the daily yield curve — a primary macro-regime input
(GD §8.1, T10Y2Y / T10Y3M recession signals).

Tests validate:
    1. Early exit when FRED_API_KEY is not set (no delegate call, no crash)
    2. Successful delegation to FREDIngester with the correct series_filter
    3. Exception from the delegate is caught and logged, not propagated
    4. get_available_tenors() / get_fred_series_ids() — trivial accessors
    5. FIX TI-1 invariant: TreasuryIngester does not inherit BronzeIngester's
       write path (GD §17.3 — it never writes Bronze data itself, FREDIngester does)

Coverage target: >=80% (CI/CD Ops Guide coverage table — src/bronze/*).
"""
from __future__ import annotations

import ast
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.bronze.treasury_ingester import (
    TreasuryIngester,
    TREASURY_FRED_SERIES,
    TENOR_LABELS,
    run,
)


@pytest.fixture
def run_date() -> date:
    return date(2026, 7, 20)


@pytest.fixture
def ingester() -> TreasuryIngester:
    return TreasuryIngester()


# ── Early-Exit Path ─────────────────────────────────────────────────────────

class TestRunNoApiKey:

    def test_missing_api_key_returns_early_no_delegate_call(self, ingester, run_date, monkeypatch):
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        with patch("src.bronze.fred_ingester.FREDIngester") as mock_fred_cls:
            ingester.run(run_date)
            mock_fred_cls.assert_not_called()

    def test_missing_api_key_does_not_raise(self, ingester, run_date, monkeypatch):
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        ingester.run(run_date)  # must not raise


# ── Successful Delegation ────────────────────────────────────────────────────

class TestRunDelegatesToFredIngester:

    def test_delegates_with_correct_series_filter(self, ingester, run_date, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "test_key")
        mock_instance = MagicMock()
        with patch(
            "src.bronze.fred_ingester.FREDIngester", return_value=mock_instance
        ) as mock_fred_cls:
            ingester.run(run_date)
            mock_fred_cls.assert_called_once_with()
            mock_instance.run.assert_called_once_with(
                run_date, series_filter=TREASURY_FRED_SERIES
            )

    def test_series_filter_passed_is_the_canonical_list_object_contents(
        self, ingester, run_date, monkeypatch
    ):
        """Regression guard: must pass the exact 13-series list, not a subset
        or a re-derived one that could silently drift from TREASURY_FRED_SERIES."""
        monkeypatch.setenv("FRED_API_KEY", "test_key")
        mock_instance = MagicMock()
        with patch(
            "src.bronze.fred_ingester.FREDIngester", return_value=mock_instance
        ):
            ingester.run(run_date)
            passed_series = mock_instance.run.call_args.kwargs["series_filter"]
            assert passed_series == TREASURY_FRED_SERIES
            assert "T10Y2Y" in passed_series
            assert "T10Y3M" in passed_series
            assert "DGS10" in passed_series


# ── Exception Handling ────────────────────────────────────────────────────────

class TestRunHandlesDelegateException:

    def test_delegate_exception_is_caught_not_propagated(self, ingester, run_date, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "test_key")
        mock_instance = MagicMock()
        mock_instance.run.side_effect = ConnectionError("FRED API timeout")
        with patch(
            "src.bronze.fred_ingester.FREDIngester", return_value=mock_instance
        ):
            ingester.run(run_date)  # must not raise despite delegate failure


# ── Trivial Accessors ─────────────────────────────────────────────────────────

class TestAccessors:

    def test_get_available_tenors_returns_tenor_label_values(self, ingester):
        tenors = ingester.get_available_tenors()
        assert tenors == list(TENOR_LABELS.values())
        assert "10Y" in tenors
        assert "spread_10y2y" in tenors

    def test_get_fred_series_ids_returns_copy_not_original(self, ingester):
        """Must return a copy — mutating the result must not affect the module
        constant (a common source of hard-to-trace cross-test pollution)."""
        ids = ingester.get_fred_series_ids()
        assert ids == TREASURY_FRED_SERIES
        ids.append("MUTATED")
        assert "MUTATED" not in TREASURY_FRED_SERIES

    def test_tenor_labels_cover_all_fred_series(self):
        """Every series in TREASURY_FRED_SERIES must have a TENOR_LABELS entry —
        an un-mapped series would silently show a raw FRED ID downstream."""
        for series_id in TREASURY_FRED_SERIES:
            assert series_id in TENOR_LABELS, (
                f"{series_id} missing from TENOR_LABELS"
            )


# ── Module-Level run() Entry Point ────────────────────────────────────────────

class TestModuleRunEntryPoint:

    def test_module_run_instantiates_and_calls_ingester(self, run_date):
        with patch("src.bronze.treasury_ingester.TreasuryIngester") as mock_cls:
            run(run_date)
            mock_cls.return_value.run.assert_called_once_with(run_date)


# ── Architectural Invariants (FIX TI-1, GD §17.3) ─────────────────────────────

class TestArchitecturalInvariants:

    def test_does_not_inherit_bronze_ingester(self):
        """FIX TI-1: plain class, not BronzeIngester subclass — TreasuryIngester
        never writes Bronze data itself, only delegates to FREDIngester."""
        from src.bronze.base_ingester import BronzeIngester
        assert not issubclass(TreasuryIngester, BronzeIngester)

    def test_never_calls_write_or_write_macro(self):
        """No write()/write_macro() call anywhere in the module body — all
        writes are performed by the delegated FREDIngester (GD §17.3)."""
        import src.bronze.treasury_ingester as module
        source = Path(module.__file__).read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in ("write", "write_macro"), (
                    "TreasuryIngester calls a write method directly — "
                    "violates FIX TI-1 (delegation-only design, GD §17.3)"
                )

    def test_syntax_valid(self):
        import src.bronze.treasury_ingester as module
        ast.parse(Path(module.__file__).read_text())
