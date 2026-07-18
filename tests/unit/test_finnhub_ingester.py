"""
tests/unit/test_finnhub_ingester.py — FinnhubIngester unit tests

FIX RISK-4 (KNOWN_RISKS.md): this file did not exist before — the ONLY
Bronze ingester in the repo with zero test coverage of any kind, and (not
coincidentally) the only one with zero SchemaValidator involvement. Tests
below cover:

  1. SchemaValidator wired into both write paths (_ingest_earnings_calendar,
     _ingest_symbol) — success and quarantine-on-mismatch.
  2. The two specific dtype fragilities the fix addresses empirically:
     an all-null eps_actual column (the NORMAL case for this ingester's
     90-day-forward earnings window) and an int-shaped revenue_estimate
     value — neither must cause a spurious quarantine.
  3. Existing FH-1/FH-2/FH-3 behavior (asset_class routing,
     get_days_to_earnings query) — smoke coverage since none existed.
"""

from __future__ import annotations

import sys
from datetime import date
from types import ModuleType
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.bronze.finnhub_ingester import (
    FinnhubIngester,
    get_days_to_earnings,
)


@pytest.fixture
def run_date() -> date:
    return date(2026, 7, 1)


@pytest.fixture
def ingester(monkeypatch) -> FinnhubIngester:
    monkeypatch.setenv("FINNHUB_API_KEY", "fake-key-for-tests")
    return FinnhubIngester()


def _fake_finnhub_module(mock_client) -> ModuleType:
    mod = ModuleType("finnhub")
    mod.Client = MagicMock(return_value=mock_client)
    return mod


class TestConstructor:
    def test_validators_instantiated(self, ingester):
        assert ingester._earnings_validator is not None
        assert ingester._quote_validator is not None


class TestRunEarlyExits:
    def test_no_api_key_returns_early(self, monkeypatch, run_date):
        monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
        ing = FinnhubIngester()
        with patch.object(ing, "_ingest_earnings_calendar") as mock_earn:
            ing.run(run_date)
        mock_earn.assert_not_called()

    def test_finnhub_not_installed_returns_early(self, ingester, run_date):
        with patch.dict(sys.modules, {"finnhub": None}):
            with patch.object(ingester, "_ingest_earnings_calendar") as mock_earn:
                ingester.run(run_date)
        mock_earn.assert_not_called()


class TestEarningsCalendarSchemaValidation:
    """FIX RISK-4 — the core of this remediation."""

    def _run_with_calendar_response(self, ingester, run_date, items):
        mock_client = MagicMock()
        mock_client.earnings_calendar.return_value = {"earningsCalendar": items}
        ingester._client = mock_client
        ingester._ingest_earnings_calendar(run_date)

    def test_valid_response_passes_validation_and_writes(self, ingester, run_date):
        items = [{
            "symbol": "AAPL", "date": "2026-08-15",
            "epsEstimate": 1.5, "epsActual": None,
            "revenueEstimate": 95000000000, "quarter": 3, "year": 2026,
        }]
        with patch.object(ingester, "write") as mock_write:
            self._run_with_calendar_response(ingester, run_date, items)
        mock_write.assert_called_once()
        assert mock_write.call_args.kwargs["asset_class"] == (
            "market/fundamental/earnings_calendar"
        )

    def test_all_null_eps_actual_does_not_cause_spurious_quarantine(
        self, ingester, run_date
    ):
        """
        FIX RISK-4's specific fragility #1: this ingester only fetches a
        90-day FORWARD window, so eps_actual is null for essentially every
        row in real operation — that must be the NORMAL case, not a schema
        mismatch. Pre-cast, an all-None column infers Polars' Null dtype,
        which would fail an exact-match Float64 check.
        """
        items = [
            {"symbol": "AAPL", "date": "2026-08-15", "epsEstimate": 1.5,
             "epsActual": None, "revenueEstimate": 95_000_000_000.0,
             "quarter": 3, "year": 2026},
            {"symbol": "MSFT", "date": "2026-08-20", "epsEstimate": 2.1,
             "epsActual": None, "revenueEstimate": 61_000_000_000.0,
             "quarter": 3, "year": 2026},
        ]
        with patch.object(ingester, "write") as mock_write, \
             patch.object(ingester._earnings_validator, "handle_mismatch") as mock_mismatch:
            self._run_with_calendar_response(ingester, run_date, items)
        mock_write.assert_called_once()
        mock_mismatch.assert_not_called()

    def test_integer_shaped_revenue_estimate_does_not_cause_spurious_quarantine(
        self, ingester, run_date
    ):
        """
        FIX RISK-4's specific fragility #2: revenueEstimate can arrive as
        a whole-number JSON integer (no decimal point). An all-integer
        batch would infer Int64 against the schema's Float64 declaration
        without the explicit cast this fix adds.
        """
        items = [{
            "symbol": "AAPL", "date": "2026-08-15",
            "epsEstimate": 1.5, "epsActual": 1.4,
            "revenueEstimate": 95000000000,  # plain int, no decimal
            "quarter": 3, "year": 2026,
        }]
        with patch.object(ingester, "write") as mock_write, \
             patch.object(ingester._earnings_validator, "handle_mismatch") as mock_mismatch:
            self._run_with_calendar_response(ingester, run_date, items)
        mock_write.assert_called_once()
        mock_mismatch.assert_not_called()

    def test_schema_mismatch_skips_write_calls_quarantine(self, ingester, run_date):
        items = [{"symbol": "AAPL", "date": "2026-08-15"}]
        with patch.object(ingester._earnings_validator, "validate",
                           return_value=(False, ["Missing column: quarter"])):
            with patch.object(ingester, "write") as mock_write, \
                 patch.object(ingester._earnings_validator, "handle_mismatch") as mock_mismatch:
                self._run_with_calendar_response(ingester, run_date, items)
        mock_write.assert_not_called()
        mock_mismatch.assert_called_once()

    def test_empty_calendar_skips_write(self, ingester, run_date):
        with patch.object(ingester, "write") as mock_write:
            self._run_with_calendar_response(ingester, run_date, [])
        mock_write.assert_not_called()

    def test_records_missing_symbol_are_skipped(self, ingester, run_date):
        items = [
            {"symbol": "", "date": "2026-08-15", "quarter": 3, "year": 2026},
            {"symbol": "AAPL", "date": "2026-08-16", "quarter": 3, "year": 2026},
        ]
        with patch.object(ingester, "write") as mock_write:
            self._run_with_calendar_response(ingester, run_date, items)
        mock_write.assert_called_once()
        written_df = mock_write.call_args.kwargs["df"]
        assert written_df["symbol"].to_list() == ["AAPL"]


class TestQuoteSchemaValidation:
    """FIX RISK-4 — quote write path."""

    def test_valid_quote_passes_validation_and_writes(self, ingester, run_date):
        mock_client = MagicMock()
        mock_client.quote.return_value = {
            "c": 152.3, "d": 1.2, "dp": 0.8, "h": 155.0, "l": 149.0,
            "o": 150.0, "pc": 151.1, "t": 1751500000,
        }
        ingester._client = mock_client
        with patch.object(ingester, "write") as mock_write:
            ingester._ingest_symbol("AAPL", run_date)
        mock_write.assert_called_once()
        assert mock_write.call_args.kwargs["asset_class"] == "market/fundamental/quote"

    def test_quote_columns_use_day_high_day_low_not_52w(self, ingester, run_date):
        """NEW — GMI_Decision_Document_v2.docx §5: high_52w/low_52w renamed
        to day_high/day_low (Finnhub's h/l fields are the current trading
        day's high/low, not a 52-week range). This locks in the rename at
        the Bronze producer side — see
        tests/unit/test_fundamental_processor.py::
        test_process_quotes_reads_day_high_day_low for the Silver consumer
        side (fundamental_processor.py::process_quotes(), a real consumer
        found during this rename despite the decision document's stated
        'zero consumers' premise)."""
        mock_client = MagicMock()
        mock_client.quote.return_value = {
            "c": 152.3, "d": 1.2, "dp": 0.8, "h": 155.0, "l": 149.0,
            "o": 150.0, "pc": 151.1, "t": 1751500000,
        }
        ingester._client = mock_client
        with patch.object(ingester, "write") as mock_write:
            ingester._ingest_symbol("AAPL", run_date)
        written_df = mock_write.call_args.kwargs["df"]
        assert "day_high" in written_df.columns
        assert "day_low" in written_df.columns
        assert "high_52w" not in written_df.columns
        assert "low_52w" not in written_df.columns
        assert written_df["day_high"].to_list() == [155.0]
        assert written_df["day_low"].to_list() == [149.0]

    def test_all_zero_quote_does_not_cause_spurious_quarantine(self, ingester, run_date):
        """Documented Finnhub quirk: invalid/delisted symbols return
        all-zero numeric fields, not null/missing — must not quarantine."""
        mock_client = MagicMock()
        mock_client.quote.return_value = {
            "c": 0, "d": 0, "dp": 0, "h": 0, "l": 0, "o": 0, "pc": 0, "t": 0,
        }
        ingester._client = mock_client
        with patch.object(ingester, "write") as mock_write, \
             patch.object(ingester._quote_validator, "handle_mismatch") as mock_mismatch:
            ingester._ingest_symbol("DELISTED", run_date)
        mock_write.assert_called_once()
        mock_mismatch.assert_not_called()

    def test_schema_mismatch_skips_write_calls_quarantine(self, ingester, run_date):
        mock_client = MagicMock()
        mock_client.quote.return_value = {"c": 152.3}
        ingester._client = mock_client
        with patch.object(ingester._quote_validator, "validate",
                           return_value=(False, ["Missing column: change"])):
            with patch.object(ingester, "write") as mock_write, \
                 patch.object(ingester._quote_validator, "handle_mismatch") as mock_mismatch:
                ingester._ingest_symbol("AAPL", run_date)
        mock_write.assert_not_called()
        mock_mismatch.assert_called_once()

    def test_quote_fetch_exception_does_not_propagate(self, ingester, run_date):
        mock_client = MagicMock()
        mock_client.quote.side_effect = RuntimeError("API down")
        ingester._client = mock_client
        with patch.object(ingester, "write") as mock_write:
            ingester._ingest_symbol("AAPL", run_date)  # must not raise
        mock_write.assert_not_called()

    def test_empty_quote_response_skips_write(self, ingester, run_date):
        mock_client = MagicMock()
        mock_client.quote.return_value = {}
        ingester._client = mock_client
        with patch.object(ingester, "write") as mock_write:
            ingester._ingest_symbol("AAPL", run_date)
        mock_write.assert_not_called()


class TestGetDaysToEarnings:
    """Basic smoke coverage — previously zero, like the rest of this file."""

    def test_returns_none_when_no_data(self, tmp_path, monkeypatch):
        import src.bronze.finnhub_ingester as fh_mod
        monkeypatch.setattr(fh_mod, "BRONZE_FUNDAMENTAL", tmp_path)
        result = get_days_to_earnings("AAPL", date(2099, 1, 1))
        assert result is None

    def test_returns_correct_day_count(self, tmp_path, monkeypatch):
        import src.bronze.finnhub_ingester as fh_mod
        monkeypatch.setattr(fh_mod, "BRONZE_FUNDAMENTAL", tmp_path)

        out_dir = tmp_path / "earnings_calendar" / "finnhub"
        out_dir.mkdir(parents=True)
        pl.DataFrame({
            "symbol": ["AAPL"], "earnings_date": ["2026-07-15"],
        }).write_parquet(out_dir / "data.parquet")

        result = get_days_to_earnings("AAPL", date(2026, 7, 1))
        assert result == 14
