"""
test_adapter_chain.py — Source Adapter Chain Integration Test
Tests the complete fallback chain behaviour for all market types.

Validates:
    1. ChainedAdapter tries adapters in order
    2. Falls back correctly when primary adapter returns None/empty
    3. _source column reflects which adapter succeeded
    4. Market-specific chains work for IDX, Forex, US stocks
    5. DailyBudgetLimiter correctly blocks AlphaVantage when exhausted
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import polars as pl
import pytest

from src.bronze.source_adapter import ChainedAdapter, SourceAdapter
# FIX NEW-4 [P2 MEDIUM] (audit_v1_7_3_uncovered_findings.docx Section 5):
# DailyBudgetLimiter no longer imported here — it lives in src.utils.rate_limiter
# (post FIX SA-1) and is imported locally where actually used, inside
# TestDailyBudgetLimiterChainIntegration below. The old top-level import broke
# collection of all 9 tests in this file (ImportError at module load) and was
# unused at module scope regardless (every usage already had its own local
# correct import).


# ── Stub Adapters ─────────────────────────────────────────────────────────────

class AlwaysReturnAdapter(SourceAdapter):
    def __init__(self, name_: str, rows: int = 3):
        self._name = name_
        self._rows = rows
        self.call_count = 0

    @property
    def name(self) -> str:
        return self._name

    def fetch(self, symbol, tf, start, end) -> pl.DataFrame:
        self.call_count += 1
        return pl.DataFrame({
            "timestamp": [date(2025, 1, i + 2) for i in range(self._rows)],
            "close":     [100.0 + i for i in range(self._rows)],
        })


class AlwaysNoneAdapter(SourceAdapter):
    def __init__(self, name_: str):
        self._name = name_
        self.call_count = 0

    @property
    def name(self) -> str:
        return self._name

    def fetch(self, symbol, tf, start, end) -> Optional[pl.DataFrame]:
        self.call_count += 1
        return None


class AlwaysRaiseAdapter(SourceAdapter):
    def __init__(self, name_: str, error_msg: str = "network error"):
        self._name = name_
        self._error = error_msg
        self.call_count = 0

    @property
    def name(self) -> str:
        return self._name

    def fetch(self, symbol, tf, start, end):
        self.call_count += 1
        raise ConnectionError(self._error)


class EmptyDataframeAdapter(SourceAdapter):
    def __init__(self, name_: str):
        self._name = name_
        self.call_count = 0

    @property
    def name(self) -> str:
        return self._name

    def fetch(self, symbol, tf, start, end) -> pl.DataFrame:
        self.call_count += 1
        return pl.DataFrame()


# ── Test Dates ────────────────────────────────────────────────────────────────

START = date(2025, 1, 2)
END   = date(2025, 1, 31)


# ── ChainedAdapter Basic Tests ────────────────────────────────────────────────

class TestChainedAdapterBasic:

    def test_single_adapter_success(self):
        primary = AlwaysReturnAdapter("primary")
        chain   = ChainedAdapter([primary])
        result  = chain.fetch("AAPL", "1D", START, END)
        assert result is not None
        assert primary.call_count == 1

    def test_source_column_reflects_adapter(self):
        primary = AlwaysReturnAdapter("yfinance")
        chain   = ChainedAdapter([primary])
        result  = chain.fetch("AAPL", "1D", START, END)
        assert "_source" in result.columns
        assert result["_source"].to_list()[0] == "yfinance"

    def test_fallback_on_none(self):
        primary   = AlwaysNoneAdapter("tvdatafeed")
        secondary = AlwaysReturnAdapter("yfinance_jk")
        chain     = ChainedAdapter([primary, secondary])
        result    = chain.fetch("BBCA", "1D", START, END)
        assert result is not None
        assert result["_source"].to_list()[0] == "yfinance_jk"
        assert primary.call_count   == 1
        assert secondary.call_count == 1

    def test_fallback_on_exception(self):
        primary   = AlwaysRaiseAdapter("tvdatafeed", "session expired")
        secondary = AlwaysReturnAdapter("yfinance_jk")
        chain     = ChainedAdapter([primary, secondary])
        result    = chain.fetch("BBCA", "1D", START, END)
        assert result is not None
        assert secondary.call_count == 1

    def test_fallback_on_empty_dataframe(self):
        primary   = EmptyDataframeAdapter("tvdatafeed")
        secondary = AlwaysReturnAdapter("yfinance_jk")
        chain     = ChainedAdapter([primary, secondary])
        result    = chain.fetch("BBCA", "1D", START, END)
        assert result is not None
        assert result["_source"].to_list()[0] == "yfinance_jk"

    def test_all_fail_returns_none(self):
        adapters = [
            AlwaysNoneAdapter("primary"),
            AlwaysRaiseAdapter("secondary"),
            EmptyDataframeAdapter("tertiary"),
        ]
        chain  = ChainedAdapter(adapters)
        result = chain.fetch("AAPL", "1D", START, END)
        assert result is None

    def test_first_success_stops_chain(self):
        """Once primary succeeds, secondary must NOT be called."""
        primary   = AlwaysReturnAdapter("primary")
        secondary = AlwaysReturnAdapter("secondary")
        chain     = ChainedAdapter([primary, secondary])
        chain.fetch("AAPL", "1D", START, END)
        assert primary.call_count   == 1
        assert secondary.call_count == 0   # Not called

    def test_three_adapter_cascade(self):
        """Tests 3-adapter chain: fail, fail, succeed."""
        a1     = AlwaysNoneAdapter("adapter1")
        a2     = AlwaysRaiseAdapter("adapter2")
        a3     = AlwaysReturnAdapter("adapter3")
        chain  = ChainedAdapter([a1, a2, a3])
        result = chain.fetch("AAPL", "1D", START, END)
        assert result is not None
        assert result["_source"].to_list()[0] == "adapter3"
        assert a1.call_count == 1
        assert a2.call_count == 1
        assert a3.call_count == 1


# ── Market-Specific Chain Tests ───────────────────────────────────────────────

class TestMarketSpecificChains:

    def test_idx_chain_pattern(self):
        """IDX: yfinance .JK only -- SOLE source since ADR-029 (GMI_Decision_
        Document_v7.docx, 30 Jul 2026). tvdatafeed retired entirely (signin
        failing since >=29 Jul 2026); yfinance .JK was already the tested
        fallback and is now the only adapter in the chain -- no more
        2-adapter tvdatafeed->yfinance cascade. See KNOWN_RISKS.md RISK-1
        (RESOLVED)."""
        yfinance = AlwaysReturnAdapter("yfinance_jk")
        chain    = ChainedAdapter([yfinance])
        result   = chain.fetch("BBCA", "1D", START, END)
        assert result is not None
        assert result["_source"].to_list()[0] == "yfinance_jk"
        assert yfinance.call_count == 1

    def test_forex_chain_pattern(self):
        """Forex: yfinance → ForexDayCache → AlphaVantage."""
        yfinance  = AlwaysNoneAdapter("yfinance_forex")
        fx_cache  = AlwaysReturnAdapter("forex_day_cache")
        av        = AlwaysReturnAdapter("alphavantage")
        chain     = ChainedAdapter([yfinance, fx_cache, av])
        result    = chain.fetch("EUR_USD", "1D", START, END)
        assert result["_source"].to_list()[0] == "forex_day_cache"
        # AlphaVantage should NOT have been called
        assert av.call_count == 0

    def test_us_stocks_chain_pattern(self):
        """US stocks: yfinance → Polygon."""
        yfinance = AlwaysNoneAdapter("yfinance")
        polygon  = AlwaysReturnAdapter("polygon")
        chain    = ChainedAdapter([yfinance, polygon])
        result   = chain.fetch("AAPL", "1D", START, END)
        assert result["_source"].to_list()[0] == "polygon"

    def test_chain_name_reflects_all_adapters(self):
        """ChainedAdapter.name should mention all constituent adapters."""
        a1    = AlwaysReturnAdapter("yfinance")
        a2    = AlwaysReturnAdapter("polygon")
        chain = ChainedAdapter([a1, a2])
        assert "yfinance" in chain.name
        assert "polygon"  in chain.name


# ── DailyBudgetLimiter Chain Integration ─────────────────────────────────────

class TestDailyBudgetLimiterChainIntegration:

    def test_budget_limiter_blocks_call(self):
        """When AV budget is exhausted, adapter returns None."""
        from src.bronze.alphavantage_adapter import AlphaVantageForexAdapter
        from src.utils.rate_limiter import SourceLimiters

        # Record calls until budget exhausted
        original_remaining = SourceLimiters.alphavantage.remaining
        while SourceLimiters.alphavantage.can_call():
            SourceLimiters.alphavantage.record_call()

        adapter = AlphaVantageForexAdapter()
        import os
        os.environ.setdefault("ALPHAVANTAGE_API_KEY", "test")
        result = adapter.fetch("EUR/USD", "1D", START, END)
        assert result is None   # Budget exhausted → None

    def test_budget_limiter_in_chain_falls_through(self):
        """Budget-limited adapter skips → chain falls through to next."""
        from src.utils.rate_limiter import DailyBudgetLimiter

        class BudgetedAdapter(SourceAdapter):
            def __init__(self, limiter: DailyBudgetLimiter):
                self._limiter = limiter

            @property
            def name(self) -> str:
                return "budgeted"

            def fetch(self, symbol, tf, start, end):
                if not self._limiter.can_call():
                    return None
                self._limiter.record_call()
                return pl.DataFrame({"timestamp": [START], "close": [1.08]})

        limiter    = DailyBudgetLimiter(1)
        limiter.record_call()   # Exhaust budget

        budgeted   = BudgetedAdapter(limiter)
        fallback   = AlwaysReturnAdapter("fallback")
        chain      = ChainedAdapter([budgeted, fallback])
        result     = chain.fetch("EUR/USD", "1D", START, END)
        assert result is not None
        assert result["_source"].to_list()[0] == "fallback"


# ── Edge Cases ────────────────────────────────────────────────────────────────

class TestChainedAdapterEdgeCases:

    def test_empty_adapter_list_raises(self):
        with pytest.raises(ValueError):
            ChainedAdapter([])

    def test_single_adapter_empty_still_returns_none(self):
        chain  = ChainedAdapter([EmptyDataframeAdapter("empty")])
        result = chain.fetch("AAPL", "1D", START, END)
        assert result is None

    def test_multiple_symbol_calls_independent(self):
        """Each symbol call is independent — no shared state between calls."""
        adapter = AlwaysReturnAdapter("yfinance", rows=5)
        chain   = ChainedAdapter([adapter])

        r1 = chain.fetch("AAPL", "1D", START, END)
        r2 = chain.fetch("MSFT", "1D", START, END)

        assert len(r1) == 5
        assert len(r2) == 5
        assert adapter.call_count == 2

    def test_result_has_data_rows(self):
        """Successful fetch must return DataFrame with rows."""
        adapter = AlwaysReturnAdapter("yfinance", rows=10)
        chain   = ChainedAdapter([adapter])
        result  = chain.fetch("AAPL", "1D", START, END)
        assert len(result) == 10

    def test_source_annotation_preserved(self):
        """_source column must be present in all success results."""
        adapter = AlwaysReturnAdapter("polygon")
        chain   = ChainedAdapter([adapter])
        result  = chain.fetch("AAPL", "1D", START, END)
        assert "_source" in result.columns
        assert all(s == "polygon" for s in result["_source"].to_list())
