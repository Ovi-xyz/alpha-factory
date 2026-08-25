"""tests/unit/test_rate_limiter.py — Rate limiter unit tests"""

import time
from datetime import date

import pytest

from src.utils.rate_limiter import DailyBudgetLimiter, RateLimiter, SourceLimiters


class TestRateLimiter:

    def test_creates_without_error(self):
        limiter = RateLimiter(calls_per_minute=60)
        assert limiter is not None

    def test_wait_enforces_minimum_interval(self):
        """Two consecutive calls must be separated by interval seconds."""
        limiter = RateLimiter(calls_per_minute=300, safety_margin=1.0)  # 5 req/s
        t0 = time.monotonic()
        limiter.wait()
        t1 = time.monotonic()
        limiter.wait()
        t2 = time.monotonic()
        gap = t2 - t1
        assert gap >= 0.18, f"Gap {gap:.3f}s too small for 300/min rate"

    def test_calls_per_second_property(self):
        limiter = RateLimiter(calls_per_minute=60, safety_margin=1.0)
        assert abs(limiter.calls_per_second - 1.0) < 0.01

    def test_safety_margin_reduces_limit(self):
        limiter = RateLimiter(calls_per_minute=100, safety_margin=0.8)
        # Effective limit should be 80/min → 1.333s interval
        assert limiter._limit == 80

    def test_repr_contains_limit(self):
        limiter = RateLimiter(calls_per_minute=60)
        r = repr(limiter)
        assert "RateLimiter" in r
        assert "/min" in r


class TestDailyBudgetLimiter:

    def test_can_call_within_budget(self):
        limiter = DailyBudgetLimiter(25)
        assert limiter.can_call() is True

    def test_budget_depletes_correctly(self):
        limiter = DailyBudgetLimiter(3)
        for _ in range(3):
            assert limiter.can_call()
            limiter.record_call()
        assert not limiter.can_call()

    def test_remaining_decrements(self):
        limiter = DailyBudgetLimiter(10)
        assert limiter.remaining == 10
        limiter.record_call()
        assert limiter.remaining == 9

    def test_used_increments(self):
        limiter = DailyBudgetLimiter(10)
        assert limiter.used == 0
        limiter.record_call()
        assert limiter.used == 1

    def test_repr_contains_budget(self):
        limiter = DailyBudgetLimiter(25)
        r = repr(limiter)
        assert "25" in r
        assert "DailyBudgetLimiter" in r

    def test_zero_remaining_stays_zero(self):
        limiter = DailyBudgetLimiter(2)
        limiter.record_call()
        limiter.record_call()
        assert limiter.remaining == 0
        # Over-recording doesn't go negative
        limiter.record_call()
        # remaining clamped at 0
        assert limiter.remaining == 0


class TestSourceLimiters:

    def test_all_limiters_defined(self):
        """SourceLimiters class has all expected attributes."""
        assert hasattr(SourceLimiters, "fred")
        assert hasattr(SourceLimiters, "polygon")
        assert hasattr(SourceLimiters, "yfinance")
        assert hasattr(SourceLimiters, "alphavantage")
        assert hasattr(SourceLimiters, "bea")

    def test_alphavantage_is_daily_budget(self):
        """AlphaVantage uses DailyBudgetLimiter (25 req/day)."""
        assert isinstance(SourceLimiters.alphavantage, DailyBudgetLimiter)
        assert SourceLimiters.alphavantage.budget == 25

    def test_fred_is_rate_limiter(self):
        """FRED uses RateLimiter (120 req/min)."""
        assert isinstance(SourceLimiters.fred, RateLimiter)

    def test_finnhub_limiter_removed(self):
        """FIX ADR-043 (GMI_Decision_Document_v10.docx): Finnhub retired in
        full — SourceLimiters.finnhub must not exist. Its only conceivable
        consumers (finnhub_ingester.py, finnhub_sentiment_ingester.py) were
        deleted in the same fix, and no other src/ module ever referenced
        it (unlike .polygon/.alphavantage/.yfinance, each of which has a
        live adapter consumer)."""
        assert not hasattr(SourceLimiters, "finnhub")
