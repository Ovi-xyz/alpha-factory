"""
rate_limiter.py — GD §11 (API Rate Limiting & Fallback Strategy)
Rate limiting utilities for all 11 data sources.

Two complementary mechanisms:
    RateLimiter:        Token-bucket based req/min throttle (FRED, Finnhub, etc.)
    DailyBudgetLimiter: Daily quota enforcement (AlphaVantage: 25 req/day)

Usage:
    # Per-request throttle (blocks until slot available)
    limiter = RateLimiter(calls_per_minute=50)  # Finnhub: 60 rpm → 50 safe
    limiter.wait()                               # call before each API request
    response = requests.get(url)

    # Daily budget
    budget = DailyBudgetLimiter(25)             # AlphaVantage free tier
    if budget.can_call():
        response = av_client.call(...)
        budget.record_call()
    else:
        logger.warning("AV budget exhausted — using fallback")
"""

from __future__ import annotations

import time
from datetime import date
from threading import Lock
from typing import Optional

from loguru import logger


# ── Token-Bucket Rate Limiter ─────────────────────────────────────────────────

class RateLimiter:
    """
    Token-bucket rate limiter for per-minute API quotas.
    Thread-safe via Lock — safe for concurrent Bronze ingesters.

    Automatically adds delay between calls to stay under the rate limit.
    Uses a sliding window approach for accurate throttling.
    """

    def __init__(
        self,
        calls_per_minute: int,
        safety_margin: float = 0.85,
    ) -> None:
        """
        Args:
            calls_per_minute: API rate limit (e.g. 60 for Finnhub)
            safety_margin:    Use only this fraction of limit (e.g. 0.85 = 85%)
                              Prevents hitting the exact limit boundary.
        """
        self._limit    = int(calls_per_minute * safety_margin)
        self._interval = 60.0 / self._limit    # seconds between calls
        self._lock     = Lock()
        self._last_call: float = 0.0

    def wait(self) -> None:
        """Block until next call is allowed. Call before each API request."""
        with self._lock:
            now     = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self._interval:
                sleep_for = self._interval - elapsed
                time.sleep(sleep_for)
            self._last_call = time.monotonic()

    @property
    def calls_per_second(self) -> float:
        return 1.0 / self._interval

    def __repr__(self) -> str:
        return (
            f"RateLimiter(limit={self._limit}/min,"
            f" interval={self._interval:.2f}s)"
        )


# ── Daily Budget Limiter ──────────────────────────────────────────────────────

class DailyBudgetLimiter:
    """
    Daily quota enforcement for APIs with per-day call limits.
    AlphaVantage free tier: 25 req/day — the most restrictive source.

    Resets automatically at midnight (date boundary check on each call).
    """

    def __init__(self, calls_per_day: int) -> None:
        self.budget       = calls_per_day
        self._used        = 0
        self._reset_date: Optional[date] = None
        self._lock        = Lock()

    def can_call(self) -> bool:
        """Return True if budget remaining for today."""
        with self._lock:
            self._maybe_reset()
            return self._used < self.budget

    def record_call(self) -> None:
        """Record that one API call was made. Call after every successful request."""
        with self._lock:
            self._maybe_reset()
            self._used += 1
            remaining = self.budget - self._used
            if remaining <= 3:
                logger.warning(
                    f"[DailyBudget] Low: {remaining}/{self.budget} calls remaining today"
                )

    @property
    def remaining(self) -> int:
        with self._lock:
            self._maybe_reset()
            return max(0, self.budget - self._used)

    @property
    def used(self) -> int:
        with self._lock:
            self._maybe_reset()
            return self._used

    def _maybe_reset(self) -> None:
        today = date.today()
        if self._reset_date != today:
            self._used       = 0
            self._reset_date = today

    def __repr__(self) -> str:
        return (
            f"DailyBudgetLimiter("
            f"budget={self.budget}, used={self._used}, "
            f"remaining={self.remaining})"
        )


# ── Pre-configured Limiters per Source (GD §11.1) ────────────────────────────

class SourceLimiters:
    """
    Singleton container for all source rate limiters.
    Import this and call .wait() / .can_call() before each API request.

    Configured per GD §11.1 Rate Limit Matrix with safety margins.
    """
    # Per-minute throttles (thread-safe, blocking)
    fred      = RateLimiter(calls_per_minute=120, safety_margin=0.80)  # → 96/min
    bls       = RateLimiter(calls_per_minute=500, safety_margin=0.80)  # per-day actually, use gently
    bea       = RateLimiter(calls_per_minute=100, safety_margin=0.85)  # → 85/min
    finnhub   = RateLimiter(calls_per_minute=60,  safety_margin=0.83)  # → 50/min
    polygon   = RateLimiter(calls_per_minute=5,   safety_margin=0.80)  # → 4/min
    yfinance  = RateLimiter(calls_per_minute=100, safety_margin=0.90)  # ~2000/hr ÷ 60

    # Daily budget limiters (non-blocking, check before call)
    alphavantage = DailyBudgetLimiter(25)    # 1 key × 25 req/day
