"""
tvdatafeed_adapter.py — IDD §6.3 (TvDatafeedAdapter)
SourceAdapter implementation untuk tvdatafeed (primary IDX source).

Integrasi dengan TvDatafeedSessionManager:
    - Session expire → auto force_reconnect()
    - Empty DataFrame → force_reconnect() (silent session death)
    - Exception 'session'/'auth' → force_reconnect()
    - Fallback via ChainedAdapter ke YFinanceJKAdapter

IDX_PARTIAL_FAILURE alert jika > 5 symbols return None dalam satu run.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import polars as pl
from loguru import logger

from src.bronze.source_adapter import SourceAdapter
from src.bronze.tvdatafeed_session import (
    TV_AVAILABLE,
    TvDatafeedSessionManager,
    get_tv_interval,
)

# Alert threshold (dari pipeline.yaml FOREX_NULL_ALERT_THRESHOLD pattern)
IDX_NULL_ALERT_THRESHOLD = 5


class TvDatafeedAdapter(SourceAdapter):
    """
    SourceAdapter untuk tvdatafeed — primary source untuk IDX30.
    Gunakan via ChainedAdapter dengan YFinanceJKAdapter sebagai fallback.

    Usage:
        idx_chain = ChainedAdapter([TvDatafeedAdapter(), YFinanceJKAdapter()])
    """

    @property
    def name(self) -> str:
        return "tvdatafeed"

    def __init__(self) -> None:
        # FIX TVA-1 (HIGH): _null_count as instance variable, not class variable.
        # Class-level counter persists across pipeline runs (in-process memory):
        # 5 failures in Run 1 → _null_count=5; Run 2 fails on first symbol → alert
        # fires immediately even though only 1 fresh failure occurred in Run 2.
        # Instance variable resets on every new TvDatafeedAdapter() instantiation,
        # which happens once per MarketOHLCVIngester.run() call — correct scope.
        self._null_count: int = 0

    def fetch(
        self,
        symbol: str,
        tf: str,
        start: date,
        end: date,
    ) -> Optional[pl.DataFrame]:
        """
        Fetch IDX OHLCV dari tvdatafeed.
        symbol: raw IDX ticker (e.g. 'BBCA', 'TLKM')
        """
        if not TV_AVAILABLE:
            return None

        session  = TvDatafeedSessionManager()
        client   = session.get_client()
        if client is None:
            return None

        interval = get_tv_interval(tf)
        if interval is None:
            logger.warning(f"[tvAdapter] TF={tf} tidak support di tvdatafeed")
            return None

        # Estimasi n_bars dari date range
        n_bars = self._estimate_n_bars(start, end, tf)

        try:
            df = client.get_hist(
                symbol=symbol,
                exchange="IDX",
                interval=interval,
                n_bars=n_bars,
            )

            if df is None or len(df) == 0:
                logger.warning(
                    f"[tvAdapter] Empty result for {symbol}/{tf}"
                    " — possible silent session expiry, forcing reconnect"
                )
                session.force_reconnect()
                self._null_count += 1   # FIX TVA-1: instance var, not class var
                self._check_null_alert(symbol)
                return None

            # Normalize DataFrame
            df = df.reset_index()
            rename_map = {
                "datetime": "timestamp",
                "open":     "open",
                "high":     "high",
                "low":      "low",
                "close":    "close",
                "volume":   "volume",
            }
            df.columns = [c.lower() for c in df.columns]
            df = df.rename(columns={
                k: v for k, v in rename_map.items() if k in df.columns
            })

            keep = ["timestamp", "open", "high", "low", "close", "volume"]
            keep = [c for c in keep if c in df.columns]

            self._null_count = 0   # FIX TVA-1: reset instance var on success
            return pl.from_pandas(df[keep])

        except Exception as e:
            err_str = str(e).lower()
            logger.warning(f"[tvAdapter] Exception {symbol}/{tf}: {e}")

            if any(kw in err_str for kw in ["session", "auth", "login", "token"]):
                logger.info("[tvAdapter] Session error detected — force reconnect")
                session.force_reconnect()

            self._null_count += 1   # FIX TVA-1: instance var
            self._check_null_alert(symbol)
            return None

    @staticmethod
    def _estimate_n_bars(start: date, end: date, tf: str) -> int:
        """Estimate bars needed dari date range + 10% buffer.

        FIX TVA-3 (MEDIUM): IDX session is ~5.5 hours (09:00-14:30 WIB),
        not 6.5 hours (US market). bars_per_day for hourly TFs corrected:
            1H: 5.5 bars/day (was 8 — 45% overestimate for IDX)
            4H: 1.5 bars/day (was 2)
        Over-requesting is harmless (tvdatafeed caps results), but wastes
        bandwidth and slows down session health check overhead.
        """
        days = max((end - start).days, 1)
        bars_per_day = {
            "1D":  1,
            "1W":  1 / 7,
            "1M":  1 / 30,
            "1H":  5.5,   # FIX TVA-3: IDX session 5.5h (was 8 = US market)
            "4H":  1.5,   # FIX TVA-3: IDX ~1.5 blocks of 4H per day (was 2)
            "15m": 22,    # 5.5h × 4 bars/h
            "5m":  66,    # 5.5h × 12 bars/h
            "1m":  330,   # 5.5h × 60 bars/h
        }
        mult = bars_per_day.get(tf, 1)
        return min(int(days * mult * 1.1) + 10, 20_000)

    def _check_null_alert(self, symbol: str) -> None:
        """Alert jika terlalu banyak IDX symbols return null."""
        if self._null_count >= IDX_NULL_ALERT_THRESHOLD:
            logger.error(
                f"[tvAdapter] IDX_PARTIAL_FAILURE: "
                f"{self._null_count} symbols returned null"
                f" (last: {symbol}). Pipeline continues with yfinance .JK fallback."
            )


# FIX TVA-2 (HIGH): YFinanceJKAdapter REMOVED from this file.
# It was dead code — market_ingester imports YFinanceJKAdapter from yfinance_adapter.py.
# Having two definitions of the same class name in different modules is a
# maintainability hazard: changes to one do not propagate to the other.
# The canonical definition is in src/bronze/yfinance_adapter.py — use that one.
# If needed here: from src.bronze.yfinance_adapter import YFinanceJKAdapter
