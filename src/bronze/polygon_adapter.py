"""
polygon_adapter.py — Bronze Polygon.io SourceAdapter (GD §3.3.2)
Fallback source for US stocks when yfinance fails.

Rate limit: 5 req/min (free tier) — managed via SourceLimiters.polygon.
API key: POLYGON_API_KEY from .env.
Coverage: US stocks daily + up to 2Y intraday.

Source Priority (GD §11.1):
    US Stocks: yfinance → Polygon.io

Polygon API format:
    Daily aggs: /v2/aggs/ticker/{ticker}/range/{mult}/{timespan}/{from}/{to}
    Timespan:   minute, day, week, month (free tier)

FIX POL-1 (HIGH): 'hour' timespan is NOT available on Polygon free tier.
    Supported free tier timespans: minute, day, week, month.
    Requesting 1H or 4H silently returns empty results — now blocked upfront.

FIX POL-2 (HIGH): 0-default OHLCV replaced with None for missing fields.
    float(r.get("o", 0)) → 0.0 on partial data; zero-price bars silently pass
    Silver null check. Now returns None so downstream null_check detects them.

FIX POL-3 (HIGH): 429 response now sleeps before returning None.
    "backing off" was logged but no sleep occurred — ChainedAdapter jumped to
    next adapter immediately without waiting for quota reset.

FIX POL-4 (MEDIUM): pagination via next_url followed for complete data retrieval.
    50K row limit silently truncates 1m data (2Y = ~196K rows). next_url
    is now followed up to MAX_PAGES pages.

FIX POL-5 (HIGH): volume cast as int() produced Int64, but polygon_ohlcv.yaml
    defines volume as float64. SchemaValidator SV-1 (exact type match) compares
    actual Int64 vs expected Float64 → FAIL → ALL Polygon OHLCV quarantined.
    POL-2 fixed open/high/low/close to use None-safe float() but missed volume.
    Fix: float(r['v']) if 'v' in r else None — consistent with POL-2 pattern
    and aligned with polygon_ohlcv.yaml schema (float64, nullable: true).
"""

from __future__ import annotations

import os
import time
from datetime import date
from typing import Optional

import pandas as pd  # FIX POL-6 (MEDIUM): module-level import (was inside loop body)
import polars as pl
import requests
from loguru import logger

from src.bronze.source_adapter import SourceAdapter
from src.utils.rate_limiter import SourceLimiters

POLYGON_BASE = "https://api.polygon.io/v2/aggs/ticker"

# FIX POL-1: mark which timespans are available on free tier.
# 'hour' is a paid-tier-only timespan — requesting it returns empty results.
_TIMESPAN_MAP: dict[str, tuple[int, str]] = {
    "5m":  (5,  "minute"),
    "15m": (15, "minute"),
    "1H":  (1,  "hour"),    # FIX POL-1: blocked below — not on free tier
    "4H":  (4,  "hour"),    # FIX POL-1: blocked below — not on free tier
    "1D":  (1,  "day"),
    "1W":  (1,  "week"),
    "1M":  (1,  "month"),
}

# FIX POL-1: timespans available on Polygon free tier
_FREE_TIER_TIMESPANS: frozenset[str] = frozenset({"minute", "day", "week", "month"})

# FIX POL-3: sleep duration after 429 (5 req/min → ~12s between requests)
_RATE_LIMIT_SLEEP: float = 12.0

# FIX POL-4: max pagination pages to follow (safety cap)
_MAX_PAGES: int = 10


class PolygonAdapter(SourceAdapter):
    """
    Polygon.io adapter for US equities.
    symbol must be in Polygon format (e.g. 'AAPL', 'BRK.B').
    """

    def __init__(self) -> None:
        self._api_key = os.getenv("POLYGON_API_KEY", "")

    @property
    def name(self) -> str:
        return "polygon"

    def fetch(
        self,
        symbol: str,
        tf: str,
        start: date,
        end: date,
    ) -> Optional[pl.DataFrame]:
        if not self._api_key:
            logger.debug("[Polygon] POLYGON_API_KEY not set — skipping")
            return None

        timespan_info = _TIMESPAN_MAP.get(tf)
        if timespan_info is None:
            logger.warning(f"[Polygon] Unsupported TF={tf}")
            return None

        mult, timespan = timespan_info

        # FIX POL-1: block hour-based TFs on free tier — they silently return empty
        if timespan not in _FREE_TIER_TIMESPANS:
            logger.info(
                f"[Polygon] TF={tf!r} uses timespan={timespan!r} which is NOT available "
                f"on Polygon free tier. Skipping — yfinance handles 1H/4H instead. "
                f"Upgrade to Polygon paid tier to enable hour-based aggregates."
            )
            return None

        url = (
            f"{POLYGON_BASE}/{symbol}/range/{mult}/{timespan}"
            f"/{start.isoformat()}/{end.isoformat()}"
        )
        params = {
            "adjusted": "true",
            "sort":     "asc",
            "limit":    50_000,
            "apiKey":   self._api_key,
        }

        # FIX POL-4: follow pagination to retrieve all results
        all_records: list[dict] = []
        pages_fetched = 0

        while url and pages_fetched < _MAX_PAGES:
            try:
                SourceLimiters.polygon.wait()
                resp = requests.get(
                    url,
                    params=params if pages_fetched == 0 else None,  # params only on first
                    timeout=30,
                )

                if resp.status_code == 429:
                    # FIX POL-3: actual sleep on 429, not just a log message
                    logger.warning(
                        f"[Polygon] Rate limited (429) — sleeping {_RATE_LIMIT_SLEEP}s"
                    )
                    time.sleep(_RATE_LIMIT_SLEEP)
                    return None   # let ChainedAdapter retry next run

                if resp.status_code != 200:
                    logger.debug(f"[Polygon] HTTP {resp.status_code} for {symbol}/{tf}")
                    return None

                data = resp.json()
                results = data.get("results", [])

                # FIX POL-2: None for missing OHLCV fields — not 0-default
                # 0.0 price silently passes Silver null_check; None is caught correctly
                # FIX POL-6: `import pandas as pd` moved to module level (was here)
                for r in results:
                    all_records.append({
                        "timestamp": pd.Timestamp(r["t"], unit="ms"),
                        "open":  float(r["o"]) if "o" in r else None,  # FIX POL-2
                        "high":  float(r["h"]) if "h" in r else None,  # FIX POL-2
                        "low":   float(r["l"]) if "l" in r else None,  # FIX POL-2
                        "close": float(r["c"]) if "c" in r else None,  # FIX POL-2
                        "volume": float(r["v"]) if "v" in r else None,  # FIX POL-5: float64 matches polygon_ohlcv.yaml; None-safe (nullable: true)
                        "vwap":  float(r["vw"]) if "vw" in r else None,
                    })

                pages_fetched += 1

                # FIX POL-4: follow next_url for pagination
                next_url = data.get("next_url")
                if next_url and data.get("status") == "OK" and len(results) == 50_000:
                    # Append API key to next_url (not included by Polygon)
                    url = next_url + f"&apiKey={self._api_key}"
                    logger.debug(
                        f"[Polygon] {symbol}/{tf}: page {pages_fetched}"
                        f" fetched {len(results)} rows, following next_url"
                    )
                else:
                    url = None   # No more pages

            except Exception as e:
                logger.warning(f"[Polygon] {symbol}/{tf}: {e}")
                return None

        if not all_records:
            return None

        if pages_fetched >= _MAX_PAGES:
            logger.warning(
                f"[Polygon] {symbol}/{tf}: reached MAX_PAGES={_MAX_PAGES}"
                f" ({len(all_records):,} rows). Some historical data may be truncated."
                f" Consider reducing date range or upgrading Polygon plan."
            )

        return pl.from_pandas(pd.DataFrame(all_records))
