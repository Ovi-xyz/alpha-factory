"""
alphavantage_adapter.py — Bronze AlphaVantage SourceAdapter (GD §3.3.2)
Supplemental source for Forex and Commodity (DXY).

Rate limit: **25 req/DAY** (most restrictive source) — DailyBudgetLimiter.
API key: ALPHAVANTAGE_API_KEY from .env.
Role: FX supplemental only. NOT used for US stocks.

Source Priority (GD §11.1):
    Forex: yfinance → ForexDayCache → AlphaVantage (DXY only if needed)
    Commodity: yfinance only (AV not used per GD §5.1)

AlphaVantage endpoints used:
    FX_DAILY:    Daily forex OHLCV
    FX_INTRADAY: Intraday forex (15min, 60min)

FIX AV-3 (HIGH): OHLCV fields used float(vals.get('1. open', 0)) — 0.0 default
    when key is missing. Zero-price bars silently pass Silver null check and quality
    gates (OHLCVProcessor checks for None/null, not 0). Inconsistent with POL-2
    which correctly returns None for missing Polygon OHLCV fields.
    Fix: float(vals['key']) if 'key' in vals else None — same None-safe pattern
    as POL-2. volume set to None (AV FX has no volume — explicit None is cleaner
    than 0 which could be misinterpreted as actual zero trading volume).
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Optional

import polars as pl
import requests
from loguru import logger

from src.bronze.source_adapter import SourceAdapter
from src.utils.rate_limiter import SourceLimiters

AV_BASE = "https://www.alphavantage.co/query"

_FUNCTION_MAP: dict[str, str] = {
    "1D":  "FX_DAILY",
    "1W":  "FX_WEEKLY",
    "1M":  "FX_MONTHLY",
    "1H":  "FX_INTRADAY",
    "15m": "FX_INTRADAY",
}

_INTERVAL_MAP: dict[str, str] = {
    "1H":  "60min",
    "15m": "15min",
}


class AlphaVantageForexAdapter(SourceAdapter):
    """
    AlphaVantage adapter for FX data.
    Uses DailyBudgetLimiter (25 req/day).

    symbol: raw forex pair format 'EUR/USD' or 'EURUSD' or normalized 'EUR_USD'.
    Used as last-resort fallback after yfinance and ForexDayCache both fail.
    """

    def __init__(self) -> None:
        self._api_key = os.getenv("ALPHAVANTAGE_API_KEY", "")

    @property
    def name(self) -> str:
        return "alphavantage"

    def fetch(
        self,
        symbol: str,
        tf: str,
        start: date,
        end: date,
    ) -> Optional[pl.DataFrame]:
        if not self._api_key:
            logger.debug("[AV] ALPHAVANTAGE_API_KEY not set — skipping")
            return None

        if not SourceLimiters.alphavantage.can_call():
            logger.warning(
                f"[AV] Daily budget exhausted"
                f" ({SourceLimiters.alphavantage.budget} req/day)."
                " Skipping AV call."
            )
            return None

        from_sym, to_sym = self._parse_pair(symbol)
        if not from_sym or not to_sym:
            return None

        function = _FUNCTION_MAP.get(tf)
        if function is None:
            logger.debug(f"[AV] TF={tf} not supported for AV FX")
            return None

        params: dict = {
            "function":   function,
            "from_symbol": from_sym,
            "to_symbol":   to_sym,
            "apikey":      self._api_key,
            "outputsize":  "full",
            "datatype":    "json",
        }
        if function == "FX_INTRADAY":
            params["interval"] = _INTERVAL_MAP.get(tf, "60min")

        try:
            resp = requests.get(AV_BASE, params=params, timeout=30)

            # FIX AV-1 (CRITICAL): record_call() only AFTER confirmed HTTP 200.
            # Previously called unconditionally — 429/500 failures consumed the
            # 25/day budget, under-reporting remaining calls and wasting quota.
            if resp.status_code != 200:
                logger.warning(f"[AV] HTTP {resp.status_code} — budget NOT consumed")
                return None
            SourceLimiters.alphavantage.record_call()  # FIX AV-1: only on success

            data = resp.json()

            # AV error messages
            if "Information" in data or "Note" in data:
                logger.warning(
                    f"[AV] API message: {data.get('Information') or data.get('Note')}"
                )
                return None

            # Find the time-series key (varies by function)
            ts_key = next(
                (k for k in data if "Time Series" in k or "FX" in k),
                None
            )
            if not ts_key or ts_key not in data:
                return None

            records = []
            for dt_str, vals in data[ts_key].items():
                try:
                    obs_date = date.fromisoformat(dt_str[:10])
                    if obs_date < start or obs_date > end:
                        continue
                    records.append({
                        "timestamp": dt_str[:10],
                        # FIX AV-3 (HIGH): None when key absent — not 0.0 default.
                        # float(vals.get('1. open', 0)) produced zero-price bars that
                        # silently passed Silver null check. None is detected correctly.
                        # Consistent with POL-2 None-safe pattern in polygon_adapter.
                        "open":  float(vals["1. open"])  if "1. open"  in vals else None,
                        "high":  float(vals["2. high"])  if "2. high"  in vals else None,
                        "low":   float(vals["3. low"])   if "3. low"   in vals else None,
                        "close": float(vals["4. close"]) if "4. close" in vals else None,
                        # FIX AV-3: explicit None for volume — AV FX has no volume data.
                        # Previous value of 0 could be misinterpreted as zero trading volume.
                        "volume": None,
                    })
                except (ValueError, KeyError):
                    pass

            if not records:
                return None

            df = pl.DataFrame(records).sort("timestamp")
            logger.info(
                f"[AV] {symbol}/{tf}: {len(df)} bars"
                f" (budget remaining: {SourceLimiters.alphavantage.remaining})"
            )
            return df

        except Exception as e:
            logger.warning(f"[AV] {symbol}/{tf}: {e}")
            return None

    @staticmethod
    def _parse_pair(symbol: str) -> tuple[str, str]:
        """Parse forex pair into (from_symbol, to_symbol) for AV API.

        FIX AV-2 (HIGH): DXY cannot be proxied via USD/EUR alone.
        DXY basket: EUR 57.6%, JPY 13.6%, GBP 11.9%, CAD 9.1%, SEK 4.2%, CHF 3.6%.
        USD/EUR proxy captures only 57.6% of DXY movement — materially wrong
        during JPY/GBP divergence periods (e.g. 2022-2023 USD/JPY +30%).
        AV does not have a native DXY endpoint.
        Return (None, None) to signal caller to skip AV and use yfinance (DX-Y.NYB).
        """
        # Handle various formats: EUR/USD, EURUSD, EUR_USD
        clean = symbol.replace("/", "").replace("_", "").replace("=X", "")

        if clean.upper() == "DXY":
            # FIX AV-2: return None instead of misleading USD/EUR proxy
            logger.info(
                "[AV] DXY requested — AV has no DXY endpoint and USD/EUR proxy "
                "is materially inaccurate (57.6% basket coverage only). "
                "Use yfinance 'DX-Y.NYB' as primary DXY source."
            )
            return "", ""

        if len(clean) == 6:
            return clean[:3].upper(), clean[3:].upper()

        logger.warning(f"[AV] Cannot parse pair: {symbol!r}")
        return "", ""
