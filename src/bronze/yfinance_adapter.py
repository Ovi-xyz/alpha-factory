"""
yfinance_adapter.py — Bronze yfinance SourceAdapter (GD §3.5)
Isolated SourceAdapter implementation for yfinance.

Primary source for:
    US Stocks  — free tier, ~2000 req/hr
    Forex      — primary, with ForexDayCache fallback
    Commodity  — primary (=F suffix)
    IDX        — SOLE source (.JK suffix) since ADR-029 (GMI_Decision_
                 Document_v7.docx, 30 Jul 2026) -- tvdatafeed retired
                 entirely. See KNOWN_RISKS.md RISK-1 (RESOLVED).
    Index      — ^GSPC, ^VIX etc.

Rate limit managed via SourceLimiters.yfinance (100/min conservative).

Usage (via ChainedAdapter):
    us_chain  = ChainedAdapter([YFinanceAdapter(), PolygonAdapter()])
    idx_chain = ChainedAdapter([YFinanceJKAdapter()])  # ADR-029: single-source
    fx_chain  = ChainedAdapter([YFinanceForexAdapter(), ForexDayCacheAdapter()])
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import polars as pl
from loguru import logger

from src.bronze.source_adapter import SourceAdapter
from src.utils.rate_limiter import SourceLimiters
from src.utils.symbol_utils import YFINANCE_SUFFIX, YFINANCE_INDEX_MAP

# yfinance interval mapping
_INTERVAL_MAP: dict[str, str] = {
    "5m":  "5m",
    "15m": "15m",
    "1H":  "1h",
    # FIX YF-1 (HIGH): '4H' entry REMOVED after v1.5 refactoring.
    # Bronze no longer fetches 4H (GD §3.1, §17.7).
    # Silver synthesizes 4H from clean Silver 1H via OHLCVProcessor.synthesize_4h().
    # Keeping '4H':'1h' was a silent mislabeling risk: if accidentally called,
    # the adapter would fetch 1H data but label it as 4H with no error raised.
    "1D":  "1d",
    "1W":  "1wk",
    "1M":  "1mo",
}


def _drop_trailing_null_ohlc(df: pl.DataFrame) -> pl.DataFrame:
    """
    FIX (chat thread, 31 Aug 2026 live-test finding): drop trailing rows
    where open/high/low/close are ALL null.

    market_ingester.py passes end=run_date to Ticker.history(). When the
    fetch executes before the session for run_date has actually occurred
    (e.g. an early-WIB-morning Bronze run, where run_date's local calendar
    day has started but its NYSE/IDX session — hours later in UTC — has
    not), yfinance can return a trailing placeholder row for that
    not-yet-traded day with null OHLC, appended after otherwise-valid
    history. config/schemas/yfinance_ohlcv.yaml marks OHLC not-nullable,
    so SchemaValidator would quarantine the ENTIRE DataFrame over this one
    artifact row — discarding legitimate history too, for effectively
    every 1D yfinance fetch run in that window (confirmed: AAPL and many
    other us_stocks/context symbols, live test 2026-08-31).

    Only TRAILING rows are dropped, and only while every OHLC column in
    that row is null — a null row in the middle of the series is a
    genuine data-quality issue and must still fail validation, not be
    silently stripped.
    """
    ohlc_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]
    if not ohlc_cols or df.is_empty():
        return df

    null_mask = None
    for c in ohlc_cols:
        m = df[c].is_null()
        null_mask = m if null_mask is None else (null_mask & m)
    mask_list = null_mask.to_list()

    end = len(mask_list)
    for i in range(end - 1, -1, -1):
        if mask_list[i]:
            end = i
        else:
            break
    return df.slice(0, end) if end < len(mask_list) else df


def _normalize_df(df) -> Optional[pl.DataFrame]:
    """Normalize pandas DataFrame from yfinance to standard Bronze schema."""
    if df is None or df.empty:
        return None
    df = df.reset_index()
    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
    rename = {"date": "timestamp", "datetime": "timestamp",
               "adj_close": "adj_close"}
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    keep = ["timestamp", "open", "high", "low", "close", "volume"]
    keep = [c for c in keep if c in df.columns]
    result = pl.from_pandas(df[keep])
    # FIX (chat thread, 31 Aug 2026): strip trailing not-yet-traded
    # placeholder row before it ever reaches SchemaValidator.
    result = _drop_trailing_null_ohlc(result)
    return result if len(result) > 0 else None


class YFinanceAdapter(SourceAdapter):
    """
    General-purpose yfinance adapter.
    Handles US stocks, indices, commodities.
    api_symbol must already be in yfinance format (e.g. 'AAPL', 'GC=F', '^GSPC').
    """

    @property
    def name(self) -> str:
        return "yfinance"

    def fetch(
        self,
        symbol: str,
        tf: str,
        start: date,
        end: date,
    ) -> Optional[pl.DataFrame]:
        try:
            import yfinance as yf
            SourceLimiters.yfinance.wait()

            # FIX YF-1 (HIGH): Explicit guard for 4H — raises ValueError so callers
            # get a clear diagnostic instead of silent mislabeling (1H data labeled as 4H).
            # Bronze does not fetch 4H after v1.5 refactoring (GD §3.1, §17.7).
            # 4H is synthesized in Silver from Silver 1H via OHLCVProcessor.synthesize_4h().
            if tf == "4H":
                raise ValueError(
                    "4H Bronze fetch is disabled since v1.5 refactoring (GD §3.1, §17.7). "
                    "Synthesize 4H from Silver 1H via OHLCVProcessor.synthesize_4h()."
                )

            interval = _INTERVAL_MAP.get(tf)
            if interval is None:
                logger.warning(f"[yfinance] Unsupported TF={tf}")
                return None

            ticker = yf.Ticker(symbol)
            hist   = ticker.history(
                start=start.isoformat(),
                end=end.isoformat(),
                interval=interval,
                auto_adjust=True,
            )
            return _normalize_df(hist)

        except Exception as e:
            logger.warning(f"[yfinance] {symbol}/{tf}: {e}")
            return None


class YFinanceForexAdapter(SourceAdapter):
    """
    yfinance adapter specialized for Forex pairs.
    Converts raw pair format (EUR/USD) to yfinance API format (EURUSD=X).
    """

    @property
    def name(self) -> str:
        return "yfinance_forex"

    def fetch(
        self,
        symbol: str,
        tf: str,
        start: date,
        end: date,
    ) -> Optional[pl.DataFrame]:
        # symbol can be raw ('EUR/USD') or normalized ('EUR_USD') or api-ready ('EURUSD=X')
        api_sym = self._to_yf_symbol(symbol)
        return YFinanceAdapter().fetch(api_sym, tf, start, end)

    @staticmethod
    def _to_yf_symbol(raw: str) -> str:
        if raw == "DXY":
            return YFINANCE_INDEX_MAP["DXY"]
        clean = raw.replace("/", "").replace("_", "")
        if not clean.endswith("=X"):
            clean += "=X"
        return clean


class YFinanceJKAdapter(SourceAdapter):
    """
    yfinance .JK adapter for IDX stocks.
    FIX ADR-029 (GMI_Decision_Document_v7.docx, 30 Jul 2026): docstring updated --
    this is now IDX30's SOLE source (tvdatafeed retired), not a fallback.
    Appends .JK suffix if not already present.
    """

    @property
    def name(self) -> str:
        return "yfinance_jk"

    def fetch(
        self,
        symbol: str,
        tf: str,
        start: date,
        end: date,
    ) -> Optional[pl.DataFrame]:
        api_sym = f"{symbol}.JK" if not symbol.endswith(".JK") else symbol
        return YFinanceAdapter().fetch(api_sym, tf, start, end)


class ForexDayCacheAdapter(SourceAdapter):
    """
    G4: ForexDayCache as a SourceAdapter for ChainedAdapter.
    Used as Fallback 1 for Forex when yfinance fails.
    """

    @property
    def name(self) -> str:
        return "forex_day_cache"

    def fetch(
        self,
        symbol: str,
        tf: str,
        start: date,
        end: date,
    ) -> Optional[pl.DataFrame]:
        if tf not in ("1D", "1W", "1M"):
            return None   # Cache only for daily+ granularity
        from src.bronze.forex_cache import ForexDayCache
        cache = ForexDayCache()
        return cache.load(symbol, end)
