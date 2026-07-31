"""
source_adapter.py — GD §3.5 (Source Abstraction Layer)
SourceAdapter pattern + ChainedAdapter untuk transparent fallback.

Prinsip Layer Independence:
  - Adapter tidak tahu tentang Silver/Gold schema
  - Adapter hanya bertanggung jawab untuk fetch raw data
  - ChainedAdapter: coba primary, fallback ke berikutnya jika gagal

Usage:
    # FIX ADR-029 (GMI_Decision_Document_v7.docx, 30 Jul 2026): tvdatafeed
    # retired entirely -- yfinance .JK is IDX30's sole source now, not a
    # fallback. See KNOWN_RISKS.md RISK-1 (RESOLVED).
    idx_chain = ChainedAdapter([YFinanceJKAdapter()])
    df = idx_chain.fetch("BBCA", "1D", start_date, end_date)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional

import polars as pl
from loguru import logger


# ── Abstract Base ─────────────────────────────────────────────────────────────

class SourceAdapter(ABC):
    """
    Abstract base untuk semua source adapters.
    Setiap data source (yfinance, Polygon, tvdatafeed, dll) mempunyai
    satu concrete subclass.
    """

    @abstractmethod
    def fetch(
        self,
        symbol: str,
        tf: str,
        start: date,
        end: date,
    ) -> Optional[pl.DataFrame]:
        """
        Fetch OHLCV data untuk satu symbol.

        Args:
            symbol: API-ready symbol (output dari to_api_symbol())
            tf:     Timeframe string ('1D', '1H', '15m', dll)
            start:  Start date (inclusive)
            end:    End date (inclusive)

        Returns:
            pl.DataFrame dengan minimal columns: timestamp, open, high, low, close, volume
            None jika fetch gagal.
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Source identifier string — dipakai di _source metadata column."""
        ...


# ── Chained Adapter ───────────────────────────────────────────────────────────

class ChainedAdapter(SourceAdapter):
    """
    Coba adapter dalam urutan — return hasil dari adapter pertama yang sukses.
    Setiap adapter yang sukses menambahkan kolom '_source' ke DataFrame.

    Pattern:
        idx_chain = ChainedAdapter([YFinanceJKAdapter()])  # ADR-029: single-source
        fx_chain  = ChainedAdapter([YFinanceForexAdapter(), ForexDayCacheAdapter()])
    """

    def __init__(self, adapters: list[SourceAdapter]) -> None:
        if not adapters:
            raise ValueError("ChainedAdapter membutuhkan minimal 1 adapter")
        self._adapters = adapters

    @property
    def name(self) -> str:
        return f"chained({'+'.join(a.name for a in self._adapters)})"

    def fetch(
        self,
        symbol: str,
        tf: str,
        start: date,
        end: date,
    ) -> Optional[pl.DataFrame]:
        """
        Coba setiap adapter berurutan.
        Return DataFrame dari adapter pertama yang return non-None non-empty.
        """
        for adapter in self._adapters:
            try:
                df = adapter.fetch(symbol, tf, start, end)
                if df is not None and len(df) > 0:
                    logger.debug(
                        f"[ChainedAdapter] {symbol}/{tf} → fetched via {adapter.name}"
                    )
                    # Annotate source — downstream bisa track mana data dari mana
                    return df.with_columns(
                        pl.lit(adapter.name).alias("_source")
                    )
                else:
                    logger.debug(
                        f"[ChainedAdapter] {symbol}/{tf} → {adapter.name}"
                        " returned empty, trying next"
                    )
            except Exception as e:
                logger.warning(
                    f"[ChainedAdapter] {symbol}/{tf} → {adapter.name}"
                    f" raised exception: {e}"
                )
                continue

        logger.error(
            f"[ChainedAdapter] All adapters failed for {symbol}/{tf}"
            f" — adapters tried: {[a.name for a in self._adapters]}"
        )
        return None


# FIX SA-1 (HIGH): DailyBudgetLimiter class and AV_LIMITER REMOVED.
#
# The canonical, thread-safe implementation lives in src/utils/rate_limiter.py
# (uses threading.Lock for concurrent safety). Having two implementations with
# different thread-safety guarantees creates a maintenance risk — developers
# could accidentally import the non-thread-safe version here.
#
# AV_LIMITER was dead code: alphavantage_adapter.py already imports
# SourceLimiters.alphavantage from rate_limiter.py (verified VERIFIED in v1.6).
#
# If a consumer inside this file needs budget limiting, import from rate_limiter:
#     from src.utils.rate_limiter import SourceLimiters
#     if SourceLimiters.alphavantage.can_call(): ...
