"""
forex_cache.py — G4 Supplementary Design v1.1 (NEW)
ForexDayCache: file-based 24h stale cache sebagai Fallback 1 untuk Forex.

Digunakan HANYA jika yfinance gagal DAN AV budget habis.
Bar dari cache di-flag staleness=True agar Silver OHLCVProcessor
set is_clean=False untuk bar tersebut.

Source Priority Matrix (Forex):
    Primary:    yfinance
    Fallback 1: ForexDayCache (file ini)
    Fallback 2: AlphaVantage (hanya DXY jika yfinance gagal)
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import polars as pl
from src.utils.atomic_io import atomic_write_parquet  # FIX BRZ-AIO-001
from loguru import logger


class ForexDayCache:
    """
    File-based 24h stale cache untuk Forex fallback.

    Save: simpan bar hari ini dengan staleness=False
    Load: ambil bar kemarin, set staleness=True
    """

    CACHE_PATH: Path = Path("data/bronze/forex_cache")
    MAX_AGE_DAYS: int = 1

    def save(self, symbol: str, df: pl.DataFrame, run_date: date) -> None:
        """Simpan bar ke cache dengan staleness=False (fresh data)."""
        self.CACHE_PATH.mkdir(parents=True, exist_ok=True)
        path = self.CACHE_PATH / f"{symbol}_{run_date.isoformat()}.parquet"

        # FIX BRZ-AIO-001: atomic write — Bronze snappy (GD §7.1)
        atomic_write_parquet(
            df.with_columns(pl.lit(False).alias("staleness")),
            path,
            compression="snappy", compression_level=None,
            row_group_size=100_000, statistics=False, use_pyarrow=False,
        )
        logger.debug(f"[ForexCache] Saved {symbol} for {run_date}")

    def load(self, symbol: str, run_date: date) -> Optional[pl.DataFrame]:
        """
        Return cached bar dari hari sebelumnya dengan staleness=True.
        Return None jika cache tidak ada atau terlalu lama.
        """
        yesterday = run_date - timedelta(days=1)
        path = self.CACHE_PATH / f"{symbol}_{yesterday.isoformat()}.parquet"

        if not path.exists():
            logger.warning(
                f"[ForexCache] No cache for {symbol} on {yesterday}"
            )
            return None

        df = pl.read_parquet(path)
        logger.warning(
            f"[ForexCache] Using stale data for {symbol}"
            " — is_clean will be False in Silver"
        )
        # staleness=True → Silver OHLCVProcessor akan set is_clean=False
        return df.with_columns(pl.lit(True).alias("staleness"))

    def is_stale_too_old(self, run_date: date) -> bool:
        """Return True jika tidak ada cache dalam MAX_AGE_DAYS terakhir."""
        target_date = run_date - timedelta(days=self.MAX_AGE_DAYS)
        parquet_files = list(self.CACHE_PATH.glob("*.parquet"))
        return not any(
            target_date.isoformat() in f.name for f in parquet_files
        )

    def cleanup_old_cache(self, keep_days: int = 7, run_date: Optional[date] = None) -> int:
        """
        Hapus cache files yang lebih lama dari keep_days.
        Return jumlah files yang dihapus.

        FIX FC-1 (LOW): run_date parameter untuk reproducibility.
        Previously used date.today() — during backfill runs (run_date=T-30),
        cleanup deleted files based on actual wall-clock date rather than pipeline
        run_date, violating the reproducibility principle from G1 Supplementary.
        Now: ref_date = run_date if provided, else date.today() (backward-compat).
        """
        # FIX FC-1: use run_date as reference if provided — not wall-clock date.today()
        ref_date = run_date if run_date is not None else date.today()
        cutoff = ref_date - timedelta(days=keep_days)
        removed = 0

        for f in self.CACHE_PATH.glob("*.parquet"):
            # Parse date dari filename: {symbol}_{YYYY-MM-DD}.parquet
            parts = f.stem.split("_")
            if len(parts) >= 2:
                try:
                    file_date = date.fromisoformat(parts[-1])
                    if file_date < cutoff:
                        f.unlink()
                        removed += 1
                except ValueError:
                    pass

        if removed:
            logger.info(f"[ForexCache] Cleaned up {removed} old cache files")
        return removed
