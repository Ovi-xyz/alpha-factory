"""
base_ingester.py — GD §3.6 (Bronze Base Ingester)
Base class untuk semua Bronze ingesters.

Responsibilities (exclusively):
  - Write Parquet ke Bronze Hive directory structure
  - Tambahkan metadata columns: _source, _ingested_at, _symbol
  - Snappy compression (prioritas write speed)

Anti-patterns yang DILARANG di BronzeIngester:
  - Membaca Silver/Gold data
  - Melakukan transformasi bisnis (join, derived columns)
  - Membuat keputusan tentang data quality downstream
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl
from src.utils.atomic_io import atomic_write_parquet  # FIX BRZ-AIO-001
from loguru import logger


class BronzeIngester(ABC):
    """
    Abstract base class untuk semua Bronze ingesters.
    Setiap data source mempunyai concrete subclass.

    Directory structure (Hive partition):
        data/bronze/{asset_class}/{source}/symbol={symbol}/year={Y}/month={M}/
    """

    BASE_PATH: Path = Path("data/bronze")

    def write(
        self,
        df: pl.DataFrame,
        source: str,
        asset_class: str,
        symbol: str,
        extra_metadata: Optional[dict] = None,
    ) -> Optional[Path]:
        """
        Write DataFrame ke Bronze Hive partition dengan idempotency check.

        Args:
            df:             DataFrame dengan raw source data
            source:         Source identifier (e.g. 'yfinance', 'polygon')
            asset_class:    Asset class path (e.g. 'market/ohlcv/us_stocks')
            symbol:         Normalized symbol (Hive partition key)
            extra_metadata: Optional extra literal columns to add

        Returns:
            Path ke file yang ditulis, atau None jika sudah ada (idempotent skip).

        FIX GD-F08: GD §3.1 menyebutkan idempotency sebagai Bronze design principle.
        Sebelum write, cek apakah file untuk hari yang sama sudah ada di partisi ini.
        Jika sudah ada, skip write dan return None — mencegah duplikasi Bronze rows.
        Filename sudah include timestamp detik (%H%M%S) sehingga setiap run baru
        memiliki nama unik, tapi date-based check (YYYYMMDD) mencegah multi-run
        pada hari yang sama dari menulis file berulang.
        """
        now = datetime.utcnow()
        path = (
            self.BASE_PATH
            / asset_class
            / f"source={source}"
            / f"symbol={symbol}"
            / f"year={now.year}"
            / f"month={now.month:02d}"
        )
        path.mkdir(parents=True, exist_ok=True)

        # FIX GD-F08: idempotency check — skip jika file untuk hari ini sudah ada
        date_prefix = now.strftime("%Y%m%d")
        existing_today = list(path.glob(f"{symbol}_raw_{date_prefix}*.parquet"))
        if existing_today:
            logger.debug(
                f"[Bronze] Idempotent skip — {len(existing_today)} file(s) already exist "
                f"for {symbol}/{source} on {date_prefix}. "
                f"Use --reset or clear sentinel to force re-ingest."
            )
            return None

        fname = path / f"{symbol}_raw_{now.strftime('%Y%m%d_%H%M%S')}.parquet"

        # Add standard metadata columns (Bronze audit trail)
        metadata_cols = [
            pl.lit(source).alias("_source"),
            pl.lit(now.isoformat()).alias("_ingested_at"),
            pl.lit(symbol).alias("_symbol"),
        ]

        # Add optional _tz_hint if present in extra_metadata (G3 Bronze extension)
        if extra_metadata:
            for key, val in extra_metadata.items():
                metadata_cols.append(pl.lit(val).alias(key))

        df = df.with_columns(metadata_cols)

        # FIX BRZ-AIO-001: atomic write — partial Bronze file corrupts IncFetchProtocol
        # Bronze uses snappy (GD §7.1 — write speed priority); no compression_level
        atomic_write_parquet(
            df, fname,
            compression="snappy", compression_level=None,
            row_group_size=100_000, statistics=False, use_pyarrow=False,
        )
        logger.debug(
            f"[Bronze] Wrote {len(df):,} rows → {fname.relative_to(self.BASE_PATH)}"
        )
        return fname

    def write_macro(
        self,
        df: pl.DataFrame,
        source: str,
        domain: str,
        series_id: str,
    ) -> Optional[Path]:
        """
        Write macro data dengan different Hive structure (no symbol partition).
        data/bronze/macro/{source}/{domain}/

        FIX BI-1 (MEDIUM): add date-based idempotency check, matching write().
        Without this check, every run (even re-runs on the same day) writes a
        new file: 60 FRED series × 52 weeks/year = 3,120 files/year accumulate
        in Bronze. Silver MacroProcessor must scan all files and dedup — growing
        O(n_files × n_rows) cost with no quality benefit.

        Check: if any file for this series_id exists with today's date prefix,
        skip writing. Each series should be written at most once per run_date.

        Returns:
            Path to the file written, or None if skipped (idempotent).
        """
        now = datetime.utcnow()
        path = self.BASE_PATH / "macro" / source / domain
        path.mkdir(parents=True, exist_ok=True)

        # FIX BI-1: idempotency check — same logic as write()
        date_prefix   = now.strftime("%Y%m%d")
        existing_today = list(path.glob(f"{series_id}_{date_prefix}*.parquet"))
        if existing_today:
            logger.debug(
                f"[Bronze] Macro idempotent skip — {series_id} already written "
                f"for {date_prefix} ({len(existing_today)} file(s) exist). "
                f"Use --reset to force re-ingest."
            )
            return None

        fname = path / f"{series_id}_{now.strftime('%Y%m%d_%H%M%S')}.parquet"

        df = df.with_columns([
            pl.lit(source).alias("_source"),
            pl.lit(now.isoformat()).alias("_ingested_at"),
            pl.lit(series_id).alias("_series_id"),
        ])

        # FIX BRZ-AIO-001: atomic write — macro Bronze (snappy, GD §7.1)
        atomic_write_parquet(
            df, fname,
            compression="snappy", compression_level=None,
            row_group_size=100_000, statistics=False, use_pyarrow=False,
        )
        logger.debug(
            f"[Bronze] Macro: {len(df):,} rows → {fname.relative_to(self.BASE_PATH)}"
        )
        return fname
