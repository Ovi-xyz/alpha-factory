"""
inc_fetch.py — G1 Supplementary Design v1.1 (BLOCKING — 3 Bugs Fixed)
Incremental Fetch Protocol: menentukan start_date untuk incremental Bronze OHLCV.

Fixes applied dari v1.0:
  FIX #1: source param sekarang digunakan untuk scope Hive partition scan
  FIX #2: run_date ditambahkan sebagai parameter wajib (menggantikan date.today())
  FIX #3: Fallback path: scan gabungan semua source jika source-specific path kosong

PENTING:
  - resolve_start_date() HARUS dipanggil dengan run_date eksplisit
  - Gunakan ini di setiap Bronze OHLCV ingester — jangan date.today() langsung
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import polars as pl
from loguru import logger


# ── Fallback Years per Timeframe ──────────────────────────────────────────────
# Sesuai GD Section 6.4 — Realistic Data Availability per Source

FALLBACK_YEARS: dict[str, float] = {
    "5m":  0.2,   # ~60 hari — accumulate forward, no historical intraday
    "15m": 0.5,   # ~180 hari — yfinance limit ~730 hari
    "1H":  2,     # 2 tahun — realistic ceiling untuk 1H di free tier.
    # FIX (chat thread, 31 Aug 2026 live-test finding): this value is now
    # SUPERSEDED for actual fetch resolution by FALLBACK_DAYS["1H"] below —
    # kept here only as a human-readable "~2 years" label / fallback for
    # any caller that doesn't also pass fallback_days. int(365*2)=730 lands
    # EXACTLY on yfinance's real "must be within the last 730 days" ceiling
    # for 1H intraday; live-test evidence (2026-08-31, idx timeframe=1H)
    # showed 28/29 symbols failing on cold-start with exactly that Yahoo
    # error — only the first request squeaked through on sub-second request
    # timing, not because 730 is actually safe. A years*365 value is also
    # leap-year-sensitive across different run_dates (int(365*2) is 730 or
    # 731 depending on which Feb 29 falls inside the window) — see
    # FALLBACK_DAYS for the fixed, run_date-independent replacement.
    # "4H" DIHAPUS v1.5: Bronze tidak lagi menyimpan 4H. Silver mensintesis
    #                     4H dari Silver 1H. Tidak ada Bronze 4H fetch.
    "1D":  10,    # 10 tahun — target utama sesuai GD Section 6.4
    "1W":  15,    # FIX v1.1: 15 tahun buffer (bukan 10) untuk 1W/1M — lebih aman
    "1M":  15,
}

# FIX (chat thread, 31 Aug 2026 live-test finding): explicit day-count
# override for timeframes where a precise, run_date-independent boundary
# matters more than a human-readable "N years" label. Takes precedence
# over FALLBACK_YEARS at the resolve_start_date() call site (see
# fallback_days parameter below) — sidesteps both the exact-730-day
# collision AND the leap-year drift inherent to int(365 * fallback_years).
# 720 days = 10-day safety margin under yfinance's confirmed 730-day wall
# for 1H intraday history, verified via live-test log evidence, not the
# prior "verified, not assumed" claim from ADR-046 Path C's implementation
# (pyproject.toml v1.17.1), which this finding contradicts.
FALLBACK_DAYS: dict[str, int] = {
    "1H": 720,
}


class IncFetchProtocol:
    """
    Menentukan start_date untuk incremental fetch Bronze OHLCV.
    Compose — tidak inherit langsung. Gunakan sebagai mixin atau instance.

    Idempotency guarantee:
        resolve_start_date() dengan run_date yang sama akan return start_date yang sama.
        Tidak pernah bergantung pada date.today() — reproducible per run_date.
    """

    DEFAULT_LOOKBACK_DAYS: int = 7  # Overlap 7 hari untuk fill gaps

    def resolve_start_date(
        self,
        bronze_path: Path,
        symbol: str,
        source: str,
        run_date: date,                 # FIX v1.1: wajib — menggantikan date.today()
        fallback_years: float = 10,
        fallback_days: Optional[int] = None,   # FIX (chat thread, 31 Aug 2026)
    ) -> date:
        """
        Return start_date yang tepat untuk incremental fetch.

        Logic:
          - Jika ada data sebelumnya: last_date - DEFAULT_LOOKBACK_DAYS (7 hari overlap)
          - Jika belum ada data: run_date - fallback_days (jika diisi),
            else run_date - fallback_years * 365

        Args:
            bronze_path:    Base path Bronze layer (e.g. Path('data/bronze/market/ohlcv'))
            symbol:         Normalized symbol (e.g. 'AAPL', 'EUR_USD')
            source:         Data source string (e.g. 'yfinance') — FIX: digunakan untuk scope
            run_date:       Date saat pipeline dijalankan — untuk reproducibility
            fallback_years: Berapa tahun ke belakang jika belum ada data
            fallback_days:  FIX (chat thread, 31 Aug 2026 live-test finding):
                            jika diberikan, dipakai APA ADANYA sebagai jumlah
                            hari — mengabaikan fallback_years sepenuhnya.
                            Untuk kasus di mana batas presisi penting (mis.
                            batas keras provider seperti yfinance 1H's real
                            "within the last 730 days" ceiling) dan
                            int(365 * fallback_years) rentan terhadap
                            leap-year drift antar run_date yang berbeda-beda
                            (bisa jadi 730 ATAU 731 tergantung run_date) —
                            lihat FALLBACK_DAYS di module-level.

        Returns:
            date object: start date untuk fetch ke API
        """
        last_date = self._scan_last_date(bronze_path, symbol, source)

        if last_date is not None:
            start = last_date - timedelta(days=self.DEFAULT_LOOKBACK_DAYS)
            logger.info(
                f"[IncFetch] {symbol}/{source}"
                f" | last={last_date} | fetch from {start}"
            )
            return start

        # FIX v1.1: pakai run_date bukan date.today() agar reproducible
        # FIX (chat thread, 31 Aug 2026): fallback_days (jika ada) menang
        # atas fallback_years — day-count eksplisit, bukan hasil kali 365
        # yang rentan leap-year drift.
        if fallback_days is not None:
            fallback = run_date - timedelta(days=fallback_days)
        else:
            fallback = run_date - timedelta(days=int(365 * fallback_years))
        logger.info(
            f"[IncFetch] {symbol}/{source}"
            f" | no prior data | fetch from {fallback}"
        )
        return fallback

    def _scan_last_date(
        self,
        bronze_path: Path,
        symbol: str,
        source: str,
    ) -> Optional[date]:
        """
        Scan Bronze Hive partition untuk menemukan last timestamp.

        FIX v1.1: source digunakan untuk scope scan ke source-specific partition.
        Fallback: scan semua source jika source-specific path kosong.
        """
        # FIX v1.1: Primary path scoped ke source partition
        primary_pattern = str(
            bronze_path / f"source={source}" / f"symbol={symbol}" / "**" / "*.parquet"
        )
        # FIX INC-1 (MEDIUM): secondary scan is now source-filtered via _source column.
        # Previous secondary_pattern scanned ALL sources at symbol={symbol} level — if
        # multiple sources existed (yfinance T-3, polygon T-1), MAX returned T-1
        # (polygon date) for a yfinance fetch, causing a missed gap [T-10, T-8].
        # Fix: after scanning the symbol-level path, filter rows by _source == source
        # to ensure only data from the requested source affects the last_date result.
        # This makes the secondary scan effectively source-scoped without requiring
        # the source= Hive partition (backward-compat for pre-GD-F08 Bronze).
        secondary_pattern = str(
            bronze_path / f"symbol={symbol}" / "**" / "*.parquet"
        )

        for idx, pattern in enumerate([primary_pattern, secondary_pattern]):
            try:
                df = pl.scan_parquet(pattern, hive_partitioning=True)
                # FIX INC-1: on secondary (non-Hive) path, filter by _source column
                # to prevent cross-source last_date contamination.
                if idx == 1:
                    schema = df.collect_schema()
                    if "_source" in schema:
                        df = df.filter(pl.col("_source") == source)
                # Cari kolom timestamp — berbagai nama yang mungkin
                ts_col = self._find_timestamp_col(df)
                if ts_col is None:
                    continue

                last_ts = (
                    df.select(pl.col(ts_col).max()).collect()[0, 0]
                )
                if last_ts is not None:
                    # Convert ke date jika datetime
                    if hasattr(last_ts, "date"):
                        return last_ts.date()
                    if isinstance(last_ts, date):
                        return last_ts
            except Exception:
                # Pattern tidak match atau file tidak ada — lanjut ke fallback
                continue

        logger.debug(
            f"[IncFetch] No existing data for {symbol}/{source} — using fallback"
        )
        return None

    @staticmethod
    def _find_timestamp_col(df: pl.LazyFrame) -> Optional[str]:
        """Return nama kolom timestamp dari schema. Coba beberapa nama umum."""
        schema = df.collect_schema()
        for candidate in ["timestamp", "datetime", "date", "Date", "Datetime"]:
            if candidate in schema:
                return candidate
        return None
