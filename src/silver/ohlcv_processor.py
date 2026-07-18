"""
ohlcv_processor.py — GD §4.3 + §4.4 (Silver OHLCV Processor)
Clean + enrich Bronze OHLCV → Silver OHLCV.

Fixes v1.2:
  CRITICAL FIX: VWAP menggunakan typical price (H+L+C)/3 — bukan close saja
  NEW: is_adjusted + adj_factor columns untuk backtest integrity

Silver OHLCV Schema (GD §4.3):
    symbol, timestamp, timeframe, open, high, low, close, volume,
    is_adjusted, adj_factor, vwap, log_return, dollar_volume,
    spread_hl, is_clean, data_source, processing_version

Processing Steps:
    1. Read Bronze OHLCV Parquet (hive_partitioning)
    2. Normalize timestamps → UTC
    3. Deduplicate (symbol, timestamp, timeframe)
    4. Calculate derived fields (log_return, VWAP, dollar_volume, spread_hl)
    5. Flag is_clean via quality checks
    6. Write Silver Parquet (zstd, row_group=50k)
"""

from __future__ import annotations

import math
from datetime import date
from pathlib import Path
from typing import Optional

import polars as pl
from src.utils.atomic_io import atomic_write_parquet  # FIX SIL-AIO-001
from loguru import logger

CURRENT_SILVER_VERSION = "1.2"

SILVER_OHLCV_PATH  = Path("data/silver/market_ohlcv")
BRONZE_OHLCV_PATH  = Path("data/bronze/market/ohlcv")

# v1.5 refactoring: 4H dipindah ke Silver layer (GD §4.1 Enrichment)
# Bronze menyediakan raw data untuk TF berikut (yfinance/Polygon menyediakan langsung):
BRONZE_TIMEFRAMES = ["5m", "15m", "1H", "1D", "1W", "1M"]
# Silver mensintesis TF berikut dari Silver raw TFs (bukan dari Bronze langsung):
SYNTHETIC_TIMEFRAMES = ["4H"]   # Silver 4H disintesis dari Silver 1H yang sudah bersih


class OHLCVProcessor:
    """
    Clean + enrich Bronze OHLCV untuk semua asset classes.
    Output ke data/silver/market_ohlcv/{market}/symbol={symbol}/{tf}_silver.parquet
    """

    OUTLIER_ZSCORE_THRESHOLD = 4.0
    NULL_TOLERANCE = 0.001       # 0.1% max null rate

    def process_symbol(
        self,
        df: pl.DataFrame,
        symbol: str,
        market: str,
        timeframe: str,
        is_adjusted: bool = True,
        adj_factor: float = 1.0,
        tz_hint: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Process satu symbol × timeframe dari Bronze → Silver schema.

        Args:
            df:          Bronze OHLCV DataFrame
            symbol:      Normalized symbol
            market:      Asset market string
            timeframe:   TF string ('1D', '1H', etc.)
            is_adjusted: True jika yfinance auto_adjust=True (default)
            adj_factor:  Cumulative adjustment factor (1.0 = no adjustment)
            tz_hint:     Bronze _tz_hint untuk timezone conversion

        Returns:
            Silver OHLCV DataFrame dengan full schema.
        """
        if df is None or len(df) == 0:
            return pl.DataFrame()

        # FIX B-F02: capture actual_source from Bronze _source column BEFORE
        # _normalize_columns drops it. ChainedAdapter (GD §3.5) populates _source
        # with the name of the adapter that actually succeeded.
        actual_source = (
            df["_source"][0] if "_source" in df.columns else "yfinance"
        )

        df = (
            df
            .pipe(self._normalize_columns)
            # FIX OP-1 (MEDIUM): `'symbol' in df.columns` was always False after
            # _normalize_columns() because the outer df reference reflects the
            # pre-pipe schema. sort(["symbol","timestamp"]) never executed — the
            # conditional always resolved to sort(["timestamp"]).
            # process_symbol() always receives single-symbol Bronze data, so sorting
            # by ["timestamp"] alone is correct and unambiguous (S-F03 preserved).
            .sort(["timestamp"])  # FIX OP-1: removed dead conditional (was: sort S-F03)
            .pipe(self._normalize_timestamps, market=market, tz_hint=tz_hint)
            .pipe(self._deduplicate, symbol=symbol, timeframe=timeframe)
            .pipe(self._add_derived_fields, timeframe=timeframe)
            .pipe(self._add_adjustment_flags, is_adjusted=is_adjusted, adj_factor=adj_factor)
            .pipe(self._flag_is_clean)
            .pipe(self._add_metadata, symbol=symbol, timeframe=timeframe, market=market, actual_source=actual_source)
        )

        return df

    # ── Processing Steps ──────────────────────────────────────────────────────

    @staticmethod
    def _normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
        """Standardize column names, drop Bronze audit metadata.

        FIX B-F02: _source kolom di-capture ke _actual_source sebelum di-drop
        agar data_source di Silver mencerminkan adapter yang benar-benar sukses.
        FIX S-F04: staleness TIDAK di-drop di sini — dibutuhkan oleh
        _flag_is_clean(). Drop dilakukan di _add_metadata() setelah is_clean set.
        """
        # FIX B-F02: simpan actual source sebelum drop Bronze metadata
        # OHLCVProcessor menyimpannya sebagai instance attribute sementara
        # (non-concurrent usage — satu call per symbol/TF)
        # NOTE: staticmethod tidak bisa set self — handled via _process_symbol wrapper

        # FIX S-F04: HANYA drop Bronze audit metadata — staleness TETAP ADA
        # staleness dibutuhkan oleh _flag_is_clean() untuk stale forex detection
        bronze_meta = ["_source", "_ingested_at", "_symbol", "_tz_hint"]
        drop_cols = [c for c in bronze_meta if c in df.columns]
        if drop_cols:
            df = df.drop(drop_cols)

        # Ensure float64 for OHLCV
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        # Ensure Int64 for volume
        if "volume" in df.columns:
            df = df.with_columns(pl.col("volume").cast(pl.Int64))

        return df

    @staticmethod
    def _normalize_timestamps(
        df: pl.DataFrame,
        market: str,
        tz_hint: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Convert timestamps → UTC.
        IDX: WIB (UTC+7) → UTC
        US:  ET (America/New_York) → UTC
        Forex: sudah UTC
        """
        if "timestamp" not in df.columns:
            return df

        # Determine source timezone
        tz_source = tz_hint or {
            "us_stocks": "America/New_York",
            "idx":       "Asia/Jakarta",
            "index":     "America/New_York",
            "commodity": "America/New_York",
            "forex":     "UTC",
        }.get(market, "UTC")

        try:
            ts_col = df["timestamp"]
            # Already datetime — ensure UTC
            if ts_col.dtype in (pl.Date,):
                df = df.with_columns(
                    pl.col("timestamp")
                    .cast(pl.Datetime("us"))
                    .dt.replace_time_zone("UTC")
                )
            elif ts_col.dtype == pl.Datetime:
                if ts_col.dtype.time_zone is None:
                    df = df.with_columns(
                        pl.col("timestamp")
                        .dt.replace_time_zone(tz_source)
                        .dt.convert_time_zone("UTC")
                    )
        except Exception as e:
            logger.debug(f"[OHLCVProcessor] Timestamp normalization note: {e}")

        return df

    @staticmethod
    def _deduplicate(
        df: pl.DataFrame,
        symbol: str,
        timeframe: str,
    ) -> pl.DataFrame:
        """
        Deduplicate by (symbol, timestamp, timeframe).
        Keep last record per key (most recent ingestion wins).
        """
        if "timestamp" not in df.columns:
            return df

        before = len(df)
        df = df.unique(subset=["timestamp"], keep="last", maintain_order=True)
        dupes = before - len(df)
        if dupes > 0:
            logger.debug(
                f"[OHLCVProcessor] Deduplicated {dupes} rows for {symbol}/{timeframe}"
            )
        return df

    @staticmethod
    def _add_derived_fields(
        df: pl.DataFrame,
        timeframe: str,
    ) -> pl.DataFrame:
        """
        Calculate derived fields:
          - log_return (ln(close/prev_close))
          - dollar_volume (close * volume)
          - spread_hl ((high - low) / close)
          - vwap (CRITICAL FIX v1.2: typical price = (H+L+C)/3)
        """
        exprs = []

        # log_return — ln(close/prev_close)
        # FIX F-OP-01 [P2]: use math.e (exact) instead of 2.71828 (approximation).
        # Relative error per bar with literal: ~6.7e-7.
        # Over 2500+ 1D bars (10Y history) the error accumulates and affects
        # Sharpe Ratio, VaR, and MTF signal quality in Gold layer.
        # BEFORE: .log(base=2.71828)
        # AFTER:  .log(base=math.e)  — exact Euler's number from Python stdlib
        if all(c in df.columns for c in ["close"]):
            exprs.append(
                (pl.col("close") / pl.col("close").shift(1)).log(base=math.e)
                .alias("log_return")
            )

        # dollar_volume (G2 FIX: dihitung eksplisit di Silver)
        if all(c in df.columns for c in ["close", "volume"]):
            exprs.append(
                (pl.col("close") * pl.col("volume")).alias("dollar_volume")
            )

        # spread_hl
        if all(c in df.columns for c in ["high", "low", "close"]):
            exprs.append(
                ((pl.col("high") - pl.col("low")) / pl.col("close"))
                .alias("spread_hl")
            )

        if exprs:
            df = df.with_columns(exprs)

        # VWAP — CRITICAL FIX v1.2: typical price (H+L+C)/3, NOT close
        # Reset per session (date-level), using cumulative sum over day
        if all(c in df.columns for c in ["high", "low", "close", "volume", "timestamp"]):
            try:
                df = (
                    df
                    .with_columns([
                        pl.col("timestamp").dt.date().alias("_date"),
                        (
                            (pl.col("high") + pl.col("low") + pl.col("close")) / 3
                        ).alias("_typical_price"),
                    ])
                    .with_columns([
                        (pl.col("_typical_price") * pl.col("volume")).alias("_tp_vol")
                    ])
                    .with_columns([
                        (
                            pl.col("_tp_vol").cum_sum().over("_date")
                            / pl.col("volume").cum_sum().over("_date")
                        ).alias("vwap")
                    ])
                    .drop(["_date", "_typical_price", "_tp_vol"])
                )
            except Exception as e:
                logger.debug(f"[OHLCVProcessor] VWAP calculation note: {e}")
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("vwap"))

        return df

    @staticmethod
    def _add_adjustment_flags(
        df: pl.DataFrame,
        is_adjusted: bool,
        adj_factor: float,
    ) -> pl.DataFrame:
        """
        NEW v1.2: Add is_adjusted + adj_factor columns.
        is_adjusted=True: close sudah adjusted (yfinance default auto_adjust=True).
        adj_factor=1.0:   No split/dividend yet, or unknown factor.
        """
        return df.with_columns([
            pl.lit(is_adjusted).alias("is_adjusted"),
            pl.lit(adj_factor).cast(pl.Float64).alias("adj_factor"),
        ])

    def _flag_is_clean(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Set is_clean flag berdasarkan quality checks:
          - Price sanity: high >= low, open/close in [low, high]
          - Null check: no null OHLCV values
          - Outlier: |z-score log_return| > 4
          - Staleness flag from ForexDayCache
        """
        # Start with all True
        clean = pl.lit(True)

        # Price sanity checks
        if all(c in df.columns for c in ["high", "low", "open", "close"]):
            clean = (
                clean
                & (pl.col("high") >= pl.col("low"))
                & (pl.col("open") >= pl.col("low"))
                & (pl.col("open") <= pl.col("high"))
                & (pl.col("close") >= pl.col("low"))
                & (pl.col("close") <= pl.col("high"))
            )

        # Null check
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                clean = clean & pl.col(col).is_not_null()

        # Outlier detection: |z-score| > 4 on log_return
        if "log_return" in df.columns:
            try:
                lr = pl.col("log_return")
                mean_ = df["log_return"].mean()
                std_  = df["log_return"].std()
                if std_ and std_ > 0:
                    zscore = (lr - mean_) / std_
                    clean = clean & (zscore.abs() <= self.OUTLIER_ZSCORE_THRESHOLD)
            except Exception:
                pass  # Keep clean=True if z-score calc fails

        # Staleness flag (G4: ForexDayCache)
        if "staleness" in df.columns:
            clean = clean & (~pl.col("staleness"))

        return df.with_columns(clean.alias("is_clean"))

    @staticmethod
    def _add_metadata(
        df: pl.DataFrame,
        symbol: str,
        timeframe: str,
        market: str,
        actual_source: str = "yfinance",
    ) -> pl.DataFrame:
        """Add metadata columns required by Silver schema.

        FIX B-F02: actual_source parameter digunakan — tidak lagi hardcode 'yfinance'.
        FIX S-F04: staleness di-drop DI SINI setelah _flag_is_clean() sudah selesai.
        staleness adalah WORKING COLUMN — tidak boleh tersimpan di Silver per GD §4.3.
        """
        # FIX S-F04: drop staleness SETELAH _flag_is_clean() — SEBELUM write ke Parquet
        # staleness bukan bagian dari GD §4.3 Silver OHLCV schema
        if "staleness" in df.columns:
            df = df.drop("staleness")

        return df.with_columns([
            pl.lit(symbol).alias("symbol"),
            pl.lit(timeframe).alias("timeframe"),
            pl.lit(actual_source).alias("data_source"),  # FIX B-F02: actual source
            pl.lit(CURRENT_SILVER_VERSION).alias("processing_version"),
        ])

    # ── 4H Synthesis (v1.5 refactoring) ──────────────────────────────────────

    def synthesize_4h(
        self,
        silver_1h_df: pl.DataFrame,
        symbol: str,
        market: str,
        is_adjusted: bool = True,
        adj_factor: float = 1.0,
    ) -> pl.DataFrame:
        """
        Sintesis Silver 4H dari Silver 1H yang sudah bersih (GD §4.1 Enrichment).

        Dipindahkan dari Bronze layer ke Silver (v1.5 refactoring).
        GD §17.7 Anti-Pattern: Bronze tidak boleh melakukan transformasi bisnis.
        4H synthetic bukan raw source data — tanggung jawab Silver.

        Input: output dari process_symbol(..., timeframe='1H') — Silver schema.
               Silver 1H dipilih sebagai sumber karena:
               - Sudah UTC-normalized → ohlcv_aggregator block arithmetic benar
               - Sudah null-handled & outlier-flagged → 4H bar bebas noise
               - Sudah adj_factor applied → 4H mencerminkan adjusted prices
               - Sudah deduplicated → tidak ada duplicate timestamp dalam blok

        Output: Silver 4H DataFrame dengan full Silver schema.

        PENTING: _add_derived_fields() TIDAK dipanggil untuk 4H bar.
            VWAP sudah dihitung oleh aggregator = sum(tp_vol)/sum(vol) per blok.
            Session-cumulative VWAP dari _add_derived_fields() tidak tepat untuk
            4H synthetic bar (blok tidak selalu sejajar sesi trading).
            Hanya log_return, dollar_volume, spread_hl yang ditambahkan via
            _add_4h_derived_fields().

        Args:
            silver_1h_df: Silver 1H DataFrame (output dari process_symbol(...,'1H')).
            symbol:       Normalized symbol string.
            market:       Asset market string ('us_stocks', 'idx', dll).
            is_adjusted:  True jika harga sudah adjusted (diwariskan dari Silver 1H).
            adj_factor:   Cumulative adjustment factor (diwariskan dari Silver 1H).

        Returns:
            Silver 4H DataFrame atau pl.DataFrame() kosong jika input tidak cukup.
        """
        if silver_1h_df is None or len(silver_1h_df) == 0:
            return pl.DataFrame()

        from src.silver.ohlcv_aggregator import aggregate_ohlcv

        # Kolom minimum yang dibutuhkan aggregator
        required = ["timestamp", "open", "high", "low", "close", "volume"]
        if not all(c in silver_1h_df.columns for c in required):
            missing = [c for c in required if c not in silver_1h_df.columns]
            logger.warning(
                f"[synthesize_4h] {symbol}: missing required cols {missing}"
            )
            return pl.DataFrame()

        # Sintesis: Silver 1H → Silver 4H
        # aggregator menghasilkan VWAP = sum(tp_vol)/sum(vol) per 4H block
        # FIX NEW-3: market diteruskan agar is_incomplete_bar dihitung session-aware
        # (lihat ohlcv_aggregator.py MARKET_SESSION_LOCAL / _expected_bars_by_block)
        agg_4h = aggregate_ohlcv(silver_1h_df, "1H", "4H", symbol, market=market)

        if agg_4h is None or len(agg_4h) == 0:
            logger.debug(f"[synthesize_4h] {symbol}: aggregator returned empty")
            return pl.DataFrame()

        # Tambah derived fields (log_return, dollar_volume, spread_hl)
        # TIDAK panggil _add_derived_fields() — VWAP sudah ada dari aggregator
        agg_4h = self._add_4h_derived_fields(agg_4h)

        # Tambah is_adjusted + adj_factor (diwariskan dari Silver 1H)
        agg_4h = self._add_adjustment_flags(agg_4h, is_adjusted, adj_factor)

        # is_clean flag: gunakan is_incomplete_bar dari aggregator jika ada
        agg_4h = self._flag_is_clean_4h(agg_4h)

        # Silver metadata columns
        agg_4h = self._add_metadata(
            agg_4h,
            symbol=symbol,
            timeframe="4H",
            market=market,
            actual_source="yfinance_aggregated",   # 4H adalah derived product
        )

        logger.debug(
            f"[synthesize_4h] {symbol}: Silver 1H→4H | {len(agg_4h)} bars produced"
        )
        return agg_4h

    @staticmethod
    def _add_4h_derived_fields(df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate derived fields untuk 4H synthetic bar.

        TIDAK menghitung VWAP — sudah disediakan oleh ohlcv_aggregator
        sebagai sum(tp_vol)/sum(vol) per blok (lebih akurat daripada
        session-cumulative VWAP dari _add_derived_fields()).

        Menghitung:
          - log_return: ln(close / prev_close) antar 4H bars
          - dollar_volume: close * volume (G2 FIX: dihitung eksplisit)
          - spread_hl: (high - low) / close

        Args:
            df: 4H aggregated DataFrame dengan kolom open/high/low/close/volume/vwap.

        Returns:
            DataFrame dengan derived fields tambahan.
        """
        exprs = []

        # log_return: ln(close / prev_close) antar 4H bars
        # FIX F-OP-01 [P2]: same fix as _add_derived_fields — use math.e.
        # BEFORE: .log(base=2.71828)  AFTER: .log(base=math.e)
        if "close" in df.columns:
            exprs.append(
                (pl.col("close") / pl.col("close").shift(1))
                .log(base=math.e)
                .alias("log_return")
            )

        # dollar_volume (G2 FIX: dihitung eksplisit di Silver)
        if all(c in df.columns for c in ["close", "volume"]):
            exprs.append(
                (pl.col("close") * pl.col("volume").cast(pl.Float64))
                .alias("dollar_volume")
            )

        # spread_hl
        if all(c in df.columns for c in ["high", "low", "close"]):
            exprs.append(
                ((pl.col("high") - pl.col("low")) / pl.col("close"))
                .alias("spread_hl")
            )

        if exprs:
            df = df.with_columns(exprs)

        return df

    def _flag_is_clean_4h(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Set is_clean flag untuk 4H synthetic bar.

        Logic sama dengan _flag_is_clean() tapi dengan tambahan:
        is_incomplete_bar=True (bar_count < 4) → is_clean=False.
        Silver consumer dapat mendeteksi 4H blocks yang tidak penuh
        (sesi pendek akibat holiday, awal data, dll).

        is_incomplete_bar adalah working column dari aggregator — di-drop
        setelah is_clean ditetapkan (bukan bagian dari GD §4.3 Silver schema).
        """
        clean = pl.lit(True)

        # Price sanity
        if all(c in df.columns for c in ["high", "low", "open", "close"]):
            clean = (
                clean
                & (pl.col("high") >= pl.col("low"))
                & (pl.col("open") >= pl.col("low"))
                & (pl.col("open") <= pl.col("high"))
                & (pl.col("close") >= pl.col("low"))
                & (pl.col("close") <= pl.col("high"))
            )

        # Null check pada OHLCV
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                clean = clean & pl.col(col).is_not_null()

        # Outlier: |z-score| > 4 pada log_return
        if "log_return" in df.columns:
            try:
                lr    = pl.col("log_return")
                mean_ = df["log_return"].mean()
                std_  = df["log_return"].std()
                if std_ and std_ > 0:
                    zscore = (lr - mean_) / std_
                    clean  = clean & (zscore.abs() <= self.OUTLIER_ZSCORE_THRESHOLD)
            except Exception:
                pass

        # is_incomplete_bar: 4H block dengan < 4 sub-bars → is_clean=False
        if "is_incomplete_bar" in df.columns:
            clean = clean & (~pl.col("is_incomplete_bar"))

        df = df.with_columns(clean.alias("is_clean"))

        # Drop working columns dari aggregator — bukan bagian dari Silver schema GD §4.3
        drop_agg_cols = [c for c in ["bar_count", "is_incomplete_bar"] if c in df.columns]
        if drop_agg_cols:
            df = df.drop(drop_agg_cols)

        return df

    # ── Write ─────────────────────────────────────────────────────────────────

    def write(
        self,
        df: pl.DataFrame,
        symbol: str,
        market: str,
        timeframe: str,
    ) -> Path:
        """Write Silver OHLCV Parquet (zstd compression, row_group=50k)."""
        out_dir = SILVER_OHLCV_PATH / market / f"symbol={symbol}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{symbol}_{timeframe}_silver.parquet"

        # FIX SIL-AIO-001: atomic write via tempfile + os.replace (crash-safe)
        atomic_write_parquet(
            df, out_path,
            compression="zstd", compression_level=3,
            row_group_size=50_000, statistics=True, use_pyarrow=True,
        )
        logger.debug(
            f"[OHLCVProcessor] Wrote {len(df):,} rows → {out_path.name}"
        )
        return out_path


# ── Job entry point (GD §14.3.2) ────────────────────────────────────────────

# v1.5: Bronze provides these TFs directly. '4H' is intentionally absent —
# it is synthesized in PASS 2 from Silver 1H (GD §4.1, §17.7 — 4H is not raw
# source data, so Bronze never fetches it; see SYNTHETIC_TIMEFRAMES above).
_RUN_BRONZE_TFS = ["5m", "15m", "1H", "1D", "1W", "1M"]

# ADD GMI-SIL-001 — Layer 2 context TFs (Architecture v2.0 §4, §6).
# Deliberately narrower than _RUN_BRONZE_TFS: MarketOHLCVIngester.run_context()
# (Bronze) uses DEFAULT_TIMEFRAMES=[1D,1W,1M] by default — identical to what
# bronze_ohlcv_daily actually fetches for Layer 1 today (job_registry.py never
# overrides timeframes=). Scanning for 5m/15m/1H context Bronze data that will
# never exist would be a silent no-op per symbol, not a correctness bug — but
# an explicit, narrower list here documents the real contract instead of
# implying intraday Layer 2 support that does not exist. No defined Layer 2
# consumer (CrossAssetEngine, GlobalIndexRegimeModule — Architecture v2.0 §6)
# operates on anything finer than 1D.
_RUN_CONTEXT_TFS = ["1D", "1W", "1M"]


def run(run_date: date) -> None:
    """
    Job entry point for 'silver_ohlcv' — dipanggil oleh job_registry.py.

    FIX GAP-6 [P1] (Production Readiness Assessment v1.7.2, GD §14.3.2): this
    module previously had no module-level run(run_date) function. GD §14.3.2
    requires every Silver/Gold module to expose one as its job_registry entry
    point. job_registry.py's _silver_ohlcv() wrapper had its own inline copy
    of this exact 2-pass logic instead of delegating here — functionally
    working today, but a duplicate-logic / drift risk (a future fix applied
    to one copy and not the other silently diverges, the exact "half-fix"
    failure pattern behind GAP-1). This run() is now the single
    implementation; job_registry.py's _silver_ohlcv() delegates to it (see
    src/scheduler/job_registry.py), matching the delegate-only pattern every
    other job wrapper in that file already uses (_silver_macro,
    _silver_fundamental, etc.).

    2-pass design (v1.5 refactoring, unchanged from the prior inline version):
      PASS 1: Bronze raw TFs (5m, 15m, 1H, 1D, 1W, 1M) → Silver, per symbol.
              FIX MI-1 preserved: Bronze read uses a wildcard glob across
              source=*/ so non-yfinance data (tvdatafeed, Polygon, AV,
              ForexDayCache) is never silently skipped.
      PASS 2: Silver 1H (clean, UTC, adj_factor applied) → Silver 4H
              synthesis. Runs after PASS 1 completes for all symbols, since
              it depends on PASS 1's Silver 1H output.
    Both passes are resumable via ProgressCheckpoint — a crash partway
    through only re-processes pending (symbol, timeframe) pairs.
    """
    from src.config.instrument_loader import get_loader
    from src.utils.progress_checkpoint import ProgressCheckpoint

    proc   = OHLCVProcessor()
    loader = get_loader()

    # ── PASS 1: Bronze raw TFs → Silver ──────────────────────────────────────
    logger.info(f"[silver_ohlcv] PASS 1: Bronze raw TFs → Silver | run_date={run_date}")

    for tf in _RUN_BRONZE_TFS:
        ckpt    = ProgressCheckpoint("silver_ohlcv_p1", run_date)
        pending = ckpt.pending_symbols(loader.symbol_list(), timeframe=tf)
        logger.info(
            f"[silver_ohlcv] PASS 1 TF={tf} | {len(pending)} symbols pending"
        )

        for inst in loader.all_symbols():
            if inst.symbol not in pending:
                continue
            try:
                # FIX MI-1 (CRITICAL, preserved): wildcard across source=*/ —
                # Bronze Hive layout is {market}/source={src}/symbol={sym}/year/month/.
                # A hardcoded source=yfinance/ path silently skips tvdatafeed,
                # yfinance_jk, polygon, and ForexDayCache-sourced data.
                pattern = str(
                    BRONZE_OHLCV_PATH / inst.market
                    / "**"
                    / f"symbol={inst.symbol}"
                    / "**"
                    / "*.parquet"
                )
                try:
                    # FIX SIL-RPQ-001: lazy scan → collect avoids full eager load on M1 8GB
                    df = pl.scan_parquet(pattern).collect()
                except Exception:
                    # No Bronze data for this symbol/TF yet — mark done, not failed.
                    ckpt.mark_done(inst.symbol, timeframe=tf)
                    continue

                silver_df = proc.process_symbol(
                    df=df,
                    symbol=inst.symbol,
                    market=inst.market,
                    timeframe=tf,
                    tz_hint=inst.timezone,
                )
                if silver_df is not None and len(silver_df) > 0:
                    proc.write(silver_df, inst.symbol, inst.market, tf)

                ckpt.mark_done(inst.symbol, timeframe=tf)
            except Exception as e:
                ckpt.mark_failed(inst.symbol, e, timeframe=tf)
                logger.error(
                    f"[silver_ohlcv] PASS 1 FAILED {inst.symbol}/{tf}: {e}"
                )

    # ── PASS 2: Silver 1H → Silver 4H synthesis ──────────────────────────────
    logger.info(
        f"[silver_ohlcv] PASS 2: Silver 1H → Silver 4H synthesis | run_date={run_date}"
    )

    ckpt_4h    = ProgressCheckpoint("silver_ohlcv_4h", run_date)
    pending_4h = ckpt_4h.pending_symbols(loader.symbol_list(), timeframe="4H")
    logger.info(
        f"[silver_ohlcv] PASS 2 TF=4H | {len(pending_4h)} symbols pending"
    )

    for inst in loader.all_symbols():
        if inst.symbol not in pending_4h:
            continue
        try:
            silver_1h_path = (
                SILVER_OHLCV_PATH / inst.market
                / f"symbol={inst.symbol}"
                / f"{inst.symbol}_1H_silver.parquet"
            )
            if not silver_1h_path.exists():
                # No Silver 1H — cannot synthesize 4H, skip without error.
                ckpt_4h.mark_done(inst.symbol, timeframe="4H")
                continue

            # FIX SIL-RPQ-001: lazy scan for single-symbol 1H Silver file
            silver_1h_df = pl.scan_parquet(str(silver_1h_path)).collect()

            silver_4h_df = proc.synthesize_4h(
                silver_1h_df=silver_1h_df,
                symbol=inst.symbol,
                market=inst.market,
            )

            if silver_4h_df is not None and len(silver_4h_df) > 0:
                proc.write(silver_4h_df, inst.symbol, inst.market, "4H")

            ckpt_4h.mark_done(inst.symbol, timeframe="4H")
        except Exception as e:
            ckpt_4h.mark_failed(inst.symbol, e, timeframe="4H")
            logger.error(
                f"[silver_ohlcv] PASS 2 FAILED {inst.symbol}/4H: {e}"
            )

    logger.info(
        f"[silver_ohlcv] Complete | "
        f"P1={ProgressCheckpoint('silver_ohlcv_p1', run_date).summary()} | "
        f"P2={ckpt_4h.summary()}"
    )


# ── ADD GMI-SIL-001 — Layer 2 Context OHLCV (Bronze → Silver) ────────────────
# Job entry point for 'silver_ohlcv_context' — job_registry.py.
#
# Gap closed: process_symbol() and write() were already fully generic w.r.t.
# 'market' as a plain string (verified empirically before writing this —
# _normalize_timestamps() takes tz_hint as an explicit override that takes
# priority over its market→timezone dict, and write() only uses market as a
# Hive path segment). Layer 2 instruments all carry market='context' (single
# flat bucket, same pattern as any Layer 1 market bucket) plus a correctly
# populated per-instrument timezone from instruments.yaml v1.4 — so this
# reuses OHLCVProcessor.process_symbol()/write() UNCHANGED. No 4H synthesis
# pass: no defined Layer 2 consumer (Architecture v2.0 §6) needs 4H, and
# Bronze context ingestion never fetches 1H for context anchors either
# (see _RUN_CONTEXT_TFS comment above) — nothing would exist to synthesize
# from. Checkpoint namespace ("silver_ohlcv_context") kept fully separate
# from Layer 1's ("silver_ohlcv_p1"/"silver_ohlcv_4h") — GD §17.3.1 pattern.
def run_context(run_date: date) -> None:
    """
    Job entry point for 'silver_ohlcv_context' — dipanggil oleh job_registry.py.

    1-pass design (berbeda dari run() Layer 1 yang 2-pass): Bronze context
    TFs (1D, 1W, 1M) → Silver, per Layer 2 instrument. Tidak ada Pass 2 —
    lihat blok komentar di atas untuk rationale (tidak ada 4H consumer
    untuk Layer 2 di cycle ini).
    """
    from src.config.instrument_loader import get_loader
    from src.utils.progress_checkpoint import ProgressCheckpoint

    proc   = OHLCVProcessor()
    loader = get_loader()
    context_instruments = loader.all_context(include_deferred=False)

    logger.info(
        f"[silver_ohlcv_context] Bronze context TFs → Silver | "
        f"run_date={run_date} | {len(context_instruments)} Layer 2 instruments"
    )

    for tf in _RUN_CONTEXT_TFS:
        ckpt    = ProgressCheckpoint("silver_ohlcv_context", run_date)
        pending = ckpt.pending_symbols(
            [inst.symbol for inst in context_instruments], timeframe=tf
        )
        logger.info(
            f"[silver_ohlcv_context] TF={tf} | {len(pending)} symbols pending"
        )

        for inst in context_instruments:
            if inst.symbol not in pending:
                continue
            try:
                # Sama seperti PASS 1 Layer 1 — wildcard source=*/ glob
                # (FIX MI-1 pattern), bronze_path = market/ohlcv/context/
                pattern = str(
                    BRONZE_OHLCV_PATH / inst.market
                    / "**"
                    / f"symbol={inst.symbol}"
                    / "**"
                    / "*.parquet"
                )
                try:
                    df = pl.scan_parquet(pattern).collect()
                except Exception:
                    # Belum ada Bronze data untuk symbol/TF ini — mark done,
                    # bukan failed (konsisten dengan PASS 1 Layer 1).
                    ckpt.mark_done(inst.symbol, timeframe=tf)
                    continue

                silver_df = proc.process_symbol(
                    df=df,
                    symbol=inst.symbol,
                    market=inst.market,       # 'context' — path segment only
                    timeframe=tf,
                    tz_hint=inst.timezone,    # per-instrument, bukan market dict
                )
                if silver_df is not None and len(silver_df) > 0:
                    proc.write(silver_df, inst.symbol, inst.market, tf)

                ckpt.mark_done(inst.symbol, timeframe=tf)
            except Exception as e:
                ckpt.mark_failed(inst.symbol, e, timeframe=tf)
                logger.error(
                    f"[silver_ohlcv_context] FAILED {inst.symbol}/{tf}: {e}"
                )

    logger.info(
        f"[silver_ohlcv_context] Complete | "
        f"{ProgressCheckpoint('silver_ohlcv_context', run_date).summary()}"
    )
