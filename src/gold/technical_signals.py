"""
technical_signals.py — IDD §3.4 + GD §5.2.2 + Architecture v2.0 §5.2
Gold Technical Signals: compute semua indicators untuk Layer 1 active_ohlcv
symbols (~190, liquidity-screened) × 5 timeframes (FIX ADR-046 Path C —
was 7; 5m/15m removed, see TIMEFRAMES below).

Output: data/gold/signals/tech_signals_{TF}.parquet

Schema (GD §5.2.2):
    symbol, timestamp, timeframe, signal_date,
    ema_9, ema_21, ema_50, ema_200,
    rsi_14, rsi_28,
    macd, macd_signal, macd_hist,
    atr_14, atr_pct, tr,
    bbands_upper, bbands_mid, bbands_lower,
    adx, di_plus, di_minus,
    volume_sma_20, relative_volume,
    trend_strength, momentum_score

G6: ProgressCheckpoint per timeframe (symbol='ALL') untuk resume

FIX GLD-L2-01 (GMI Wave 1 Bronze/Silver Solidification): the Silver read
previously globbed data/silver/market_ohlcv/**/ with NO market filter.
Since GMI Cycle 3 added Layer 2 context OHLCV under the same root
(market_ohlcv/context/...), this silently ALSO scanned Layer 2 rows —
meaning RSI/MACD/ADX/BBands were being computed for VIX, DXY, 13 global
equity indices, 25 ETFs, and 8 commodity context anchors as if they were
tradeable candidates, and written into the SAME tech_signals_{TF}.parquet
that gold_mtf/gold_screener consume for the Layer 1 trading watchlist.
This directly contradicts ADR-003's own stated rationale for reclassifying
VIX/DXY out of Layer 1 in the first place ("RSI pada VIX khususnya tidak
bermakna karena VIX adalah threshold-based regime indicator" — Architecture
Extension v1.0 ADR-003). Fixed via silver_scope.layer1_globs() (see that
module's docstring for the full empirical reproduction, shared with the
analogous quality_validator.py fix, FIX QV-L2-01).

ADD GLD-ACTIVE-001 (Architecture v2.0 §5.2): gold_signals was specified to
process ONLY active_ohlcv (~190 liquidity-screened Layer 1 symbols), not
the full 640-symbol Layer 1 universe — a ~69% (190/640) compute reduction
that was never implemented. Now applied via
ActiveSymbolsResolver.load_ohlcv(run_date), with a graceful fallback to
the full (Layer-1-scoped) universe if silver_active_symbols has not yet
run for run_date (expected only for direct/out-of-sequence invocation —
see _resolve_active_ohlcv_symbols() docstring).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl
from loguru import logger

from src.gold.indicators.core_indicators import (
    add_atr,
    add_ema,
    add_macd,
    add_momentum_features,
    add_rsi,
)
from src.gold.indicators.pandas_indicators import add_adx, add_bbands
from src.utils.atomic_io import atomic_write_parquet
from src.utils.progress_checkpoint import ProgressCheckpoint
from src.utils.silver_scope import layer1_globs  # FIX GLD-L2-01

# ── Constants ─────────────────────────────────────────────────────────────────

# FIX ADR-046 Path C (GMI_Decision_Document_v11.docx §2, decided by Ovi):
# 5m/15m removed — never fetched into Bronze under Path C (only 1H was
# wired up alongside ADR-045's partition fix), so attempting them here
# only ever produced "no Layer 1 Silver data yet" warnings on every run,
# forever. Trimming avoids that permanently-wasted iteration and matches
# mtf_alignment.py's own TIMEFRAMES list exactly (both files must agree —
# see that module's docstring for the full recalibrated grade table).
TIMEFRAMES        = ["1H", "4H", "1D", "1W", "1M"]
GOLD_SIG_PATH      = Path("data/gold/signals")
SILVER_OHLCV_PATH  = Path("data/silver/market_ohlcv")  # FIX GLD-L2-01: was SILVER_PATH_TMPL string


def _apply_volatility_flag(df: pl.DataFrame, run_date: date) -> pl.DataFrame:
    """
    GD §15.1: VIX spike guard.
    If VIX > 40, add high_volatility_flag=True to all signals.
    Trading Engine uses this flag to reduce/skip position sizing.
    """
    vix_value = _get_latest_vix(run_date)

    return df.with_columns([
        pl.lit(vix_value > 40 if vix_value is not None else False)
          .alias("high_volatility_flag"),
        pl.lit(float(vix_value) if vix_value is not None else 0.0)
          .cast(pl.Float64)
          .alias("vix_at_signal"),
    ])


def _get_latest_vix(run_date: date) -> float | None:
    """Read latest VIX close from Silver OHLCV (context market) — GD §9.1.

    FIX GD-F01: VIX Spike Guard (GD §15.1) harus membaca VIX dari
    Silver OHLCV (data/silver/market_ohlcv/context/) karena frekuensi update
    harian selaras dengan pipeline daily cadence. Silver macro FRED VIXCLS
    digunakan sebagai fallback saja — series ini kadang delay beberapa hari.

    FIX GMI-GLD-001: DUA bug diperbaiki di sini, ditemukan berurutan lewat
    empirical testing (bukan asumsi dari code reading saja):
      (1) Path SALAH: market_ohlcv/index/ (Layer 1 — PERMANENTLY EMPTY sejak
          ADR-003 mereklasifikasi VIX ke Layer 2 context, Architecture
          Extension v1.0 §2.2) -> market_ohlcv/context/ (Layer 2,
          diverifikasi empiris: Instrument('VIX').market == 'context').
      (2) Glob pattern SALAH SECARA TERPISAH dan LEBIH FUNDAMENTAL: pattern
          asli memakai DUA '**' dalam satu path
          ('context/**/symbol=VIX/**/*_1D_silver.parquet'). DuckDB
          read_parquet() menolak ini secara eksplisit — "IO Error: Cannot
          use multiple '**' in one path" — diverifikasi empiris via DuckDB
          langsung, TERMASUK terhadap string index/ ASLI (pre-fix), yang
          membuktikan bug ini SUDAH ADA SEBELUM ADR-003 sekalipun, bukan
          efek samping dari fix (1). except Exception: pass di bawah
          MENYEMBUNYIKAN kedua bug ini sekaligus sejak awal — primary read
          TIDAK PERNAH benar-benar berhasil, fallback FRED VIXCLS SELALU
          dipakai, tanpa exception yang pernah terlihat oleh siapapun.
      Fix (2): Silver write() (ohlcv_processor.py) menghasilkan struktur
      path yang SEPENUHNYA deterministic untuk symbol tertentu —
      market_ohlcv/{market}/symbol={symbol}/{symbol}_{tf}_silver.parquet —
      nol variable directory depth. Tidak ada '**' yang dibutuhkan sama
      sekali; satu '*' pada filename sudah cukup.
    Sebelum fix ini, VIX Spike Guard (GD §15.1) diam-diam berjalan dengan
    proxy FRED yang lebih stale dari yang didesain (docstring fallback
    sendiri: "kadang delay beberapa hari") — bukan crash, tapi silent
    degradation sejak fungsi ini pertama kali ditulis.
    """
    # Primary: Silver OHLCV — context market (Layer 2), symbol=VIX, 1D TF.
    # FIX GMI-GLD-001 (2): satu '*' pada filename — TIDAK ada '**' sama
    # sekali (DuckDB menolak >1 '**' per path; struktur write() Silver
    # deterministic untuk symbol tunggal, jadi tidak dibutuhkan).
    vix_ohlcv_glob = "data/silver/market_ohlcv/context/symbol=VIX/*_1D_silver.parquet"
    try:
        con = duckdb.connect()
        # FIX GLD-003: $name parameterized query — f-string SQL dilarang GD §17.7
        result = con.execute(
            """
            SELECT close
            FROM read_parquet($glob, hive_partitioning=true)
            WHERE CAST(timestamp AS DATE) <= $run_date
              AND is_clean = TRUE
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            {"glob": vix_ohlcv_glob, "run_date": run_date},
        ).fetchone()
        if result and result[0] is not None:
            return float(result[0])
    except Exception:
        pass

    # Fallback: Silver macro FRED VIXCLS (FIX GD-F01: secondary, not primary)
    vix_glob = "data/silver/macro_enriched/fred_*_silver.parquet"
    try:
        con = duckdb.connect()
        # FIX GLD-003: $name parameterized query — f-string SQL dilarang GD §17.7
        result = con.execute(
            """
            SELECT value
            FROM read_parquet($glob, hive_partitioning=true)
            WHERE series_id = 'VIXCLS'
              AND CAST(observation_date AS DATE) <= $run_date
            ORDER BY observation_date DESC
            LIMIT 1
            """,
            {"glob": vix_glob, "run_date": run_date},
        ).fetchone()
        if result and result[0] is not None:
            logger.debug("[gold_signals] VIX from macro fallback (OHLCV not available)")
            return float(result[0])
    except Exception:
        pass
    return None


def _resolve_active_ohlcv_symbols(run_date: date) -> list[str] | None:
    """
    ADD GLD-ACTIVE-001 (Architecture v2.0 §5.2): return the Layer 1
    active_ohlcv symbol list for run_date, or None if unavailable.

    None triggers a fallback to the full (Layer-1-scoped, via
    layer1_globs()) universe in _process_timeframe — degraded (no
    liquidity filter applied) but still CORRECT, rather than crashing the
    whole job. This matters for direct/isolated invocation (unit tests,
    a --force out-of-sequence run): the production DependencyGuard
    already guarantees silver_active_symbols has completed before
    gold_signals runs in the normal scheduled path (job_registry.py:
    JOB_REGISTRY['gold_signals']['depends_on'] includes
    'silver_active_symbols'), so this fallback should not trigger in
    normal operation.
    """
    from src.silver.active_symbols import ActiveSymbolsResolver
    try:
        symbols = ActiveSymbolsResolver().load_ohlcv(run_date)
    except FileNotFoundError:
        logger.warning(
            f"[gold_signals] active_ohlcv not resolved for {run_date} "
            "(silver_active_symbols not yet run) — falling back to the "
            "full Layer 1 universe for this run. Expected only when "
            "invoking gold_signals directly/out of sequence; the "
            "scheduled DependencyGuard path prevents this."
        )
        return None
    if not symbols:
        logger.warning(
            f"[gold_signals] active_ohlcv resolved but empty for {run_date} "
            "— falling back to full Layer 1 universe."
        )
        return None
    return symbols


def run(run_date: date) -> None:
    """
    Entry point untuk gold_signals job.
    G6: Checkpoint per timeframe — resume-safe jika crash mid-way.

    ADD GLD-ACTIVE-001: active_ohlcv resolved ONCE per run (not per
    timeframe) — the list is the same across all 7 TFs for a given
    run_date, so resolving it 7x would be wasted I/O.
    """
    checkpoint = ProgressCheckpoint("gold_signals", run_date)
    active_symbols = _resolve_active_ohlcv_symbols(run_date)

    for tf in TIMEFRAMES:
        if checkpoint.is_done("ALL", timeframe=tf):
            logger.info(f"[gold_signals] TF={tf} already done — skipping")
            continue

        try:
            rows = _process_timeframe(tf, run_date, active_symbols)
            checkpoint.mark_done("ALL", timeframe=tf)
            logger.info(f"[gold_signals] TF={tf} ✓ — {rows:,} rows written")
        except Exception as e:
            checkpoint.mark_failed("ALL", e, timeframe=tf)
            logger.error(f"[gold_signals] TF={tf} FAILED: {e}")
            # Continue to next TF rather than abort entire job
            continue

    summary = checkpoint.summary()
    logger.info(f"[gold_signals] Run complete | {summary}")


def _process_timeframe(
    tf: str, run_date: date, active_symbols: list[str] | None = None
) -> int:
    """
    Process satu timeframe: read Silver → compute indicators → write Gold.
    Return jumlah rows yang ditulis.

    FIX GLD-L2-01: silver glob scoped to Layer 1 markets only via
    layer1_globs() — see module docstring for the pollution bug this fixes.

    ADD GLD-ACTIVE-001: when active_symbols is provided (the normal case),
    further filters to that list via DuckDB's `= ANY($param)` list-binding
    (no f-string SQL, GD §17.7). When None (fallback), processes the full
    Layer-1-scoped universe — degraded but correct.
    """
    globs = layer1_globs(SILVER_OHLCV_PATH, f"*_{tf}_silver.parquet")
    if not globs:
        logger.warning(f"[gold_signals] TF={tf}: no Layer 1 Silver data yet")
        return 0

    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'; SET threads=4;")

    try:
        if active_symbols:
            # FIX GLD-003 convention: $name parameterized query throughout
            df = con.execute(
                """
                SELECT symbol, timestamp, open, high, low, close, volume
                FROM read_parquet($globs, hive_partitioning=true)
                WHERE is_clean = TRUE
                  AND symbol = ANY($active_symbols)
                ORDER BY symbol, timestamp
                """,
                {"globs": globs, "active_symbols": active_symbols},
            ).pl()
        else:
            df = con.execute(
                """
                SELECT symbol, timestamp, open, high, low, close, volume
                FROM read_parquet($globs, hive_partitioning=true)
                WHERE is_clean = TRUE
                ORDER BY symbol, timestamp
                """,
                {"globs": globs},
            ).pl()
    except Exception as e:
        logger.warning(
            f"[gold_signals] TF={tf}: Silver data not found or empty — {e}"
        )
        return 0

    if df.is_empty():
        logger.warning(f"[gold_signals] TF={tf}: No clean Silver data")
        return 0

    logger.info(
        f"[gold_signals] TF={tf} | {len(df):,} rows | "
        f"{df['symbol'].n_unique()} symbols"
    )

    # ── Apply indicators pipeline ─────────────────────────────────────────────
    df = (
        df
        .sort(["symbol", "timestamp"])
        .pipe(add_ema,  periods=[9, 21, 50, 200])
        .pipe(add_rsi,  periods=[14, 28])
        .pipe(add_macd)
        .pipe(add_atr,  period=14)
        .pipe(add_bbands, period=20, std=2.0)
        .pipe(add_adx,    period=14)
        .pipe(add_momentum_features)
        .with_columns([
            pl.lit(tf).alias("timeframe"),
            pl.lit(run_date.isoformat()).alias("signal_date"),
        ])
    )

    # GD §15.1: VIX spike guard — flag all signals if VIX > 40
    df = _apply_volatility_flag(df, run_date)

    # ── Write output — FIX GLD-004: atomic write ──────────────────────────────
    GOLD_SIG_PATH.mkdir(parents=True, exist_ok=True)
    out_path = GOLD_SIG_PATH / f"tech_signals_{tf}.parquet"

    # FIX GLD-004: atomic_write_parquet via tempfile + os.replace
    atomic_write_parquet(
        df,
        out_path,
        compression="zstd",
        compression_level=3,
        row_group_size=50_000,
        statistics=True,
        use_pyarrow=True,
    )

    return len(df)
