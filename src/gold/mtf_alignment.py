"""
mtf_alignment.py — GD §5.2.3 + §6.3 (Gold MTF Alignment Store)
Multi-Timeframe alignment score, signal quality grading, entry/stop zones.

FIX ADR-046 Path C (GMI_Decision_Document_v11.docx §2, decided by Ovi):
MTF score coverage was structurally unreachable before this fix —
5m/15m/1H were never fetched into Bronze, and since 4H synthesizes from
Silver 1H only (ohlcv_aggregator.py), 4H was empty too. Only 1D/1W/1M ever
contributed a real value, capping |mtf_score| at 3 while screener.py
required >=5 AND signal_quality IN ('A','B') — the watchlist was
mathematically incapable of returning a single row. Path C (the middle
path of three considered): wire up Bronze 1H alone (not 5m/15m, see
ADR-045 for the Bronze partition fix this depends on) — 1H both
contributes a real trend value directly and unblocks 4H's existing
synthesis, raising real contributors from 3 to 5 without touching 5m/15m
at all. The score range and grade boundaries below are recalibrated to
that 5-contributor reality per Ovi's explicit instruction, rather than
left at their old 7-timeframe values while quietly serving fewer real
inputs (the exact failure mode this fix exists to close).

MTF Score: sum of trend direction per timeframe (-5 to +5)
  +1: bullish, -1: bearish, 0: neutral
  Timeframes: 1H, 4H, 1D, 1W, 1M — 5m/15m deliberately excluded from the
  sum (Path C), not merely padded to 0 — see TIMEFRAMES below.

Signal Quality (grade D removed — every symbol now falls into A/B/C):
  A: |score| >= 4   (strong alignment)
  B: |score| == 3   (good alignment)
  C: |score| <= 2   (weak — catch-all, was the "D: skip" bucket pre-fix)

Output: data/gold/mtf/mtf_alignment_{date}.parquet
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl
from loguru import logger

from src.utils.atomic_io import atomic_write_parquet
from src.utils.progress_checkpoint import ProgressCheckpoint

GOLD_SIGNALS_PATH = Path("data/gold/signals")
GOLD_MTF_PATH     = Path("data/gold/mtf")
# FIX GLD-MTF-COV-01: promoted from a string literal inline in
# _apply_regime_compatible() to a module-level constant, matching the
# GOLD_SIGNALS_PATH / GOLD_MTF_PATH pattern above and macro_regime.py's own
# REGIME_STORE_PATH. Same default value — pure hardcode-avoidance/
# testability fix, no behavior change. Before this fix the path could not
# be monkeypatched in tests, so _apply_regime_compatible() was only ever
# exercised via a hand-duplicated copy of its logic in test code, never the
# real function. NOTE (flagged, not fixed here — out of this file's scope):
# this same literal is independently hardcoded inline (not as a module
# constant) in sector_rotation.py and views.py, and as a display-only
# literal in pipeline_dashboard.py; consolidating all Gold-layer output
# paths behind one shared constants module is a reasonable follow-up but is
# a broader, separately-scoped change.
REGIME_STORE_PATH = Path("data/gold/macro/regime_store.parquet")
# FIX ADR-046 Path C: 5m/15m removed — they are never fetched into Bronze
# under Path C (only 1H was wired up; see ADR-045/046) and would otherwise
# sit here as permanent always-0 padding forever, which is precisely the
# "wired but silently zeroed" shape this whole fix exists to close.
TIMEFRAMES        = ["1H", "4H", "1D", "1W", "1M"]


def run(run_date: date) -> None:
    """
    Compute MTF alignment for all symbols.
    Reads from gold/signals/tech_signals_{TF}.parquet.
    """
    checkpoint = ProgressCheckpoint("gold_mtf", run_date)

    if checkpoint.is_done("ALL"):
        logger.info("[gold_mtf] Already done — skipping")
        return

    try:
        df = _compute_mtf_alignment(run_date)
        if df is None or df.is_empty():
            logger.warning("[gold_mtf] No signal data available")
            return

        # Write output — FIX GLD-004: atomic write pattern
        GOLD_MTF_PATH.mkdir(parents=True, exist_ok=True)
        out_path = GOLD_MTF_PATH / f"mtf_alignment_{run_date.isoformat()}.parquet"
        # FIX GLD-004: atomic_write_parquet via tempfile + os.replace
        atomic_write_parquet(
            df,
            out_path,
            compression="zstd",
            compression_level=3,
            row_group_size=50_000,
        )
        checkpoint.mark_done("ALL")
        logger.info(
            f"[gold_mtf] {len(df):,} rows → {out_path.name}"
        )
    except Exception as e:
        checkpoint.mark_failed("ALL", e)
        raise


def _compute_mtf_alignment(run_date: date) -> pl.DataFrame:
    """
    Compute MTF score per symbol from tech_signals across all TFs.
    Returns DataFrame with full MTF schema.
    """
    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'; SET threads=4;")

    # Collect trend direction per TF for most recent bar per symbol
    tf_dfs: list[pl.DataFrame] = []

    for tf in TIMEFRAMES:
        sig_path = GOLD_SIGNALS_PATH / f"tech_signals_{tf}.parquet"
        if not sig_path.exists():
            logger.debug(f"[gold_mtf] No signals for TF={tf} — treating as neutral")
            continue

        try:
            # FIX GLD-003: $name parameterized query — f-string SQL dilarang GD §17.7
            tf_df = con.execute(
                """
                SELECT
                    symbol,
                    $tf AS timeframe,
                    -- Trend: 1=bull (close>ema_50), -1=bear, 0=neutral
                    CASE
                        WHEN close > ema_50 AND ema_9 > ema_21  THEN  1
                        WHEN close < ema_50 AND ema_9 < ema_21  THEN -1
                        ELSE 0
                    END AS trend_dir,
                    rsi_14,
                    macd_hist,
                    atr_14,
                    close,
                    signal_date
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn
                    FROM read_parquet($path)
                )
                WHERE rn = 1
                """,
                {"path": str(sig_path), "tf": tf},
            ).pl()
            tf_dfs.append(tf_df)
        except Exception as e:
            logger.debug(f"[gold_mtf] TF={tf}: {e}")

    if not tf_dfs:
        return pl.DataFrame()

    # Build wide format: one row per symbol, one column per TF trend
    all_signals = pl.concat(tf_dfs)

    # Pivot: symbol × timeframe → trend_dir columns
    pivot = all_signals.pivot(
        values="trend_dir",
        index="symbol",
        on="timeframe",
        aggregate_function="first",
    )

    # Fill missing TF columns with 0 (neutral)
    for tf in TIMEFRAMES:
        tf_col = tf
        if tf_col not in pivot.columns:
            pivot = pivot.with_columns(pl.lit(0).alias(tf_col))

    # Compute MTF score: sum of all TF trend directions
    tf_cols = [c for c in TIMEFRAMES if c in pivot.columns]
    pivot = pivot.with_columns([
        pl.sum_horizontal([pl.col(c).fill_null(0) for c in tf_cols])
          .alias("mtf_score")
    ])

    # Signal quality grade — FIX ADR-046 Path C: recalibrated for the
    # 5-timeframe -5..+5 range (was -7..+7). Grade D removed: C is now the
    # catch-all "otherwise" bucket, matching the old D's "weak, skip" role.
    pivot = pivot.with_columns([
        pl.when(pl.col("mtf_score").abs() >= 4).then(pl.lit("A"))
          .when(pl.col("mtf_score").abs() == 3).then(pl.lit("B"))
          .otherwise(pl.lit("C"))
          .alias("signal_quality")
    ])

    # FIX G-F03: ATR harus diambil dari TF=1H secara eksplisit, bukan next()
    # dari tf_dfs yang menggunakan TF pertama yang punya atr_14.
    # 1H adalah Primary Signal TF (GD §6.2) untuk entry/stop zone computation.
    atr_1h_df = next(
        (df.select(["symbol", "atr_14", "close"])
         for df in tf_dfs if len(df) > 0 and df["timeframe"][0] == "1H" and "atr_14" in df.columns),
        None,
    )
    # Fallback: gunakan TF apapun yang punya atr_14, tapi log warning
    if atr_1h_df is None:
        atr_1h_df = next(
            (df.select(["symbol", "atr_14", "close"])
             for df in tf_dfs if "atr_14" in df.columns),
            None,
        )
        if atr_1h_df is not None:
            logger.warning(
                "[gold_mtf] 1H signals not found — using fallback TF for ATR. "
                "entry_zone/stop_zone/RRR may be inaccurate."
            )

    if atr_1h_df is not None:
        pivot = pivot.join(
            atr_1h_df.rename({"atr_14": "atr", "close": "last_close"}),
            on="symbol",
            how="left",
        )
        # Entry zone: close ± 0.5 * ATR; Stop zone: close − 1.5 * ATR (long bias)
        pivot = pivot.with_columns([
            (pl.col("last_close") - 0.5 * pl.col("atr")).alias("entry_zone_low"),
            (pl.col("last_close") + 0.5 * pl.col("atr")).alias("entry_zone_high"),
            (pl.col("last_close") - 1.5 * pl.col("atr")).alias("stop_zone_1H"),
        ])
        # FIX G-F04: RRR = profit_target / risk
        # entry_zone_1H = mid of entry zone = last_close - 0.25*ATR (conservative entry)
        # stop_zone_1H  = last_close - 1.5*ATR
        # target         = last_close + 1.5*ATR (symmetric 1:1.5 target)
        # RRR = (target - entry) / (entry - stop)
        pivot = pivot.with_columns([
            pl.when(pl.col("atr") > 0)
              .then(
                  # profit target delta / risk delta — based on actual price levels
                  (1.5 * pl.col("atr"))
                  / (1.25 * pl.col("atr"))   # entry at -0.25*ATR from close, stop at -1.5*ATR
              )
              .otherwise(pl.lit(None).cast(pl.Float64))
              .alias("reward_risk_ratio"),
        ])
    else:
        for col in ["entry_zone_low", "entry_zone_high", "stop_zone_1H",
                    "last_close", "atr", "reward_risk_ratio"]:
            pivot = pivot.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    # regime_compatible: join with regime_store if available
    pivot = _apply_regime_compatible(pivot, run_date)

    pivot = pivot.with_columns([
        pl.lit(str(run_date)).alias("date"),
    ])

    return pivot


def _apply_regime_compatible(df: pl.DataFrame, run_date: date) -> pl.DataFrame:
    """
    Set regime_compatible = True for symbols whose mtf_score direction
    matches the current macro regime bias.

    RISK_ON  → long bias (positive score) = compatible
    RISK_OFF → short/defensive bias (negative score) = compatible
    Other    → all symbols compatible (regime-agnostic)
    """
    regime = "NEUTRAL"

    try:
        import duckdb
        con = duckdb.connect()
        # FIX GLD-003: $name parameterized query — f-string SQL dilarang GD §17.7
        result = con.execute(
            """
            SELECT regime
            FROM read_parquet($path)
            WHERE CAST(date AS DATE) <= $run_date
            ORDER BY date DESC
            LIMIT 1
            """,
            {"path": str(REGIME_STORE_PATH), "run_date": run_date},
        ).fetchone()
        if result:
            regime = result[0]
    except Exception:
        pass   # No regime store yet — default all compatible

    # Apply compatibility filter
    if regime == "RISK_ON":
        compatible_expr = pl.col("mtf_score") > 0
    elif regime == "RISK_OFF":
        compatible_expr = pl.col("mtf_score") < 0
    else:
        compatible_expr = pl.lit(True)   # All compatible in other regimes

    return df.with_columns([
        compatible_expr.alias("regime_compatible"),
        pl.lit(regime).alias("active_regime"),
    ])


def get_mtf_summary(run_date: date) -> dict:
    """Return summary stats for health reporter."""
    path = GOLD_MTF_PATH / f"mtf_alignment_{run_date.isoformat()}.parquet"
    if not path.exists():
        return {}

    df = pl.read_parquet(path)
    return {
        "total_symbols": len(df),
        "grade_A": df.filter(pl.col("signal_quality") == "A").shape[0],
        "grade_B": df.filter(pl.col("signal_quality") == "B").shape[0],
        "grade_C": df.filter(pl.col("signal_quality") == "C").shape[0],
        # FIX ADR-046 Path C: grade_D removed — grade D no longer exists
        # in the classification scheme (C is now the catch-all bucket).
    }
