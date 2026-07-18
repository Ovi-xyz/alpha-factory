"""
core_indicators.py — IDD §3.2 (Pure Polars Core Technical Indicators)
EMA, RSI, MACD, ATR, momentum features — zero C-extension dependency.

Library decision (IDD §3.1):
    Pure Polars:  EMA, RSI, MACD, ATR, momentum → zero dep, ARM64-native
    pandas-ta:    BBands, ADX → thin wrapper (in pandas_indicators.py)
    ta-lib:       AVOIDED — C compilation issues on M1 ARM64

Input:  Silver OHLCV DataFrame
Output: Same DataFrame + indicator columns
"""

from __future__ import annotations

import polars as pl
from loguru import logger


def add_ema(
    df: pl.DataFrame,
    periods: list[int] = [9, 21, 50, 200],
    col: str = "close",
) -> pl.DataFrame:
    """
    Exponential Moving Average via Polars ewm_mean.
    Uses Wilder's smoothing: alpha = 2/(span+1).
    Partitioned over 'symbol' for multi-symbol DataFrames.
    """
    exprs = [
        pl.col(col)
        .ewm_mean(span=p, adjust=False)
        .over("symbol")
        .alias(f"ema_{p}")
        for p in periods
    ]
    return df.with_columns(exprs)


def add_rsi(
    df: pl.DataFrame,
    periods: list[int] = [14, 28],
    col: str = "close",
) -> pl.DataFrame:
    """
    Relative Strength Index — Wilder smoothing (alpha = 1/period).
    Formula: RSI = 100 - (100 / (1 + RS))
             RS  = avg_gain / avg_loss (Wilder EMA)
    """
    exprs = []
    for p in periods:
        delta = pl.col(col).diff().over("symbol")
        gain  = pl.when(delta > 0).then(delta).otherwise(0)
        loss  = pl.when(delta < 0).then(-delta).otherwise(0)

        avg_g = gain.ewm_mean(alpha=1 / p, adjust=False).over("symbol")
        avg_l = loss.ewm_mean(alpha=1 / p, adjust=False).over("symbol")

        # RS = avg_gain / avg_loss — avoid div by zero
        rs  = avg_g / pl.when(avg_l != 0).then(avg_l).otherwise(1e-10)
        rsi = (100 - (100 / (1 + rs))).alias(f"rsi_{p}")
        exprs.append(rsi)

    return df.with_columns(exprs)


def add_macd(
    df: pl.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    col: str = "close",
) -> pl.DataFrame:
    """
    MACD: Moving Average Convergence Divergence.
    Outputs: macd, macd_signal, macd_hist
    """
    ema_fast  = pl.col(col).ewm_mean(span=fast,   adjust=False).over("symbol")
    ema_slow  = pl.col(col).ewm_mean(span=slow,   adjust=False).over("symbol")
    macd_line = (ema_fast - ema_slow).alias("_macd_raw")

    df = df.with_columns([macd_line])

    sig = (
        pl.col("_macd_raw")
        .ewm_mean(span=signal, adjust=False)
        .over("symbol")
    )

    return df.with_columns([
        pl.col("_macd_raw").alias("macd"),
        sig.alias("macd_signal"),
        (pl.col("_macd_raw") - sig).alias("macd_hist"),
    ]).drop("_macd_raw")


def add_atr(
    df: pl.DataFrame,
    period: int = 14,
) -> pl.DataFrame:
    """
    Average True Range — Wilder's smoothing.
    TR = max(high-low, |high-prev_close|, |low-prev_close|)
    ATR = EWM(TR, alpha=1/period)
    """
    tr = pl.max_horizontal([
        pl.col("high") - pl.col("low"),
        (pl.col("high") - pl.col("close").shift(1)).abs(),
        (pl.col("low")  - pl.col("close").shift(1)).abs(),
    ]).over("symbol")

    atr     = tr.ewm_mean(alpha=1 / period, adjust=False).over("symbol")
    atr_pct = (atr / pl.col("close")).over("symbol")

    return df.with_columns([
        tr.alias("tr"),
        atr.alias(f"atr_{period}"),
        atr_pct.alias("atr_pct"),
    ])


def add_momentum_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Additional momentum & volume features:
      - volume_sma_20:   20-period simple MA of volume
      - relative_volume: volume / volume_sma_20 (volume spike detection)
      - trend_strength:  1 if close > ema_50 else 0
      - momentum_score:  RSI_14 - 50 (centered around 0)
    """
    exprs = [
        pl.col("volume")
        .rolling_mean(20)
        .over("symbol")
        .alias("volume_sma_20"),
    ]

    # Relative volume — needs volume_sma_20 computed first
    df = df.with_columns(exprs)

    exprs2 = []

    if "volume_sma_20" in df.columns:
        exprs2.append(
            (
                pl.col("volume")
                / pl.when(pl.col("volume_sma_20") != 0)
                  .then(pl.col("volume_sma_20"))
                  .otherwise(1)
            ).alias("relative_volume")
        )

    if "ema_50" in df.columns:
        exprs2.append(
            (pl.col("close") > pl.col("ema_50"))
            .cast(pl.Int8)
            .alias("trend_strength")
        )

    if "rsi_14" in df.columns:
        exprs2.append(
            pl.col("rsi_14").sub(50).alias("momentum_score")
        )

    if exprs2:
        df = df.with_columns(exprs2)

    return df
