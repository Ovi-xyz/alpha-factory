"""
ohlcv_aggregator.py — Silver 4H Synthesizer
Synthetic 4H bar construction from Silver 1H bars.

GD §4.1 Silver Responsibilities — Enrichment:
    Sintesis timeframe dari timeframe yang lebih rendah adalah enrichment/derived
    data. Bar 4H tidak tersedia dari free-tier sources (yfinance, Polygon) — harus
    disintesis dari Silver 1H yang sudah bersih.

DIPINDAH dari Bronze → Silver (v1.5 refactoring).
    Bronze layer hanya menyimpan data as-is dari source (GD §3.1 Design Principles).
    4H bukan raw source data — ini adalah derived/synthetic bar (GD §17.7 Anti-Patterns).

Input: Silver 1H DataFrame — sudah UTC-normalized, null-handled, adj_factor-applied.
       Kualitas 4H bar lebih baik karena disintesis dari Silver (bukan Bronze) 1H.

Supported aggregation:
    1H → 4H   — satu-satunya kombinasi valid di Silver layer

Aggregation rules:
    open:   chronologically first bar of the period  [FIX Bug 1: sort_by inside agg]
    high:   max across all bars in period
    low:    min across all bars in period
    close:  chronologically last bar of the period   [FIX Bug 1: sort_by inside agg]
    volume: sum across all bars in period
    vwap:   sum(typical_price_i * volume_i) / sum(volume_i)  [FIX Bug 2: pre-agg tp_vol]

Timeframe grouping:
    4H: UTC-aligned 4-hour blocks [00-03],[04-07],[08-11],[12-15],[16-19],[20-23]
        Timestamp = canonical block-start (UTC), not first sub-bar's timestamp.
        [FIX Bug 6: removed incorrect "WIB/ET session aware" claim]

    FIX NEW-3 [P1 HIGH] (audit_v1_7_3_uncovered_findings.docx, Section 4, Opsi A):
        Bug 6 above removed the *block-grouping* session claim correctly (blocks
        are genuinely UTC-fixed), but left completeness validation using a flat
        EXPECTED_BARS['4H']=4 regardless of how many of a block's 4 hours
        actually fall within the instrument's trading session. A block straddling
        session open/close legitimately contains 1-3 real sub-bars and is NOT
        incomplete — yet the flat threshold flagged it bar_count<4, causing
        ~67% of US/IDX 4H bars to be misclassified is_incomplete_bar=True
        (downstream: is_clean=False in OHLCVProcessor._flag_is_clean_4h).

        Fix: expected sub-bar count is now computed PER BLOCK as the number of
        the block's 4 candidate UTC hours that overlap the instrument's regular
        LOCAL trading session (see MARKET_SESSION_LOCAL below). Conversion from
        UTC to local exchange time uses the IANA timezone database via Polars
        dt.convert_time_zone — the same mechanism already used by
        OHLCVProcessor._normalize_timestamps — so the result is DST-correct
        (the local session window itself is fixed in local time and therefore
        does not shift; the UTC blocks it overlaps DO shift across the year).
        Forex/commodity (near-24h markets) keep the flat EXPECTED_BARS=4 —
        Section 4 of the audit notes these are not materially affected.


Output columns (in addition to open/high/low/close/volume/vwap):
    bar_count:         int — number of sub-bars aggregated into this bar  [FIX Bug 4]
    is_incomplete_bar: bool — True if bar_count < EXPECTED_BARS['4H']   [FIX Bug 4]

Passthrough columns preserved if present in input (FIX Bug 5):
    _source, symbol, _tz_hint

CATATAN: _aggregate_weekly() dan _aggregate_monthly() DIHAPUS di v1.5.
    1W dan 1M adalah raw Bronze data langsung dari yfinance (interval=1wk/1mo).
    Keduanya adalah dead code — tidak ada pemanggilan 1D→1W atau 1D→1M di codebase.
"""

from __future__ import annotations

from typing import Literal

import polars as pl
from loguru import logger

# v1.5: Hanya 4H yang didukung — 1W/1M adalah raw Bronze data dari yfinance
TF = Literal["4H"]

# Expected sub-bar count per aggregated bar — used for is_incomplete_bar flag.
# FIX Bug 4: a block with fewer sub-bars than expected is flagged for downstream
# quality assessment. Silver is_clean logic can use this to set is_clean=False.
#
# Thresholds are conservative (not every block will be "full" due to session
# boundaries), so is_incomplete_bar is informational — not a hard reject.
EXPECTED_BARS: dict[str, int] = {
    "4H": 4,   # 4 × 1H bars per 4-hour block — flat fallback (near-24h / unknown markets)
}

# FIX NEW-3 [P1 HIGH]: regular trading session expressed in LOCAL EXCHANGE TIME.
# (iana_tz_name, open_hour, open_minute, close_hour, close_minute)
#
#   us_stocks / index: NYSE/NASDAQ regular session, 09:30-16:00 America/New_York.
#       Real session hours (not the rate-limiter's intentionally conservative
#       "8h" sizing buffer used in tvdatafeed_adapter.py's N_BARS_PER_DAY — that
#       value is a safe-over-estimate for fetch sizing, not a completeness spec).
#   idx: 09:00-14:30 Asia/Jakarta — reuses the exact session window already
#       established by FIX TVA-3 (tvdatafeed_adapter.py) for internal consistency;
#       not re-derived independently here.
#
# Because conversion from UTC to local time uses the IANA tz database (DST-aware,
# same as OHLCVProcessor._normalize_timestamps), which UTC hours fall inside this
# LOCAL window shifts across the year automatically — no separate DST table needed.
MARKET_SESSION_LOCAL: dict[str, tuple[str, int, int, int, int]] = {
    "us_stocks": ("America/New_York", 9, 30, 16, 0),
    "index":     ("America/New_York", 9, 30, 16, 0),
    "idx":       ("Asia/Jakarta",      9,  0, 14, 30),
}

# Markets trading (near-)continuously — session-overlap adjustment would add
# complexity without meaningfully changing the result (audit §4: "Forex/commodity
# yang trading mendekati 24 jam relatif tidak terdampak"). These keep the flat
# EXPECTED_BARS['4H'] fallback, identical to pre-fix behavior.
NEAR_24H_MARKETS: frozenset[str] = frozenset({"forex", "commodity"})

# Metadata columns that must survive group_by aggregation (FIX Bug 5).
# These have uniform values within a single-symbol DataFrame — all sub-bars
# share the same _source (from ChainedAdapter) and symbol.
_PASSTHROUGH_COLS: tuple[str, ...] = ("_source", "symbol", "_tz_hint")


# ── Public entry point ─────────────────────────────────────────────────────────

def aggregate_ohlcv(
    df: pl.DataFrame,
    from_tf: str,
    to_tf: TF,
    symbol: str,
    market: str = "",
) -> pl.DataFrame:
    """
    Aggregate Silver OHLCV from 1H to 4H.

    v1.5: Hanya kombinasi 1H→4H yang didukung (GD §4.1 Enrichment).
    1W dan 1M diambil langsung sebagai raw data dari yfinance Bronze layer.

    Args:
        df:      Silver 1H DataFrame. Required: timestamp, open, high, low, close, volume.
                 Optional (preserved if present): _source, symbol, _tz_hint.
                 Input HARUS sudah UTC-normalized (output dari OHLCVProcessor).
        from_tf: Source timeframe string — harus '1H'.
        to_tf:   Target timeframe — harus '4H'.
        symbol:  Symbol name — used for logging and error messages.
        market:  Instrument market ('us_stocks' | 'idx' | 'index' | 'forex' |
                 'commodity'). FIX NEW-3: drives session-aware expected sub-bar
                 count for is_incomplete_bar. Default "" preserves the legacy
                 flat EXPECTED_BARS['4H']=4 behavior for callers that omit it
                 (e.g. existing unit tests that don't yet pass market) —
                 production usage (OHLCVProcessor.synthesize_4h) always passes
                 the real market.

    Returns:
        Aggregated 4H DataFrame dengan schema:
            timestamp, open, high, low, close, volume, vwap,
            bar_count, is_incomplete_bar
            [+ _source, symbol, _tz_hint if present in input]

    Raises:
        ValueError: If from_tf/to_tf combination is not supported (only 1H→4H).
    """
    if df.is_empty():
        return df

    # FIX Bug 7: validate upfront — don't silently produce wrong output
    if not can_aggregate(from_tf, to_tf):
        raise ValueError(
            f"[Aggregator] {symbol}: unsupported aggregation {from_tf!r}→{to_tf!r}. "
            f"Silver layer hanya mendukung 1H→4H. "
            f"1W dan 1M adalah raw Bronze data dari yfinance."
        )

    # Only 4H synthesis supported — can_aggregate() guarantees this
    return _aggregate_4h(df, symbol, market)


# ── Core aggregator ────────────────────────────────────────────────────────────

def _aggregate_4h(df: pl.DataFrame, symbol: str, market: str = "") -> pl.DataFrame:
    """
    Group Silver 1H bars into UTC-aligned 4H blocks.

    Block boundaries (UTC): [00-03],[04-07],[08-11],[12-15],[16-19],[20-23].
    Timestamp of the output bar = canonical block-start (UTC midnight + block_hour).

    Input sudah UTC karena berasal dari Silver 1H (OHLCVProcessor UTC-normalize).
    Safety check tetap ada via FIX R1 (timezone assertion).

    FIX Bug 1 (CRITICAL): sort_by("timestamp") inside every first()/last() agg
        expression. group_by() uses hash-based parallelism and does NOT guarantee
        row order within groups, even after df.sort(). Without sort_by, open picks
        an arbitrary sub-bar's price (not the chronologically first), and close picks
        an arbitrary sub-bar's price (not the chronologically last).

    FIX Bug 2 (CRITICAL): _tp_vol = typical_price × volume is computed per sub-bar
        BEFORE group_by. VWAP = sum(_tp_vol) / sum(volume) over the block.
        Computing (H+L+C)/3 on the aggregated OHLC uses block-level max(H) and
        min(L) — not the per-bar prices — yielding errors up to 1.93% on
        heavy-volume-skew sessions (typical at market open/close).

    FIX Bug 3 (HIGH): canonical block-start timestamp reconstructed from
        _date + _block_hour (integer arithmetic), not from first() of sub-bar
        timestamps. If the first sub-bar of a block is missing (gap/holiday),
        first() gives a wrong timestamp (e.g., 09:00 instead of 08:00), breaking
        MTF alignment queries that expect exact block-start times.

    FIX Bug 4 (HIGH): bar_count and is_incomplete_bar added for downstream
        Silver quality assessment. A block with bar_count < expected is flagged.

    FIX Bug 5 (HIGH): _source, symbol, _tz_hint captured before group_by and
        restored after — group_by().agg() silently drops non-aggregated columns.

    FIX NEW-3 (HIGH): "expected" sub-bar count is no longer a flat 4 — it is
        computed per block as the number of the block's 4 candidate UTC hours
        that overlap the instrument's regular LOCAL trading session (see
        MARKET_SESSION_LOCAL / _expected_bars_by_block). A block legitimately
        spanning session open/close (e.g. only 2 of its 4 hours are trading
        hours) is no longer flagged incomplete merely for having < 4 sub-bars.
    """
    df = df.sort("timestamp")
    passthrough_vals = _capture_passthrough(df)

    # FIX Bug 2: compute per-sub-bar tp_vol BEFORE aggregation
    df = df.with_columns([
        pl.col("timestamp").dt.date().alias("_date"),
        ((pl.col("timestamp").dt.hour() // 4) * 4).cast(pl.Int32).alias("_block_hour"),
        (
            (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
            * pl.col("volume").cast(pl.Float64)
        ).alias("_tp_vol"),
    ])

    # FIX Bug 1 + Bug 2: sort_by inside agg; VWAP from _tp_vol sum
    agg = (
        df.group_by(["_date", "_block_hour"])
        .agg([
            pl.col("open").sort_by("timestamp").first().alias("open"),   # FIX Bug 1
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").sort_by("timestamp").last().alias("close"),  # FIX Bug 1
            pl.col("volume").sum().alias("volume"),
            # FIX Bug 2: true VWAP from sub-bar tp×vol sums
            (pl.col("_tp_vol").sum() / pl.col("volume").cast(pl.Float64).sum()).alias("vwap"),
            # FIX Bug 4: sub-bar count for completeness check
            pl.len().alias("bar_count"),
        ])
    )

    # FIX NEW-3: session-aware expected sub-bar count, per block.
    expected = _expected_bars_by_block(agg["_date"], agg["_block_hour"], market)
    agg = agg.with_columns(expected.alias("_expected_bars"))

    # FIX Bug 3: reconstruct canonical block-start timestamp
    # _date (Date) cast to Datetime("us") gives midnight; add block_hour as microseconds.
    # This is always correct regardless of which sub-bars are present in the block.
    agg = agg.with_columns([
        (
            pl.col("_date").cast(pl.Datetime("us"))
            + (pl.col("_block_hour").cast(pl.Int64) * 3_600_000_000)
              .cast(pl.Duration("us"))
        ).alias("timestamp"),
        # FIX NEW-3: completeness flag now session-aware. A block with
        # _expected_bars == 0 has no overlap with the trading session at all
        # (structurally never expected to have data) and is never flagged
        # incomplete regardless of bar_count.
        pl.when(pl.col("_expected_bars") > 0)
          .then(pl.col("bar_count") < pl.col("_expected_bars"))
          .otherwise(False)
          .alias("is_incomplete_bar"),
    ])

    agg = (
        agg.sort("timestamp")
           .drop(["_date", "_block_hour", "_expected_bars"])
    )

    # FIX Bug 5: restore passthrough columns dropped by group_by
    agg = _restore_passthrough(agg, passthrough_vals)

    incomplete_count = agg["is_incomplete_bar"].sum()
    logger.debug(
        f"[Aggregator] {symbol}: 1H→4H | {len(df)} sub-bars → {len(agg)} 4H bars"
        + (f" | {incomplete_count} incomplete blocks" if incomplete_count else "")
    )
    return agg


# ── Helpers ────────────────────────────────────────────────────────────────────

def _expected_bars_by_block(
    block_dates: pl.Series,
    block_hours: pl.Series,
    market: str,
) -> pl.Series:
    """
    FIX NEW-3 [P1 HIGH]: compute the expected sub-bar count for each 4H block.

    For markets with a defined regular LOCAL trading session
    (MARKET_SESSION_LOCAL), the expected count is the number of the block's 4
    candidate UTC hours [block_hour, block_hour+1, block_hour+2, block_hour+3]
    whose corresponding 1-hour window OVERLAPS the session window when both
    are compared in local exchange time. Standard interval-overlap test:
        bar [h, h+1) overlaps [open, close)  ⟺  h < close AND h+1 > open
    This correctly includes a boundary hour that only partially overlaps the
    session (e.g. a bar starting just before open, ending just after it).

    UTC → local conversion uses the IANA tz database via Polars
    dt.convert_time_zone (same mechanism as OHLCVProcessor._normalize_timestamps),
    so the result is DST-correct: which UTC hours map into the session window
    shifts automatically across the year, without a separate DST lookup table.

    Markets in NEAR_24H_MARKETS, or any market not present in
    MARKET_SESSION_LOCAL (including market="" — the legacy-compatible default),
    fall back to the flat EXPECTED_BARS['4H'] for every block — i.e. unchanged
    pre-fix behavior.

    Args:
        block_dates: pl.Series[Date] — UTC calendar date of each block (one
                     row per (date, block_hour) pair from the group_by).
        block_hours: pl.Series[Int32] — block-start hour (0,4,8,12,16,20).
        market:      Instrument market string.

    Returns:
        pl.Series[Int32] — expected sub-bar count per row, same length/order
        as the inputs.
    """
    n = len(block_dates)
    if n == 0:
        return pl.Series("_expected_bars", [], dtype=pl.Int32)

    if market in NEAR_24H_MARKETS or market not in MARKET_SESSION_LOCAL:
        return pl.Series("_expected_bars", [EXPECTED_BARS["4H"]] * n, dtype=pl.Int32)

    tz_name, open_h, open_m, close_h, close_m = MARKET_SESSION_LOCAL[market]
    open_minutes  = open_h * 60 + open_m
    close_minutes = close_h * 60 + close_m

    base = (
        pl.DataFrame({"_blk_date": block_dates, "_blk_hour": block_hours})
        .with_row_index("_blk_id")
    )
    long = (
        base.join(pl.DataFrame({"_offset": [0, 1, 2, 3]}), how="cross")
        .with_columns(
            (pl.col("_blk_hour") + pl.col("_offset")).alias("_cand_hour")
        )
        .with_columns(
            (
                pl.col("_blk_date").cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
                + (pl.col("_cand_hour").cast(pl.Int64) * 3_600_000_000)
                  .cast(pl.Duration("us"))
            ).alias("_cand_ts_utc")
        )
        .with_columns(
            pl.col("_cand_ts_utc").dt.convert_time_zone(tz_name).alias("_cand_ts_local")
        )
        .with_columns(
            (
                pl.col("_cand_ts_local").dt.hour().cast(pl.Int32) * 60
                + pl.col("_cand_ts_local").dt.minute().cast(pl.Int32)
            ).alias("_local_min_start")
        )
        .with_columns(
            # Standard interval overlap: [local_min_start, +60) ∩ [open, close) != ∅
            (
                (pl.col("_local_min_start") < close_minutes)
                & ((pl.col("_local_min_start") + 60) > open_minutes)
            ).alias("_in_session")
        )
    )

    expected = (
        long.group_by("_blk_id")
            .agg(pl.col("_in_session").sum().cast(pl.Int32).alias("_expected_bars"))
            .sort("_blk_id")
    )
    return expected["_expected_bars"]


def _capture_passthrough(df: pl.DataFrame) -> dict[str, object]:
    """
    FIX Bug 5: capture scalar values of metadata columns before group_by drops them.

    Assumes uniform values within a single-symbol DataFrame — all sub-bars of a
    symbol share the same _source (set by ChainedAdapter), symbol name, and _tz_hint.
    Capturing the first row's value is therefore deterministic and correct.

    Returns:
        {col_name: scalar_value} for each _PASSTHROUGH_COL present in df.
    """
    captured: dict[str, object] = {}
    for col in _PASSTHROUGH_COLS:
        if col in df.columns and len(df) > 0:
            captured[col] = df[col][0]
    return captured


def _restore_passthrough(
    agg: pl.DataFrame,
    passthrough_vals: dict[str, object],
) -> pl.DataFrame:
    """
    FIX Bug 5: re-add metadata columns after aggregation as literal columns.

    Skips any column already present in agg (prevents overwriting if the
    aggregation somehow preserved it via group_by key).
    """
    if not passthrough_vals:
        return agg
    return agg.with_columns([
        pl.lit(val).alias(col)
        for col, val in passthrough_vals.items()
        if col not in agg.columns
    ])


def can_aggregate(from_tf: str, to_tf: str) -> bool:
    """
    Return True if the from_tf → to_tf aggregation is supported.

    v1.5: Silver layer hanya mendukung 1H→4H.
    1W dan 1M adalah raw Bronze data dari yfinance — bukan hasil sintesis.

    Previously in Bronze layer this also supported ('1D', '1W') dan ('1D', '1M'),
    but those were dead code — market_ingester never called them (yfinance provides
    1W and 1M directly via interval='1wk' and interval='1mo').

    FIX Bug 7: digunakan oleh aggregate_ohlcv() untuk validasi input upfront.
    """
    _SUPPORTED: frozenset[tuple[str, str]] = frozenset({
        ("1H", "4H"),   # Satu-satunya kombinasi valid — 4H synthetic dari Silver 1H
    })
    return (from_tf, to_tf) in _SUPPORTED
