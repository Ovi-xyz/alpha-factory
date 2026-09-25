"""
signal_aggregation.py — Gold Layer — Architecture v2.0 §5.2.7, §5.3
Signal Aggregation: per-symbol composite indicator score + per-sector
breadth analytics, computed from gold_signals' tech_signals_{TF}.parquet
output, scoped to the Layer 1 active_ohlcv universe.

CONTEXT (why this module didn't exist until now): job_registry.py's
'gold_global_regime' entry has carried a comment since GMI Wave 1 Cycle 4
(Architecture v2.0 §6.5) explicitly deferring this exact module —
"that wiring belongs to a later signal_aggregation/screener-integration
pass ... out of scope for this module." active_symbols.py's own module
docstring already lists 'signal_aggregation' as a consumer of
ActiveSymbolsResolver.load_ohlcv() — this module fulfils both waiting
references.

Output columns (Architecture v2.0 §5.2.7 table):
    composite_score, composite_grade, sector_breadth_pct,
    sector_momentum, breadth_divergence

None of these are spelled out to the formula level anywhere upstream —
Architecture v2.0 §5.3 only says composite_score is a "weighted avg of
normalized RSI, MACD momentum, ADX trend strength, and relative_volume
across timeframes." The concrete formulas below are this module's own
explicit, documented design decision (ways-of-working: "Scope decisions
must be explicit ... belong in module docstrings", not left implicit) —
tunable later, not asserted as the one true weighting scheme:

  Per timeframe (latest bar only, from tech_signals_{TF}.parquet), four
  components, each independently bounded to roughly [-1, 1] and
  null-safe (a missing/invalid input yields a null component, not a
  crash or a silently-wrong zero):

    rsi_component    = clip((rsi_14 - 50) / 50, -1, 1)
                        RSI is already bounded [0, 100]; centering on the
                        neutral 50 line and scaling by 50 gives a signed
                        [-1, 1] momentum read.
    macd_component    = tanh(macd_hist / atr_14)             [null if atr_14 <= 0]
                        macd_hist is priced in absolute terms (varies by
                        symbol price level) — dividing by ATR_14 makes it
                        scale-free before bounding with tanh.
    adx_component     = min(adx / 50, 1) * sign(di_plus - di_minus)
                        ADX (unsigned trend strength, [0, 100], >=25
                        conventionally "trending") gets its direction
                        from DI+/DI-; dividing by 50 saturates at 1.0
                        around ADX=50 (a very strong trend), matching
                        ADX's own convention that >50 is already extreme.
    volume_component  = tanh(relative_volume - 1.0)
                        relative_volume is centered on 1.0 (== the 20D
                        average); this is a volume-expansion CONFIRMATION
                        term, not itself directional — expanding volume
                        reinforces whatever direction the other three
                        components already indicate once averaged, exactly
                        the reinforcing (not independently-directional)
                        role Architecture v2.0 §5.3 assigns it.

  tf_composite_{TF} = mean of whichever of the 4 components are non-null
                       for that timeframe (pl.mean_horizontal — skips
                       nulls rather than propagating them; a timeframe
                       with zero non-null components contributes null,
                       not zero, to the cross-TF average below — a zero
                       there would be indistinguishable from "confirmed
                       neutral", not "no data").

  composite_score = mean across TIMEFRAMES of whichever tf_composite_{TF}
                     values are non-null (same null-skip behaviour, one
                     level up) — equal-weighted across the 5 active
                     timeframes (matches mtf_alignment.py's own
                     equal-weighted -5..+5 sum; no per-TF weighting
                     scheme is asserted here either). Symbols with ZERO
                     non-null timeframes (all 4 components missing on
                     every TF — e.g. a brand-new active_ohlcv symbol with
                     insufficient indicator warm-up history) get
                     composite_score = 0.0 (neutral default, matching this
                     codebase's existing fallback convention — e.g.
                     sector_rotation.py's NEUTRAL_WEIGHTS, mtf_alignment's
                     fill_null(0) for a missing TF trend) rather than
                     null, so that "no nulls for active symbols"
                     (Architecture v2.0 §10.2 testing requirement) holds
                     — auditable via tf_coverage_count (0-5) rather than
                     silently indistinguishable from a genuinely-computed
                     neutral score.

  composite_grade  = bucket on |composite_score| — A >= 0.60, B >= 0.35,
                      C >= 0.15, D otherwise. Thresholds are this module's
                      own choice (no upstream spec value exists); tunable.

  sector_breadth_pct = 100 * (count of active_ohlcv symbols in a sector
                        whose latest 1D close > ema_50) / (count of
                        active_ohlcv symbols in that sector with 1D data)
                        — Architecture v2.0 §5.3's literal "% symbols in
                        sector above EMA_50 (daily)" definition. Sector
                        grouping reuses sector_rotation.py's own already-
                        published 'sector' column (sector_regime_weights
                        .parquet: GICS sector for us_stocks, else market
                        name) rather than recomputing a second, competing
                        sector definition — falls back to InstrumentLoader
                        directly (same 'inst.sector or inst.market' rule)
                        only if that file isn't available yet.

  sector_momentum   = sector_breadth_pct(run_date) minus the most recent
                        prior signal_aggregation_{date}.parquet's
                        sector_breadth_pct for that sector, found by
                        scanning this module's own output directory for
                        the file dated closest to run_date - 7 calendar
                        days (~5 trading days) within a
                        [run_date-10, run_date-4] window. Null (not 0.0 —
                        genuinely unknown, not "no change") when no prior
                        output exists yet (first ever run, or a gap wider
                        than the window).

  breadth_divergence = (mtf_score / 5.0) - composite_score — both terms
                        rescaled onto the same [-1, 1] axis
                        (mtf_alignment.py's mtf_score already ranges
                        -5..+5). Large |divergence| flags a symbol where
                        the multi-timeframe trend structure and the
                        near-term momentum/volume composite disagree — a
                        genuine data-quality/conviction signal for
                        Trading Engine to weigh, per Architecture v2.0
                        §5.3's own "breadth_divergence ... quality
                        signal" framing. Null (not 0.0) when mtf_score is
                        unavailable for that symbol/date — see dependency
                        note below.

DEPENDENCY DESIGN — soft, not hard (deliberate divergence from
Architecture v2.0 §6.6's own dependency-graph sketch, which lists
gold_mtf as an input): job_registry.py's 'signal_aggregation' entry
depends_on ONLY ['gold_signals', 'silver_active_symbols'] — the two
genuinely load-bearing inputs (tech_signals_{TF}.parquet and the
active_ohlcv universe list). mtf_alignment_{date}.parquet (for
breadth_divergence) and sector_regime_weights.parquet (for sector
labels) are read directly by this module with a try/except-and-degrade-
to-null/fallback pattern — NOT registered as hard JOB_REGISTRY
dependencies. This mirrors the graceful-degrade pattern screener.py
already established for global_regime_tbl/lead_lag_tbl/forecast_tbl
(see that module's own docstring): composite_score and sector_breadth_pct
are this module's core, always-computable output; mtf_score is a
value-add enrichment on top, not a hard prerequisite. A hard dependency
here would mean a missing/stale gold_mtf run blocks composite_score too
— a reliability regression for a Separation-of-Concerns "informational
DATA field, not a filter/ranking input" (GD §0.2/§0.3), exactly the kind
of unnecessary fragility Agent 4/Agent 9 (Alpha Factory Compiler
Reliability/Adversarial passes) exist to catch. For the identical
reason, this module is NOT added to gold_screener's own depends_on
either — it is wired into screener.py as a new soft/optional source
(same has_X / try-except / _empty_*_df() / LEFT JOIN idiom already used
there for active_tbl/sector_tbl/global_regime_tbl/lead_lag_tbl/
forecast_tbl), not a blocking one. See screener.py's own module
docstring for the corresponding integration note.

Output: data/gold/signal_aggregation/signal_aggregation_{date}.parquet
Schema:
    symbol, market, sector, composite_score, composite_grade,
    tf_coverage_count, mtf_score, breadth_divergence,
    sector_breadth_pct, sector_momentum, sector_symbol_count,
    agg_version, date
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import polars as pl
from loguru import logger

from src.gold.technical_signals import TIMEFRAMES  # single source of truth (avoid a 3rd copy)
from src.utils.atomic_io import atomic_write_parquet
from src.utils.progress_checkpoint import ProgressCheckpoint

GOLD_SIGNALS_PATH    = Path("data/gold/signals")
GOLD_MTF_PATH         = Path("data/gold/mtf")
GOLD_SECTOR_PATH      = Path("data/gold/sector/sector_regime_weights.parquet")
GOLD_SIGNAL_AGG_PATH  = Path("data/gold/signal_aggregation")

AGG_VERSION = "1.0"

# ── composite_grade thresholds — this module's own choice, tunable ──────────
GRADE_A_THRESHOLD = 0.60
GRADE_B_THRESHOLD = 0.35
GRADE_C_THRESHOLD = 0.15

# ── sector_momentum lookback window (calendar days) ──────────────────────────
# ~5 trading days back, tolerant of weekends the same way silver_macro's
# stale_tolerance windows elsewhere in this codebase are (job_registry.py).
SECTOR_MOMENTUM_TARGET_DAYS = 7
SECTOR_MOMENTUM_MIN_DAYS    = 4
SECTOR_MOMENTUM_MAX_DAYS    = 10


def run(run_date: date) -> None:
    """Job entry point for the 'signal_aggregation' job."""
    checkpoint = ProgressCheckpoint("signal_aggregation", run_date)
    if checkpoint.is_done("ALL"):
        logger.info("[signal_aggregation] Already done — skipping")
        return

    try:
        df = _compute(run_date)
        if df is None or df.is_empty():
            logger.warning(f"[signal_aggregation] No candidates for {run_date}")
            checkpoint.mark_done("ALL")
            return

        GOLD_SIGNAL_AGG_PATH.mkdir(parents=True, exist_ok=True)
        out_path = (
            GOLD_SIGNAL_AGG_PATH / f"signal_aggregation_{run_date.isoformat()}.parquet"
        )
        atomic_write_parquet(
            df, out_path, compression="zstd", compression_level=3, row_group_size=50_000
        )
        checkpoint.mark_done("ALL")
        logger.info(f"[signal_aggregation] {len(df):,} symbols → {out_path.name}")
    except Exception as e:
        checkpoint.mark_failed("ALL", e)
        raise


# ── Active universe ───────────────────────────────────────────────────────────

def _active_ohlcv_symbols(run_date: date) -> list[str] | None:
    """
    Return the Layer 1 active_ohlcv symbol list, or None if unavailable.
    None triggers a fallback to "whatever symbols appear in tech_signals"
    in _load_tf_components — degraded (no liquidity filter) but correct,
    matching technical_signals.py's own _resolve_active_ohlcv_symbols()
    fallback shape (not imported directly — that function is private to
    its own module; this is this module's own copy of the same small,
    stable idiom, matching how GOLD_MTF_PATH etc. are independently
    declared per-module across this codebase rather than cross-imported).
    """
    from src.silver.active_symbols import ActiveSymbolsResolver

    try:
        symbols = ActiveSymbolsResolver().load_ohlcv(run_date)
    except FileNotFoundError:
        logger.warning(
            f"[signal_aggregation] active_ohlcv not resolved for {run_date} "
            "(silver_active_symbols not yet run) — falling back to whatever "
            "symbols tech_signals contains for this run."
        )
        return None
    if not symbols:
        logger.warning(
            f"[signal_aggregation] active_ohlcv resolved but empty for {run_date} "
            "— falling back to whatever symbols tech_signals contains."
        )
        return None
    return symbols


# ── Per-timeframe composite components ────────────────────────────────────────

def _load_tf_components(tf: str, active_symbols: list[str] | None) -> pl.DataFrame:
    """
    Read tech_signals_{tf}.parquet, keep the latest bar per symbol, and
    compute the 4 bounded [-1, 1] components + this TF's mean composite.
    Returns columns: symbol, tf_composite_{tf}. Empty DataFrame if the
    file doesn't exist yet or the read fails (graceful — a single missing
    TF must not abort the whole aggregation, matching gold_signals' own
    per-TF checkpoint/continue-on-failure design).
    """
    path = GOLD_SIGNALS_PATH / f"tech_signals_{tf}.parquet"
    if not path.exists():
        logger.debug(f"[signal_aggregation] TF={tf}: no tech_signals yet")
        return pl.DataFrame()

    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'; SET threads=4;")
    try:
        if active_symbols:
            raw = con.execute(
                """
                SELECT symbol, rsi_14, macd_hist, atr_14, adx, di_plus,
                       di_minus, relative_volume
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn
                    FROM read_parquet($path)
                    WHERE symbol = ANY($symbols)
                )
                WHERE rn = 1
                """,
                {"path": str(path), "symbols": active_symbols},
            ).pl()
        else:
            raw = con.execute(
                """
                SELECT symbol, rsi_14, macd_hist, atr_14, adx, di_plus,
                       di_minus, relative_volume
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn
                    FROM read_parquet($path)
                )
                WHERE rn = 1
                """,
                {"path": str(path)},
            ).pl()
    except Exception as e:
        logger.debug(f"[signal_aggregation] TF={tf}: read failed — {e}")
        return pl.DataFrame()

    if raw.is_empty():
        return raw

    col = f"tf_composite_{tf}"
    out = raw.with_columns([
        pl.col("rsi_14").clip(0, 100).sub(50).truediv(50).alias("_rsi_c"),
        pl.when(pl.col("atr_14") > 0)
          .then(pl.col("macd_hist") / pl.col("atr_14"))
          .otherwise(None)
          .tanh()
          .alias("_macd_c"),
        (
            pl.min_horizontal([pl.col("adx").fill_null(0.0) / 50.0, pl.lit(1.0)])
            * pl.when(pl.col("di_plus").is_not_null() & pl.col("di_minus").is_not_null())
                .then((pl.col("di_plus") - pl.col("di_minus")).sign())
                .otherwise(None)
        ).alias("_adx_c"),
        (pl.col("relative_volume") - 1.0).tanh().alias("_vol_c"),
    ]).with_columns(
        pl.mean_horizontal(["_rsi_c", "_macd_c", "_adx_c", "_vol_c"]).alias(col)
    ).select(["symbol", col])

    return out


def _compute_composite_scores(
    run_date: date, active_symbols: list[str] | None
) -> pl.DataFrame:
    """
    Merge per-TF composites across TIMEFRAMES (full/outer join on symbol —
    a symbol missing one TF must not be dropped, the exact silent-drop
    failure class this codebase has repeatedly found and fixed, e.g.
    ADR-046 Path C, FIX GLD-L2-01, RISK-6). Returns:
        symbol, composite_score, composite_grade, tf_coverage_count
    """
    tf_frames = [_load_tf_components(tf, active_symbols) for tf in TIMEFRAMES]
    tf_cols = [f"tf_composite_{tf}" for tf in TIMEFRAMES]
    non_empty = [df for df in tf_frames if not df.is_empty()]

    if not non_empty:
        return pl.DataFrame({
            "symbol":            pl.Series([], dtype=pl.Utf8),
            "composite_score":   pl.Series([], dtype=pl.Float64),
            "composite_grade":   pl.Series([], dtype=pl.Utf8),
            "tf_coverage_count": pl.Series([], dtype=pl.Int32),
        })

    merged = non_empty[0]
    for df in non_empty[1:]:
        merged = merged.join(df, on="symbol", how="full", coalesce=True)

    # Ensure every TIMEFRAMES column exists even if that TF had no data at all.
    for c in tf_cols:
        if c not in merged.columns:
            merged = merged.with_columns(pl.lit(None).cast(pl.Float64).alias(c))

    merged = merged.with_columns([
        pl.mean_horizontal(tf_cols).alias("_raw_composite"),
        pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int32) for c in tf_cols])
          .alias("tf_coverage_count"),
    ]).with_columns(
        # No nulls for active symbols (Architecture v2.0 §10.2): a symbol
        # with zero non-null TF composites (tf_coverage_count == 0)
        # defaults to a neutral 0.0, auditable via tf_coverage_count.
        pl.col("_raw_composite").fill_null(0.0).clip(-1.0, 1.0).alias("composite_score")
    ).with_columns(
        pl.when(pl.col("composite_score").abs() >= GRADE_A_THRESHOLD).then(pl.lit("A"))
          .when(pl.col("composite_score").abs() >= GRADE_B_THRESHOLD).then(pl.lit("B"))
          .when(pl.col("composite_score").abs() >= GRADE_C_THRESHOLD).then(pl.lit("C"))
          .otherwise(pl.lit("D"))
          .alias("composite_grade")
    ).select(["symbol", "composite_score", "composite_grade", "tf_coverage_count"])

    return merged


# ── Sector labels ──────────────────────────────────────────────────────────────

def _load_sector_map() -> pl.DataFrame:
    """
    Return symbol -> (market, sector) for the Layer 1 universe.
    Primary: sector_regime_weights.parquet (gold_sector's own published
    'sector' column — reuse, don't recompute a second definition).
    Fallback: InstrumentLoader directly, same 'inst.sector or inst.market'
    rule sector_rotation.py itself uses, if gold_sector hasn't run yet.
    """
    if GOLD_SECTOR_PATH.exists():
        try:
            return pl.read_parquet(GOLD_SECTOR_PATH).select(["symbol", "market", "sector"])
        except Exception as e:
            logger.debug(f"[signal_aggregation] sector_regime_weights read failed: {e}")

    try:
        from src.config.instrument_loader import get_loader
        loader = get_loader()
        rows = [
            {"symbol": inst.symbol, "market": inst.market, "sector": inst.sector or inst.market}
            for inst in loader.all_symbols()
        ]
        return pl.DataFrame(rows) if rows else _empty_sector_map()
    except Exception as e:
        logger.debug(f"[signal_aggregation] InstrumentLoader sector fallback failed: {e}")
        return _empty_sector_map()


def _empty_sector_map() -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": pl.Series([], dtype=pl.Utf8),
        "market": pl.Series([], dtype=pl.Utf8),
        "sector": pl.Series([], dtype=pl.Utf8),
    })


# ── Sector breadth ─────────────────────────────────────────────────────────────

def _compute_sector_breadth(
    run_date: date, active_symbols: list[str] | None, sector_map: pl.DataFrame
) -> pl.DataFrame:
    """
    % of active_ohlcv symbols per sector with latest 1D close > ema_50.
    Returns: sector, sector_breadth_pct, sector_symbol_count.
    Empty (not an error) if tech_signals_1D.parquet doesn't exist yet.
    """
    path = GOLD_SIGNALS_PATH / "tech_signals_1D.parquet"
    if not path.exists():
        return pl.DataFrame({
            "sector":               pl.Series([], dtype=pl.Utf8),
            "sector_breadth_pct":   pl.Series([], dtype=pl.Float64),
            "sector_symbol_count":  pl.Series([], dtype=pl.Int32),
        })

    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'; SET threads=4;")
    try:
        if active_symbols:
            latest = con.execute(
                """
                SELECT symbol, close, ema_50
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn
                    FROM read_parquet($path)
                    WHERE symbol = ANY($symbols)
                )
                WHERE rn = 1
                """,
                {"path": str(path), "symbols": active_symbols},
            ).pl()
        else:
            latest = con.execute(
                """
                SELECT symbol, close, ema_50
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn
                    FROM read_parquet($path)
                )
                WHERE rn = 1
                """,
                {"path": str(path)},
            ).pl()
    except Exception as e:
        logger.debug(f"[signal_aggregation] 1D read for sector breadth failed: {e}")
        return pl.DataFrame({
            "sector":               pl.Series([], dtype=pl.Utf8),
            "sector_breadth_pct":   pl.Series([], dtype=pl.Float64),
            "sector_symbol_count":  pl.Series([], dtype=pl.Int32),
        })

    if latest.is_empty():
        return pl.DataFrame({
            "sector":               pl.Series([], dtype=pl.Utf8),
            "sector_breadth_pct":   pl.Series([], dtype=pl.Float64),
            "sector_symbol_count":  pl.Series([], dtype=pl.Int32),
        })

    joined = latest.join(sector_map.select(["symbol", "sector"]), on="symbol", how="left")
    joined = joined.with_columns(pl.col("sector").fill_null("Unknown"))
    joined = joined.filter(
        pl.col("close").is_not_null() & pl.col("ema_50").is_not_null()
    )
    if joined.is_empty():
        return pl.DataFrame({
            "sector":               pl.Series([], dtype=pl.Utf8),
            "sector_breadth_pct":   pl.Series([], dtype=pl.Float64),
            "sector_symbol_count":  pl.Series([], dtype=pl.Int32),
        })

    breadth = (
        joined.with_columns((pl.col("close") > pl.col("ema_50")).cast(pl.Int32).alias("_above"))
        .group_by("sector")
        .agg([
            (pl.col("_above").sum() * 100.0 / pl.len()).alias("sector_breadth_pct"),
            pl.len().cast(pl.Int32).alias("sector_symbol_count"),
        ])
    )
    return breadth


def _load_prior_sector_breadth(run_date: date) -> pl.DataFrame:
    """
    Find the most recent signal_aggregation_{date}.parquet dated closest
    to run_date - SECTOR_MOMENTUM_TARGET_DAYS, within
    [run_date - MAX_DAYS, run_date - MIN_DAYS]. Returns sector,
    sector_breadth_pct (renamed to prior_sector_breadth_pct) — empty if
    no file in that window exists (e.g. first ever run).
    """
    best_path: Path | None = None
    best_delta: int | None = None
    for offset in range(SECTOR_MOMENTUM_MIN_DAYS, SECTOR_MOMENTUM_MAX_DAYS + 1):
        candidate_date = run_date - timedelta(days=offset)
        candidate = GOLD_SIGNAL_AGG_PATH / f"signal_aggregation_{candidate_date.isoformat()}.parquet"
        if candidate.exists():
            delta = abs(offset - SECTOR_MOMENTUM_TARGET_DAYS)
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_path = candidate

    if best_path is None:
        return pl.DataFrame({
            "sector":                    pl.Series([], dtype=pl.Utf8),
            "prior_sector_breadth_pct":  pl.Series([], dtype=pl.Float64),
        })

    try:
        prior = pl.read_parquet(best_path).select(["sector", "sector_breadth_pct"]).unique(
            subset=["sector"], keep="first"
        )
        return prior.rename({"sector_breadth_pct": "prior_sector_breadth_pct"})
    except Exception as e:
        logger.debug(f"[signal_aggregation] Prior breadth read failed ({best_path}): {e}")
        return pl.DataFrame({
            "sector":                    pl.Series([], dtype=pl.Utf8),
            "prior_sector_breadth_pct":  pl.Series([], dtype=pl.Float64),
        })


# ── MTF scores (soft input — breadth_divergence only) ─────────────────────────

def _load_mtf_scores(run_date: date) -> pl.DataFrame:
    """symbol, mtf_score — empty if mtf_alignment hasn't run for run_date yet."""
    path = GOLD_MTF_PATH / f"mtf_alignment_{run_date.isoformat()}.parquet"
    if not path.exists():
        return pl.DataFrame({
            "symbol":    pl.Series([], dtype=pl.Utf8),
            "mtf_score": pl.Series([], dtype=pl.Int64),
        })
    try:
        return pl.read_parquet(path).select(["symbol", "mtf_score"])
    except Exception as e:
        logger.debug(f"[signal_aggregation] mtf_alignment read failed: {e}")
        return pl.DataFrame({
            "symbol":    pl.Series([], dtype=pl.Utf8),
            "mtf_score": pl.Series([], dtype=pl.Int64),
        })


# ── Orchestration ──────────────────────────────────────────────────────────────

def _compute(run_date: date) -> pl.DataFrame:
    active_symbols = _active_ohlcv_symbols(run_date)

    composite = _compute_composite_scores(run_date, active_symbols)
    if composite.is_empty():
        return composite

    sector_map = _load_sector_map()
    df = composite.join(sector_map, on="symbol", how="left")
    df = df.with_columns([
        pl.col("sector").fill_null("Unknown"),
        pl.col("market").fill_null("Unknown"),
    ])

    # Sector breadth + momentum — broadcast sector-level metrics onto each
    # member symbol row.
    breadth = _compute_sector_breadth(run_date, active_symbols, sector_map)
    prior_breadth = _load_prior_sector_breadth(run_date)
    breadth = breadth.join(prior_breadth, on="sector", how="left")
    breadth = breadth.with_columns(
        (pl.col("sector_breadth_pct") - pl.col("prior_sector_breadth_pct")).alias("sector_momentum")
    ).drop("prior_sector_breadth_pct")

    df = df.join(breadth, on="sector", how="left")

    # MTF (soft) — breadth_divergence null when unavailable, not 0.0.
    mtf = _load_mtf_scores(run_date)
    df = df.join(mtf, on="symbol", how="left")
    df = df.with_columns(
        (pl.col("mtf_score") / 5.0 - pl.col("composite_score")).alias("breadth_divergence")
    )

    df = df.with_columns([
        pl.lit(AGG_VERSION).alias("agg_version"),
        pl.lit(str(run_date)).alias("date"),
    ])

    return df.select([
        "symbol", "market", "sector",
        "composite_score", "composite_grade", "tf_coverage_count",
        "mtf_score", "breadth_divergence",
        "sector_breadth_pct", "sector_momentum", "sector_symbol_count",
        "agg_version", "date",
    ])


def load_signal_aggregation(run_date: date) -> pl.DataFrame:
    """Load a previously written signal_aggregation output for run_date."""
    path = GOLD_SIGNAL_AGG_PATH / f"signal_aggregation_{run_date.isoformat()}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"signal_aggregation not resolved for {run_date}")
    return pl.read_parquet(path)
