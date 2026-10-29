"""
screener.py — GD §5.2.4 (Gold Screener & Watchlist)
DuckDB query join: MTF + Correlation + Context Regime (+ cross-asset / signal aggregation).

ADR-055 v2 (GMI-HMM2-CUT-001) — THE DESIGNATED INTERFACE OF THE (FUTURE) TRADING ENGINE. The Gold layer is organised as
four engines — technical (gold_signals, gold_mtf, signal_aggregation), cross-asset
(gold_global_regime, gold_cross_asset_correlation, gold_lead_lag, gold_forecast), dual-HMM regime
(gold_raw_macro/market, gold_hmm_macro/market_regime, gold_context_regime + two fit jobs) and the
specialist/decision intelligence on top (ADR-052..054) — and this module is where they meet:
gold_screener is the one output the Trading Engine contract will point at. No Trading Engine
exists yet — the Gold layer is still unstable and unfinished — so this schema is provisional and
nothing outside the repo depends on it; the contract is formalised when the Gold layer is done
(ADR-054). The retired rule-based
regime (macro_regime.py / regime_store.parquet: regime, regime_composite, regime_confidence,
regime_transition, transition_alert) is replaced by the dual-HMM context regime, joined as-of from
data/gold/hmm/context_regime.parquet (soft source, same degrade-to-NULL idiom as every other
optional source here):

    context_date, context_age_days      which context row, and how stale it is vs run_date
                                        (beyond CONTEXT_REGIME_TOLERANCE_DAYS -> all NULL, never a
                                        silently month-old state)
    macro_label, macro_activity, macro_momentum
                                        expansion | slowdown | recession | recovery (ENVIRONMENT)
    market_label, market_probability, market_score, market_age_days
                                        risk_off | mixed | risk_on (BEHAVIOUR)
    context_label                       "<macro_label>/<market_label>"
    context_transition, context_transition_alert
                                        the label changed vs the previous known one (data, not a
                                        decision — GD §0.3)
    regime_in_sample                    True if the row lies inside a model's training window;
                                        anything judging the regime's accuracy must filter on it

Also gone, for the same reason: `sector_weight_adj`, the sector concentration guard built on it,
and the `sector` column itself. `gold_sector` (regime -> sector weights) was archived with
macro_regime.py — regime-free it had nothing left to do — and the watchlist carries no sector
label (Ovi, 21 Oct 2026). The sector *breadth* columns (`sector_breadth_pct`, `sector_momentum`)
still arrive from signal_aggregation, joined by symbol.
The screener is the only job allowed to read the HMM engine's store besides the HMM engine itself
and PITDataLoader: it is the synthesis tip, not one of the three independent domains.

FIX ADR-043/ADR-044 (GMI_Decision_Document_v10.docx): Finnhub retired in
full — _enrich_earnings() and _enrich_sentiment() (and their call sites)
removed. days_to_earnings, next_earnings_date, near_earnings_flag,
sentiment_score, and buzz_score remain in the watchlist output schema
exactly as before — sourced entirely from this module's own main query,
which hardcodes each as a typed NULL placeholder. No Interface Contract
change (GD §0.4, §17.6): the promised columns still exist, still
correctly typed, simply permanently null instead of sometimes-populated.

v1.2 additions (retained):
    - Correlation cluster deduplication: max 2 per cluster
      (source: cross_asset_corr.parquet read directly — the legacy
      correlation_clusters.parquet bridge is archived, RISK-31 update 29 Oct 2026)

ADD GMI Wave 1 Cycle 4 — CrossAssetEngine integration (Architecture v2.0
§5.3, §9.1 Phase 5, §10.1 roadmap item "gold_screener: integrate
CrossAssetEngine outputs"). Three new optional sources, following the
EXACT same soft-dependency, graceful-degrade-to-NULL pattern already
established here for active_tbl/regime_tbl — never a filter
or ranking input, purely informational DATA fields (Section 0.2/0.3
Separation of Concerns: pipeline supplies data, Trading Engine decides),
same treatment as days_to_earnings/sentiment_score before Finnhub's
retirement:

    global_regime_tbl (data/gold/cross_asset/global_regime.parquet,
        GlobalIndexRegimeModule, daily) -> global_risk_score,
        global_regime_label, dm_em_divergence. Single-row-per-date
        broadcast table, same LEFT JOIN ... ON TRUE pattern as
        regime_tbl (GLD-SCR-001) for the identical reason: a CROSS JOIN
        against a same-day-missing table would silently zero the whole
        watchlist, not just this table's columns.

    lead_lag_tbl (data/gold/cross_asset/lead_lag_matrix.parquet,
        LeadLagModule, weekly) -> lead_lag_signal (bool: at least one
        BH-significant leader this week), lead_lag_top_leader,
        lead_lag_optimal_lag. Pre-aggregated per follower symbol in
        Python before registration — the raw store is one row per
        (leader, follower), screener wants one row per symbol.

    forecast_tbl (data/gold/cross_asset/cross_asset_forecast.parquet,
        ForecastModule, weekly) -> forecast_return_1d, forecast_stable,
        filtered to horizon_days=1 (nearest-term, most relevant to a
        watchlist of current candidates) — one row per symbol.

None of these three are in gold_screener's depends_on (job_registry.py) —
deliberately, matching silver_active_symbols' own precedent (feeds
active_tbl, also not a hard dependency): all three degrade to empty/NULL
via the same has_X / try-except / _empty_*_df() idiom already used for
active_tbl/regime_tbl, so a missing or stale (weekly, possibly
several days old within the week) CrossAssetEngine output never blocks
the screener the way a missing gold_mtf does.

ADD GMI-SIGAGG-001 — signal_aggregation integration (Architecture v2.0
§5.2.7, §9.1 Phase 5). A fourth soft source, same idiom as the three
above: signal_agg_tbl (data/gold/signal_aggregation/
signal_aggregation_{date}.parquet, one row per active_ohlcv symbol) ->
composite_score, composite_grade, sector_breadth_pct, sector_momentum,
breadth_divergence. Purely informational DATA fields (Section 0.2/0.3
Separation of Concerns), never a filter or ranking input — identical
treatment to days_to_earnings/sentiment_score before Finnhub's
retirement. NOT in gold_screener's depends_on, for the same reliability
reason as the three CrossAssetEngine sources: see
signal_aggregation.py's own module docstring for the full rationale
(this deliberately supersedes Architecture v2.0 §6.6's dependency-graph
sketch, which predates this established soft-dependency pattern).

Output: data/gold/screener/watchlist_{date}.parquet
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl
from loguru import logger

from src.config.instrument_loader import get_loader
from src.utils.atomic_io import atomic_write_parquet
from src.utils.silver_scope import layer1_globs
from src.utils.trade_date import with_trade_date  # FIX TS-DATE-01

SILVER_OHLCV_ROOT  = Path("data/silver/market_ohlcv")
GOLD_MTF_PATH       = Path("data/gold/mtf")
GOLD_CONTEXT_REGIME_PATH = Path("data/gold/hmm/context_regime.parquet")   # ADR-055 v2: replaces regime_store
GOLD_SCREENER_PATH  = Path("data/gold/screener")
# ADD GMI Wave 1 Cycle 4 — CrossAssetEngine outputs (see module docstring).
GOLD_CROSS_ASSET_PATH   = Path("data/gold/cross_asset")
GLOBAL_REGIME_PATH      = GOLD_CROSS_ASSET_PATH / "global_regime.parquet"
LEAD_LAG_PATH           = GOLD_CROSS_ASSET_PATH / "lead_lag_matrix.parquet"
CROSS_ASSET_FORECAST_PATH = GOLD_CROSS_ASSET_PATH / "cross_asset_forecast.parquet"
# Cluster source for the GD §15.1 max-per-cluster guard: CorrelationModule's own pairwise output,
# read directly (legacy_correlation_bridge.py and correlation_clusters.parquet are archived).
CROSS_ASSET_CORR_PATH   = GOLD_CROSS_ASSET_PATH / "cross_asset_corr.parquet"
# ADD GMI-SIGAGG-001 — see module docstring.
SIGNAL_AGG_PATH = Path("data/gold/signal_aggregation")
# FIX GLD-SCR-002: this was previously built inline as an ad hoc string/
# f-string Path construction inside build_watchlist() (not the SQL-injection
# kind — a plain filesystem path — but an un-patchable hardcode all the
# same, matching the same class of issue fixed via REGIME_STORE_PATH in
# mtf_alignment.py this same thread). Promoted to a module-level root
# alongside the constants above; the per-run_date filename is still built
# dynamically where it's used.
# FIX ADR-044: SILVER_SENTIMENT and SILVER_SENTIMENT_ROOT removed —
# _enrich_sentiment() (their sole consumer) was removed in the same fix.
SILVER_ACTIVE_SYMBOLS_ROOT = Path("data/silver/active_symbols")

# Screening filters (hard thresholds for watchlist inclusion)
# FIX ADR-046 Path C (GMI_Decision_Document_v11.docx §2, Ovi's path choice):
# recalibrated from 5 to 3 to match mtf_alignment.py's new 5-timeframe score
# range (-5..+5, grade B boundary == 3) after Bronze 1H was wired up and the
# 5m/15m timeframes were dropped from the sum entirely (Path C: "middle
# path", not Path A). See mtf_alignment.py's module docstring for the full
# recalibrated grade table.
MIN_MTF_SCORE      = 3       # |score| >= 3 → grade A or B only
MIN_DOLLAR_VOLUME  = 1_000_000   # USD 1M/day
# MIN_SECTOR_WEIGHT removed (ADR-055 v2): the sector concentration guard needed the regime-keyed
# sector_weight_adj, which no longer exists (gold_sector archived); the `sector` column is gone too.
CONTEXT_REGIME_TOLERANCE_DAYS = 5   # first-pass: same as context_regime.MARKET_TOLERANCE_DAYS
TOP_N_WATCHLIST    = 20
MAX_PER_CLUSTER    = 2       # Correlation concentration guard (GD §15.1)


# ── Empty placeholder DataFrame helpers ───────────────────────────────────────
# FIX GLD-003: digunakan untuk menggantikan '/dev/null' path injection
# di f-string SQL. Arrow table registration ke DuckDB memungkinkan LEFT JOIN
# terhadap tabel kosong tanpa perlu f-string conditional path logic.

# Columns the screener reads from context_regime.parquet (ADR-055 v2), in output order.
CONTEXT_REGIME_READ_COLUMNS = (
    "date", "macro_label", "macro_activity", "macro_momentum",
    "market_label", "market_probability", "market_score", "market_age_days",
    "context_label", "context_transition", "context_transition_alert", "in_sample",
)


def _empty_context_regime_df() -> pl.DataFrame:
    """Placeholder context-regime DataFrame (typed, zero rows) — ADR-055 v2."""
    return pl.DataFrame({
        "context_date":             pl.Series([], dtype=pl.Date),
        "context_age_days":         pl.Series([], dtype=pl.Int32),
        "macro_label":              pl.Series([], dtype=pl.Utf8),
        "macro_activity":           pl.Series([], dtype=pl.Float64),
        "macro_momentum":           pl.Series([], dtype=pl.Float64),
        "market_label":             pl.Series([], dtype=pl.Utf8),
        "market_probability":       pl.Series([], dtype=pl.Float64),
        "market_score":             pl.Series([], dtype=pl.Float64),
        "market_age_days":          pl.Series([], dtype=pl.Int32),
        "context_label":            pl.Series([], dtype=pl.Utf8),
        "context_transition":       pl.Series([], dtype=pl.Boolean),
        "context_transition_alert": pl.Series([], dtype=pl.Utf8),
        "regime_in_sample":         pl.Series([], dtype=pl.Boolean),
    })


def _load_context_regime(run_date: date) -> pl.DataFrame:
    """
    The latest context_regime row on or before run_date, within CONTEXT_REGIME_TOLERANCE_DAYS
    (as-of, PIT-safe), shaped for the main query. Zero rows when the store is missing, empty
    before run_date, too stale, or unreadable (the last two are logged — a schema problem must
    not look like a missing file).
    """
    if not GOLD_CONTEXT_REGIME_PATH.exists():
        return _empty_context_regime_df()
    try:
        df = (pl.read_parquet(GOLD_CONTEXT_REGIME_PATH)
              .select(CONTEXT_REGIME_READ_COLUMNS)
              .with_columns(pl.col("date").cast(pl.Date))
              .filter(pl.col("date") <= run_date)
              .sort("date", descending=True)
              .head(1))
    except Exception as e:
        logger.warning(f"[gold_screener] context_regime unreadable ({e}) — regime columns will be NULL")
        return _empty_context_regime_df()
    if df.is_empty():
        return _empty_context_regime_df()
    age = (run_date - df["date"][0]).days
    if age > CONTEXT_REGIME_TOLERANCE_DAYS:
        logger.warning(
            f"[gold_screener] latest context_regime row is {age} days old "
            f"(> {CONTEXT_REGIME_TOLERANCE_DAYS}) — regime columns will be NULL"
        )
        return _empty_context_regime_df()
    return (df.rename({"date": "context_date", "in_sample": "regime_in_sample"})
              .with_columns(pl.lit(age).cast(pl.Int32).alias("context_age_days"))
              .select(_empty_context_regime_df().columns))


def _empty_active_df() -> pl.DataFrame:
    """Placeholder active symbols DataFrame dengan schema minimal."""
    return pl.DataFrame({
        "symbol":           pl.Series([], dtype=pl.Utf8),
        "dollar_volume_20d": pl.Series([], dtype=pl.Float64),
    })


# ADD GMI Wave 1 Cycle 4 — CrossAssetEngine placeholder schemas, same
# "empty typed DataFrame" idiom as the three above (FIX GLD-003).

def _empty_global_regime_df() -> pl.DataFrame:
    """Placeholder GlobalIndexRegimeModule DataFrame dengan schema minimal."""
    return pl.DataFrame({
        "global_risk_score":   pl.Series([], dtype=pl.Float64),
        "global_regime_label": pl.Series([], dtype=pl.Utf8),
        "dm_em_divergence":    pl.Series([], dtype=pl.Float64),
    })


def _empty_lead_lag_df() -> pl.DataFrame:
    """Placeholder LeadLagModule per-follower aggregate DataFrame dengan schema minimal."""
    return pl.DataFrame({
        "follower":              pl.Series([], dtype=pl.Utf8),
        "lead_lag_signal_count": pl.Series([], dtype=pl.Int64),
        "lead_lag_top_leader":   pl.Series([], dtype=pl.Utf8),
        "lead_lag_top_lag":      pl.Series([], dtype=pl.Int64),
    })


def _empty_forecast_df() -> pl.DataFrame:
    """Placeholder ForecastModule (horizon=1) DataFrame dengan schema minimal."""
    return pl.DataFrame({
        "symbol":              pl.Series([], dtype=pl.Utf8),
        "forecast_return_1d":  pl.Series([], dtype=pl.Float64),
        "forecast_stable":     pl.Series([], dtype=pl.Boolean),
    })


def _empty_signal_agg_df() -> pl.DataFrame:
    """Placeholder signal_aggregation DataFrame dengan schema minimal (ADD GMI-SIGAGG-001)."""
    return pl.DataFrame({
        "symbol":              pl.Series([], dtype=pl.Utf8),
        "composite_score":     pl.Series([], dtype=pl.Float64),
        "composite_grade":     pl.Series([], dtype=pl.Utf8),
        "sector_breadth_pct":  pl.Series([], dtype=pl.Float64),
        "sector_momentum":     pl.Series([], dtype=pl.Float64),
        "breadth_divergence":  pl.Series([], dtype=pl.Float64),
    })


def run(run_date: date) -> None:
    """Job entry point for gold_screener.

    FIX GD-F02: Data Freshness Gate (GD §15.1) — screener tidak boleh
    jalan jika > 5% instrumen tidak punya data terbaru. Gate diperiksa
    SEBELUM build_watchlist() dipanggil.
    """
    # FIX GD-F02: check data freshness before building watchlist
    _check_data_freshness(run_date)

    df = build_watchlist(run_date)
    if df is None or df.is_empty():
        logger.warning(f"[gold_screener] No candidates found for {run_date}")
        return

    GOLD_SCREENER_PATH.mkdir(parents=True, exist_ok=True)
    out_path = GOLD_SCREENER_PATH / f"watchlist_{run_date.isoformat()}.parquet"
    # FIX GLD-004: atomic_write_parquet via tempfile + os.replace
    atomic_write_parquet(df, out_path, compression="zstd", compression_level=3)
    logger.info(
        f"[gold_screener] {len(df)} watchlist candidates → {out_path.name}"
    )


def _check_data_freshness(run_date: date) -> None:
    """FIX GD-F02: Data Freshness Gate — GD §15.1.

    Periksa coverage Silver OHLCV 1D: jika < 95% instrumen (643 total)
    punya bar terbaru dalam 2 hari kerja terakhir, screener diblokir.
    Ini mencegah watchlist yang dihasilkan dari data stale.

    Raises:
        RuntimeError: jika coverage < COVERAGE_MIN_PCT (95%).
    """
    # FIX GLD-005: gunakan get_loader().count() bukan hardcoded 643.
    # Saat instruments.yaml diperluas ke 692 (GMI Architecture Extension),
    # gate ini akan otomatis menggunakan count yang benar tanpa edit manual.
    TOTAL_INSTRUMENTS  = get_loader().count()
    COVERAGE_MIN_PCT   = 95.0
    FRESHNESS_DAYS     = 3      # 2 hari kerja + 1 buffer untuk weekend

    # FIX ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8,
    # 2026-07-11): the old unfiltered "market_ohlcv/**/*_1D_silver.parquet"
    # glob was EXACTLY the same masking-bug class already fixed in
    # quality_validator.py::_check_coverage — COUNT(DISTINCT symbol) from a
    # glob that also matches Layer 2 context OHLCV (VIX, DXY, ETFs, added
    # in GMI Cycle 3, same market_ohlcv/ root), divided by a Layer-1-only
    # denominator (get_loader().count()). A handful of fresh Layer 2
    # anchors could silently push coverage_pct above 100% of the true
    # Layer 1 figure — meaning this gate, whose entire purpose is to BLOCK
    # the screener on stale Layer 1 data, could pass while Layer 1 was
    # actually stale. Found by Gate G-8's static scanner, not by manual
    # code-reading (unlike quality_validator.py's original fix) — this is
    # the exact scenario ADR-022's own rationale anticipated.
    silver_1d_globs = layer1_globs(SILVER_OHLCV_ROOT, "*_1D_silver.parquet")
    if not silver_1d_globs:
        logger.warning(
            "[gold_screener] No Layer 1 Silver 1D data found yet — "
            "skipping freshness gate (pre-backfill state)"
        )
        return

    try:
        con = duckdb.connect()
        # FIX GLD-003: $name parameterized query — f-string SQL dilarang GD §17.7
        result = con.execute(
            with_trade_date("""
            SELECT COUNT(DISTINCT symbol) AS fresh_count
            FROM read_parquet($glob, hive_partitioning=true)
            WHERE __TRADE_DATE__ >= CAST($run_date AS DATE) - INTERVAL (CAST($freshness_days AS INTEGER)) DAY
              AND is_clean = TRUE
            """),
            {
                "glob":          silver_1d_globs,
                "run_date":      str(run_date),
                "freshness_days": FRESHNESS_DAYS,
            },
        ).fetchone()

        fresh_count  = result[0] if result else 0
        coverage_pct = (fresh_count / TOTAL_INSTRUMENTS) * 100

        if coverage_pct < COVERAGE_MIN_PCT:
            raise RuntimeError(
                f"[gold_screener] DATA FRESHNESS GATE FAILED — "
                f"{fresh_count}/{TOTAL_INSTRUMENTS} symbols fresh "
                f"({coverage_pct:.1f}% < {COVERAGE_MIN_PCT}% threshold). "
                f"Screener diblokir per GD §15.1. "
                f"Jalankan ulang bronze_ohlcv_daily + silver_ohlcv terlebih dahulu."
            )
        logger.info(
            f"[gold_screener] Data freshness OK: "
            f"{fresh_count}/{TOTAL_INSTRUMENTS} symbols ({coverage_pct:.1f}%)"
        )
    except RuntimeError:
        raise
    except Exception as e:
        # Jika Silver belum ada (phase awal), skip gate dengan warning
        logger.warning(
            f"[gold_screener] Data freshness check skipped (no Silver data yet): {e}"
        )


def build_watchlist(run_date: date) -> pl.DataFrame:
    """
    Build ranked watchlist via DuckDB multi-source join.
    Returns top-N candidates with full annotation.

    FIX GLD-003: semua path dan values diparameterisasi via DuckDB $name binding
    atau Arrow table registration — tidak ada f-string SQL (GD §17.7 anti-pattern).
    """
    # Check required inputs
    mtf_path = GOLD_MTF_PATH / f"mtf_alignment_{run_date.isoformat()}.parquet"
    if not mtf_path.exists():
        logger.warning(f"[gold_screener] MTF alignment not found for {run_date}")
        return pl.DataFrame()

    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'; SET threads=4;")

    # ── Register optional sources as Arrow tables ─────────────────────────────
    # Pattern: load each optional source into a Polars DF, register ke DuckDB.
    # Jika file tidak ada → register DataFrame kosong dengan schema minimal.
    # Menggantikan /dev/null injection pattern yang tidak cross-platform.
    # FIX GLD-003: tidak ada f-string SQL.

    has_corr    = CROSS_ASSET_CORR_PATH.exists()
    active_sym_path = (
        SILVER_ACTIVE_SYMBOLS_ROOT / f"active_{run_date.isoformat()}.parquet"
    )
    has_active_symbols = active_sym_path.exists()

    # Context regime (dual-HMM, ADR-055 v2) — one as-of row, broadcast via LEFT JOIN ... ON TRUE
    # (GLD-SCR-001: a CROSS JOIN against a legitimately empty table would zero the watchlist).
    con.register("context_regime_tbl", _load_context_regime(run_date).to_arrow())

    # Active symbols table (dollar_volume_20d) — FIX G-F01
    if has_active_symbols:
        try:
            active_df = pl.read_parquet(active_sym_path).select(
                ["symbol", "dollar_volume_20d"]
            )
        except Exception:
            active_df = _empty_active_df()
    else:
        active_df = _empty_active_df()
    con.register("active_tbl", active_df.to_arrow())

    # ADD GMI Wave 1 Cycle 4 — CrossAssetEngine sources (see module
    # docstring). Same has_X / try-except / _empty_*_df() idiom as above.

    # Global regime table (GlobalIndexRegimeModule, daily) — single row
    # for run_date, broadcast via LEFT JOIN ON TRUE like regime_tbl.
    if GLOBAL_REGIME_PATH.exists():
        try:
            global_regime_df = pl.read_parquet(GLOBAL_REGIME_PATH).filter(
                pl.col("date").cast(pl.Utf8) == str(run_date)
            ).select(
                ["global_risk_score", "global_regime_label", "dm_em_divergence"]
            ).head(1)
        except Exception:
            global_regime_df = _empty_global_regime_df()
    else:
        global_regime_df = _empty_global_regime_df()
    con.register("global_regime_tbl", global_regime_df.to_arrow())

    # Lead-lag table (LeadLagModule, weekly) — pre-aggregated per follower
    # symbol here in Python (raw store is one row per leader x follower).
    if LEAD_LAG_PATH.exists():
        try:
            lead_lag_raw = pl.read_parquet(LEAD_LAG_PATH)
            lead_lag_df = (
                lead_lag_raw
                .filter(pl.col("bh_significant"))
                .sort("p_value_adjusted")
                .group_by("follower", maintain_order=True)
                .agg([
                    pl.len().alias("lead_lag_signal_count"),
                    pl.col("leader").first().alias("lead_lag_top_leader"),
                    pl.col("optimal_lag").first().alias("lead_lag_top_lag"),
                ])
            )
        except Exception:
            lead_lag_df = _empty_lead_lag_df()
    else:
        lead_lag_df = _empty_lead_lag_df()
    con.register("lead_lag_tbl", lead_lag_df.to_arrow())

    # Forecast table (ForecastModule, weekly) — nearest-term (1-day)
    # horizon only, one row per symbol.
    if CROSS_ASSET_FORECAST_PATH.exists():
        try:
            forecast_df = (
                pl.read_parquet(CROSS_ASSET_FORECAST_PATH)
                .filter(pl.col("horizon_days") == 1)
                .select(["symbol", "forecast_return", "stable"])
                .rename({"forecast_return": "forecast_return_1d", "stable": "forecast_stable"})
            )
        except Exception:
            forecast_df = _empty_forecast_df()
    else:
        forecast_df = _empty_forecast_df()
    con.register("forecast_tbl", forecast_df.to_arrow())

    # Signal aggregation table (signal_aggregation job, daily) — one row
    # per active_ohlcv symbol (ADD GMI-SIGAGG-001, see module docstring).
    signal_agg_path = SIGNAL_AGG_PATH / f"signal_aggregation_{run_date.isoformat()}.parquet"
    if signal_agg_path.exists():
        try:
            signal_agg_df = pl.read_parquet(signal_agg_path).select([
                "symbol", "composite_score", "composite_grade",
                "sector_breadth_pct", "sector_momentum", "breadth_divergence",
            ])
        except Exception:
            signal_agg_df = _empty_signal_agg_df()
    else:
        signal_agg_df = _empty_signal_agg_df()
    con.register("signal_agg_tbl", signal_agg_df.to_arrow())

    # ── Parameterized query — no f-string SQL ─────────────────────────────────
    # FIX GLD-003: read_parquet($mtf_path) + $min_mtf_score,
    # $min_dollar_volume, $run_date — semua via $name binding.
    QUERY = """
    WITH mtf AS (
        SELECT
            symbol,
            mtf_score,
            signal_quality,
            entry_zone_low,
            entry_zone_high,
            stop_zone_1H,
            reward_risk_ratio,
            last_close
        FROM read_parquet($mtf_path)
        WHERE ABS(mtf_score) >= $min_mtf_score
          AND signal_quality IN ('A', 'B')
    )
    SELECT
        m.symbol,
        m.mtf_score,
        m.signal_quality,
        m.entry_zone_low,
        m.entry_zone_high,
        m.stop_zone_1H,
        ROUND(m.reward_risk_ratio, 2)              AS reward_risk_ratio,
        m.last_close,
        COALESCE(a.dollar_volume_20d, 0)           AS dollar_volume_20d,
        r.context_date,
        r.context_age_days,
        r.macro_label,
        r.macro_activity,
        r.macro_momentum,
        r.market_label,
        r.market_probability,
        r.market_score,
        r.market_age_days,
        r.context_label,
        r.context_transition,
        r.context_transition_alert,
        r.regime_in_sample,
        NULL::INTEGER                               AS days_to_earnings,
        NULL::DATE                                  AS next_earnings_date,
        (NULL::INTEGER) <= 3                        AS near_earnings_flag,
        NULL::DOUBLE                                AS sentiment_score,
        NULL::DOUBLE                                AS buzz_score,
        gr.global_risk_score,
        gr.global_regime_label,
        gr.dm_em_divergence,
        COALESCE(ll.lead_lag_signal_count, 0) > 0   AS lead_lag_signal,
        ll.lead_lag_top_leader,
        ll.lead_lag_top_lag,
        f.forecast_return_1d,
        f.forecast_stable,
        sa.composite_score,
        sa.composite_grade,
        sa.sector_breadth_pct,
        sa.sector_momentum,
        sa.breadth_divergence,
        $run_date                                  AS watchlist_date
    FROM mtf m
    LEFT JOIN active_tbl  a ON m.symbol = a.symbol
    -- FIX GLD-SCR-001: was CROSS JOIN (SELECT * FROM regime_tbl LIMIT 1) r.
    -- CROSS JOIN against a subquery that legitimately produces ZERO rows
    -- (context_regime.parquet missing, or present but with no row within
    -- tolerance of this run_date — e.g. a first run before the HMM fit
    -- jobs, or a backfill date the regime engine never covered) is a Cartesian
    -- product with an empty relation, which is empty by definition — it
    -- silently discarded the ENTIRE watchlist regardless of how many
    -- valid MTF/active candidates existed. Empirically reproduced
    -- with a standalone DuckDB query (0 rows out with CROSS JOIN, 2/2
    -- preserved with LEFT JOIN ... ON TRUE, r.* correctly NULL rather
    -- than dropping rows) before this fix was written. LEFT JOIN ON TRUE
    -- is the correct "broadcast at most one row, degrade to NULL, never
    -- drop the left side" join — the same graceful-degrade contract
    -- active_tbl already gets via LEFT JOIN + COALESCE above.
    LEFT JOIN (SELECT * FROM context_regime_tbl LIMIT 1) r ON TRUE
    -- ADD GMI Wave 1 Cycle 4: global_regime_tbl is the same "single row
    -- for this run_date, or legitimately zero rows" shape as regime_tbl
    -- above — same LEFT JOIN ... ON TRUE broadcast, same reasoning.
    LEFT JOIN (SELECT * FROM global_regime_tbl LIMIT 1) gr ON TRUE
    LEFT JOIN lead_lag_tbl ll ON m.symbol = ll.follower
    LEFT JOIN forecast_tbl  f ON m.symbol = f.symbol
    -- ADD GMI-SIGAGG-001: same per-symbol LEFT JOIN shape as forecast_tbl.
    LEFT JOIN signal_agg_tbl sa ON m.symbol = sa.symbol
    WHERE COALESCE(a.dollar_volume_20d, 1e9) > $min_dollar_volume
    ORDER BY ABS(m.mtf_score) DESC,
             COALESCE(m.reward_risk_ratio, 0) DESC
    """

    try:
        df = con.execute(
            QUERY,
            {
                "mtf_path":          str(mtf_path),
                "min_mtf_score":     MIN_MTF_SCORE,
                "min_dollar_volume": MIN_DOLLAR_VOLUME,
                "run_date":          str(run_date),
            },
        ).pl()
    except Exception as e:
        logger.warning(f"[gold_screener] Query failed: {e} — using simplified query")
        df = _simplified_watchlist(mtf_path, run_date)

    if df.is_empty():
        return df

    # FIX ADR-044 (GMI_Decision_Document_v10.docx): _enrich_earnings() and
    # _enrich_sentiment() call sites removed — Finnhub retired in full
    # (ADR-043). days_to_earnings, next_earnings_date, near_earnings_flag,
    # sentiment_score, buzz_score remain in the watchlist output schema
    # exactly as before, sourced entirely from the main query's own typed
    # NULL placeholders above — no Interface Contract change (GD §0.4,
    # §17.6): the promised columns still exist, still correctly typed,
    # simply permanently null instead of sometimes-populated.

    # Correlation cluster deduplication (GD §15.1: max 2 per cluster)
    if has_corr:
        df = _deduplicate_by_cluster(df, run_date, con)

    # Final top-N
    df = df.head(TOP_N_WATCHLIST)
    return df


def _simplified_watchlist(mtf_path: Path, run_date: date) -> pl.DataFrame:
    """Simplified watchlist without JOIN when inputs not available."""
    try:
        df = pl.read_parquet(mtf_path)
        return (
            df.filter(pl.col("signal_quality").is_in(["A", "B"]))
              .sort("mtf_score", descending=True)
              .with_columns(pl.lit(str(run_date)).alias("watchlist_date"))
              .head(TOP_N_WATCHLIST)
        )
    except Exception as e:
        logger.error(f"[gold_screener] Simplified query also failed: {e}")
        return pl.DataFrame()


def _deduplicate_by_cluster(
    df: pl.DataFrame,
    run_date: date,
    con: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    """Keep max MAX_PER_CLUSTER symbols per correlation cluster.

    Cluster source is cross_asset_corr.parquet (pairwise, upper triangle).
    CorrelationModule assigns ONE cluster_id per symbol per run, and a symbol
    is symbol_a in some pairs and symbol_b in others (the last symbol only
    ever as symbol_b) — so both sides are unioned. This is the same
    projection legacy_correlation_bridge.py used to materialise.
    """
    try:
        # FIX GLD-003: $name parameterized query — f-string SQL dilarang GD §17.7
        clusters = con.execute(
            """
            SELECT symbol_a AS symbol, cluster_id_a AS cluster_id
            FROM read_parquet($path)
            UNION
            SELECT symbol_b AS symbol, cluster_id_b AS cluster_id
            FROM read_parquet($path)
            ORDER BY symbol, cluster_id
            """,
            {"path": str(CROSS_ASSET_CORR_PATH)},
        ).pl()
        # Cardinality guard: the left join below must never fan out and
        # duplicate watchlist rows, even if the pairwise file were ever to
        # carry two cluster ids for one symbol (it cannot within one run).
        clusters = clusters.unique(subset=["symbol"], keep="first", maintain_order=True)

        df = df.join(clusters, on="symbol", how="left")
        df = df.with_columns(
            pl.col("cluster_id").fill_null(-1)
        )
        # Rank within cluster — preserves the DataFrame's existing row
        # order (i.e. the caller's ORDER BY ABS(mtf_score) DESC, ... from
        # build_watchlist), so rank 0 is always the highest-priority
        # candidate already in the cluster.
        # FIX GLD-SCR-003: pl.int_ranges() (plural) broadcasts a single
        # List[Int64] value (e.g. [0,1,2]) to every row in a group — it
        # does NOT number rows individually. The subsequent
        # `cluster_rank < MAX_PER_CLUSTER` comparison then raised
        # `SchemaError: could not evaluate '<' comparison ... List(Int64)`
        # on every call where correlation data was actually present,
        # caught by the except below and silently ignored — verified
        # empirically with a standalone repro before this fix (see
        # thread report). pl.int_range() (singular) is the correct
        # per-row "position within group" primitive.
        df = df.with_columns(
            pl.int_range(pl.len()).over("cluster_id").alias("cluster_rank")
        )
        df = df.filter(pl.col("cluster_rank") < MAX_PER_CLUSTER)
        df = df.drop(["cluster_id", "cluster_rank"])
    except Exception as e:
        logger.warning(f"[gold_screener] Cluster dedup skipped: {e}")

    return df


def load_watchlist(run_date: date) -> pl.DataFrame:
    """Load saved watchlist for run_date."""
    path = GOLD_SCREENER_PATH / f"watchlist_{run_date.isoformat()}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Watchlist not found for {run_date}")
    return pl.read_parquet(path)
