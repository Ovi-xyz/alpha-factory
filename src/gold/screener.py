"""
screener.py — GD §5.2.4 (Gold Screener & Watchlist)
DuckDB query join: MTF + Regime + Sector + Correlation + Earnings + Sentiment.

v1.2 additions:
    - days_to_earnings: DATA field (tidak memfilter — Trading Engine yang decide)
    - near_earnings_flag: Boolean field (days_to_earnings <= 3)
    - sentiment_score: dari Silver sentiment layer
    - Correlation cluster deduplication: max 2 per cluster

Output: data/gold/screener/watchlist_{date}.parquet

Separation of Concerns (GD §0.3):
    - near_earnings_flag: DATA, bukan filter. Trading Engine yang decide.
    - sentiment_score: DATA informational. Bukan keputusan.
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

SILVER_OHLCV_ROOT  = Path("data/silver/market_ohlcv")
GOLD_MTF_PATH       = Path("data/gold/mtf")
GOLD_REGIME_PATH    = Path("data/gold/macro/regime_store.parquet")
GOLD_SECTOR_PATH    = Path("data/gold/sector/sector_regime_weights.parquet")
GOLD_CORR_PATH      = Path("data/gold/correlation/correlation_clusters.parquet")
SILVER_SENTIMENT    = "data/silver/sentiment/date=*/*.parquet"
GOLD_SCREENER_PATH  = Path("data/gold/screener")
# FIX GLD-SCR-002: these two were previously built inline as ad hoc string/
# f-string Path constructions inside build_watchlist()/_enrich_sentiment()
# (not the SQL-injection kind — plain filesystem paths — but un-patchable
# hardcodes all the same, matching the same class of issue fixed via
# REGIME_STORE_PATH in mtf_alignment.py this same thread). Promoted to
# module-level roots alongside the constants above; the per-run_date
# filename is still built dynamically where it's used.
SILVER_ACTIVE_SYMBOLS_ROOT = Path("data/silver/active_symbols")
SILVER_SENTIMENT_ROOT      = Path("data/silver/sentiment")

# Screening filters (hard thresholds for watchlist inclusion)
MIN_MTF_SCORE      = 5       # |score| >= 5 → grade A or B only
MIN_DOLLAR_VOLUME  = 1_000_000   # USD 1M/day
MIN_SECTOR_WEIGHT  = 0.5     # Exclude sectors in RISK_OFF penalty
TOP_N_WATCHLIST    = 20
MAX_PER_CLUSTER    = 2       # Correlation concentration guard (GD §15.1)


# ── Empty placeholder DataFrame helpers ───────────────────────────────────────
# FIX GLD-003: digunakan untuk menggantikan '/dev/null' path injection
# di f-string SQL. Arrow table registration ke DuckDB memungkinkan LEFT JOIN
# terhadap tabel kosong tanpa perlu f-string conditional path logic.

def _empty_regime_df() -> pl.DataFrame:
    """Placeholder regime DataFrame dengan schema minimal."""
    return pl.DataFrame({
        "regime":            pl.Series([], dtype=pl.Utf8),
        "composite_score":   pl.Series([], dtype=pl.Float64),
        "confidence":        pl.Series([], dtype=pl.Float64),
        "regime_transition": pl.Series([], dtype=pl.Boolean),
        "transition_alert":  pl.Series([], dtype=pl.Utf8),
    })


def _empty_sector_df() -> pl.DataFrame:
    """Placeholder sector DataFrame dengan schema minimal."""
    return pl.DataFrame({
        "symbol":           pl.Series([], dtype=pl.Utf8),
        "sector":           pl.Series([], dtype=pl.Utf8),
        "sector_weight_adj": pl.Series([], dtype=pl.Float64),
    })


def _empty_active_df() -> pl.DataFrame:
    """Placeholder active symbols DataFrame dengan schema minimal."""
    return pl.DataFrame({
        "symbol":           pl.Series([], dtype=pl.Utf8),
        "dollar_volume_20d": pl.Series([], dtype=pl.Float64),
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
            """
            SELECT COUNT(DISTINCT symbol) AS fresh_count
            FROM read_parquet($glob, hive_partitioning=true)
            WHERE CAST(timestamp AS DATE) >= CAST($run_date AS DATE) - INTERVAL (CAST($freshness_days AS INTEGER)) DAY
              AND is_clean = TRUE
            """,
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

    has_regime  = GOLD_REGIME_PATH.exists()
    has_sector  = GOLD_SECTOR_PATH.exists()
    has_corr    = GOLD_CORR_PATH.exists()
    active_sym_path = (
        SILVER_ACTIVE_SYMBOLS_ROOT / f"active_{run_date.isoformat()}.parquet"
    )
    has_active_symbols = active_sym_path.exists()

    # Regime table
    if has_regime:
        try:
            regime_df = pl.read_parquet(GOLD_REGIME_PATH).filter(
                pl.col("date").cast(pl.Utf8) == str(run_date)
            ).head(1)
        except Exception:
            regime_df = _empty_regime_df()
    else:
        regime_df = _empty_regime_df()
    con.register("regime_tbl", regime_df.to_arrow())

    # Sector table
    if has_sector:
        try:
            sector_df = pl.read_parquet(GOLD_SECTOR_PATH).select(
                ["symbol", "sector", "sector_weight_adj"]
            )
        except Exception:
            sector_df = _empty_sector_df()
    else:
        sector_df = _empty_sector_df()
    con.register("sector_tbl", sector_df.to_arrow())

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

    # ── Parameterized query — no f-string SQL ─────────────────────────────────
    # FIX GLD-003: read_parquet($mtf_path) + $min_mtf_score, $min_sector_weight,
    # $min_dollar_volume, $run_date — semua via $name binding.
    QUERY = """
    WITH mtf AS (
        SELECT
            symbol,
            mtf_score,
            signal_quality,
            regime_compatible,
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
        COALESCE(s.sector,            'Unknown')   AS sector,
        COALESCE(s.sector_weight_adj, 1.0)         AS sector_weight_adj,
        COALESCE(a.dollar_volume_20d, 0)           AS dollar_volume_20d,
        r.regime,
        r.composite_score                          AS regime_composite,
        r.confidence                               AS regime_confidence,
        r.regime_transition,
        r.transition_alert,
        NULL::INTEGER                               AS days_to_earnings,
        NULL::DATE                                  AS next_earnings_date,
        (NULL::INTEGER) <= 3                        AS near_earnings_flag,
        NULL::DOUBLE                                AS sentiment_score,
        NULL::DOUBLE                                AS buzz_score,
        $run_date                                  AS watchlist_date
    FROM mtf m
    LEFT JOIN sector_tbl s ON m.symbol = s.symbol
    LEFT JOIN active_tbl  a ON m.symbol = a.symbol
    -- FIX GLD-SCR-001: was CROSS JOIN (SELECT * FROM regime_tbl LIMIT 1) r.
    -- CROSS JOIN against a subquery that legitimately produces ZERO rows
    -- (regime_store.parquet missing, or present but with no row for this
    -- exact run_date — e.g. --force run ahead of gold_regime, or a
    -- backfill date regime detection never covered) is a Cartesian
    -- product with an empty relation, which is empty by definition — it
    -- silently discarded the ENTIRE watchlist regardless of how many
    -- valid MTF/sector/active candidates existed. Empirically reproduced
    -- with a standalone DuckDB query (0 rows out with CROSS JOIN, 2/2
    -- preserved with LEFT JOIN ... ON TRUE, r.* correctly NULL rather
    -- than dropping rows) before this fix was written. LEFT JOIN ON TRUE
    -- is the correct "broadcast at most one row, degrade to NULL, never
    -- drop the left side" join — the same graceful-degrade contract
    -- sector_tbl/active_tbl already get via LEFT JOIN + COALESCE above.
    LEFT JOIN (SELECT * FROM regime_tbl LIMIT 1) r ON TRUE
    WHERE COALESCE(s.sector_weight_adj, 1.0) > $min_sector_weight
      AND COALESCE(a.dollar_volume_20d, 1e9) > $min_dollar_volume
    ORDER BY ABS(m.mtf_score) DESC,
             COALESCE(m.reward_risk_ratio, 0) DESC
    """

    try:
        df = con.execute(
            QUERY,
            {
                "mtf_path":          str(mtf_path),
                "min_mtf_score":     MIN_MTF_SCORE,
                "min_sector_weight": MIN_SECTOR_WEIGHT,
                "min_dollar_volume": MIN_DOLLAR_VOLUME,
                "run_date":          str(run_date),
            },
        ).pl()
    except Exception as e:
        logger.warning(f"[gold_screener] Query failed: {e} — using simplified query")
        df = _simplified_watchlist(mtf_path, run_date)

    if df.is_empty():
        return df

    # ── Enrich with earnings data (v1.2 DATA field) ──────────────────────────
    # days_to_earnings is a DATA field — Trading Engine decides what to do
    if not df.is_empty():
        df = _enrich_earnings(df, run_date)

    # ── Enrich with sentiment data (v1.2 DATA field) ──────────────────────────
    if not df.is_empty():
        df = _enrich_sentiment(df, run_date)

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
    """Keep max MAX_PER_CLUSTER symbols per correlation cluster."""
    try:
        # FIX GLD-003: $name parameterized query — f-string SQL dilarang GD §17.7
        clusters = con.execute(
            """
            SELECT symbol, cluster_id
            FROM read_parquet($path)
            """,
            {"path": str(GOLD_CORR_PATH)},
        ).pl()

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
        logger.debug(f"[gold_screener] Cluster dedup skipped: {e}")

    return df


def _enrich_earnings(df: pl.DataFrame, run_date: date) -> pl.DataFrame:
    """
    Populate days_to_earnings and near_earnings_flag from Silver fundamental.
    GD §5.2.4: DATA field — Trading Engine decides whether to trade around earnings.
    Reads from data/silver/fundamental/earnings_{date}.parquet (processed from Finnhub).
    """
    try:
        from src.silver.fundamental_processor import FundamentalProcessor
        proc           = FundamentalProcessor()
        upcoming       = proc.get_upcoming_earnings(run_date, within_days=90)
        earnings_map: dict[str, int] = {}

        if not upcoming.is_empty():
            for row in upcoming.iter_rows(named=True):
                sym = row.get("symbol", "")
                dte = row.get("days_to_earnings")
                if sym and dte is not None:
                    # Keep smallest (soonest) if multiple entries
                    if sym not in earnings_map or dte < earnings_map[sym]:
                        earnings_map[sym] = int(dte)

        symbols = df["symbol"].to_list()
        dte_vals  = [earnings_map.get(s) for s in symbols]

        df = df.with_columns([
            pl.Series("days_to_earnings", dte_vals, dtype=pl.Int32),
        ]).with_columns([
            (
                pl.col("days_to_earnings").is_not_null()
                & (pl.col("days_to_earnings") <= 3)
            ).alias("near_earnings_flag"),
        ])

    except Exception as e:
        logger.debug(f"[Screener] Earnings enrichment skipped: {e}")

    return df


def _enrich_sentiment(df: pl.DataFrame, run_date: date) -> pl.DataFrame:
    """
    Join Silver sentiment data into screener output.
    GD §5.2.4: sentiment_score is a DATA field (informational).
    """
    sentiment_path = (
        SILVER_SENTIMENT_ROOT
        / f"date={run_date.isoformat()}"
        / "sentiment_silver.parquet"
    )
    if not sentiment_path.exists():
        return df

    try:
        sentiment = pl.read_parquet(sentiment_path).select([
            "symbol", "sentiment_score", "buzz_score"
        ])
        df = df.join(sentiment, on="symbol", how="left", suffix="_sent")
        # Resolve column name conflicts if already present
        for col in ["sentiment_score", "buzz_score"]:
            if f"{col}_sent" in df.columns:
                df = df.drop(col).rename({f"{col}_sent": col})
    except Exception as e:
        logger.debug(f"[Screener] Sentiment enrichment skipped: {e}")

    return df


def load_watchlist(run_date: date) -> pl.DataFrame:
    """Load saved watchlist for run_date."""
    path = GOLD_SCREENER_PATH / f"watchlist_{run_date.isoformat()}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Watchlist not found for {run_date}")
    return pl.read_parquet(path)
