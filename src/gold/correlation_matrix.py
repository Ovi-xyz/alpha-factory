"""
correlation_matrix.py — GD §5.2.5 (Gold Correlation Matrix Store)
Rolling 60D correlation matrix — active symbols only (~200).

v1.2 optimization: filter ke active_symbols (~200) bukan semua 643.
Full 643×643 matrix tidak realistis (RAM 8GB M1) tanpa chunking.
~200×200 matrix (~20K pairs) fits comfortably.

Refresh: Weekly Sunday (gold_correlation job).
Output: data/gold/correlation/correlation_clusters.parquet

Schema: symbol, cluster_id, correlation_avg, n_cluster_members, computed_date
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from loguru import logger

from src.utils.atomic_io import atomic_write_parquet
# FIX ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8, 2026-07-11):
# SILVER_1D_PATH was an unfiltered "market_ohlcv/**/*_1D_silver.parquet"
# glob. active_symbols is already Layer 1-only (never contains a Layer 2
# ticker), so the downstream `symbol IN active_symbols_tbl` filter meant
# this was not silently corrupting results the way quality_validator.py's
# aggregate COUNT/MAX queries were — but it still unnecessarily scanned
# Layer 2 Silver OHLCV files (added in GMI Cycle 3, same market_ohlcv/
# root) on every weekly run. Fixed for consistency with the rest of the
# Bronze/Silver Solidification work, ahead of GMI Wave 1 Cycle 4 (whose
# own CorrelationModule will deliberately want BOTH layers merged — see
# silver_scope.py's module docstring — and should build that explicitly
# via layer1_globs() + [context_glob()], not by inheriting this kind of
# unfiltered glob).
from src.utils.silver_scope import layer1_globs

SILVER_OHLCV_ROOT = Path("data/silver/market_ohlcv")
GOLD_CORR_PATH  = Path("data/gold/correlation")
ACTIVE_SYM_PATH = Path("data/silver/active_symbols")

MIN_HISTORY_DAYS = 65   # Min bars needed for 60D rolling (with buffer)
LOOKBACK_DAYS    = 65   # Rolling window
N_CLUSTERS       = 10   # Hierarchical clustering target


def run(run_date: date) -> None:
    """Job entry point for gold_correlation (weekly Sunday)."""
    logger.info(f"[gold_correlation] Starting | run_date={run_date}")

    active_symbols = _load_active_symbols(run_date)
    if not active_symbols:
        logger.warning(
            "[gold_correlation] No active symbols found — skipping"
        )
        return

    logger.info(
        f"[gold_correlation] Computing 60D correlation"
        f" for {len(active_symbols)} active symbols"
    )

    # FIX ADR-022/RISK-6: Layer 1-scoped glob list — see module header.
    silver_1d_paths = layer1_globs(SILVER_OHLCV_ROOT, "*_1D_silver.parquet")
    if not silver_1d_paths:
        logger.warning("[gold_correlation] No Layer 1 Silver 1D data found — skipping")
        return

    corr_df = compute_correlation_matrix(
        silver_1d_path=silver_1d_paths,
        active_symbols=active_symbols,
        min_history_days=MIN_HISTORY_DAYS,
        run_date=run_date,
    )

    if corr_df is None or corr_df.is_empty():
        logger.warning("[gold_correlation] Empty correlation matrix")
        return

    cluster_df = assign_clusters(corr_df, active_symbols, run_date)

    GOLD_CORR_PATH.mkdir(parents=True, exist_ok=True)
    out_path = GOLD_CORR_PATH / "correlation_clusters.parquet"
    # FIX GLD-004: atomic_write_parquet via tempfile + os.replace
    atomic_write_parquet(
        cluster_df,
        out_path,
        compression="zstd",
        compression_level=3,
    )
    logger.info(
        f"[gold_correlation] {len(cluster_df)} symbols clustered"
        f" into {cluster_df['cluster_id'].n_unique()} clusters → {out_path.name}"
    )


def compute_correlation_matrix(
    silver_1d_path,  # str | list[str] — Layer1-scoped list from layer1_globs() in run()
    active_symbols: list[str],
    min_history_days: int = 65,
    run_date: Optional[date] = None,
) -> Optional[pl.DataFrame]:
    """
    Compute rolling 60D correlation matrix for active_symbols.
    Returns wide correlation matrix (symbols as columns).

    GD §5.2.5: active_symbols filter reduces matrix from 643×643
    to ~200×200 — 25× more RAM-efficient.

    FIX GD-F03: Warn if active_symbols > 250 — M1 8GB RAM constraint.
    Correlation matrix memory usage scales as O(n²): at 200 symbols ~20K pairs,
    at 300 symbols ~45K pairs. Above 250 symbols risk OOM on M1.
    """
    if not active_symbols:
        return None

    # FIX GD-F03: RAM guard for M1 8GB
    MAX_SYMBOLS_RAM_SAFE = 250
    if len(active_symbols) > MAX_SYMBOLS_RAM_SAFE:
        logger.warning(
            f"[gold_correlation] active_symbols count {len(active_symbols)} "
            f"exceeds RAM-safe threshold of {MAX_SYMBOLS_RAM_SAFE} for M1 8GB. "
            f"Matrix will be {len(active_symbols)}×{len(active_symbols)} "
            f"(~{len(active_symbols)**2:,} pairs). Consider reducing "
            f"THRESHOLDS['us_stocks']['dollar_volume_20d'] to tighten universe. "
            f"GD §5.2.5 target: ~200 symbols."
        )

    run_date = run_date or date.today()
    start    = run_date - timedelta(days=LOOKBACK_DAYS)

    symbols_list = list(active_symbols)

    try:
        con = duckdb.connect()
        con.execute("SET memory_limit='3GB'; SET threads=4;")

        # FIX GLD-003: parameterize query — f-string SQL dilarang GD §17.7.
        # active_symbols diregister sebagai tabel DuckDB (Arrow registration)
        # sebagai pengganti pola lama yang melakukan string injection via
        # join list symbols ke dalam SQL literal — SQL injection risk.
        active_df = pl.DataFrame({"symbol": symbols_list})
        con.register("active_symbols_tbl", active_df.to_arrow())

        # Fetch log_returns for active symbols in window
        returns_df = con.execute(
            """
            SELECT
                symbol,
                CAST(timestamp AS DATE) AS date,
                log_return
            FROM read_parquet($path, hive_partitioning=true)
            WHERE CAST(timestamp AS DATE) >= $start
              AND CAST(timestamp AS DATE) <= $run_date
              AND symbol IN (SELECT symbol FROM active_symbols_tbl)
              AND log_return IS NOT NULL
              AND is_clean = TRUE
            ORDER BY date, symbol
            """,
            {
                "path":     silver_1d_path,
                "start":    start,
                "run_date": run_date,
            },
        ).pl()

        if returns_df.is_empty():
            logger.warning("[Correlation] No return data in window")
            return None

        # Filter symbols with enough history
        sym_counts = (
            returns_df
            .group_by("symbol")
            .agg(pl.len().alias("n_days"))
            .filter(pl.col("n_days") >= min_history_days * 0.8)   # 80% min
        )
        valid_symbols = sym_counts["symbol"].to_list()

        if len(valid_symbols) < 2:
            logger.warning(
                f"[Correlation] Only {len(valid_symbols)} symbols have"
                " enough history — need at least 2"
            )
            return None

        # Pivot: date × symbol returns matrix
        pivot = (
            returns_df
            .filter(pl.col("symbol").is_in(valid_symbols))
            .pivot(
                values="log_return",
                index="date",
                on="symbol",
                aggregate_function="first",
            )
            .sort("date")
        )

        # Polars correlation matrix
        sym_cols  = [c for c in pivot.columns if c != "date"]
        corr_data = pivot.select(sym_cols)
        corr_mat  = corr_data.corr()   # n_symbols × n_symbols correlation matrix

        return corr_mat

    except Exception as e:
        logger.error(f"[Correlation] Matrix computation failed: {e}")
        return None


def assign_clusters(
    corr_matrix: pl.DataFrame,
    symbols: list[str],
    run_date: date,
) -> pl.DataFrame:
    """
    Assign cluster IDs via hierarchical clustering on correlation distance.
    Distance = 1 - abs(correlation).

    Returns: DataFrame with columns: symbol, cluster_id, correlation_avg, computed_date
    """
    try:
        from sklearn.cluster import AgglomerativeClustering  # type: ignore
        import numpy as np

        # Build symmetric distance matrix
        n = len(corr_matrix)
        sym_cols = corr_matrix.columns[:n]

        corr_np  = corr_matrix.select(sym_cols).to_numpy()
        # Clip to [-1, 1] and compute distance
        corr_np  = np.clip(corr_np, -1, 1)
        dist_mat = 1 - np.abs(corr_np)
        np.fill_diagonal(dist_mat, 0)

        # Determine n_clusters — not more than n_symbols / 2
        n_clusters = min(N_CLUSTERS, max(2, n // 2))

        clustering = AgglomerativeClustering(
            n_clusters=n_clusters,
            metric="precomputed",
            linkage="average",
        )
        labels = clustering.fit_predict(dist_mat)

        # Compute avg abs correlation per symbol (connectivity score)
        corr_avg = np.abs(corr_np).mean(axis=1)

        # Build result DataFrame
        # Map symbols to cluster — corr_matrix columns may be subset of active_symbols
        available_symbols = list(corr_matrix.columns[:n])

        result = pl.DataFrame({
            "symbol":         available_symbols,
            "cluster_id":     labels.tolist(),
            "correlation_avg": corr_avg.tolist(),
            "computed_date":  [str(run_date)] * len(available_symbols),
        })

        # Add cluster size for context
        cluster_sizes = (
            result.group_by("cluster_id")
            .agg(pl.len().alias("n_cluster_members"))
        )
        result = result.join(cluster_sizes, on="cluster_id", how="left")

        return result

    except ImportError:
        logger.warning(
            "[Correlation] scikit-learn not installed — assigning all to cluster 0"
        )
        return pl.DataFrame({
            "symbol":           symbols,
            "cluster_id":       [0] * len(symbols),
            "correlation_avg":  [0.5] * len(symbols),
            "n_cluster_members": [len(symbols)] * len(symbols),
            "computed_date":    [str(run_date)] * len(symbols),
        })

    except Exception as e:
        logger.error(f"[Correlation] Clustering failed: {e}")
        return pl.DataFrame({
            "symbol":           symbols,
            "cluster_id":       list(range(len(symbols))),
            "correlation_avg":  [0.5] * len(symbols),
            "n_cluster_members": [1] * len(symbols),
            "computed_date":    [str(run_date)] * len(symbols),
        })


def _load_active_symbols(run_date: date) -> list[str]:
    """Load active symbols from silver layer."""
    # Try most recent active symbols file
    files = sorted(ACTIVE_SYM_PATH.glob("active_*.parquet"), reverse=True)
    if files:
        try:
            return pl.read_parquet(files[0])["symbol"].to_list()
        except Exception:
            pass

    # Fallback: all instruments
    from src.config.instrument_loader import get_loader
    return get_loader().symbol_list()
