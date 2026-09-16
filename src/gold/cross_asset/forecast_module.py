"""
forecast_module.py — Architecture v2.0 §6.4 (CrossAssetEngine —
ForecastModule). GMI Wave 1 Cycle 4 — fourth and final module, and the
one Gate 1 blocked (KNOWN_RISKS.md RISK-16, instruments_taxonomy.yaml's
own ADR-049 comment) until Ovi ran the real BIS weight extraction this
session — see gold/cross_asset/broad_dollar.py for the extraction output
and the weight/sign-convention decisions made from it.

PCA input scope — DELIBERATE, DOCUMENTED LIMITATION: Architecture v2.0
§6.4.2 Step 1 says "Layer 2 returns (25 series)" without qualification.
This implementation's PCA input is loader.forecast_context() (OHLCV-based
Layer 2 instruments only — dollar, dollar_basket, equity dm/em,
volatility, commodity, ETF credit/commodity/international/thematic) PLUS
the derived broad_dollar_return column. It does NOT include the FRED
rates series (SOFR, DGS2/5/10/30, T10Y2Y, T10Y3M) or the BIS central
bank rates (silver_global_rates.parquet, Data Source & Rates Adjustment
v1.0 §9) — both live in a genuinely different Silver schema
(series_id/observation_date/value, forward-filled point-in-time series)
than the symbol/timestamp/log_return OHLCV schema every other input here
uses, and folding them in cleanly needs its own read/pivot/align path,
not a one-line addition. Out of scope for this pass; flagged here rather
than silently omitted. See silver/global_rates_processor.py and
silver/macro_processor.py for what that follow-up work would read from.

VAR variable count — DELIBERATE DEPARTURE from Architecture v2.0 §6.4.1's
own code sketch. That sketch builds ONE VAR across [PCs, ALL active
equities] simultaneously (`VAR(np.hstack([X_pca, y]))` where `y` is every
active_ohlcv symbol's return series at once). With ~190 active equities
and 3-5 PCs, that is a ~195-variable VAR fit on a ~60-observation window
— statistically unidentifiable (Architecture v2.0 §6.4.3 itself
acknowledges the degrees-of-freedom problem without resolving it with a
concrete limit). This implementation instead fits one SEPARATE VAR per
equity — variables = [PC1..PC_k, that equity's own return] (k+1 total,
typically 4-6) — looping over the active_ohlcv universe. This is the
standard, well-identified way to use a handful of common factors as
shared macro-context regressors for many individual univariate
forecasts; it is what "PCA pre-processing is a day-one design
requirement" (§6.4.1) is actually for, not a requirement that every
equity share one giant joint VAR system.

Lag selection: BIC, not AIC — resolves OD-1 (Architecture v2.0 §12,
listed "OPEN" but with an explicit textual preference: "AIC tends to
overfit — BIC preferred"). statsmodels VAR.fit(maxlags=5, ic='bic').

k_ar == 0 edge case (real, not hypothetical — confirmed empirically):
when BIC finds no lag structure at all (equity return shows no
detectable dependence on the PC factors at lags 1-5), statsmodels'
VARResults.forecast() and .is_stable() both raise on an empty
coefficient array. Handled explicitly: k_ar=0 forecasts the equity's own
training-window MEAN return at every horizon (what a VAR(0) intercept-
only model conceptually represents) and is reported stable=True
(trivially — a constant has no dynamics to be unstable).

Stability check: VARResults.is_stable() (companion-matrix eigenvalues
inside the unit circle, §8.3) for k_ar > 0. A per-equity VAR that fails
this check is written with stable=False and its forecast is NOT
suppressed — a hard drop was considered and rejected (silently losing
coverage for a symbol is worse than an honestly-flagged low-confidence
row, same reasoning as reliability_flag/partial_coverage_flag elsewhere
in this codebase); consumers should treat stable=False rows with real
skepticism.

Regime-transition trigger (§6.4.3: "if GlobalIndexRegimeModule detects
regime_transition=True, ForecastModule re-runs within the daily
pipeline") is a scheduler/runner.py-level orchestration concern — reading
global_regime.parquet's regime_transition column and issuing a
`--job gold_forecast --force` outside the normal Sunday slot — not
something ForecastModule.compute() itself does. Out of scope for this
module; noted so it isn't mistaken for an oversight.

Output: data/gold/cross_asset/cross_asset_forecast.parquet
Schema (not given explicitly anywhere in Architecture v2.0 — only a code
sketch exists, unlike CorrelationModule/LeadLagModule's schema tables —
defined here): symbol, computation_date, horizon_days (1-5),
forecast_return, n_pcs, pca_variance_explained, var_lag, stable, regime.

Job registry: 'gold_forecast', depends_on=['silver_active_symbols',
'silver_context_anchors', 'gold_cross_asset_correlation', 'gold_lead_lag']
(weekly Sunday, per Architecture v2.0 §6.6's pipeline table — pointed at
the new cross_asset jobs rather than the old gold_correlation, consistent
with CorrelationModule/LeadLagModule's own job entries). NOT yet wired
into the regime-transition trigger or gold_screener — out of scope.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import polars as pl
from loguru import logger

from src.config.instrument_loader import get_loader
from src.gold.cross_asset.broad_dollar import compute_broad_dollar, load_fx_returns
from src.silver.active_symbols import ActiveSymbolsResolver
from src.utils.atomic_io import atomic_write_parquet
from src.utils.silver_scope import context_glob, layer1_globs

SILVER_OHLCV_PATH     = Path("data/silver/market_ohlcv")
REGIME_STORE_PATH     = Path("data/gold/macro/regime_store.parquet")
GOLD_CROSS_ASSET_PATH = Path("data/gold/cross_asset")
FORECAST_STORE_PATH   = GOLD_CROSS_ASSET_PATH / "cross_asset_forecast.parquet"

LOOKBACK_DAYS        = 65    # Calendar days — bounds the Silver scan window only.
                              # matches Correlation/LeadLag; §6.4.3 "60-day rolling
                              # window is the minimum"
MIN_OBSERVATIONS     = 40    # FIX XAE-CAL-01 (16 Sep 2026): observation-count native,
                              # calendar-aware — replaces MIN_HISTORY_RATIO=0.8, which
                              # was unreachable for any 5-day-week market (max 48
                              # weekdays in a 65-day window, empirically verified
                              # against live Silver data, 16 Sep 2026 session). See
                              # correlation_module.py's MIN_OBSERVATIONS comment for
                              # full rationale/calibration. This threshold gates BOTH
                              # the Layer 2 PCA input pivot and the Layer 1 equity
                              # returns pivot (both go through _load_pivot below).
PCA_VARIANCE_TARGET  = 0.95  # §6.4.2 Step 2
MAX_VAR_LAG          = 5
FORECAST_HORIZONS    = [1, 2, 3, 4, 5]
MIN_PAIR_OBS         = MAX_VAR_LAG + 15  # buffer beyond CorrelationModule/LeadLagModule's own minimums,
                                          # since VAR needs extra obs beyond max lag to be identified at all


class ForecastModule:
    """Architecture v2.0 §6.4 — PCA(Layer 2 + Broad Dollar) -> per-equity
    VAR(PCs, equity_return), BIC lag selection, 1-5 day forecast."""

    def compute(self, run_date: date) -> Optional[pl.DataFrame]:
        followers = self._resolve_followers(run_date)
        if followers is None:
            return None

        pca_scores_df, n_pcs, variance_explained = self._build_pca_scores(run_date)
        if pca_scores_df is None:
            return None

        equity_returns = self._load_equity_returns(followers, run_date)
        if equity_returns is None or equity_returns.is_empty():
            logger.warning("[gold_forecast] No Layer 1 equity return data — skipping")
            return None

        regime = self._get_current_regime(run_date)
        rows: list[dict] = []
        pc_cols = [c for c in pca_scores_df.columns if c != "date"]

        for symbol in followers:
            if symbol not in equity_returns.columns:
                continue
            forecast_rows = self._fit_and_forecast_one_equity(
                pca_scores_df, pc_cols, equity_returns, symbol
            )
            if forecast_rows is None:
                continue
            for horizon, value, var_lag, stable in forecast_rows:
                rows.append({
                    "symbol":                  symbol,
                    "computation_date":        str(run_date),
                    "horizon_days":            horizon,
                    "forecast_return":         round(float(value), 8),
                    "n_pcs":                   n_pcs,
                    "pca_variance_explained":  round(variance_explained, 6),
                    "var_lag":                 var_lag,
                    "stable":                  stable,
                    "regime":                  regime,
                })

        if not rows:
            logger.warning("[gold_forecast] No equity produced a fittable VAR — nothing to write")
            return None
        return pl.DataFrame(rows)

    # ── Layer 1 followers ─────────────────────────────────────────────────

    @staticmethod
    def _resolve_followers(run_date: date) -> Optional[list[str]]:
        try:
            return ActiveSymbolsResolver().load_ohlcv(run_date)
        except FileNotFoundError as e:
            logger.warning(f"[gold_forecast] {e} — skipping")
            return None

    # ── PCA input assembly (Layer 2 forecast_context + Broad Dollar) ───────

    def _build_pca_scores(
        self, run_date: date
    ) -> tuple[Optional[pl.DataFrame], int, float]:
        layer2_symbols = [inst.symbol for inst in get_loader().forecast_context()]
        layer2_pivot: Optional[pl.DataFrame] = None
        if layer2_symbols:
            layer2_pivot = self._load_pivot(
                layer2_symbols, run_date, include_layer2=True, include_layer1=False
            )
        else:
            logger.info(
                "[gold_forecast] forecast_context() returned no eligible symbols — "
                "continuing with Broad Dollar alone, if available"
            )

        fx_returns = load_fx_returns(run_date, LOOKBACK_DAYS)
        broad_dollar = compute_broad_dollar(fx_returns) if fx_returns is not None else None

        if layer2_pivot is None and broad_dollar is None:
            logger.warning("[gold_forecast] Neither Layer 2 context data nor FX data available — skipping")
            return None, 0, 0.0

        if layer2_pivot is not None and broad_dollar is not None:
            combined = layer2_pivot.join(broad_dollar, on="date", how="full", coalesce=True)
        elif layer2_pivot is not None:
            combined = layer2_pivot
        else:
            combined = broad_dollar

        combined = combined.sort("date")
        feature_cols = [c for c in combined.columns if c != "date"]
        if not feature_cols:
            logger.warning("[gold_forecast] No PCA input feature columns available at all")
            return None, 0, 0.0

        pcs, n_pcs, variance_explained = self._fit_pca(combined, feature_cols)
        if pcs is None:
            return None, 0, 0.0

        pc_col_names = [f"pc_{i+1}" for i in range(n_pcs)]
        scores_df = pl.DataFrame(pcs, schema=pc_col_names).with_columns(
            combined["date"]
        )
        return scores_df, n_pcs, variance_explained

    @staticmethod
    def _fit_pca(
        combined: pl.DataFrame, feature_cols: list[str]
    ) -> tuple[Optional[np.ndarray], int, float]:
        from sklearn.decomposition import PCA
        from sklearn.preprocessing import StandardScaler

        data = combined.select(feature_cols).to_numpy().copy()
        # Column-mean imputation for sparse coverage — same convention as
        # CorrelationModule's Ledoit-Wolf input prep.
        col_means = np.nanmean(data, axis=0)
        col_means = np.nan_to_num(col_means, nan=0.0)
        inds = np.where(np.isnan(data))
        data[inds] = np.take(col_means, inds[1])

        if data.shape[0] < 10:
            logger.warning(
                f"[gold_forecast] Only {data.shape[0]} rows available for PCA — too few"
            )
            return None, 0, 0.0

        scaler = StandardScaler()
        scaled = scaler.fit_transform(data)

        n_components = min(PCA_VARIANCE_TARGET, data.shape[0] - 1, data.shape[1])
        pca = PCA(n_components=n_components if isinstance(n_components, float) else int(n_components))
        try:
            scores = pca.fit_transform(scaled)
        except Exception as e:
            logger.error(f"[gold_forecast] PCA fit failed: {e}")
            return None, 0, 0.0

        n_pcs = scores.shape[1]
        variance_explained = float(np.sum(pca.explained_variance_ratio_))
        return scores, n_pcs, variance_explained

    # ── Layer 1 equity returns ──────────────────────────────────────────────

    def _load_equity_returns(self, symbols: list[str], run_date: date) -> Optional[pl.DataFrame]:
        return self._load_pivot(symbols, run_date, include_layer2=False, include_layer1=True)

    def _load_pivot(
        self, symbols: list[str], run_date: date, include_layer2: bool, include_layer1: bool
    ) -> Optional[pl.DataFrame]:
        globs: list[str] = []
        if include_layer1:
            globs += layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        if include_layer2:
            ctx_glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if ctx_glob is not None:
                globs += [ctx_glob]
        if not globs:
            return None

        start = run_date - timedelta(days=LOOKBACK_DAYS)
        con = duckdb.connect()
        con.execute("SET memory_limit='3GB'; SET threads=4;")
        sym_df = pl.DataFrame({"symbol": symbols})
        con.register("forecast_symbols_tbl", sym_df.to_arrow())
        try:
            returns = con.execute(
                """
                SELECT symbol, CAST(timestamp AS DATE) AS date, log_return
                FROM read_parquet($globs, hive_partitioning=true)
                WHERE CAST(timestamp AS DATE) >= $start
                  AND CAST(timestamp AS DATE) <= $run_date
                  AND symbol IN (SELECT symbol FROM forecast_symbols_tbl)
                  AND log_return IS NOT NULL
                  AND is_clean = TRUE
                ORDER BY date, symbol
                """,
                {"globs": globs, "start": start, "run_date": run_date},
            ).pl()
        except Exception as e:
            logger.error(f"[gold_forecast] Silver read failed: {e}")
            return None
        if returns.is_empty():
            return None

        # FIX XAE-CAL-01: observation-count native — see MIN_OBSERVATIONS above.
        sym_counts = (
            returns.group_by("symbol").agg(pl.len().alias("n_obs"))
            .filter(pl.col("n_obs") >= MIN_OBSERVATIONS)
        )
        valid = sym_counts["symbol"].to_list()
        if not valid:
            return None
        return (
            returns.filter(pl.col("symbol").is_in(valid))
            .pivot(values="log_return", index="date", on="symbol", aggregate_function="first")
            .sort("date")
        )

    # ── Per-equity VAR fit + forecast ────────────────────────────────────

    def _fit_and_forecast_one_equity(
        self,
        pca_scores_df: pl.DataFrame,
        pc_cols: list[str],
        equity_returns: pl.DataFrame,
        symbol: str,
    ) -> Optional[list[tuple[int, float, int, bool]]]:
        merged = pca_scores_df.join(
            equity_returns.select(["date", symbol]), on="date", how="inner"
        ).drop_nulls()
        if merged.height < MIN_PAIR_OBS:
            return None

        data = merged.select(pc_cols + [symbol]).to_numpy()
        try:
            from statsmodels.tsa.vector_ar.var_model import VAR

            model = VAR(data)
            results = model.fit(maxlags=MAX_VAR_LAG, ic="bic")
        except Exception as e:
            logger.debug(f"[gold_forecast] VAR fit failed for {symbol}: {e}")
            return None

        k_ar = results.k_ar
        if k_ar == 0:
            # No lag structure found — VAR(0) is conceptually a
            # constant/mean forecast at every horizon. See module
            # docstring for why this is handled explicitly rather than
            # calling .forecast()/.is_stable() (both raise on k_ar=0).
            mean_return = float(np.mean(data[:, -1]))
            return [(h, mean_return, 0, True) for h in FORECAST_HORIZONS]

        try:
            stable = bool(results.is_stable())
        except Exception as e:
            logger.debug(f"[gold_forecast] is_stable() failed for {symbol}: {e}")
            stable = False

        try:
            forecast = results.forecast(y=data[-k_ar:], steps=max(FORECAST_HORIZONS))
        except Exception as e:
            logger.debug(f"[gold_forecast] forecast() failed for {symbol}: {e}")
            return None

        equity_col_idx = len(pc_cols)  # equity is always the last column
        return [
            (h, float(forecast[h - 1, equity_col_idx]), k_ar, stable)
            for h in FORECAST_HORIZONS
        ]

    # ── Regime tagging (same convention as correlation_module.py) ─────────

    @staticmethod
    def _get_current_regime(run_date: date) -> str:
        if not REGIME_STORE_PATH.exists():
            return "UNKNOWN"
        try:
            df = pl.read_parquet(REGIME_STORE_PATH)
            prior = df.filter(pl.col("date") <= str(run_date)).sort("date", descending=True)
            if prior.is_empty():
                return "UNKNOWN"
            return prior.row(0, named=True)["regime"]
        except Exception as e:
            logger.debug(f"[gold_forecast] Could not read regime: {e}")
            return "UNKNOWN"


def run(run_date: date) -> None:
    """Job entry point for gold_forecast (weekly Sunday + regime-transition
    trigger — the latter is a scheduler-level concern, see module docstring)."""
    logger.info(f"[gold_forecast] Starting | run_date={run_date}")

    module = ForecastModule()
    result = module.compute(run_date)
    if result is None or result.is_empty():
        logger.warning("[gold_forecast] Nothing written (no data)")
        return

    GOLD_CROSS_ASSET_PATH.mkdir(parents=True, exist_ok=True)
    atomic_write_parquet(
        result,
        FORECAST_STORE_PATH,
        compression="zstd",
        compression_level=3,
    )
    n_symbols = result["symbol"].n_unique()
    n_unstable = int((~result["stable"]).sum())
    logger.info(
        f"[gold_forecast] {n_symbols} symbols x {len(FORECAST_HORIZONS)} horizons "
        f"({n_unstable} unstable-VAR rows) -> {FORECAST_STORE_PATH.name}"
    )
