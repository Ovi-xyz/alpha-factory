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

FIX GMI-FORECAST-DIM-01 (17 Sep 2026) — subcategory aggregation before
PCA + decoupled PCA lookback window. Confirmed empirically via
2026-09-17-cross-asset-engine-running-log.txt: 100% VAR fit failure
across the active_ohlcv universe ("maxlags is too large for the number
of observations and the number of equations"), identical error for
every equity regardless of ticker — the tell that the shared PCA output
was the bottleneck, not any per-symbol data gap. Root cause: at the time
forecast_context() had grown to 40 instruments (dollar_basket alone
added 7 minor-currency legs across ADR-013/014/024/036/037) while
LOOKBACK_DAYS stayed 65 calendar days (~47-48 trading-day observations,
per correlation_module.py's own MIN_OBSERVATIONS calibration comment) —
p~=41 features (incl. broad_dollar_return) against n~=47-48 rows is
exactly the regime where PCA(n_components=0.95) on noisy daily return
data has no concentrated factor structure to find and instead retains
25-40+ components rather than the 3-5 Architecture v2.0 §6.4.1-6.4.3
assumed, making every per-equity VAR(n_pcs+1, maxlags=5) unidentifiable
regardless of which equity is being fit.

Two independent fixes, not a parameter cap on n_components (which would
discard information by fiat rather than address the p/n ratio itself):
  1. _aggregate_by_subcategory() collapses the ~40 raw instrument columns
     into one z-scored composite per context_category (the taxonomy's own
     economic grouping in instruments_taxonomy.yaml — e.g. the 9
     context_equity_dm indices become one composite instead of 9
     collinear PCA columns), cutting p from ~41 to ~13-14 subcategories.
  2. PCA_LOOKBACK_DAYS decouples the Layer 2/Broad-Dollar PCA input window
     from LOOKBACK_DAYS (still 65, governing per-equity follower reads).
     Widened to 180 calendar days (~125 trading days) specifically for
     the PCA fit — ForecastModule has no CorrelationModule-style
     regime-purity requirement forcing a short window (§6.2.1 wants
     correlation matrices to reflect the CURRENT regime specifically;
     nothing analogous is stated for the PCA factor-extraction step here),
     so widening it is not a hidden trade-off against another documented
     requirement. Result: p~=13-14 against n~=125 (ratio ~9x) is a
     genuinely well-conditioned PCA fit, standard practice territory
     (n > 5-10x p) rather than the prior p~=n knife-edge.
Both changes are config-over-code against instruments_taxonomy.yaml's
existing context_category field — no new schema, no new files. See
_aggregate_by_subcategory()'s own docstring for the z-score-before-
average mechanics and the backward-compatible fallback for callers
(tests) that don't model context_category.

FIX GMI-FORECAST-DIM-02 (19 Sep 2026) — hard PCA component cap (MAX_PCS),
follow-up to DIM-01 above. Confirmed empirically via the 2026-09-19
gold_forecast run: 100% VAR fit failure persisted UNCHANGED after DIM-01
shipped, identical error message, every symbol. Root cause: DIM-01's
subcategory aggregation collapses ~40 raw columns to ~13-14 composites,
but those composites are economically near-orthogonal BY DESIGN (that is
the entire point of the taxonomy grouping) — so PCA(n_components=
PCA_VARIANCE_TARGET=0.95) no longer compresses them the way it compressed
the original heavily-redundant raw columns; it now retains close to all
~13-14 composites to reach 95% variance. DIM-01 fixed the PCA FIT's own
p>n numerical problem but never touched the PCA OUTPUT size, so the
identical downstream neqs-vs-n_totobs identification failure reappeared
one level down (n_pcs instead of raw p). Fix: n_components is now a hard
integer cap (MAX_PCS=4), decoupled from PCA_VARIANCE_TARGET, sized so
neqs=n_pcs+1=5 stays identifiable at maxlags=5 against the realistic
~40-48-observation per-equity window — confirmed against the real
statsmodels VAR.fit() (neqs=14 replicates the production failure exactly
at n_totobs=44; neqs=5 is identifiable from n_totobs>=36). MIN_PAIR_OBS
raised from MAX_VAR_LAG+15=20 to 40 in lockstep (20 was insufficient even
for the capped neqs=5 case), matching correlation_module.py's own
MIN_OBSERVATIONS=40 (XAE-CAL-01) for the same 65-day window. See MAX_PCS
and MIN_PAIR_OBS's own comments below for the full numeric derivation.

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

LOOKBACK_DAYS        = 65    # Calendar days — bounds the PER-EQUITY (follower)
                              # Silver scan window only. matches Correlation/LeadLag;
                              # §6.4.3 "60-day rolling window is the minimum".
                              # FIX GMI-FORECAST-DIM-01 (17 Sep 2026): no longer used
                              # for the Layer 2/Broad-Dollar PCA input — see
                              # PCA_LOOKBACK_DAYS below and the module docstring.
PCA_LOOKBACK_DAYS    = 180   # FIX GMI-FORECAST-DIM-01 (17 Sep 2026): calendar days
                              # for the Layer 2 forecast_context() + Broad Dollar PCA
                              # input specifically — deliberately decoupled from
                              # LOOKBACK_DAYS above. ~125 trading days vs. p~=13-14
                              # subcategory composites (post-aggregation) is a
                              # well-conditioned PCA fit (n > 5-10x p); the prior
                              # shared 65-day window gave p~=41 raw instruments
                              # against n~=47-48 rows — see module docstring for the
                              # full empirical root-cause account. Not shared with
                              # LOOKBACK_DAYS because ForecastModule's PCA step has no
                              # CorrelationModule-style regime-purity requirement
                              # forcing a short window (§6.2.1's rationale for a short
                              # window is specific to that module).
MIN_OBSERVATIONS     = 40    # FIX XAE-CAL-01 (16 Sep 2026): observation-count native,
                              # calendar-aware — replaces MIN_HISTORY_RATIO=0.8, which
                              # was unreachable for any 5-day-week market (max 48
                              # weekdays in a 65-day window, empirically verified
                              # against live Silver data, 16 Sep 2026 session). See
                              # correlation_module.py's MIN_OBSERVATIONS comment for
                              # full rationale/calibration. This threshold gates BOTH
                              # the Layer 2 PCA input pivot and the Layer 1 equity
                              # returns pivot (both go through _load_pivot below).
PCA_VARIANCE_TARGET  = 0.95  # §6.4.2 Step 2 — ORIGINAL spec target. FIX
                              # GMI-FORECAST-DIM-02 (19 Sep 2026): no longer
                              # drives component SELECTION (see MAX_PCS
                              # below) — kept only as the nominal spec
                              # citation; variance_explained is still
                              # reported in the output schema against
                              # whatever MAX_PCS components are actually
                              # retained.
MAX_PCS              = 4     # FIX GMI-FORECAST-DIM-02 (19 Sep 2026): hard
                              # cap on retained PCA components, decoupled
                              # from PCA_VARIANCE_TARGET. Root cause:
                              # after GMI-FORECAST-DIM-01's subcategory
                              # aggregation, the ~13-14 composite columns
                              # are economically near-orthogonal BY
                              # CONSTRUCTION (that's the taxonomy's whole
                              # point — dollar vs equity_dm vs
                              # commodity_energy etc. are meant to be
                              # distinct signals), so PCA(n_components=0.95)
                              # no longer compresses to 3-5 PCs the way it
                              # did on ~40 raw, heavily redundant
                              # instrument columns — it now retains close
                              # to all ~13-14 composites to hit 95%
                              # variance, reproducing the identical
                              # "maxlags too large" 100% VAR failure via
                              # neqs=n_pcs+1 instead of via raw p.
                              # Confirmed empirically against the real
                              # statsmodels VAR.fit(): neqs=14 at
                              # n_totobs=44 fails identically to the
                              # 17/19 Sep production logs; neqs=5 (i.e.
                              # n_pcs=MAX_PCS=4) is identifiable from
                              # n_totobs>=36 with margin. 4, not 5 (the
                              # upper end of Architecture v2.0 §6.4.2's
                              # "3-5 PCs" range), because n_pcs=5 only
                              # becomes identifiable at n_totobs>=44 — too
                              # tight against the ~43-48 typical
                              # per-equity observation count in a 65-day
                              # window (correlation_module.py's own
                              # MIN_OBSERVATIONS calibration comment) to
                              # survive a worse week.
MAX_VAR_LAG          = 5
FORECAST_HORIZONS    = [1, 2, 3, 4, 5]
MIN_PAIR_OBS         = 40    # FIX GMI-FORECAST-DIM-02 (19 Sep 2026): raised
                              # from MAX_VAR_LAG+15=20 — that floor let
                              # merged samples as small as 20 rows into a
                              # VAR(neqs=5, maxlags=5) fit, which
                              # statsmodels cannot identify until
                              # n_totobs>=36 (confirmed empirically, see
                              # MAX_PCS above). 40 matches
                              # correlation_module.py's own
                              # MIN_OBSERVATIONS (XAE-CAL-01) for the
                              # identical 65-day window and gives the same
                              # margin for a worse week (e.g. an Eid
                              # al-Fitr closure landing inside the window).


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
        # FIX GMI-FORECAST-DIM-01: layer2_instruments (not just symbols)
        # kept around so _aggregate_by_subcategory can read context_category
        # off each Instrument after the pivot is built.
        layer2_instruments = get_loader().forecast_context()
        layer2_symbols = [inst.symbol for inst in layer2_instruments]
        layer2_pivot: Optional[pl.DataFrame] = None
        if layer2_symbols:
            # FIX GMI-FORECAST-DIM-01: PCA_LOOKBACK_DAYS (wider, decoupled
            # from per-equity LOOKBACK_DAYS) — see module docstring + the
            # constant's own comment for the full empirical rationale.
            layer2_pivot = self._load_pivot(
                layer2_symbols, run_date, include_layer2=True, include_layer1=False,
                lookback_days=PCA_LOOKBACK_DAYS,
            )
            if layer2_pivot is not None:
                layer2_pivot = self._aggregate_by_subcategory(layer2_pivot, layer2_instruments)
        else:
            logger.info(
                "[gold_forecast] forecast_context() returned no eligible symbols — "
                "continuing with Broad Dollar alone, if available"
            )

        # FIX GMI-FORECAST-DIM-01: PCA_LOOKBACK_DAYS, matching layer2_pivot's
        # window — load_fx_returns()'s own docstring: "Broad Dollar and PCA
        # read from one consistent window" (still true, just a wider one now).
        fx_returns = load_fx_returns(run_date, PCA_LOOKBACK_DAYS)
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

    # ── Subcategory aggregation (FIX GMI-FORECAST-DIM-01) ────────────────

    @staticmethod
    def _aggregate_by_subcategory(
        pivot: pl.DataFrame, instruments: list
    ) -> pl.DataFrame:
        """
        FIX GMI-FORECAST-DIM-01 (17 Sep 2026): collapse forecast_context()'s
        ~40 individual instrument columns into one composite per
        context_category BEFORE PCA — see module docstring for the full
        empirical root-cause account (p~=41 vs n~=47-48 -> PCA retaining
        25-40+ components instead of the assumed 3-5, 100% VAR fit failure).

        Grouping uses instruments_taxonomy.yaml's own context_category field
        (e.g. all 9 context_equity_dm indices -> one composite) — an
        economic classification already maintained for domain-score
        routing, not a new statistical threshold invented here.

        Per-column z-score (nan-safe, ddof=0) before averaging within a
        group, so unequal-volatility members (HKD's near-zero-variance peg
        vs. NICKEL's structural-break-era swings) don't let the loudest
        member dominate the composite — same z-score-before-combine
        convention instruments_taxonomy.yaml's _meta.contributes_to
        aggregation methods already use throughout (z_score_level,
        z_score_momentum_20d, etc.). A flat/zero-variance column (std==0,
        e.g. a currency peg with literally no observed movement in-window)
        gets std=1.0 substituted rather than dividing by zero — its
        z-score reduces to (x - mean), i.e. contributes its own noise
        rather than exploding to inf/NaN.

        Backward-compat fallback: an instrument with no context_category
        attribute (or None — e.g. test doubles that only set .symbol)
        becomes its own singleton group keyed by its symbol, reproducing
        the pre-fix one-column-per-symbol behavior exactly. This means
        every existing test built on a bare _FakeInstrument(symbol) is
        unaffected — aggregation is a genuine no-op when category info
        isn't modeled, not a behavior change disguised as backward-compat.
        """
        symbol_to_category: dict[str, str] = {}
        for inst in instruments:
            category = getattr(inst, "context_category", None) or inst.symbol
            symbol_to_category[inst.symbol] = category

        groups: dict[str, list[str]] = {}
        for col in pivot.columns:
            if col == "date":
                continue
            category = symbol_to_category.get(col, col)
            groups.setdefault(category, []).append(col)

        composite_cols: dict[str, np.ndarray] = {}
        for category, cols in groups.items():
            data = pivot.select(cols).to_numpy()
            col_mean = np.nanmean(data, axis=0)
            col_std = np.nanstd(data, axis=0)
            col_std = np.where((col_std < 1e-12) | np.isnan(col_std), 1.0, col_std)
            z = (data - col_mean) / col_std
            composite_cols[category] = np.nanmean(z, axis=1)

        result = pl.DataFrame(composite_cols)
        return result.with_columns(pivot["date"])

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

        # FIX GMI-FORECAST-DIM-02 (19 Sep 2026): hard integer cap (MAX_PCS),
        # not a variance-explained target — see MAX_PCS's own comment.
        # PCA_VARIANCE_TARGET (0.95) was always the effective min() winner
        # here for any realistic data.shape, so the float/int branch below
        # never actually exercised its int path in production.
        n_components = min(MAX_PCS, data.shape[0] - 1, data.shape[1])
        pca = PCA(n_components=int(n_components))
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
        self, symbols: list[str], run_date: date, include_layer2: bool, include_layer1: bool,
        lookback_days: int = LOOKBACK_DAYS,
    ) -> Optional[pl.DataFrame]:
        # FIX GMI-FORECAST-DIM-01: lookback_days now an explicit parameter
        # (default unchanged) so the Layer 2 PCA-input call can pass
        # PCA_LOOKBACK_DAYS while the per-equity follower call keeps the
        # original LOOKBACK_DAYS — see module docstring.
        globs: list[str] = []
        if include_layer1:
            globs += layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        if include_layer2:
            ctx_glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if ctx_glob is not None:
                globs += [ctx_glob]
        if not globs:
            return None

        start = run_date - timedelta(days=lookback_days)
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
