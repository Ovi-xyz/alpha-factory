"""
src/gold/hmm/raw_market.py — ADR-051 (GMI-HMM2-001), market branch, module 2 of 5.

Layer 2 of hmm-gatekeeper-xgboost.md ("Market Raw Observation Module"): the
daily market-BEHAVIOUR panel feeding the market HMM. Two feature sources, both
Silver, routed by the `hmm_market:` block of config/fred_series.yaml
(validated by config.market_config):

  fred            VIXCLS level; DEXUSEU/DEXJPUS 20-observation log change;
                  DGS5/DGS10/T10Y2Y day-over-day change — PIT-gated by the SAME
                  fred_pit rule and basis as raw_macro (one rule, one implementation)
  silver_breadth  cross-section of the Layer 1 US-stock 1D bars: share of
                  symbols up, median return, dispersion, and the RMS of the
                  median return over the last `rms_window` valid trading days

Input   data/silver/macro_enriched/fred_<date>_silver.parquet
        data/silver/market_ohlcv/us_stocks/symbol=<S>/<S>_1D_silver.parquet
Output  data/gold/hmm/raw_market.parquet
            as_of_date                 Date    US trading day — "known at the end of this day"
            <feature> x10              Float64 one column per hmm_market.features entry
            obs_date__<fred feature>   Date    the period the carried FRED value MEASURES
            available_on__<...>        Date    first date the system could use it
            avail_basis__<...>         String  recorded | estimated | mixed (see fred_pit)
            us_n_symbols               Int32   clean Layer 1 returns on that date
            us_n_eligible              Int32   Layer 1 symbols already listed on that date
            us_coverage_ratio          Float64 us_n_symbols / us_n_eligible
            us_breadth_valid           Boolean passed BOTH breadth guards (below)
            computation_date / processing_version / source_snapshot / pit_basis
            breadth_guard_version      String  hmm_market.breadth.guard_version
            breadth_guard_fingerprint  String  digest of every guard field (see config)
            breadth_symbol_files       Int32   Layer 1 symbol files actually read
        For the breadth features the observation date IS the as-of date (a day's
        returns are complete at its close), so they carry no obs_date__ column.
        Ages are not stored: as_of_date - obs_date__<feature>.

DOMAIN INDEPENDENCE (ADR-051 Amendment 2 §A): Silver only. NO Gold store is read
— in particular not gold_global_regime (a CrossAssetEngine output = the
cross_asset_xgboost domain). Reading it would feed the same breadth information
to two specialists (double counting in decision_xgboost) and let a CrossAssetEngine
outage degrade this domain. market_config makes a Gold source unexpressible.

LAYER 1 ONLY, BY EXPLICIT FILE LIST — not a glob. The breadth universe is the
InstrumentLoader's Layer 1 `us_stocks` list, resolved to explicit file paths.
Measured on the live repo (6 Oct 2026): market_ohlcv/us_stocks holds 543
directories against 542 registered symbols — the extra is `symbol=CL`, stale
Colgate-Palmolive data frozen at 2026-09-10 left over from the CL stock / CL=F
crude ticker collision. An `us_stocks/**` glob (the ADR-022/RISK-6 failure
class, cf. src/utils/silver_scope.py) would silently fold it into every breadth
number; an explicit list cannot.

ROWS are US trading days — dates with at least one clean Layer 1 return — not
weekdays: a holiday has no market behaviour to observe, and a carried-forward
"share up" would be a fabricated observation. FRED features are carried to those
dates as-of, under hmm.pit.basis (their availability lags: VIXCLS is usually
usable the next day). Breadth for date D is complete at D's close.

TIMESTAMPS: Silver 1D bars are stamped at LOCAL exchange midnight
(America/New_York), not UTC as the Grand Design text says; the stamp's own
calendar date is the trading date, so the date is taken with `.dt.date()`
WITHOUT a timezone conversion (converting a local-midnight stamp to UTC or back
could shift the date).

BREADTH GUARDS (first-pass calibration, ADR-055 E7 — see hmm_market.breadth). A
date is VALID iff both pass, and the decision is reproducible from the stored row
alone plus the versioned thresholds:
  A  us_n_symbols >= min_symbols            (garbage-in floor)
  B  us_coverage_ratio >= min_coverage_ratio (a PARTIAL day — e.g. a late ingest of
     the latest bar — fails: clean returns / ELIGIBLE symbols, where eligible =
     symbols whose first clean return is on or before the date, so names listed
     later never count against earlier years)
An invalid date keeps its row and its counts; only the four breadth features are
nulled. The RMS window runs over VALID days only: an invalid day neither
contributes to nor blanks it. (Replaces a trailing-median partial-day rule that
could not be reproduced from a single stored row.)

RMS_MEDIAN_RETURN is sqrt(mean(median_return^2)) over the last `rms_window` valid
days: zero-mean by construction (no mean is estimated) and a statistic of the
cross-sectional MEDIAN return, not the volatility of any tradable portfolio —
hence not called "realised volatility".

IDEMPOTENCY: every run rebuilds the whole panel from Silver (~1.4M rows read, a
few seconds on the M1) and atomically replaces the store. Nothing here looks
past run_date, and every guard is a per-date function of that date's own row.

FAILURE POLICY (loud, like raw_macro): no FRED snapshot, a configured FRED series
with zero observations, no Layer 1 breadth data, or fewer symbol files than
min_symbols — each is a registry/ingestion defect, not a degraded mode.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional, Sequence

import polars as pl
from loguru import logger

from src.gold.hmm import fred_pit
from src.gold.hmm.config import BreadthSettings, MarketConfig, market_config
from src.utils.atomic_io import atomic_write_parquet

GOLD_HMM_PATH      = Path("data/gold/hmm")
RAW_MARKET_PATH    = GOLD_HMM_PATH / "raw_market.parquet"
SILVER_OHLCV_ROOT  = Path("data/silver/market_ohlcv")
PROCESSING_VERSION = "1.1"      # 1.1: coverage-ratio guard, rms_median_return, three-date lineage

KEY = fred_pit.KEY              # "as_of_date"

_METRIC_COLUMN = {
    "share_up":          "_share_up",
    "median_return":     "_median_return",
    "dispersion":        "_dispersion",
    "rms_median_return": "_rms_median_return",
}


class RawMarketError(RuntimeError):
    """Registry/config/data defect that makes the market panel unbuildable."""


# ── Layer 1 universe → explicit Silver file list ─────────────────────────────

def layer1_symbols(market: str) -> list[str]:
    """Layer 1 symbols of one market, from the InstrumentLoader (the single
    gateway to the instrument universe — never a directory listing)."""
    from src.config.instrument_loader import get_loader
    return sorted(i.symbol for i in get_loader().by_market(market) if i.is_layer1)


def silver_files(root: Path, market: str, timeframe: str, symbols: Sequence[str]) -> list[tuple[str, Path]]:
    """(symbol, path) for every symbol whose Silver file exists. Explicit paths:
    a stray directory on disk can never enter the universe."""
    out = []
    for s in symbols:
        p = root / market / f"symbol={s}" / f"{s}_{timeframe}_silver.parquet"
        if p.exists():
            out.append((s, p))
    return out


def load_returns(files: Sequence[tuple[str, Path]]) -> pl.DataFrame:
    """symbol, date (Date — the BAR's date), log_return (Float64) — clean, finite
    returns only. Mirrors PITDataLoader's `is_clean = TRUE` (a NULL is_clean — the
    first bar of every file — is excluded, as is a NULL log_return). One lazy
    frame per file, concatenated, so per-file dtype differences cannot break the
    scan."""
    if not files:
        return pl.DataFrame(schema={"symbol": pl.String, "date": pl.Date, "log_return": pl.Float64})
    lfs = [
        pl.scan_parquet(p).select(
            pl.lit(s).alias("symbol"),
            pl.col("timestamp").dt.date().alias("date"),     # stamp's own date — see TIMESTAMPS
            pl.col("log_return").cast(pl.Float64),
            pl.col("is_clean").cast(pl.Boolean),
        ).filter(pl.col("is_clean") & pl.col("log_return").is_not_null() & pl.col("log_return").is_finite())
        .select("symbol", "date", "log_return")
        for s, p in files
    ]
    return pl.concat(lfs).collect()


# ── Breadth ──────────────────────────────────────────────────────────────────

def compute_breadth(returns: pl.DataFrame, st: BreadthSettings) -> pl.DataFrame:
    """Per-date cross-sectional behaviour, one row per date with >= 1 clean return.

    Columns: as_of_date, us_n_symbols (Int32), us_n_eligible (Int32),
    us_coverage_ratio (Float64), us_breadth_valid (Boolean), _share_up,
    _median_return, _dispersion, _rms_median_return (Float64, null where
    us_breadth_valid is False)."""
    daily = (
        returns.group_by("date")
        .agg(
            pl.len().cast(pl.Int32).alias("us_n_symbols"),
            (pl.col("log_return") > 0).mean().alias("_share_up"),
            pl.col("log_return").median().alias("_median_return"),
            pl.col("log_return").std(ddof=1).alias("_dispersion"),
        )
        .sort("date")
    )

    # Eligible = symbols whose FIRST clean return is on or before the date: a
    # cumulative count over first-return dates. A date with any return has >= 1
    # eligible symbol (its own), so the ratio is well defined and <= 1.
    eligible = (
        returns.group_by("symbol").agg(pl.col("date").min().alias("first_date"))
        .group_by("first_date").agg(pl.len().alias("k"))
        .sort("first_date")
        .select("first_date", pl.col("k").cum_sum().cast(pl.Int32).alias("us_n_eligible"))
    )
    daily = (
        daily.join_asof(eligible, left_on="date", right_on="first_date", strategy="backward")
        .drop("first_date")
        .with_columns((pl.col("us_n_symbols") / pl.col("us_n_eligible")).alias("us_coverage_ratio"))
    )
    valid_expr = (pl.col("us_n_symbols") >= st.min_symbols) & (pl.col("us_coverage_ratio") >= st.min_coverage_ratio)
    daily = daily.with_columns(valid_expr.alias("us_breadth_valid"))

    # RMS of the median return over the last W VALID days (zero-mean by construction).
    w = st.rms_window
    rms = (
        daily.filter(pl.col("us_breadth_valid"))
        .select(
            "date",
            (pl.col("_median_return") ** 2).rolling_mean(window_size=w, min_samples=w).sqrt()
            .alias("_rms_median_return"),
        )
    )
    daily = daily.join(rms, on="date", how="left")

    valid = pl.col("us_breadth_valid")
    cols = ["_share_up", "_median_return", "_dispersion", "_rms_median_return"]
    return daily.with_columns(
        [pl.when(valid).then(pl.col(c)).otherwise(None).alias(c) for c in cols]
    ).select(
        pl.col("date").alias(KEY), "us_n_symbols", "us_n_eligible", "us_coverage_ratio", "us_breadth_valid", *cols,
    )


# ── Build / run ──────────────────────────────────────────────────────────────

def build(
    run_date: date,
    silver_dir: Path | str | None = None,
    ohlcv_root: Path | str | None = None,
    config_path: Path | str | None = None,
    cfg: Optional[MarketConfig] = None,
    symbols: Optional[Sequence[str]] = None,
) -> pl.DataFrame:
    """Build the market-behaviour panel as of `run_date` (no I/O write).
    `symbols` overrides the InstrumentLoader's Layer 1 list (tests only)."""
    cfg = cfg or market_config(config_path)
    if cfg.breadth is None:
        raise RawMarketError("hmm_market has no silver_breadth feature — nothing defines the trading-day index")
    st = cfg.breadth

    snap = fred_pit.latest_snapshot(run_date, silver_dir)
    if snap is None:
        raise RawMarketError(f"no FRED Silver snapshot on or before {run_date} — run silver_macro first")
    needed = cfg.required_series()
    obs = fred_pit.load_observations(snap, needed)
    missing = sorted(set(needed) - set(obs["series_id"].unique().to_list()))
    if missing:
        raise RawMarketError(
            f"{snap.name}: configured series with no observations: {missing} "
            f"(registry/ingestion defect — not a short-history case)")

    root = SILVER_OHLCV_ROOT if ohlcv_root is None else Path(ohlcv_root)
    universe = list(symbols) if symbols is not None else layer1_symbols(st.market)
    files = silver_files(root, st.market, st.timeframe, universe)
    if len(files) < st.min_symbols:
        raise RawMarketError(
            f"only {len(files)}/{len(universe)} Layer 1 {st.market} {st.timeframe} Silver files found "
            f"under {root} (min_symbols={st.min_symbols}) — run silver_ohlcv first")
    if len(files) < 0.95 * len(universe):
        logger.warning(f"[raw_market] breadth universe shrunk: {len(files)}/{len(universe)} Layer 1 files present")

    returns = load_returns(files).filter(pl.col("date") <= run_date)    # nothing past run_date
    if returns.height == 0:
        raise RawMarketError(f"no clean Layer 1 {st.market} returns on or before {run_date}")
    breadth = compute_breadth(returns, st)

    fred_panel = fred_pit.build_panel(
        obs, cfg.fred_features(), cfg.lag_days, end=run_date,
        calendar=breadth.select(KEY), basis=cfg.pit_basis)
    panel = fred_panel.join(breadth, on=KEY, how="left").sort(KEY)

    # Breadth metric columns -> configured feature names.
    panel = panel.rename({_METRIC_COLUMN[f.metric]: f.feature for f in cfg.breadth_features()})
    panel = panel.drop([c for c in _METRIC_COLUMN.values() if c in panel.columns])

    names = cfg.feature_names()
    fred_names = [f.feature for f in cfg.fred_features()]
    return panel.select(
        KEY, *names,
        *[f"obs_date__{n}" for n in fred_names],
        *[f"available_on__{n}" for n in fred_names],
        *[f"avail_basis__{n}" for n in fred_names],
        "us_n_symbols", "us_n_eligible", "us_coverage_ratio", "us_breadth_valid",
    ).with_columns(
        pl.lit(run_date).alias("computation_date"),
        pl.lit(PROCESSING_VERSION).alias("processing_version"),
        pl.lit(snap.name).alias("source_snapshot"),
        pl.lit(cfg.pit_basis).alias("pit_basis"),
        pl.lit(st.guard_version).alias("breadth_guard_version"),
        pl.lit(st.fingerprint()).alias("breadth_guard_fingerprint"),
        pl.lit(len(files), dtype=pl.Int32).alias("breadth_symbol_files"),
    )


def run(run_date: date) -> None:
    """Job entry point ('gold_raw_market' in JOB_REGISTRY)."""
    df = build(run_date)
    atomic_write_parquet(df, RAW_MARKET_PATH)
    valid = int(df["us_breadth_valid"].sum())
    logger.info(
        f"[raw_market] {run_date}: {df.height} trading days {df[KEY].min()}..{df[KEY].max()} "
        f"({valid} with valid breadth) from {df['source_snapshot'][0]}, "
        f"{df['breadth_symbol_files'][0]} Layer 1 symbol files, guard v{df['breadth_guard_version'][0]} "
        f"[{df['breadth_guard_fingerprint'][0]}] -> {RAW_MARKET_PATH}"
    )
