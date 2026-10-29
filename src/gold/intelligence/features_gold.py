"""
features_gold.py — ADR-053 D2: the as-of feature builder of `gold_xgboost` (technical domain).

ONE BUILDER FOR BOTH PATHS. The fit path joins every labelled (symbol, decision_date) to the
per-bar indicator history; the predict path joins (symbol, run_date). Same functions, same
columns, same availability rule — no train/serve skew by construction.

WHY NOT PITDataLoader.get_signals (measured 26 Oct 2026, ADR-053 finding F3)
    `tech_signals_{TF}.parquet` holds ~15 years of causal per-bar indicators, but its
    `signal_date` column is ONE constant (the last run date) on every row, so a `signal_date <=
    trade_date` guard returns nothing for any earlier date; and `vix_at_signal` /
    `high_volatility_flag` are single constants across all 141,645 historical rows (a run-day
    market value — cross-domain and lookahead). This builder keys on the BAR date instead and
    never reads either column.

AVAILABILITY RULE (matches the label convention, ADR-052 Amendment A1)
    A decision on date T may use a bar iff the bar was COMPLETE before T:
        1D   bar_date + 1 day <= T                       (the labeler's anchor is the last bar < T)
        1W   Sunday of the bar's week + 1 day <= T       (weekly bars are stamped at the week START)
        1M   first day of the month after the bar <= T   (monthly bars are stamped at the month START)
    The in-progress weekly / monthly bar is deliberately NOT used: it cannot be reconstructed as of
    a past date from the stored file, and a `timestamp < T` guard would hand a mid-week decision the
    rest of that week (up to ~4 days of lookahead; ~a month for 1M). This differs from gold_mtf's
    live trend columns, which read the latest (possibly partial) bar — stated, not hidden.

TIMEFRAMES: 1D + 1W + 1M only. 1H/4H history is ~2 years (GD §6.4) — using them would either cut
the training window to 2 years or make them missing for most of it (a tree then learns calendar
time, not structure). Complete cases only; nothing is imputed.

INDEPENDENCE: reads only the technical store (data/gold/signals); the symbol -> market map comes from
the labels at fit time and is persisted inside the model bundle, so inference needs no registry.

FIRST-PASS feature choices (KNOWN_RISKS RISK-40).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Mapping, Optional

import polars as pl

from src.gold.intelligence.core import IntelligenceError

SIGNALS_DIR = Path("data/gold/signals")
TIMEFRAMES = ("1D", "1W", "1M")
# Bars a symbol needs BEFORE its indicators count: EMA/MACD/ADX are computed from the first bar with no
# minimum period, so the first bars are warm-up artifacts, not signal. Features are null until then
# (complete-case filtering drops the row) — 250 daily bars cover ema_200, 60 weekly / 36 monthly the
# EMA-50 / MACD(26,9) chains.
WARMUP_BARS = {"1D": 250, "1W": 60, "1M": 36}
TOLERANCE = {"1D": timedelta(days=7), "1W": timedelta(days=14), "1M": timedelta(days=45)}
MARKETS = ("commodity", "forex", "idx", "us_stocks")
DIRECTIONS = ("long", "short")

_RAW = ("symbol", "timestamp", "close", "ema_9", "ema_21", "ema_50", "ema_200", "rsi_14", "macd_hist", "adx",
        "di_plus", "di_minus", "atr_pct", "bbands_upper", "bbands_lower", "relative_volume", "momentum_score")

# per-TF feature -> short prefix
_FEATS_1D = ("trend", "dist_ema21", "dist_ema50", "ema9_21", "ema50_200", "rsi_14", "macd_hist_pct", "adx",
             "di_diff", "atr_pct", "bb_pos", "rel_vol", "momentum", "ret_5", "ret_20")
_FEATS_1W = ("trend", "dist_ema21", "dist_ema50", "ema9_21", "rsi_14", "macd_hist_pct", "adx", "di_diff",
             "atr_pct", "bb_pos")
_FEATS_1M = ("rsi_14", "dist_ema21", "macd_hist_pct")
TF_FEATURES = {"1D": tuple(f"d_{f}" for f in _FEATS_1D), "1W": tuple(f"w_{f}" for f in _FEATS_1W),
               "1M": tuple(f"m_{f}" for f in _FEATS_1M)}
BREADTH_FEATURES = ("breadth_ema50_market", "breadth_ema50_market_chg5")
DERIVED = ("mtf_score_dw",)
FEATURES = (*TF_FEATURES["1D"], *TF_FEATURES["1W"], *TF_FEATURES["1M"], *BREADTH_FEATURES, *DERIVED,
            "direction_sign", "market_code")


def _bar_features(df: pl.DataFrame, tf: str) -> pl.DataFrame:
    """Per-bar derived features + `avail_date` for one timeframe (all causal: one bar's own
    indicator columns, plus close-to-close returns on 1D)."""
    pre = {"1D": "d_", "1W": "w_", "1M": "m_"}[tf]
    d = df.with_columns(pl.col("timestamp").dt.date().alias("bar_date")).sort(["symbol", "bar_date"])
    safe = lambda num, den: pl.when(den.abs() > 1e-12).then(num / den).otherwise(None)
    ex = {
        "trend": pl.when((pl.col("close") > pl.col("ema_50")) & (pl.col("ema_9") > pl.col("ema_21"))).then(1.0)
                   .when((pl.col("close") < pl.col("ema_50")) & (pl.col("ema_9") < pl.col("ema_21"))).then(-1.0)
                   .otherwise(0.0).alias("trend"),
        "dist_ema21": (pl.col("close") / pl.col("ema_21") - 1.0).alias("dist_ema21"),
        "dist_ema50": (pl.col("close") / pl.col("ema_50") - 1.0).alias("dist_ema50"),
        "ema9_21": (pl.col("ema_9") / pl.col("ema_21") - 1.0).alias("ema9_21"),
        "ema50_200": (pl.col("ema_50") / pl.col("ema_200") - 1.0).alias("ema50_200"),
        "rsi_14": pl.col("rsi_14").alias("rsi_14"),
        "macd_hist_pct": (pl.col("macd_hist") / pl.col("close")).alias("macd_hist_pct"),
        "adx": pl.col("adx").alias("adx"),
        "di_diff": (pl.col("di_plus") - pl.col("di_minus")).alias("di_diff"),
        "atr_pct": pl.col("atr_pct").alias("atr_pct"),
        "bb_pos": safe(pl.col("close") - pl.col("bbands_lower"), pl.col("bbands_upper") - pl.col("bbands_lower")).alias("bb_pos"),
        "rel_vol": pl.col("relative_volume").alias("rel_vol"),
        "momentum": pl.col("momentum_score").alias("momentum"),
    }
    wanted = {"1D": _FEATS_1D, "1W": _FEATS_1W, "1M": _FEATS_1M}[tf]
    exprs = [ex[f] for f in wanted if f in ex]
    d = d.with_columns(exprs)
    if tf == "1D":
        d = d.with_columns((pl.col("close") / pl.col("close").shift(5).over("symbol") - 1.0).alias("ret_5"),
                           (pl.col("close") / pl.col("close").shift(20).over("symbol") - 1.0).alias("ret_20"))
    if tf == "1D":
        avail = pl.col("bar_date") + pl.duration(days=1)
    elif tf == "1W":
        avail = pl.col("bar_date") + pl.duration(days=(7 - pl.col("bar_date").dt.weekday()).cast(pl.Int64) + 1)
    else:
        avail = pl.col("bar_date").dt.month_end() + pl.duration(days=1)
    d = d.with_columns(avail.alias("avail_date"))
    feat_cols = [f for f in wanted]
    warm = pl.int_range(pl.len()).over("symbol") < WARMUP_BARS[tf]
    d = d.with_columns([pl.when(warm).then(None).otherwise(pl.col(f)).alias(f) for f in feat_cols])
    if tf == "1D":                                            # breadth reads the raw EMA-50: mask its warm-up too
        d = d.with_columns(pl.when(warm).then(None).otherwise(pl.col("ema_50")).alias("ema_50"))
    keep = ["symbol", "bar_date", "avail_date"] + ([f"{c}" for c in wanted])
    if tf == "1D":
        keep += ["close", "ema_50"]                           # breadth needs the raw pair, dropped later
    out = d.select(keep).rename({f: pre + f for f in wanted})
    return out.with_columns([pl.col(c).cast(pl.Float32) for c in out.columns
                             if c not in ("symbol", "bar_date", "avail_date")]).with_columns(
        [pl.when(pl.col(c).is_finite()).then(pl.col(c)).otherwise(None).alias(c)
         for c in out.columns if c not in ("symbol", "bar_date", "avail_date")])


def load_tables(signals_dir: Path | str = SIGNALS_DIR) -> dict[str, pl.DataFrame]:
    """Per-TF per-bar feature tables from data/gold/signals/tech_signals_{TF}.parquet."""
    out = {}
    for tf in TIMEFRAMES:
        path = Path(signals_dir) / f"tech_signals_{tf}.parquet"
        if not path.exists():
            raise IntelligenceError(f"{path} missing — run gold_signals")
        cols = list(_RAW)
        have = pl.read_parquet_schema(path)
        missing = [c for c in cols if c not in have]
        if missing:
            raise IntelligenceError(f"{path.name} lacks columns {missing}")
        out[tf] = _bar_features(pl.read_parquet(path, columns=cols), tf)
    return out


def breadth_table(daily: pl.DataFrame, symbol_market: pl.DataFrame) -> pl.DataFrame:
    """Share of the tech-signal universe of a market whose daily close is above its EMA-50, per
    (market, bar_date), and its 5-bar change. The universe is the set of symbols with a bar that
    day (survivorship of the current active list applies — KNOWN_RISKS RISK-40)."""
    d = (daily.select("symbol", "bar_date", "avail_date", "close", "ema_50")
         .join(symbol_market, on="symbol", how="inner")
         .filter(pl.col("ema_50").is_not_null() & pl.col("close").is_not_null()))
    b = (d.group_by(["market", "bar_date", "avail_date"])
         .agg((pl.col("close") > pl.col("ema_50")).cast(pl.Float64).mean().alias("breadth_ema50_market"),
              pl.len().alias("_n"))
         .filter(pl.col("_n") >= 3)
         .sort(["market", "bar_date"]))
    return b.with_columns((pl.col("breadth_ema50_market") - pl.col("breadth_ema50_market").shift(5).over("market"))
                          .alias("breadth_ema50_market_chg5")).drop("_n").with_columns(
        pl.col("breadth_ema50_market").cast(pl.Float32), pl.col("breadth_ema50_market_chg5").cast(pl.Float32))


def _asof(left: pl.DataFrame, right: pl.DataFrame, by: str, cols: list[str], tolerance: timedelta) -> pl.DataFrame:
    """Latest right row with avail_date <= decision_date, same `by`, within tolerance of the bar."""
    r = right.select(by, "avail_date", *cols).sort("avail_date")
    return left.sort("decision_date").join_asof(r, left_on="decision_date", right_on="avail_date", by=by,
                                                strategy="backward", allow_exact_matches=True,
                                                tolerance=tolerance)


def asof_features(decisions: pl.DataFrame, tables: Mapping[str, pl.DataFrame], symbol_market: pl.DataFrame) -> pl.DataFrame:
    """Features for each distinct (symbol, decision_date) of `decisions`; `market` attached from the map."""
    base = decisions.select("symbol", "decision_date").unique().join(symbol_market, on="symbol", how="left")
    out = base
    for tf in TIMEFRAMES:
        cols = list(TF_FEATURES[tf])
        out = _asof(out, tables[tf], "symbol", cols, TOLERANCE[tf]).drop("avail_date")
    br = breadth_table(tables["1D"], symbol_market)
    out = _asof(out, br, "market", list(BREADTH_FEATURES), TOLERANCE["1D"]).drop("avail_date")
    return out.with_columns((pl.col("d_trend") + pl.col("w_trend")).alias("mtf_score_dw"))


def _finish(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("direction") == "long").then(1.0).otherwise(-1.0).cast(pl.Float32).alias("direction_sign"),
        pl.col("market").replace_strict({m: float(i) for i, m in enumerate(MARKETS)}, default=None,
                                        return_dtype=pl.Float32).alias("market_code"))


def training_frame(labels: pl.DataFrame, tables: Mapping[str, pl.DataFrame]) -> pl.DataFrame:
    """One row per labelled (symbol, decision_date, direction); label in {0,1}, weight 1. Complete cases only."""
    sm = labels.select("symbol", "market").unique(subset=["symbol"], keep="first")
    feats = asof_features(labels, tables, sm)
    j = labels.join(feats.drop("market"), on=["symbol", "decision_date"], how="inner").rename({"label_binary": "label"})
    j = _finish(j).with_columns(pl.col("label").cast(pl.Float32), pl.lit(1.0).cast(pl.Float32).alias("weight"))
    ok = pl.all_horizontal([pl.col(c).is_not_null() for c in FEATURES])
    return j.filter(ok).select("symbol", "market", "decision_date", "direction", "label", "weight", "label_end_date",
                               *FEATURES).sort(["decision_date", "symbol", "direction"])


def symbol_market_map(labels: pl.DataFrame) -> dict[str, str]:
    sm = labels.select("symbol", "market").unique(subset=["symbol"], keep="first")
    return dict(zip(sm["symbol"].to_list(), sm["market"].to_list()))


def prediction_rows(run_date: date, tables: Mapping[str, pl.DataFrame], symbol_market: Mapping[str, str]) -> pl.DataFrame:
    """(symbol x direction) rows for `run_date`; only symbols of the persisted map with complete features."""
    sm = pl.DataFrame({"symbol": list(symbol_market), "market": list(symbol_market.values())})
    dec = pl.DataFrame({"symbol": sm["symbol"], "decision_date": [run_date] * sm.height})
    feats = asof_features(dec, tables, sm)
    feats = feats.filter(pl.all_horizontal([pl.col(c).is_not_null() for c in (*TF_FEATURES["1D"], *TF_FEATURES["1W"],
                                                                               *TF_FEATURES["1M"], *BREADTH_FEATURES)]))
    grid = pl.DataFrame({"direction": list(DIRECTIONS)})
    out = feats.join(grid, how="cross").with_columns(pl.lit(run_date).alias("as_of_date"))
    return _finish(out)
