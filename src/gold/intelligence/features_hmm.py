"""
features_hmm.py — ADR-053 D2: the as-of feature builder of `hmm_xgboost` (fit AND predict).

ONE BUILDER FOR BOTH PATHS. `regime_feature_table()` takes two frames with the schema of the
dual-HMM stores (macro: monetary/growth/inflation/liquidity/macro_*; market: market_*) and
returns one row per macro date. The fit path hands it the walk-forward stores
(`data/gold/hmm/walk_forward/*` — rows scored by a model that had not seen them); the predict
path hands it the production stores. Same function, same columns, same strict as-of rule — no
train/serve skew by construction.

AS-OF RULE (matches the label convention, ADR-052 Amendment A1)
    A decision on date T sees feature rows dated STRICTLY BEFORE T: the labeler's anchor bar is
    the last bar with trade date < T, and a market row dated D already contains bar D.

INDEPENDENCE (ADR-051 A2 §A / ADR-053)
    Reads the HMM domain's own stores as Parquet only. It does not import src.gold.hmm (that
    package is imported nowhere outside its own jobs), the technical chain, or the
    CrossAssetEngine, and none of them is read here.

FEATURE CHOICES (each a stated first-pass decision, KNOWN_RISKS RISK-40)
    - stress_* is EXCLUDED while its history is short (~3 years of Silver): a feature that is
      missing for most of the training window lets a tree learn "calendar time", not regime.
      This mirrors the macro_label rule, which also drops short-history domains.
    - Complete cases only: a row with any missing feature is dropped, never imputed.
    - `market_post_mixed` is dropped (the three posteriors sum to 1).
    - labels are aggregated to (decision_date, market, direction): the features are identical
      for every symbol of a market on a date, so one weighted row is exactly the replicated
      rows (core.py, "WEIGHTS / AGGREGATED ROWS").
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import polars as pl

from src.gold.intelligence.core import IntelligenceError

HMM_DIR = Path("data/gold/hmm")
WF_MACRO_PATH = HMM_DIR / "walk_forward" / "macro_regime_wf.parquet"
WF_MARKET_PATH = HMM_DIR / "walk_forward" / "market_regime_wf.parquet"
MACRO_PATH = HMM_DIR / "macro_regime.parquet"
MARKET_PATH = HMM_DIR / "market_regime.parquet"

MARKET_TOLERANCE_DAYS = 5      # as context_regime: the longest routine market closure (holiday weekend)
MAX_FEATURE_AGE_DAYS = 7       # predict: a feature row older than this is not scored

MARKETS = ("commodity", "forex", "idx", "us_stocks")      # Layer 1 markets (src.utils.silver_scope.layer1_markets)
DIRECTIONS = ("long", "short")

MACRO_DOMAINS = ("monetary", "growth", "inflation", "liquidity")   # stress excluded (short history)
MACRO_FEATURES = tuple(f"{d}_{k}" for d in MACRO_DOMAINS for k in ("score", "probability")) + (
    "macro_activity", "macro_momentum")
MARKET_FEATURES = ("market_score", "market_probability", "market_post_risk_off", "market_post_risk_on",
                   "market_score_d5")
STATE_FEATURES = MACRO_FEATURES + MARKET_FEATURES
FEATURES = STATE_FEATURES + ("direction_sign", "market_code")

TAG_COLUMNS = ("macro_label", "market_label")


def _need_cols(df: pl.DataFrame, cols, what: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise IntelligenceError(f"{what} lacks columns {missing}")


def regime_feature_table(macro: pl.DataFrame, market: pl.DataFrame) -> pl.DataFrame:
    """One row per macro date: macro features + the latest market row at or before it (within
    MARKET_TOLERANCE_DAYS), the 5-row change of market_score, and the two labels as tags.
    Returns columns: feature_date, market_date, market_age_days, STATE_FEATURES, macro_label, market_label."""
    _need_cols(macro, ("date", *MACRO_FEATURES, "macro_label"), "macro regime frame")
    _need_cols(market, ("date", "market_score", "market_probability", "market_post_risk_off",
                        "market_post_risk_on", "market_label"), "market regime frame")
    mk = (market.sort("date")
          .with_columns((pl.col("market_score") - pl.col("market_score").shift(5)).alias("market_score_d5"))
          .select(pl.col("date").alias("market_date"), "market_score", "market_probability", "market_post_risk_off",
                  "market_post_risk_on", "market_score_d5", "market_label"))
    mc = macro.sort("date").select(pl.col("date").alias("feature_date"), *MACRO_FEATURES, "macro_label")
    out = mc.join_asof(mk, left_on="feature_date", right_on="market_date", strategy="backward",
                       tolerance=timedelta(days=MARKET_TOLERANCE_DAYS))
    return out.with_columns((pl.col("feature_date") - pl.col("market_date")).dt.total_days().cast(pl.Int32)
                            .alias("market_age_days"))


def attach_asof(decisions: pl.DataFrame, features: pl.DataFrame) -> pl.DataFrame:
    """Join each decision_date to the latest feature row dated STRICTLY BEFORE it."""
    return (decisions.sort("decision_date")
            .join_asof(features.sort("feature_date"), left_on="decision_date", right_on="feature_date",
                       strategy="backward", allow_exact_matches=False))


def training_frame(labels: pl.DataFrame, features: pl.DataFrame) -> pl.DataFrame:
    """(decision_date, market, direction) rows: label = mean outcome, weight = symbol count,
    label_end_date = the latest window end in the group (conservative for the purge), plus the
    as-of features. Complete cases only."""
    agg = (labels.group_by(["decision_date", "market", "direction"])
           .agg(pl.col("label_binary").mean().alias("label"), pl.len().cast(pl.Float64).alias("weight"),
                pl.col("label_end_date").max().alias("label_end_date")))
    joined = attach_asof(agg, features)
    joined = joined.with_columns(
        pl.when(pl.col("direction") == "long").then(1.0).otherwise(-1.0).alias("direction_sign"),
        pl.col("market").replace_strict({m: float(i) for i, m in enumerate(MARKETS)}, default=None,
                                        return_dtype=pl.Float64).alias("market_code"),
    )
    return (joined.filter(pl.all_horizontal([pl.col(c).is_not_null() & pl.col(c).is_not_nan() for c in FEATURES]))
            .select("decision_date", "market", "direction", "label", "weight", "label_end_date",
                    "feature_date", *FEATURES).sort("decision_date"))


def prediction_rows(features: pl.DataFrame, run_date: date) -> Optional[pl.DataFrame]:
    """The (market x direction) grid for `run_date` from the latest feature row strictly before it,
    or None when there is none, it is older than MAX_FEATURE_AGE_DAYS, or any feature is missing."""
    row = features.filter(pl.col("feature_date") < run_date).sort("feature_date").tail(1)
    if row.height == 0:
        return None
    if (run_date - row["feature_date"][0]).days > MAX_FEATURE_AGE_DAYS:
        return None
    if any(row[c][0] is None or row[c][0] != row[c][0] for c in STATE_FEATURES):
        return None
    grid = pl.DataFrame({"market": [m for m in MARKETS for _ in DIRECTIONS],
                         "direction": [d for _ in MARKETS for d in DIRECTIONS]})
    return (grid.join(row, how="cross")
            .with_columns(pl.when(pl.col("direction") == "long").then(1.0).otherwise(-1.0).alias("direction_sign"),
                          pl.col("market").replace_strict({m: float(i) for i, m in enumerate(MARKETS)},
                                                          return_dtype=pl.Float64).alias("market_code"),
                          pl.lit(run_date).alias("as_of_date")))
