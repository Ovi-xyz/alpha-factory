"""
src/gold/hmm/context_regime.py — ADR-051 (GMI-HMM2-001), module 5 of 5.

Layer 5 of hmm-gatekeeper-xgboost.md ("Context Module"): context regime = (macro regime, market
regime) — the ENVIRONMENT state and the BEHAVIOR state side by side. A PURE JOIN of the two regime
stores, rebuilt from scratch on every run: no model, no fit job, nothing learned here.

Input   data/gold/hmm/macro_regime.parquet    (hmm_macro_regime's store)
        data/gold/hmm/market_regime.parquet   (hmm_market_regime's store)
        Nothing else is read — in particular no raw_* store, no model file, and no store of another
        intelligence domain (ADR-051 Amendment 2 §A).
Output  data/gold/hmm/context_regime.parquet  (keyed on `date`; the path PITDataLoader.
                                               get_context_regime already reads)
            date                        Date    as-of date — the MACRO store's date (the spine)
            macro_label                 String  expansion | slowdown | recession | recovery
            macro_activity / macro_momentum   Float64   the macro_label rule's inputs
            macro_in_sample             Boolean
            market_date                 Date    date of the market row attached (null: none in tolerance)
            market_age_days             Int32   date - market_date: how STALE the attached market state is
            market_label                String  risk_off | mixed | risk_on
            market_probability / market_score Float64
            market_in_sample            Boolean
            market_n_features_observed  Int32
            context_label               String  "<macro_label>/<market_label>" when both are known
            context_transition          Boolean context_label differs from the previous row's known label
            context_transition_alert    String  "<prev label> -> <label>" on a transition, else null
            in_sample                   Boolean OR of the sides actually used (see below)
            computation_date / processing_version
            macro_config_fingerprint / market_config_fingerprint   the models' own digests, per row

JOIN RULE
    The spine is the macro store (a row for every weekday it has a posterior). The market state is
    attached AS-OF: the latest market row on or before the spine date, within MARKET_TOLERANCE_DAYS
    calendar days — so a US market holiday carries the previous session's state, with
    market_age_days saying so (a Monday after a holiday weekend is 4 days; a 5-day tolerance covers
    the longest routine closure). Beyond the tolerance the market columns are null rather than
    silently carrying a month-old state. Dates before the market store begins are macro-only rows.

HONEST HISTORY
    in_sample = macro_in_sample OR (market_in_sample when a market row is attached). A row is
    out-of-sample only if EVERY side it uses is. Anything training on this table (ADR-053/054) must
    filter on it.

TRANSITION SIGNAL (ADR-055 v2). The retired regime_store carried `regime_transition` /
    `transition_alert` as DATA (GD §5.2.1 v1.2, meant for the future Trading Engine); with that store
    gone the equivalent is produced here, where the regime is, and the screener (the designated
    future Trading Engine interface — no consumer exists yet) stays a pure join. A transition compares a row's context_label with the most recent
    EARLIER known label (a macro-only row before the market store begins neither starts nor ends
    one). The first known label of the store is not a transition. Pipeline supplies the fact; the
    Trading Engine decides what to do with it (GD §0.3).

THIN BY DESIGN (ADR-051 Consequences). Per-domain macro states and the three-way posteriors stay in
their own stores; this table carries the labels and scores a consumer needs to key on. It is
rebuilt every run, so adding a column later is free — there is no append-only schema to migrate.

MISSING INPUT. If either store is absent (no model has been fitted yet) nothing is written and the
existing context_regime, if any, is left untouched: a context regime without one of its two sides is
not a context regime.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from src.gold.hmm import hmm_macro_regime, hmm_market_regime
from src.utils.atomic_io import atomic_write_parquet

CONTEXT_REGIME_PATH = Path("data/gold/hmm/context_regime.parquet")
PROCESSING_VERSION = "1.1"   # 1.1: + context_transition / context_transition_alert (ADR-055 v2)
MARKET_TOLERANCE_DAYS = 5          # first-pass: covers the longest routine US market closure (holiday weekend)

MACRO_COLS = ("date", "macro_label", "macro_activity", "macro_momentum", "in_sample", "model_config_fingerprint")
MARKET_COLS = ("date", "market_label", "market_probability", "market_score", "in_sample",
               "n_features_observed", "model_config_fingerprint")

COLUMNS = ["date", "macro_label", "macro_activity", "macro_momentum", "macro_in_sample",
           "market_date", "market_age_days", "market_label", "market_probability", "market_score",
           "market_in_sample", "market_n_features_observed", "context_label",
           "context_transition", "context_transition_alert", "in_sample",
           "computation_date", "processing_version", "macro_config_fingerprint", "market_config_fingerprint"]


class ContextRegimeError(RuntimeError):
    """A regime store exists but does not have the columns this join needs."""


def _read(path: Path, cols: tuple[str, ...], run_date: date, what: str) -> pl.DataFrame:
    df = pl.read_parquet(path)
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ContextRegimeError(f"{path}: {what} store is missing columns {missing}")
    return df.select(cols).filter(pl.col("date") <= run_date).sort("date")


def build(macro: pl.DataFrame, market: pl.DataFrame, run_date: date,
          tolerance_days: int = MARKET_TOLERANCE_DAYS) -> pl.DataFrame:
    """The context frame from the two (already read, date-sorted, date <= run_date) stores."""
    m = macro.rename({"in_sample": "macro_in_sample", "model_config_fingerprint": "macro_config_fingerprint"})
    k = (market.with_columns(pl.col("date").alias("market_date"))
         .rename({"in_sample": "market_in_sample", "n_features_observed": "market_n_features_observed",
                  "model_config_fingerprint": "market_config_fingerprint"}))
    joined = m.join_asof(k, on="date", strategy="backward", tolerance=f"{tolerance_days}d")
    out = joined.with_columns(
        (pl.col("date") - pl.col("market_date")).dt.total_days().cast(pl.Int32).alias("market_age_days"),
        pl.when(pl.col("macro_label").is_not_null() & pl.col("market_label").is_not_null())
        .then(pl.concat_str([pl.col("macro_label"), pl.lit("/"), pl.col("market_label")]))
        .otherwise(None).alias("context_label"),
        (pl.col("macro_in_sample") | pl.col("market_in_sample").fill_null(False)).alias("in_sample"),
        pl.lit(run_date).alias("computation_date"),
        pl.lit(PROCESSING_VERSION).alias("processing_version"),
    )
    # Transition vs the most recent EARLIER known label (forward-fill, then shift one row).
    prev_known = pl.col("context_label").forward_fill().shift(1)
    is_transition = (pl.col("context_label").is_not_null() & prev_known.is_not_null()
                     & (pl.col("context_label") != prev_known)).fill_null(False)
    out = out.with_columns(
        is_transition.alias("context_transition"),
        pl.when(is_transition)
        .then(pl.concat_str([prev_known, pl.lit(" -> "), pl.col("context_label")]))
        .otherwise(None).alias("context_transition_alert"),
    )
    return out.select(COLUMNS)


def run(run_date: date) -> None:
    """Daily job ('gold_context_regime'): join the two regime stores, overwrite atomically."""
    macro_path, market_path = hmm_macro_regime.MACRO_REGIME_PATH, hmm_market_regime.MARKET_REGIME_PATH
    absent = [str(p) for p in (macro_path, market_path) if not Path(p).exists()]
    if absent:
        logger.warning(f"[hmm_context] {absent} missing — nothing written (run the weekly fit jobs once: "
                       f"gold_hmm_macro_fit --force, gold_hmm_market_fit --force)")
        return
    macro = _read(macro_path, MACRO_COLS, run_date, "macro")
    market = _read(market_path, MARKET_COLS, run_date, "market")
    if macro.height == 0:
        logger.warning(f"[hmm_context] {run_date}: macro store has no rows on or before the run date — nothing written")
        return
    out = build(macro, market, run_date)
    atomic_write_parquet(out, CONTEXT_REGIME_PATH)
    both = out["context_label"].is_not_null().sum()
    logger.info(f"[hmm_context] {run_date}: {out.height} rows ({both} with both sides) -> {CONTEXT_REGIME_PATH}")
