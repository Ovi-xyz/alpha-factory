"""
src/gold/hmm/hmm_market_regime.py — ADR-051 (GMI-HMM2-001), market branch, module 4 of 5.

Layer 4 of hmm-gatekeeper-xgboost.md ("Market Module", role: BEHAVIOR STATE): one 3-state
diagonal-covariance GaussianHMM over raw_market's panel -> `market_label`
(risk_off / mixed / risk_on) with its probability, score and full posterior. Settings:
hmm_market.labels / hmm_market.fit and the per-feature signed weights in
config/fred_series.yaml (validated by config.market_model_config).

Input   data/gold/hmm/raw_market.parquet           (this package's own store; Silver-only
                                                    upstream — no Gold store of another domain)
Model   data/gold/hmm/models/market.json           fitted parameters + scaler + lineage
Output  data/gold/hmm/market_regime.parquet        (keyed on `date`, the platform's Gold
                                                    convention, which IS the as-of date)
            date                       Date    as-of date (a US trading day): filtered with
                                               data known through it
            market_label               String  decoded state label (config vocabulary)
            market_probability         Float64 filtered posterior of that label
            market_score               Float64 posterior-weighted state position in [-1, +1]
                                               (-1 = risk_off ... +1 = risk_on)
            market_post_<label> x3     Float64 the full filtered posterior
            market_short_history       Boolean training window < hmm_market.fit.short_history_min_obs
            market_model_fit_date      Date    fit date of the model that produced the row
            n_features_observed        Int32   how many model features were non-null on the row
            in_sample                  Boolean see HONEST HISTORY
            computation_date / processing_version / model_config_fingerprint

The HMM mechanics (robust scaling, composite-start-vs-random-starts fit, forward filter with null
marginalisation, column building, atomic persistence) are the SHARED engine in hmm_core — the same
code the macro branch runs. What is specific to this module: the feature routing and weights, the
vocabulary, the store layout and the honest-history columns.

TWO JOBS, ONE STORE
    run_fit(run_date)  weekly (Sunday): fit, persist the model, then append the rows the store
                       does not yet have (the first run fills history).
    run(run_date)      daily: score with the PERSISTED model and append new rows. No hard
                       dependency on the fit job: a stale-but-present model still scores
                       (visible via market_model_fit_date); with NO model at all it writes
                       nothing (soft degrade, logged).

HONEST HISTORY (ADR-051 Amendment 2 §E)
    market_short_history  training window shorter than hmm_market.fit.short_history_min_obs.
    in_sample             True when the row's date is on or before the model's training end. The
                          store is APPEND-ONLY: rows are only ever added after the last stored
                          date, so an out-of-sample row is never overwritten by a refit. Anything
                          training on this table (ADR-053/054) must filter on in_sample.
    n_features_observed   a row whose breadth guard nulled the US-stock features (a thin or
                          partial day) is still scored — the filter marginalises missing features
                          exactly — but on fewer features; this column makes that visible. Because
                          the store is append-only, such a row is NOT recomputed when the late
                          bars arrive (KNOWN_RISKS: partial latest day).

DOMAIN INDEPENDENCE (ADR-051 Amendment 2 §A): reads only raw_market and its own model; no
CrossAssetEngine / technical-chain store (in particular not gold_global_regime), and none of
them reads this one.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import numpy as np
import polars as pl
from loguru import logger

from src.gold.hmm import raw_market
from src.gold.hmm.config import MarketModelConfig, market_model_config
from src.gold.hmm.hmm_core import append_store, fit_model, load_model_file, save_json, state_columns

GOLD_HMM_PATH      = Path("data/gold/hmm")
MARKET_REGIME_PATH = GOLD_HMM_PATH / "market_regime.parquet"
MODELS_DIR         = GOLD_HMM_PATH / "models"
PROCESSING_VERSION = "1.0"
MODEL_SCHEMA       = 1
MODEL_NAME         = "market"

RAW_KEY = "as_of_date"
PREFIX = "market"


class MarketRegimeError(RuntimeError):
    """Registry/config/data defect that makes the market regime unbuildable."""


# ═══ Model persistence ═══════════════════════════════════════════════════════

def model_path() -> Path:
    return MODELS_DIR / f"{MODEL_NAME}.json"


def save_model(model: Mapping[str, Any]) -> None:
    save_json(model_path(), model)


def load_model() -> Optional[dict]:
    return load_model_file(model_path(), MODEL_SCHEMA, MODEL_NAME, MarketRegimeError)


def _load_panel(run_date: date) -> pl.DataFrame:
    path = raw_market.RAW_MARKET_PATH
    if not Path(path).exists():
        raise MarketRegimeError(f"{path} missing — run gold_raw_market first")
    panel = pl.scan_parquet(path).filter(pl.col(RAW_KEY) <= run_date).sort(RAW_KEY).collect()
    if panel.height == 0:
        raise MarketRegimeError(f"{path}: no rows on or before {run_date}")
    return panel


def _require_columns(panel: pl.DataFrame, feats: Sequence[str]) -> None:
    missing = [f for f in feats if f not in panel.columns]
    if missing:
        raise MarketRegimeError(f"raw_market is missing market features {missing} — rebuild raw_market (gold_raw_market)")


# ═══ Weekly fit ══════════════════════════════════════════════════════════════

def fit(panel: pl.DataFrame, run_date: date, cfg: MarketModelConfig) -> str:
    """Fit the model on panel rows <= run_date and persist it. Returns the status line. A refused or
    skipped fit keeps whatever model file already exists."""
    feats = cfg.feature_names()
    _require_columns(panel, feats)
    w = np.array([cfg.weights[f] for f in feats], dtype=float)
    record, status = fit_model(panel, RAW_KEY, MODEL_NAME, feats, w, cfg.labels, cfg.fit,
                               cfg.fingerprint(), run_date, PROCESSING_VERSION, MODEL_SCHEMA)
    if record is None:
        logger.warning(f"[hmm_market] {status}")
        return status
    save_model(record)
    logger.info(f"[hmm_market] {status}")
    return status


# ═══ Scoring ═════════════════════════════════════════════════════════════════

def build_rows(panel: pl.DataFrame, model: Mapping[str, Any], cfg: MarketModelConfig) -> pl.DataFrame:
    """The market columns + n_features_observed + in_sample for every panel row that has a posterior.
    `date` is the as-of date. Lineage columns are added by the caller."""
    cols = state_columns(panel, PREFIX, cfg.labels, model)
    has = cols[f"{PREFIX}_label"].is_not_null().to_numpy()
    train_end = date.fromisoformat(model["train_end"])
    dates = panel[RAW_KEY].to_list()
    in_sample = has & np.array([d <= train_end for d in dates], dtype=bool)
    observed = panel.select(pl.sum_horizontal([pl.col(f).is_not_null().cast(pl.Int32) for f in model["features"]])
                            .alias("n_features_observed"))["n_features_observed"].cast(pl.Int32)
    out = pl.DataFrame({"date": panel[RAW_KEY], **cols})
    out = out.with_columns(observed, pl.Series("in_sample", in_sample, dtype=pl.Boolean))
    return out.filter(pl.col(f"{PREFIX}_label").is_not_null())


def _column_order(cfg: MarketModelConfig) -> list[str]:
    return (["date", f"{PREFIX}_label", f"{PREFIX}_probability", f"{PREFIX}_score"]
            + [f"{PREFIX}_post_{lab}" for lab in cfg.labels]
            + [f"{PREFIX}_short_history", f"{PREFIX}_model_fit_date", "n_features_observed", "in_sample",
               "computation_date", "processing_version", "model_config_fingerprint"])


def append_new_rows(panel: pl.DataFrame, cfg: MarketModelConfig, run_date: date) -> int:
    """Score the panel with the persisted model and APPEND the rows after the last stored date (all
    of them when the store is empty). Never rewrites a stored row. Returns rows added."""
    model = load_model()
    if model is None:
        logger.warning("[hmm_market] no fitted model — nothing written "
                       "(run gold_hmm_market_fit --force once to create it)")
        return 0
    _require_columns(panel, model["features"])
    if model["config_fingerprint"] != cfg.fingerprint():
        logger.warning(f"[hmm_market] model fitted under config {model['config_fingerprint']} but the current "
                       f"config is {cfg.fingerprint()} — the weekly fit will realign it")

    rows = build_rows(panel, model, cfg)
    existing = pl.read_parquet(MARKET_REGIME_PATH) if MARKET_REGIME_PATH.exists() else None
    if existing is not None:
        rows = rows.filter(pl.col("date") > existing["date"].max())
    if rows.height == 0:
        logger.info(f"[hmm_market] {run_date}: store already current — no new rows")
        return 0
    rows = rows.with_columns(
        pl.lit(run_date).alias("computation_date"),
        pl.lit(PROCESSING_VERSION).alias("processing_version"),
        pl.lit(model["config_fingerprint"]).alias("model_config_fingerprint"),
    ).select(_column_order(cfg))
    return append_store(existing, rows, MARKET_REGIME_PATH, MarketRegimeError)


# ═══ Job entry points ════════════════════════════════════════════════════════

def run_fit(run_date: date) -> None:
    """Weekly job ('gold_hmm_market_fit'): fit, persist, then append the rows the store lacks."""
    cfg = market_model_config()
    panel = _load_panel(run_date)
    status = fit(panel, run_date, cfg)
    added = append_new_rows(panel, cfg, run_date)
    logger.info(f"[hmm_market] fit {run_date}: {'fitted' if status.startswith('fitted') else 'not fitted'}, "
                f"{added} rows appended -> {MARKET_REGIME_PATH}")


def run(run_date: date) -> None:
    """Daily job ('gold_hmm_market_regime'): score with the persisted model, append new rows."""
    cfg = market_model_config()
    panel = _load_panel(run_date)
    added = append_new_rows(panel, cfg, run_date)
    logger.info(f"[hmm_market] {run_date}: {added} rows appended -> {MARKET_REGIME_PATH}")
