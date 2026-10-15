"""
src/gold/hmm/hmm_macro_regime.py — ADR-051 (GMI-HMM2-001), macro branch, module 3 of 5.

Layer 3 of hmm-gatekeeper-xgboost.md ("Macro Module", role: ENVIRONMENT STATE): one
3-state diagonal-covariance GaussianHMM per macro domain (monetary, growth, inflation,
stress, liquidity) over raw_macro's PIT-gated panel, plus a business-cycle `macro_label`
(expansion / slowdown / recession / recovery) from a deterministic rule over the domain
scores. Settings: hmm.labels / hmm.fit / hmm.macro_label and the per-feature signed
weights in config/fred_series.yaml (validated by config.macro_model_config).

Input   data/gold/hmm/raw_macro.parquet            (this package's own store; Gold never
                                                    reads Bronze, and no other domain's store)
Models  data/gold/hmm/models/macro_<domain>.json   fitted parameters + scaler + lineage
Output  data/gold/hmm/macro_regime.parquet         (keyed on `date` — the platform's Gold
                                                    convention, which IS the as-of date)
            date                       Date    as-of date: filtered with data known through it
            per domain d:
              <d>_label                String  decoded state label (config vocabulary)
              <d>_probability          Float64 filtered posterior of that label
              <d>_score                Float64 posterior-weighted state position in [-1, +1]
              <d>_post_<label> x3      Float64 the full filtered posterior, one column per label
              <d>_short_history        Boolean training window < hmm.fit.short_history_min_obs
              <d>_model_fit_date       Date    fit date of the model that produced the row
            macro_activity             Float64 mean oriented domain score (see the rule)
            macro_momentum             Float64 activity change over momentum_window_bd weekdays
            macro_label                String  expansion | slowdown | recession | recovery
            in_sample                  Boolean see HONEST HISTORY
            computation_date / processing_version / model_config_fingerprint

TWO JOBS, ONE STORE
    run_fit(run_date)  weekly (Sunday): fit every domain, persist the models, then append
                       the rows the store does not yet have (the first run fills history).
    run(run_date)      daily: score with the PERSISTED models and append new rows. No hard
                       dependency on the fit job: a stale-but-present model still scores
                       (visible via <d>_model_fit_date); with NO model at all it writes
                       nothing (soft degrade, logged).

FIT (ADR-051 Amendment 2 §D)
    - robust scaling: median / MAD (x1.4826), clipped to +/- hmm.fit.scaler_clip, fitted on
      the training window and persisted with the model; a zero MAD falls back to the std,
      then to 1.0 (a rate held flat for years has MAD 0, not an infinite scale).
    - training window: the domain's complete-case rows (every feature present) up to run_date.
    - diagonal covariance (the yield levels are strongly collinear).
    - states ordered ASCENDING along the signed feature weights: composite(state) = sum of
      weight x state mean (scaled units); weight 0.0 = in the emission model, out of the
      ordering. State 0 is therefore always the first label of hmm.labels.<domain>.
    - one deterministic start (tertile bins of the oriented composite) competes with
      hmm.fit.n_random_seeds random EM starts on LIKELIHOOD. Random starts alone landed on a
      basin 1,531 log-likelihood units worse than the true one on a 10-feature world (ADR
      A2 §D) and a share guard cannot see that failure. Candidates are tried in descending
      likelihood and the first NON-DEGENERATE one wins (a state under min_state_share, a
      collapsed variance, or non-finite parameters is degenerate); if none is, the fit is
      REFUSED and the previous model kept.

INFERENCE: a numpy forward FILTER over the whole panel from the JSON parameters — no
smoothing (so no lookahead), no hmmlearn, state memory carried through the transition
matrix. A feature that is null on a row is MARGINALISED out of that row's emission (exact for
a diagonal covariance); a row with nothing observed is a pure prediction step; rows before a
domain's first observation have no posterior.

HONEST HISTORY (ADR-051 Amendment 2 §E)
    <d>_short_history  training window shorter than hmm.fit.short_history_min_obs (the stress
                       domain: HY spread has ~3 years of Silver). A short domain is fitted and
                       reported but EXCLUDED from macro_label.
    in_sample          True when the row's date is on or before the training end of ANY
                       domain model that produced it — i.e. a row is out-of-sample only if
                       EVERY contributing model was trained strictly before it. The store is
                       APPEND-ONLY: rows are only ever added after the last stored date, so
                       an out-of-sample row is never overwritten by a refit. Anything training
                       on this table (ADR-053/054) must filter on in_sample.

MACRO LABEL (ADR-051 Amendment 2 §F; first-pass calibration, review per ADR-055 E7)
    activity = mean of sign x score over hmm.macro_label.axes, short_history domains excluded,
    the `required` domain mandatory; momentum = activity - activity 63 weekdays earlier.
    activity > +t   -> expansion, or slowdown if momentum < -m
    activity < -t   -> recession, or recovery if momentum > +m
    |activity| <= t -> the previous label carries (as does an undefined activity)
    The rule is applied sequentially to NEW rows only, seeded with the last stored label.
    Monetary and inflation are carried as domain states, not cycle axes.

DOMAIN INDEPENDENCE (ADR-051 Amendment 2 §A): reads only raw_macro and its own models; no
CrossAssetEngine / technical-chain store, and none of them reads this one.
"""

from __future__ import annotations

import math
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import numpy as np
import polars as pl
from loguru import logger

from src.gold.hmm import raw_macro
from src.gold.hmm.config import MacroLabelRule, MacroModelConfig, macro_model_config

GOLD_HMM_PATH      = Path("data/gold/hmm")
MACRO_REGIME_PATH  = GOLD_HMM_PATH / "macro_regime.parquet"
MODELS_DIR         = GOLD_HMM_PATH / "models"
PROCESSING_VERSION = "1.0"
MODEL_SCHEMA       = 1

RAW_KEY = "as_of_date"
EXPANSION, SLOWDOWN, RECESSION, RECOVERY = "expansion", "slowdown", "recession", "recovery"


class MacroRegimeError(RuntimeError):
    """Registry/config/data defect that makes the macro regime unbuildable."""


# The shared engine lives in hmm_core (ADR-051: one implementation for both branches). Every moved
# name is re-exported here so this module's public surface is unchanged.
from src.gold.hmm.hmm_core import (  # noqa: E402,F401  (re-exports)
    Candidate, _MAD_TO_SIGMA, _EPS, _composite_init, _diag, _fit_candidate, _log_emission,
    _quiet_hmmlearn, apply_scaler, fit_domain, fit_model, fit_scaler, forward_filter,
    state_columns as domain_columns, empty_state_columns as _empty_domain_frame,
    atomic_write_text as _atomic_write_text, append_store, load_model_file, save_json,
)


# ═══ Model persistence ═══════════════════════════════════════════════════════

def model_path(domain: str) -> Path:
    return MODELS_DIR / f"macro_{domain}.json"


def save_model(domain: str, model: Mapping[str, Any]) -> None:
    save_json(model_path(domain), model)


def load_model(domain: str) -> Optional[dict]:
    return load_model_file(model_path(domain), MODEL_SCHEMA, domain, MacroRegimeError)


def _load_panel(run_date: date) -> pl.DataFrame:
    path = raw_macro.RAW_MACRO_PATH
    if not Path(path).exists():
        raise MacroRegimeError(f"{path} missing — run gold_raw_macro first")
    panel = pl.scan_parquet(path).filter(pl.col(RAW_KEY) <= run_date).sort(RAW_KEY).collect()
    if panel.height == 0:
        raise MacroRegimeError(f"{path}: no rows on or before {run_date}")
    return panel


def _feats(cfg: MacroModelConfig, domain: str) -> list[str]:
    return [f.feature for f in cfg.macro.features_in(domain)]


def _require_columns(panel: pl.DataFrame, feats: Sequence[str], domain: str) -> None:
    missing = [f for f in feats if f not in panel.columns]
    if missing:
        raise MacroRegimeError(f"raw_macro is missing {domain} features {missing} — rebuild raw_macro (gold_raw_macro)")


# ═══ Weekly fit ══════════════════════════════════════════════════════════════

def fit_all(panel: pl.DataFrame, run_date: date, cfg: MacroModelConfig) -> dict[str, str]:
    """Fit every domain on panel rows <= run_date; persist winners. Returns a status per domain.
    A refused / skipped domain keeps whatever model file it already had."""
    status: dict[str, str] = {}
    for domain in cfg.domains:
        feats = _feats(cfg, domain)
        _require_columns(panel, feats, domain)
        w = np.array([cfg.weights[domain][f] for f in feats], dtype=float)
        record, status[domain] = fit_model(panel, RAW_KEY, domain, feats, w, cfg.labels[domain], cfg.fit,
                                           cfg.fingerprint(), run_date, PROCESSING_VERSION, MODEL_SCHEMA)
        if record is None:
            logger.warning(f"[hmm_macro] {domain}: {status[domain]}")
            continue
        save_model(domain, record)
        logger.info(f"[hmm_macro] {domain}: {status[domain]}")
    return status


# ═══ Scoring ═════════════════════════════════════════════════════════════════

def build_rows(panel: pl.DataFrame, models: Mapping[str, Optional[Mapping[str, Any]]],
               cfg: MacroModelConfig) -> pl.DataFrame:
    """Per-domain columns + in_sample for every panel row that has at least one posterior.
    `date` is the as-of date. macro_* columns and lineage are added by the caller."""
    data: dict[str, pl.Series] = {"date": panel[RAW_KEY]}
    in_sample_any = np.zeros(panel.height, dtype=bool)
    dates = panel[RAW_KEY].to_list()
    for domain in cfg.domains:
        m = models.get(domain)
        cols = domain_columns(panel, domain, cfg.labels[domain], m)
        data.update(cols)
        if m is not None:
            train_end = date.fromisoformat(m["train_end"])
            has = cols[f"{domain}_label"].is_not_null().to_numpy()
            in_sample_any |= has & np.array([d <= train_end for d in dates], dtype=bool)
    out = pl.DataFrame(data)
    any_posterior = pl.any_horizontal([pl.col(f"{d}_label").is_not_null() for d in cfg.domains])
    return out.with_columns(pl.Series("in_sample", in_sample_any, dtype=pl.Boolean)).filter(any_posterior)


# ═══ Macro label ═════════════════════════════════════════════════════════════

def activity_expr(rule: MacroLabelRule) -> pl.Expr:
    """Mean oriented score over the axes; short_history (or unscored) domains are excluded; the
    `required` domain must itself contribute, else activity is null."""
    def contributes(d: str) -> pl.Expr:
        return pl.col(f"{d}_score").is_not_null() & (pl.col(f"{d}_short_history") == False)   # noqa: E712

    terms = [pl.when(contributes(d)).then(sign * pl.col(f"{d}_score")) for d, sign in rule.axes.items()]
    n = pl.sum_horizontal([t.is_not_null().cast(pl.Int32) for t in terms])
    return pl.when(contributes(rule.required) & (n > 0)).then(pl.sum_horizontal(terms) / n)


def label_sequence(activity: Sequence[Optional[float]], momentum: Sequence[Optional[float]],
                   rule: MacroLabelRule, seed: Optional[str]) -> list[Optional[str]]:
    """Apply the §F rule row by row. Strict inequalities; inside the band (or with an undefined
    activity) the previous label carries."""
    out: list[Optional[str]] = []
    prev = seed
    for a, m in zip(activity, momentum):
        if a is None or (isinstance(a, float) and math.isnan(a)):
            lab = prev
        elif a > rule.activity_threshold:
            lab = SLOWDOWN if (m is not None and m < -rule.momentum_threshold) else EXPANSION
        elif a < -rule.activity_threshold:
            lab = RECOVERY if (m is not None and m > rule.momentum_threshold) else RECESSION
        else:
            lab = prev
        out.append(lab)
        prev = lab
    return out


def add_macro_label(rows: pl.DataFrame, history_activity: pl.DataFrame, seed: Optional[str],
                    rule: MacroLabelRule) -> pl.DataFrame:
    """macro_activity / macro_momentum / macro_label for NEW rows. Momentum looks back
    `momentum_window_bd` weekdays into history_activity (date, macro_activity) plus the new rows."""
    rows = rows.sort("date").with_columns(activity_expr(rule).cast(pl.Float64).alias("macro_activity"))
    pool = pl.concat([
        history_activity.select("date", pl.col("macro_activity").alias("_a_prev")),
        rows.select("date", pl.col("macro_activity").alias("_a_prev")),
    ]).unique(subset=["date"], keep="last")
    rows = (
        rows.with_columns(pl.col("date").dt.add_business_days(-rule.momentum_window_bd).alias("_lk"))
        .join(pool.rename({"date": "_lk"}), on="_lk", how="left")
        .with_columns((pl.col("macro_activity") - pl.col("_a_prev")).alias("macro_momentum"))
        .drop("_lk", "_a_prev")
    )
    labels = label_sequence(rows["macro_activity"].to_list(), rows["macro_momentum"].to_list(), rule, seed)
    return rows.with_columns(pl.Series("macro_label", labels, dtype=pl.String))


# ═══ Append-only store ═══════════════════════════════════════════════════════

def _fingerprint_of(models: Mapping[str, Optional[Mapping[str, Any]]]) -> str:
    fps = {m["config_fingerprint"] for m in models.values() if m is not None}
    return fps.pop() if len(fps) == 1 else "mixed"


def append_new_rows(panel: pl.DataFrame, cfg: MacroModelConfig, run_date: date) -> int:
    """Score the panel with the persisted models and APPEND the rows after the last stored date
    (all of them when the store is empty). Never rewrites a stored row. Returns rows added."""
    models = {d: load_model(d) for d in cfg.domains}
    if all(m is None for m in models.values()):
        logger.warning("[hmm_macro] no fitted model for any domain — nothing written "
                       "(run gold_hmm_macro_fit --force once to create them)")
        return 0
    for d, m in models.items():
        if m is None:
            logger.warning(f"[hmm_macro] {d}: no model — its columns stay null")
            continue
        _require_columns(panel, m["features"], d)
        if m["config_fingerprint"] != cfg.fingerprint():
            logger.warning(f"[hmm_macro] {d}: model fitted under config {m['config_fingerprint']} but the current "
                           f"config is {cfg.fingerprint()} — the weekly fit will realign it")

    rows = build_rows(panel, models, cfg)
    existing = pl.read_parquet(MACRO_REGIME_PATH) if MACRO_REGIME_PATH.exists() else None
    if existing is not None:
        rows = rows.filter(pl.col("date") > existing["date"].max())
    if rows.height == 0:
        logger.info(f"[hmm_macro] {run_date}: store already current — no new rows")
        return 0

    if existing is not None:
        needed = {"date", "macro_activity", "macro_label"}      # read below for the momentum lookback and the label seed
        if not needed <= set(existing.columns):
            raise MacroRegimeError(
                f"{MACRO_REGIME_PATH} lacks columns {sorted(needed - set(existing.columns))} — the store is "
                f"append-only and cannot be migrated in place; archive it and let the fit job rebuild it")
    history = existing.select("date", "macro_activity") if existing is not None else \
        pl.DataFrame(schema={"date": pl.Date, "macro_activity": pl.Float64})
    seed = existing.sort("date")["macro_label"][-1] if existing is not None else None
    rows = add_macro_label(rows, history, seed, cfg.label_rule).with_columns(
        pl.lit(run_date).alias("computation_date"),
        pl.lit(PROCESSING_VERSION).alias("processing_version"),
        pl.lit(_fingerprint_of(models)).alias("model_config_fingerprint"),
    )
    rows = rows.select(_column_order(cfg))
    return append_store(existing, rows, MACRO_REGIME_PATH, MacroRegimeError)


def _column_order(cfg: MacroModelConfig) -> list[str]:
    cols = ["date"]
    for d in cfg.domains:
        cols += [f"{d}_label", f"{d}_probability", f"{d}_score"]
        cols += [f"{d}_post_{lab}" for lab in cfg.labels[d]]
        cols += [f"{d}_short_history", f"{d}_model_fit_date"]
    return cols + ["macro_activity", "macro_momentum", "macro_label", "in_sample",
                   "computation_date", "processing_version", "model_config_fingerprint"]


# ═══ Job entry points ════════════════════════════════════════════════════════

def run_fit(run_date: date) -> None:
    """Weekly job ('gold_hmm_macro_fit'): fit, persist, then append the rows the store lacks."""
    cfg = macro_model_config()
    panel = _load_panel(run_date)
    status = fit_all(panel, run_date, cfg)
    added = append_new_rows(panel, cfg, run_date)
    logger.info(f"[hmm_macro] fit {run_date}: "
                f"{sum(s.startswith('fitted') for s in status.values())}/{len(status)} domains fitted, "
                f"{added} rows appended -> {MACRO_REGIME_PATH}")


def run(run_date: date) -> None:
    """Daily job ('gold_hmm_macro_regime'): score with the persisted models, append new rows."""
    cfg = macro_model_config()
    panel = _load_panel(run_date)
    added = append_new_rows(panel, cfg, run_date)
    logger.info(f"[hmm_macro] {run_date}: {added} rows appended -> {MACRO_REGIME_PATH}")
