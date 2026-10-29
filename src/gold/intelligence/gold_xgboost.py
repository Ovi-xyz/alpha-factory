"""
gold_xgboost.py — ADR-053 (GMI-XGB-001) specialist #2: the technical (Gold signal chain) domain's win-probability model.

TARGET (ADR-053 D1): P(a 2-7 day swing setup on THIS symbol in THIS direction reaches its profit
barrier before its stop) given ONLY the technical chain's per-bar indicators (1D / 1W / 1M, last
COMPLETE bars — see features_gold.py), the market-level breadth of the same indicator universe,
the direction, and the symbol's market. No regime state and no cross-asset series is read; no
VIX value either (tech_signals' `vix_at_signal` is a run-day constant — excluded on purpose).

TWO JOBS (job_registry: gold_gold_xgboost_fit / gold_gold_xgboost_predict — the ADR-053 names)
    run_fit(run_date)  weekly. Walk-forward OOF + validation + final bagged model; skips (logged +
                       recorded) on a missing / stale / foreign label set.
    run(run_date)      daily. Scores every (symbol x direction) the model knows, from the latest
                       completed bars; no model -> writes nothing (soft degrade).

The symbol -> market map is persisted INSIDE the model bundle at fit time (from the labels), so
inference needs neither the label file nor the instrument registry; a symbol the model has never
seen is not scored until the next weekly fit.

OUTPUT  data/gold/intelligence/gold_xgboost/predictions.parquet, one row per
        (as_of_date, symbol, direction): probability, confidence, quality, regime_tag, age_days,
        model_fit_date, lineage.   `quality` is the technical domain's Gatekeeper scope status.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import polars as pl
from loguru import logger

from src.gold.intelligence import core
from src.gold.intelligence import features_gold as fg
from src.gold.intelligence import labels as lab
from src.gold.intelligence.core import FrameSpec, IntelligenceConfig, ModelStore

MODEL_NAME = "gold_xgboost"
GATE_SCOPE = "technical"
SPEC = FrameSpec(key_cols=("decision_date", "symbol", "direction"))
FEATURES = list(fg.FEATURES)


def _store(cfg: IntelligenceConfig) -> ModelStore:
    return ModelStore(cfg.output_root, MODEL_NAME)


# ═══ Weekly fit ══════════════════════════════════════════════════════════════

def run_fit(run_date: date, cfg: Optional[IntelligenceConfig] = None, labels_dir=None, signals_dir=None) -> dict:
    cfg = (cfg or core.load_config()).for_model(MODEL_NAME)
    store = _store(cfg)
    store.dir.mkdir(parents=True, exist_ok=True)

    def skip(reason: str) -> dict:
        status = {"status": "skipped", "reason": reason, "fit_date": run_date.isoformat()}
        core.write_json_atomic(store.fit_status, status)
        logger.warning(f"[{MODEL_NAME}] fit skipped — {reason}")
        return status

    try:
        labels = lab.load_training_labels(cfg, run_date, labels_dir)
    except lab.LabelInputError as e:
        return skip(f"{type(e).__name__}: {e}")
    try:
        tables = fg.load_tables(signals_dir or fg.SIGNALS_DIR)
    except core.IntelligenceError as e:
        return skip(f"technical store unavailable: {e}")
    frame = fg.training_frame(labels, tables)
    if frame.height == 0:
        return skip("no labelled row has complete technical features")
    meta = {"model": MODEL_NAME, "gate_scope": GATE_SCOPE, "label_file_newest_end": str(labels["label_end_date"].max()),
            "symbol_market": fg.symbol_market_map(labels), "timeframes": list(fg.TIMEFRAMES)}
    return core.run_fit_pipeline(store, frame, FEATURES, SPEC, cfg, run_date, meta)


# ═══ Daily predict ═══════════════════════════════════════════════════════════

def regime_tag(score: pl.Expr) -> pl.Expr:
    """Technical-domain tag from the daily+weekly trend sum: bull (>=2) / bear (<=-2) / mixed."""
    return pl.when(score >= 2).then(pl.lit("bull")).when(score <= -2).then(pl.lit("bear")).otherwise(pl.lit("mixed"))


def predict(run_date: date, tables, bundle: dict, quality: str) -> Optional[pl.DataFrame]:
    rows = fg.prediction_rows(run_date, tables, bundle["symbol_market"])
    if rows.height == 0:
        return None
    scored = core.predict_bundle(bundle, rows, run_date)
    last_bar = tables["1D"]["bar_date"].max()
    return rows.select("as_of_date", "symbol", "market", "direction", regime_tag(pl.col("mtf_score_dw")).alias("regime_tag")
                       ).hstack(scored.select("probability", "confidence", "p_raw", "bag_std")).with_columns(
        pl.lit(max(0, (run_date - last_bar).days)).cast(pl.Int32).alias("age_days"),
        pl.lit(quality).alias("quality"), pl.lit(MODEL_NAME).alias("model"),
        pl.lit(date.fromisoformat(bundle["fit_date"])).alias("model_fit_date"),
        pl.lit(f"{MODEL_NAME}|fit={bundle['fit_date']}|cfg={bundle['config_fingerprint']}|"
               f"feat={bundle['feature_spec_hash']}|label={bundle['label_spec']}").alias("lineage"),
        pl.lit(run_date).alias("computation_date"),
    ).sort(["symbol", "direction"])


def gate_quality(run_date: date) -> str:
    from src.utils.gatekeeper import domain_gate_status
    return domain_gate_status(GATE_SCOPE, run_date)


def run(run_date: date, cfg: Optional[IntelligenceConfig] = None, quality: Optional[str] = None, signals_dir=None) -> int:
    cfg = (cfg or core.load_config()).for_model(MODEL_NAME)
    store = _store(cfg)
    bundle = core.load_bundle(store.model)
    if bundle is None:
        logger.warning(f"[{MODEL_NAME}] no fitted model — nothing written (run gold_gold_xgboost_fit)")
        return 0
    try:
        tables = fg.load_tables(signals_dir or fg.SIGNALS_DIR)
    except core.IntelligenceError as e:
        logger.warning(f"[{MODEL_NAME}] technical store unavailable ({e}) — nothing written")
        return 0
    if quality is None:
        quality = gate_quality(run_date)
    out = predict(run_date, tables, bundle, quality)
    if out is None or out.height == 0:
        logger.warning(f"[{MODEL_NAME}] {run_date}: no symbol has complete features — nothing written")
        return 0
    n = core.append_predictions(store, out, run_date)
    logger.info(f"[{MODEL_NAME}] {run_date}: {out.height} rows (store {n}); quality={quality}; "
                f"p range {out['probability'].min():.3f}..{out['probability'].max():.3f}")
    return out.height
