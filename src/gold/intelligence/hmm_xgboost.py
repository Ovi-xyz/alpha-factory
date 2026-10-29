"""
hmm_xgboost.py — ADR-053 (GMI-XGB-001) specialist #1: the regime (dual-HMM) domain's win-probability model.

TARGET (ADR-053 D1): P(a 2-7 day swing setup in this market and direction reaches its profit
barrier before its stop) given ONLY the dual-HMM engine's state: macro domain scores /
posteriors, the business-cycle activity and momentum, the market-behaviour state. Inputs from
other domains (indicators, cross-asset series) are not read — decision_xgboost (ADR-054) is
the only place the three specialists meet.

TWO JOBS (registered in job_registry as gold_hmm_xgboost_fit / gold_hmm_xgboost_predict)
    run_fit(run_date)  weekly. Walk-forward OOF on the honest as-of regime history
                       (data/gold/hmm/walk_forward/*) + the published label file, validation
                       report, final bagged model. Skips (logged + recorded) on a missing /
                       stale / foreign label set — it never trains on one.
    run(run_date)      daily. Scores the (market x direction) grid from the PRODUCTION regime
                       stores with the latest fitted model; no model -> writes nothing (soft
                       degrade, as every other Gold module).

OUTPUT  data/gold/intelligence/hmm_xgboost/predictions.parquet, one row per
        (as_of_date, market, direction): probability, confidence, quality, regime_tag, age_days,
        model_fit_date, lineage.   `quality` is this domain's Gatekeeper scope status (ADR-050).
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import polars as pl
from loguru import logger

from src.gold.intelligence import core
from src.gold.intelligence import features_hmm as fh
from src.gold.intelligence import labels as lab
from src.gold.intelligence.core import FrameSpec, IntelligenceConfig, ModelStore

MODEL_NAME = "hmm_xgboost"
GATE_SCOPE = "hmm"                       # Gatekeeper domain scope (ADR-053 D4)
SPEC = FrameSpec(key_cols=("decision_date", "market", "direction"))
FEATURES = list(fh.FEATURES)


def _store(cfg: IntelligenceConfig) -> ModelStore:
    return ModelStore(cfg.output_root, MODEL_NAME)


def _read(path) -> Optional[pl.DataFrame]:
    return pl.read_parquet(path) if path.exists() else None


# ═══ Weekly fit ══════════════════════════════════════════════════════════════

def run_fit(run_date: date, cfg: Optional[IntelligenceConfig] = None, labels_dir=None) -> dict:
    cfg = (cfg or core.load_config()).for_model(MODEL_NAME)
    store = _store(cfg)
    store.dir.mkdir(parents=True, exist_ok=True)
    try:
        labels = lab.load_training_labels(cfg, run_date, labels_dir)
    except lab.LabelInputError as e:
        status = {"status": "skipped", "reason": f"{type(e).__name__}: {e}", "fit_date": run_date.isoformat()}
        core.write_json_atomic(store.fit_status, status)
        logger.warning(f"[{MODEL_NAME}] fit skipped — {e}")
        return status
    macro, market = _read(fh.WF_MACRO_PATH), _read(fh.WF_MARKET_PATH)
    if macro is None or market is None:
        status = {"status": "skipped", "reason": "walk-forward regime history missing — run gold_hmm_walk_forward",
                  "fit_date": run_date.isoformat()}
        core.write_json_atomic(store.fit_status, status)
        logger.warning(f"[{MODEL_NAME}] fit skipped — {status['reason']}")
        return status
    features = fh.regime_feature_table(macro, market)
    frame = fh.training_frame(labels, features)
    meta = {"model": MODEL_NAME, "gate_scope": GATE_SCOPE, "label_file_newest_end": str(labels["label_end_date"].max()),
            "feature_source": "walk_forward", "markets": list(fh.MARKETS)}
    return core.run_fit_pipeline(store, frame, FEATURES, SPEC, cfg, run_date, meta)


# ═══ Daily predict ═══════════════════════════════════════════════════════════

def predict(run_date: date, cfg: IntelligenceConfig, macro: pl.DataFrame, market: pl.DataFrame,
            bundle: dict, quality: str) -> Optional[pl.DataFrame]:
    """Pure scoring of the (market x direction) grid; None when no usable feature row exists."""
    features = fh.regime_feature_table(macro, market)
    grid = fh.prediction_rows(features, run_date)
    if grid is None:
        return None
    scored = core.predict_bundle(bundle, grid, run_date)
    return grid.select("as_of_date", "market", "direction",
                       pl.col("macro_label").alias("macro_label"), pl.col("market_label").alias("market_label"),
                       (pl.col("macro_label") + pl.lit("/") + pl.col("market_label")).alias("regime_tag"),
                       (pl.lit(run_date) - pl.col("feature_date")).dt.total_days().cast(pl.Int32).alias("age_days"),
                       ).hstack(scored.select("probability", "confidence", "p_raw", "bag_std")).with_columns(
        pl.lit(quality).alias("quality"), pl.lit(MODEL_NAME).alias("model"),
        pl.lit(date.fromisoformat(bundle["fit_date"])).alias("model_fit_date"),
        pl.lit(f"{MODEL_NAME}|fit={bundle['fit_date']}|cfg={bundle['config_fingerprint']}|"
               f"feat={bundle['feature_spec_hash']}|label={bundle['label_spec']}").alias("lineage"),
        pl.lit(run_date).alias("computation_date"),
    )


def run(run_date: date, cfg: Optional[IntelligenceConfig] = None, quality: Optional[str] = None) -> int:
    cfg = (cfg or core.load_config()).for_model(MODEL_NAME)
    store = _store(cfg)
    bundle = core.load_bundle(store.model)
    if bundle is None:
        logger.warning(f"[{MODEL_NAME}] no fitted model — nothing written (run gold_hmm_xgboost_fit)")
        return 0
    macro, market = _read(fh.MACRO_PATH), _read(fh.MARKET_PATH)
    if macro is None or market is None:
        logger.warning(f"[{MODEL_NAME}] production regime stores missing — nothing written")
        return 0
    if quality is None:
        quality = gate_quality(run_date)
    out = predict(run_date, cfg, macro, market, bundle, quality)
    if out is None:
        logger.warning(f"[{MODEL_NAME}] {run_date}: no usable regime row within {fh.MAX_FEATURE_AGE_DAYS} days — nothing written")
        return 0
    n = core.append_predictions(store, out, run_date)
    logger.info(f"[{MODEL_NAME}] {run_date}: {out.height} rows (store {n}); quality={quality}; "
                f"p range {out['probability'].min():.3f}..{out['probability'].max():.3f}")
    return out.height


def gate_quality(run_date: date) -> str:
    """This domain's Gatekeeper scope status for `run_date`, computed from the sentinels of its own
    input chain (the Gatekeeper job itself runs later in the day); 'unknown' if it cannot be computed."""
    from src.utils.gatekeeper import domain_gate_status
    return domain_gate_status(GATE_SCOPE, run_date)
