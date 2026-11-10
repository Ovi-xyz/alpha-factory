"""
scripts/studies/technical_block_ablation.py — ADR-053 Amendment 2: same-window feature-block ablation.
OFFLINE UTILITY.

Question it answers: does an optional feature block of the technical specialist (today: `candle`; later an
intraday block, once one exists) add out-of-fold value over the base set — on the SAME rows, the SAME folds and the
SAME hyper-parameters?  Not a pipeline job and not imported from `src/` (pinned by
tests/integration/test_block_ablation_isolation_adr053.py).  Unlike scripts/studies/hmm_replay.py it deliberately
REUSES the production engine (src.gold.intelligence.core walk-forward / purge / embargo / causal calibration, the
as-of builder in features_technical, the label reader in labels): an ablation of a re-implementation would say
nothing about the real model.

Design (metrics fixed in advance, ADR-053 Amendment 2; thresholds are [U] until calibrated on real OOF):
  * both arms are cut to the intersection of their complete-case rows and to `--window-start`, so a difference can
    only come from the features;
  * both arms run `core.walk_forward_oof` with the technical model's own config (+ the CLI walk-forward overrides);
  * report: weighted AUC, logloss, Brier skill, base rate per arm; paired deltas (B - A); a MOVING-BLOCK BOOTSTRAP
    over decision dates (`--block-days` consecutive dates per block) gives a 95% interval for each delta; per-year
    delta AUC as a point estimate only;
  * verdict `contributes` iff the delta-AUC interval is strictly above 0 AND the delta-logloss interval is strictly
    below 0; `worse` for the mirror image; otherwise `no_evidence`.  The verdict gates NOTHING in the pipeline.

Scope limits (restated so the output is not over-read): the effective sample is the number of DATES, not rows;
overlapping label windows make date-block intervals optimistic; AUC is rank quality, not P&L; one window, one
config; survivorship (RISK-28) applies to the label set.

--span-check prints the walk-forward blocks a given label span yields under the current config WITHOUT fitting
anything — e.g. a ~2-year intraday history under the shipped initial_train_months (36) yields zero blocks.

CLI (from the repo root)
    python -m scripts.studies.technical_block_ablation --blocks-a base --blocks-b base,candle \
        [--window-start 2024-09-01] [--initial-train-months 12] [--n-boot 300] [--out data/studies/ablation.json]
    python -m scripts.studies.technical_block_ablation --span-check --first-label-date 2024-10-01 \
        --last-decision-date 2026-10-16 --initial-train-months 12
"""
from __future__ import annotations

import argparse
import copy
import dataclasses
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import polars as pl
import yaml

from src.gold.intelligence import core
from src.gold.intelligence import features_technical as fg
from src.gold.intelligence import labels as lab
from src.gold.intelligence import technical_xgboost as gx

CI_LEVEL = 0.95
KEY = list(gx.SPEC.key_cols)            # (decision_date, symbol, direction)


# ═══ configuration ═══════════════════════════════════════════════════════════

def study_config(initial_train_months: Optional[int] = None, test_block_months: Optional[int] = None,
                 min_train_rows: Optional[int] = None, config_path: Path | str | None = None) -> core.IntelligenceConfig:
    """The technical model's production config, with optional walk-forward overrides. Overrides are validated by
    the engine's own validator (a throwaway copy is validated; the override is then applied to the model config)."""
    base = core.load_config(config_path)
    cfg = base.for_model(gx.MODEL_NAME)
    over = {k: v for k, v in (("initial_train_months", initial_train_months), ("test_block_months", test_block_months),
                              ("min_train_rows", min_train_rows)) if v is not None}
    if not over:
        return cfg
    with open(config_path or core.CONFIG_PATH) as fh:
        raw = copy.deepcopy(yaml.safe_load(fh))
    raw["walk_forward"].update(over)
    core.validate_config(raw)                                   # raises IntelligenceError on a nonsensical override
    return dataclasses.replace(cfg, **over)


def span_check(first_label: date, last_decision: date, cfg: core.IntelligenceConfig) -> dict:
    """The out-of-fold blocks a label span yields under `cfg` (no data, no fit)."""
    starts = core.block_starts(first_label, last_decision, cfg)
    out = {"first_label_date": str(first_label), "last_decision_date": str(last_decision),
           "initial_train_months": cfg.initial_train_months, "test_block_months": cfg.test_block_months,
           "n_blocks": len(starts), "block_starts": [str(s) for s in starts]}
    if starts:
        out["oof_months_available"] = round((last_decision - starts[0]).days / 30.4375, 1)
    else:
        out["oof_months_available"] = 0.0
        out["note"] = "zero out-of-fold blocks: the history is shorter than initial_train_months + one block"
    return out


# ═══ arms ════════════════════════════════════════════════════════════════════

def build_arms(labels: pl.DataFrame, tables, blocks_a: Sequence[str], blocks_b: Sequence[str],
               window_start: Optional[date] = None) -> tuple[pl.DataFrame, pl.DataFrame, list[str], list[str]]:
    """Two training frames over IDENTICAL rows. B's features must contain A's (a nested ablation)."""
    ba, bb = fg.normalize_blocks(blocks_a), fg.normalize_blocks(blocks_b)
    fa_cols, fb_cols = list(fg.feature_list(ba)), list(fg.feature_list(bb))
    if not set(fa_cols) <= set(fb_cols):
        raise core.IntelligenceError(f"arm B {list(bb)} does not contain arm A {list(ba)}: not a nested ablation")
    fa = fg.training_frame(labels, tables, ba)
    fb = fg.training_frame(labels, tables, bb)
    if window_start is not None:                  # arm B is cut to the same window by the intersection below
        fa = fa.filter(pl.col("decision_date") >= window_start)
    common = fa.select(KEY).join(fb.select(KEY), on=KEY, how="inner")
    fa, fb = fa.join(common, on=KEY, how="inner"), fb.join(common, on=KEY, how="inner")
    assert fa.height == fb.height
    sa, sb = fa.sort(KEY), fb.sort(KEY)
    if sa["label"].to_list() != sb["label"].to_list():
        raise core.IntelligenceError("the two arms disagree on labels for the same keys")
    return sa, sb, fa_cols, fb_cols


def run_arm(frame: pl.DataFrame, features: Sequence[str], cfg: core.IntelligenceConfig) -> pl.DataFrame:
    return core.walk_forward_oof(frame, list(features), gx.SPEC, cfg)


# ═══ paired comparison ═══════════════════════════════════════════════════════

def _logloss(y: np.ndarray, p: np.ndarray, w: np.ndarray) -> float:
    pc = np.clip(p, 1e-6, 1 - 1e-6)
    return float(-(w * (y * np.log(pc) + (1 - y) * np.log(1 - pc))).sum() / w.sum())


def _blocks_of_dates(dates: np.ndarray, block_days: int) -> list[np.ndarray]:
    """Row-index groups: consecutive distinct decision dates chunked into blocks of `block_days` dates."""
    uniq = np.unique(dates)
    chunk_of = {d: i // block_days for i, d in enumerate(uniq)}
    ids = np.array([chunk_of[d] for d in dates])
    return [np.flatnonzero(ids == b) for b in range(int(ids.max()) + 1)]


def paired_report(oof_a: pl.DataFrame, oof_b: pl.DataFrame, n_boot: int = 300, block_days: int = 20,
                  seed: int = 7) -> dict:
    """Paired deltas (B - A) on the OOF rows both arms scored, with a moving-block bootstrap over dates."""
    j = oof_a.select(*KEY, "label", "weight", pl.col("p").alias("p_a")).join(
        oof_b.select(*KEY, pl.col("p").alias("p_b")), on=KEY, how="inner").sort(KEY)
    if j.height == 0:
        return {"status": "no_common_oof_rows"}
    y, w = j["label"].to_numpy().astype(float), j["weight"].to_numpy().astype(float)
    pa, pb = j["p_a"].to_numpy().astype(float), j["p_b"].to_numpy().astype(float)
    dates = j["decision_date"].to_numpy()
    ma, mb = core.metrics(y, pa, w), core.metrics(y, pb, w)
    d_auc, d_ll = mb["auc"] - ma["auc"], _logloss(y, pb, w) - _logloss(y, pa, w)

    blocks = _blocks_of_dates(dates, block_days)
    rng = np.random.default_rng(seed)
    boots_auc, boots_ll = [], []
    for _ in range(n_boot):
        idx = np.concatenate([blocks[i] for i in rng.integers(0, len(blocks), size=len(blocks))])
        auc_a, auc_b = core.weighted_auc(y[idx], pa[idx], w[idx]), core.weighted_auc(y[idx], pb[idx], w[idx])
        if np.isnan(auc_a) or np.isnan(auc_b):
            continue
        boots_auc.append(auc_b - auc_a)
        boots_ll.append(_logloss(y[idx], pb[idx], w[idx]) - _logloss(y[idx], pa[idx], w[idx]))
    lo, hi = 100 * (1 - CI_LEVEL) / 2, 100 * (1 + CI_LEVEL) / 2
    ci_auc = [float(np.percentile(boots_auc, lo)), float(np.percentile(boots_auc, hi))] if boots_auc else [float("nan")] * 2
    ci_ll = [float(np.percentile(boots_ll, lo)), float(np.percentile(boots_ll, hi))] if boots_ll else [float("nan")] * 2

    if ci_auc[0] > 0 and ci_ll[1] < 0:
        verdict = "contributes"
    elif ci_auc[1] < 0 and ci_ll[0] > 0:
        verdict = "worse"
    else:
        verdict = "no_evidence"
    years = np.array([d.astype("datetime64[Y]").astype(int) + 1970 for d in dates])
    by_year = {}
    for yr in sorted(set(years.tolist())):
        m = years == yr
        a, b = core.weighted_auc(y[m], pa[m], w[m]), core.weighted_auc(y[m], pb[m], w[m])
        by_year[int(yr)] = {"n_dates": int(len(np.unique(dates[m]))), "delta_auc": None if np.isnan(a) or np.isnan(b) else round(b - a, 5)}
    return {
        "status": "ok", "verdict": verdict, "n_rows": int(j.height), "n_dates": int(len(np.unique(dates))),
        "n_blocks_bootstrap": len(blocks), "block_days": block_days, "n_boot_valid": len(boots_auc),
        "first_oof_date": str(j["decision_date"].min()), "last_oof_date": str(j["decision_date"].max()),
        "base_rate": round(float(ma["base_rate"]), 5),
        "arm_a": {k: round(float(ma[k]), 5) for k in ("auc", "logloss", "brier_skill", "top_decile_lift")},
        "arm_b": {k: round(float(mb[k]), 5) for k in ("auc", "logloss", "brier_skill", "top_decile_lift")},
        "delta_auc": round(float(d_auc), 5), "delta_auc_ci95": [round(v, 5) for v in ci_auc],
        "delta_logloss": round(float(d_ll), 6), "delta_logloss_ci95": [round(v, 6) for v in ci_ll],
        "by_year": by_year,
        "rule": "contributes iff delta_auc_ci95[0] > 0 and delta_logloss_ci95[1] < 0 [U]; gates nothing",
    }


# ═══ orchestration ═══════════════════════════════════════════════════════════

def run_ablation(labels: pl.DataFrame, tables, blocks_a: Sequence[str], blocks_b: Sequence[str],
                 cfg: core.IntelligenceConfig, window_start: Optional[date] = None,
                 n_boot: int = 300, block_days: int = 20, seed: int = 7) -> dict:
    fa, fb, cols_a, cols_b = build_arms(labels, tables, blocks_a, blocks_b, window_start)
    report: dict = {
        "study": "technical_block_ablation", "model": gx.MODEL_NAME, "label_spec": cfg.label_spec,
        "arm_a": {"blocks": list(fg.normalize_blocks(blocks_a)), "n_features": len(cols_a),
                  "feature_hash": core.feature_spec_hash(cols_a)},
        "arm_b": {"blocks": list(fg.normalize_blocks(blocks_b)), "n_features": len(cols_b),
                  "feature_hash": core.feature_spec_hash(cols_b)},
        "window_start": None if window_start is None else str(window_start),
        "n_rows_common": int(fa.height), "n_dates_common": int(fa["decision_date"].n_unique()),
        "config": {"initial_train_months": cfg.initial_train_months, "test_block_months": cfg.test_block_months,
                   "embargo_days": cfg.embargo_days, "min_train_rows": cfg.min_train_rows,
                   "config_fingerprint": cfg.fingerprint()},
    }
    if fa.height == 0:
        return {**report, "status": "no_common_rows"}
    report["span"] = span_check(fa["decision_date"].min(), fa["decision_date"].max(), cfg)
    oa, ob = run_arm(fa, cols_a, cfg), run_arm(fb, cols_b, cfg)
    if oa.height == 0 or ob.height == 0:
        return {**report, "status": "no_oof_blocks"}
    report["n_folds"] = int(oa[core.KEY_FOLD].n_unique())
    report["paired"] = paired_report(oa, ob, n_boot=n_boot, block_days=block_days, seed=seed)
    report["status"] = report["paired"]["status"]
    report["caveats"] = ["effective sample = number of dates, not rows", "date-block intervals are optimistic under "
                         "overlapping label windows", "AUC is rank quality, not P&L", "one window, one config"]
    return report


def _date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--blocks-a", default="base")
    ap.add_argument("--blocks-b", default="base,candle")
    ap.add_argument("--labels-dir", type=Path, default=None)
    ap.add_argument("--signals-dir", type=Path, default=None)
    ap.add_argument("--run-date", type=_date, default=None, help="date the label-freshness rule is judged at (default today)")
    ap.add_argument("--window-start", type=_date, default=None)
    ap.add_argument("--initial-train-months", type=int, default=None)
    ap.add_argument("--test-block-months", type=int, default=None)
    ap.add_argument("--min-train-rows", type=int, default=None)
    ap.add_argument("--n-boot", type=int, default=300)
    ap.add_argument("--block-days", type=int, default=20)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--span-check", action="store_true")
    ap.add_argument("--first-label-date", type=_date, default=None)
    ap.add_argument("--last-decision-date", type=_date, default=None)
    a = ap.parse_args(argv)

    cfg = study_config(a.initial_train_months, a.test_block_months, a.min_train_rows)
    if a.span_check:
        if a.first_label_date is None or a.last_decision_date is None:
            ap.error("--span-check needs --first-label-date and --last-decision-date")
        print(json.dumps(span_check(a.first_label_date, a.last_decision_date, cfg), indent=2))
        return 0

    blocks_a, blocks_b = [s for s in a.blocks_a.split(",") if s], [s for s in a.blocks_b.split(",") if s]
    labels = lab.load_training_labels(cfg, a.run_date or date.today(), a.labels_dir)
    tables = fg.load_tables(a.signals_dir or fg.SIGNALS_DIR, fg.normalize_blocks([*blocks_a, *blocks_b]))
    report = run_ablation(labels, tables, blocks_a, blocks_b, cfg, a.window_start, a.n_boot, a.block_days, a.seed)
    text = json.dumps(report, indent=2, default=str)
    print(text)
    if a.out is not None:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        core.write_json_atomic(a.out, json.loads(text))
    return 0 if report.get("status") == "ok" else 2


if __name__ == "__main__":
    sys.exit(main())
