"""
core.py — ADR-053 (GMI-XGB-001): the shared engine behind the three specialist models.

DOMAIN-AGNOSTIC BY CONSTRUCTION. This module knows nothing about HMM states, indicators or
cross-asset series. It receives a frame of (key columns, feature columns, label, weight,
label_end_date) and returns out-of-fold predictions, a validation report and a persisted model
bundle. Each specialist (hmm_xgboost / gold_xgboost / cross_asset_xgboost) owns its own feature
builder and its own stores; nothing in here may import or read another domain's code or data
(pinned by tests/integration/test_intelligence_domain_independence_adr053.py).

WHAT "NEVER IN-SAMPLE" MEANS HERE (ADR-053 / ADR-054 hard requirement)
    1. Expanding-window walk-forward in calendar blocks. A block's training rows are those
       whose label window had ENDED, with an embargo, before the block starts:
           label_end_date <= block_start - embargo_days.
       A training row whose [decision_date, label_end_date] window reaches into the test block
       is purged — the outcome it carries is partly the test block's future.
    2. The calibrator applied to a block is isotonic regression fitted on the OUT-OF-FOLD raw
       predictions of EARLIER blocks only (causal); the final production calibrator is fitted on
       all of them. Calibrating a block with a map that saw its own outcomes would leak.
    3. Predicting with a model whose fit date is after the requested date is refused.

WEIGHTS / AGGREGATED ROWS
    `weight` lets a specialist whose features are identical across many rows (date-level
    features shared by every symbol) train on one aggregated row per distinct feature vector:
    label = mean outcome in [0, 1], weight = row count. XGBoost's logistic loss is identical to
    the sum over the replicated rows, so nothing is approximated; AUC / Brier / log-loss are
    computed weighted (a fractional label is expanded to a win share and a loss share).

PERSISTENCE
    One JSON file per model: metadata + base64 XGBoost boosters (UBJSON) + the isotonic map
    as two arrays (applied with numpy.interp — no pickle, no sklearn at inference). Atomic
    write (temp file + os.replace).

FIRST-PASS: hyper-parameters (config/intelligence.yaml) are untuned (KNOWN_RISKS RISK-40).
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

import numpy as np
import polars as pl
import xgboost as xgb
import yaml
from loguru import logger
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score

CONFIG_PATH = Path("config/intelligence.yaml")
BUNDLE_SCHEMA = 1
PROCESSING_VERSION = "1.0"

KEY_FOLD = "fold_start"


class IntelligenceError(RuntimeError):
    """Config / data defect that makes a specialist unfittable or unscorable."""


class LeakageError(IntelligenceError):
    """An invariant that keeps a fit out-of-sample was violated (never downgraded to a warning)."""


# ═══ Config ══════════════════════════════════════════════════════════════════

_TOP = frozenset({"version", "label", "walk_forward", "xgb", "bags", "calibration", "confidence", "output_root"})
_TOP_OPT = frozenset({"models"})
_WF = frozenset({"initial_train_months", "test_block_months", "embargo_days", "min_train_rows"})
_XGB = frozenset({"n_estimators", "max_depth", "learning_rate", "subsample", "colsample_bytree",
                  "min_child_weight", "reg_lambda", "max_bin", "nthread"})


@dataclass(frozen=True)
class IntelligenceConfig:
    version: str
    label_spec: str
    label_dir: Path
    label_max_age_days: int
    initial_train_months: int
    test_block_months: int
    embargo_days: int
    min_train_rows: int
    xgb: Mapping[str, Any]
    bags_oof: int
    bags_final: int
    calibration_min_rows: int
    confidence_std_cap: float
    output_root: Path
    model_overrides: Mapping[str, Mapping[str, Mapping[str, Any]]] = None   # name -> {"xgb": {...}, "walk_forward": {...}}

    def for_model(self, name: str) -> "IntelligenceConfig":
        """This config with the model's `models:` overrides applied (xgb / walk_forward keys only).
        The fingerprint of the result includes them, so an override is a different model."""
        ov = (self.model_overrides or {}).get(name)
        if not ov:
            return self
        wf = ov.get("walk_forward", {})
        return replace(
            self, xgb={**self.xgb, **ov.get("xgb", {})},
            initial_train_months=wf.get("initial_train_months", self.initial_train_months),
            test_block_months=wf.get("test_block_months", self.test_block_months),
            embargo_days=wf.get("embargo_days", self.embargo_days),
            min_train_rows=wf.get("min_train_rows", self.min_train_rows),
        )

    def fingerprint(self) -> str:
        """Digest of everything that changes what a fitted model means (labels, folds, xgb, bags,
        calibration, confidence). Written into every model and every stored OOF row."""
        payload = {
            "label_spec": self.label_spec, "wf": [self.initial_train_months, self.test_block_months,
                                                   self.embargo_days, self.min_train_rows],
            "xgb": dict(self.xgb), "bags": [self.bags_oof, self.bags_final],
            "cal": self.calibration_min_rows, "conf": self.confidence_std_cap,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:12]


def _need(cond: bool, msg: str) -> None:
    if not cond:
        raise IntelligenceError(msg)


def _int(x: Any, lo: int) -> bool:
    return isinstance(x, int) and not isinstance(x, bool) and x >= lo


def validate_config(raw: Mapping[str, Any]) -> IntelligenceConfig:
    _need(isinstance(raw, Mapping), "intelligence config must be a mapping")
    _need(_TOP <= set(raw) <= (_TOP | _TOP_OPT),
          f"intelligence config keys: unknown={sorted(set(raw) - _TOP - _TOP_OPT)} missing={sorted(_TOP - set(raw))}")
    lab, wf, xg = raw["label"], raw["walk_forward"], raw["xgb"]
    _need(isinstance(lab, Mapping) and {"spec", "dir", "max_age_days"} <= set(lab), "label block needs spec/dir/max_age_days")
    _need(isinstance(lab["spec"], str) and lab["spec"], "label.spec must be a non-empty string")
    _need(_int(lab["max_age_days"], 1), "label.max_age_days must be an int >= 1")
    _need(isinstance(wf, Mapping) and set(wf) == _WF, f"walk_forward keys must be {sorted(_WF)}")
    _need(_int(wf["initial_train_months"], 6), "initial_train_months must be an int >= 6")
    _need(wf["test_block_months"] in (1, 2, 3, 4, 6, 12), "test_block_months must divide a year")
    _need(_int(wf["embargo_days"], 0), "embargo_days must be an int >= 0")
    _need(_int(wf["min_train_rows"], 100), "min_train_rows must be an int >= 100")
    _need(isinstance(xg, Mapping) and set(xg) == _XGB, f"xgb keys must be {sorted(_XGB)}")
    bags = raw["bags"]
    _need(isinstance(bags, Mapping) and set(bags) == {"oof", "final"} and _int(bags["oof"], 1) and _int(bags["final"], 1),
          "bags needs int oof/final >= 1")
    cal, conf = raw["calibration"], raw["confidence"]
    _need(isinstance(cal, Mapping) and _int(cal.get("min_rows"), 100), "calibration.min_rows must be an int >= 100")
    _need(isinstance(conf, Mapping) and isinstance(conf.get("std_cap"), (int, float)) and conf["std_cap"] > 0,
          "confidence.std_cap must be > 0")
    models = raw.get("models") or {}
    _need(isinstance(models, Mapping), "models must be a mapping")
    for name, ov in models.items():
        _need(isinstance(ov, Mapping) and set(ov) <= {"xgb", "walk_forward"}, f"models.{name}: only xgb / walk_forward overrides")
        _need(set(ov.get("xgb", {})) <= _XGB, f"models.{name}.xgb: unknown keys {sorted(set(ov.get('xgb', {})) - _XGB)}")
        _need(set(ov.get("walk_forward", {})) <= _WF, f"models.{name}.walk_forward: unknown keys")
    return IntelligenceConfig(
        version=str(raw["version"]), label_spec=lab["spec"], label_dir=Path(lab["dir"]),
        label_max_age_days=lab["max_age_days"], initial_train_months=wf["initial_train_months"],
        test_block_months=wf["test_block_months"], embargo_days=wf["embargo_days"],
        min_train_rows=wf["min_train_rows"], xgb=dict(xg), bags_oof=bags["oof"], bags_final=bags["final"],
        calibration_min_rows=cal["min_rows"], confidence_std_cap=float(conf["std_cap"]),
        output_root=Path(raw["output_root"]), model_overrides={k: dict(v) for k, v in models.items()},
    )


def load_config(path: Path | str | None = None) -> IntelligenceConfig:
    with open(CONFIG_PATH if path is None else path) as fh:
        return validate_config(yaml.safe_load(fh))


def feature_spec_hash(features: Sequence[str]) -> str:
    """Order-sensitive digest of a feature list: a reordered / renamed feature is a different model."""
    return hashlib.sha256("|".join(features).encode()).hexdigest()[:12]


# ═══ Folds and purging ═══════════════════════════════════════════════════════

def _add_months(d: date, months: int) -> date:
    m = d.month - 1 + months
    return date(d.year + m // 12, m % 12 + 1, 1)


def block_starts(first_label: date, last_decision: date, cfg: IntelligenceConfig) -> list[date]:
    """Out-of-fold block start dates: the first month boundary at least `initial_train_months`
    after the first label, then every `test_block_months`, while a block can still contain a
    decision date <= last_decision."""
    first = _add_months(date(first_label.year, first_label.month, 1), cfg.initial_train_months)
    first = first if first > first_label else _add_months(first, 1)
    out: list[date] = []
    d = first
    while d <= last_decision:
        out.append(d)
        d = _add_months(d, cfg.test_block_months)
    return out


def purged_train_mask(label_end: pl.Series, block_start: date, embargo_days: int) -> pl.Series:
    """Rows whose label window ended at least `embargo_days` before the block starts."""
    return label_end <= (block_start - timedelta(days=embargo_days))


def assert_no_overlap(train: pl.DataFrame, test_start: date, embargo_days: int, date_col: str, end_col: str) -> None:
    """Structural leakage guard run on every fold (and mutation-tested): no training window may
    reach into [test_start - embargo, ...)."""
    if train.height == 0:
        return
    worst_end = train[end_col].max()
    worst_dec = train[date_col].max()
    limit = test_start - timedelta(days=embargo_days)
    if worst_end > limit or worst_dec >= test_start:
        raise LeakageError(f"training rows reach the test block: max label_end {worst_end} > {limit} "
                           f"or max decision {worst_dec} >= {test_start}")


# ═══ XGBoost: bagged fit / predict ═══════════════════════════════════════════

def _params(cfg: IntelligenceConfig, seed: int) -> dict[str, Any]:
    x = cfg.xgb
    return {
        "objective": "binary:logistic", "eval_metric": "logloss", "tree_method": "hist",
        "max_depth": int(x["max_depth"]), "eta": float(x["learning_rate"]),
        "subsample": float(x["subsample"]), "colsample_bytree": float(x["colsample_bytree"]),
        "min_child_weight": float(x["min_child_weight"]), "lambda": float(x["reg_lambda"]),
        "max_bin": int(x["max_bin"]), "nthread": int(x["nthread"]), "seed": int(seed), "verbosity": 0,
    }


def _dmatrix(X: np.ndarray, features: Sequence[str], y: Optional[np.ndarray] = None,
             w: Optional[np.ndarray] = None) -> xgb.DMatrix:
    return xgb.DMatrix(X, label=y, weight=w, feature_names=list(features), missing=np.nan)


def fit_bagged(X: np.ndarray, y: np.ndarray, w: Optional[np.ndarray], features: Sequence[str],
               cfg: IntelligenceConfig, n_bags: int, seed0: int = 0) -> list[xgb.Booster]:
    """n_bags boosters differing only in seed (row/column subsampling): their spread is the
    model's own uncertainty. Labels may be fractional in [0, 1] (aggregated rows)."""
    dtrain = _dmatrix(X, features, y, w)
    return [xgb.train(_params(cfg, seed0 + i), dtrain, num_boost_round=int(cfg.xgb["n_estimators"]))
            for i in range(n_bags)]


def predict_bagged(boosters: Sequence[xgb.Booster], X: np.ndarray, features: Sequence[str]) -> tuple[np.ndarray, np.ndarray]:
    """(mean probability, std across bags) per row."""
    d = _dmatrix(X, features)
    P = np.vstack([b.predict(d) for b in boosters]).astype(np.float64)     # xgboost returns float32
    return P.mean(axis=0), P.std(axis=0)


def confidence_from_std(std: np.ndarray, std_cap: float) -> np.ndarray:
    return 1.0 - np.minimum(1.0, std / std_cap)


# ═══ Calibration (isotonic, stored as two arrays) ════════════════════════════

def fit_calibrator(p: np.ndarray, y: np.ndarray, w: Optional[np.ndarray]) -> dict[str, list[float]]:
    iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    iso.fit(p, y, sample_weight=w)
    return {"x": iso.X_thresholds_.tolist(), "y": iso.y_thresholds_.tolist()}


def apply_calibrator(cal: Optional[Mapping[str, Sequence[float]]], p: np.ndarray) -> np.ndarray:
    if cal is None:
        return p
    return np.interp(p, np.asarray(cal["x"]), np.asarray(cal["y"]))


# ═══ Metrics (weighted, fractional labels expanded) ══════════════════════════

def _expand(y: np.ndarray, w: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Fractional label -> (win share, loss share) pair, so weighted binary metrics are exact."""
    yy = np.concatenate([np.ones_like(y), np.zeros_like(y)])
    ww = np.concatenate([w * y, w * (1.0 - y)])
    keep = ww > 0
    return yy[keep], ww[keep]


def weighted_auc(y: np.ndarray, p: np.ndarray, w: np.ndarray) -> float:
    """Exact weighted AUC for fractional labels (each row counts as a win share and a loss share)."""
    yy, ww = _expand(y, w)
    pp = np.concatenate([p, p])[np.concatenate([w * y, w * (1.0 - y)]) > 0]
    if yy.size == 0 or yy.min() == yy.max():
        return float("nan")
    return float(roc_auc_score(yy, pp, sample_weight=ww))


def metrics(y: np.ndarray, p: np.ndarray, w: Optional[np.ndarray] = None) -> dict[str, float]:
    """auc, brier, brier_skill (vs constant base rate), logloss, logloss_gain, base_rate, and the
    weighted win rate / lift of the top decile of p."""
    w = np.ones_like(y, dtype=float) if w is None else w.astype(float)
    n = float(w.sum())
    base = float((w * y).sum() / n)
    pc = np.clip(p, 1e-6, 1 - 1e-6)
    # Brier on replicated binary rows is sum(p^2 - 2*p*y_i + y_i) because y_i^2 == y_i; with an
    # aggregated (fractional) label the same sum is p^2 - 2*p*ybar + ybar, NOT (p - ybar)^2.
    brier = float((w * (p ** 2 - 2 * p * y + y)).sum() / n)
    brier_base = float((w * (base ** 2 - 2 * base * y + y)).sum() / n)
    ll = float(-(w * (y * np.log(pc) + (1 - y) * np.log(1 - pc))).sum() / n)
    b = min(max(base, 1e-6), 1 - 1e-6)
    ll_base = float(-(w * (y * np.log(b) + (1 - y) * np.log(1 - b))).sum() / n)
    order = np.argsort(-p, kind="stable")
    cum = np.cumsum(w[order])
    top = order[cum <= 0.1 * n] if (cum <= 0.1 * n).any() else order[:1]
    top_rate = float((w[top] * y[top]).sum() / w[top].sum())
    return {
        "n": n, "base_rate": base, "auc": weighted_auc(y, p, w), "brier": brier,
        "brier_skill": 1.0 - brier / brier_base if brier_base > 0 else float("nan"),
        "logloss": ll, "logloss_gain": ll_base - ll,
        "top_decile_win_rate": top_rate, "top_decile_lift": top_rate / base if base > 0 else float("nan"),
    }


# ═══ Walk-forward out-of-fold engine ═════════════════════════════════════════

@dataclass(frozen=True)
class FrameSpec:
    """Column names of the training frame a specialist hands to the engine."""
    date_col: str = "decision_date"
    end_col: str = "label_end_date"
    label_col: str = "label"           # in [0, 1]; aggregated rows carry the mean outcome
    weight_col: str = "weight"
    key_cols: tuple[str, ...] = ("decision_date",)   # carried through to the OOF store


def _sort_cols(spec: "FrameSpec") -> list[str]:
    """Total order of a training frame: decision date, then every key column. A frame built by a
    group_by has no stable row order, and XGBoost's row subsampling is positional — an unsorted
    frame makes two fits of identical data differ (found by the second-fit reuse test)."""
    return list(dict.fromkeys([spec.date_col, *spec.key_cols]))


def walk_forward_oof(frame: pl.DataFrame, features: Sequence[str], spec: FrameSpec, cfg: IntelligenceConfig,
                     done: Optional[pl.DataFrame] = None,
                     fit_fn: Callable[..., list] = fit_bagged,
                     purge_fn: Callable[[pl.Series, date, int], pl.Series] = purged_train_mask,
                     today: Optional[date] = None) -> pl.DataFrame:
    """Out-of-fold predictions for every block not already in `done` (the stored OOF frame).

    Returns the NEW rows only: spec.key_cols + label + weight + p_raw + p + bag_std + fold_start.
    Blocks are processed in order; the calibrator for a block is fitted on every earlier block's
    OOF raw predictions (done + new). A block with fewer than cfg.min_train_rows purged training
    rows is skipped. `fit_fn` / `purge_fn` are injectable for mutation tests ONLY.
    """
    for c in (*features, spec.date_col, spec.end_col, spec.label_col, spec.weight_col):
        _need(c in frame.columns, f"training frame lacks column {c!r}")
    frame = frame.sort(_sort_cols(spec), maintain_order=True)    # row ORDER feeds xgboost's row subsampling
    if frame.height == 0:
        return pl.DataFrame()
    first, last = frame[spec.date_col].min(), frame[spec.date_col].max()
    starts = block_starts(first, last, cfg)
    done_folds = set(done[KEY_FOLD].unique().to_list()) if done is not None and done.height else set()

    # earlier-block OOF raw predictions for the causal calibrator
    cal_p = [done["p_raw"].to_numpy()] if done is not None and done.height else []
    cal_y = [done[spec.label_col].to_numpy()] if done is not None and done.height else []
    cal_w = [done[spec.weight_col].to_numpy()] if done is not None and done.height else []
    new_parts: list[pl.DataFrame] = []

    for i, start in enumerate(starts):
        end = _add_months(start, cfg.test_block_months)
        test = frame.filter((pl.col(spec.date_col) >= start) & (pl.col(spec.date_col) < end))
        if test.height == 0 or start in done_folds:
            continue
        train = frame.filter(purge_fn(frame[spec.end_col], start, cfg.embargo_days)
                             & (pl.col(spec.date_col) < start))
        assert_no_overlap(train, start, cfg.embargo_days, spec.date_col, spec.end_col)
        if train.height < cfg.min_train_rows:
            logger.warning(f"[intelligence] block {start}: {train.height} purged training rows < "
                           f"{cfg.min_train_rows} — block skipped")
            continue
        boosters = fit_fn(train.select(features).to_numpy().astype(np.float64),
                          train[spec.label_col].to_numpy().astype(np.float64),
                          train[spec.weight_col].to_numpy().astype(np.float64), features, cfg, cfg.bags_oof,
                          seed0=1000 + i * 10)
        p_raw, std = predict_bagged(boosters, test.select(features).to_numpy().astype(np.float64), features)
        n_cal = sum(len(a) for a in cal_p)
        cal = (fit_calibrator(np.concatenate(cal_p), np.concatenate(cal_y), np.concatenate(cal_w))
               if n_cal >= cfg.calibration_min_rows else None)
        p = apply_calibrator(cal, p_raw)
        part = test.select(*spec.key_cols, spec.label_col, spec.weight_col).with_columns(
            pl.Series("p_raw", p_raw), pl.Series("p", p), pl.Series("bag_std", std),
            pl.lit(start).alias(KEY_FOLD), pl.lit(cal is not None).alias("calibrated"),
            pl.lit(cfg.fingerprint()).alias("config_fingerprint"),
        )
        new_parts.append(part)
        cal_p.append(p_raw)
        cal_y.append(test[spec.label_col].to_numpy().astype(np.float64))
        cal_w.append(test[spec.weight_col].to_numpy().astype(np.float64))
        logger.info(f"[intelligence] block {start}..{end}: train={train.height} test={test.height} "
                    f"calibrated={cal is not None}")
    return pl.concat(new_parts) if new_parts else pl.DataFrame()


def validation_report(oof: pl.DataFrame, spec: FrameSpec, cfg: IntelligenceConfig) -> dict[str, Any]:
    """The per-specialist walk-forward metric ADR-053 requires reviewed before ADR-054 stacks
    anything on top: overall + per calendar year, on the causally calibrated p."""
    if oof.height == 0:
        return {"n_blocks": 0}
    y, p, w = (oof[spec.label_col].to_numpy().astype(float), oof["p"].to_numpy(), oof[spec.weight_col].to_numpy().astype(float))
    praw = oof["p_raw"].to_numpy()
    rep: dict[str, Any] = {
        "n_blocks": int(oof[KEY_FOLD].n_unique()),
        "first_oof_date": str(oof[spec.date_col].min()), "last_oof_date": str(oof[spec.date_col].max()),
        "overall": metrics(y, p, w), "overall_auc_raw": weighted_auc(y, praw, w),
    }
    years = oof[spec.date_col].dt.year().to_numpy()
    rep["by_year"] = {
        int(yr): {k: round(v, 5) for k, v in metrics(y[years == yr], p[years == yr], w[years == yr]).items()}
        for yr in sorted(set(years.tolist()))
    }
    return rep


# ═══ Final model + bundle ════════════════════════════════════════════════════

def fit_final(frame: pl.DataFrame, features: Sequence[str], spec: FrameSpec, cfg: IntelligenceConfig,
              oof: pl.DataFrame, fit_date: date, meta: Mapping[str, Any]) -> dict[str, Any]:
    """Production model: bagged boosters on every row whose label window ended before
    fit_date - embargo, calibrator fitted on ALL out-of-fold raw predictions."""
    frame = frame.sort(_sort_cols(spec), maintain_order=True)
    train = frame.filter(purged_train_mask(frame[spec.end_col], fit_date, cfg.embargo_days))
    _need(train.height >= cfg.min_train_rows, f"final fit: {train.height} rows < min_train_rows {cfg.min_train_rows}")
    assert_no_overlap(train, fit_date, cfg.embargo_days, spec.date_col, spec.end_col)
    boosters = fit_bagged(train.select(features).to_numpy().astype(np.float64),
                          train[spec.label_col].to_numpy().astype(np.float64),
                          train[spec.weight_col].to_numpy().astype(np.float64), features, cfg, cfg.bags_final, seed0=1)
    cal = None
    if oof.height >= cfg.calibration_min_rows:
        cal = fit_calibrator(oof["p_raw"].to_numpy(), oof[spec.label_col].to_numpy().astype(float),
                             oof[spec.weight_col].to_numpy().astype(float))
    return {
        "schema": BUNDLE_SCHEMA, "processing_version": PROCESSING_VERSION,
        "fit_date": fit_date.isoformat(), "train_start": str(train[spec.date_col].min()),
        "train_end": str(train[spec.date_col].max()), "n_train_rows": int(train.height),
        "features": list(features), "feature_spec_hash": feature_spec_hash(features),
        "config_fingerprint": cfg.fingerprint(), "label_spec": cfg.label_spec,
        "confidence_std_cap": cfg.confidence_std_cap, "calibrator": cal,
        "boosters_b64": [base64.b64encode(bytes(b.save_raw(raw_format="ubj"))).decode() for b in boosters],
        **dict(meta),
    }


def save_bundle(path: Path, bundle: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(bundle, fh, sort_keys=True)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def load_bundle(path: Path, expect_schema: int = BUNDLE_SCHEMA) -> Optional[dict]:
    if not Path(path).exists():
        return None
    b = json.loads(Path(path).read_text())
    _need(b.get("schema") == expect_schema, f"{path}: bundle schema {b.get('schema')} != {expect_schema}")
    return b


def _boosters(bundle: Mapping[str, Any]) -> list[xgb.Booster]:
    out = []
    for s in bundle["boosters_b64"]:
        b = xgb.Booster()
        b.load_model(bytearray(base64.b64decode(s)))
        out.append(b)
    return out


def predict_bundle(bundle: Mapping[str, Any], X: pl.DataFrame, as_of: date) -> pl.DataFrame:
    """probability / confidence / bag_std for each row of X (which must hold the bundle's
    features, in any column order). Refuses a model fitted after `as_of`."""
    fit_d = date.fromisoformat(bundle["fit_date"])
    if fit_d > as_of:
        raise LeakageError(f"model fitted {fit_d} cannot score {as_of} (fit date is in the future)")
    feats = bundle["features"]
    missing = [f for f in feats if f not in X.columns]
    _need(not missing, f"prediction frame lacks model features {missing}")
    P, S = predict_bagged(_boosters(bundle), X.select(feats).to_numpy().astype(np.float64), feats)
    return pl.DataFrame({
        "p_raw": P, "probability": apply_calibrator(bundle.get("calibrator"), P),
        "confidence": confidence_from_std(S, float(bundle["confidence_std_cap"])), "bag_std": S,
    })


# ═══ Stores and the generic fit pipeline ═════════════════════════════════════

@dataclass(frozen=True)
class ModelStore:
    """Every artifact a specialist owns lives under <output_root>/<name>/ — nothing else writes there."""
    root: Path
    name: str

    @property
    def dir(self) -> Path:
        return self.root / self.name

    @property
    def model(self) -> Path:
        return self.dir / "model.json"

    @property
    def oof(self) -> Path:
        return self.dir / "oof_predictions.parquet"

    @property
    def validation(self) -> Path:
        return self.dir / "validation.json"

    @property
    def fit_status(self) -> Path:
        return self.dir / "fit_status.json"

    @property
    def predictions(self) -> Path:
        return self.dir / "predictions.parquet"


def write_json_atomic(path: Path, obj: Mapping[str, Any]) -> None:
    save_bundle(path, obj)          # same atomic temp+replace writer; the payload is plain JSON


def write_parquet_atomic(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    os.close(fd)
    try:
        df.write_parquet(tmp, compression="zstd")
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def _reusable_oof(path: Path, cfg: IntelligenceConfig, features: Sequence[str]) -> Optional[pl.DataFrame]:
    """Stored OOF rows to keep (every completed block): only when the fingerprint, feature list
    and processing version are unchanged — otherwise the whole history is rebuilt (logged)."""
    if not path.exists():
        return None
    oof = pl.read_parquet(path)
    ok = {"config_fingerprint", "feature_spec_hash", "processing_version", KEY_FOLD} <= set(oof.columns) and oof.height > 0
    if ok and (set(oof["config_fingerprint"].unique()) != {cfg.fingerprint()}
               or set(oof["feature_spec_hash"].unique()) != {feature_spec_hash(features)}
               or set(oof["processing_version"].unique()) != {PROCESSING_VERSION}):
        logger.warning(f"[intelligence] {path.name}: config / feature spec / version changed — OOF rebuilt from scratch")
        ok = False
    return oof if ok else None


def run_fit_pipeline(store: ModelStore, frame: pl.DataFrame, features: Sequence[str], spec: FrameSpec,
                     cfg: IntelligenceConfig, fit_date: date, meta: Mapping[str, Any]) -> dict[str, Any]:
    """Incremental OOF -> validation report -> final model. Returns the fit_status record (also written).

    The OOF store is insert-only per block: completed blocks are never recomputed unless the
    fingerprint changes. The last, still-open block is recomputed each run (its test rows grow)."""
    store.dir.mkdir(parents=True, exist_ok=True)
    frame = frame.sort(_sort_cols(spec), maintain_order=True)
    stored = _reusable_oof(store.oof, cfg, features)
    done = None
    if stored is not None:
        last = stored[KEY_FOLD].max()
        done = stored.filter(pl.col(KEY_FOLD) < last)          # the open block is recomputed
        if done.height == 0:
            done = None
    new = walk_forward_oof(frame, features, spec, cfg, done=done)
    parts = [x for x in (done, new) if x is not None and x.height]
    if parts:
        oof = pl.concat(parts, how="diagonal_relaxed").sort(spec.date_col)
        if "feature_spec_hash" not in oof.columns or oof["feature_spec_hash"].null_count():
            oof = oof.with_columns(pl.lit(feature_spec_hash(features)).alias("feature_spec_hash"),
                                   pl.lit(PROCESSING_VERSION).alias("processing_version"))
        write_parquet_atomic(oof, store.oof)
    else:
        oof = pl.DataFrame()
    report = validation_report(oof, spec, cfg) if oof.height else {"n_blocks": 0}
    write_json_atomic(store.validation, {"fit_date": fit_date.isoformat(), "config_fingerprint": cfg.fingerprint(),
                                         "feature_spec_hash": feature_spec_hash(features), **report})
    status: dict[str, Any] = {"fit_date": fit_date.isoformat(), "n_oof_rows": int(oof.height),
                              "n_blocks": report.get("n_blocks", 0)}
    try:
        bundle = fit_final(frame, features, spec, cfg, oof, fit_date, meta)
    except IntelligenceError as e:
        status.update(status="skipped", reason=str(e))
        logger.warning(f"[intelligence] {store.name}: final fit skipped — {e}")
    else:
        save_bundle(store.model, bundle)
        status.update(status="fitted", n_train_rows=bundle["n_train_rows"], train_end=bundle["train_end"])
        logger.info(f"[intelligence] {store.name}: fitted on {bundle['n_train_rows']} rows, "
                    f"{status['n_blocks']} OOF blocks, auc={report.get('overall', {}).get('auc')}")
    write_json_atomic(store.fit_status, status)
    return status


def append_predictions(store: ModelStore, new: pl.DataFrame, as_of_date: date) -> int:
    """Idempotent daily write: rows for `as_of_date` replace any earlier rows of that date; every
    other date is untouched. Returns the stored row count."""
    if new.height == 0:
        return 0
    old = pl.read_parquet(store.predictions) if store.predictions.exists() else None
    if old is not None:
        old = old.filter(pl.col("as_of_date") != as_of_date)
        new = pl.concat([old, new], how="diagonal_relaxed")
    new = new.sort("as_of_date")
    write_parquet_atomic(new, store.predictions)
    return new.height
