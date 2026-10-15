"""
src/gold/hmm/hmm_core.py — ADR-051 (GMI-HMM2-001): the HMM engine both regime branches share.

One implementation of everything that is NOT branch-specific, so the macro and market models
cannot drift apart (the GAP-1 failure class — a second copy of the same logic):

    robust scaling            fit_scaler / apply_scaler        median / MAD, clipped
    fitting                   fit_domain / fit_model           deterministic composite start
                                                               competing with random EM starts on
                                                               likelihood; degenerate fits refused
    inference                 forward_filter                   numpy forward FILTER, no lookahead,
                                                               null features marginalised
    output columns            state_columns                    <prefix>_label / _probability /
                                                               _score / _post_<label> / ...
    persistence               atomic_write_text / save_json / load_model_file / append_store

Branch-specific code stays in its own module: which features feed which model, the label
vocabulary, the store layout, the macro business-cycle rule. This module imports NOTHING from
raw_macro / raw_market / the regime modules and reads no store — it is pure math plus file
helpers, so it cannot couple the branches through state (GD §17.7).

The function bodies below were MOVED verbatim from hmm_macro_regime.py; a golden-master check
(old vs refactored macro fit on a synthetic panel, byte-identical model JSON and store) gated
the move. hmm_macro_regime re-exports every moved name, so its public surface is unchanged.
"""

from __future__ import annotations

import contextlib
import json
import logging
import math
import os
import tempfile
import warnings
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Type

import numpy as np
import polars as pl

from src.gold.hmm.config import FitSettings
from src.utils.atomic_io import atomic_write_parquet

_EPS = 1e-12
_MAD_TO_SIGMA = 1.4826


# ═══ Robust scaling ══════════════════════════════════════════════════════════

def fit_scaler(X: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Per-column (center, scale): median and 1.4826 x MAD; a zero MAD falls back to the
    std, then to 1.0."""
    center = np.median(X, axis=0)
    mad = np.median(np.abs(X - center), axis=0) * _MAD_TO_SIGMA
    std = np.std(X, axis=0)
    scale = np.where(mad > _EPS, mad, np.where(std > _EPS, std, 1.0))
    return center, scale


def apply_scaler(X: np.ndarray, center: np.ndarray, scale: np.ndarray, clip: float) -> np.ndarray:
    """(X - center) / scale clipped to +/- clip. NaN stays NaN (a missing value is missing)."""
    return np.clip((X - center) / scale, -clip, clip)


# ═══ Fitting ═════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class Candidate:
    start: np.ndarray
    trans: np.ndarray
    means: np.ndarray
    vars: np.ndarray
    loglik: float
    init: str
    converged: bool
    shares: np.ndarray
    degenerate: bool


def _composite_init(Z: np.ndarray, w: np.ndarray, k: int, var_floor: float):
    """Deterministic start: tertile (k-quantile) bins of the oriented composite. Returns
    (start, trans, means, vars) or None when a bin is too small to estimate from."""
    comp = Z @ w
    edges = np.quantile(comp, np.linspace(0.0, 1.0, k + 1)[1:-1])
    bins = np.searchsorted(edges, comp, side="right")
    counts = np.bincount(bins, minlength=k)
    if (counts < 2).any():
        return None
    means = np.vstack([Z[bins == s].mean(axis=0) for s in range(k)])
    variances = np.vstack([np.maximum(Z[bins == s].var(axis=0), var_floor) for s in range(k)])
    trans = np.ones((k, k))                                    # +1 smoothing: no zero rows/entries
    for a, b in zip(bins[:-1], bins[1:]):
        trans[a, b] += 1.0
    trans /= trans.sum(axis=1, keepdims=True)
    return counts / counts.sum(), trans, means, variances


@contextlib.contextmanager
def _quiet_hmmlearn():
    """hmmlearn logs 'Model is not converging' for EM log-likelihood wiggles of ~1e-4 (the variance
    floor perturbs monotonicity at tolerance level). Whether a candidate converged is RECORDED
    (Candidate.converged, the model file's audit list); the stream noise adds nothing."""
    lg = logging.getLogger("hmmlearn")
    old = lg.level
    lg.setLevel(logging.CRITICAL)
    try:
        yield
    finally:
        lg.setLevel(old)


def _diag(model) -> np.ndarray:
    cv = np.asarray(model.covars_)
    return cv if cv.ndim == 2 else np.diagonal(cv, axis1=1, axis2=2)


def _fit_candidate(Z: np.ndarray, w: np.ndarray, st: FitSettings, init: str, seed: int) -> Optional[Candidate]:
    from hmmlearn.hmm import GaussianHMM          # fit-time only: inference never imports hmmlearn
    k = st.n_states
    common = dict(n_components=k, covariance_type="diag", n_iter=st.n_iter, tol=st.tol, min_covar=st.var_floor)
    if init == "composite":
        start = _composite_init(Z, w, k, st.var_floor)
        if start is None:
            return None
        model = GaussianHMM(init_params="", params="stmc", **common)
        model.startprob_, model.transmat_, model.means_, model.covars_ = start
    else:
        model = GaussianHMM(random_state=seed, **common)
    with warnings.catch_warnings(), _quiet_hmmlearn():
        warnings.simplefilter("ignore")
        model.fit(Z)
        loglik = float(model.score(Z))
        shares = np.bincount(model.predict(Z), minlength=k) / len(Z)
    start_p, trans, means, variances = (np.array(model.startprob_), np.array(model.transmat_),
                                        np.array(model.means_), np.array(_diag(model)))
    order = np.argsort(means @ w, kind="stable")               # ascending along the oriented composite
    start_p, means, variances, shares = start_p[order], means[order], variances[order], shares[order]
    trans = trans[np.ix_(order, order)]
    finite = all(np.isfinite(a).all() for a in (start_p, trans, means, variances)) and math.isfinite(loglik)
    degenerate = (not finite) or shares.min() < st.min_state_share or variances.min() <= st.var_floor * 1.0001
    return Candidate(start_p, trans, means, variances, loglik, init if init == "composite" else f"random:{seed}",
                     bool(model.monitor_.converged), shares, bool(degenerate))


def fit_domain(Z: np.ndarray, w: np.ndarray, st: FitSettings) -> tuple[Optional[Candidate], list[dict]]:
    """Fit one domain. Returns (winner | None, per-candidate audit list). Winner = the highest
    likelihood NON-DEGENERATE candidate among the deterministic start and the random starts."""
    plan = [("composite", 0)] + [("random", s) for s in range(st.n_random_seeds)]
    cands: list[Candidate] = []
    audit: list[dict] = []
    for init, seed in plan:
        name = init if init == "composite" else f"random:{seed}"
        try:
            c = _fit_candidate(Z, w, st, init, seed)
        except Exception as exc:                               # EM can fail numerically on a bad start
            audit.append({"init": name, "error": f"{type(exc).__name__}: {exc}"[:120]})
            continue
        if c is None:
            audit.append({"init": name, "error": "initial bins too small"})
            continue
        cands.append(c)
        audit.append({"init": name, "loglik": c.loglik, "degenerate": c.degenerate, "converged": c.converged})
    for c in sorted(cands, key=lambda c: c.loglik, reverse=True):
        if not c.degenerate:
            return c, audit
    return None, audit


# ═══ Inference: forward filter ═══════════════════════════════════════════════

def _log_emission(z: np.ndarray, means: np.ndarray, variances: np.ndarray) -> np.ndarray:
    """log N(z | mean_k, diag(var_k)) per state, over the OBSERVED dimensions only (exact
    marginalisation for a diagonal covariance). Nothing observed -> zeros (uninformative)."""
    m = ~np.isnan(z)
    if not m.any():
        return np.zeros(means.shape[0])
    d = z[m] - means[:, m]
    return -0.5 * np.sum(np.log(2.0 * np.pi * variances[:, m]) + d * d / variances[:, m], axis=1)


def forward_filter(Z: np.ndarray, start: np.ndarray, trans: np.ndarray,
                   means: np.ndarray, variances: np.ndarray) -> np.ndarray:
    """Filtered state posteriors P(state_t | z_1..z_t), shape (T, K). Row t uses ONLY rows <= t.
    Rows before the first observed row are NaN; a later row with nothing observed is a pure
    prediction step (alpha @ trans)."""
    T, k = Z.shape[0], means.shape[0]
    post = np.full((T, k), np.nan)
    alpha: Optional[np.ndarray] = None
    for t in range(T):
        observed = not np.isnan(Z[t]).all()
        if alpha is None:
            if not observed:
                continue
            pred = start
        else:
            pred = alpha @ trans
        le = _log_emission(Z[t], means, variances)
        w = pred * np.exp(le - le.max())
        s = w.sum()
        alpha = w / s if s > 0 and np.isfinite(s) else pred
        post[t] = alpha
    return post


# ═══ Whole-model fit (one named model on one panel) ══════════════════════════

def fit_model(panel: pl.DataFrame, key: str, name: str, feats: Sequence[str], w: np.ndarray,
              labels: Sequence[str], fit: FitSettings, fingerprint: str, run_date: date,
              processing_version: str, schema: int) -> tuple[Optional[dict], str]:
    """Fit one model on the panel's complete-case rows (every feature present) up to the panel's
    end. Returns (model record | None, status line). None means skipped (too few rows) or refused
    (no non-degenerate candidate): the caller keeps whatever model it already had. The record is
    the JSON-serialisable parameter set + scaler + lineage that forward inference needs — nothing
    about it is branch-specific except `name` (the macro domain, or "market")."""
    feats = list(feats)
    sub = panel.select(key, *feats).filter(pl.all_horizontal([pl.col(f).is_not_null() for f in feats]))
    n = sub.height
    if n < fit.min_obs_to_fit:
        return None, f"skipped: {n} complete rows < min_obs_to_fit {fit.min_obs_to_fit}"
    X = sub.select(feats).to_numpy()
    center, scale = fit_scaler(X)
    Z = apply_scaler(X, center, scale, fit.scaler_clip)
    cand, audit = fit_domain(Z, w, fit)
    if cand is None:
        return None, f"refused: no non-degenerate candidate of {len(audit)} (previous model kept)"
    short = n < fit.short_history_min_obs
    record = {
        "schema": schema, "domain": name, "processing_version": processing_version,
        "model_version": fit.model_version, "config_fingerprint": fingerprint,
        "features": feats, "weights": w.tolist(), "labels": list(labels),
        "scaler": {"center": center.tolist(), "scale": scale.tolist(), "clip": fit.scaler_clip},
        "startprob": cand.start.tolist(), "transmat": cand.trans.tolist(),
        "means": cand.means.tolist(), "vars": cand.vars.tolist(),
        "train_start": sub[key].min().isoformat(), "train_end": sub[key].max().isoformat(),
        "n_obs": n, "short_history": short, "fit_date": run_date.isoformat(),
        "loglik": cand.loglik, "converged": cand.converged, "init": cand.init,
        "state_shares": cand.shares.tolist(), "candidates": audit,
    }
    status = (f"fitted: n={n} {sub[key].min()}..{sub[key].max()} init={cand.init} "
              f"loglik={cand.loglik:.1f} shares={np.round(cand.shares, 3).tolist()}"
              f"{' SHORT_HISTORY' if short else ''}")
    return record, status


# ═══ Output columns ══════════════════════════════════════════════════════════

def empty_state_columns(n: int, prefix: str, labels: Sequence[str]) -> dict[str, pl.Series]:
    cols: dict[str, pl.Series] = {
        f"{prefix}_label": pl.Series([None] * n, dtype=pl.String),
        f"{prefix}_probability": pl.Series([None] * n, dtype=pl.Float64),
        f"{prefix}_score": pl.Series([None] * n, dtype=pl.Float64),
    }
    for lab in labels:
        cols[f"{prefix}_post_{lab}"] = pl.Series([None] * n, dtype=pl.Float64)
    cols[f"{prefix}_short_history"] = pl.Series([None] * n, dtype=pl.Boolean)
    cols[f"{prefix}_model_fit_date"] = pl.Series([None] * n, dtype=pl.Date)
    return cols


def state_columns(panel: pl.DataFrame, prefix: str, labels: Sequence[str],
                   model: Optional[Mapping[str, Any]]) -> dict[str, pl.Series]:
    """The per-model output columns (`<prefix>_label` ...) for EVERY panel row (nulls where no posterior)."""
    n = panel.height
    if model is None:
        return empty_state_columns(n, prefix, labels)
    feats = model["features"]
    X = panel.select(feats).to_numpy()
    sc = model["scaler"]
    Z = apply_scaler(X, np.array(sc["center"]), np.array(sc["scale"]), sc["clip"])
    post = forward_filter(Z, np.array(model["startprob"]), np.array(model["transmat"]),
                          np.array(model["means"]), np.array(model["vars"]))
    valid = ~np.isnan(post).any(axis=1)
    k = post.shape[1]
    safe = np.where(valid[:, None], post, 0.0)
    idx = safe.argmax(axis=1)
    values = np.linspace(-1.0, 1.0, k)                          # state position: -1 (first label) .. +1 (last)
    labs = model["labels"]

    def col(a: np.ndarray, dtype=pl.Float64) -> pl.Series:
        return pl.Series([None if not v else float(x) for x, v in zip(a, valid)], dtype=dtype)

    cols: dict[str, pl.Series] = {
        f"{prefix}_label": pl.Series([labs[i] if v else None for i, v in zip(idx, valid)], dtype=pl.String),
        f"{prefix}_probability": col(safe.max(axis=1)),
        f"{prefix}_score": col(safe @ values),
    }
    for s, lab in enumerate(labs):
        cols[f"{prefix}_post_{lab}"] = col(safe[:, s])
    cols[f"{prefix}_short_history"] = pl.Series([bool(model["short_history"]) if v else None for v in valid], dtype=pl.Boolean)
    fit_d = date.fromisoformat(model["fit_date"])
    cols[f"{prefix}_model_fit_date"] = pl.Series([fit_d if v else None for v in valid], dtype=pl.Date)
    return cols


# ═══ Persistence ═════════════════════════════════════════════════════════════

def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(text)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def save_json(path: Path, obj: Mapping[str, Any]) -> None:
    atomic_write_text(path, json.dumps(obj, indent=1))


def load_model_file(path: Path, schema: int, name: str, error_cls: Type[Exception]) -> Optional[dict]:
    """The persisted model at `path`, or None when absent. A file whose schema/name differ from what
    the caller expects raises `error_cls` — a stale or foreign model must never silently score."""
    if not path.exists():
        return None
    m = json.loads(path.read_text())
    if m.get("schema") != schema or m.get("domain") != name:
        raise error_cls(f"{path}: unexpected model schema/domain ({m.get('schema')!r}, {m.get('domain')!r})")
    return m


def append_store(existing: Optional[pl.DataFrame], rows: pl.DataFrame, path: Path,
                 error_cls: Type[Exception], key: str = "date") -> int:
    """Append `rows` (already restricted to dates after the stored maximum) to the append-only store
    at `path` and write it atomically. A schema that differs from the stored one raises: the store
    cannot be migrated in place. Returns the number of rows added."""
    n_new = rows.height
    if existing is not None:
        if existing.columns != rows.columns or existing.schema != rows.schema:
            raise error_cls(
                f"{path} has a different schema than this build produces — the store is "
                f"append-only and cannot be migrated in place; archive it and let the fit job rebuild it")
        rows = pl.concat([existing, rows])
    atomic_write_parquet(rows.sort(key), path)
    return n_new
