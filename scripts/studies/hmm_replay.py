"""
scripts/studies/hmm_replay.py — ADR-058 HMM-R06: forward-filter replay harness. OFFLINE UTILITY.

Not a pipeline job and not imported from `src/` (pinned by
tests/integration/test_hmm_update_adr058.py, in the style of the ADR-052 labeler isolation). It imports
NOTHING from `src` on purpose: it is a SECOND, independent implementation of the causal forward filter
that production runs (src/gold/hmm/hmm_core.py), so that
    (1) a stored regime table can be reproduced from the persisted model JSON alone — the reproduction
        gate: max |posterior difference| <= 1e-3 and 100% label agreement — and
    (2) the sensitivity of the labels to the AGE of one input (HMM-R03's problem: a live VIX that is 3-10
        days old against a ~1-day lag in the training window) can be measured by replacing only that
        feature with a lagged reference series and filtering again, parameters held fixed.

Scope limits (the guide's §10, restated so the output is not over-read):
  * the replay varies ONE feature; every other feature stays as stored;
  * parameters are the stored (in-sample) fit; lags are fixed, not the real weekly sawtooth;
  * the reference series (e.g. Silver ^VIX close) is an ideal, not ground truth;
  * no SQL, nothing is written unless --out is given.

CLI
    python scripts/studies/hmm_replay.py --model data/gold/hmm/models/market.json \
        --panel data/gold/hmm/raw_market.parquet --store data/gold/hmm/market_regime.parquet --prefix market \
        [--feature VIXCLS --reference vix_silver.parquet --lags 1 3 5 7 10]
    A reference parquet has columns `date` and `value`. Exit status 1 when the reproduction gate fails.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import polars as pl

KEY = "as_of_date"
GATE_MAX_ABS_DIFF = 1e-3
GATE_LABEL_AGREEMENT = 1.0


# ═══ the filter (independent of src.gold.hmm.hmm_core) ═══════════════════════

def load_model(path: Path | str) -> dict:
    return json.loads(Path(path).read_text())


def _log_emission(z: np.ndarray, means: np.ndarray, var: np.ndarray) -> np.ndarray:
    """log N(z | mean_k, diag(var_k)) per state over the OBSERVED dimensions (diagonal covariance:
    exact marginalisation of the missing ones). Nothing observed -> zeros."""
    obs = ~np.isnan(z)
    if not obs.any():
        return np.zeros(means.shape[0])
    d = z[obs] - means[:, obs]
    return -0.5 * np.sum(np.log(2.0 * np.pi * var[:, obs]) + d * d / var[:, obs], axis=1)


def forward_filter(model: dict, X: np.ndarray) -> np.ndarray:
    """P(state_t | x_1..x_t), shape (T, K), from the model JSON alone. Rows before the first observed
    row are NaN; a later row with nothing observed is a pure prediction step (alpha @ A)."""
    sc = model["scaler"]
    Z = np.clip((X - np.array(sc["center"])) / np.array(sc["scale"]), -sc["clip"], sc["clip"])
    A, pi = np.array(model["transmat"]), np.array(model["startprob"])
    mu, var = np.array(model["means"]), np.array(model["vars"])
    post = np.full((Z.shape[0], len(pi)), np.nan)
    alpha: Optional[np.ndarray] = None
    for t in range(Z.shape[0]):
        if alpha is None:
            if np.isnan(Z[t]).all():
                continue
            prior = pi
        else:
            prior = alpha @ A
        ll = _log_emission(Z[t], mu, var)
        w = prior * np.exp(ll - ll.max())
        s = w.sum()
        alpha = w / s if s > 0 and np.isfinite(s) else prior
        post[t] = alpha
    return post


def labels_of(model: dict, post: np.ndarray) -> list[Optional[str]]:
    valid = ~np.isnan(post).any(axis=1)
    idx = np.where(valid[:, None], post, 0.0).argmax(axis=1)
    return [model["labels"][i] if v else None for i, v in zip(idx, valid)]


def filter_panel(model: dict, panel: pl.DataFrame, feature_override: Optional[dict[str, np.ndarray]] = None):
    """Filter the whole (date-sorted) panel; optionally replace named feature columns. Returns
    (dates, posteriors, labels)."""
    panel = panel.sort(KEY)
    X = panel.select(model["features"]).to_numpy().astype(float)
    for name, values in (feature_override or {}).items():
        X[:, model["features"].index(name)] = values
    post = forward_filter(model, X)
    return panel[KEY], post, labels_of(model, post)


# ═══ (1) reproduction gate ═══════════════════════════════════════════════════

@dataclass(frozen=True)
class Reproduction:
    n_rows: int
    max_abs_diff: float
    label_agreement: float

    @property
    def passed(self) -> bool:
        return (self.n_rows > 0 and self.max_abs_diff <= GATE_MAX_ABS_DIFF
                and self.label_agreement >= GATE_LABEL_AGREEMENT)


def reproduce(model: dict, panel: pl.DataFrame, stored: pl.DataFrame, prefix: str) -> Reproduction:
    """Replay the model over the panel and compare with the stored `<prefix>_post_<label>` columns and
    `<prefix>_label`, on every stored date that the panel also has. Restrict `stored` beforehand to
    compare a subset (e.g. only the rows written by one model fit)."""
    dates, post, labs = filter_panel(model, panel)
    replay = pl.DataFrame({"date": dates, "label": labs}).with_columns(
        [pl.Series(f"p_{lab}", post[:, i]) for i, lab in enumerate(model["labels"])])
    cols = ["date", f"{prefix}_label", *[f"{prefix}_post_{lab}" for lab in model["labels"]]]
    j = stored.select(cols).join(replay, on="date", how="inner")
    if j.height == 0:
        return Reproduction(0, float("nan"), float("nan"))
    diffs = [float(np.nanmax(np.abs(j[f"{prefix}_post_{lab}"].to_numpy() - j[f"p_{lab}"].to_numpy())))
             for lab in model["labels"]]
    agree = float((j[f"{prefix}_label"] == j["label"]).fill_null(False).mean())
    return Reproduction(j.height, max(diffs), agree)


# ═══ (2) lag sensitivity ═════════════════════════════════════════════════════

def lagged_values(reference: pl.DataFrame, as_of: pl.Series, lag_days: int) -> np.ndarray:
    """For each as-of date, the latest reference `value` whose `date` <= as_of - lag_days
    (calendar days; NaN when none)."""
    left = pl.DataFrame({"t": as_of - timedelta(days=lag_days), "i": np.arange(len(as_of))}).sort("t")
    ref = reference.select(pl.col("date").alias("t"), pl.col("value")).sort("t")
    j = left.join_asof(ref, on="t", strategy="backward").sort("i")
    return j["value"].cast(pl.Float64).fill_null(float("nan")).to_numpy()


def entry_delays(base: Sequence[Optional[str]], lagged: Sequence[Optional[str]], target: str,
                 max_match: int = 15) -> tuple[list[int], int]:
    """Entries into `target` in the baseline vs the lagged run: for each baseline entry the signed row
    offset (lagged - base) of the NEAREST lagged entry within +/- max_match rows. Returns
    (offsets, n_unmatched)."""
    def entries(labels):
        return [i for i in range(len(labels)) if labels[i] == target and (i == 0 or labels[i - 1] != target)]
    lag_entries = entries(lagged)
    offsets, unmatched = [], 0
    for b in entries(base):
        near = [e for e in lag_entries if abs(e - b) <= max_match]
        if not near:
            unmatched += 1
        else:
            offsets.append(min(near, key=lambda e: (abs(e - b), e)) - b)
    return offsets, unmatched


def lag_sensitivity(model: dict, panel: pl.DataFrame, feature: str, reference: pl.DataFrame,
                    lags: Sequence[int], baseline_lag: int = 1, target: str = "risk_off") -> pl.DataFrame:
    """One row per lag: share of rows whose label differs from the baseline lag's, and the
    `target`-entry delay statistics. Only `feature` is replaced; parameters stay as stored."""
    panel = panel.sort(KEY)
    dates = panel[KEY]
    _, _, base = filter_panel(model, panel, {feature: lagged_values(reference, dates, baseline_lag)})
    rows = []
    for lag in lags:
        _, _, lab = filter_panel(model, panel, {feature: lagged_values(reference, dates, lag)})
        pairs = [(a, b) for a, b in zip(base, lab) if a is not None and b is not None]
        changed = float(np.mean([a != b for a, b in pairs])) if pairs else float("nan")
        offs, unmatched = entry_delays(base, lab, target)
        rows.append({"lag_days": lag, "share_labels_changed": changed, "n_entries_matched": len(offs),
                     "n_entries_unmatched": unmatched,
                     "mean_entry_delay_rows": float(np.mean(offs)) if offs else float("nan"),
                     "p90_entry_delay_rows": float(np.percentile(offs, 90)) if offs else float("nan")})
    return pl.DataFrame(rows)


# ═══ CLI ═════════════════════════════════════════════════════════════════════

def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("CLI")[0].strip().splitlines()[0])
    ap.add_argument("--model", required=True)
    ap.add_argument("--panel", required=True)
    ap.add_argument("--store", required=True)
    ap.add_argument("--prefix", required=True, help="market, or the macro domain name")
    ap.add_argument("--feature")
    ap.add_argument("--reference")
    ap.add_argument("--lags", type=int, nargs="*", default=[1, 3, 5, 7, 10])
    ap.add_argument("--out")
    a = ap.parse_args(argv)
    model, panel, stored = load_model(a.model), pl.read_parquet(a.panel), pl.read_parquet(a.store)
    rep = reproduce(model, panel, stored, a.prefix)
    print(f"reproduction: rows={rep.n_rows} max_abs_diff={rep.max_abs_diff:.2e} "
          f"label_agreement={rep.label_agreement:.4f} -> {'PASS' if rep.passed else 'FAIL'}")
    if a.feature and a.reference:
        table = lag_sensitivity(model, panel, a.feature, pl.read_parquet(a.reference), a.lags)
        print(table)
        if a.out:
            table.write_parquet(a.out)
    return 0 if rep.passed else 1


if __name__ == "__main__":
    sys.exit(main())
