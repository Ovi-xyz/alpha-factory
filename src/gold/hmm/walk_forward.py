"""
walk_forward.py — ADR-053 (GMI-XGB-001) D2: honest as-of regime history for `hmm_xgboost`.

WHY THIS MODULE EXISTS
    The production stores (`macro_regime.parquet`, `market_regime.parquet`) are filtered with
    the CURRENT model, fitted on the whole window. Measured 26 Oct 2026: macro 7,862 of 7,862
    rows `in_sample`, market 2,541 of 2,542 (ADR-051 A2 §E). The filter itself never looks
    ahead, but the model PARAMETERS (means, variances, transition matrix, scaler) were
    estimated on days that come after the row. A supervised model trained on those rows would
    learn a regime signal that never existed out-of-sample (ADR-053 / ADR-054 forbid exactly
    that). This module rebuilds the history the way production would have produced it:

        expanding window, refit at every `refit_months` boundary;
        the model used for a row dated D was fitted on panel rows strictly BEFORE the fold
        start F <= D; the posterior is the same causal forward filter production runs.

    Everything is reused from the audited engine (hmm_core.fit_model / state_columns, the
    macro/market `build_rows`, the §F macro-label rule) — no second implementation of any of
    it. Only the fold loop and the stitching are new.

STITCHING SEMANTICS (matches the append-only production store)
    The macro label rule is stateful (momentum looks 63 weekdays back, a label inside the band
    carries). Folds are therefore processed in order and each fold's `add_macro_label` sees the
    previous folds' stored activity and last label — exactly what the daily job sees.

INCREMENTAL
    A completed fold's rows never change (its model is fitted on a frozen past; the panel rows
    it scores are PIT-gated). Re-runs keep every stored fold except the last (open) one, which
    is recomputed because new days extend it. A changed config fingerprint or processing
    version rebuilds everything (logged).

CADENCE VS PRODUCTION
    Production refits every Sunday; this reconstruction refits every `refit_months` months
    (default 3). Parameter drift between the two is second-order; the point is that no row is
    scored by a model that saw its own future. Stated, not hidden: KNOWN_RISKS RISK-40.

DOMAIN INDEPENDENCE (ADR-051 A2 §A)
    Reads only raw_macro / raw_market and writes its own two stores under data/gold/hmm/. Job
    `gold_hmm_walk_forward` is an HMM-domain job; the downstream `hmm_xgboost` reads the stores
    as Parquet only and never imports this package.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Optional

import numpy as np
import polars as pl
import yaml
from loguru import logger

from src.gold.hmm import hmm_macro_regime as macro_mod
from src.gold.hmm import hmm_market_regime as market_mod
from src.gold.hmm import raw_macro, raw_market
from src.gold.hmm.config import (
    MacroModelConfig, MarketModelConfig, macro_model_config, market_model_config,
)
from src.gold.hmm.hmm_core import fit_model
from src.utils.atomic_io import atomic_write_parquet

PROCESSING_VERSION = "1.0"
WF_DIR = Path("data/gold/hmm/walk_forward")
WF_MACRO_PATH = WF_DIR / "macro_regime_wf.parquet"
WF_MARKET_PATH = WF_DIR / "market_regime_wf.parquet"
CONFIG_PATH = Path("config/hmm_walk_forward.yaml")
RAW_KEY = "as_of_date"

_CONFIG_KEYS = frozenset({"refit_months", "macro_first_fold", "market_min_train_obs"})


class WalkForwardError(RuntimeError):
    """Config / data defect that makes the walk-forward history unbuildable."""


@dataclass(frozen=True)
class WalkForwardConfig:
    refit_months: int            # fold length; folds start on calendar month boundaries
    macro_first_fold: date       # first macro fold start (long history -> a fixed date, not a count)
    market_min_train_obs: int    # market panel is short: first fold = first boundary with this many complete rows behind it


def validate_config(raw: Mapping[str, Any]) -> WalkForwardConfig:
    if not isinstance(raw, Mapping):
        raise WalkForwardError("hmm_walk_forward config must be a mapping")
    unknown = set(raw) - _CONFIG_KEYS
    missing = _CONFIG_KEYS - set(raw)
    if unknown or missing:
        raise WalkForwardError(f"hmm_walk_forward config keys: unknown={sorted(unknown)} missing={sorted(missing)}")
    months = raw["refit_months"]
    if not isinstance(months, int) or isinstance(months, bool) or months not in (1, 2, 3, 4, 6, 12):
        raise WalkForwardError(f"refit_months must divide a year (1,2,3,4,6,12), got {months!r}")
    first = raw["macro_first_fold"]
    if isinstance(first, str):
        first = date.fromisoformat(first)
    if not isinstance(first, date):
        raise WalkForwardError("macro_first_fold must be an ISO date")
    n = raw["market_min_train_obs"]
    if not isinstance(n, int) or isinstance(n, bool) or n < 60:
        raise WalkForwardError(f"market_min_train_obs must be an int >= 60, got {n!r}")
    return WalkForwardConfig(refit_months=months, macro_first_fold=first, market_min_train_obs=n)


def load_config(path: Path | str | None = None) -> WalkForwardConfig:
    with open(CONFIG_PATH if path is None else path) as fh:
        return validate_config(yaml.safe_load(fh))


# ═══ Fold calendar ═══════════════════════════════════════════════════════════

def fold_starts(first: date, last: date, months: int) -> list[date]:
    """Month-boundary fold starts: `first` snapped DOWN to a `months`-boundary, then every
    `months` months up to `last` inclusive."""
    d = date(first.year, ((first.month - 1) // months) * months + 1, 1)
    out: list[date] = []
    while d <= last:
        out.append(d)
        m = d.month + months
        d = date(d.year + (m - 1) // 12, (m - 1) % 12 + 1, 1)
    return out


def _first_market_fold(panel: pl.DataFrame, feats: list[str], months: int, min_obs: int) -> Optional[date]:
    cc = panel.filter(pl.all_horizontal([pl.col(f).is_not_null() for f in feats])).sort(RAW_KEY)
    if cc.height < min_obs:
        return None
    start = cc[RAW_KEY][min_obs - 1]                     # the date by which min_obs complete rows exist
    candidates = fold_starts(start, date(2100, 1, 1), months)[:2]
    return next((c for c in candidates if c > start), None)


# ═══ Macro ═══════════════════════════════════════════════════════════════════

def _macro_models(panel: pl.DataFrame, fold: date, cfg: MacroModelConfig, run_date: date) -> dict[str, Optional[dict]]:
    train = panel.filter(pl.col(RAW_KEY) < fold)
    models: dict[str, Optional[dict]] = {}
    for domain in cfg.domains:
        feats = macro_mod._feats(cfg, domain)
        macro_mod._require_columns(panel, feats, domain)
        w = np.array([cfg.weights[domain][f] for f in feats], dtype=float)
        record, status = fit_model(train, RAW_KEY, domain, feats, w, cfg.labels[domain], cfg.fit,
                                   cfg.fingerprint(), fold, macro_mod.PROCESSING_VERSION, macro_mod.MODEL_SCHEMA)
        if record is None:
            logger.debug(f"[hmm_wf] macro {domain} fold {fold}: {status}")
        models[domain] = record
    return models


def build_macro(panel: pl.DataFrame, cfg: MacroModelConfig, wf: WalkForwardConfig, run_date: date,
                keep: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Walk-forward macro rows (all folds >= macro_first_fold not already in `keep`), stitched
    after `keep`. Returns keep + new rows, sorted by date."""
    panel = panel.filter(pl.col(RAW_KEY) <= run_date).sort(RAW_KEY)
    starts = fold_starts(wf.macro_first_fold, run_date, wf.refit_months)
    resume = keep["fold_start"].max() if keep is not None and keep.height else None
    todo = [s for s in starts if resume is None or s > resume]
    history = (keep.select("date", "macro_activity") if keep is not None and keep.height
               else pl.DataFrame(schema={"date": pl.Date, "macro_activity": pl.Float64}))
    seed = keep.sort("date")["macro_label"][-1] if keep is not None and keep.height else None
    parts = [keep] if keep is not None and keep.height else []

    for i, fold in enumerate(todo):
        nxt = todo[i + 1] if i + 1 < len(todo) else None
        models = _macro_models(panel, fold, cfg, run_date)
        if all(m is None for m in models.values()):
            logger.warning(f"[hmm_wf] macro fold {fold}: no model could be fitted — fold skipped")
            continue
        scored = panel.filter(pl.col(RAW_KEY) < nxt) if nxt is not None else panel
        rows = macro_mod.build_rows(scored, models, cfg).filter(pl.col("date") >= fold)
        if nxt is not None:
            rows = rows.filter(pl.col("date") < nxt)
        if rows.height == 0:
            continue
        rows = macro_mod.add_macro_label(rows, history, seed, cfg.label_rule)
        train_ends = [date.fromisoformat(m["train_end"]) for m in models.values() if m is not None]
        rows = rows.with_columns(
            pl.lit(fold).alias("fold_start"),
            pl.lit(max(train_ends)).alias("wf_train_end"),
            pl.lit(run_date).alias("computation_date"),
            pl.lit(PROCESSING_VERSION).alias("processing_version"),
            pl.lit(cfg.fingerprint()).alias("model_config_fingerprint"),
        )
        if bool(rows["in_sample"].any()):                      # a fold row at/after F can never be in-sample
            raise WalkForwardError(f"macro fold {fold}: a scored row lies inside its own training window")
        parts.append(rows)
        history = pl.concat([history, rows.select("date", "macro_activity")]).unique(subset=["date"], keep="last")
        seed = rows.sort("date")["macro_label"][-1]
    if not parts:
        return pl.DataFrame()
    return pl.concat(parts, how="diagonal_relaxed").sort("date")


# ═══ Market ══════════════════════════════════════════════════════════════════

def build_market(panel: pl.DataFrame, cfg: MarketModelConfig, wf: WalkForwardConfig, run_date: date,
                 keep: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    panel = panel.filter(pl.col(RAW_KEY) <= run_date).sort(RAW_KEY)
    feats = cfg.feature_names()
    market_mod._require_columns(panel, feats)
    first = _first_market_fold(panel, feats, wf.refit_months, wf.market_min_train_obs)
    if first is None:
        logger.warning(f"[hmm_wf] market: fewer than {wf.market_min_train_obs} complete rows — nothing built")
        return pl.DataFrame()
    starts = fold_starts(first, run_date, wf.refit_months)
    resume = keep["fold_start"].max() if keep is not None and keep.height else None
    todo = [s for s in starts if resume is None or s > resume]
    w = np.array([cfg.weights[f] for f in feats], dtype=float)
    parts = [keep] if keep is not None and keep.height else []

    for i, fold in enumerate(todo):
        nxt = todo[i + 1] if i + 1 < len(todo) else None
        record, status = fit_model(panel.filter(pl.col(RAW_KEY) < fold), RAW_KEY, market_mod.MODEL_NAME, feats, w,
                                   cfg.labels, cfg.fit, cfg.fingerprint(), fold,
                                   market_mod.PROCESSING_VERSION, market_mod.MODEL_SCHEMA)
        if record is None:
            logger.warning(f"[hmm_wf] market fold {fold}: {status} — fold skipped")
            continue
        scored = panel.filter(pl.col(RAW_KEY) < nxt) if nxt is not None else panel
        rows = market_mod.build_rows(scored, record, cfg).filter(pl.col("date") >= fold)
        if nxt is not None:
            rows = rows.filter(pl.col("date") < nxt)
        if rows.height == 0:
            continue
        if bool(rows["in_sample"].any()):
            raise WalkForwardError(f"market fold {fold}: a scored row lies inside its own training window")
        parts.append(rows.with_columns(
            pl.lit(fold).alias("fold_start"),
            pl.lit(date.fromisoformat(record["train_end"])).alias("wf_train_end"),
            pl.lit(run_date).alias("computation_date"),
            pl.lit(PROCESSING_VERSION).alias("processing_version"),
            pl.lit(cfg.fingerprint()).alias("model_config_fingerprint"),
        ))
    if not parts:
        return pl.DataFrame()
    return pl.concat(parts, how="diagonal_relaxed").sort("date")


# ═══ Incremental plumbing ════════════════════════════════════════════════════

def _reusable(path: Path, fingerprint: str) -> Optional[pl.DataFrame]:
    """Stored rows to keep: every completed fold (all but the last) of a store whose fingerprint
    and processing version still match; None -> rebuild from scratch."""
    if not path.exists():
        return None
    stored = pl.read_parquet(path)
    needed = {"date", "fold_start", "model_config_fingerprint", "processing_version"}
    if not needed <= set(stored.columns) or stored.height == 0:
        logger.warning(f"[hmm_wf] {path.name}: unusable store — rebuilding")
        return None
    fps = set(stored["model_config_fingerprint"].unique().to_list())
    vers = set(stored["processing_version"].unique().to_list())
    if fps != {fingerprint} or vers != {PROCESSING_VERSION}:
        logger.warning(f"[hmm_wf] {path.name}: config/version changed (fingerprints {sorted(fps)} -> {fingerprint}) — rebuilding")
        return None
    last = stored["fold_start"].max()
    done = stored.filter(pl.col("fold_start") < last)
    return done if done.height else None


def run(run_date: date) -> None:
    """Job entry point ('gold_hmm_walk_forward' in JOB_REGISTRY)."""
    wf = load_config()
    macro_cfg, market_cfg = macro_model_config(), market_model_config()

    if not Path(raw_macro.RAW_MACRO_PATH).exists() or not Path(raw_market.RAW_MARKET_PATH).exists():
        raise WalkForwardError("raw_macro / raw_market missing — run gold_raw_macro and gold_raw_market first")
    macro_panel = pl.read_parquet(raw_macro.RAW_MACRO_PATH)
    market_panel = pl.read_parquet(raw_market.RAW_MARKET_PATH)

    macro = build_macro(macro_panel, macro_cfg, wf, run_date, _reusable(WF_MACRO_PATH, macro_cfg.fingerprint()))
    market = build_market(market_panel, market_cfg, wf, run_date, _reusable(WF_MARKET_PATH, market_cfg.fingerprint()))
    for df, path, name in ((macro, WF_MACRO_PATH, "macro"), (market, WF_MARKET_PATH, "market")):
        if df.height == 0:
            logger.warning(f"[hmm_wf] {name}: empty — store not written")
            continue
        atomic_write_parquet(df, path)
        logger.info(f"[hmm_wf] {run_date}: {name} {df.height} rows {df['date'].min()}..{df['date'].max()} "
                    f"folds={df['fold_start'].n_unique()} -> {path}")
