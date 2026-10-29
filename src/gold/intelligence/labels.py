"""
labels.py — ADR-053 D3: how the weekly fit jobs get their training labels.

The label constructor reads bars AFTER the decision date by design (ADR-052). Its module is
pinned out of every pipeline job and Gold module (the ADR-052 isolation
pin in tests/integration), so a fit job cannot import it. The hand-off is a
Parquet file (GD §17.6: Parquet is the message-passing medium between components):

    the ADR-052 labeling script in scripts/ with  --grid-all --years 15 --publish
        -> data/training/labels/labels_<tag>.parquet        (offline, operator-run)
    fit jobs  -> this module  -> read-only, validated

The reader REFUSES (typed errors, never a silent fallback) a label file that
    - does not exist                                  -> LabelsMissing
    - holds more than one `label_spec`, or one that differs from config/intelligence.yaml
                                                      -> LabelSpecMismatch
    - lacks a required column                         -> LabelSchemaError
    - has a newest label_end_date older than `max_age_days` before the fit date
                                                      -> LabelsStale
A fit job turns any of these into a logged, recorded SKIP (the previous model keeps scoring,
visible through its age) — a stale or foreign label set must never train a model.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import polars as pl

from src.gold.intelligence.core import IntelligenceConfig, IntelligenceError

REQUIRED = ("symbol", "market", "decision_date", "direction", "label_binary", "label_end_date", "label_spec")
DIRECTIONS = ("long", "short")


class LabelInputError(IntelligenceError):
    """Base: the label set cannot be used for a fit."""


class LabelsMissing(LabelInputError):
    pass


class LabelsStale(LabelInputError):
    pass


class LabelSpecMismatch(LabelInputError):
    pass


class LabelSchemaError(LabelInputError):
    pass


def latest_label_file(labels_dir: Path) -> Path:
    files = sorted(Path(labels_dir).glob("labels_*.parquet"), key=lambda p: (p.stat().st_mtime, p.name))
    if not files:
        raise LabelsMissing(f"no labels_*.parquet in {labels_dir} — run the ADR-052 labeling script "
                            f"(scripts/, flags --grid-all --years 15 --publish)")
    return files[-1]


def load_training_labels(cfg: IntelligenceConfig, fit_date: date, labels_dir: Optional[Path] = None) -> pl.DataFrame:
    """Validated label frame: REQUIRED columns only, label_binary as Int8, directions lower-case."""
    path = latest_label_file(cfg.label_dir if labels_dir is None else labels_dir)
    lab = pl.read_parquet(path)
    missing = [c for c in REQUIRED if c not in lab.columns]
    if missing:
        raise LabelSchemaError(f"{path.name}: missing columns {missing}")
    if lab.height == 0:
        raise LabelsMissing(f"{path.name}: no rows")
    specs = lab["label_spec"].unique().to_list()
    if specs != [cfg.label_spec]:
        raise LabelSpecMismatch(f"{path.name}: label_spec {specs} != configured {cfg.label_spec!r}")
    bad_dir = set(lab["direction"].unique().to_list()) - set(DIRECTIONS)
    if bad_dir:
        raise LabelSchemaError(f"{path.name}: unexpected direction values {sorted(bad_dir)}")
    newest = lab["label_end_date"].max()
    if newest < fit_date - timedelta(days=cfg.label_max_age_days):
        raise LabelsStale(f"{path.name}: newest label_end_date {newest} is older than "
                          f"{cfg.label_max_age_days} days before {fit_date} — relabel before fitting")
    if lab.select(pl.col("label_binary").is_in([0, 1]).all()).item() is False:
        raise LabelSchemaError(f"{path.name}: label_binary holds values other than 0/1")
    return lab.select(*REQUIRED).with_columns(pl.col("label_binary").cast(pl.Int8))
