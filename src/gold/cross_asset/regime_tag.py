"""
src/gold/cross_asset/regime_tag.py — the CrossAssetEngine's own regime tag (ADR-055 v2, R3).

The three weekly CrossAssetEngine modules (correlation, lead-lag, forecast) stamp every output row
with a `regime` string. That string is a TAG — it labels the market state the run happened in; it
does not condition the estimate (see each module's docstring). It used to be read from the retired
rule-based `regime_store.parquet` (5-state vocabulary). It is now the engine's OWN label:
`global_regime_label` from `data/gold/cross_asset/global_regime.parquet` (GlobalIndexRegimeModule,
daily; vocabulary RISK_ON / RISK_OFF / MIXED).

WHY NOT THE HMM CONTEXT REGIME. The cross-asset engine is one of three independent intelligence
domains; reading the HMM engine's context_regime would couple them and feed HMM information into
cross_asset_xgboost (double counting inside decision_xgboost) — ADR-051 Amendment 2 §A, ADR-055 F2.
Tagging with its own domain's label keeps the engine closed over its own stores.

ONE IMPLEMENTATION. The three modules previously carried three byte-identical copies of the
reader (the GAP-1 / GAP-6 "second copy is a drift risk" pattern); they now share this one.

VOCABULARY CHANGE. Rows written before the cutover carry the 5-state tags (RISK_ON, RISK_OFF,
STAGFLATION, REFLATION, DISINFLATION); rows written after carry RISK_ON / RISK_OFF / MIXED or
UNKNOWN. The three stores are overwritten each weekly run, so no file mixes the two vocabularies
(ADR-055 v1 R3 required checking that no consumer partitions on the old tag: none does — see
tests/unit/test_cross_asset_regime_tag.py::test_no_src_consumer_filters_on_legacy_regime_tags).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import polars as pl
from loguru import logger

GLOBAL_REGIME_PATH = Path("data/gold/cross_asset/global_regime.parquet")
UNKNOWN = "UNKNOWN"


def current_regime_tag(run_date: date, path: Optional[Path] = None) -> str:
    """
    The latest `global_regime_label` on or before run_date, or "UNKNOWN" when the store is
    missing, empty before run_date, unreadable, or its latest label is null.
    """
    path = Path(path) if path is not None else GLOBAL_REGIME_PATH
    if not path.exists():
        return UNKNOWN
    try:
        df = pl.read_parquet(path, columns=["date", "global_regime_label"])
        # `date` is stored as an ISO string in this store (a Date would compare identically
        # once cast); compare as strings so both shapes work.
        prior = (df.filter(pl.col("date").cast(pl.Utf8) <= run_date.isoformat())
                   .sort(pl.col("date").cast(pl.Utf8), descending=True))
        if prior.is_empty():
            return UNKNOWN
        label = prior.row(0, named=True)["global_regime_label"]
        return label if label else UNKNOWN
    except Exception as e:
        logger.debug(f"[cross_asset] Could not read global regime tag: {e}")
        return UNKNOWN
