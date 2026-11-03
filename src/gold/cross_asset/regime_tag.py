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

STALENESS GUARD (XAE-R01, cross_asset_recalibration_guide G7). The reader used to return the newest
label however old it was, so any run date after the last written row read a stale RISK_OFF as current.
It now also takes `max_age_days` (default MAX_TAG_AGE_DAYS = 5, the same tolerance ADR-051 A2 gives
`context_regime`'s market side): when the newest row's `as_of_bar_date` (the trade date of the latest
bar the label was computed from; legacy rows without the column fall back to `date`) is older than
that, the tag is UNKNOWN and a WARNING is logged. The public return type stays `str`
(UNKNOWN is the "no usable label" value every caller already handles).

WHAT THE LABEL MEANS (guide G6). `global_regime_label` is a VOLATILITY conditioner, not a direction
signal: on the SPX proxy, 5-day forward P(up) is 0.608 / 0.605 / 0.616 for RISK_ON / MIXED / RISK_OFF
while the standard deviation of the forward return is 1.64 / 2.23 / 3.82%. Nothing may encode
RISK_OFF as a monotone bearish feature (guide section 9).

VOCABULARY CHANGE. Rows written before the cutover carry the 5-state tags (RISK_ON, RISK_OFF,
STAGFLATION, REFLATION, DISINFLATION); rows written after carry RISK_ON / RISK_OFF / MIXED or
UNKNOWN. The three stores are overwritten each weekly run, so no file mixes the two vocabularies
(ADR-055 v1 R3 required checking that no consumer partitions on the old tag: none does — see
tests/unit/test_cross_asset_regime_tag.py::test_no_src_consumer_filters_on_legacy_regime_tags).
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Optional

import polars as pl
from loguru import logger

GLOBAL_REGIME_PATH = Path("data/gold/cross_asset/global_regime.parquet")
UNKNOWN = "UNKNOWN"
MAX_TAG_AGE_DAYS: int = 5   # [U] XAE-R01 — matches context_regime's 5-day tolerance (ADR-051 A2)


def _to_date(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def current_regime_tag(
    run_date: date,
    path: Optional[Path] = None,
    max_age_days: Optional[int] = None,
) -> str:
    """
    The latest `global_regime_label` on or before run_date, or "UNKNOWN" when the store is
    missing, empty before run_date, unreadable, its latest label is null, or (XAE-R01) the newest
    row is more than `max_age_days` calendar days older than run_date (default MAX_TAG_AGE_DAYS;
    pass a negative number to disable the guard). A stale label logs a WARNING and returns UNKNOWN.
    """
    path = Path(path) if path is not None else GLOBAL_REGIME_PATH
    limit = MAX_TAG_AGE_DAYS if max_age_days is None else max_age_days
    if not path.exists():
        return UNKNOWN
    try:
        schema_cols = pl.read_parquet_schema(path).keys()
        cols = ["date", "global_regime_label"] + (
            ["as_of_bar_date"] if "as_of_bar_date" in schema_cols else [])
        df = pl.read_parquet(path, columns=cols)
        # `date` is stored as an ISO string in this store (a Date would compare identically
        # once cast); compare as strings so both shapes work.
        # Two rows can share a `date` (a same-day re-run after Silver advanced one bar): newest bar wins.
        keys = [pl.col("date").cast(pl.Utf8)] + (
            [pl.col("as_of_bar_date")] if "as_of_bar_date" in cols else [])
        prior = (df.filter(pl.col("date").cast(pl.Utf8) <= run_date.isoformat())
                   .sort(keys, descending=True, nulls_last=True))
        if prior.is_empty():
            return UNKNOWN
        row = prior.row(0, named=True)
        label = row["global_regime_label"]
        if not label:
            return UNKNOWN
        if limit >= 0:
            basis = _to_date(row.get("as_of_bar_date")) or _to_date(row["date"])
            if basis is not None and (run_date - basis).days > limit:
                logger.warning(
                    f"[cross_asset] global regime tag is stale: newest label {label!r} is from bar "
                    f"{basis} ({(run_date - basis).days} days before {run_date}, limit {limit}) "
                    f"-> tagging UNKNOWN"
                )
                return UNKNOWN
        return label
    except Exception as e:
        logger.debug(f"[cross_asset] Could not read global regime tag: {e}")
        return UNKNOWN
