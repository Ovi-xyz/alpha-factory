"""
src/utils/quality_baselines.py — GHR-06 (ADR-059, GMI-GHR-REC-001)

Baseline-aware severity for a WARNING-tier quality check that has a KNOWN, STABLE non-zero count.

Problem (ADR-059 GK1): `context_gap_detection` was the only failing check on 16/17/18 Oct, with the
same detail each day ("72 isolated Layer 2 gaps > 5 calendar days, 17 peer-confirmed closures
excluded"). Warning-tier => Silver raw `warn` => Gold inherits it through the cascade, so the gate
could not reach `pass` — and a chronic red also hides the day a NEW gap appears. XAE-R05 (calendar
oracle) removes the false positives; this removes the remaining blind spot: a stable known count is
`info`, anything that moves is `warn`.

Rule (severity_effective), given an ACTIVE baseline:
    count > baseline.count            -> warn   (the problem got bigger)
    any gap not in the previous run   -> warn   (a NEW isolated gap, even if the count is unchanged)
    otherwise, count > 0              -> info   (stable, known; passes, recorded for the audit trail)
    otherwise                         -> ok
With NO active baseline the legacy rule applies unchanged: warn iff count > legacy_threshold (50).

A baseline is a decision, not a default, so it is never implicit:
    config/quality_baselines.yaml  ->  baselines: {<check>: {count, date, justification, expires}}
All four fields are required (a half-specified entry is IGNORED with a WARNING — never half-active),
and an expired baseline is IGNORED with a WARNING, so a chronic red can never hide a new problem
indefinitely. The shipped file holds no baseline: the number must be calibrated on live Silver after
XAE-R05, which the development sandbox cannot do.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml
from loguru import logger

BASELINES_PATH = Path("config/quality_baselines.yaml")
REQUIRED_FIELDS = ("count", "date", "justification", "expires")


@dataclass(frozen=True)
class Baseline:
    check: str
    count: int
    date: date
    justification: str
    expires: date


def _as_date(v) -> date | None:
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v))
    except ValueError:
        return None


def load_baseline(check: str, as_of: date, path: Path | None = None) -> Baseline | None:
    """The ACTIVE baseline for `check` on `as_of`, else None (absent / malformed / expired)."""
    p = Path(path) if path is not None else BASELINES_PATH
    if not p.exists():
        return None
    try:
        raw = (yaml.safe_load(p.read_text()) or {}).get("baselines") or {}
    except (OSError, yaml.YAMLError) as e:
        logger.warning(f"[QualityBaselines] {p} unreadable ({e}) — no baseline active")
        return None
    entry = raw.get(check)
    if not entry:
        return None
    missing = [f for f in REQUIRED_FIELDS if entry.get(f) in (None, "")]
    count, born, expires = entry.get("count"), _as_date(entry.get("date")), _as_date(entry.get("expires"))
    if missing or isinstance(count, bool) or not isinstance(count, int) or count < 0 \
            or born is None or expires is None or expires < born:
        logger.warning(f"[QualityBaselines] baseline for {check!r} is incomplete or invalid "
                       f"(missing={missing}) — IGNORED, legacy threshold applies")
        return None
    if as_of > expires:
        logger.warning(f"[QualityBaselines] baseline for {check!r} EXPIRED on {expires} — IGNORED; "
                       f"re-calibrate and re-date it")
        return None
    return Baseline(check, count, born, str(entry["justification"]), expires)


def gap_severity(
    isolated_count: int,
    isolated_keys: list[str] | None,
    prev_keys: list[str] | None,
    baseline: Baseline | None,
    legacy_threshold: int = 50,
) -> tuple[str, list[str]]:
    """(severity_effective in {'ok','info','warn'}, new_keys). See the module docstring.

    `isolated_keys` None = identities unavailable; `prev_keys` None = no previous evaluated run.
    Neither is ever guessed: with no identities or no previous run nothing counts as "new"."""
    new_keys = sorted(set(isolated_keys) - set(prev_keys)) if isolated_keys is not None and prev_keys is not None else []
    if baseline is None:
        return ("warn" if isolated_count > legacy_threshold else "ok"), new_keys
    if isolated_count > baseline.count or new_keys:
        return "warn", new_keys
    return ("info" if isolated_count > 0 else "ok"), new_keys
