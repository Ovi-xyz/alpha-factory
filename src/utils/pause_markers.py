"""
src/utils/pause_markers.py — GHR-09 (ADR-059, GMI-GHR-REC-001)

Deliberate-pause markers for the Gatekeeper.

Problem (ADR-059 GK5/GK7, S2): when an operator pauses Silver/Gold on purpose (chain alignment), the
paused days carry no Silver/Gold sentinels, so the Gatekeeper recomputes every one of them as
``block``. On the first day after the pause any Silver/Gold failure then counts back through the
paused days and emits ``block`` IMMEDIATELY, with no 4-day confirmation — a pause must not be
indistinguishable from an outage. A marker lets the history say ``skipped`` for those days instead.

Storage: ``<data>/.pause/<scope>.json``, one file per paused LAYER scope (bronze | silver | gold):

    {"scope": "silver",
     "periods": [{"from_date": "2026-10-19", "reason": "chain alignment",
                  "created_by": "ovi", "expected_resume": "2026-11-05", "resumed_on": null}]}

``periods`` is a list (a deviation from the ADR-059 sketch of one flat record) because the Gatekeeper
recomputes history from the sentinels on every run: a marker deleted at resume time would turn the
paused days back into ``block`` in the next recomputation. ``--resume`` therefore CLOSES the open
period (``resumed_on``) and keeps it. A date ``d`` is paused iff
``from_date <= d < resumed_on`` (``resumed_on`` null = still paused).

Domain scopes (hmm / technical / cross_asset) are not pausable directly: a domain day is ``skipped``
when any silver/gold layer its chain depends on is paused that day (gatekeeper.py).

Writes are atomic (tempfile + os.replace). Reading is tolerant: a missing or unreadable marker is
"not paused" with a WARNING, never an exception — the Gatekeeper must not die on a damaged marker.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from loguru import logger

from src.utils.data_paths import pause_dir

PAUSABLE_SCOPES = ("bronze", "silver", "gold")


@dataclass(frozen=True)
class PausePeriod:
    from_date: date
    reason: str = ""
    created_by: str = ""
    expected_resume: date | None = None
    resumed_on: date | None = None          # exclusive end; None = still paused

    def covers(self, day: date) -> bool:
        return self.from_date <= day and (self.resumed_on is None or day < self.resumed_on)


def _parse_date(value) -> date | None:
    return date.fromisoformat(value) if value else None


def _marker_path(scope: str, root: Path | None) -> Path:
    return (root or pause_dir()) / f"{scope}.json"


def load_pauses(root: Path | None = None) -> dict[str, list[PausePeriod]]:
    """{scope: [periods]} for every readable marker; unreadable markers are skipped with a WARNING."""
    base = root or pause_dir()
    out: dict[str, list[PausePeriod]] = {}
    if not base.exists():
        return out
    for scope in PAUSABLE_SCOPES:
        path = base / f"{scope}.json"
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text())
            out[scope] = [
                PausePeriod(
                    from_date=date.fromisoformat(p["from_date"]),
                    reason=p.get("reason", ""),
                    created_by=p.get("created_by", ""),
                    expected_resume=_parse_date(p.get("expected_resume")),
                    resumed_on=_parse_date(p.get("resumed_on")),
                )
                for p in raw.get("periods", [])
            ]
        except (OSError, ValueError, KeyError, TypeError) as e:
            logger.warning(f"[PauseMarkers] {path} unreadable ({e}) — treated as not paused")
    return out


def is_paused(pauses: dict[str, list[PausePeriod]], scope: str, day: date) -> bool:
    return any(p.covers(day) for p in pauses.get(scope, ()))


def paused_layers_on(pauses: dict[str, list[PausePeriod]], day: date) -> frozenset[str]:
    return frozenset(s for s in pauses if is_paused(pauses, s, day))


def active_pause(pauses: dict[str, list[PausePeriod]], scope: str, day: date) -> PausePeriod | None:
    for p in pauses.get(scope, ()):
        if p.covers(day):
            return p
    return None


def _write_marker(scope: str, periods: list[dict], root: Path | None) -> None:
    path = _marker_path(scope, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{scope}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump({"scope": scope, "periods": periods}, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def _read_periods(scope: str, root: Path | None) -> list[dict]:
    path = _marker_path(scope, root)
    if not path.exists():
        return []
    return list(json.loads(path.read_text()).get("periods", []))


def _validate(scopes) -> list[str]:
    scopes = list(scopes)
    bad = [s for s in scopes if s not in PAUSABLE_SCOPES]
    if bad or not scopes:
        raise ValueError(f"pausable scopes are {PAUSABLE_SCOPES}; got {scopes}")
    return scopes


def add_pause(scopes, from_date: date, reason: str, created_by: str = "",
              expected_resume: date | None = None, root: Path | None = None) -> None:
    """Open a pause period for each scope. Refuses to open a second period while one is open."""
    for scope in _validate(scopes):
        periods = _read_periods(scope, root)
        if any(p.get("resumed_on") is None for p in periods):
            raise ValueError(f"scope {scope!r} already has an open pause; --resume it first")
        periods.append({
            "from_date": from_date.isoformat(),
            "reason": reason,
            "created_by": created_by,
            "expected_resume": expected_resume.isoformat() if expected_resume else None,
            "resumed_on": None,
        })
        _write_marker(scope, periods, root)


def end_pause(scopes, resumed_on: date, root: Path | None = None) -> None:
    """Close the open period of each scope: days before `resumed_on` stay `skipped` in the history."""
    for scope in _validate(scopes):
        periods = _read_periods(scope, root)
        open_ = [p for p in periods if p.get("resumed_on") is None]
        if not open_:
            raise ValueError(f"scope {scope!r} has no open pause")
        if date.fromisoformat(open_[0]["from_date"]) > resumed_on:
            raise ValueError(f"resume date {resumed_on} is before the pause start {open_[0]['from_date']}")
        open_[0]["resumed_on"] = resumed_on.isoformat()
        _write_marker(scope, periods, root)
