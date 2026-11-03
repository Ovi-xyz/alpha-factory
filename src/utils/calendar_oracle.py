"""
calendar_oracle.py — XAE-R05 (cross_asset_recalibration_guide Q3/Q4, decision D2).

Answers one question for the Silver gap check: "was every weekday inside this gap an official non-session
day for the exchange that publishes the series?" Deterministic, needs no peer group, so it classifies the
indices that peer agreement cannot (N225, DAX, JKSE — each alone on its holiday calendar).

THREE-VALUED ANSWER, never a guess:
    True   every weekday strictly inside (prev_ts, ts) is a non-session day  -> market closure
    False  at least one such weekday WAS a session day                        -> a bar is genuinely missing
    None   unknown: no calendar for the symbol, `exchange_calendars` not installed, or the gap lies outside
           the calendar's range                                               -> caller falls back to peers

The caller (gap_analysis.classify_gaps_by_peer_agreement) treats None as "ask the peers", so a missing
library or an unmapped symbol degrades to exactly the pre-R05 behaviour — but never silently: `status()`
reports which symbols were unknown and why, and the quality check logs it.

Pure Python (no DuckDB, no filesystem except the one YAML read in `from_config`), so it can be tested with
a fake calendar factory and no installed library.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Optional

import yaml
from loguru import logger

DEFAULT_CONFIG_PATH = Path("config/exchange_calendars.yaml")


def _default_factory(code: str):
    import exchange_calendars as xc          # lazy: an absent library must degrade, not break import
    return xc.get_calendar(code)


class CalendarOracle:
    def __init__(self, symbol_to_calendar: dict[str, str],
                 calendar_factory: Optional[Callable[[str], object]] = None):
        self._map = dict(symbol_to_calendar)
        self._factory = calendar_factory or _default_factory
        self._cache: dict[str, object] = {}
        self._failed: dict[str, str] = {}           # calendar code -> reason it could not be loaded
        self._unknown: dict[str, str] = {}          # symbol -> reason of the last "unknown" answer

    # ── construction ──────────────────────────────────────────────────────
    @classmethod
    def from_config(cls, path: Optional[Path] = None,
                    calendar_factory: Optional[Callable[[str], object]] = None) -> "CalendarOracle":
        path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
        try:
            cfg = yaml.safe_load(path.read_text()) or {}
        except Exception as e:
            logger.warning(f"[calendar_oracle] cannot read {path} ({e}) — no calendars, peer fallback only")
            return cls({}, calendar_factory)
        return cls({str(k): str(v) for k, v in (cfg.get("calendars") or {}).items()}, calendar_factory)

    # ── calendar access ───────────────────────────────────────────────────
    def _calendar(self, code: str):
        if code in self._cache:
            return self._cache[code]
        if code in self._failed:
            return None
        try:
            cal = self._factory(code)
        except Exception as e:                       # ImportError, unknown code, ...
            self._failed[code] = f"{type(e).__name__}: {e}"
            logger.warning(f"[calendar_oracle] calendar {code} unavailable ({self._failed[code]}) — "
                           f"its symbols fall back to peer agreement")
            return None
        self._cache[code] = cal
        return cal

    # ── the question ──────────────────────────────────────────────────────
    def is_closure(self, symbol: str, prev_ts: date, ts: date) -> Optional[bool]:
        code = self._map.get(symbol)
        if code is None:
            self._unknown[symbol] = "no calendar mapped"
            return None
        cal = self._calendar(code)
        if cal is None:
            self._unknown[symbol] = f"calendar {code} unavailable"
            return None
        try:
            first = cal.first_session.date()
            last = cal.last_session.date()
            if prev_ts < first or ts > last:
                self._unknown[symbol] = f"gap outside {code} range [{first}, {last}]"
                return None
            d = prev_ts + timedelta(days=1)
            while d < ts:
                if d.weekday() < 5 and cal.is_session(d.isoformat()):
                    return False
                d += timedelta(days=1)
            return True
        except Exception as e:
            self._unknown[symbol] = f"{type(e).__name__}: {e}"
            logger.debug(f"[calendar_oracle] {symbol} {prev_ts}..{ts}: {e}")
            return None

    # ── reporting ─────────────────────────────────────────────────────────
    def status(self) -> dict:
        """What a caller needs to log: which calendars failed to load and which symbols were unknown."""
        return {"failed_calendars": dict(self._failed), "unknown_symbols": dict(self._unknown),
                "mapped_symbols": len(self._map)}
