"""
gap_analysis.py — ADD GAP-PEER-01 (chat thread, 8 Sep 2026)

Peer-agreement classification for Silver OHLCV gap detection. Shared by
quality_validator.py's _check_gap_detection() and _check_context_gap_detection()
— previously two independent copies of an identical WITH-gaps CTE (the same
"same bug, unmirrored fix" shape already found elsewhere in this pipeline,
e.g. macro_processor.py's stale EIA glob never receiving FIX EIA-5's path
correction when it was applied to eia_ingester.py's cache scan).

ROOT CAUSE this replaces: a flat >5-calendar-day threshold, applied
uniformly to every market, cannot distinguish "this whole market was
legitimately closed" from "this one symbol has a real, isolated data
problem". Live diagnostic (8 Sep 2026, all 594 Layer 1 + 58 Layer 2 symbols,
copied via Filesystem MCP and queried directly) found the reported counts
were fully explained by market-wide holiday closures:
  - Layer 1 "282 gaps": 100% IDX (all 30 symbols, identical min=6/max=12
    day-gap signature scaling with each symbol's listing age — the
    fingerprint of a shared national holiday calendar, e.g. Eid al-Fitr).
    us_stocks/forex/commodity contributed zero.
  - Layer 2 "87 gaps": concentrated in SSEC(28)/JKSE(13)/TWSE(13)/
    KOSPI(12)/N225(12)/HSI(4) — Asia-Pacific equity indices sharing
    regional holiday timing (Lunar New Year etc.) — plus DAX(2) and
    ALUMINIUM(3, a one-time 2016 cold-start artifact, not a calendar
    effect).
Neither is missing data the incremental-fetch design could ever backfill
(IncFetchProtocol only extends forward from the last known date — see
Supplementary Design v1.1 G1). The checks were correctly implemented SQL
that was simply blind to market calendars, producing a permanent ~369-
occurrence noise floor that made both WARNING checks chronically red for
reasons requiring no action, while providing no better signal for an
actual future isolated gap than a check that didn't exist at all.

DESIGN: a gap on symbol X is classified as a market closure (excluded from
the count) if at least MIN_PEER_AGREEMENT (default 50%) of X's OTHER peers
in the same peer group show an OVERLAPPING gap over the same date window.
"Peer group" is deliberately the caller's choice (see silver_scope.py's
layer1_peer_groups()/context_peer_groups()) — this module knows nothing
about markets, InstrumentLoader, or Parquet. Peers with no gap at all
correctly count toward the denominator (they traded normally, which is
itself informative) but never toward the numerator. Symbols with zero
comparable peers (peer_group of size 1, or missing from the peer map
entirely) cannot be evaluated for agreement and are conservatively kept
as isolated — i.e. NOT suppressed. Under-suppressing preserves today's
detection behavior exactly; over-suppressing could hide a real problem,
so ties/unknowns resolve toward keeping the check's current sensitivity.

This module is pure Python — no DuckDB, no filesystem, no InstrumentLoader
import. quality_validator.py owns the SQL fetch (see its
_fetch_raw_gap_events()); silver_scope.py owns turning InstrumentLoader
into a peer map. Keeping the classification itself free of both means it
can be exhaustively unit-tested with synthetic data (tests/unit/
test_gap_analysis.py) without touching live Parquet or the real
instrument universe.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date

DEFAULT_MIN_PEER_AGREEMENT: float = 0.5


@dataclass(frozen=True)
class GapEvent:
    """One symbol's single day_gap-over-threshold occurrence.

    prev_ts / ts are the last bar before, and first bar after, the gap —
    i.e. the gap's open interval is (prev_ts, ts). peer_group is whatever
    grouping key the caller assigns (Layer 1: InstrumentLoader market;
    Layer 2: context_category); None means "no group — cannot be peer-
    checked", not "group of size zero".
    """
    symbol: str
    prev_ts: date
    ts: date
    day_gap: int
    peer_group: str | None = None


def _overlaps(a: GapEvent, b: GapEvent) -> bool:
    """True if the open intervals (a.prev_ts, a.ts) and (b.prev_ts, b.ts)
    intersect. Standard interval-intersection test: a starts before b ends
    AND b starts before a ends. Touching endpoints (a.ts == b.prev_ts) do
    NOT count as overlapping — that's two back-to-back but distinct gaps,
    not the same closure event."""
    return a.prev_ts < b.ts and b.prev_ts < a.ts


def classify_gaps_by_peer_agreement(
    events: list[GapEvent],
    peer_group_sizes: dict[str, int],
    min_peer_agreement: float = DEFAULT_MIN_PEER_AGREEMENT,
) -> tuple[list[GapEvent], list[GapEvent]]:
    """
    Split gap events into (isolated, market_closure).

    peer_group_sizes: {peer_group_name: total_symbol_count_in_group} —
    the TOTAL membership of the group (from the instrument universe, via
    silver_scope's peer-group builders), NOT derived from `events` alone.
    This distinction matters: a peer that traded normally (no gap at all)
    must still count in the denominator, since "most of the market closed"
    and "most of the market traded fine except these 2" are very different
    facts about the SAME numerator. Deriving the denominator only from
    symbols that happen to appear in `events` would silently inflate
    agreement in small event sets.

    isolated:        kept toward the calling check's WARNING count,
                      identical to today's un-classified behavior.
    market_closure:  excluded from the count; callers should log these at
                      DEBUG, not delete or modify any underlying data —
                      this function only changes what one check counts.
    """
    if not 0.0 <= min_peer_agreement <= 1.0:
        raise ValueError(
            f"min_peer_agreement must be in [0.0, 1.0], got {min_peer_agreement}"
        )

    by_group: dict[str, list[GapEvent]] = defaultdict(list)
    for e in events:
        if e.peer_group is not None:
            by_group[e.peer_group].append(e)

    isolated: list[GapEvent] = []
    closures: list[GapEvent] = []

    for e in events:
        if e.peer_group is None:
            isolated.append(e)
            continue

        n_peers = peer_group_sizes.get(e.peer_group, 0) - 1  # exclude self
        if n_peers <= 0:
            # No comparable peers (singleton group, or group missing from
            # peer_group_sizes entirely) — cannot evaluate agreement.
            # Conservative default: do not suppress.
            isolated.append(e)
            continue

        agreeing_peers = {
            other.symbol
            for other in by_group[e.peer_group]
            if other.symbol != e.symbol and _overlaps(e, other)
        }
        agreement = len(agreeing_peers) / n_peers

        if agreement >= min_peer_agreement:
            closures.append(e)
        else:
            isolated.append(e)

    return isolated, closures
