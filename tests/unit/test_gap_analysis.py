"""tests/unit/test_gap_analysis.py — ADD GAP-PEER-01

Pure unit tests for src/utils/gap_analysis.py. No DuckDB, no filesystem,
no InstrumentLoader — synthetic GapEvent lists only, so these exercise the
classification logic in exact isolation from everything quality_validator.py
wires it into.
"""

from datetime import date

import pytest

from src.utils.gap_analysis import GapEvent, _overlaps, classify_gaps_by_peer_agreement


def _ev(symbol, prev_ts, ts, day_gap=None, peer_group="grp"):
    if day_gap is None:
        day_gap = (ts - prev_ts).days
    return GapEvent(symbol=symbol, prev_ts=prev_ts, ts=ts, day_gap=day_gap, peer_group=peer_group)


class TestOverlaps:

    def test_identical_windows_overlap(self):
        a = _ev("A", date(2026, 1, 1), date(2026, 1, 10))
        b = _ev("B", date(2026, 1, 1), date(2026, 1, 10))
        assert _overlaps(a, b) is True

    def test_partial_overlap(self):
        a = _ev("A", date(2026, 1, 1), date(2026, 1, 10))
        b = _ev("B", date(2026, 1, 5), date(2026, 1, 15))
        assert _overlaps(a, b) is True
        assert _overlaps(b, a) is True  # symmetric

    def test_disjoint_windows_do_not_overlap(self):
        a = _ev("A", date(2026, 1, 1), date(2026, 1, 10))
        b = _ev("B", date(2026, 2, 1), date(2026, 2, 10))
        assert _overlaps(a, b) is False

    def test_touching_endpoints_do_not_overlap(self):
        """a.ts == b.prev_ts is two back-to-back but distinct gaps, not
        one shared closure — must NOT count as overlapping."""
        a = _ev("A", date(2026, 1, 1), date(2026, 1, 10))
        b = _ev("B", date(2026, 1, 10), date(2026, 1, 20))
        assert _overlaps(a, b) is False


class TestClassifyGapsByPeerAgreement:

    def test_empty_events_returns_empty(self):
        isolated, closures = classify_gaps_by_peer_agreement([], {})
        assert isolated == []
        assert closures == []

    def test_full_market_agreement_is_closure(self):
        """All 3 peers in a group of 3 show the identical gap window ->
        each one's agreement is 2/2 = 100% -> all classified as closures.
        This is the IDX/SSEC/JKSE scenario from the live diagnostic."""
        events = [
            _ev("A", date(2026, 4, 10), date(2026, 4, 20), peer_group="idx"),
            _ev("B", date(2026, 4, 10), date(2026, 4, 20), peer_group="idx"),
            _ev("C", date(2026, 4, 10), date(2026, 4, 20), peer_group="idx"),
        ]
        isolated, closures = classify_gaps_by_peer_agreement(events, {"idx": 3})
        assert isolated == []
        assert {e.symbol for e in closures} == {"A", "B", "C"}

    def test_isolated_gap_with_no_peer_agreement_stays_isolated(self):
        """Only 1 symbol in a group of 3 has a gap; its 2 peers traded
        normally (no gap at all, so they never appear in `events`) ->
        agreement = 0/2 = 0% -> isolated. This is the "real, actionable
        gap" case the whole mechanism must still catch."""
        events = [
            _ev("A", date(2026, 4, 10), date(2026, 4, 20), peer_group="us_stocks"),
        ]
        isolated, closures = classify_gaps_by_peer_agreement(events, {"us_stocks": 3})
        assert {e.symbol for e in isolated} == {"A"}
        assert closures == []

    def test_exactly_at_threshold_counts_as_closure(self):
        """1 of 2 other peers agrees -> exactly 50% -> >= 0.5 threshold ->
        classified as closure (boundary is inclusive)."""
        events = [
            _ev("A", date(2026, 6, 1), date(2026, 6, 10), peer_group="g"),
            _ev("B", date(2026, 6, 1), date(2026, 6, 10), peer_group="g"),
            # C has no gap at all -- doesn't appear in events, still counts
            # in the denominator via peer_group_sizes.
        ]
        isolated, closures = classify_gaps_by_peer_agreement(
            events, {"g": 3}, min_peer_agreement=0.5
        )
        assert {e.symbol for e in closures} == {"A", "B"}
        assert isolated == []

    def test_just_below_threshold_stays_isolated(self):
        """1 of 3 other peers agrees -> 33% -> below 0.5 -> isolated."""
        events = [
            _ev("A", date(2026, 6, 1), date(2026, 6, 10), peer_group="g"),
            _ev("B", date(2026, 6, 1), date(2026, 6, 10), peer_group="g"),
            # C, D have no gap -- part of denominator (group size 4), not events
        ]
        isolated, closures = classify_gaps_by_peer_agreement(
            events, {"g": 4}, min_peer_agreement=0.5
        )
        # A's agreement: only B overlaps, out of 3 other peers (B, C, D) = 1/3 = 33%
        assert {e.symbol for e in isolated} == {"A", "B"}
        assert closures == []

    def test_non_overlapping_gaps_in_same_group_stay_isolated(self):
        """Two symbols in the same group both have gaps, but on
        completely different, non-overlapping dates -> neither agrees
        with the other -> both isolated. Distinguishes real coincidence
        from an actual shared closure."""
        events = [
            _ev("A", date(2026, 1, 1), date(2026, 1, 10), peer_group="g"),
            _ev("B", date(2026, 6, 1), date(2026, 6, 10), peer_group="g"),
        ]
        isolated, closures = classify_gaps_by_peer_agreement(events, {"g": 2})
        assert {e.symbol for e in isolated} == {"A", "B"}
        assert closures == []

    def test_no_peer_group_is_conservatively_isolated(self):
        """peer_group=None (caller couldn't map the symbol to any group)
        must never be suppressed, regardless of peer_group_sizes content."""
        events = [_ev("X", date(2026, 1, 1), date(2026, 1, 10), peer_group=None)]
        isolated, closures = classify_gaps_by_peer_agreement(events, {"anything": 100})
        assert {e.symbol for e in isolated} == {"X"}
        assert closures == []

    def test_singleton_group_is_conservatively_isolated(self):
        """Group of size 1 (n_peers = 1 - 1 = 0 after excluding self) has
        no comparable peers -- must not be suppressed."""
        events = [_ev("DXY", date(2026, 1, 1), date(2026, 1, 10), peer_group="context_dollar")]
        isolated, closures = classify_gaps_by_peer_agreement(events, {"context_dollar": 1})
        assert {e.symbol for e in isolated} == {"DXY"}
        assert closures == []

    def test_group_missing_from_sizes_is_conservatively_isolated(self):
        """peer_group set on the event but absent from peer_group_sizes
        (e.g. a stale/renamed category) -> treated as size 0 -> isolated,
        never crashes."""
        events = [_ev("A", date(2026, 1, 1), date(2026, 1, 10), peer_group="unknown_group")]
        isolated, closures = classify_gaps_by_peer_agreement(events, {})
        assert {e.symbol for e in isolated} == {"A"}
        assert closures == []

    def test_groups_do_not_cross_contaminate(self):
        """A market-wide closure in group 'idx' must not affect
        classification of an isolated gap in unrelated group 'forex'."""
        events = [
            _ev("BBCA", date(2026, 4, 10), date(2026, 4, 20), peer_group="idx"),
            _ev("BBRI", date(2026, 4, 10), date(2026, 4, 20), peer_group="idx"),
            _ev("EUR_USD", date(2026, 4, 10), date(2026, 4, 20), peer_group="forex"),
        ]
        isolated, closures = classify_gaps_by_peer_agreement(
            events, {"idx": 2, "forex": 5}
        )
        assert {e.symbol for e in closures} == {"BBCA", "BBRI"}
        assert {e.symbol for e in isolated} == {"EUR_USD"}

    def test_agreeing_peer_must_be_different_symbol(self):
        """A symbol never counts as its own peer even if somehow
        duplicated in the events list (defensive; shouldn't happen given
        one row per symbol per gap from the SQL fetch, but the exclusion
        must hold regardless)."""
        events = [
            _ev("A", date(2026, 1, 1), date(2026, 1, 10), peer_group="g"),
            _ev("A", date(2026, 1, 1), date(2026, 1, 10), peer_group="g"),
        ]
        isolated, closures = classify_gaps_by_peer_agreement(events, {"g": 5})
        # Both A-events: only "the other A" would overlap, but same-symbol
        # matches are excluded -> 0 agreeing peers out of 4 others -> isolated.
        assert len(closures) == 0
        assert len(isolated) == 2

    @pytest.mark.parametrize("bad_value", [-0.1, 1.1, 2.0])
    def test_invalid_min_peer_agreement_raises(self, bad_value):
        with pytest.raises(ValueError):
            classify_gaps_by_peer_agreement([], {}, min_peer_agreement=bad_value)

    @pytest.mark.parametrize("boundary_value", [0.0, 1.0])
    def test_boundary_min_peer_agreement_values_are_valid(self, boundary_value):
        # Must not raise.
        classify_gaps_by_peer_agreement([], {}, min_peer_agreement=boundary_value)
