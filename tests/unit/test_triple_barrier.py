"""
tests/unit/test_triple_barrier.py — ADR-052 (GMI-TB-001) triple-barrier labeler.

Fixtures reproduce the REAL Silver 1D shape measured on live files (AAPL/BBCA, Oct 2026):
tz-aware timestamps stamped 00:00 local exchange time, one bar per trading day.

Controlled volatility: warm-up bars (open=close=100, high=101, low=99) have true range
2/100 = 200 bps, so with the default config
    stop   = 0.375 * 200 = 75 bps        (free, inside [50, 100])
    R      = 0.9375 * 200 / 75 = 2.5     (free, inside [2, 3])
    profit = 2.5 * 75 = 187.5 bps
and for an entry at 100.0 the barrier prices are exact:
    long : profit 101.875 / stop  99.25
    short: profit  98.125 / stop 100.75
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from src.backtest import triple_barrier as tb
from src.backtest.triple_barrier import (
    BarrierConfig,
    LookaheadTripleBarrierLabeler,
    compute_barrier_widths,
    compute_barrier_widths_vec,
    resolve_barriers_vec,
)

WARM = 20
WARMUP_BAR = (100.0, 101.0, 99.0, 100.0)      # TR = 200 bps
FLAT = (100.0, 100.0, 100.0, 100.0)
START = date(2025, 1, 6)                      # a Monday


# ── fixture helpers ──────────────────────────────────────────────────────────

def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _pad(forward: list[tuple], horizon: int = 7) -> list[tuple]:
    return list(forward) + [FLAT] * max(0, horizon - len(forward))


def _write_silver(
    root: Path, market: str, symbol: str, bars: list[tuple], *,
    tz: str = "America/New_York", clean: list[bool] | None = None, start: date = START,
) -> list[date]:
    dates = _weekdays(start, len(bars))
    ts = (
        pl.Series("timestamp", [datetime(d.year, d.month, d.day) for d in dates])
        .cast(pl.Datetime("ms")).dt.replace_time_zone(tz)
    )
    df = pl.DataFrame({
        "timestamp": ts,
        "open": [b[0] for b in bars], "high": [b[1] for b in bars],
        "low": [b[2] for b in bars], "close": [b[3] for b in bars],
        "volume": [1_000_000] * len(bars),
        "is_clean": clean if clean is not None else [True] * len(bars),
        "symbol": [symbol] * len(bars),
    })
    path = root / market / f"symbol={symbol}"
    path.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path / f"{symbol}_1D_silver.parquet")
    return dates


def _env(tmp_path: Path, forward: list[tuple], *, warmup: int = WARM, market: str = "us_stocks",
         symbol: str = "TST", tz: str = "America/New_York", clean_forward=None):
    """Write warm-up + forward bars. decision_date = date of the FIRST forward bar."""
    bars = [WARMUP_BAR] * warmup + list(forward)
    clean = None
    if clean_forward is not None:
        clean = [True] * warmup + list(clean_forward)
    dates = _write_silver(tmp_path, market, symbol, bars, tz=tz, clean=clean)
    return tmp_path, dates[warmup], dates


def _triples(symbol: str, d: date, direction: str = "long", market: str | None = None) -> pl.DataFrame:
    data = {"symbol": [symbol], "decision_date": [d], "direction": [direction]}
    if market is not None:
        data["market"] = [market]
    return pl.DataFrame(data)


def _label(root: Path, triples: pl.DataFrame, cfg: BarrierConfig | None = None,
           markets: list[str] | None = None) -> tb.LabelingResult:
    return LookaheadTripleBarrierLabeler(
        cfg, silver_root=root, allowed_markets=markets or ["us_stocks"]
    ).label(triples)


def _one(res: tb.LabelingResult) -> dict:
    assert res.labels.height == 1, (res.labels, res.skipped)
    return res.labels.row(0, named=True)


# ═════════════════════════════════════════════════════════════════════════════
# Barrier widths — joint construction
# ═════════════════════════════════════════════════════════════════════════════

class TestBarrierWidths:

    def test_default_worked_example(self):
        w = compute_barrier_widths(200.0, BarrierConfig())
        assert w.stop_bps == pytest.approx(75.0)
        assert w.rr == pytest.approx(2.5)
        assert w.profit_bps == pytest.approx(187.5)
        assert (w.stop_clamp, w.rr_clamp) == (0, 0)

    def test_invariants_hold_over_the_entire_vol_range(self):
        cfg = BarrierConfig()
        vols = np.concatenate([[1e-6, 1e-3], np.logspace(-1, 6, 4000)])
        out = compute_barrier_widths_vec(vols, cfg)
        assert out["valid"].all()
        s, p, rr = out["stop_bps"], out["profit_bps"], out["rr"]
        assert (s >= 50 - 1e-9).all() and (s <= 100 + 1e-9).all()
        assert (rr >= 2 - 1e-9).all() and (rr <= 3 + 1e-9).all()
        assert (p >= 100 - 1e-9).all() and (p <= 300 + 1e-9).all()
        np.testing.assert_allclose(p / s, rr, rtol=1e-9)

    def test_one_to_one_pair_is_impossible_by_construction(self):
        # ADR-052: 100bps/100bps sits inside both bps ranges but is R:R 1:1.
        cfg = BarrierConfig()
        vols = np.logspace(-2, 7, 5000)
        out = compute_barrier_widths_vec(vols, cfg)
        assert (out["rr"] >= 2.0 - 1e-9).all()
        assert not ((np.isclose(out["stop_bps"], 100)) & (np.isclose(out["profit_bps"], 100))).any()

    def test_clamp_flags(self):
        cfg = BarrierConfig()
        lo = compute_barrier_widths(10.0, cfg)        # s_raw 3.75 -> floor
        hi = compute_barrier_widths(1000.0, cfg)      # s_raw 375  -> ceiling
        assert lo.stop_bps == 50.0 and lo.stop_clamp == -1
        assert hi.stop_bps == 100.0 and hi.stop_clamp == 1
        assert hi.rr_clamp == 1 and hi.profit_bps == pytest.approx(300.0)
        assert lo.rr_clamp == -1 and lo.profit_bps == pytest.approx(100.0)

    def test_rr_clamps_bind_independently_of_the_profit_band(self):
        # Multiplier ratio outside [2, 3] with the stop FREE: only the R:R clamp can hold
        # the line (the profit-band clip alone would let R:R drift to 4 / 1.5).
        hi = compute_barrier_widths(250.0, BarrierConfig(stop_vol_mult=0.3, profit_vol_mult=1.2))
        lo = compute_barrier_widths(250.0, BarrierConfig(stop_vol_mult=0.3, profit_vol_mult=0.45))
        assert hi.stop_bps == pytest.approx(75.0) and hi.rr == pytest.approx(3.0)
        assert hi.profit_bps == pytest.approx(225.0) and hi.rr_clamp == 1
        assert lo.stop_bps == pytest.approx(75.0) and lo.rr == pytest.approx(2.0)
        assert lo.profit_bps == pytest.approx(150.0) and lo.rr_clamp == -1

    @pytest.mark.parametrize("bad", [float("nan"), 0.0, -5.0, float("inf"), None])
    def test_unusable_vol_returns_none(self, bad):
        assert compute_barrier_widths(bad, BarrierConfig()) is None

    def test_vectorised_and_scalar_agree(self):
        cfg = BarrierConfig()
        for v in (30.0, 100.0, 200.0, 333.0, 900.0):
            w = compute_barrier_widths(v, cfg)
            o = compute_barrier_widths_vec(np.array([v]), cfg)
            assert w.stop_bps == o["stop_bps"][0] and w.profit_bps == o["profit_bps"][0]


class TestBarrierConfig:

    @pytest.mark.parametrize("h", [0, 1, 8, 30])
    def test_horizon_bounds(self, h):
        with pytest.raises(ValueError, match="horizon_days"):
            BarrierConfig(horizon_days=h)

    @pytest.mark.parametrize("h", [2, 5, 7])
    def test_horizon_inside_bounds_ok(self, h):
        assert BarrierConfig(horizon_days=h).horizon_days == h

    def test_profit_band_must_be_guaranteed_by_construction(self):
        with pytest.raises(ValueError, match="rr_min"):
            BarrierConfig(rr_min=1.0)                 # 1 * 50 = 50 < profit floor 100
        with pytest.raises(ValueError, match="rr_max"):
            BarrierConfig(rr_max=4.0)                 # 4 * 100 = 400 > profit ceil 300

    @pytest.mark.parametrize("kw", [
        {"entry_mode": "bogus"}, {"time_barrier_policy": "bogus"}, {"same_bar_policy": "bogus"},
        {"stop_vol_mult": 0}, {"profit_vol_mult": -1}, {"vol_lookback": 1},
        {"min_vol_obs": 99}, {"stop_floor_bps": 200.0}, {"max_anchor_staleness_days": 0},
    ])
    def test_invalid_values_rejected(self, kw):
        with pytest.raises(ValueError):
            BarrierConfig(**kw)

    def test_spec_encodes_every_choice(self):
        spec = BarrierConfig(time_barrier_policy="exclude", same_bar_policy="profit_first",
                             entry_mode="signal_close", horizon_days=5).spec
        for token in ("h5", "signal_close", "time=exclude", "same=profit_first", "rr2-3"):
            assert token in spec


# ═════════════════════════════════════════════════════════════════════════════
# Touch rules
# ═════════════════════════════════════════════════════════════════════════════

class TestTouchRules:

    def test_long_profit(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([
            (100.0, 100.5, 99.8, 100.2),
            (100.3, 102.0, 100.1, 101.8),             # high >= 101.875
        ]))
        r = _one(_label(root, _triples("TST", d, "long")))
        assert r["outcome"] == "profit" and r["label"] == "win" and r["label_binary"] == 1
        assert r["bars_held"] == 2
        assert r["entry_price"] == 100.0
        assert r["exit_price"] == pytest.approx(101.875)
        assert r["realized_bps"] == pytest.approx(187.5)
        assert r["vol_bps"] == pytest.approx(200.0)
        assert (r["stop_bps"], r["rr"]) == (pytest.approx(75.0), pytest.approx(2.5))
        assert not r["gap_through"] and not r["same_bar_ambiguous"]

    def test_long_stop(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([
            (100.0, 100.4, 99.6, 100.0),
            (99.9, 100.0, 99.0, 99.2),                # low <= 99.25
        ]))
        r = _one(_label(root, _triples("TST", d, "long")))
        assert r["outcome"] == "stop" and r["label"] == "loss" and r["label_binary"] == 0
        assert r["exit_price"] == pytest.approx(99.25)
        assert r["realized_bps"] == pytest.approx(-75.0)
        assert r["bars_held"] == 2

    def test_short_profit(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 100.3, 99.7, 99.8), (99.5, 99.7, 97.9, 98.0)]))
        r = _one(_label(root, _triples("TST", d, "short")))
        assert r["outcome"] == "profit"
        assert r["exit_price"] == pytest.approx(98.125)
        assert r["realized_bps"] == pytest.approx(187.5)

    def test_short_stop(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 100.3, 99.7, 99.8), (100.4, 101.0, 100.2, 100.9)]))
        r = _one(_label(root, _triples("TST", d, "short")))
        assert r["outcome"] == "stop"
        assert r["exit_price"] == pytest.approx(100.75)
        assert r["realized_bps"] == pytest.approx(-75.0)

    def test_time_barrier_default_policy_is_loss(self, tmp_path):
        bars = [(100.0, 100.4, 99.6, 100.1)] * 6 + [(100.1, 100.4, 99.6, 100.3)]
        root, d, dates = _env(tmp_path, bars)
        r = _one(_label(root, _triples("TST", d, "long")))
        assert r["outcome"] == "time" and r["label"] == "loss"
        assert r["bars_held"] == 7
        assert r["exit_price"] == pytest.approx(100.3)
        assert r["realized_bps"] == pytest.approx(30.0)
        assert r["label_end_date"] == dates[WARM + 6]

    def test_time_barrier_realized_sign_for_short(self, tmp_path):
        bars = [(100.0, 100.4, 99.6, 100.1)] * 6 + [(100.1, 100.4, 99.6, 100.3)]
        root, d, _ = _env(tmp_path, bars)
        r = _one(_label(root, _triples("TST", d, "short")))
        assert r["outcome"] == "time" and r["realized_bps"] == pytest.approx(-30.0)

    def test_time_barrier_exclude_policy_moves_row_to_skipped(self, tmp_path):
        bars = [(100.0, 100.4, 99.6, 100.1)] * 7
        root, d, _ = _env(tmp_path, bars)
        res = _label(root, _triples("TST", d), BarrierConfig(time_barrier_policy="exclude"))
        assert res.labels.is_empty()
        assert res.skipped["reason"].to_list() == [tb.SKIP_EXCLUDED_TIME]

    @pytest.mark.parametrize("policy,outcome", [("stop_first", "stop"), ("profit_first", "profit")])
    def test_same_bar_double_touch_policies(self, tmp_path, policy, outcome):
        root, d, _ = _env(tmp_path, _pad([(100.0, 102.0, 99.0, 100.5)]))   # both barriers, open inside
        r = _one(_label(root, _triples("TST", d), BarrierConfig(same_bar_policy=policy)))
        assert r["outcome"] == outcome
        assert r["same_bar_ambiguous"] is True and r["bars_held"] == 1

    def test_same_bar_exclude_policy(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 102.0, 99.0, 100.5)]))
        res = _label(root, _triples("TST", d), BarrierConfig(same_bar_policy="exclude"))
        assert res.labels.is_empty()
        assert res.skipped["reason"].to_list() == [tb.SKIP_EXCLUDED_SAME_BAR]

    def test_gap_through_stop_fills_at_open(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 100.3, 99.8, 100.0), (98.9, 99.5, 98.5, 99.0)]))
        r = _one(_label(root, _triples("TST", d, "long")))
        assert r["outcome"] == "stop" and r["gap_through"] is True
        assert r["exit_price"] == pytest.approx(98.9)
        assert r["realized_bps"] == pytest.approx(-110.0)       # worse than the 75 bps stop

    def test_gap_through_profit_fills_at_open(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 100.3, 99.8, 100.0), (102.5, 103.0, 101.0, 102.0)]))
        r = _one(_label(root, _triples("TST", d, "long")))
        assert r["outcome"] == "profit" and r["gap_through"] is True
        assert r["exit_price"] == pytest.approx(102.5)
        assert r["realized_bps"] == pytest.approx(250.0)

    def test_gap_open_beyond_profit_beats_later_stop_in_same_bar(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 100.3, 99.8, 100.0), (102.5, 103.0, 98.0, 99.0)]))
        r = _one(_label(root, _triples("TST", d, "long")))
        assert r["outcome"] == "profit" and r["gap_through"] and not r["same_bar_ambiguous"]

    def test_short_gap_through_stop(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 100.3, 99.8, 100.0), (101.2, 101.5, 100.8, 101.0)]))
        r = _one(_label(root, _triples("TST", d, "short")))
        assert r["outcome"] == "stop" and r["gap_through"]
        assert r["realized_bps"] == pytest.approx(-120.0)

    def test_touch_exactly_on_barrier_counts(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 101.875, 99.9, 101.0)]))
        assert _one(_label(root, _triples("TST", d)))["outcome"] == "profit"

    def test_entry_bar_range_counts_in_next_open_mode(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 102.0, 99.9, 101.5)]))
        r = _one(_label(root, _triples("TST", d)))
        assert r["outcome"] == "profit" and r["bars_held"] == 1
        assert r["first_bar_date"] == d

    def test_signal_close_entry_uses_anchor_close_and_can_gap_on_first_bar(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(102.5, 103.0, 101.0, 102.0)]))
        sc = _one(_label(root, _triples("TST", d), BarrierConfig(entry_mode="signal_close")))
        assert sc["entry_price"] == 100.0
        assert sc["outcome"] == "profit" and sc["gap_through"] and sc["bars_held"] == 1
        no = _one(_label(root, _triples("TST", d)))             # next_open: entry IS 102.5
        assert no["entry_price"] == 102.5 and no["outcome"] != "profit"

    def test_signal_close_with_nonpositive_anchor_close_skipped(self, tmp_path):
        bars = [WARMUP_BAR] * (WARM - 1) + [(100.0, 101.0, -1.0, -2.0)] + _pad([FLAT])
        dates = _write_silver(tmp_path, "us_stocks", "TST", bars)
        res = _label(tmp_path, _triples("TST", dates[WARM]), BarrierConfig(entry_mode="signal_close"))
        assert res.skipped["reason"].to_list() == [tb.SKIP_INVALID_ENTRY]


# ═════════════════════════════════════════════════════════════════════════════
# Vectorised resolver vs an independent loop-based reference
# ═════════════════════════════════════════════════════════════════════════════

def _ref_resolve(o, h, l, c, entry, direction, s, p, policy):
    sign = 1.0 if direction == "long" else -1.0
    pt = entry * (1 + sign * p / 1e4)
    st = entry * (1 - sign * s / 1e4)
    for k in range(len(o)):
        if direction == "long":
            gp, gs = o[k] >= pt, o[k] <= st
            hp, hs = gp or h[k] >= pt, gs or l[k] <= st
        else:
            gp, gs = o[k] <= pt, o[k] >= st
            hp, hs = gp or l[k] <= pt, gs or h[k] >= st
        if not (hp or hs):
            continue
        if gp:
            return "profit", k, o[k], False, True
        if gs:
            return "stop", k, o[k], False, True
        if hp and hs:
            if policy == "stop_first":
                return "stop", k, st, True, False
            if policy == "profit_first":
                return "profit", k, pt, True, False
            return "excluded", k, float("nan"), True, False
        return ("profit", k, pt, False, False) if hp else ("stop", k, st, False, False)
    return "time", len(o) - 1, c[-1], False, False


class TestResolverMatchesReference:

    @pytest.mark.parametrize("direction", ["long", "short"])
    @pytest.mark.parametrize("policy", ["stop_first", "profit_first", "exclude"])
    def test_random_windows(self, direction, policy):
        rng = np.random.default_rng(52)
        m, H = 600, 7
        entry = rng.uniform(20, 200, m)
        s = rng.uniform(50, 100, m)
        p = s * rng.uniform(2, 3, m)
        # random walks with occasional gaps and fat ranges so every branch occurs
        o = np.empty((m, H)); h = np.empty((m, H)); l = np.empty((m, H)); c = np.empty((m, H))
        prev = entry.copy()
        for k in range(H):
            gap = rng.normal(0, 0.006, m) * (rng.random(m) < 0.25) + rng.normal(0, 0.001, m)
            o[:, k] = prev * (1 + gap) if k else entry
            up = np.abs(rng.normal(0, 0.012, m)); dn = np.abs(rng.normal(0, 0.012, m))
            h[:, k] = o[:, k] * (1 + up); l[:, k] = o[:, k] * (1 - dn)
            c[:, k] = rng.uniform(l[:, k], h[:, k]); prev = c[:, k]

        got = resolve_barriers_vec(o, h, l, c, entry, direction, s, p, policy)
        names = {0: "profit", 1: "stop", 2: "time", 3: "excluded"}
        seen = set()
        for i in range(m):
            e_out, e_k, e_px, e_amb, e_gap = _ref_resolve(
                o[i], h[i], l[i], c[i], entry[i], direction, s[i], p[i], policy)
            assert names[int(got["code"][i])] == e_out, i
            assert int(got["bar_index"][i]) == e_k, i
            assert bool(got["same_bar_ambiguous"][i]) == e_amb, i
            assert bool(got["gap_through"][i]) == e_gap, i
            if e_out != "excluded":
                assert got["exit_price"][i] == pytest.approx(e_px), i
            seen.add((e_out, e_amb, e_gap))
        # the sample really exercised the interesting branches
        assert any(x[1] for x in seen) and any(x[2] for x in seen)
        assert {"profit", "stop", "time"} <= {x[0] for x in seen} or policy == "exclude"

    @pytest.mark.parametrize("direction,o,h,l,code,exit_px", [
        # Malformed bars where the OPEN is the only thing that crosses a barrier: high/low
        # alone register no touch, so only the gap flags can resolve them.
        ("long",  102.5, 100.5, 99.9, 0, 102.5),    # open above profit barrier, range inside
        ("long",   98.9, 100.0, 99.5, 1, 98.9),     # open below stop barrier, range inside
        ("short",  97.5, 100.2, 98.5, 0, 97.5),     # open below short-profit barrier
        ("short", 101.2, 100.4, 99.8, 1, 101.2),    # open above short-stop barrier
    ])
    def test_open_beyond_barrier_is_a_gap_touch_even_if_range_disagrees(
        self, direction, o, h, l, code, exit_px
    ):
        r = resolve_barriers_vec(
            np.array([[o]]), np.array([[h]]), np.array([[l]]), np.array([[100.0]]),
            np.array([100.0]), direction, np.array([75.0]), np.array([187.5]),
        )
        assert int(r["code"][0]) == code and bool(r["gap_through"][0])
        assert r["exit_price"][0] == pytest.approx(exit_px)

    def test_rejects_bad_arguments(self):
        z = np.ones((1, 2))
        with pytest.raises(ValueError):
            resolve_barriers_vec(z, z, z, z, np.ones(1), "up", np.ones(1), np.ones(1))
        with pytest.raises(ValueError):
            resolve_barriers_vec(z, z, z, z, np.ones(1), "long", np.ones(1), np.ones(1), "nope")


# ═════════════════════════════════════════════════════════════════════════════
# Time alignment / point-in-time discipline
# ═════════════════════════════════════════════════════════════════════════════

class TestAlignment:

    def test_anchor_is_last_bar_strictly_before_decision_date(self, tmp_path):
        root, d, dates = _env(tmp_path, _pad([(100.0, 100.4, 99.6, 100.0)] * 7))
        r = _one(_label(root, _triples("TST", d)))
        assert r["decision_date"] == d
        assert r["anchor_date"] == dates[WARM - 1] < d      # bar dated d is NOT the anchor
        assert r["first_bar_date"] == d                      # it is the first forward bar

    def test_weekend_decision_date_anchors_on_friday(self, tmp_path):
        # 20 warm-up weekdays from Mon 6 Jan 2025 end on Fri 31 Jan; decide on Sun 2 Feb
        root, _, dates = _env(tmp_path, _pad([(100.0, 100.4, 99.6, 100.0)] * 7))
        sunday = dates[WARM - 1] + timedelta(days=2)
        assert sunday.weekday() == 6
        r = _one(_label(root, _triples("TST", sunday)))
        assert r["anchor_date"] == dates[WARM - 1] and r["first_bar_date"] == dates[WARM]

    def test_label_end_date_and_purge_fields(self, tmp_path):
        root, d, dates = _env(tmp_path, _pad([(100.0, 100.4, 99.6, 100.0), (100.0, 102.0, 100.0, 101.0)]))
        r = _one(_label(root, _triples("TST", d)))
        assert r["label_end_date"] == dates[WARM + 1]
        assert r["anchor_date"] < r["decision_date"] <= r["first_bar_date"] <= r["label_end_date"]

    @pytest.mark.parametrize("tz", ["America/New_York", "Asia/Jakarta", "Asia/Tokyo",
                                    "Europe/London", "Australia/Sydney"])
    def test_trading_date_is_the_stamps_own_date_in_every_zone(self, tmp_path, tz):
        # Local-midnight stamps east of UTC are the PREVIOUS day in UTC; converting would
        # shift every date back one day (src/utils/trade_date.py).
        root, d, dates = _env(tmp_path, _pad([(100.0, 100.4, 99.6, 100.0)] * 7), tz=tz)
        r = _one(_label(root, _triples("TST", d)))
        assert r["first_bar_date"] == d and r["anchor_date"] == dates[WARM - 1]

    def test_volatility_and_widths_use_nothing_after_the_anchor(self, tmp_path):
        a, d, _ = _env(tmp_path / "a", _pad([(100.0, 100.4, 99.6, 100.0)] * 7))
        b, d2, _ = _env(tmp_path / "b", _pad([(100.0, 140.0, 60.0, 90.0)] * 7))
        assert d == d2
        ra = _one(_label(a, _triples("TST", d)))
        rb = _one(_label(b, _triples("TST", d)))
        for col in ("vol_bps", "stop_bps", "profit_bps", "rr", "stop_clamp", "rr_clamp", "anchor_date"):
            assert ra[col] == rb[col], col

    def test_forward_bars_do_not_alter_earlier_decision_vol(self, tmp_path):
        # a decision date in the middle of the history sees only its own past
        bars = [WARMUP_BAR] * 30 + [(100.0, 150.0, 70.0, 120.0)] * 12
        dates = _write_silver(tmp_path, "us_stocks", "TST", bars)
        early = _one(_label(tmp_path, _triples("TST", dates[WARM])))
        assert early["vol_bps"] == pytest.approx(200.0)


# ═════════════════════════════════════════════════════════════════════════════
# Skips and data hygiene
# ═════════════════════════════════════════════════════════════════════════════

class TestSkipsAndHygiene:

    def test_horizon_incomplete_is_not_labeled(self, tmp_path):
        root, d, _ = _env(tmp_path, [(100.0, 100.4, 99.6, 100.0)] * 5)
        res = _label(root, _triples("TST", d))
        assert res.labels.is_empty()
        assert res.skipped["reason"].to_list() == [tb.SKIP_HORIZON_INCOMPLETE]
        # …but a shorter configured horizon that IS fully observed gets labeled
        ok = _label(root, _triples("TST", d), BarrierConfig(horizon_days=5))
        assert ok.labels.height == 1 and ok.labels["bars_held"][0] == 5

    def test_unclean_but_structurally_valid_bar_is_kept_and_counted(self, tmp_path):
        # is_clean=False also marks real |z|>4 moves; dropping them would bias the labels.
        root, d, _ = _env(
            tmp_path, _pad([(100.0, 100.4, 99.6, 100.0), (100.0, 100.4, 99.6, 100.0), (100.0, 102.0, 99.9, 101.0)]),
            clean_forward=[True, True, False, True, True, True, True],
        )
        r = _one(_label(root, _triples("TST", d)))
        assert r["outcome"] == "profit" and r["n_unclean_in_horizon"] == 1

    @pytest.mark.parametrize("bad_bar", [
        (100.0, 99.0, 101.0, 100.0),            # high < low
        (100.0, 100.5, -6.0, -5.0),             # sign-crossing (WTI Apr-2020 shape)
        (100.0, float("nan"), 99.0, 100.0),     # non-finite
    ])
    def test_structurally_invalid_bar_in_horizon_skips_row(self, tmp_path, bad_bar):
        fwd = [(100.0, 100.4, 99.6, 100.0), (100.0, 100.4, 99.6, 100.0), bad_bar]
        root, d, _ = _env(tmp_path, _pad(fwd))
        res = _label(root, _triples("TST", d))
        assert res.labels.is_empty()
        assert res.skipped["reason"].to_list() == [tb.SKIP_INVALID_BAR]

    def test_malformed_open_above_high_is_labeled_not_dropped(self, tmp_path):
        # forex-style malformed bar (open prints above the quoted high) inside the horizon
        root, d, _ = _env(tmp_path, _pad([(100.0, 100.3, 99.8, 100.0), (102.5, 100.5, 99.0, 100.0)]))
        r = _one(_label(root, _triples("TST", d)))
        assert r["outcome"] == "profit" and r["gap_through"] is True
        assert r["exit_price"] == pytest.approx(102.5)

    @pytest.mark.parametrize("last_warmup", [
        (103.0, 101.0, 99.0, 100.0),    # open above high  -> repaired high 103
        (97.0, 101.0, 99.0, 100.0),     # open below low   -> repaired low 97
    ])
    def test_bar_repair_widens_true_range_in_the_vol_estimate(self, tmp_path, last_warmup):
        # raw TR of the malformed bar is 200 bps; repaired to the enclosing range it is 400,
        # so the 14-bar mean is (13*200 + 400)/14 — not 200.
        bars = [WARMUP_BAR] * (WARM - 1) + [last_warmup] + _pad([(100.0, 102.0, 99.9, 101.0)])
        dates = _write_silver(tmp_path, "us_stocks", "TST", bars)
        r = _one(_label(tmp_path, _triples("TST", dates[WARM])))
        assert r["vol_bps"] == pytest.approx((13 * 200 + 400) / 14)

    def test_anchor_stale_when_data_ends_long_before_decision(self, tmp_path):
        root, _, dates = _env(tmp_path, _pad([(100.0, 100.4, 99.6, 100.0)] * 7))
        res = _label(root, _triples("TST", dates[-1] + timedelta(days=30)))
        assert res.skipped["reason"].to_list() == [tb.SKIP_ANCHOR_STALE]

    def test_no_prior_bar_before_history_starts(self, tmp_path):
        root, _, dates = _env(tmp_path, _pad([(100.0, 100.4, 99.6, 100.0)] * 7))
        res = _label(root, _triples("TST", dates[0]))
        assert res.skipped["reason"].to_list() == [tb.SKIP_NO_PRIOR_BAR]

    def test_vol_unavailable_without_enough_history(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 100.4, 99.6, 100.0)] * 7), warmup=5)
        res = _label(root, _triples("TST", d))
        assert res.skipped["reason"].to_list() == [tb.SKIP_VOL_UNAVAILABLE]

    def test_nonpositive_price_inside_vol_window_does_not_poison_vol(self, tmp_path):
        # one corrupt warm-up bar: vol still computed from the valid ones (>= min_vol_obs)
        bars = [WARMUP_BAR] * 10 + [(100.0, 101.0, -1.0, -2.0)] + [WARMUP_BAR] * 9 + _pad([(100.0, 102.0, 99.9, 101.0)])
        dates = _write_silver(tmp_path, "us_stocks", "TST", bars)
        r = _one(_label(tmp_path, _triples("TST", dates[WARM])))
        assert r["vol_bps"] == pytest.approx(200.0)


# ═════════════════════════════════════════════════════════════════════════════
# Market resolution (Layer 1 only)
# ═════════════════════════════════════════════════════════════════════════════

class TestMarketResolution:

    def _two_markets(self, tmp_path):
        fwd = _pad([(100.0, 100.4, 99.6, 100.0)] * 7)
        _, d, _ = _env(tmp_path, fwd, market="commodity", symbol="CL")
        _env(tmp_path, fwd, market="us_stocks", symbol="CL")
        return d

    def test_symbol_in_two_markets_is_ambiguous_without_hint(self, tmp_path):
        d = self._two_markets(tmp_path)
        res = _label(tmp_path, _triples("CL", d), markets=["commodity", "us_stocks"])
        assert res.skipped["reason"].to_list() == [tb.SKIP_AMBIGUOUS_MARKET]

    def test_market_hint_disambiguates(self, tmp_path):
        d = self._two_markets(tmp_path)
        res = _label(tmp_path, _triples("CL", d, market="commodity"), markets=["commodity", "us_stocks"])
        assert res.labels["market"].to_list() == ["commodity"]

    def test_layer2_context_symbol_is_never_labeled(self, tmp_path):
        fwd = _pad([(100.0, 100.4, 99.6, 100.0)] * 7)
        _, d, _ = _env(tmp_path, fwd, market="context", symbol="VIX")
        by_scan = _label(tmp_path, _triples("VIX", d))
        by_hint = _label(tmp_path, _triples("VIX", d, market="context"))
        assert by_scan.skipped["reason"].to_list() == [tb.SKIP_NOT_LAYER1]
        assert by_hint.skipped["reason"].to_list() == [tb.SKIP_NOT_LAYER1]

    def test_unknown_symbol(self, tmp_path):
        res = _label(tmp_path, _triples("NOPE", date(2025, 3, 3)))
        assert res.skipped["reason"].to_list() == [tb.SKIP_NO_SILVER_FILE]

    def test_hint_pointing_at_missing_file(self, tmp_path):
        (tmp_path / "us_stocks").mkdir()
        res = _label(tmp_path, _triples("NOPE", date(2025, 3, 3), market="us_stocks"))
        assert res.skipped["reason"].to_list() == [tb.SKIP_NO_SILVER_FILE]


# ═════════════════════════════════════════════════════════════════════════════
# Input contract, output schema, audit surface
# ═════════════════════════════════════════════════════════════════════════════

class TestContract:

    def test_requires_decision_date_and_explains_entry_date(self, tmp_path):
        lab = LookaheadTripleBarrierLabeler(silver_root=tmp_path, allowed_markets=["us_stocks"])
        with pytest.raises(ValueError, match="ANCHOR"):
            lab.label(pl.DataFrame({"symbol": ["A"], "entry_date": [date(2025, 1, 1)], "direction": ["long"]}))
        with pytest.raises(ValueError, match="decision_date"):
            lab.label(pl.DataFrame({"symbol": ["A"], "direction": ["long"]}))

    def test_bad_direction_rejected(self, tmp_path):
        lab = LookaheadTripleBarrierLabeler(silver_root=tmp_path, allowed_markets=["us_stocks"])
        with pytest.raises(ValueError, match="direction"):
            lab.label(pl.DataFrame({"symbol": ["A"], "decision_date": [date(2025, 1, 1)], "direction": ["up"]}))

    def test_string_and_datetime_decision_dates_are_coerced(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 102.0, 99.9, 101.0)]))
        as_str = pl.DataFrame({"symbol": ["TST"], "decision_date": [d.isoformat()], "direction": ["LONG"]})
        as_dt = pl.DataFrame({"symbol": ["TST"], "decision_date": [datetime(d.year, d.month, d.day, 7, 0)],
                              "direction": ["long"]})
        assert _one(_label(root, as_str))["decision_date"] == d
        assert _one(_label(root, as_dt))["decision_date"] == d

    def test_duplicate_triples_collapse_to_one_row(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 102.0, 99.9, 101.0)]))
        res = _label(root, pl.concat([_triples("TST", d), _triples("TST", d)]))
        assert res.labels.height == 1

    def test_output_schema_and_row_invariants(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 102.0, 99.9, 101.0)]))
        both = pl.concat([_triples("TST", d, "long"), _triples("TST", d, "short")])
        res = _label(root, both)
        assert res.labels.columns == tb._LABEL_COLUMNS
        L = res.labels
        assert L.height == 2
        assert (L["stop_bps"].is_between(50, 100)).all()
        assert (L["rr"].is_between(2, 3)).all()
        assert (L["profit_bps"].is_between(100, 300)).all()
        assert (L["bars_held"].is_between(1, 7)).all()
        assert (L["label_binary"] == (L["outcome"] == "profit").cast(pl.Int8)).all()
        assert (L["anchor_date"] < L["decision_date"]).all()
        assert (L["label_spec"] == BarrierConfig().spec).all()
        assert (L["labeler_version"] == tb.LABELER_VERSION).all()

    def test_deterministic(self, tmp_path):
        root, d, _ = _env(tmp_path, _pad([(100.0, 102.0, 99.9, 101.0)]))
        t = pl.concat([_triples("TST", d, "long"), _triples("TST", d, "short")])
        assert _label(root, t).labels.equals(_label(root, t).labels)

    def test_empty_input_gives_typed_empty_frames(self, tmp_path):
        empty = pl.DataFrame(schema={"symbol": pl.String, "decision_date": pl.Date, "direction": pl.String})
        res = _label(tmp_path, empty)
        assert res.labels.is_empty() and res.skipped.is_empty()
        assert res.labels.columns == tb._LABEL_COLUMNS and res.skipped.columns == tb._SKIP_COLUMNS
        assert res.summary()["n_labeled"] == 0

    def test_summary_reports_calibration_surface(self, tmp_path):
        fwd = _pad([(100.0, 100.4, 99.6, 100.0), (100.3, 102.0, 100.1, 101.8)])
        root, d, _ = _env(tmp_path, fwd)
        res = _label(root, pl.concat([_triples("TST", d), _triples("MISSING", d)]))
        sm = res.summary()
        assert sm["n_labeled"] == 1 and sm["n_skipped"] == 1
        assert sm["skipped_by_reason"] == {tb.SKIP_NO_SILVER_FILE: 1}
        assert sm["win_rate"] == 1.0
        assert sm["zero_drift_reference_win_rate"] == pytest.approx(75.0 / (75.0 + 187.5))
        assert sm["stop_clamp_share"]["free"] == 1.0
        for k in ("same_bar_ambiguous_rate", "gap_through_rate", "rr_clamp_share",
                  "outcome_share", "mean_bars_held", "label_spec"):
            assert k in sm

    def test_lookahead_warning_is_unmissable(self):
        assert "LOOKAHEAD" in tb.__doc__ and "NEVER be imported" in tb.__doc__
        assert "Lookahead" in tb.LookaheadTripleBarrierLabeler.__name__
