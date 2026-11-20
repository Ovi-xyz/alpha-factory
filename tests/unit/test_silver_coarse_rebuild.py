"""
FIX SIL-COARSE-REBUILD-01 + SIL-SEAM-QUARANTINE-01 [chat thread, 17-18 Nov 2026] — Silver 1W/1M are rebuilt from the
Silver 1D bars (non-FX markets), and a series whose close jumps by a split factor and reverses is quarantined whole.

Shapes measured on live Silver / Bronze (see the block above coarse_rebuild_exclusion in the processor): ABBV 1W
(Tuesday-stamped Tue..Mon blocks), AG 1M (a Sunday-1st month absent, November frozen at a first snapshot), ALL / CSX 1W
(a constant 1.00416 / 1.0027 vintage step), MNST 1M (exactly 0.5x of a 1D that is not split-adjusted) and MNST 1D
(seven reversing factor-2 jumps between 2026-07-20 and 08-11).
Fixtures are healthy by default so each test reaches exactly one branch.
"""
from __future__ import annotations

import math
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

import src.silver.ohlcv_processor as P
from src.silver.ohlcv_processor import OHLCVProcessor

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import audit_silver_ohlcv as A  # noqa: E402

NY = "America/New_York"


# ── fixtures ────────────────────────────────────────────────────────────────────────────────────

def bdays(start: date, n: int, skip=()):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5 and d not in skip:
            out.append(d)
        d += timedelta(days=1)
    return out


def walk(n: int, start=100.0):
    """A deterministic noisy walk: daily moves stay far from any split factor and ratios are not constant."""
    c, x = [], start
    for i in range(n):
        x *= math.exp(0.004 * math.sin(i * 1.7) + 0.003 * (((i * 37) % 11) - 5) / 5)
        c.append(round(x, 4))
    return c


def bronze(dates, closes, tz=NY, hour=0):
    ts = pl.Series("timestamp", [datetime(d.year, d.month, d.day, hour) for d in dates],
                   dtype=pl.Datetime("ms")).dt.replace_time_zone(tz)
    return pl.DataFrame({"timestamp": ts, "open": [x - 0.4 for x in closes], "high": [x + 1.0 for x in closes],
                         "low": [x - 1.2 for x in closes], "close": list(closes),
                         "volume": [1000 + 7 * i for i in range(len(closes))]})


@pytest.fixture
def proc():
    return OHLCVProcessor()


def silver(proc, dates, closes, tf, tz=NY, market="us_stocks", sym="X"):
    return proc.process_symbol(bronze(dates, closes, tz), sym, market, tf, tz_hint=tz)


def daily_silver(proc, n=130, start=date(2026, 1, 5), tz=NY, market="us_stocks", skip=()):
    days = bdays(start, n, skip)
    return days, silver(proc, days, walk(len(days)), "1D", tz, market)


def truth(proc, daily, tf):
    """The weekly / monthly bars Silver 1D implies (one row per period, first period included)."""
    return proc._derive_period_bars(daily, tf)


def coarse_from_truth(proc, daily, tf, tz=NY, drop_first=False, market="us_stocks"):
    """A Bronze-built coarse Silver that already equals the 1D aggregate, stamped at the period start."""
    t = truth(proc, daily, tf)
    periods = t["_period"].to_list()
    closes = t["close"].to_list()
    df = bronze(periods, closes, tz)
    df = df.with_columns(pl.Series("open", t["open"].to_list()), pl.Series("high", t["high"].to_list()),
                         pl.Series("low", t["low"].to_list()), pl.Series("volume", t["volume"].to_list()))
    return proc.process_symbol(df, "X", market, tf, tz_hint=tz)


def rebuild(proc, coarse, daily, tf, **kw):
    kw.setdefault("symbol", "X")
    kw.setdefault("market", "us_stocks")
    return proc.rebuild_coarse_from_daily(coarse, daily, timeframe=tf, **kw)


def pingpong_closes(n=100):
    """Three bars at half the level inside an otherwise smooth walk: two factor-2 jumps, three bars apart."""
    return [x / 2 if 40 <= i < 43 else x for i, x in enumerate(walk(n))]


# ═════════════════════════════════════════════════════════════════════════════════════════════════
class TestSeamFunctions:

    def test_a_split_factor_jump_is_found_in_both_directions(self):
        seams = P.find_split_seams([100, 101, 50.5, 51, 102, 103])
        assert [(i, k, d) for i, k, d in seams] == [(2, 2.0, "down"), (4, 2.0, "up")]

    def test_the_tolerance_is_eight_percent_around_the_factor(self):
        assert P.find_split_seams([100, 193])            # 1.93x: a 2x split riding on -3.5%
        assert not P.find_split_seams([100, 180])        # 1.80x: not a split factor
        assert not P.find_split_seams([100, 150])        # +50%: an ordinary large move

    def test_jumps_that_undo_each_other_within_the_window_reverse(self):
        seams = P.find_split_seams([100, 50, 51, 102, 103])
        assert P.reversing_seams(seams) == {0, 1}

    def test_a_reversal_later_than_the_window_is_not_a_reversal(self):
        closes = [100.0] + [50.0] * 40 + [100.0]
        assert len(P.find_split_seams(closes)) == 2
        assert P.reversing_seams(P.find_split_seams(closes)) == set()
        assert P.SEAM_REVERT_BARS < 40

    def test_jumps_of_different_factors_do_not_pair(self):
        assert P.reversing_seams(P.find_split_seams([100, 50, 51, 153, 154])) == set()

    def test_a_constant_ratio_over_enough_bars_is_found(self):
        assert P.constant_ratio_step([0.5] * 20 + [1.0] * 5, 1e-3) == pytest.approx(0.5)

    def test_a_varying_ratio_or_too_few_bars_is_not_a_step(self):
        assert P.constant_ratio_step([1.02 + 0.01 * i for i in range(30)], 1e-3) is None
        assert P.constant_ratio_step([0.5] * (P.BASIS_MIN_PERIODS - 1), 1e-3) is None

    def test_the_audit_uses_the_processors_rules(self):
        for name in ("SEAM_TOL", "SEAM_REVERT_BARS", "SPLIT_FACTORS", "BASIS_MIN_PERIODS", "BASIS_SHARE",
                     "BASIS_BAND", "BASIS_FAIL_LOG"):
            assert getattr(A, name) is getattr(P, name), name
        assert A.find_split_seams is P.find_split_seams and A.reversing_seams is P.reversing_seams


# ═════════════════════════════════════════════════════════════════════════════════════════════════
class TestSeamQuarantine:

    def test_a_reversing_series_is_quarantined_whole(self, proc):
        days = bdays(date(2026, 1, 5), 100)
        s = silver(proc, days, pingpong_closes(100), "1D")
        assert s["is_clean"].sum() == 0 and s.height == 100

    def test_the_quarantine_is_recomputed_from_the_data(self, proc):
        days = bdays(date(2026, 1, 5), 100)
        assert silver(proc, days, walk(100), "1D")["is_clean"].sum() > 90      # the glitch gone: clean again

    def test_one_persistent_jump_is_not_quarantined(self, proc):
        days = bdays(date(2026, 1, 5), 100)
        c = [x * 0.5 if i >= 50 else x for i, x in enumerate(walk(100))]
        s = silver(proc, days, c, "1D")
        assert s["is_clean"].sum() > 90

    def test_it_applies_to_1d_only(self, proc):
        """SIL-SEAM-SCOPE-01: 1H is out, and so are 1W / 1M (a reversal there is an ordinary move)."""
        days = bdays(date(2026, 1, 5), 100)
        assert silver(proc, days, pingpong_closes(100), "1H")["is_clean"].sum() > 0
        assert silver(proc, days, pingpong_closes(100), "1D")["is_clean"].sum() == 0
        assert silver(proc, days, pingpong_closes(100), "1W")["is_clean"].sum() > 0
        assert silver(proc, days, pingpong_closes(100), "1M")["is_clean"].sum() > 0

    def test_a_weekly_series_with_a_reversing_seam_is_not_quarantined(self, proc):
        """The 18 Nov rule quarantined this whole series (27 coarse series on live Silver). 25 weekly bars are six
        months; a volatile name halves and recovers inside that. Only the two jump bars fail the z-score test."""
        mondays = [date(2026, 1, 5) + timedelta(days=7 * i) for i in range(40)]
        c = [x / 2 if 20 <= i < 22 else x for i, x in enumerate(walk(40))]
        s = silver(proc, mondays, c, "1W")
        assert s["is_clean"].sum() >= 35 and s.height == 40


# ═════════════════════════════════════════════════════════════════════════════════════════════════
class TestRebuild:

    def test_a_coarse_series_that_already_equals_1d_is_left_alone(self, proc):
        _, d = daily_silver(proc)
        w = coarse_from_truth(proc, d, "1W")
        out, st = rebuild(proc, w, d, "1W")
        assert st["status"] == "rebuilt" and st["replaced"] == 0 and st["filled"] == 0
        assert out.height == w.height and not out["rebuilt_from_1d"].any()
        assert out["close"].to_list() == w["close"].to_list()

    def test_a_stale_in_progress_bar_is_replaced(self, proc):
        _, d = daily_silver(proc)
        w = coarse_from_truth(proc, d, "1W")
        bad = w.with_columns(pl.when(pl.int_range(pl.len()) == w.height - 1).then(pl.col("close") * 1.05)
                             .otherwise(pl.col("close")).alias("close"))
        out, st = rebuild(proc, bad, d, "1W")
        assert st["replaced"] == 1 and out["rebuilt_from_1d"].sum() == 1
        assert out["close"][-1] == pytest.approx(w["close"][-1], rel=1e-9) and out["rebuilt_from_1d"][-1]

    def test_tuesday_blocks_become_monday_weeks_closing_on_the_friday(self, proc):
        """ABBV: the bar stamped Tue 11 Aug closes on Monday 17 Aug; rebuilt it is Mon 10 Aug and closes Fri 14."""
        days, d = daily_silver(proc, n=130)
        t = truth(proc, d, "1W")
        tues = [p + timedelta(days=1) for p in t["_period"].to_list()]
        # Tue..Mon block closes = the daily close of the Monday AFTER the stamp (the last daily of [Tue, next Tue))
        dm = dict(zip(d["timestamp"].dt.date().to_list(), d["close"].to_list()))
        closes = []
        for s in tues:
            last = max(x for x in dm if s <= x < s + timedelta(days=7))
            closes.append(dm[last])
        tue_w = proc.process_symbol(bronze(tues, closes), "X", "us_stocks", "1W", tz_hint=NY)
        out, st = rebuild(proc, tue_w, d, "1W")
        assert st["replaced"] >= 20                                          # every eligible week: ~26 in the fixture
        assert {x.weekday() for x in out["timestamp"].dt.date().to_list()[2:]} == {0}
        row = out.filter(pl.col("timestamp").dt.date() == t["_period"][5]).row(0, named=True)
        assert row["close"] == pytest.approx(t["close"][5], rel=1e-12) and row["rebuilt_from_1d"]

    def test_an_absent_month_is_filled_at_the_first_of_the_month(self, proc):
        """AG: a Sunday-1st month missing from Bronze 1M."""
        _, d = daily_silver(proc, n=170)
        m = coarse_from_truth(proc, d, "1M")
        gone = m["timestamp"].dt.date().to_list()[2]
        short = m.filter(pl.col("timestamp").dt.date() != gone)
        out, st = rebuild(proc, short, d, "1M")
        assert st["filled"] == 1 and out.height == m.height
        row = out.filter(pl.col("timestamp").dt.date() == gone).row(0, named=True)
        assert row["rebuilt_from_1d"] and row["timestamp"].hour == 0 and gone.day == 1

    def test_the_first_1d_period_and_the_history_before_1d_keep_their_bronze_bar(self, proc):
        days, d = daily_silver(proc, n=130, start=date(2026, 1, 7))              # a Wednesday: first week partial
        t = truth(proc, d, "1W")
        first = t["_period"][0]
        old = [first - timedelta(days=7 * k) for k in (3, 2, 1)]
        cp = [first] + t["_period"].to_list()[1:6]
        w = proc.process_symbol(bronze(old + cp, [90.0, 91.0, 92.0, 55.0] + t["close"].to_list()[1:6]),
                                "X", "us_stocks", "1W", tz_hint=NY)
        out, st = rebuild(proc, w, d, "1W")
        kept = out.filter(~pl.col("rebuilt_from_1d"))
        assert kept["close"].to_list()[:4] == [90.0, 91.0, 92.0, 55.0]            # history + the partial first week
        assert st["status"] == "rebuilt"

    def test_fx_is_excluded(self, proc):
        _, d = daily_silver(proc, market="forex")
        w = coarse_from_truth(proc, d, "1W", market="forex")
        bad = w.with_columns(pl.col("close") * 1.01)
        out, st = rebuild(proc, bad, d, "1W", market="forex", skip_reason=P.coarse_rebuild_exclusion("forex"))
        assert st["status"] == "excluded_fx" and not out["rebuilt_from_1d"].any()
        assert out["close"].to_list() == bad["close"].to_list()

    def test_exclusion_names_layer1_forex_and_the_two_context_fx_categories_only(self):
        assert P.coarse_rebuild_exclusion("forex") == "fx"
        assert P.coarse_rebuild_exclusion("context", "context_dollar_basket") == "fx"
        assert P.coarse_rebuild_exclusion("context", "context_fx_normalization") == "fx"
        assert P.coarse_rebuild_exclusion("context", "context_dollar") is None       # DXY is an index
        assert P.coarse_rebuild_exclusion("us_stocks") is None
        assert P.coarse_rebuild_exclusion("commodity") is None

    def test_without_a_1d_nothing_changes(self, proc):
        _, d = daily_silver(proc)
        w = coarse_from_truth(proc, d, "1W")
        for none in (None, d.head(0)):
            out, st = rebuild(proc, w, none, "1W")
            assert st["status"] == "no_1d" and not out["rebuilt_from_1d"].any()

    def test_a_1d_with_a_split_seam_is_never_the_source(self, proc):
        """MNST: 1D is the stale side; the correct 1M must not be overwritten by it."""
        days = bdays(date(2026, 1, 5), 130)
        c = pingpong_closes(130)
        d = silver(proc, days, c, "1D")
        m_good = silver(proc, [date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1), date(2026, 4, 1)],
                        [100.0, 101.0, 102.0, 103.0], "1M")
        out, st = rebuild(proc, m_good, d, "1M")
        assert st["status"] == "skipped_1d_seam" and not out["rebuilt_from_1d"].any()
        assert out["close"].to_list() == m_good["close"].to_list()
        assert out["is_clean"].sum() > 0          # the independent (right) side is not quarantined with the stale 1D

    def test_a_persistent_1d_jump_does_not_block_the_rebuild(self, proc):
        """SIL-SEAM-SCOPE-01: NG (2026-01-29) and MNTS (2026-05-26) each hold one persistent 2x day; the 18 Nov guard
        skipped the rebuild for 38 such symbols and left a stale November fragment and Wednesday-stamped bars."""
        days = bdays(date(2026, 1, 5), 130)
        c = [x * 0.5 if i >= 60 else x for i, x in enumerate(walk(130))]
        d = silver(proc, days, c, "1D")
        w = coarse_from_truth(proc, d, "1W")
        bad = w.with_columns(pl.when(pl.int_range(pl.len()) == w.height - 1).then(pl.col("close") * 1.05)
                             .otherwise(pl.col("close")).alias("close"))
        out, st = rebuild(proc, bad, d, "1W")
        assert st["status"] == "rebuilt" and st["replaced"] == 1 and out["rebuilt_from_1d"][-1]

    def test_a_split_like_constant_basis_is_not_rebuilt(self, proc):
        _, d = daily_silver(proc)
        w = coarse_from_truth(proc, d, "1W")
        half = proc.process_symbol(
            bronze(w["timestamp"].dt.date().to_list(), [x / 2 for x in w["close"].to_list()]), "X", "us_stocks", "1W",
            tz_hint=NY)
        out, st = rebuild(proc, half, d, "1W")
        assert st["status"] == "skipped_basis" and not out["rebuilt_from_1d"].any()

    def test_a_dividend_sized_constant_basis_is_the_repair(self, proc):
        """ALL / CSX 1W: 1.00416 on 519 of 533 weeks; 1D is the fresher vintage, the rebuild fixes it."""
        _, d = daily_silver(proc)
        w = coarse_from_truth(proc, d, "1W")
        stale = proc.process_symbol(
            bronze(w["timestamp"].dt.date().to_list(), [x * 1.00416 for x in w["close"].to_list()]), "X", "us_stocks",
            "1W", tz_hint=NY)
        out, st = rebuild(proc, stale, d, "1W")
        assert st["status"] == "rebuilt" and st["replaced"] >= stale.height - 2
        ok = out.filter(pl.col("rebuilt_from_1d"))["close"].to_list()
        ref = {r["timestamp"]: r["close"] for r in w.iter_rows(named=True)}
        assert all(abs(c / ref[t] - 1) < 1e-9 for t, c in zip(out.filter(pl.col("rebuilt_from_1d"))["timestamp"], ok))

    def test_the_rebuilt_bar_is_the_aggregate_of_its_daily_bars(self, proc):
        days, d = daily_silver(proc)
        t = truth(proc, d, "1W")
        w = coarse_from_truth(proc, d, "1W").with_columns(pl.col("close") * 1.1)   # force replacement everywhere
        w = w.with_columns(pl.col("close").clip(upper_bound=pl.col("high")))
        out, _ = rebuild(proc, w, d, "1W")
        dd = d.with_columns(P.period_key_expr("1W").alias("_p"))
        k = t["_period"][7]
        grp = dd.filter(pl.col("_p") == k).sort("timestamp")
        row = out.filter(pl.col("timestamp").dt.date() == k).row(0, named=True)
        assert row["open"] == grp["open"][0] and row["high"] == grp["high"].max() and row["low"] == grp["low"].min()
        assert row["close"] == grp["close"][-1] and row["volume"] == grp["volume"].sum()

    def test_a_crash_bar_flagged_unclean_in_1d_still_belongs_to_its_week(self, proc):
        days = bdays(date(2026, 1, 5), 130)
        c = walk(130)
        c[60] = c[60] * 0.7                                                 # a real -30% day, not a split factor
        d = silver(proc, days, c, "1D")
        assert not d["is_clean"][60]
        w = coarse_from_truth(proc, d, "1W").with_columns(pl.col("close") * 1.1)
        w = w.with_columns(pl.col("close").clip(upper_bound=pl.col("high")))
        out, _ = rebuild(proc, w, d, "1W")
        k = d.with_columns(P.period_key_expr("1W").alias("_p"))["_p"][60]
        assert out.filter(pl.col("timestamp").dt.date() == k)["low"][0] <= c[60] - 1.2 + 1e-9

    def test_log_return_is_recomputed_across_the_replaced_rows(self, proc):
        _, d = daily_silver(proc)
        w = coarse_from_truth(proc, d, "1W").with_columns(pl.col("close") * 1.0)
        w = w.with_columns(pl.when(pl.int_range(pl.len()) == 10).then(pl.col("close") * 0.97)
                           .otherwise(pl.col("close")).alias("close"))
        out, st = rebuild(proc, w, d, "1W")
        assert st["replaced"] >= 1
        lr = (out["close"] / out["close"].shift(1)).log().to_list()
        assert all(abs(a - b) < 1e-12 for a, b in zip(out["log_return"].to_list()[1:], lr[1:]))

    def test_stamps_keep_the_dtype_and_local_midnight_across_a_dst_change(self, proc):
        _, d = daily_silver(proc, n=130, start=date(2026, 2, 2))           # DST starts Sun 8 Mar 2026 in New York
        w = coarse_from_truth(proc, d, "1W").with_columns(pl.col("close") * 1.1)
        w = w.with_columns(pl.col("close").clip(upper_bound=pl.col("high")))
        out, _ = rebuild(proc, w, d, "1W")
        assert out.schema["timestamp"] == w.schema["timestamp"]
        assert {t.hour for t in out["timestamp"].to_list()} == {0}

    def test_a_non_dst_zone_keeps_its_local_midnight(self, proc):
        _, d = daily_silver(proc, tz="Asia/Jakarta", market="idx")
        w = coarse_from_truth(proc, d, "1M", tz="Asia/Jakarta", market="idx").with_columns(pl.col("close") * 1.1)
        w = w.with_columns(pl.col("close").clip(upper_bound=pl.col("high")))
        out, st = rebuild(proc, w, d, "1M", market="idx")
        assert st["replaced"] >= 1
        assert {(t.day, t.hour) for t in out["timestamp"].to_list()} == {(1, 0)}

    def test_the_same_inputs_give_the_same_output_and_the_flag_is_always_a_column(self, proc):
        _, d = daily_silver(proc)
        w = coarse_from_truth(proc, d, "1W").with_columns(pl.col("close") * 1.002)
        a, _ = rebuild(proc, w, d, "1W")
        b, _ = rebuild(proc, w, d, "1W")
        assert a.equals(b) and "rebuilt_from_1d" in a.columns and a.columns[-1] == "rebuilt_from_1d"
        assert a["processing_version"].unique().to_list() == [P.CURRENT_SILVER_VERSION]

    def test_the_output_schema_is_the_coarse_schema_plus_the_flag(self, proc):
        _, d = daily_silver(proc)
        w = coarse_from_truth(proc, d, "1W").with_columns(pl.col("close") * 1.002)
        out, _ = rebuild(proc, w, d, "1W")
        assert out.columns == w.columns + ["rebuilt_from_1d"]
        assert {c: out.schema[c] for c in w.columns} == dict(w.schema)

    def test_a_timeframe_that_is_not_coarse_is_returned_untouched(self, proc):
        _, d = daily_silver(proc)
        out, st = rebuild(proc, d, d, "1D")
        assert st["status"] == "not_coarse" and out is d


# ═════════════════════════════════════════════════════════════════════════════════════════════════
class TestEndToEnd:

    @staticmethod
    def _write(root: Path, df: pl.DataFrame, market, sym, tf):
        out = root / market / f"symbol={sym}"
        out.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out / f"{sym}_{tf}_silver.parquet")

    def test_the_rebuilt_series_pass_the_audit_the_bronze_built_ones_failed(self, proc, tmp_path):
        """ABBV-shaped Tue..Mon weeks: leak FAIL before, no leak / window / anchor finding after."""
        days, d = daily_silver(proc, n=160)
        t = truth(proc, d, "1W")
        dm = dict(zip(d["timestamp"].dt.date().to_list(), d["close"].to_list()))
        tues = [p + timedelta(days=1) for p in t["_period"].to_list()]
        closes = [dm[max(x for x in dm if s <= x < s + timedelta(days=7))] for s in tues]
        tue_w = proc.process_symbol(bronze(tues, closes), "ABBV", "us_stocks", "1W", tz_hint=NY)
        out, _ = rebuild(proc, tue_w, d, "1W", symbol="ABBV")

        def run_audit(root):
            opts = A.Options(silver_root=root, checks=("window", "leak", "anchor", "basis"), use_registry=False)
            return A.run_audit(opts, A.discover(root, ("1D", "1W")))

        before, after = tmp_path / "before", tmp_path / "after"
        for root, w in ((before, tue_w), (after, out.drop("rebuilt_from_1d"))):
            self._write(root, d, "us_stocks", "ABBV", "1D")
            self._write(root, w, "us_stocks", "ABBV", "1W")
        assert [f for f in run_audit(before).findings if f.check == "leak" and f.severity == A.FAIL]
        res = run_audit(after)
        assert not [f for f in res.findings if f.check in ("leak", "window", "basis")
                    and f.severity in (A.FAIL, A.WARN)]
        assert res.facts["gate"][0]["technical_xgboost"]["verdict"] == "PASS"

    def test_run_rebuilds_coarse_from_the_1d_written_in_the_same_run(self, tmp_path, monkeypatch):
        from tests.unit.test_ohlcv_processor import TestRunEntryPoint as T
        monkeypatch.chdir(tmp_path)
        inst = T._fake_instrument("AAPL", "us_stocks")
        monkeypatch.setattr("src.config.instrument_loader.get_loader", lambda: T._fake_loader([inst]))
        days = bdays(date(2024, 1, 1), 60)
        for tf, dates, closes in (("1D", days, walk(60)),
                                  ("1W", [days[0] + timedelta(days=7 * i) for i in range(8)], [500.0] * 8)):
            ts = pl.Series("timestamp", [datetime(x.year, x.month, x.day) for x in dates],
                           dtype=pl.Datetime("ms")).dt.replace_time_zone(NY)
            df = pl.DataFrame({"timestamp": ts, "open": [c - 0.4 for c in closes], "high": [c + 1 for c in closes],
                               "low": [c - 1.2 for c in closes], "close": closes, "volume": [1000] * len(closes)})
            out_dir = (tmp_path / P.BRONZE_OHLCV_PATH / "us_stocks" / f"timeframe={tf}" / "source=yfinance"
                       / "symbol=AAPL" / "year=2024" / "month=01")
            out_dir.mkdir(parents=True, exist_ok=True)
            df.write_parquet(out_dir / "AAPL_raw_fixture.parquet")
        result = P.run(date(2025, 6, 1))
        w = pl.read_parquet(tmp_path / P.SILVER_OHLCV_PATH / "us_stocks" / "symbol=AAPL" / "AAPL_1W_silver.parquet")
        assert w["rebuilt_from_1d"].sum() >= 6 and w["processing_version"][0] == P.CURRENT_SILVER_VERSION
        assert not [x for x in result.warnings if "not rebuilt" in x]

    def test_a_failure_in_the_rebuild_keeps_the_bronze_bars_and_is_reported(self, tmp_path, monkeypatch, proc):
        monkeypatch.chdir(tmp_path)
        from tests.unit.test_ohlcv_processor import TestRunEntryPoint as T
        inst = T._fake_instrument("AAPL", "us_stocks")
        _, d = daily_silver(proc)
        self._write(tmp_path / P.SILVER_OHLCV_PATH, d, "us_stocks", "AAPL", "1D")
        w = coarse_from_truth(proc, d, "1W")

        def boom(*a, **k):
            raise RuntimeError("boom")
        monkeypatch.setattr(OHLCVProcessor, "rebuild_coarse_from_daily", boom)
        notes: list[str] = []
        out = P._rebuild_coarse(proc, w, inst, "1W", notes)
        assert notes == ["AAPL/1W: rebuild_error"] and not out["rebuilt_from_1d"].any()
        assert out["close"].to_list() == w["close"].to_list()
        assert P._rebuild_warnings("silver_ohlcv", notes)[0].startswith("silver_ohlcv: 1 1W/1M series not rebuilt")

    def test_a_seam_skip_is_surfaced_as_a_job_warning(self, tmp_path, monkeypatch, proc):
        monkeypatch.chdir(tmp_path)
        from tests.unit.test_ohlcv_processor import TestRunEntryPoint as T
        inst = T._fake_instrument("MNST", "us_stocks")
        days = bdays(date(2026, 1, 5), 130)
        self._write(tmp_path / P.SILVER_OHLCV_PATH, silver(proc, days, pingpong_closes(130), "1D"),
                    "us_stocks", "MNST", "1D")
        m = silver(proc, [date(2026, 1, 1), date(2026, 2, 1)], [100.0, 101.0], "1M")
        notes: list[str] = []
        P._rebuild_coarse(proc, m, inst, "1M", notes)
        assert notes == ["MNST/1M: skipped_1d_seam"]
        assert "MNST/1M: skipped_1d_seam" in P._rebuild_warnings("silver_ohlcv", notes)[0]
        assert P._rebuild_warnings("silver_ohlcv", []) == ()

    def test_the_processor_does_not_read_parquet_eagerly(self):
        src = Path(P.__file__).read_text()
        assert not [l for l in src.splitlines() if "pl.read_parquet(" in l and not l.lstrip().startswith("#")]


# ═════════════════════════════════════════════════════════════════════════════════════════════════
class TestVersion:

    def test_the_rebuild_and_the_quarantine_changed_silver_semantics_so_the_version_moved_past_1_2(self):
        """1.3 = 1W/1M rebuilt from 1D + the rebuilt_from_1d column + split-seam quarantine. The audit's `meta`
        check then reports every file written by the old code as `rebuild pending`."""
        assert tuple(int(x) for x in P.CURRENT_SILVER_VERSION.split(".")) >= (1, 3)

    def test_scoping_the_quarantine_and_the_guard_and_positive_prices_moved_it_to_1_4(self):
        """1.4 = SIL-SEAM-SCOPE-01 + SIL-OHLC-POSITIVE-01: is_clean and the rebuild set changed for files already
        written at 1.3, so `meta` must report them as `rebuild pending` (that is the rollout's verification)."""
        assert tuple(int(x) for x in P.CURRENT_SILVER_VERSION.split(".")) >= (1, 4)


class TestRunWiring:
    """The rebuild is wired into run() and run_context(): the JobResult warning, the context FX exclusion."""

    @staticmethod
    def _bronze_file(tmp_path, market, tf, symbol, dates, closes, tz=NY):
        ts = pl.Series("timestamp", [datetime(x.year, x.month, x.day) for x in dates],
                       dtype=pl.Datetime("ms")).dt.replace_time_zone(tz)
        df = pl.DataFrame({"timestamp": ts, "open": [c - 0.4 for c in closes], "high": [c + 1 for c in closes],
                           "low": [c - 1.2 for c in closes], "close": closes, "volume": [1000] * len(closes)})
        out = (tmp_path / P.BRONZE_OHLCV_PATH / market / f"timeframe={tf}" / "source=yfinance"
               / f"symbol={symbol}" / "year=2026" / "month=01")
        out.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out / f"{symbol}_raw_fixture.parquet")

    def test_run_reports_a_1d_seam_skip_in_the_job_result_and_quarantines_the_1d(self, tmp_path, monkeypatch):
        from tests.unit.test_ohlcv_processor import TestRunEntryPoint as T
        monkeypatch.chdir(tmp_path)
        inst = T._fake_instrument("MNST", "us_stocks")
        monkeypatch.setattr("src.config.instrument_loader.get_loader", lambda: T._fake_loader([inst]))
        days = bdays(date(2026, 1, 5), 130)
        self._bronze_file(tmp_path, "us_stocks", "1D", "MNST", days, pingpong_closes(130))
        months = [date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1), date(2026, 4, 1)]
        self._bronze_file(tmp_path, "us_stocks", "1M", "MNST", months, [100.0, 101.0, 102.0, 103.0])
        result = P.run(date(2026, 6, 1))
        assert any("MNST/1M: skipped_1d_seam" in w for w in result.warnings)
        base = tmp_path / P.SILVER_OHLCV_PATH / "us_stocks" / "symbol=MNST"
        assert pl.read_parquet(base / "MNST_1D_silver.parquet")["is_clean"].sum() == 0
        m = pl.read_parquet(base / "MNST_1M_silver.parquet")
        assert not m["rebuilt_from_1d"].any() and m["close"].to_list() == [100.0, 101.0, 102.0, 103.0]

    def test_run_context_rebuilds_a_non_fx_context_series_and_leaves_context_fx_alone(self, tmp_path, monkeypatch):
        from dataclasses import replace
        from tests.unit.test_ohlcv_processor import TestRunContextEntryPoint as C
        monkeypatch.chdir(tmp_path)
        vix = C._fake_context_instrument("VIX")
        krw = replace(C._fake_context_instrument("KRW"), context_category="context_dollar_basket")
        monkeypatch.setattr("src.config.instrument_loader.get_loader", lambda: C._fake_loader([vix, krw]))
        days = bdays(date(2026, 1, 5), 130)
        mondays = [days[0] + timedelta(days=7 * i) for i in range(12)]
        for sym in ("VIX", "KRW"):
            self._bronze_file(tmp_path, "context", "1D", sym, days, walk(130))
            self._bronze_file(tmp_path, "context", "1W", sym, mondays, [500.0] * 12)
        P.run_context(date(2026, 6, 1))
        base = tmp_path / P.SILVER_OHLCV_PATH / "context"
        vix_w = pl.read_parquet(base / "symbol=VIX" / "VIX_1W_silver.parquet")
        krw_w = pl.read_parquet(base / "symbol=KRW" / "KRW_1W_silver.parquet")
        assert vix_w["rebuilt_from_1d"].sum() >= 10
        assert not krw_w["rebuilt_from_1d"].any() and set(krw_w["close"].to_list()) == {500.0}

    def test_both_run_loops_call_the_rebuild_for_coarse_timeframes_only(self):
        src = Path(P.__file__).read_text()
        assert src.count("silver_df = _rebuild_coarse(proc, silver_df, inst, tf,") == 2
        assert src.count("if tf in _PERIOD_DEDUP_TIMEFRAMES and silver_df is not None") == 2
