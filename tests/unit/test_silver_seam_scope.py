"""
FIX SIL-SEAM-SCOPE-01 + SIL-OHLC-POSITIVE-01 [chat thread, 19 Nov 2026] - the 18 Nov seam quarantine and rebuild
guard were too broad, and `_flag_is_clean` had no price > 0 test.

Measured on silver_audit_2026-11-19.json and real Silver files (see the block above COARSE_REBUILD_TOL in the
processor): 27 coarse series quarantined whole (CVNA 1D has no seam and its 1M closes equal the 1D month-end closes;
MARA 1W reconciles 532 of 532 periods yet 0 of 760 rows were clean), 38 symbols whose rebuild was skipped by one
persistent 2x day (NG, MNTS), and CL's April-2020 1W/1M bars with low <= 0 marked is_clean = TRUE.
Fixtures are healthy by default so each test reaches exactly one branch.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

import src.silver.ohlcv_processor as P
from src.silver.ohlcv_processor import OHLCVProcessor, find_split_seams, reversing_seams
from tests.unit.test_silver_coarse_rebuild import (
    NY, bdays, bronze, coarse_from_truth, daily_silver, pingpong_closes, rebuild, silver, truth, walk,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import audit_silver_ohlcv as A  # noqa: E402


@pytest.fixture
def proc():
    return OHLCVProcessor()


def trend(factors, start=date(2025, 1, 1), base=100.0):
    """Business days and closes where each calendar month moves by ``factors[i]`` through MANY daily steps
    (a trend, not a step): the month-end close is the previous month-end times the factor."""
    days, closes, prev = [], [], base
    y, m = start.year, start.month
    for f in factors:
        first = date(y, m, 1)
        nxt = date(y + (m == 12), m % 12 + 1, 1)
        month_days = [d for d in bdays(first, 23) if d < nxt]
        k = len(month_days)
        for i, d in enumerate(month_days):
            days.append(d)
            closes.append(round(prev * f ** ((i + 1) / k), 6))
        prev = closes[-1]
        y, m = nxt.year, nxt.month
    return days, closes


# a volatile name: doubles twice, halves twice, doubles, halves (CVNA 2022-23 / MARA 2017-18 shape)
VOLATILE = (1.0, 2.0, 2.0, 0.5, 0.5, 2.0, 0.5, 1.0, 1.0, 1.0)


# =================================================================================================
class TestAVolatileNameKeepsItsCoarseSeries:

    def test_premise_the_daily_series_has_no_seam_and_the_monthly_one_reverses(self, proc):
        days, closes = trend(VOLATILE)
        assert find_split_seams(closes) == []                         # no single-day step anywhere
        d = silver(proc, days, closes, "1D")
        m_closes = truth(proc, d, "1M")["close"].to_list()
        assert reversing_seams(find_split_seams(m_closes))            # the 18 Nov rule would have fired here

    def test_the_1d_series_is_clean(self, proc):
        days, closes = trend(VOLATILE)
        d = silver(proc, days, closes, "1D")
        assert d["is_clean"].sum() >= d.height - 1

    def test_a_bronze_built_1m_with_reversing_2x_months_is_clean(self, proc):
        days, closes = trend(VOLATILE)
        d = silver(proc, days, closes, "1D")
        m = coarse_from_truth(proc, d, "1M")
        assert m["is_clean"].sum() >= m.height - 2

    def test_the_rebuilt_1m_with_reversing_2x_months_is_clean(self, proc):
        days, closes = trend(VOLATILE)
        d = silver(proc, days, closes, "1D")
        m = coarse_from_truth(proc, d, "1M")
        stale = m.with_columns(pl.when(pl.int_range(pl.len()) == m.height - 1).then(pl.col("close") * 1.05)
                               .otherwise(pl.col("close")).alias("close"))
        out, st = rebuild(proc, stale, d, "1M")
        assert st["status"] == "rebuilt" and st["replaced"] == 1
        assert out["is_clean"].sum() >= out.height - 2

    def test_the_audit_does_not_fail_it_and_judges_it_against_1d(self, proc, tmp_path):
        days, closes = trend(VOLATILE)
        d = silver(proc, days, closes, "1D")
        m = coarse_from_truth(proc, d, "1M")
        for tf, df in (("1D", d), ("1M", m)):
            out = tmp_path / "us_stocks" / "symbol=CVNA"
            out.mkdir(parents=True, exist_ok=True)
            df.write_parquet(out / f"CVNA_{tf}_silver.parquet")
        res = A.run_audit(A.Options(silver_root=tmp_path, checks=("seam", "basis", "window"), use_registry=False),
                          A.discover(tmp_path, ("1D", "1M")))
        assert not [f for f in res.findings if f.severity == A.FAIL]
        g = res.facts["gate"][0]["technical_xgboost"]
        assert g["quarantined_count"] == 0 and g["verdict"] != "BLOCK"


# =================================================================================================
class TestACoarseSeriesInheritsAQuarantined1DOnlyWhenItReproducesIt:
    """MNST: 1D alternates between two levels (quarantined). The 1W, built from the same vendor feed, equals that
    1D period by period and carries its wrong basis; the 1M is the independent, right side."""

    @staticmethod
    def _glitchy(proc):
        days = bdays(date(2026, 1, 5), 130)
        d = silver(proc, days, pingpong_closes(130), "1D")
        assert d["is_clean"].sum() == 0                               # premise: the 1D is quarantined
        return d

    def test_a_coarse_series_equal_to_the_quarantined_1d_is_quarantined(self, proc):
        d = self._glitchy(proc)
        w = coarse_from_truth(proc, d, "1W")
        assert w["is_clean"].sum() > 0                                # before the guard: clean on its own
        out, st = rebuild(proc, w, d, "1W")
        assert st["status"] == "skipped_1d_seam" and not out["rebuilt_from_1d"].any()
        assert out["is_clean"].sum() == 0 and out.height == w.height
        assert out["close"].to_list() == w["close"].to_list()         # quarantined, not altered

    def test_a_coarse_series_that_differs_from_it_is_left_clean(self, proc):
        d = self._glitchy(proc)
        m_good = silver(proc, [date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1), date(2026, 4, 1)],
                        [100.0, 101.0, 102.0, 103.0], "1M")
        out, st = rebuild(proc, m_good, d, "1M")
        assert st["status"] == "skipped_1d_seam"
        assert out["is_clean"].sum() == m_good["is_clean"].sum() > 0

    def test_the_inheritance_share_is_95_percent_of_the_periods(self, proc):
        d = self._glitchy(proc)
        w = coarse_from_truth(proc, d, "1W")

        def perturb(k):
            idx = set(range(2, 2 + k))
            return w.with_columns(pl.when(pl.int_range(pl.len()).is_in(list(idx))).then(pl.col("close") * 1.2)
                                  .otherwise(pl.col("close")).alias("close"))

        assert rebuild(proc, perturb(1), d, "1W")[0]["is_clean"].sum() == 0          # ~96% equal: inherits
        assert rebuild(proc, perturb(4), d, "1W")[0]["is_clean"].sum() > 0           # ~84% equal: independent

    def test_the_thresholds(self):
        assert P.SEAM_QUARANTINE_TIMEFRAMES == frozenset({"1D"})
        assert P.SEAM_INHERIT_SHARE == 0.95
        assert P.SEAM_BASIS_MIN_PERIODS == 3 < P.BASIS_MIN_PERIODS

    def test_run_wiring_quarantines_the_mirroring_series_and_reports_it(self, proc, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from tests.unit.test_ohlcv_processor import TestRunEntryPoint as T
        inst = T._fake_instrument("MNST", "us_stocks")
        d = self._glitchy(proc)
        out_dir = tmp_path / P.SILVER_OHLCV_PATH / "us_stocks" / "symbol=MNST"
        out_dir.mkdir(parents=True)
        d.write_parquet(out_dir / "MNST_1D_silver.parquet")
        w = coarse_from_truth(proc, d, "1W")
        notes: list[str] = []
        got = P._rebuild_coarse(proc, w, inst, "1W", notes)
        assert notes == ["MNST/1W: skipped_1d_seam"] and got["is_clean"].sum() == 0


# =================================================================================================
class TestAPersistentSeamNoLongerBlocksTheRebuild:

    @staticmethod
    def _jump_daily(proc, n=130, at=60, factor=0.5):
        days = bdays(date(2026, 1, 5), n)
        c = [x * factor if i >= at else x for i, x in enumerate(walk(n))]
        return days, silver(proc, days, c, "1D")

    def test_wednesday_stamped_weekly_bars_are_restamped_despite_a_persistent_seam(self, proc):
        """MNTS: Wednesday-stamped weeks (2 days of lookahead); one persistent 2x day in its 1D."""
        _, d = self._jump_daily(proc)
        t = truth(proc, d, "1W")
        weds = [p + timedelta(days=2) for p in t["_period"].to_list()]
        dm = dict(zip(d["timestamp"].dt.date().to_list(), d["close"].to_list()))
        closes = [dm[max(x for x in dm if s <= x < s + timedelta(days=7))] for s in weds]
        wed_w = proc.process_symbol(bronze(weds, closes), "X", "us_stocks", "1W", tz_hint=NY)
        out, st = rebuild(proc, wed_w, d, "1W")
        assert st["status"] == "rebuilt" and st["replaced"] >= 20
        assert {x.weekday() for x in out["timestamp"].dt.date().to_list()[2:]} == {0}

    def test_a_missing_month_is_filled_despite_a_persistent_seam(self, proc):
        """NG: 17 rebuildable months missing and a stale November fragment."""
        _, d = self._jump_daily(proc, n=170, at=100)
        m = coarse_from_truth(proc, d, "1M")
        gone = m["timestamp"].dt.date().to_list()[2]
        out, st = rebuild(proc, m.filter(pl.col("timestamp").dt.date() != gone), d, "1M")
        assert st["status"] == "rebuilt" and st["filled"] == 1 and out.height == m.height

    def test_a_recent_unadjusted_split_in_1d_is_still_caught(self, proc):
        """1D unadjusted before a 2:1 split ~4 months ago, vendor monthly bars adjusted (right): only 4 mismatching
        months, fewer than BASIS_MIN_PERIODS, but 1D holds a persistent seam so the test drops to 3 and skips."""
        n, at = 170, 85
        days = bdays(date(2026, 1, 5), n)
        base = walk(n)
        right = silver(proc, days, base, "1D")
        vendor = coarse_from_truth(proc, right, "1M")
        wrong = silver(proc, days, [x * 2 if i < at else x for i, x in enumerate(base)], "1D")
        assert find_split_seams(wrong["close"].to_list()) and not reversing_seams(find_split_seams(wrong["close"].to_list()))
        out, st = rebuild(proc, vendor, wrong, "1M")
        assert st["status"] == "skipped_basis" and not out["rebuilt_from_1d"].any()
        assert out["close"].to_list() == vendor["close"].to_list()

    def test_without_a_seam_four_constant_mismatches_still_need_twelve(self, proc):
        """The default test is unchanged: only a 1D that holds a seam lowers the minimum."""
        _, d = daily_silver(proc, n=170)
        m = coarse_from_truth(proc, d, "1M")
        stale = m.with_columns(pl.when(pl.int_range(pl.len()).is_between(1, 4)).then(pl.col("close") * 0.5)
                               .otherwise(pl.col("close")).alias("close"))
        out, st = rebuild(proc, stale, d, "1M")
        assert st["status"] == "rebuilt" and st["replaced"] == 4

    def test_a_reversing_seam_still_skips_with_the_rebuild_status_unchanged(self, proc):
        days = bdays(date(2026, 1, 5), 130)
        d = silver(proc, days, pingpong_closes(130), "1D")
        w = coarse_from_truth(proc, d, "1W")
        assert rebuild(proc, w, d, "1W")[1]["status"] == "skipped_1d_seam"


# =================================================================================================
class TestNonPositivePricesAreNeverClean:
    """CL 2020-04: 1D flagged only because log_return turned NaN; the vendor 1W/1M bars (low <= 0, finite log_return)
    were is_clean = TRUE and the audit's ohlc check FAILed them."""

    @staticmethod
    def _months(proc, **edit):
        dates = [date(2020, 1, 1) + timedelta(days=31 * i) for i in range(12)]
        dates = [d.replace(day=1) for d in dates]
        df = bronze(dates, [60.0 + i for i in range(12)])
        row = {"open": 60.0 + 3, "high": 63.0 + 1.0, "low": 63.0 - 1.2, "close": 63.0}
        row.update(edit)
        return proc.process_symbol(
            df.with_columns([pl.when(pl.int_range(pl.len()) == 3).then(pl.lit(v)).otherwise(pl.col(k)).alias(k)
                             for k, v in row.items() if k in edit]),
            "CL", "commodity", "1M", tz_hint=NY)

    def test_a_low_below_zero_with_a_finite_log_return_is_not_clean(self, proc):
        s = self._months(proc, low=-3.0)
        assert s["low"][3] == -3.0 and s["log_return"][3] is not None and s["log_return"][3] == s["log_return"][3]
        assert s["is_clean"][3] is False
        assert s["is_clean"].sum() >= 9                               # the rest of the series is not collateral

    def test_a_zero_low_and_open_is_not_clean(self, proc):
        """KOSPI 1M 2026-11-01: low = 0.0 and is_clean = TRUE. open = 0 as well keeps `open >= low`, so only a
        positivity test can flag it."""
        assert self._months(proc, open=0.0, low=0.0)["is_clean"][3] is False

    def test_a_healthy_series_is_unaffected(self, proc):
        s = self._months(proc)
        assert s["is_clean"].sum() >= s.height - 2

    def test_the_4h_synthesis_applies_the_same_rule(self, proc):
        df = pl.DataFrame({"open": [10.0, 10.0, 10.0], "high": [11.0, 11.0, 11.0], "low": [9.0, -1.0, 9.0],
                           "close": [10.0, 10.0, 10.0]})
        assert proc._flag_is_clean_4h(df)["is_clean"].to_list() == [True, False, True]

    def test_the_4h_synthesis_rejects_a_zero_and_an_infinite_price_too(self, proc):
        zero = pl.DataFrame({"open": [10.0, 0.0, 10.0], "high": [11.0, 11.0, 11.0], "low": [9.0, 0.0, 9.0],
                             "close": [10.0, 10.0, 10.0]})
        assert proc._flag_is_clean_4h(zero)["is_clean"].to_list() == [True, False, True]
        inf = pl.DataFrame({"open": [10.0, 10.0, 10.0], "high": [11.0, float("inf"), 11.0],
                            "low": [9.0, 9.0, 9.0], "close": [10.0, 10.0, 10.0]})
        assert proc._flag_is_clean_4h(inf)["is_clean"].to_list() == [True, False, True]

    def test_nan_and_inf_prices_are_not_clean(self, proc):
        df = pl.DataFrame({"open": [10.0, 10.0, 10.0], "high": [11.0, float("inf"), 11.0],
                           "low": [9.0, 9.0, 9.0], "close": [10.0, 10.0, 10.0]})
        assert proc._flag_is_clean(df)["is_clean"].to_list() == [True, False, True]

    def test_the_processors_output_no_longer_fails_the_audits_ohlc_check(self, proc, tmp_path):
        s = self._months(proc, low=-3.0)
        out = tmp_path / "commodity" / "symbol=CL"
        out.mkdir(parents=True)
        s.write_parquet(out / "CL_1M_silver.parquet")
        res = A.run_audit(A.Options(silver_root=tmp_path, checks=("ohlc",), use_registry=False),
                          A.discover(tmp_path, ("1M",)))
        assert not [f for f in res.findings if f.check == "ohlc" and f.severity == A.FAIL]
        assert any("price <= 0" in f.message for f in res.findings if f.severity == A.WARN)
