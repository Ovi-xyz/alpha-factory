"""
tests/unit/test_hmm_fred_pit.py — ADR-051 Amendment 2 §C: src/gold/hmm/fred_pit.py.

The load-bearing property is NO LOOKAHEAD: a panel row for as_of_date D may only
contain a value that was usable on D. Two bases (hmm.pit.basis):
    estimated  D >= max(release_date, observation_date + lag)
    recorded   D >= the date the pipeline first recorded the row (Silver _ingested_at),
               clamped to the observation date; backfilled history stays estimated.
test_matches_bruteforce_reference checks the estimated rule against an independent
brute-force implementation on a randomised multi-series fixture.
"""
import random
from datetime import date, timedelta

import polars as pl
import pytest

from src.gold.hmm import fred_pit as fp
from src.gold.hmm.config import DerivedSpec, FeatureSpec


def _obs(rows, sid="S"):
    """rows: (observation_date, value, release_date|None)"""
    return pl.DataFrame(
        {
            "series_id": [sid] * len(rows),
            "observation_date": [r[0] for r in rows],
            "release_date": [r[2] for r in rows],
            "value": [r[1] for r in rows],
        },
        schema={"series_id": pl.String, "observation_date": pl.Date,
                "release_date": pl.Date, "value": pl.Float64},
    )


def _f(name, series=None, transform="level", derived=None, domain="d"):
    return FeatureSpec(feature=name, domain=domain, series=series, transform=transform, derived=derived)


def _row(panel, d):
    r = panel.filter(pl.col("as_of_date") == d)
    assert r.height == 1, f"{d} not in panel"
    return r.to_dicts()[0]


# ── snapshot discovery + loading ─────────────────────────────────────────────

class TestSnapshots:
    def _touch(self, d, name):
        (d / name).write_bytes(b"")

    def test_latest_on_or_before_run_date(self, tmp_path):
        for n in ("fred_2026-09-19_silver.parquet", "fred_2026-09-25_silver.parquet",
                  "fred_2026-09-30_silver.parquet"):
            self._touch(tmp_path, n)
        assert fp.latest_snapshot(date(2026, 9, 27), tmp_path).name == "fred_2026-09-25_silver.parquet"
        assert fp.latest_snapshot(date(2026, 9, 30), tmp_path).name == "fred_2026-09-30_silver.parquet"

    def test_none_when_all_after_run_date(self, tmp_path):
        self._touch(tmp_path, "fred_2026-09-25_silver.parquet")
        assert fp.latest_snapshot(date(2026, 9, 1), tmp_path) is None

    def test_ignores_other_sources_and_garbage(self, tmp_path):
        for n in ("bls_2026-09-25_silver.parquet", "fred_notadate_silver.parquet", "fred_2026-09-25.parquet"):
            self._touch(tmp_path, n)
        assert fp.latest_snapshot(date(2026, 12, 31), tmp_path) is None

    def test_missing_directory(self, tmp_path):
        assert fp.latest_snapshot(date(2026, 1, 1), tmp_path / "nope") is None


class TestLoadObservations:
    def _write(self, tmp_path, rows):
        df = pl.DataFrame(rows, orient="row", schema={
            "series_id": pl.String, "observation_date": pl.Date, "release_date": pl.String,
            "value": pl.Float64, "_ingested_at": pl.String})
        p = tmp_path / "fred_2026-09-25_silver.parquet"
        df.write_parquet(p)
        return p

    def test_parses_release_date_filters_series_and_drops_null_values(self, tmp_path):
        p = self._write(tmp_path, [
            ("A", date(2026, 1, 1), "2026-02-05", 1.0, "t1"),
            ("A", date(2026, 2, 1), "2026-03-05", None, "t1"),
            ("B", date(2026, 1, 1), "2026-02-05", 9.0, "t1"),
        ])
        out = fp.load_observations(p, ["A"])
        assert out.schema["release_date"] == pl.Date
        assert out.select("series_id", "observation_date", "release_date", "value").rows() == [
            ("A", date(2026, 1, 1), date(2026, 2, 5), 1.0)]
        # "t1" is not a timestamp: no recorded date, so the row can only be estimated
        assert out["ingested_on"].to_list() == [None] and out["backfilled"].to_list() == [True]

    def test_dedupe_keeps_latest_ingested(self, tmp_path):
        p = self._write(tmp_path, [
            ("A", date(2026, 1, 1), "2026-02-05", 1.0, "2026-02-06T00:00:00"),
            ("A", date(2026, 1, 1), "2026-02-05", 2.0, "2026-03-01T00:00:00"),
        ])
        assert fp.load_observations(p, ["A"])["value"].to_list() == [2.0]


# ── availability rule ────────────────────────────────────────────────────────

class TestAvailability:
    def test_max_of_release_and_obs_plus_lag(self):
        o = _obs([(date(2026, 4, 1), 1.5, date(2026, 5, 1))])          # GDP-shaped: release = obs + 30
        assert fp.with_availability(o, 120)["available_on"][0] == date(2026, 7, 30)   # obs + 120 wins
        assert fp.with_availability(o, 10)["available_on"][0] == date(2026, 5, 1)     # release wins

    def test_null_release_degrades_to_obs_plus_lag_not_null(self):
        o = _obs([(date(2026, 4, 1), 1.0, None)])
        assert fp.with_availability(o, 7)["available_on"][0] == date(2026, 4, 8)


class TestPitGating:
    def test_quarterly_value_not_visible_on_synthetic_release_date(self):
        """REGRESSION GUARD (Amendment 2 §C): Bronze release_date for quarterly
        series is quarter-start + 30 d while the advance estimate lands ~+120 d.
        Trusting release_date alone leaks ~90 days of lookahead — the panel must
        not contain the value anywhere before obs + 120 (2026-07-30)."""
        obs = _obs([(date(2026, 4, 1), 1.5, date(2026, 5, 1))], sid="GDP")
        panel = fp.build_panel(obs, [_f("GDP", "GDP")], lambda s: 120, end=date(2026, 8, 7))
        assert panel["as_of_date"].min() == date(2026, 7, 30)        # not 2026-05-01
        assert _row(panel, date(2026, 7, 30))["GDP"] == 1.5
        r = _row(panel, date(2026, 7, 30))
        assert (r["as_of_date"] - r["obs_date__GDP"]).days == 120
        assert r["available_on__GDP"] == date(2026, 7, 30) and r["avail_basis__GDP"] == "estimated"

    def test_weekly_batch_series_override(self):
        """release_date says next day, but the series posts in a weekly step:
        the per-series lag (8 d) governs, not release_date."""
        obs = _obs([(date(2026, 9, 15), 107.0, date(2026, 9, 16))], sid="WTI")
        panel = fp.build_panel(obs, [_f("WTI", "WTI")], lambda s: 8, end=date(2026, 9, 25))
        assert panel["as_of_date"].min() == date(2026, 9, 23)        # not 2026-09-16
        assert _row(panel, date(2026, 9, 23))["WTI"] == 107.0

    def test_late_published_older_observation_never_overrides_newer(self):
        """obs Jan is published late (after Feb); the panel must keep carrying
        Feb, not regress to Jan."""
        obs = _obs([
            (date(2026, 2, 2), 2.0, date(2026, 2, 3)),
            (date(2026, 1, 5), 1.0, date(2026, 2, 10)),     # older obs, later availability
        ])
        panel = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 2, 13))
        assert _row(panel, date(2026, 2, 9))["S"] == 2.0
        assert _row(panel, date(2026, 2, 12))["S"] == 2.0

    def test_matches_bruteforce_reference(self):
        rnd = random.Random(51)
        start = date(2020, 1, 1)
        rows = []
        for i in range(120):
            od = start + timedelta(days=rnd.randint(0, 400))
            rel = od + timedelta(days=rnd.randint(0, 40)) if rnd.random() > 0.2 else None
            rows.append((od, float(i), rel))
        rows = list({r[0]: r for r in rows}.values())            # one row per observation_date
        lag = 17
        end = date(2021, 6, 30)
        panel = fp.build_panel(_obs(rows), [_f("S", "S")], lambda s: lag, end=end)

        def reference(d):
            usable = []
            for od, v, rel in rows:
                avail = od + timedelta(days=lag)
                if rel is not None and rel > avail:
                    avail = rel
                if avail <= d:
                    usable.append((od, v))
            return max(usable)[1] if usable else None            # latest OBSERVATION wins

        checked = 0
        for r in panel.iter_rows(named=True):
            assert r["S"] == reference(r["as_of_date"]), r["as_of_date"]
            checked += 1
        assert checked > 200


# ── forward fill, ages, calendar ─────────────────────────────────────────────

class TestForwardFillAndCalendar:
    def test_forward_fill_and_obs_date_over_gap(self):
        obs = _obs([(date(2026, 9, 14), 1.0, date(2026, 9, 15))])   # Monday obs
        panel = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 18))
        r = _row(panel, date(2026, 9, 18))                           # Friday
        assert r["S"] == 1.0 and (r["as_of_date"] - r["obs_date__S"]).days == 4

    def test_weekend_end_date_stops_at_last_weekday(self):
        obs = _obs([(date(2026, 9, 14), 1.0, date(2026, 9, 15))])
        panel = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 20))   # Sunday
        assert panel["as_of_date"].max() == date(2026, 9, 18)

    def test_panel_has_no_weekend_rows(self):
        obs = _obs([(date(2026, 9, 1), 1.0, None)])
        panel = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 30))
        assert all(d.weekday() < 5 for d in panel["as_of_date"].to_list())

    def test_panel_starts_at_first_availability(self):
        obs = _obs([(date(2026, 9, 1), 1.0, None)])
        panel = fp.build_panel(obs, [_f("S", "S")], lambda s: 3, end=date(2026, 9, 30))
        assert panel["as_of_date"].min() == date(2026, 9, 4)

    def test_weekday_calendar_helper(self):
        cal = fp.weekday_calendar(date(2026, 9, 18), date(2026, 9, 22))       # Fri..Tue
        assert cal["as_of_date"].to_list() == [date(2026, 9, 18), date(2026, 9, 21), date(2026, 9, 22)]
        assert fp.weekday_calendar(date(2026, 9, 22), date(2026, 9, 18)).height == 0

    def test_column_order_and_dtypes(self):
        obs = pl.concat([_obs([(date(2026, 9, 1), 1.0, None)], "A"), _obs([(date(2026, 9, 1), 2.0, None)], "B")])
        panel = fp.build_panel(obs, [_f("B", "B"), _f("A", "A")], lambda s: 1, end=date(2026, 9, 10))
        assert panel.columns == ["as_of_date", "B", "A", "obs_date__B", "obs_date__A",
                                 "available_on__B", "available_on__A", "avail_basis__B", "avail_basis__A"]
        assert panel.schema["B"] == pl.Float64 and panel.schema["as_of_date"] == pl.Date
        assert panel.schema["obs_date__B"] == pl.Date and panel.schema["available_on__B"] == pl.Date
        assert panel.schema["avail_basis__B"] == pl.String

    def test_absent_series_is_all_null_column_not_error(self):
        obs = _obs([(date(2026, 9, 1), 1.0, None)], "A")
        panel = fp.build_panel(obs, [_f("A", "A"), _f("GONE", "GONE")], lambda s: 1, end=date(2026, 9, 10))
        assert panel["GONE"].null_count() == panel.height and panel.height > 0

    def test_no_observations_at_all_gives_empty_panel(self):
        empty = _obs([])
        assert fp.build_panel(empty, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 10)).height == 0

    def test_end_before_first_availability_gives_empty_panel(self):
        obs = _obs([(date(2026, 9, 1), 1.0, None)])
        assert fp.build_panel(obs, [_f("S", "S")], lambda s: 30, end=date(2026, 9, 10)).height == 0


# ── transforms ───────────────────────────────────────────────────────────────

class TestPctChange12m:
    def _monthly(self, months):
        return _obs([(date(y, m, 1), v, None) for (y, m), v in months.items()], "CPI")

    def test_value_is_percent_and_available_with_its_own_observation(self):
        """YoY is computed on the native monthly series and becomes usable exactly
        when ITS latest observation does (Aug-2026 obs + 45 d = 2026-09-15)."""
        months = {(2025, 8): 100.0, (2026, 8): 103.5}
        panel = fp.build_panel(self._monthly(months), [_f("YOY", "CPI", "pct_change_12m")],
                               lambda s: 45, end=date(2026, 10, 30))
        assert panel["as_of_date"].min() == date(2026, 9, 15)
        assert _row(panel, date(2026, 9, 15))["YOY"] == pytest.approx(3.5)
        r = _row(panel, date(2026, 9, 15))
        assert r["obs_date__YOY"] == date(2026, 8, 1) and (r["as_of_date"] - r["obs_date__YOY"]).days == 45

    def test_missing_month_gives_no_value_instead_of_misaligned_pair(self):
        """A shift(12) implementation would pair Aug-2026 with Sep-2025 here."""
        months = {(2025, 8): 100.0, (2025, 10): 101.0, (2026, 8): 110.0}   # Sep-2025 missing
        out = fp.TRANSFORMS["pct_change_12m"](self._monthly(months))
        assert out["observation_date"].to_list() == [date(2026, 8, 1)]
        assert out["value"].to_list() == [pytest.approx(10.0)]

    def test_zero_base_is_dropped_not_inf(self):
        months = {(2025, 8): 0.0, (2026, 8): 5.0}
        assert fp.TRANSFORMS["pct_change_12m"](self._monthly(months)).height == 0


class TestDerived:
    def _two(self, a_rows, b_rows):
        return pl.concat([_obs(a_rows, "A"), _obs(b_rows, "B")])

    def test_subtract_value_and_obs_date_is_older_leg(self):
        obs = self._two([(date(2026, 9, 14), 5.0, None)], [(date(2026, 9, 11), 2.0, None)])
        feat = _f("RY", derived=DerivedSpec("subtract", "A", "B"))
        panel = fp.build_panel(obs, [feat], lambda s: 1, end=date(2026, 9, 16))
        r = _row(panel, date(2026, 9, 16))
        assert r["RY"] == pytest.approx(3.0)
        assert r["obs_date__RY"] == date(2026, 9, 11)            # the OLDER leg (B)
        assert (r["as_of_date"] - r["obs_date__RY"]).days == 5   # leg B is 5 days old, leg A is 2
        assert r["available_on__RY"] == date(2026, 9, 15)        # usable once BOTH legs are (A: 09-14 + 1)

    def test_null_until_both_legs_available(self):
        obs = self._two([(date(2026, 9, 1), 5.0, None)], [(date(2026, 9, 8), 2.0, None)])
        feat = _f("RY", derived=DerivedSpec("subtract", "A", "B"))
        panel = fp.build_panel(obs, [_f("A", "A"), feat], lambda s: 1, end=date(2026, 9, 15))
        early = _row(panel, date(2026, 9, 4))
        assert early["RY"] is None and early["obs_date__RY"] is None and early["available_on__RY"] is None
        assert _row(panel, date(2026, 9, 10))["RY"] == pytest.approx(3.0)

    def test_derived_legs_are_pit_gated_separately(self):
        obs = self._two([(date(2026, 9, 1), 5.0, None)], [(date(2026, 9, 1), 2.0, None)])
        feat = _f("RY", derived=DerivedSpec("subtract", "A", "B"))
        lags = {"A": 1, "B": 10}
        panel = fp.build_panel(obs, [_f("A", "A"), feat], lambda s: lags[s], end=date(2026, 9, 20))
        assert _row(panel, date(2026, 9, 7))["RY"] is None          # A usable, B not yet
        assert _row(panel, date(2026, 9, 11))["RY"] == pytest.approx(3.0)

    def test_unsupported_op_raises(self):
        obs = self._two([(date(2026, 9, 1), 5.0, None)], [(date(2026, 9, 1), 2.0, None)])
        with pytest.raises(ValueError, match="unsupported derived op"):
            fp.build_panel(obs, [_f("X", derived=DerivedSpec("divide", "A", "B"))], lambda s: 1,
                           end=date(2026, 9, 10))


# ═══ Phase B additions: market-branch transforms + caller-supplied calendar ═══

class TestLogChange20:
    def _daily(self, values, start=date(2026, 1, 5)):
        """Consecutive WEEKDAY observations carrying `values`."""
        rows, d = [], start
        for v in values:
            while d.weekday() >= 5:
                d += timedelta(days=1)
            rows.append((d, v, None))
            d += timedelta(days=1)
        return _obs(rows, "FX")

    def test_value_is_log_ratio_to_the_20th_previous_observation(self):
        import math
        vals = [100.0 * math.exp(0.01 * k) for k in range(25)]
        out = fp.TRANSFORMS["log_change_20"](self._daily(vals))
        assert out.height == 5                                   # rows 20..24 only
        assert out["value"].to_list() == [pytest.approx(0.20)] * 5

    def test_first_twenty_observations_are_dropped_not_zero_filled(self):
        out = fp.TRANSFORMS["log_change_20"](self._daily([1.0 + 0.01 * k for k in range(20)]))
        assert out.height == 0

    def test_is_observation_indexed_not_calendar_indexed(self):
        """A 10-day hole in the data does not change which observation is 'the 20th
        previous' — ADR-051 specifies 20 OBSERVATIONS."""
        rows = [(date(2026, 1, 1) + timedelta(days=k), 1.0 + 0.01 * k, None) for k in range(15)]
        rows += [(date(2026, 3, 1) + timedelta(days=k), 2.0 + 0.01 * k, None) for k in range(10)]   # after a hole
        out = fp.TRANSFORMS["log_change_20"](_obs(rows, "FX"))
        import math
        # observation #20 (0-indexed) is the 6th of the second block; its base is observation #0
        first = out.sort("observation_date").row(0, named=True)
        assert first["observation_date"] == date(2026, 3, 6)
        assert first["value"] == pytest.approx(math.log((2.0 + 0.05) / 1.0))

    def test_non_positive_values_are_dropped_not_clipped(self):
        vals = [1.0] * 25
        vals[0] = 0.0                                            # base of row 20 is 0 -> undefined
        vals[24] = -1.0                                          # value of row 24 is negative -> undefined
        obs = self._daily(vals)
        out = fp.TRANSFORMS["log_change_20"](obs)
        dates = obs.sort("observation_date")["observation_date"].to_list()
        assert out.sort("observation_date")["observation_date"].to_list() == dates[21:24]   # rows 21, 22, 23 survive

    def test_available_exactly_when_its_own_observation_is(self):
        vals = [100.0 + k for k in range(25)]
        obs = self._daily(vals)
        panel = fp.build_panel(obs, [_f("CH", "FX", "log_change_20")], lambda s: 10, end=date(2026, 3, 31))
        # first usable value is observation #20, usable 10 days after that observation
        o20 = obs.sort("observation_date")["observation_date"][20]
        assert panel.row(0, named=True)["as_of_date"] == o20 + timedelta(days=10)
        assert panel["as_of_date"].max() == date(2026, 3, 31)          # Tuesday: last weekday <= end


class TestDiff1:
    def test_value_is_change_vs_previous_observation_first_row_dropped(self):
        obs = _obs([(date(2026, 9, 14), 4.0, None), (date(2026, 9, 15), 4.2, None), (date(2026, 9, 16), 4.1, None)], "R")
        out = fp.TRANSFORMS["diff_1"](obs).sort("observation_date")
        assert out["observation_date"].to_list() == [date(2026, 9, 15), date(2026, 9, 16)]
        assert out["value"].to_list() == [pytest.approx(0.2), pytest.approx(-0.1)]

    def test_change_across_a_holiday_is_vs_the_previous_observation(self):
        obs = _obs([(date(2026, 9, 4), 4.0, None), (date(2026, 9, 8), 4.5, None)], "R")   # Mon 9/7 holiday
        out = fp.TRANSFORMS["diff_1"](obs)
        assert out["value"].to_list() == [pytest.approx(0.5)]

    def test_input_order_does_not_matter(self):
        a = _obs([(date(2026, 9, 14), 4.0, None), (date(2026, 9, 15), 4.2, None)], "R")
        b = _obs([(date(2026, 9, 15), 4.2, None), (date(2026, 9, 14), 4.0, None)], "R")
        assert fp.TRANSFORMS["diff_1"](a)["value"].to_list() == fp.TRANSFORMS["diff_1"](b)["value"].to_list()


class TestCallerSuppliedCalendar:
    def _cal(self, *ds):
        return pl.DataFrame({"as_of_date": list(ds)}, schema={"as_of_date": pl.Date})

    def test_rows_are_exactly_the_supplied_dates_holiday_excluded(self):
        obs = _obs([(date(2026, 9, 1), 1.0, None)])
        cal = self._cal(date(2026, 9, 3), date(2026, 9, 4), date(2026, 9, 8))     # Mon 9/7 (holiday) absent
        panel = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 30), calendar=cal)
        assert panel["as_of_date"].to_list() == [date(2026, 9, 3), date(2026, 9, 4), date(2026, 9, 8)]
        assert panel["S"].to_list() == [1.0, 1.0, 1.0]

    def test_early_rows_are_kept_all_null_not_trimmed(self):
        """The caller owns the row index: a trading day with no value yet stays."""
        obs = _obs([(date(2026, 9, 10), 1.0, None)])
        cal = self._cal(date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 14))
        panel = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 30), calendar=cal)
        assert panel["as_of_date"].to_list() == [date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 14)]
        assert panel["S"].to_list() == [None, None, 1.0]
        assert panel["obs_date__S"].to_list() == [None, None, date(2026, 9, 10)]

    def test_dates_after_end_are_dropped(self):
        obs = _obs([(date(2026, 9, 1), 1.0, None)])
        cal = self._cal(date(2026, 9, 3), date(2026, 9, 20))
        panel = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 10), calendar=cal)
        assert panel["as_of_date"].to_list() == [date(2026, 9, 3)]

    def test_duplicate_and_unsorted_dates_are_normalised(self):
        obs = _obs([(date(2026, 9, 1), 1.0, None)])
        cal = self._cal(date(2026, 9, 4), date(2026, 9, 3), date(2026, 9, 4))
        panel = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 30), calendar=cal)
        assert panel["as_of_date"].to_list() == [date(2026, 9, 3), date(2026, 9, 4)]

    def test_empty_calendar_gives_empty_panel(self):
        obs = _obs([(date(2026, 9, 1), 1.0, None)])
        assert fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 30),
                              calendar=self._cal()).height == 0

    def test_no_observations_with_calendar_gives_all_null_rows(self):
        cal = self._cal(date(2026, 9, 3), date(2026, 9, 4))
        panel = fp.build_panel(_obs([]), [_f("S", "S")], lambda s: 1, end=date(2026, 9, 30), calendar=cal)
        assert panel.height == 2 and panel["S"].null_count() == 2

    def test_pit_gating_still_applies_on_a_supplied_calendar(self):
        obs = _obs([(date(2026, 9, 14), 1.0, date(2026, 9, 15))], "WTI")
        cal = self._cal(date(2026, 9, 21), date(2026, 9, 22))          # obs 09-14 + 8 d = 09-22
        panel = fp.build_panel(obs, [_f("WTI", "WTI")], lambda s: 8, end=date(2026, 9, 30), calendar=cal)
        assert panel["WTI"].to_list() == [None, 1.0]


# ═══ Recorded availability (hmm.pit.basis) ══════════════════════════════════════════════════════════════════════════════

def _orec(rows, sid="S"):
    """rows: (observation_date, value, release_date|None, ingested_on|None, backfilled)"""
    return pl.DataFrame(
        {
            "series_id": [sid] * len(rows),
            "observation_date": [r[0] for r in rows],
            "release_date": [r[2] for r in rows],
            "value": [r[1] for r in rows],
            "ingested_on": [r[3] for r in rows],
            "backfilled": [r[4] for r in rows],
        },
        schema={"series_id": pl.String, "observation_date": pl.Date, "release_date": pl.Date,
                "value": pl.Float64, "ingested_on": pl.Date, "backfilled": pl.Boolean},
    )


class TestLoadObservationsLineage:
    def _snap(self, tmp_path, rows, ing_dtype=pl.String, with_ing=True):
        schema = {"series_id": pl.String, "observation_date": pl.Date, "release_date": pl.String, "value": pl.Float64}
        if with_ing:
            schema["_ingested_at"] = ing_dtype
        df = pl.DataFrame(rows, orient="row", schema=schema)
        p = tmp_path / "fred_2026-09-30_silver.parquet"
        df.write_parquet(p)
        return p

    def test_first_ingest_batch_per_series_is_backfilled_later_rows_are_not(self, tmp_path):
        p = self._snap(tmp_path, [
            ("A", date(2026, 1, 1), "2026-02-05", 1.0, "2026-08-20T09:00:00"),     # A's first batch
            ("A", date(2026, 2, 1), "2026-03-05", 2.0, "2026-08-20T09:00:00"),     # same batch
            ("A", date(2026, 3, 1), "2026-04-05", 3.0, "2026-09-25T06:00:00"),     # recorded later
            ("B", date(2026, 3, 1), "2026-04-05", 9.0, "2026-09-25T06:00:00"),     # B's only batch = its first
        ])
        out = fp.load_observations(p, ["A", "B"]).sort("series_id", "observation_date")
        assert out["ingested_on"].to_list() == [date(2026, 8, 20), date(2026, 8, 20), date(2026, 9, 25), date(2026, 9, 25)]
        assert out["backfilled"].to_list() == [True, True, False, True]            # per SERIES, not global

    def test_unparseable_or_null_stamp_is_treated_as_backfilled(self, tmp_path):
        p = self._snap(tmp_path, [
            ("A", date(2026, 1, 1), "2026-02-05", 1.0, "2026-08-20T09:00:00"),
            ("A", date(2026, 2, 1), "2026-03-05", 2.0, "garbage"),
            ("A", date(2026, 3, 1), "2026-04-05", 3.0, None),
            ("A", date(2026, 4, 1), "2026-05-05", 4.0, "2026-09-25T06:00:00"),
        ])
        out = fp.load_observations(p, ["A"]).sort("observation_date")
        assert out["ingested_on"].to_list() == [date(2026, 8, 20), None, None, date(2026, 9, 25)]
        assert out["backfilled"].to_list() == [True, True, True, False]

    def test_snapshot_without_ingested_at_has_no_recorded_information(self, tmp_path):
        p = self._snap(tmp_path, [("A", date(2026, 1, 1), "2026-02-05", 1.0)], with_ing=False)
        out = fp.load_observations(p, ["A"])
        assert out["ingested_on"].to_list() == [None] and out["backfilled"].to_list() == [True]

    def test_timezone_aware_stamp_is_converted_to_the_utc_date(self, tmp_path):
        from datetime import datetime
        # 2026-09-26 02:00 in Jakarta (UTC+7) is 2026-09-25 19:00 UTC -> the UTC date is 09-25
        ts = pl.Series([datetime(2026, 8, 20, 9), datetime(2026, 9, 26, 2)]).dt.replace_time_zone("Asia/Jakarta")
        df = pl.DataFrame({"series_id": ["A", "A"], "observation_date": [date(2026, 1, 1), date(2026, 2, 1)],
                           "release_date": ["2026-02-05", "2026-03-05"], "value": [1.0, 2.0], "_ingested_at": ts})
        p = tmp_path / "fred_2026-09-30_silver.parquet"
        df.write_parquet(p)
        out = fp.load_observations(p, ["A"]).sort("observation_date")
        assert out["ingested_on"].to_list() == [date(2026, 8, 20), date(2026, 9, 25)]

    def test_backfilled_is_decided_before_a_transform_drops_rows(self, tmp_path):
        """A transform may drop a series' first rows (diff_1 drops row 0); the flag was computed on the
        raw series, so the surviving rows are still correctly backfilled / recorded."""
        p = self._snap(tmp_path, [
            ("R", date(2026, 9, 1), "2026-09-02", 4.0, "2026-08-20T09:00:00"),
            ("R", date(2026, 9, 2), "2026-09-03", 4.5, "2026-08-20T09:00:00"),
            ("R", date(2026, 9, 3), "2026-09-04", 4.2, "2026-09-25T06:00:00"),
        ])
        out = fp.TRANSFORMS["diff_1"](fp.load_observations(p, ["R"])).sort("observation_date")
        assert out["backfilled"].to_list() == [True, False]


class TestAvailabilityBases:
    def test_recorded_uses_ingested_on_for_a_non_backfilled_row(self):
        o = _orec([(date(2026, 9, 1), 1.0, date(2026, 9, 2), date(2026, 9, 20), False)])
        r = fp.with_availability(o, 3, "recorded").row(0, named=True)
        assert r["available_on"] == date(2026, 9, 20) and r["avail_basis"] == "recorded"

    def test_recorded_basis_leaves_backfilled_history_estimated(self):
        o = _orec([(date(2026, 9, 1), 1.0, date(2026, 9, 2), date(2026, 8, 20), True)])
        r = fp.with_availability(o, 3, "recorded").row(0, named=True)
        assert r["available_on"] == date(2026, 9, 4) and r["avail_basis"] == "estimated"   # obs + 3 beats release

    def test_recorded_is_clamped_to_the_observation_date(self):
        """A forward-dated value (e.g. IORB) is never available before the day it describes."""
        o = _orec([(date(2026, 10, 1), 1.0, None, date(2026, 9, 15), False)])
        r = fp.with_availability(o, 1, "recorded").row(0, named=True)
        assert r["available_on"] == date(2026, 10, 1) and r["avail_basis"] == "recorded"

    def test_estimated_basis_ignores_recorded_information_entirely(self):
        o = _orec([(date(2026, 9, 1), 1.0, date(2026, 9, 2), date(2026, 9, 20), False)])
        r = fp.with_availability(o, 3, "estimated").row(0, named=True)
        assert r["available_on"] == date(2026, 9, 4) and r["avail_basis"] == "estimated"

    def test_non_backfilled_row_without_a_timestamp_falls_back_to_estimated(self):
        o = _orec([(date(2026, 9, 1), 1.0, None, None, False)])
        r = fp.with_availability(o, 3, "recorded").row(0, named=True)
        assert r["available_on"] == date(2026, 9, 4) and r["avail_basis"] == "estimated"

    def test_invalid_basis_raises(self):
        with pytest.raises(ValueError, match="pit basis"):
            fp.with_availability(_orec([(date(2026, 9, 1), 1.0, None, None, True)]), 1, "whenever")

    def test_pit_bases_constant_is_exactly_the_two_config_values(self):
        assert fp.PIT_BASES == frozenset({"recorded", "estimated"})      # "mixed" is output-only (derived)


class TestPanelBases:
    def test_recorded_panel_starts_when_the_system_actually_held_the_value(self):
        obs = _orec([(date(2026, 9, 1), 1.0, None, date(2026, 9, 10), False)])
        rec = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 18), basis="recorded")
        est = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 18), basis="estimated")
        assert rec["as_of_date"].min() == date(2026, 9, 10) and est["as_of_date"].min() == date(2026, 9, 2)
        r = _row(rec, date(2026, 9, 10))
        assert r["available_on__S"] == date(2026, 9, 10) and r["avail_basis__S"] == "recorded"
        assert r["obs_date__S"] == date(2026, 9, 1)

    def test_default_basis_is_recorded(self):
        obs = _orec([(date(2026, 9, 1), 1.0, None, date(2026, 9, 10), False)])
        panel = fp.build_panel(obs, [_f("S", "S")], lambda s: 1, end=date(2026, 9, 18))
        assert panel["as_of_date"].min() == date(2026, 9, 10)

    def test_derived_basis_is_mixed_when_legs_differ_and_availability_is_the_later_leg(self):
        a = _orec([(date(2026, 9, 1), 5.0, None, date(2026, 9, 10), False)], "A")     # recorded -> 09-10
        b = _orec([(date(2026, 9, 1), 2.0, None, date(2026, 8, 20), True)], "B")      # backfilled -> est 09-02
        feat = _f("RY", derived=DerivedSpec("subtract", "A", "B"))
        panel = fp.build_panel(pl.concat([a, b]), [feat], lambda s: 1, end=date(2026, 9, 18), basis="recorded")
        r = _row(panel, date(2026, 9, 11))
        assert r["RY"] == pytest.approx(3.0)
        assert r["avail_basis__RY"] == "mixed" and r["available_on__RY"] == date(2026, 9, 10)

    def test_derived_basis_is_kept_when_both_legs_agree(self):
        a = _orec([(date(2026, 9, 1), 5.0, None, date(2026, 9, 10), False)], "A")
        b = _orec([(date(2026, 9, 2), 2.0, None, date(2026, 9, 11), False)], "B")
        feat = _f("RY", derived=DerivedSpec("subtract", "A", "B"))
        panel = fp.build_panel(pl.concat([a, b]), [feat], lambda s: 1, end=date(2026, 9, 18), basis="recorded")
        r = _row(panel, date(2026, 9, 14))
        assert r["avail_basis__RY"] == "recorded" and r["available_on__RY"] == date(2026, 9, 11)
        assert r["obs_date__RY"] == date(2026, 9, 1)

    def test_recorded_basis_never_serves_a_value_before_its_recorded_date(self):
        """No-lookahead on the recorded rule, brute-forced: for every row, the carried observation
        must have available_on <= as_of_date, whatever mix of backfilled / recorded rows exist."""
        rnd = random.Random(7)
        rows = []
        for i in range(80):
            od = date(2026, 1, 1) + timedelta(days=i * 2)
            ing = od + timedelta(days=rnd.randint(0, 20))
            rows.append((od, float(i), None, ing, i < 30))                         # first 30 are the backfill
        panel = fp.build_panel(_orec(rows), [_f("S", "S")], lambda s: 5, end=date(2026, 8, 1), basis="recorded")
        assert panel.height > 50
        for r in panel.iter_rows(named=True):
            if r["S"] is not None:
                assert r["available_on__S"] <= r["as_of_date"]
                assert r["obs_date__S"] <= r["available_on__S"]
