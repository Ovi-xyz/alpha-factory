"""tests/unit/test_calendar_oracle.py — XAE-R05 (cross_asset_recalibration_guide Q3/Q4, decision D2).

Three layers: (1) CalendarOracle with a FAKE calendar factory — no library needed; (2) the real
`exchange_calendars` package against known historical closures; (3) the config file's parity with the
instrument taxonomy and with the library's calendar names."""

from datetime import date, timedelta
from pathlib import Path

import pytest
import yaml

from src.utils.calendar_oracle import CalendarOracle, DEFAULT_CONFIG_PATH
from src.utils.gap_analysis import GapEvent, classify_gaps, classify_gaps_by_peer_agreement


class _FakeCal:
    """Sessions = every weekday except the listed holidays; range configurable."""
    class _D:
        def __init__(self, d): self._d = d
        def date(self): return self._d

    def __init__(self, holidays=(), first=date(2000, 1, 1), last=date(2100, 1, 1)):
        self._h = {d.isoformat() for d in holidays}
        self.first_session = self._D(first)
        self.last_session = self._D(last)

    def is_session(self, iso):
        d = date.fromisoformat(iso)
        return d.weekday() < 5 and iso not in self._h


def _oracle(cal, mapping=None):
    return CalendarOracle(mapping or {"JKSE": "XFAKE"}, calendar_factory=lambda code: cal)


class TestWithFakeCalendar:

    def test_every_missing_weekday_a_holiday_is_a_closure(self):
        # Thu 2026-03-19 .. Tue 2026-03-24: Fri 20 is a holiday, 21/22 weekend
        o = _oracle(_FakeCal(holidays=[date(2026, 3, 20)]))
        assert o.is_closure("JKSE", date(2026, 3, 19), date(2026, 3, 24) - timedelta(days=1)) is True

    def test_a_session_day_inside_the_gap_means_a_bar_is_genuinely_missing(self):
        o = _oracle(_FakeCal(holidays=[date(2026, 3, 20)]))
        # Mon 23 and Tue 24 are sessions with no bar
        assert o.is_closure("JKSE", date(2026, 3, 19), date(2026, 3, 26)) is False

    def test_weekend_only_gap_is_a_closure(self):
        o = _oracle(_FakeCal())
        assert o.is_closure("JKSE", date(2026, 3, 20), date(2026, 3, 23)) is True

    def test_endpoints_are_not_part_of_the_gap(self):
        """prev_ts and ts are the last/first bars — they are sessions by construction."""
        o = _oracle(_FakeCal())
        assert o.is_closure("JKSE", date(2026, 3, 19), date(2026, 3, 20)) is True   # nothing between

    def test_unmapped_symbol_is_unknown_not_closure(self):
        o = _oracle(_FakeCal())
        assert o.is_closure("BRENT", date(2026, 3, 19), date(2026, 3, 26)) is None
        assert o.status()["unknown_symbols"]["BRENT"] == "no calendar mapped"

    def test_gap_outside_calendar_range_is_unknown(self):
        o = _oracle(_FakeCal(first=date(2010, 1, 1)))
        assert o.is_closure("JKSE", date(2009, 12, 1), date(2010, 1, 8)) is None
        o2 = _oracle(_FakeCal(last=date(2026, 3, 1)))
        assert o2.is_closure("JKSE", date(2026, 2, 20), date(2026, 3, 10)) is None

    def test_factory_failure_is_unknown_loud_and_cached(self):
        calls = []
        def boom(code):
            calls.append(code)
            raise ImportError("No module named 'exchange_calendars'")
        o = CalendarOracle({"JKSE": "XIDX", "N225": "XIDX"}, calendar_factory=boom)
        assert o.is_closure("JKSE", date(2026, 3, 19), date(2026, 3, 26)) is None
        assert o.is_closure("N225", date(2026, 3, 19), date(2026, 3, 26)) is None
        assert calls == ["XIDX"], "a failed calendar must not be retried per event"
        assert "XIDX" in o.status()["failed_calendars"]
        assert "ImportError" in o.status()["failed_calendars"]["XIDX"]

    def test_calendar_error_during_query_is_unknown_not_a_crash(self):
        class Bad(_FakeCal):
            def is_session(self, iso): raise RuntimeError("calendar bug")
        o = _oracle(Bad())
        assert o.is_closure("JKSE", date(2026, 3, 19), date(2026, 3, 26)) is None

    def test_missing_config_degrades_to_empty_oracle(self, tmp_path):
        o = CalendarOracle.from_config(tmp_path / "absent.yaml")
        assert o.is_closure("N225", date(2026, 1, 1), date(2026, 1, 9)) is None


class TestWithRealCalendars:
    """Known historical facts, against the real library."""

    @pytest.fixture(autouse=True)
    def _need_lib(self):
        pytest.importorskip("exchange_calendars")

    @pytest.fixture
    def oracle(self):
        return CalendarOracle.from_config(DEFAULT_CONFIG_PATH)

    def test_tokyo_new_year_closure(self, oracle):
        # Tokyo is closed 31 Dec - 3 Jan: last bar Fri 29 Dec 2023, first bar Thu 4 Jan 2024 (6-day gap)
        assert oracle.is_closure("N225", date(2023, 12, 29), date(2024, 1, 4)) is True

    def test_jakarta_idul_fitri_2024_closure(self, oracle):
        # IDX closed 8-15 Apr 2024 (Idul Fitri + cuti bersama): last bar Fri 5 Apr, first bar Tue 16 Apr
        assert oracle.is_closure("JKSE", date(2024, 4, 5), date(2024, 4, 16)) is True

    def test_missing_tokyo_session_days_are_not_a_closure(self, oracle):
        # 6-11 Mar 2024 were normal Tokyo sessions
        assert oracle.is_closure("N225", date(2024, 3, 5), date(2024, 3, 12)) is False

    def test_frankfurt_christmas_gap(self, oracle):
        # Xetra: closed 24-26 Dec 2023 (24 Dec Sunday; 25, 26 holidays). Last bar Fri 22, first Wed 27.
        assert oracle.is_closure("DAX", date(2023, 12, 22), date(2023, 12, 27)) is True

    def test_hong_kong_lunar_new_year_2024(self, oracle):
        # HKEX closed Mon 12 and Tue 13 Feb (LNY); last bar Fri 9 Feb, first Wed 14 Feb
        assert oracle.is_closure("HSI", date(2024, 2, 9), date(2024, 2, 14)) is True
        # ...and Wed 14 Feb was a session: a series that skips it is genuinely missing a bar
        assert oracle.is_closure("HSI", date(2024, 2, 9), date(2024, 2, 15)) is False

    def test_vix_follows_nyse(self, oracle):
        # Good Friday 2024 (29 Mar): last bar Thu 28 Mar, first bar Mon 1 Apr
        assert oracle.is_closure("VIX", date(2024, 3, 28), date(2024, 4, 1)) is True


class TestConfigParity:

    def test_every_mapped_code_is_a_real_calendar_name(self):
        xc = pytest.importorskip("exchange_calendars")
        names = set(xc.get_calendar_names(include_aliases=True))
        cfg = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text())
        bad = {s: c for s, c in cfg["calendars"].items() if c not in names}
        assert not bad, f"unknown exchange_calendars codes: {bad}"

    def test_every_mapped_symbol_is_a_live_context_instrument(self):
        from src.config.instrument_loader import get_loader
        live = {i.symbol for i in get_loader().all_context(include_deferred=True)}
        cfg = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text())
        assert set(cfg["calendars"]) <= live, set(cfg["calendars"]) - live

    def test_the_indices_peer_agreement_could_not_classify_are_mapped(self):
        """guide Q3: N225 and DAX (isolated by construction in a nine-member DM group) and JKSE."""
        cfg = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text())["calendars"]
        for sym in ("N225", "DAX", "JKSE"):
            assert sym in cfg

    def test_pyproject_declares_the_dependency(self):
        text = Path("pyproject.toml").read_text()
        assert "exchange-calendars" in text


# ── classify_gaps (pure) ─────────────────────────────────────────────────────

def _ev(symbol, prev_ts, ts, peer_group="grp"):
    return GapEvent(symbol=symbol, prev_ts=prev_ts, ts=ts, day_gap=(ts - prev_ts).days, peer_group=peer_group)


class TestClassifyGapsOracle:
    SIZES = {"grp": 9}

    def test_oracle_true_makes_a_lone_member_a_closure(self):
        """The N225 case: no peer shares the gap, peer agreement says isolated, the calendar says closed."""
        e = _ev("N225", date(2023, 12, 29), date(2024, 1, 4))
        assert classify_gaps_by_peer_agreement([e], self.SIZES) == ([e], [])
        c = classify_gaps([e], self.SIZES, oracle=lambda ev: True)
        assert c.closures == [e] and c.isolated == [] and c.closure_by_oracle == 1

    def test_oracle_false_overrides_peer_agreement(self):
        """Every peer shares the gap, but the calendar says session days have no bar: isolated."""
        events = [_ev(s, date(2026, 3, 1), date(2026, 3, 9)) for s in "ABCDEFGHI"]
        peers_only = classify_gaps(events, self.SIZES)
        assert len(peers_only.closures) == 9
        with_oracle = classify_gaps(events, self.SIZES, oracle=lambda ev: False)
        assert len(with_oracle.isolated) == 9 and with_oracle.isolated_by_oracle == 9

    def test_oracle_none_falls_back_to_the_peer_rule(self):
        events = [_ev(s, date(2026, 3, 1), date(2026, 3, 9)) for s in "ABCDEFGHI"]
        c = classify_gaps(events, self.SIZES, oracle=lambda ev: None)
        assert len(c.closures) == 9 and c.decided_by_peers == 9 and c.closure_by_oracle == 0

    def test_mixed_verdicts_are_counted_by_how_they_were_decided(self):
        a = _ev("A", date(2026, 3, 1), date(2026, 3, 9))
        b = _ev("B", date(2026, 4, 1), date(2026, 4, 9))
        c_ = _ev("C", date(2026, 5, 1), date(2026, 5, 9))
        verdicts = {"A": True, "B": False, "C": None}
        out = classify_gaps([a, b, c_], self.SIZES, oracle=lambda ev: verdicts[ev.symbol])
        assert (out.closure_by_oracle, out.isolated_by_oracle, out.decided_by_peers) == (1, 1, 1)
        assert out.isolated == [b, c_] and out.closures == [a]

    def test_no_oracle_is_identical_to_the_legacy_function(self):
        events = [_ev(s, date(2026, 3, 1), date(2026, 3, 9)) for s in "ABCDE"]
        legacy = classify_gaps_by_peer_agreement(events, self.SIZES)
        new = classify_gaps(events, self.SIZES)
        assert (new.isolated, new.closures) == legacy


# ── wiring into the validator ────────────────────────────────────────────────

class TestValidatorUsesTheOracle:

    @staticmethod
    def _write(tmp_path, symbol, days):
        import datetime
        import polars as pl
        d = tmp_path / "context" / f"symbol={symbol}"
        d.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": [symbol] * len(days),
            "timestamp": [datetime.datetime(x.year, x.month, x.day, tzinfo=datetime.timezone.utc) for x in days],
            "is_clean": [True] * len(days),
        }).write_parquet(d / f"{symbol}_1D_silver.parquet")

    @staticmethod
    def _weekdays(start, end, skip=()):
        out, d = [], start
        while d <= end:
            if d.weekday() < 5 and d not in skip:
                out.append(d)
            d += timedelta(days=1)
        return out

    def _counts(self, tmp_path, monkeypatch, oracle):
        import duckdb
        import src.silver.quality_validator as qv_mod
        from src.utils.silver_scope import context_peer_groups
        peer_map, peer_sizes = context_peer_groups()
        qv = qv_mod.QualityValidator()
        glob = str(tmp_path / "context" / "symbol=*" / "*_1D_silver.parquet")
        return qv, qv._classified_gap_counts(duckdb.connect(), glob, peer_map, peer_sizes, oracle=oracle)

    def test_lone_calendar_closures_stop_counting_as_isolated(self, tmp_path, monkeypatch):
        pytest.importorskip("exchange_calendars")
        # N225 across the 2023/24 Tokyo New Year closure; JKSE across Idul Fitri 2024
        n225 = self._weekdays(date(2023, 12, 18), date(2024, 1, 31),
                              skip={date(2023, 12, 31), date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)})
        jkse = self._weekdays(date(2024, 3, 25), date(2024, 4, 30),
                              skip={date(2024, 4, d) for d in range(8, 16)})
        self._write(tmp_path, "N225", n225)
        self._write(tmp_path, "JKSE", jkse)

        _, (iso_peer, clo_peer, _) = self._counts(tmp_path, monkeypatch, oracle=None)
        assert iso_peer == 2 and clo_peer == 0            # the defect: both lone closures read as isolated

        qv, (iso, clo, _) = self._counts(tmp_path, monkeypatch, CalendarOracle.from_config())
        assert (iso, clo) == (0, 2)
        assert qv._last_gap_classification.closure_by_oracle == 2

    def test_a_genuinely_missing_session_run_stays_isolated_with_the_oracle(self, tmp_path, monkeypatch):
        pytest.importorskip("exchange_calendars")
        n225 = self._weekdays(date(2024, 2, 19), date(2024, 4, 5), skip=set(self._weekdays(date(2024, 3, 6), date(2024, 3, 11))))
        self._write(tmp_path, "N225", n225)
        qv, (iso, clo, _) = self._counts(tmp_path, monkeypatch, CalendarOracle.from_config())
        assert (iso, clo) == (1, 0)
        assert qv._last_gap_classification.isolated_by_oracle == 1

    def test_unavailable_library_degrades_to_peer_only_and_reports_it(self, tmp_path, monkeypatch):
        n225 = self._weekdays(date(2023, 12, 18), date(2024, 1, 31),
                              skip={date(2023, 12, 31), date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)})
        self._write(tmp_path, "N225", n225)
        def no_lib(code): raise ImportError("exchange_calendars not installed")
        oracle = CalendarOracle.from_config(calendar_factory=no_lib)
        _, (iso, clo, _) = self._counts(tmp_path, monkeypatch, oracle)
        assert (iso, clo) == (1, 0)                       # pre-R05 behaviour, not a crash
        assert oracle.status()["failed_calendars"]

    def test_context_check_logs_a_warning_when_the_oracle_degrades(self, tmp_path, monkeypatch):
        from loguru import logger
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        # a 6-day Tokyo New Year gap, so the gap check actually consults the oracle
        self._write(tmp_path, "N225", self._weekdays(
            date(2023, 12, 18), date(2024, 1, 31),
            skip={date(2023, 12, 31), date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)}))
        def no_lib(code): raise ImportError("exchange_calendars not installed")
        monkeypatch.setattr(qv_mod.CalendarOracle, "from_config",
                            classmethod(lambda cls, *a, **k: CalendarOracle({"N225": "XTKS"}, no_lib)))
        msgs = []
        sink = logger.add(lambda m: msgs.append((m.record["level"].name, m.record["message"])), level="DEBUG")
        try:
            assert qv_mod.QualityValidator()._check_context_gap_detection(date(2024, 3, 1)) is True
        finally:
            logger.remove(sink)
        assert any(l == "WARNING" and "calendar oracle degraded" in m for l, m in msgs)
