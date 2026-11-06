"""
tests/unit/test_membership_log_ghr14_adr059.py — GHR-14 (ADR-059): append-only active-set membership log.

GD3/GD4: the 16->17 Oct run swapped F, WMB, CVNA, CB out and PTC, OXY, MO, SHW in (all within ~5% of the
dollar-volume cutoff), and dated membership files existed only for 16-18 Oct. The log records the changes
so that membership on any logged date is reconstructible from the log alone.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.silver import membership_log as ml

D1, D2, D3 = date(2026, 10, 16), date(2026, 10, 17), date(2026, 10, 18)


def _write(d: Path, day: date, members: dict[tuple[str, str], float | None], *, fallback: bool = False) -> None:
    d.mkdir(parents=True, exist_ok=True)
    rows = [{"symbol": s, "market": m, "dollar_volume_20d": v, "clean_days": 100, "last_close": 10.0,
             "eligibility_reason": "ok", "resolved_date": day.isoformat(), "resolver_version": "1.1",
             "unknown_market_count": 0, "is_fallback": fallback} for (m, s), v in members.items()]
    pl.DataFrame(rows).write_parquet(d / f"active_ohlcv_{day.isoformat()}.parquet")


def _us(*syms, base=1e9):
    return {("us_stocks", s): base - i * 1e6 for i, s in enumerate(syms)}


def _ev(log: pl.DataFrame, day: date, event: str, market="us_stocks") -> set[str]:
    return set(log.filter((pl.col("date") == day.isoformat()) & (pl.col("event") == event)
                          & (pl.col("market") == market))["symbol"].to_list())


class TestSeedAndDiff:

    def test_seeds_from_existing_files_with_initial_entries(self, tmp_path):
        _write(tmp_path, D1, _us("A", "B", "C"))
        _write(tmp_path, D2, _us("A", "B", "D"))
        _write(tmp_path, D3, _us("A", "D", "E"))
        assert ml.record_membership(tmp_path, D3) == 3 + 2 + 2
        log = ml.read_log(tmp_path)
        assert _ev(log, D1, "enter") == {"A", "B", "C"} and _ev(log, D1, "leave") == set()
        assert log.filter(pl.col("date") == D1.isoformat())["compared_with"].null_count() == 3     # initial
        assert _ev(log, D2, "enter") == {"D"} and _ev(log, D2, "leave") == {"C"}
        assert _ev(log, D3, "enter") == {"E"} and _ev(log, D3, "leave") == {"B"}
        assert set(log.filter(pl.col("date") == D3.isoformat())["compared_with"].to_list()) == {D2.isoformat()}

    def test_the_16_to_17_oct_swap(self, tmp_path):
        stay = _us(*[f"S{i:02d}" for i in range(20)], base=2e9)
        left = {("us_stocks", s): v for s, v in zip(("F", "WMB", "CVNA", "CB"), (547e6, 548e6, 551e6, 554e6))}
        entered = {("us_stocks", s): v for s, v in zip(("PTC", "OXY", "MO", "SHW"), (527e6, 531e6, 540e6, 543e6))}
        _write(tmp_path, D1, {**stay, **left})
        _write(tmp_path, D2, {**stay, **entered})
        ml.record_membership(tmp_path, D2)
        log = ml.read_log(tmp_path)
        assert _ev(log, D2, "leave") == {"F", "WMB", "CVNA", "CB"}
        assert _ev(log, D2, "enter") == {"PTC", "OXY", "MO", "SHW"}
        lv = log.filter((pl.col("date") == D2.isoformat()) & (pl.col("event") == "leave"))
        assert sorted(lv["dollar_volume_20d"].to_list()) == [547e6, 548e6, 551e6, 554e6]   # from the PREVIOUS file

    def test_rank_is_within_the_market_and_leave_uses_the_previous_file(self, tmp_path):
        d1 = {**_us("A", "B", base=9e9), ("idx", "X"): 5e9, ("idx", "Y"): 4e9}
        d2 = {**_us("A", base=9e9), ("idx", "Y"): 4e9, ("idx", "Z"): 8e9}
        _write(tmp_path, D1, d1)
        _write(tmp_path, D2, d2)
        ml.record_membership(tmp_path, D2)
        log = ml.read_log(tmp_path)
        by = {(r["date"], r["market"], r["symbol"], r["event"]): r["rank"] for r in log.iter_rows(named=True)}
        assert by[(D1.isoformat(), "idx", "X", "enter")] == 1 and by[(D1.isoformat(), "us_stocks", "A", "enter")] == 1
        assert by[(D2.isoformat(), "idx", "Z", "enter")] == 1                  # top of IDX today
        assert by[(D2.isoformat(), "us_stocks", "B", "leave")] == 2            # B's rank in the file it left
        assert by[(D2.isoformat(), "idx", "X", "leave")] == 1

    def test_null_dollar_volume_does_not_break_ranking(self, tmp_path):
        _write(tmp_path, D1, {("forex", "EUR_USD"): None, ("forex", "GBP_USD"): None, **_us("A")})
        assert ml.record_membership(tmp_path, D1) == 3


class TestReplay:

    def test_log_alone_reproduces_every_dated_file(self, tmp_path):
        files = {D1: _us("A", "B", "C"), D2: _us("A", "B", "D"), D3: _us("A", "D", "E")}
        for day, m in files.items():
            _write(tmp_path, day, m)
        ml.record_membership(tmp_path, D3)
        log = ml.read_log(tmp_path)
        for day, m in files.items():
            assert ml.members_as_of(log, day) == set(m), day

    def test_before_the_first_logged_day_is_empty_and_between_days_holds_the_last(self, tmp_path):
        _write(tmp_path, D1, _us("A", "B"))
        _write(tmp_path, D3, _us("A", "C"))
        ml.record_membership(tmp_path, D3)
        log = ml.read_log(tmp_path)
        assert ml.members_as_of(log, date(2026, 10, 1)) == set()               # never invents history
        assert ml.members_as_of(log, D2) == set(_us("A", "B"))                 # D2 has no file: D1 still holds


class TestAppendOnlyAndIdempotent:

    def test_rerunning_a_day_changes_nothing(self, tmp_path):
        _write(tmp_path, D1, _us("A", "B"))
        _write(tmp_path, D2, _us("A", "C"))
        ml.record_membership(tmp_path, D2)
        before = (tmp_path / ml.LOG_FILENAME).read_bytes()
        assert ml.record_membership(tmp_path, D2) == 0
        assert (tmp_path / ml.LOG_FILENAME).read_bytes() == before

    def test_existing_rows_are_never_modified(self, tmp_path):
        _write(tmp_path, D1, _us("A", "B"))
        _write(tmp_path, D2, _us("A", "C"))
        ml.record_membership(tmp_path, D2)
        first = ml.read_log(tmp_path)
        _write(tmp_path, D3, _us("C", "D"))
        assert ml.record_membership(tmp_path, D3) == 2
        after = ml.read_log(tmp_path)
        assert after.head(first.height).equals(first)
        assert after.height == first.height + 2

    def test_the_first_resolution_of_a_day_is_authoritative(self, tmp_path):
        """A same-day re-resolution that comes out DIFFERENT is not logged: re-logging it would need
        corrective events whose replay order is ambiguous (a symbol that left in run 1 and re-enters in
        run 2). Known limitation: the log keeps the first resolution of the day."""
        _write(tmp_path, D1, _us("A", "B"))
        _write(tmp_path, D2, _us("A", "C"))
        ml.record_membership(tmp_path, D2)
        before = ml.read_log(tmp_path)
        _write(tmp_path, D2, _us("A", "Z"))                    # the resolver is re-run and comes out different
        assert ml.record_membership(tmp_path, D2) == 0
        assert ml.read_log(tmp_path).equals(before)

    def test_append_skips_keys_already_logged(self, tmp_path):
        """Defence in depth under the forward-only guard: the same events can never be written twice."""
        ev = pl.DataFrame([{"date": "2026-10-16", "market": "us_stocks", "symbol": "A", "event": "enter",
                            "rank": 1, "dollar_volume_20d": 1e9, "compared_with": None, "logged_at": "x"}],
                          schema=ml.SCHEMA)
        assert ml._append(tmp_path, ev) == 1
        before = (tmp_path / ml.LOG_FILENAME).read_bytes()
        assert ml._append(tmp_path, ev) == 0
        assert (tmp_path / ml.LOG_FILENAME).read_bytes() == before

    def test_seeding_is_a_single_atomic_write(self, tmp_path, monkeypatch):
        """A crash part-way through seeding must not leave a partial log."""
        for day, m in ((D1, _us("A")), (D2, _us("A", "B")), (D3, _us("B"))):
            _write(tmp_path, day, m)
        writes: list[int] = []
        real_write = ml.atomic_write_parquet
        monkeypatch.setattr(ml, "atomic_write_parquet", lambda df, *a, **k: (writes.append(df.height), real_write(df, *a, **k)))
        ml.record_membership(tmp_path, D3)
        assert len(writes) == 1 and writes[0] == 3                  # D1 enter A | D2 enter B | D3 leave A

    def test_a_new_day_is_diffed_against_the_log_not_against_the_previous_file(self, tmp_path):
        """The log says D2 = {B}. D2's file is gone. D3 = {B, C}: only C is new. Diffing D3 against the
        D1 file ({A}) would append 'A leaves' and 'B enters' on D3, contradicting the logged D2."""
        _write(tmp_path, D1, _us("A"))
        _write(tmp_path, D2, _us("B"))
        ml.record_membership(tmp_path, D2)
        (tmp_path / f"active_ohlcv_{D2.isoformat()}.parquet").unlink()
        _write(tmp_path, D3, _us("B", "C"))
        assert ml.record_membership(tmp_path, D3) == 1
        log = ml.read_log(tmp_path)
        assert _ev(log, D3, "enter") == {"C"} and _ev(log, D3, "leave") == set()
        assert ml.members_as_of(log, D3) == set(_us("B", "C"))

    def test_leavers_lose_rank_and_volume_only_when_their_previous_file_is_gone(self, tmp_path):
        _write(tmp_path, D1, _us("A", "B", base=9e9))
        ml.record_membership(tmp_path, D1)
        _write(tmp_path, D2, _us("A", base=9e9))
        ml.record_membership(tmp_path, D2)
        left = ml.read_log(tmp_path).filter((pl.col("date") == D2.isoformat()) & (pl.col("event") == "leave"))
        assert left["rank"].to_list() == [2] and left["dollar_volume_20d"].null_count() == 0
        (tmp_path / f"active_ohlcv_{D2.isoformat()}.parquet").unlink()                 # D2 file lost ...
        (tmp_path / f"active_ohlcv_{D1.isoformat()}.parquet").unlink()
        _write(tmp_path, D3, _us("C", base=9e9))                                       # ... then A leaves on D3
        ml.record_membership(tmp_path, D3)
        gone = ml.read_log(tmp_path).filter((pl.col("date") == D3.isoformat()) & (pl.col("event") == "leave"))
        assert set(gone["symbol"]) == {"A"} and gone["rank"].null_count() == 1

    def test_a_past_day_backfill_is_a_noop_once_the_log_is_ahead(self, tmp_path):
        _write(tmp_path, D1, _us("A"))
        _write(tmp_path, D3, _us("A", "B"))
        ml.record_membership(tmp_path, D3)
        before = ml.read_log(tmp_path)
        _write(tmp_path, D2, _us("A", "Z"))
        assert ml.record_membership(tmp_path, D2) == 0
        assert ml.read_log(tmp_path).equals(before)

    def test_the_log_is_replaced_atomically_leaving_no_temp_files(self, tmp_path):
        _write(tmp_path, D1, _us("A"))
        ml.record_membership(tmp_path, D1)
        assert not list(tmp_path.glob("*.tmp")) and not list(tmp_path.glob(".*"))


class TestFallbackAndFuture:

    def test_a_fallback_resolution_is_not_membership_and_not_a_baseline(self, tmp_path):
        _write(tmp_path, D1, _us("A", "B"))
        _write(tmp_path, D2, _us("A", "B", "C", "D", "E", "F"), fallback=True)   # full-universe placeholder
        _write(tmp_path, D3, _us("A", "C"))
        ml.record_membership(tmp_path, D3)
        log = ml.read_log(tmp_path)
        assert log.filter(pl.col("date") == D2.isoformat()).is_empty()
        assert _ev(log, D3, "enter") == {"C"} and _ev(log, D3, "leave") == {"B"}      # diffed against D1
        assert set(log.filter(pl.col("date") == D3.isoformat())["compared_with"]) == {D1.isoformat()}

    def test_recording_a_fallback_day_after_the_log_exists_appends_nothing(self, tmp_path):
        _write(tmp_path, D1, _us("A"))
        ml.record_membership(tmp_path, D1)
        _write(tmp_path, D2, _us("A", "B", "C"), fallback=True)
        assert ml.record_membership(tmp_path, D2) == 0
        assert ml.read_log(tmp_path)["date"].unique().to_list() == [D1.isoformat()]

    def test_a_call_only_ever_logs_its_own_run_date(self, tmp_path):
        """A real D2 file exists but was never logged; recording the (fallback) day D3 must not quietly
        log D2 on the way."""
        _write(tmp_path, D1, _us("A"))
        ml.record_membership(tmp_path, D1)
        _write(tmp_path, D2, _us("A", "B"))                    # real, but nobody called record for D2
        _write(tmp_path, D3, _us("A", "B", "C"), fallback=True)
        assert ml.record_membership(tmp_path, D3) == 0
        assert set(ml.read_log(tmp_path)["date"]) == {D1.isoformat()}

    def test_no_files_or_only_fallbacks_log_nothing(self, tmp_path):
        assert ml.record_membership(tmp_path, D1) == 0
        _write(tmp_path, D1, _us("A"), fallback=True)
        assert ml.record_membership(tmp_path, D1) == 0
        assert not (tmp_path / ml.LOG_FILENAME).exists()

    def test_files_dated_after_run_date_are_not_logged(self, tmp_path):
        _write(tmp_path, D1, _us("A"))
        _write(tmp_path, D3, _us("A", "B"))
        ml.record_membership(tmp_path, D1)
        assert set(ml.read_log(tmp_path)["date"]) == {D1.isoformat()}

    def test_an_unreadable_file_is_skipped_not_fatal(self, tmp_path):
        _write(tmp_path, D1, _us("A"))
        (tmp_path / f"active_ohlcv_{D2.isoformat()}.parquet").write_bytes(b"junk")
        _write(tmp_path, D3, _us("A", "B"))
        ml.record_membership(tmp_path, D3)
        log = ml.read_log(tmp_path)
        assert _ev(log, D3, "enter") == {"B"}


class TestResolverHook:

    @pytest.fixture
    def resolver(self, tmp_path, monkeypatch):
        from src.silver.active_symbols import ActiveSymbolsResolver

        monkeypatch.setattr(ActiveSymbolsResolver, "OUTPUT_PATH", property(lambda self: tmp_path))
        return ActiveSymbolsResolver()

    @staticmethod
    def _result(members: dict[tuple[str, str], float]) -> pl.DataFrame:
        return pl.DataFrame([{"symbol": s, "market": m, "dollar_volume_20d": v, "clean_days": 100,
                              "last_close": 10.0, "eligibility_reason": "ok"} for (m, s), v in members.items()])

    def test_save_writes_the_log(self, resolver, tmp_path):
        resolver._save(self._result(_us("A", "B")), D1, 0)
        resolver._save(self._result(_us("A", "C")), D2, 0)
        log = ml.read_log(tmp_path)
        assert _ev(log, D1, "enter") == {"A", "B"} and _ev(log, D2, "leave") == {"B"}

    def test_a_log_failure_never_fails_the_resolution(self, resolver, tmp_path, monkeypatch):
        monkeypatch.setattr(ml, "record_membership", lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
        resolver._save(self._result(_us("A")), D1, 0)
        assert (tmp_path / f"active_ohlcv_{D1.isoformat()}.parquet").exists()
        assert (tmp_path / f"active_{D1.isoformat()}.parquet").exists()
