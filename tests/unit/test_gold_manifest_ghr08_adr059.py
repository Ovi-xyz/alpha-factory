"""
tests/unit/test_gold_manifest_ghr08_adr059.py — GHR-08 (ADR-059): explain a Gold row delta at run time.

GD1: the 17 Oct deltas (1D +352, 1W +285, 1M +59) looked too large for one new day. They were an
active-set swap (F, WMB, CVNA, CB out; PTC, OXY, MO, SHW in) plus one new bar per persisting symbol:
+171 new bars + 10,095 entered rows - 9,922 left rows = +344 (1D). The baseline file had been
overwritten, so this had to be reconstructed afterwards. The manifest records it when it happens.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from src.gold import gold_manifest as gm

UTC = timezone.utc
D0 = datetime(2026, 1, 1, 5, 0, tzinfo=UTC)


def _frame(spec: dict[str, int], *, start: datetime = D0, close: float = 10.0) -> pl.DataFrame:
    """{symbol: n_bars} -> (symbol, timestamp, close), n consecutive daily bars from `start`."""
    rows = [(s, start + timedelta(days=i), close + i) for s, n in spec.items() for i in range(n)]
    return pl.DataFrame(rows, schema={"symbol": pl.Utf8, "timestamp": pl.Datetime("us", "UTC"),
                                      "close": pl.Float64}, orient="row")


class TestDecompose:

    def test_reproduces_the_16_to_17_oct_reconstruction(self):
        persisting = {f"P{i:03d}": 10 for i in range(171)}
        prev = _frame({**persisting, "F": 2480, "WMB": 2480, "CVNA": 2481, "CB": 2481})
        cur = _frame({**{s: n + 1 for s, n in persisting.items()},
                      "PTC": 2520, "OXY": 2525, "MO": 2525, "SHW": 2525})
        d = gm.decompose(prev, cur)
        assert d["n_added_persisting"] == 171
        assert d["n_entered_rows"] == 10_095
        assert d["n_left_rows"] == 9_922
        assert d["n_removed_persisting"] == 0
        assert d["n_changed"] == 0
        assert d["delta_rows"] == 344 == cur.height - prev.height
        assert json.loads(d["entered_symbols"]) == {"MO": 2525, "OXY": 2525, "PTC": 2520, "SHW": 2525}
        assert json.loads(d["left_symbols"]) == {"CB": 2481, "CVNA": 2481, "F": 2480, "WMB": 2480}

    def test_identity_holds_with_every_term_non_zero(self):
        prev = _frame({"A": 10, "B": 10, "GONE": 6})
        cur = _frame({"A": 12, "B": 10, "NEW": 4}).filter(~((pl.col("symbol") == "B") & (pl.col("timestamp") == D0)))
        d = gm.decompose(prev, cur)
        assert d["n_added_persisting"] == 2 and d["n_removed_persisting"] == 1
        assert d["n_entered_rows"] == 4 and d["n_left_rows"] == 6
        assert d["delta_rows"] == cur.height - prev.height

    def test_a_dropped_bar_of_a_persisting_symbol_is_a_removal_not_a_departure(self):
        """A flag flip (is_clean) removes a bar from a symbol that STAYED — GD6/GD7."""
        prev = _frame({"A": 10})
        cur = prev.slice(0, 9)
        d = gm.decompose(prev, cur)
        assert (d["n_removed_persisting"], d["n_left_rows"], d["delta_rows"]) == (1, 0, -1)

    def test_a_restated_close_is_a_change_and_not_a_delta(self):
        prev = _frame({"A": 5})
        cur = prev.with_columns(pl.when(pl.col("timestamp") == D0).then(pl.col("close") + 1.0)
                                .otherwise(pl.col("close")).alias("close"))
        d = gm.decompose(prev, cur)
        assert d["n_changed"] == 1 and d["delta_rows"] == 0

    def test_identical_frames_decompose_to_zero(self):
        f = _frame({"A": 5, "B": 5})
        d = gm.decompose(f, f)
        assert all(d[k] == 0 for k in ("n_added_persisting", "n_entered_rows", "n_left_rows",
                                       "n_removed_persisting", "n_changed", "delta_rows"))


class TestRecordRun:

    def test_first_run_has_no_comparison(self, tmp_path):
        row = gm.record_run(_frame({"A": 5}), "1D", date(2026, 10, 16), keys_base=tmp_path)
        assert row["rows_total"] == 5 and row["n_symbols"] == 1 and row["run_seq"] == 1
        assert row["prev_run_date"] is None and row["delta_rows"] is None
        assert (tmp_path / "tech_signals_1D_2026-10-16.parquet").exists()

    def test_next_day_is_compared_with_the_previous_snapshot(self, tmp_path):
        gm.record_run(_frame({"A": 5, "F": 3}), "1D", date(2026, 10, 16), keys_base=tmp_path)
        row = gm.record_run(_frame({"A": 6, "PTC": 4}), "1D", date(2026, 10, 17), keys_base=tmp_path)
        assert row["prev_run_date"] == "2026-10-16" and row["prev_rows_total"] == 8
        assert (row["n_added_persisting"], row["n_entered_rows"], row["n_left_rows"]) == (1, 4, 3)
        assert row["delta_rows"] == 10 - 8

    def test_same_day_rerun_reports_no_additions_and_keeps_the_first_snapshot(self, tmp_path):
        frame = _frame({"A": 5})
        gm.record_run(frame, "1D", date(2026, 10, 17), keys_base=tmp_path)
        first = (tmp_path / "tech_signals_1D_2026-10-17.parquet")
        before = first.read_bytes()
        row = gm.record_run(frame, "1D", date(2026, 10, 17), keys_base=tmp_path)
        assert row["run_seq"] == 2
        assert row["n_added_persisting"] == 0 and row["n_entered_rows"] == 0 and row["delta_rows"] == 0
        assert (tmp_path / "tech_signals_1D_2026-10-17.r2.parquet").exists()
        assert first.read_bytes() == before                        # never overwritten

    def test_retention_keeps_the_last_seven_per_timeframe_only(self, tmp_path):
        for i in range(10):
            gm.record_run(_frame({"A": 3}), "1D", date(2026, 10, 1) + timedelta(days=i), keys_base=tmp_path)
        gm.record_run(_frame({"A": 3}), "1W", date(2026, 10, 1), keys_base=tmp_path)
        days = [d for d, _, _ in gm.list_snapshots("1D", tmp_path)]
        assert len(days) == gm.KEEP_SNAPSHOTS_PER_TF == 7
        assert days[0] == "2026-10-04" and days[-1] == "2026-10-10"
        assert len(gm.list_snapshots("1W", tmp_path)) == 1

    def test_snapshots_are_ordered_numerically_not_lexically(self, tmp_path):
        for name in ("tech_signals_1D_2026-10-17.parquet", "tech_signals_1D_2026-10-17.r2.parquet",
                     "tech_signals_1D_2026-10-17.r10.parquet", "tech_signals_1D_2026-10-16.r3.parquet"):
            _frame({"A": 1}).write_parquet(tmp_path / name)
        order = [(d, s) for d, s, _ in gm.list_snapshots("1D", tmp_path)]
        assert order == [("2026-10-16", 3), ("2026-10-17", 1), ("2026-10-17", 2), ("2026-10-17", 10)]

    def test_timeframes_do_not_pick_up_each_others_snapshots(self, tmp_path):
        gm.record_run(_frame({"A": 5}), "1D", date(2026, 10, 16), keys_base=tmp_path)
        row = gm.record_run(_frame({"A": 5}), "1W", date(2026, 10, 17), keys_base=tmp_path)
        assert row["prev_run_date"] is None

    def test_an_unreadable_previous_snapshot_means_no_comparison_not_a_failure(self, tmp_path):
        (tmp_path / "tech_signals_1D_2026-10-16.parquet").write_bytes(b"not parquet")
        row = gm.record_run(_frame({"A": 5}), "1D", date(2026, 10, 17), keys_base=tmp_path)
        assert row["prev_run_date"] is None and row["rows_total"] == 5
        assert (tmp_path / "tech_signals_1D_2026-10-17.parquet").exists()

    def test_a_backfill_is_compared_with_the_latest_EARLIER_snapshot(self, tmp_path):
        gm.record_run(_frame({"A": 4}), "1D", date(2026, 10, 10), keys_base=tmp_path)
        gm.record_run(_frame({"A": 6}), "1D", date(2026, 10, 20), keys_base=tmp_path)
        row = gm.record_run(_frame({"A": 5}), "1D", date(2026, 10, 15), keys_base=tmp_path)
        assert row["prev_run_date"] == "2026-10-10"

    def test_cohort_max_stamps_for_daily_bars_only(self, tmp_path):
        us = _frame({"AAPL": 3}, start=datetime(2026, 10, 14, 4, 0, tzinfo=UTC))
        idx = _frame({"BBCA": 2}, start=datetime(2026, 10, 14, 17, 0, tzinfo=UTC))
        row = gm.record_run(pl.concat([us, idx]), "1D", date(2026, 10, 17), keys_base=tmp_path)
        stamps = json.loads(row["cohort_max_stamps"])
        assert set(stamps) == {"04:00", "17:00"}
        assert stamps["04:00"].startswith("2026-10-16T04:00") and stamps["17:00"].startswith("2026-10-15T17:00")
        assert gm.record_run(pl.concat([us, idx]), "1H", date(2026, 10, 17), keys_base=tmp_path)["cohort_max_stamps"] is None

    def test_a_date_typed_timestamp_is_tolerated(self, tmp_path):
        f = pl.DataFrame({"symbol": ["A", "A"], "timestamp": [date(2026, 1, 1), date(2026, 1, 2)], "close": [1.0, 2.0]})
        assert gm.record_run(f, "1D", date(2026, 10, 17), keys_base=tmp_path)["cohort_max_stamps"] is None


class TestWriteManifest:

    def test_no_rows_writes_nothing(self, tmp_path):
        assert gm.write_manifest([], date(2026, 10, 17), manifest_base=tmp_path) is None
        assert not list(tmp_path.glob("*"))

    def test_reruns_get_their_own_file_and_never_rewrite_an_earlier_one(self, tmp_path):
        row = {"run_date": "2026-10-17", "timeframe": "1D", "rows_total": 5}
        p1 = gm.write_manifest([row], date(2026, 10, 17), manifest_base=tmp_path)
        before = p1.read_bytes()
        p2 = gm.write_manifest([{**row, "rows_total": 6}], date(2026, 10, 17), manifest_base=tmp_path)
        assert p1.name == "gold_signals_2026-10-17.parquet" and p2.name == "gold_signals_2026-10-17.r2.parquet"
        assert p1.read_bytes() == before


class TestGoldSignalsIntegration:
    """Through the real gold_signals entry points (Silver -> Gold), not the module alone."""

    @staticmethod
    def _silver(tmp_path: Path, symbol: str, n: int) -> None:
        path = tmp_path / "us_stocks" / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": [symbol] * n,
            "timestamp": [datetime(2026, 5, 1, 4, 0, tzinfo=UTC) + timedelta(days=d) for d in range(n)],
            "open": [150.0 + d for d in range(n)], "high": [155.0 + d for d in range(n)],
            "low": [145.0 + d for d in range(n)], "close": [152.0 + d for d in range(n)],
            "volume": [1_000_000] * n, "is_clean": [True] * n,
        }).write_parquet(path)

    @pytest.fixture
    def env(self, tmp_path, monkeypatch):
        import src.gold.technical_signals as ts

        monkeypatch.setattr(ts, "TIMEFRAMES", ["1D"])
        monkeypatch.setattr(ts, "SILVER_OHLCV_PATH", tmp_path / "silver")
        monkeypatch.setattr(ts, "GOLD_SIG_PATH", tmp_path / "gold" / "signals")
        monkeypatch.setattr(gm, "keys_dir", lambda: tmp_path / "keys")
        monkeypatch.setattr(gm, "manifest_dir", lambda: tmp_path / "manifest")
        return ts, tmp_path

    def test_run_returns_its_rows_and_writes_manifest_and_snapshot(self, env):
        ts, tmp = env
        self._silver(tmp / "silver", "AAPL", 30)
        self._silver(tmp / "silver", "MSFT", 30)
        with patch.object(ts, "_resolve_active_ohlcv_symbols", return_value=["AAPL", "MSFT"]):
            out = ts.run(date(2031, 6, 2))
        assert out.rows_written == 60
        man = pl.read_parquet(next((tmp / "manifest").glob("gold_signals_2031-06-02*.parquet")))
        assert man["timeframe"].to_list() == ["1D"] and man["rows_total"].to_list() == [60]
        assert man["n_symbols"].to_list() == [2]
        assert (tmp / "keys" / "tech_signals_1D_2031-06-02.parquet").exists()

    def test_a_symbol_swap_between_runs_is_explained(self, env):
        ts, tmp = env
        self._silver(tmp / "silver", "AAPL", 30)
        self._silver(tmp / "silver", "MSFT", 30)
        self._silver(tmp / "silver", "NVDA", 20)
        with patch.object(ts, "_resolve_active_ohlcv_symbols", return_value=["AAPL", "MSFT"]):
            ts.run(date(2031, 6, 3))
        with patch.object(ts, "_resolve_active_ohlcv_symbols", return_value=["AAPL", "NVDA"]):
            ts.run(date(2031, 6, 4))
        man = pl.read_parquet(next((tmp / "manifest").glob("gold_signals_2031-06-04*.parquet")))
        r = man.to_dicts()[0]
        assert json.loads(r["entered_symbols"]) == {"NVDA": 20} and json.loads(r["left_symbols"]) == {"MSFT": 30}
        assert r["delta_rows"] == (30 + 20) - (30 + 30) == r["rows_total"] - r["prev_rows_total"]

    def test_a_resumed_run_with_everything_done_reports_zero_rows_and_no_manifest(self, env):
        ts, tmp = env
        self._silver(tmp / "silver", "AAPL", 30)
        with patch.object(ts, "_resolve_active_ohlcv_symbols", return_value=["AAPL"]):
            first = ts.run(date(2031, 6, 5))
            second = ts.run(date(2031, 6, 5))                 # checkpoint: the TF is already done
        assert first.rows_written == 30 and second.rows_written == 0
        assert len(list((tmp / "manifest").glob("gold_signals_2031-06-05*.parquet"))) == 1

    def test_a_manifest_failure_never_fails_the_job(self, env):
        ts, tmp = env
        self._silver(tmp / "silver", "AAPL", 30)
        with patch.object(ts, "_resolve_active_ohlcv_symbols", return_value=["AAPL"]), \
                patch.object(gm, "record_run", side_effect=OSError("disk full")), \
                patch.object(gm, "write_manifest", side_effect=OSError("disk full")):
            out = ts.run(date(2031, 6, 6))
        assert out.rows_written == 30
        assert (tmp / "gold" / "signals" / "tech_signals_1D.parquet").exists()
