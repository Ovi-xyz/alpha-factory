"""tests/unit/test_label_triple_barrier_script.py — ADR-052 offline labeling driver."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from scripts.label_triple_barrier import (
    grid_triples, inspection_sample, main, summarise_by_market, triples_from_mtf,
)
from src.backtest.triple_barrier import BarrierConfig, LookaheadTripleBarrierLabeler

WARMUP = (100.0, 101.0, 99.0, 100.0)


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _silver(root: Path, market: str, symbol: str, n: int = 60, start: date = date(2026, 1, 5)) -> list[date]:
    """n bars: flat-ish 200 bps true range, then a rising drift so some trades win."""
    dates = _weekdays(start, n)
    bars = [(100.0 + 0.4 * i, 101.0 + 0.4 * i, 99.0 + 0.4 * i, 100.0 + 0.4 * i) for i in range(n)]
    ts = (pl.Series("timestamp", [datetime(d.year, d.month, d.day) for d in dates])
          .cast(pl.Datetime("ms")).dt.replace_time_zone("America/New_York"))
    df = pl.DataFrame({"timestamp": ts, "open": [b[0] for b in bars], "high": [b[1] for b in bars],
                       "low": [b[2] for b in bars], "close": [b[3] for b in bars],
                       "volume": [1] * n, "is_clean": [True] * n, "symbol": [symbol] * n})
    d = root / market / f"symbol={symbol}"
    d.mkdir(parents=True, exist_ok=True)
    df.write_parquet(d / f"{symbol}_1D_silver.parquet")
    return dates


def _labeler(root: Path, markets=("us_stocks", "commodity")) -> LookaheadTripleBarrierLabeler:
    return LookaheadTripleBarrierLabeler(BarrierConfig(), silver_root=root, allowed_markets=list(markets))


def _mtf(dirpath: Path, run_date: date, rows: list[tuple], legacy_cols: bool = False) -> None:
    data = {"symbol": [r[0] for r in rows], "date": [run_date.isoformat()] * len(rows),
            "mtf_score": [r[1] for r in rows], "signal_quality": [r[2] for r in rows]}
    if legacy_cols:
        data["regime_compatible"] = [True] * len(rows)
        data["active_regime"] = ["RISK_ON"] * len(rows)
    dirpath.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(data).write_parquet(dirpath / f"mtf_alignment_{run_date.isoformat()}.parquet")


class TestTriplesFromMtf:

    def test_gate_and_direction(self, tmp_path):
        _mtf(tmp_path, date(2026, 3, 2), [("AAA", 4, "A"), ("BBB", -3, "B"), ("CCC", 2, "A"),
                                          ("DDD", 5, "C"), ("EEE", -5, "A")])
        t = triples_from_mtf(str(tmp_path / "mtf_alignment_*.parquet"))
        got = {(r["symbol"], r["direction"]) for r in t.iter_rows(named=True)}
        assert got == {("AAA", "long"), ("BBB", "short"), ("EEE", "short")}
        assert t.schema["decision_date"] == pl.Date

    def test_mixed_legacy_and_current_files_read_together(self, tmp_path):
        # ADR-055 v2 drift class: files before/after the cutover differ in columns.
        _mtf(tmp_path, date(2026, 3, 2), [("AAA", 4, "A")], legacy_cols=True)
        _mtf(tmp_path, date(2026, 3, 3), [("AAA", 4, "A")], legacy_cols=False)
        t = triples_from_mtf(str(tmp_path / "mtf_alignment_*.parquet"))
        assert t.height == 2

    def test_no_files_is_empty_not_an_exception(self, tmp_path):
        assert triples_from_mtf(str(tmp_path / "nothing_*.parquet")).is_empty()


class TestGridTriples:

    def test_both_directions_every_step_and_market_carried(self, tmp_path):
        dates = _silver(tmp_path, "us_stocks", "AAA", n=40)
        g = grid_triples(_labeler(tmp_path), ["AAA"], years=5, step=5, today=date(2026, 12, 31))
        assert g.columns == ["symbol", "market", "decision_date", "direction"]
        assert set(g["direction"].to_list()) == {"long", "short"}
        assert set(g["market"].to_list()) == {"us_stocks"}
        assert sorted(set(g["decision_date"].to_list())) == dates[::5]

    def test_years_window_cuts_history(self, tmp_path):
        dates = _silver(tmp_path, "us_stocks", "AAA", n=40)
        g = grid_triples(_labeler(tmp_path), None, years=0.1, step=1, today=dates[-1])
        assert min(g["decision_date"].to_list()) >= dates[-1] - timedelta(days=int(365.25 * 0.1))

    def test_same_symbol_in_two_markets_stays_unambiguous(self, tmp_path):
        _silver(tmp_path, "us_stocks", "CL", n=40)
        _silver(tmp_path, "commodity", "CL", n=40)
        lab = _labeler(tmp_path)
        g = grid_triples(lab, ["CL"], years=5, step=10, today=date(2026, 12, 31))
        assert set(g["market"].to_list()) == {"us_stocks", "commodity"}
        res = lab.label(g)
        assert "ambiguous_market" not in res.skipped["reason"].to_list()
        assert set(res.labels["market"].to_list()) == {"us_stocks", "commodity"}

    def test_bad_step_rejected_and_no_symbols_is_empty(self, tmp_path):
        with pytest.raises(ValueError):
            grid_triples(_labeler(tmp_path), None, step=0)
        assert grid_triples(_labeler(tmp_path), ["ZZZ"]).is_empty()


class TestReporting:

    def _result(self, tmp_path):
        _silver(tmp_path, "us_stocks", "AAA", n=60)
        lab = _labeler(tmp_path)
        g = grid_triples(lab, ["AAA"], years=5, step=3, today=date(2026, 12, 31))
        return lab, lab.label(g)

    def test_by_market_table(self, tmp_path):
        _, res = self._result(tmp_path)
        t = summarise_by_market(res.labels)
        assert t["market"].to_list() == ["us_stocks"]
        for col in ("n", "win_rate", "zero_drift_ref", "stop_at_floor", "stop_at_ceiling",
                    "time_exit", "same_bar_amb", "gap_through", "mean_bars", "median_vol_bps"):
            assert col in t.columns
        assert summarise_by_market(pl.DataFrame()).is_empty()

    def test_inspection_sample_has_barriers_and_forward_bars(self, tmp_path):
        lab, res = self._result(tmp_path)
        s = inspection_sample(res, lab, n=5)
        assert s.height == 5
        r = s.row(0, named=True)
        assert r["forward_bars"].count("|") + 1 >= 1 and "O" in r["forward_bars"]
        sign = 1 if r["direction"] == "long" else -1
        assert sign * (r["profit_price"] - r["entry_price"]) > 0 > sign * (r["stop_price"] - r["entry_price"])
        assert inspection_sample(res, lab, n=0).is_empty()


class TestMain:

    def test_grid_run_writes_outputs_and_summary(self, tmp_path, capsys):
        silver, out = tmp_path / "silver", tmp_path / "out"
        _silver(silver, "us_stocks", "AAA", n=60)
        rc = main(["--grid", "AAA", "--years", "50", "--step", "2", "--silver-root", str(silver),
                   "--out-dir", str(out), "--inspect", "4"])
        # layer1 markets come from the instrument registry; tolerate an environment where
        # 'us_stocks' resolves (the real registry has it)
        assert rc == 0
        tag = max(p.stem.split("_", 1)[1] for p in out.glob("labels_*.parquet"))
        assert (out / f"labels_{tag}.parquet").exists() and (out / f"skipped_{tag}.parquet").exists()
        assert (out / f"inspection_{tag}.csv").exists()
        summary = json.loads((out / f"summary_{tag}.json").read_text())
        assert summary["n_labeled"] > 0 and "zero_drift_reference_win_rate" in summary
        assert "us_stocks" in capsys.readouterr().out

    def test_empty_source_returns_1(self, tmp_path, capsys):
        rc = main(["--from-mtf", "--mtf-glob", str(tmp_path / "none_*.parquet"),
                   "--out-dir", str(tmp_path / "out")])
        assert rc == 1 and "No triples" in capsys.readouterr().out

    def test_config_overrides_reach_the_label_spec(self, tmp_path):
        silver, out = tmp_path / "silver", tmp_path / "out"
        _silver(silver, "us_stocks", "AAA", n=60)
        main(["--grid", "AAA", "--years", "50", "--horizon", "5", "--time-policy", "exclude",
              "--same-bar", "profit_first", "--entry-mode", "signal_close",
              "--stop-vol-mult", "0.3", "--profit-vol-mult", "0.75",
              "--silver-root", str(silver), "--out-dir", str(out)])
        spec = pl.read_parquet(next(out.glob("labels_*.parquet")))["label_spec"][0]
        for token in ("h5", "signal_close", "time=exclude", "same=profit_first", "x0.3/0.75"):
            assert token in spec
