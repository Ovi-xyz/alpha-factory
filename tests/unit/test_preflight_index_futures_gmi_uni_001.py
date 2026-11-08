"""
GMI-UNI-001 follow-up (chat thread, Ovi, 7 Nov 2026): scripts/preflight/check_index_futures.py.

The first version gated NKD_F on a same-date correlation floor (>= 0.50) that nobody had measured, and failed on
the first live run at 0.397 although the instrument was sound. Live diagnosis (same day): level ratio vs ^N225
median 1.0038, corr(k=0) +0.455, corr(k=+1) +0.417, two-lag R^2 0.42 — the CME bar for date D contains Tokyo
session D plus the Europe/US session, so a same-date test is the wrong shape for a Tokyo underlying.

Pinned here:
  * only measured / structural facts gate (data present, depth, freshness, index-level sanity);
  * the cash-index relationship is INFORMATION: an uncorrelated cash series must not change the exit status;
  * returns are computed on the COMMON calendar (a holiday in one market must not distort the other's return);
  * the lag convention: k=+1 pairs future[D] with cash[D+1].

The synthetic world encodes the CME-session overlap explicitly, so these tests check the CODE, not Yahoo.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.preflight import check_index_futures as cif
from src.config.instrument_loader import get_loader

N = 3000


def _idx(n: int = N, tz: str = "America/New_York") -> pd.DatetimeIndex:
    end = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
    return pd.bdate_range(end=end, periods=n).tz_localize(tz)


def _frame(logret, idx, start: float = 20_000.0, volume: bool = True) -> pd.DataFrame:
    close = start * np.exp(np.cumsum(logret))
    df = pd.DataFrame({"Open": close, "High": close, "Low": close, "Close": close}, index=idx)
    df["Volume"] = np.random.default_rng(0).integers(1_000, 5_000, len(idx)) if volume else 0
    return df


def _world(seed: int = 7):
    """future[D] = early + tokyo + us[D];  cash[D] = us[D-1] + early + tokyo  (the CME-session overlap)."""
    rng = np.random.default_rng(seed)
    s = 0.008
    us, tk, ea = (rng.normal(0, s * np.sqrt(v), N) for v in (1.0, 0.62, 0.04))
    fut = ea + tk + us
    cash = np.r_[0.0, us[:-1]] + ea + tk + rng.normal(0, s * 0.15, N)
    return fut, cash


def _fetcher(frames: dict):
    def fetch(ticker, period="max"):
        if ticker not in frames:
            raise RuntimeError(f"no frame for {ticker}")
        return frames[ticker]
    return fetch


def _inst(symbol="NKD_F"):
    return get_loader().get(symbol)


def _ok_frames(fut_start=20_000.0):
    fut, cash = _world()
    return {"NKD=F": _frame(fut, _idx(), fut_start),
            "^N225": _frame(cash, _idx(tz="Asia/Tokyo"), 20_000.0)}


# ── gates ─────────────────────────────────────────────────────────────────────

class TestGates:

    def test_sound_series_passes_and_reports_the_lag_table(self):
        ok, notes = cif._check_one(_inst(), _fetcher(_ok_frames()))
        text = "\n".join(notes)
        assert ok is True, text
        assert "k=-1" in text and "k=0" in text and "k=+1" in text and "two-lag R^2" in text
        assert "level ratio vs N225" in text

    def test_equity_level_price_fails_the_gate(self):
        """A bare-root equity ticker (Eversource ~USD 60) must fail: this is the ES vs ES=F guard."""
        fut, cash = _world()
        frames = {"NKD=F": _frame(fut, _idx(), 60.0),                      # random walk around USD 60
                  "^N225": _frame(cash, _idx(tz="Asia/Tokyo"))}
        ok, notes = cif._check_one(_inst(), _fetcher(frames))
        assert ok is False and any("FAIL last close" in n for n in notes)

    def test_shallow_history_fails_the_gate(self):
        fut, cash = _world()
        short = _idx(n=1200)                                     # ~4.6 years
        frames = {"NKD=F": _frame(fut[-1200:], short), "^N225": _frame(cash[-1200:], _idx(1200, "Asia/Tokyo"))}
        ok, notes = cif._check_one(_inst(), _fetcher(frames))
        assert ok is False and any("FAIL history" in n for n in notes)

    def test_stale_last_bar_fails_the_gate(self):
        fut, cash = _world()
        old = _idx().shift(-30, freq="D")                       # last bar ~a month old
        frames = {"NKD=F": _frame(fut, old), "^N225": _frame(cash, _idx(tz="Asia/Tokyo"))}
        ok, notes = cif._check_one(_inst(), _fetcher(frames))
        assert ok is False and any("FAIL last bar" in n for n in notes)

    def test_missing_columns_and_empty_frames_fail(self):
        fut, cash = _world()
        bad = _frame(fut, _idx()).drop(columns=["Volume"])
        assert cif._check_one(_inst(), _fetcher({"NKD=F": bad}))[0] is False
        assert cif._check_one(_inst(), _fetcher({"NKD=F": pd.DataFrame()}))[0] is False


    def test_zero_row_frame_and_none_fail_cleanly_without_raising(self):
        """Right columns but no rows (and a fetcher returning None) must be a reported FAIL, not an exception."""
        cols = ["Open", "High", "Low", "Close", "Volume"]
        for empty in (pd.DataFrame(columns=cols), None):
            ok, notes = cif._check_one(_inst(), _fetcher({"NKD=F": empty}))
            assert ok is False and notes == ["FAIL no data returned"]


# ── information never gates (the regression for the original false FAIL) ──────

class TestInformationNeverGates:

    def test_uncorrelated_cash_series_does_not_fail(self):
        """The first version failed NKD_F on correlation alone. Even a cash series with zero relationship must
        now leave the exit status to the gates — the numbers are printed, not judged."""
        fut, _ = _world()
        rng = np.random.default_rng(99)
        frames = {"NKD=F": _frame(fut, _idx()),
                  "^N225": _frame(rng.normal(0, 0.008, N), _idx(tz="Asia/Tokyo"))}
        ok, notes = cif._check_one(_inst(), _fetcher(frames))
        assert ok is True, "\n".join(notes)
        lc_line = next(n for n in notes if "return corr vs N225" in n)
        assert "not a gate" in lc_line

    def test_missing_cash_index_does_not_fail(self):
        fut, _ = _world()
        ok, notes = cif._check_one(_inst(), _fetcher({"NKD=F": _frame(fut, _idx())}))   # cash fetch raises
        assert ok is True
        assert any("relationship unavailable" in n for n in notes)

    def test_no_correlation_threshold_remains_in_the_module(self):
        assert all(len(v) == 2 for v in cif.TRACKS.values())     # (cash symbol, yahoo ticker) — no floor
        assert not any(name.startswith("MIN_CORR") for name in dir(cif))


# ── relationship maths ────────────────────────────────────────────────────────

class TestCashRelationship:

    def test_cme_session_overlap_signature(self):
        """Same-date correlation is moderate, next-day is comparable, the two together explain much more."""
        fut, cash = _world()
        rel = cif.cash_relationship(_frame(fut, _idx()), _frame(cash, _idx(tz="Asia/Tokyo")))
        lc = rel["lag_corr"]
        assert 0.30 < lc[0] < 0.55
        assert lc[1] > 0.45 and lc[1] > lc[-1] + 0.30
        assert rel["r2_two_lags"] > 1.5 * lc[0] ** 2

    def test_lag_convention_k_plus_one_pairs_future_d_with_cash_d_plus_one(self):
        rng = np.random.default_rng(3)
        base = rng.normal(0, 0.01, N)
        fut = _frame(base, _idx())
        cash_next = _frame(np.r_[0.0, base[:-1]], _idx(tz="Asia/Tokyo"))  # cash return on D+1 == future return on D
        lc = cif.cash_relationship(fut, cash_next)["lag_corr"]
        assert lc[1] > 0.95 and abs(lc[0]) < 0.1 and abs(lc[-1]) < 0.1

    def test_returns_are_computed_on_the_common_calendar(self):
        """A holiday present in only one market must not distort returns. cash == future on every shared date
        but is missing 10% of dates; computed on each series' own calendar the correlation would drop below 1."""
        rng = np.random.default_rng(5)
        idx = _idx()
        fut = _frame(rng.normal(0, 0.01, N), idx)
        keep = np.ones(N, bool)
        keep[rng.choice(np.arange(10, N - 10), size=N // 10, replace=False)] = False
        cash = fut.loc[keep, ["Close"]].copy()
        cash.index = cash.index.tz_localize(None).tz_localize("Asia/Tokyo")
        rel = cif.cash_relationship(fut, cash)
        assert rel["lag_corr"][0] > 0.999
        assert rel["ratio"]["median"] == pytest.approx(1.0)

    def test_level_ratio_direction_is_future_over_cash(self):
        """future / cash, not the inverse: with the future at twice the cash level the median ratio is 2."""
        rng = np.random.default_rng(11)
        cash = _frame(rng.normal(0, 0.01, N), _idx(tz="Asia/Tokyo"))
        fut = cash.copy() * 2.0
        fut.index = fut.index.tz_localize(None).tz_localize("America/New_York")
        assert cif.cash_relationship(fut, cash)["ratio"]["median"] == pytest.approx(2.0)

    def test_level_ratio_and_jump_audit(self):
        fut, cash = _world()
        f = _frame(fut, _idx())
        f.iloc[1500, f.columns.get_loc("Close")] = f["Close"].iloc[1499] * 1.20      # an isolated +20% bar
        f.iloc[1501:, f.columns.get_loc("Close")] = f["Close"].iloc[1501:] * 1.20 / f["Close"].iloc[1500] * f["Close"].iloc[1500]
        rel = cif.cash_relationship(f, _frame(cash, _idx(tz="Asia/Tokyo")))
        assert rel["jumps"]["n"] >= 1
        assert any(d for d in rel["jumps"]["unmatched"])      # the artefact has no cash-index counterpart


# ── entry point ───────────────────────────────────────────────────────────────

class TestMain:

    def test_list_mode_is_offline_and_exits_zero(self, capsys):
        assert cif.main(["--list"]) == 0
        out = capsys.readouterr().out
        for sym in ("ES_F", "NKD_F", "NQ_F", "YM_F"):
            assert sym in out

    def test_unknown_symbol_exits_one(self):
        assert cif.main(["--symbol", "NOPE"]) == 1

    def test_exit_status_follows_the_gates(self, monkeypatch):
        monkeypatch.setattr(cif, "_check_one", lambda inst: (inst.symbol != "NQ_F", ["x"]))
        assert cif.main([]) == 1
        monkeypatch.setattr(cif, "_check_one", lambda inst: (True, ["x"]))
        assert cif.main([]) == 0
