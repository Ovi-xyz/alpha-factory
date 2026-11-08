"""
scripts/preflight/check_index_futures.py

GMI-UNI-001 (chat thread, Ovi, 7 Nov 2026): Layer 1 gained the market `index_futures`
(ES_F, NKD_F, NQ_F, YM_F -> Yahoo ES=F, NKD=F, NQ=F, YM=F).

Run this on hardware with Yahoo access BEFORE the first `bronze_ohlcv_daily` run that includes the new symbols
(the sandbox that authored it has none, same constraint as every script in scripts/preflight/, ADR-025).

GATES (exit code 1 if any fails) — every gate has a measured or structural basis:
  1. Yahoo returns a non-empty daily frame with Open/High/Low/Close/Volume.
  2. History depth >= MIN_HISTORY_YEARS (platform design: 10 years; Bronze cold start asks for 10).
     Measured live 7 Nov 2026: ES 26.1y, NKD 22.7y, NQ 26.1y, YM 24.6y.
  3. The last bar is recent (<= MAX_LAST_BAR_AGE_DAYS calendar days). Measured: 1 day for all four.
  4. The ticker is the FUTURE, not an equity sharing the root: last close > MIN_INDEX_LEVEL. Yahoo's bare `ES`
     is Eversource (~USD 60-70). Measured: ES 7,402 / NKD 58,265 / NQ 27,085 / YM 51,385.

INFORMATION ONLY (never affects the exit code) — printed because they were NOT calibrated when this was written:
  5. Level ratio future / cash index on the common calendar (median, p05, p95, share within +-3%). Identity
     evidence that does not depend on timing. NQ tracks the Nasdaq-100 and the cash proxy is the Composite, so
     its ratio is not expected to be 1 — read its stability, not its level. Measured NKD: median 1.0038.
  6. Return correlation with the cash index at lags k = -1, 0, +1 and the two-lag R^2, on COMMON-calendar returns.
     A same-date correlation alone is misleading for NKD: the CME bar for date D spans ET D-1 18:00..D 17:00, so it
     contains Tokyo session D plus the whole Europe/US session D, and the Europe/US part reaches ^N225 on D+1.
     Measured 7 Nov 2026 (last 250 sessions): ES 0.984, NQ 0.957, YM 0.993 same-date; NKD k=0 +0.455, k=+1 +0.417,
     two-lag R^2 0.42, corr vs ES 0.76. The original gate (same-date >= 0.50 for NKD) produced a false FAIL (0.397).
  7. Volume==0 share, one-day moves > JUMP_PCT and how many of them have no same-sign cash-index move >=
     JUMP_MATCH_MIN within +-1 day (candidate roll / tick artefacts), bar stamp timezone.

Usage:
    python scripts/preflight/check_index_futures.py            # all four, live
    python scripts/preflight/check_index_futures.py --symbol NKD_F
    python scripts/preflight/check_index_futures.py --list     # offline: print the registry rows only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

MIN_HISTORY_YEARS = 10
MAX_LAST_BAR_AGE_DAYS = 6
MIN_INDEX_LEVEL = 1_000.0
CORR_WINDOW = 250
JUMP_PCT = 0.08
JUMP_MATCH_MIN = 0.03          # info heuristic only: "a cash-index move of at least 3% in the same direction"

# future symbol -> (Layer 2 cash index symbol, Yahoo ticker of that index). No thresholds: see header.
TRACKS: dict[str, tuple[str, str]] = {
    "ES_F":  ("SPX",  "^GSPC"),
    "NQ_F":  ("IXIC", "^IXIC"),
    "YM_F":  ("DJI",  "^DJI"),
    "NKD_F": ("N225", "^N225"),
}


def _registry_rows(only: str | None):
    from src.config.instrument_loader import get_loader

    rows = get_loader().by_market("index_futures")
    return [r for r in rows if only is None or r.symbol == only]


def _yf_fetch(ticker: str, period: str = "max"):
    import yfinance as yf

    return yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=False)


def _by_date(df):
    """Close series indexed by tz-naive calendar date of the bar stamp (NY midnight vs Tokyo midnight)."""
    import pandas as pd

    s = df["Close"].copy()
    s.index = pd.DatetimeIndex(s.index).tz_localize(None).normalize()
    return s[~s.index.duplicated(keep="last")].dropna()


def cash_relationship(fut_df, cash_df, window: int = CORR_WINDOW) -> dict:
    """Timing-independent and lag-aware relationship of a future to its cash index (information only).

    Returns are computed on the COMMON calendar (both series reduced to shared dates first), so a holiday that
    exists in only one market does not make one return span two days and the other one day.
    """
    import numpy as np
    import pandas as pd

    fut, cash = _by_date(fut_df), _by_date(cash_df)
    common = fut.index.intersection(cash.index)
    J = pd.DataFrame({"f": fut.loc[common], "c": cash.loc[common]})
    R = J.pct_change().dropna()
    out: dict = {"n_common": len(J)}

    ratio = (J["f"] / J["c"]).tail(window)
    out["ratio"] = {"median": float(ratio.median()), "p05": float(ratio.quantile(.05)),
                    "p95": float(ratio.quantile(.95)), "within3": float(((ratio - 1).abs() <= 0.03).mean())}

    rw = R.tail(window)
    out["lag_corr"] = {k: float(rw["f"].corr(R["c"].shift(-k).loc[rw.index])) for k in (-1, 0, 1)}
    X = np.column_stack([R["c"].loc[rw.index], R["c"].shift(-1).loc[rw.index], np.ones(len(rw))])
    ok = ~np.isnan(X).any(axis=1)
    y = rw["f"].to_numpy()[ok]
    beta, *_ = np.linalg.lstsq(X[ok], y, rcond=None)
    out["r2_two_lags"] = float(1 - (y - X[ok] @ beta).var() / y.var())

    unmatched = []
    jumps = R["f"][R["f"].abs() > JUMP_PCT]
    for d, r in jumps.items():
        i = R.index.get_loc(d)
        near = [float(R["c"].iloc[i + k]) for k in (-1, 0, 1) if 0 <= i + k < len(R)]
        if not any(np.sign(v) == np.sign(r) and abs(v) >= JUMP_MATCH_MIN for v in near):
            unmatched.append(str(d.date()))
    out["jumps"] = {"n": int(len(jumps)), "unmatched": unmatched}
    return out


def _check_one(inst, fetch=_yf_fetch) -> tuple[bool, list[str]]:
    import pandas as pd

    notes: list[str] = []
    ok = True
    df = fetch(inst.yfinance_symbol, "max")
    if df is None or df.empty:
        return False, ["FAIL no data returned"]
    missing = {"Open", "High", "Low", "Close", "Volume"} - set(df.columns)
    if missing:
        return False, [f"FAIL missing columns {sorted(missing)}"]

    first, last = df.index[0], df.index[-1]
    years = (last - first).days / 365.25
    ok &= years >= MIN_HISTORY_YEARS
    notes.append(f"{'ok  ' if years >= MIN_HISTORY_YEARS else 'FAIL'} history {years:.1f}y ({first.date()} .. {last.date()})")

    age = (pd.Timestamp.now(tz=last.tz) - last).days
    ok &= age <= MAX_LAST_BAR_AGE_DAYS
    notes.append(f"{'ok  ' if age <= MAX_LAST_BAR_AGE_DAYS else 'FAIL'} last bar {age}d old")

    level = float(df["Close"].iloc[-1])
    ok &= level > MIN_INDEX_LEVEL
    notes.append(f"{'ok  ' if level > MIN_INDEX_LEVEL else 'FAIL'} last close {level:,.2f} (floor {MIN_INDEX_LEVEL:,.0f} — an equity ticker would sit far below)")

    cash_sym, cash_yahoo = TRACKS.get(inst.symbol, (None, None))
    if cash_yahoo:
        try:
            rel = cash_relationship(df, fetch(cash_yahoo, "max"))
            r = rel["ratio"]
            notes.append(f"info level ratio vs {cash_sym} ({cash_yahoo}): median {r['median']:.4f} p05 {r['p05']:.4f} "
                         f"p95 {r['p95']:.4f} within +-3% {r['within3']:.0%}")
            lc = rel["lag_corr"]
            notes.append(f"info return corr vs {cash_sym}: k=-1 {lc[-1]:+.3f}  k=0 {lc[0]:+.3f}  k=+1 {lc[1]:+.3f}  "
                         f"two-lag R^2 {rel['r2_two_lags']:.3f}  [U: not a gate, uncalibrated]")
            j = rel["jumps"]
            notes.append(f"info {j['n']} one-day moves > {JUMP_PCT:.0%}; {len(j['unmatched'])} with no same-sign "
                         f"{cash_sym} move >= {JUMP_MATCH_MIN:.0%} within +-1 day {j['unmatched'][-8:]}")
        except Exception as exc:                                    # info section must never decide the gate
            notes.append(f"info cash-index relationship unavailable: {type(exc).__name__}: {exc}")

    zero_share = float((df["Volume"].fillna(0) == 0).mean())
    notes.append(f"info volume==0 on {zero_share:.1%} of bars; bar stamp tz={last.tz} last stamp={last}")
    return bool(ok), notes


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--symbol", help="check one future by registry symbol, e.g. ES_F")
    ap.add_argument("--list", action="store_true", help="offline: print the registry rows and exit")
    args = ap.parse_args(argv)

    rows = _registry_rows(args.symbol)
    if not rows:
        print(f"no index_futures instrument matches {args.symbol!r}")
        return 1
    if args.list:
        for r in rows:
            print(f"{r.symbol:6s} raw={r.raw_symbol:6s} yahoo={r.yfinance_symbol:6s} market={r.market} tz={r.timezone}")
        return 0

    failed = 0
    for r in rows:
        print(f"\n== {r.symbol}  (Yahoo {r.yfinance_symbol})")
        try:
            ok, notes = _check_one(r)
        except Exception as exc:                                    # network / parsing — report, do not hide
            ok, notes = False, [f"FAIL {type(exc).__name__}: {exc}"]
        for n in notes:
            print("  " + n)
        failed += 0 if ok else 1
    print(f"\n{len(rows) - failed}/{len(rows)} index futures passed the gates")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
