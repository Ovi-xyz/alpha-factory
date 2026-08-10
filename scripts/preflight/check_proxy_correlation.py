"""
scripts/preflight/check_proxy_correlation.py

ADD (Ovi, this thread) -- the proxy correlation studies for CPO/RUBBER/
TIN/NICKEL, one of the three items explicitly left open across every
dev-log entry since 2026-08-01 ("Proxy correlation study for F34.SI/
STA.BK/AFM.V/NIC.AX -- still open"), and the specific item RISK-15 (2026-
08-08) was closed first to unblock: "the correlation studies need a
commodity price benchmark to correlate the proxies against, and these 6
[FRED Track 2] series are exactly that benchmark, now available for the
first time." That benchmark was confirmed live 9 Aug 2026 (all 6 PASS,
check_fred_commodity_series.py) -- this script is what actually runs the
correlation, now that both legs (Track 1 proxy, Track 2 benchmark) are
confirmed reachable.

Why this exists: instruments_taxonomy.yaml's own NICKEL/TIN/CPO/RUBBER
entries all carry the identical comment -- "proxy_for/
proxy_correlation_expected deliberately NOT set -- no empirical
[ticker]-vs-[commodity]-price correlation analysis exists yet (unlike
VALE's ~0.81, which came from an actual study)." validate_instruments.py
requires proxy_correlation_expected whenever proxy_for is present, so
these 4 instruments cannot get either field until a real number exists.
This script produces that number; wiring it into instruments_taxonomy.yaml
(alongside a decided proxy_for benchmark identifier, e.g. "IRON_ORE_
SGX_FE62"'s pattern) is a deliberate follow-up decision, NOT done here --
matching every one of those four instruments' own comments about what is
and isn't fabricated.

Methodology, and why it differs from VALE/Iron Ore's ~0.78-0.85 (ADR-005):
none of these 4 commodities has a live DAILY official benchmark reachable
from this platform -- that is precisely why each is proxied at all (the
original raw feeds -- BMDI:FCPO1!, SGX:SICOM_TSR20, LME:SN -- are the
tvdatafeed-routed contracts ADR-029 retired). The only empirical
benchmark available for these 4 is each one's FRED Track 2 MONTHLY World
Bank Pink Sheet series (RISK-15). A same-frequency rolling-60-trading-day
comparison (VALE's apparent method) is not possible against a monthly
series. This script instead: (1) pulls the equity proxy's daily close via
yfinance and resamples to one value per calendar month (last trading day
of the month -- a point-in-time price, dated to the 1st of that month to
align with FRED's own date convention, e.g. "latest=2026-06-01" in the 9
Aug 2026 preflight log); (2) pulls the FRED series' full monthly history
(a month-AVERAGE price, per World Bank Pink Sheets convention); (3)
computes month-over-month percent change independently for each series
(comparing RETURNS, not price levels, to avoid the spurious-correlation
risk of two independently trending level series -- the same reasoning
CorrelationModule already applies platform-wide, Architecture v2.0 §6.2:
"215 instruments, 1D returns"); (4) aligns the two return series on their
common months and reports the Pearson correlation coefficient plus the
number of overlapping months used. Point-in-time proxy price vs.
month-average benchmark price is a real, acknowledged methodological
mismatch -- not hidden, and the best available approximation given no
daily benchmark exists for any of these 4.

compute_proxy_correlation() (and its _to_returns() helper) is the pure,
testable half -- no network, operates on plain {date_str: float} dicts,
same separation-of-concerns pattern as check_bis_eer_weights.py's
extract_us_weights_from_sheet(). _fetch_proxy_monthly_closes() and
_fetch_fred_monthly() are the I/O half.

Same authoring/execution split as every other script in this directory:
this sandbox has no network route to either finance.yahoo.com or
api.stlouisfed.org. Authoring does not require network access; running
it does -- the next step on real hardware, alongside a real
FRED_API_KEY (already in .env per RISK-15's own verification).

A per-symbol [PASS]/[FAIL] here means "the study completed" (both legs
fetched, enough overlapping months to compute a correlation) -- NOT "the
proxy is good enough." A low correlation is a valid, useful research
result, not a failure of this script; it is printed either way; only
missing/unreachable data or too few overlapping months causes a FAIL.

Usage:
    python scripts/preflight/check_proxy_correlation.py
    python scripts/preflight/check_proxy_correlation.py --symbol NICKEL
    python scripts/preflight/check_proxy_correlation.py --period 20y

Exit code 0 = every study computed a correlation (regardless of its
value). Exit code 1 = FRED_API_KEY missing, or at least one symbol could
not be fetched/aligned into a usable result.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from statistics import StatisticsError, correlation

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# FIX (same pattern as every other script in this directory -- "issues
# even though .env already filled"): python-dotenv is a declared
# dependency but is never invoked automatically -- os.getenv() only sees
# variables the shell has separately exported without this.
from dotenv import load_dotenv
load_dotenv()

FRED_OBSERVATIONS_ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"

# Track 1 (daily equity proxy, yfinance) -- duplicated from
# check_yfinance_tickers.py's CANDIDATE_PROXY_TICKERS deliberately, not
# imported, matching every preflight script's own independence rationale
# in this directory (check_bis_cbpol_d.py's EXPECTED_REF_AREAS,
# check_fred_commodity_series.py's EXPECTED_COMMODITY_SERIES): a genuinely
# separate check should not silently inherit a bug from the module it
# would otherwise import. Now live-confirmed (30 Jul 2026 verify-preflight
# log, GMI_Decision_Document_v7.docx §1.2) and adopted into
# instruments_taxonomy.yaml (ADR-030-033).
PROXY_TICKERS: dict[str, str] = {
    "CPO":    "F34.SI",   # Wilmar International (SGX)
    "RUBBER": "STA.BK",   # Sri Trang Agro-Industry (SET)
    "TIN":    "AFM.V",    # Alphamin Resources (TSXV)
    "NICKEL": "NIC.AX",   # Nickel Industries (ASX)
}

# Track 2 (monthly official benchmark, FRED) -- duplicated from
# check_fred_commodity_series.py's EXPECTED_COMMODITY_SERIES, same
# independence rationale. Only the 4 new (ADR-030-033) series are
# relevant here -- IRON_ORE/COAL_NEWC (ADR-005/006) already have an
# established proxy_correlation_expected and are out of this script's
# scope.
BENCHMARK_SERIES: dict[str, str] = {
    "CPO":    "PPOILUSDM",
    "RUBBER": "PRUBBUSDM",
    "TIN":    "PTINUSDM",
    "NICKEL": "PNICKUSDM",
}

# A correlation from a handful of overlapping months is not a meaningful
# proxy-validation result -- this project treats "can't compute a
# trustworthy number" and "computed a possibly-misleading one" as a
# fail-open vs. fail-closed choice, and fails closed (same stance
# SchemaValidator takes: quarantine over silently-wrong data, GD §3.7).
MIN_OVERLAPPING_MONTHS = 12


def _to_returns(monthly_levels: dict[str, float]) -> dict[str, float]:
    """Sort a {month_key: level} series chronologically and compute
    month-over-month percent change against the immediately preceding
    entry IN THIS SAME SERIES. Missing months are skipped, never
    forward-filled or interpolated -- neither yfinance's resampled proxy
    series nor FRED's own history is guaranteed contiguous, and
    fabricating a fill value here would fabricate a return that never
    happened."""
    months = sorted(monthly_levels)
    returns: dict[str, float] = {}
    for prev, cur in zip(months, months[1:]):
        prev_val = monthly_levels[prev]
        cur_val = monthly_levels[cur]
        if prev_val:
            returns[cur] = (cur_val - prev_val) / prev_val
    return returns


def compute_proxy_correlation(
    proxy_monthly_levels: dict[str, float],
    benchmark_monthly_levels: dict[str, float],
    min_periods: int = MIN_OVERLAPPING_MONTHS,
) -> tuple[float | None, int]:
    """Pure correlation logic -- no network, no I/O -- so this is
    unit-testable against plain synthetic dicts without a live fetch.

    Both inputs are {"YYYY-MM-01": price_level} dicts. Returns are
    computed independently per series (a return in month M needs month
    M-1 of the SAME series -- computing returns AFTER intersecting the
    two level-series first would silently use the wrong prior-month
    baseline whenever the two series have different gaps), then the two
    return series are aligned on their common months and correlated.

    Returns (None, n) if fewer than `min_periods` overlapping return
    pairs exist, or if either return series has zero variance over the
    overlap (statistics.correlation raises StatisticsError in that case
    -- a constant series has an undefined correlation, not a zero one).
    """
    proxy_returns = _to_returns(proxy_monthly_levels)
    benchmark_returns = _to_returns(benchmark_monthly_levels)

    common_months = sorted(set(proxy_returns) & set(benchmark_returns))
    n = len(common_months)
    if n < min_periods:
        return None, n

    xs = [proxy_returns[m] for m in common_months]
    ys = [benchmark_returns[m] for m in common_months]

    try:
        return correlation(xs, ys), n
    except StatisticsError:
        return None, n


def _fetch_proxy_monthly_closes(yf_symbol: str, period: str = "15y") -> dict[str, float]:
    """Fetch daily OHLCV via yfinance, resample to the last trading day's
    close of each calendar month, and key the result to the 1st of that
    month -- aligning with FRED's own monthly date convention (observed
    directly in the 9 Aug 2026 preflight log, e.g. "latest=2026-06-01").
    This is a month-END price standing in for FRED's month-AVERAGE figure
    -- see the module docstring's methodology section for why that
    mismatch is accepted rather than hidden."""
    import yfinance as yf

    df = yf.download(yf_symbol, period=period, interval="1d", progress=False)
    if df is None or df.empty:
        return {}

    close = df["Close"]
    if hasattr(close, "columns"):
        # yfinance sometimes returns MultiIndex columns for a single
        # ticker depending on version/call shape -- same normalization
        # check_yfinance_tickers.py's _check_one() already applies.
        close = close.iloc[:, 0]

    monthly = close.resample("ME").last().dropna()
    return {
        ts.replace(day=1).strftime("%Y-%m-%d"): float(val)
        for ts, val in monthly.items()
    }


def _fetch_fred_monthly(series_id: str, api_key: str, limit: int = 500) -> dict[str, float]:
    """Same endpoint and FRED missing-value convention ('.') as
    check_fred_commodity_series.py's _fetch_observations() /_check_one(),
    but limit raised from 12 (that script's existence/freshness check) to
    500 (~40Y of monthly observations -- a full-history pull for
    correlation, not a liveness check) and sort_order left ascending,
    since this needs a full chronological series to align against
    the resampled proxy series, not just the latest value."""
    import httpx

    resp = httpx.get(
        FRED_OBSERVATIONS_ENDPOINT,
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "asc",
            "limit": limit,
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    payload = resp.json()

    out: dict[str, float] = {}
    for obs in payload.get("observations", []):
        val = obs.get("value")
        if val not in (None, ".", ""):
            out[obs["date"]] = float(val)
    return out


def _study_one(
    symbol: str, yf_symbol: str, fred_series: str, api_key: str, period: str = "15y",
) -> tuple[bool, str]:
    try:
        proxy_levels = _fetch_proxy_monthly_closes(yf_symbol, period=period)
    except Exception as e:
        return False, f"yfinance fetch raised: {e}"
    if not proxy_levels:
        return False, f"yfinance returned no usable monthly closes for {yf_symbol}"

    try:
        benchmark_levels = _fetch_fred_monthly(fred_series, api_key)
    except Exception as e:
        return False, f"FRED fetch raised: {e}"
    if not benchmark_levels:
        return False, f"FRED returned no usable observations for {fred_series}"

    corr, n = compute_proxy_correlation(proxy_levels, benchmark_levels)
    if corr is None:
        return False, (
            f"only {n} overlapping monthly return pairs (need >= {MIN_OVERLAPPING_MONTHS}), "
            f"or one series had zero variance over the overlap -- cannot compute a "
            f"meaningful correlation"
        )

    return True, (
        f"OK -- corr={corr:+.4f} over {n} overlapping monthly returns "
        f"({yf_symbol} monthly close vs {fred_series} monthly avg). "
        f"Reference point: VALE/Iron Ore's own established proxy correlation "
        f"is ~0.78-0.85 (ADR-005, Architecture Extension v1.0)."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=None, help="Only study one symbol (e.g. NICKEL)")
    parser.add_argument(
        "--period", default="15y",
        help="yfinance history window for the proxy leg (default: 15y). "
             "The FRED leg always pulls its full available history regardless.",
    )
    args = parser.parse_args()

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("FAIL: FRED_API_KEY not set in .env -- cannot fetch the benchmark leg.")
        return 1

    targets = list(PROXY_TICKERS.items())
    if args.symbol:
        targets = [(s, t) for s, t in targets if s == args.symbol]
        if not targets:
            print(f"No mapping for symbol {args.symbol!r}. Known: {list(PROXY_TICKERS)}")
            return 1

    failures = []
    for symbol, yf_symbol in sorted(targets):
        ok, msg = _study_one(symbol, yf_symbol, BENCHMARK_SERIES[symbol], api_key, period=args.period)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {symbol:8s} {msg}")
        if not ok:
            failures.append(symbol)

    print()
    if failures:
        print(f"{len(failures)}/{len(targets)} studies FAILED to complete: {failures}")
        return 1

    print(f"All {len(targets)} proxy correlation studies completed.")
    print("Wiring these values into instruments_taxonomy.yaml's proxy_correlation_expected")
    print("(paired with a decided proxy_for benchmark identifier, e.g. IRON_ORE_SGX_FE62's")
    print("pattern) is a follow-up decision -- not done by this script.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
