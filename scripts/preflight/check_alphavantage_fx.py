"""
scripts/preflight/check_alphavantage_fx.py

ADD (Ovi, this thread — 14 Aug 2026): "start with this sequent" preflight
pass. Same authoring/execution split as every other script in this
directory — authored now, executed later on network-enabled hardware
(this sandbox has no route to www.alphavantage.co).

UPD ADR-048 (USD/CNH Source Adjustment Addendum v1.0, 29 Aug 2026): CNH
went from "AlphaVantage not involved at all" to "AlphaVantage is CNH's
sole, permanent, no-fallback source" (market_ingester.py's data_source
override). This script is CNH's specific pre-flight gate before that
routing is trusted live — Ovi's own framing for this fix: "preflight
modules always run first before doing live test." Two changes follow
directly from that:

  1. Tier 1 gained a second zero-cost static check (CNH pair-parse,
     alongside the pre-existing DXY skip-sentinel check).
  2. Tier 2's live-fetch was quietly hand-rolling its own copy of
     AlphaVantageForexAdapter's request-building logic instead of calling
     the adapter directly — a genuine bug in this script, not just a
     style nit: the moment ADR-048 added outputsize compact/full sizing
     to the real adapter, this script's duplicate logic silently fell out
     of sync with it (it still always requested 'full', unconditionally).
     A preflight check that exercises a hand-copied stand-in instead of
     the real code path defeats the entire point of "preflight validates
     what is about to go live." Fixed by routing Tier 2 through
     AlphaVantageForexAdapter.fetch() directly — same class,
     same method, same code path market_ingester.py actually calls in
     production. Tier 3 (new) is built the same way.

BUDGET WARNING -- read before running: AlphaVantage free tier is 25
req/DAY total (DailyBudgetLimiter, SourceLimiters.alphavantage), the
single scarcest resource of any of the 11 sources in this pipeline, and
CNH is now a PERMANENT daily consumer of it (3 calls/day: 1D/1W/1M via
market_ingester.py's run_context(), not just an occasional DXY-fallback
draw — see KNOWN_RISKS.md RISK-22). Every other preflight script in this
directory defaults to a full or near-full check; this one deliberately
does NOT, and is split into three tiers:

  Tier 1 (default, zero network calls, zero budget cost):
    (a) DXY skip-sentinel -- FIX AV-2 states AV has no native DXY
        endpoint and a USD/EUR proxy would only capture 57.6% of the DXY
        basket, so _parse_pair() is supposed to return ("", "") as a
        sentinel telling the caller to skip AV entirely for DXY.
    (b) CNH pair-parse -- ADR-048: market_ingester.py's
        _run_context_symbol() builds CNH's api_symbol as
        f"{from_symbol}_{to_symbol}" = 'USD_CNH'. Confirms _parse_pair()
        resolves this to a REAL (from, to) tuple ('USD', 'CNH') -- the
        opposite outcome from the DXY case above, and the exact string
        shape CNH's sole data source depends on. Both (a) and (b) are
        fully checkable by calling _parse_pair() with real inputs -- no
        HTTP request needed -- so they cost nothing to verify.

  Tier 2 (--live-fetch, opt-in, costs exactly 1 of the 25 daily
  requests): fetches FX_DAILY for exactly ONE real pair (default EUR/USD,
  AlphaVantageForexAdapter's actual primary supplemental target per GD
  §11.1) via the real adapter, over a short (30-day) window -- stays
  inside the adapter's own outputsize='compact' branch (FIX ADR-048's
  <=100-day threshold) on purpose, since this tier checks response SHAPE,
  not historical depth. Never loops over multiple pairs -- there is no
  "--all" mode here on purpose.

  Tier 3 (--check-cnh, opt-in, costs exactly 1 of the 25 daily requests):
  dedicated USD/CNH depth check via the real adapter over a >100-day
  window -- deliberately crosses the adapter's outputsize='full'
  threshold (FIX ADR-048), reproducing the USD/CNH Source Adjustment
  Addendum v1.0 §3.2 empirical finding (3,079 clean daily rows,
  2014-11-07 -> 2026-08-27, ~11.8 years) as a permanent, re-runnable gate
  rather than a one-time chat-session finding. CNH has NO fallback
  (ADR-048's own decision text: "yfinance tidak digunakan untuk instrumen
  ini dalam kapasitas apapun") -- if AlphaVantage's CNH coverage ever
  silently degrades the way yfinance's USDCNH=X did (the entire reason
  this ADR exists), this is the check that would catch it before it
  reaches production, not after.

Usage:
    python scripts/preflight/check_alphavantage_fx.py
    python scripts/preflight/check_alphavantage_fx.py --live-fetch
    python scripts/preflight/check_alphavantage_fx.py --live-fetch --pair GBP/USD
    python scripts/preflight/check_alphavantage_fx.py --check-cnh
    python scripts/preflight/check_alphavantage_fx.py --live-fetch --check-cnh

Exit code 0 = every requested tier passes (Tier 1 always runs).
Exit code 1 = any requested tier fails, or a requested live tier's
ALPHAVANTAGE_API_KEY is missing.

Out of src/, by design (this whole scripts/preflight/ directory) -- no
unit test coverage requirement applies here (Ovi, 30 Aug 2026); this
script's only "test" is being run for real, on real network-enabled
hardware, against the real AlphaVantage API, which is the entire point of
a preflight gate.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
load_dotenv()

_DEFAULT_PAIR = "EUR/USD"
_TIER3_MIN_EXPECTED_ROWS = 1000  # addendum's own empirical baseline: 3,079 rows (~11.8y)


def _check_dxy_skip_static() -> tuple[bool, str]:
    """Tier 1a -- zero network cost. Confirms FIX AV-2's sentinel behavior
    by calling the real adapter method, not by re-deriving the logic."""
    try:
        from src.bronze.alphavantage_adapter import AlphaVantageForexAdapter
    except Exception as e:
        return False, f"could not import AlphaVantageForexAdapter: {e}"

    from_sym, to_sym = AlphaVantageForexAdapter._parse_pair("DXY")
    if (from_sym, to_sym) != ("", ""):
        return False, (
            f"_parse_pair('DXY') returned ({from_sym!r}, {to_sym!r}), expected "
            f"('', '') -- FIX AV-2's skip-sentinel appears to have regressed. "
            f"A non-empty pair here means AV would attempt a materially "
            f"inaccurate USD/EUR proxy for DXY (57.6% basket coverage only)."
        )
    return True, "OK -- _parse_pair('DXY') correctly returns ('', '') skip-sentinel (FIX AV-2)"


def _check_cnh_pair_parse_static() -> tuple[bool, str]:
    """Tier 1b -- zero network cost. ADR-048 (29 Aug 2026):
    market_ingester.py's _run_context_symbol() builds CNH's api_symbol as
    f"{from_symbol}_{to_symbol}" = 'USD_CNH' (not derived from inst.symbol
    alone -- 'CNH' is a bare 3-char currency code, not a parseable pair).
    Confirms _parse_pair() resolves that exact string to a REAL (from, to)
    tuple -- the opposite of the DXY skip-sentinel case above. A
    regression here would silently break CNH's sole, no-fallback data
    source (see market_ingester.py's data_source override)."""
    try:
        from src.bronze.alphavantage_adapter import AlphaVantageForexAdapter
    except Exception as e:
        return False, f"could not import AlphaVantageForexAdapter: {e}"

    from_sym, to_sym = AlphaVantageForexAdapter._parse_pair("USD_CNH")
    if (from_sym, to_sym) != ("USD", "CNH"):
        return False, (
            f"_parse_pair('USD_CNH') returned ({from_sym!r}, {to_sym!r}), "
            f"expected ('USD', 'CNH') -- this is the exact symbol string "
            f"market_ingester.py's _run_context_symbol() builds for CNH "
            f"under ADR-048. CNH has no fallback source; a regression here "
            f"means CNH's Bronze write silently stops happening."
        )
    return True, "OK -- _parse_pair('USD_CNH') correctly returns ('USD', 'CNH') (ADR-048)"


def _check_live_fetch(pair: str) -> tuple[bool, str]:
    """Tier 2 -- live, 1/25 daily budget. UPD ADR-048: routes through the
    REAL AlphaVantageForexAdapter.fetch() (see module docstring for why
    the previous hand-rolled version of this check was itself a bug).
    30-day window stays inside the adapter's outputsize='compact' branch
    on purpose -- this tier checks response SHAPE, not historical depth
    (that is Tier 3, _check_cnh_live_depth, which needs a >100-day window
    on purpose)."""
    from src.bronze.alphavantage_adapter import AlphaVantageForexAdapter

    adapter = AlphaVantageForexAdapter()
    end = date.today()
    start = end - timedelta(days=30)
    df = adapter.fetch(pair, "1D", start, end)

    if df is None:
        return False, (
            f"AlphaVantageForexAdapter.fetch({pair!r}, '1D', ...) returned "
            f"None -- check ALPHAVANTAGE_API_KEY, daily budget "
            f"(SourceLimiters.alphavantage), pair format (e.g. 'EUR/USD'), "
            f"and AV API status."
        )
    if len(df) == 0:
        return False, "fetch succeeded but returned 0 rows"

    latest = df.sort("timestamp").tail(1).to_dicts()[0]
    return True, f"OK -- {len(df)} bars, latest={latest['timestamp']} close={latest['close']}"


def _check_cnh_live_depth() -> tuple[bool, str]:
    """Tier 3 -- live, 1/25 daily budget. ADR-048 (29 Aug 2026): dedicated
    USD/CNH depth check via the real adapter, reproducing the USD/CNH
    Source Adjustment Addendum v1.0 §3.2 empirical finding (3,079 clean
    daily rows, 2014-11-07 -> 2026-08-27, ~11.8 years) as a permanent,
    re-runnable gate. The >100-day window is deliberate -- it crosses the
    adapter's own outputsize='full' threshold (FIX ADR-048), the same
    branch market_ingester.py's cold-start Bronze backfill would hit for
    this instrument. _TIER3_MIN_EXPECTED_ROWS is a conservative floor
    (1000, addendum's own baseline was 3,079) -- generous headroom against
    normal variation, while still catching a "returns almost nothing"
    regression of the exact shape yfinance's USDCNH=X had (1 row) -- the
    entire reason ADR-048 exists. CNH has NO fallback; this is the check
    that would catch a silent AV-side degradation before production does."""
    from src.bronze.alphavantage_adapter import AlphaVantageForexAdapter

    adapter = AlphaVantageForexAdapter()
    end = date.today()
    start = end - timedelta(days=365 * 13)  # >100 days -> forces outputsize='full'
    df = adapter.fetch("USD_CNH", "1D", start, end)

    if df is None:
        return False, (
            "AlphaVantageForexAdapter.fetch('USD_CNH', '1D', ...) returned "
            "None -- check ALPHAVANTAGE_API_KEY and daily budget "
            "(SourceLimiters.alphavantage). CNH has NO fallback (ADR-048) "
            "-- a failure here means CNH's Bronze write silently does not "
            "happen today, for any timeframe."
        )
    n = len(df)
    if n == 0:
        return False, "fetch succeeded but returned 0 rows"

    if n < _TIER3_MIN_EXPECTED_ROWS:
        return False, (
            f"only {n} rows returned -- addendum's own empirical baseline "
            f"(29 Aug 2026) was 3,079 rows spanning ~11.8 years. A count "
            f"this far below baseline suggests AlphaVantage's USD/CNH "
            f"coverage has changed or degraded since -- investigate before "
            f"trusting CNH's live Bronze data, do not assume the "
            f"addendum's numbers still hold."
        )

    sorted_df = df.sort("timestamp")
    first_ts = sorted_df.head(1).to_dicts()[0]["timestamp"]
    last_ts = sorted_df.tail(1).to_dicts()[0]["timestamp"]
    return True, f"OK -- {n} bars, range {first_ts}..{last_ts}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-fetch", action="store_true",
        help="Run Tier 2: a real FX_DAILY fetch for --pair (default "
             f"{_DEFAULT_PAIR}) via the real adapter, 30-day window "
             "(outputsize='compact'). Costs 1 of the 25 daily "
             "AlphaVantage requests. Off by default -- see module docstring.",
    )
    parser.add_argument(
        "--pair", default=_DEFAULT_PAIR,
        help=f"Pair to check with --live-fetch (default: {_DEFAULT_PAIR})",
    )
    parser.add_argument(
        "--check-cnh", action="store_true",
        help="Run Tier 3: a dedicated USD/CNH depth check (ADR-048) via a "
             ">100-day window forcing outputsize='full'. Costs 1 of the 25 "
             "daily AlphaVantage requests. Reproduces the USD/CNH Source "
             "Adjustment Addendum v1.0 §3.2 empirical check (~3,079 rows, "
             "~11.8 years) as a re-runnable gate -- CNH has no fallback if "
             "this ever silently degrades. Off by default.",
    )
    args = parser.parse_args()

    all_ok = True

    ok1a, msg1a = _check_dxy_skip_static()
    print(f"[{'PASS' if ok1a else 'FAIL'}] Tier 1 (static, 0 budget) -- DXY skip     {msg1a}")
    all_ok = all_ok and ok1a

    ok1b, msg1b = _check_cnh_pair_parse_static()
    print(f"[{'PASS' if ok1b else 'FAIL'}] Tier 1 (static, 0 budget) -- CNH parse    {msg1b}")
    all_ok = all_ok and ok1b

    if not args.live_fetch and not args.check_cnh:
        print()
        print("Tier 2 (--live-fetch) and Tier 3 (--check-cnh) skipped -- each "
              "costs 1 of the 25 daily AlphaVantage requests. See module docstring.")
        return 0 if all_ok else 1

    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        print()
        print("FAIL: --live-fetch/--check-cnh requested but ALPHAVANTAGE_API_KEY not set in .env.")
        return 1

    if args.live_fetch:
        ok2, msg2 = _check_live_fetch(args.pair)
        print(f"[{'PASS' if ok2 else 'FAIL'}] Tier 2 (live pair, 1/25 daily budget)  {args.pair}  {msg2}")
        all_ok = all_ok and ok2

    if args.check_cnh:
        ok3, msg3 = _check_cnh_live_depth()
        print(f"[{'PASS' if ok3 else 'FAIL'}] Tier 3 (CNH depth, 1/25 daily budget)  USD/CNH  {msg3}")
        all_ok = all_ok and ok3

    print()
    if not all_ok:
        print("At least one check FAILED.")
        return 1

    print("AlphaVantage checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
