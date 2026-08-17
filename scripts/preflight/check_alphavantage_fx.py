"""
scripts/preflight/check_alphavantage_fx.py

ADD (Ovi, this thread — 14 Aug 2026): "start with this sequent" preflight
pass. Same authoring/execution split as every other script in this
directory — authored now, executed later on network-enabled hardware
(this sandbox has no route to www.alphavantage.co).

BUDGET WARNING -- read before running: AlphaVantage free tier is 25
req/DAY total (DailyBudgetLimiter, SourceLimiters.alphavantage), the
single scarcest resource of any of the 11 sources in this pipeline. Every
other preflight script in this directory defaults to a full or
near-full check; this one deliberately does NOT, and is split into two
tiers:

  Tier 1 (default, zero network calls, zero budget cost):
    Imports AlphaVantageForexAdapter and calls _parse_pair("DXY") directly
    -- FIX AV-2 states AV has no native DXY endpoint and a USD/EUR proxy
    would only capture 57.6% of the DXY basket, so _parse_pair() is
    supposed to return ("", "") as a sentinel telling the caller to skip
    AV entirely for DXY. This is fully checkable by calling the function
    with real inputs -- no HTTP request needed -- so it costs nothing to
    verify, unlike every other check in this directory.

  Tier 2 (--live-fetch, opt-in only, costs exactly 1 of the 25 daily
  requests): Fetches FX_DAILY for exactly ONE real pair (default EUR/USD,
  AlphaVantageForexAdapter's actual primary supplemental target per GD
  §11.1) and confirms the response shape matches what the adapter parses
  (a "Time Series FX (Daily)"-style key, OHLC fields present). Never loops
  over multiple pairs -- there is no "--all" mode here on purpose.

Usage:
    python scripts/preflight/check_alphavantage_fx.py
    python scripts/preflight/check_alphavantage_fx.py --live-fetch
    python scripts/preflight/check_alphavantage_fx.py --live-fetch --pair GBP/USD

Exit code 0 = Tier 1 passes (and Tier 2 passes too, if requested).
Exit code 1 = Tier 1 fails, or (with --live-fetch) the live fetch fails /
ALPHAVANTAGE_API_KEY is missing.
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

AV_BASE = "https://www.alphavantage.co/query"
_DEFAULT_PAIR = "EUR/USD"


def _check_dxy_skip_static() -> tuple[bool, str]:
    """Tier 1 -- zero network cost. Confirms FIX AV-2's sentinel behavior
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


def _check_live_fetch(pair: str, api_key: str) -> tuple[bool, str]:
    import requests
    clean = pair.replace("/", "").upper()
    if len(clean) != 6:
        return False, f"cannot parse pair {pair!r} -- expected e.g. 'EUR/USD'"
    from_sym, to_sym = clean[:3], clean[3:]

    try:
        resp = requests.get(
            AV_BASE,
            params={
                "function": "FX_DAILY",
                "from_symbol": from_sym,
                "to_symbol": to_sym,
                "apikey": api_key,
                "outputsize": "compact",
                "datatype": "json",
            },
            timeout=30,
        )
    except Exception as e:
        return False, f"request raised: {e}"

    if resp.status_code != 200:
        return False, f"HTTP {resp.status_code} -- budget NOT consumed per AV-1 semantics"

    try:
        data = resp.json()
    except Exception as e:
        return False, f"response not valid JSON: {e}"

    if "Information" in data or "Note" in data:
        return False, f"AV API message (likely budget/rate limit): {data.get('Information') or data.get('Note')}"

    ts_key = next((k for k in data if "Time Series" in k or "FX" in k), None)
    if not ts_key:
        return False, f"no Time Series / FX key found in response keys: {list(data.keys())}"

    series = data[ts_key]
    if not series:
        return False, f"key {ts_key!r} present but empty"

    latest_date = max(series.keys())
    latest_vals = series[latest_date]
    required = {"1. open", "2. high", "3. low", "4. close"}
    missing = required - set(latest_vals.keys())
    if missing:
        return False, f"latest bar missing required keys {missing} (got: {sorted(latest_vals.keys())})"

    return True, f"OK -- {len(series)} bars, latest={latest_date} close={latest_vals['4. close']}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-fetch", action="store_true",
        help="Also run Tier 2: a real FX_DAILY fetch that costs 1 of the "
             "25 daily AlphaVantage requests. Off by default -- see module docstring.",
    )
    parser.add_argument(
        "--pair", default=_DEFAULT_PAIR,
        help=f"Pair to check with --live-fetch (default: {_DEFAULT_PAIR})",
    )
    args = parser.parse_args()

    ok1, msg1 = _check_dxy_skip_static()
    status1 = "PASS" if ok1 else "FAIL"
    print(f"[{status1}] Tier 1 (static, 0 budget)  {msg1}")

    if not args.live_fetch:
        print()
        print("Tier 2 (live fetch) skipped -- pass --live-fetch to also spend "
              "1 of the 25 daily AlphaVantage requests confirming FX_DAILY shape.")
        return 0 if ok1 else 1

    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        print()
        print("FAIL: --live-fetch requested but ALPHAVANTAGE_API_KEY not set in .env.")
        return 1

    ok2, msg2 = _check_live_fetch(args.pair, api_key)
    status2 = "PASS" if ok2 else "FAIL"
    print(f"[{status2}] Tier 2 (live, 1/25 daily budget spent)  {args.pair}  {msg2}")

    print()
    if not (ok1 and ok2):
        print("At least one tier FAILED.")
        return 1

    print("AlphaVantage FX checks passed (DXY-skip sentinel + 1 live pair fetch).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
