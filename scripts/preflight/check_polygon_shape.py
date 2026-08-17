"""
scripts/preflight/check_polygon_shape.py

ADD (Ovi, this thread — 14 Aug 2026): "start with this sequent" preflight
pass. Same authoring/execution split as every other script in this
directory — authored now, executed later on network-enabled hardware
(this sandbox has no route to api.polygon.io).

Confirms, empirically, that Polygon's aggs endpoint still returns the
field shape polygon_ohlcv.yaml / polygon_adapter.py depend on: o/h/l/c/v
(+vw) keys present, non-empty results for a live US ticker. Deliberately
checks ONLY the 'day' timespan by default -- FIX POL-1 already documents
that 'hour' is not available on Polygon's free tier and is blocked
upfront in polygon_adapter.py itself, so a preflight check against 1H
would just re-confirm a known, already-handled limitation rather than
catch anything new. --timespan lets you check minute/week/month too if
you want, but day is what actually matters for Polygon's role in this
pipeline (US stocks fallback when yfinance fails, GD §11.1).

RATE LIMIT WARNING: Polygon free tier is 5 req/min (SourceLimiters.polygon
in src/utils/rate_limiter.py). This script defaults to checking exactly
ONE ticker (AAPL) for exactly this reason -- looping over even a handful
of symbols risks tripping the same 429 path FIX POL-3 exists to handle.
Use --symbol to check a different one; there is deliberately no "check
all Layer 1 US stocks" mode here, unlike check_yfinance_tickers.py's
default full-registry sweep (yfinance's ~2000/hr limit makes that safe;
Polygon's 5/min does not).

Usage:
    python scripts/preflight/check_polygon_shape.py
    python scripts/preflight/check_polygon_shape.py --symbol MSFT
    python scripts/preflight/check_polygon_shape.py --timespan week

Exit code 0 = the ticker returns a non-empty, correctly-shaped result.
Exit code 1 = POLYGON_API_KEY missing, or the request failed/malformed.
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

POLYGON_BASE = "https://api.polygon.io/v2/aggs/ticker"

# Same free-tier-availability set as polygon_adapter.py's _FREE_TIER_TIMESPANS
_TIMESPAN_LOOKBACK_DAYS: dict[str, int] = {
    "day": 10, "week": 60, "month": 365, "minute": 1,
}
_DEFAULT_TICKER = "AAPL"


def _check_one(symbol: str, timespan: str, api_key: str) -> tuple[bool, str]:
    import requests

    lookback = _TIMESPAN_LOOKBACK_DAYS.get(timespan, 10)
    end = date.today()
    start = end - timedelta(days=lookback)
    mult = 1

    url = f"{POLYGON_BASE}/{symbol}/range/{mult}/{timespan}/{start.isoformat()}/{end.isoformat()}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50, "apiKey": api_key}

    try:
        resp = requests.get(url, params=params, timeout=30)
    except Exception as e:
        return False, f"request raised: {e}"

    if resp.status_code == 429:
        return False, "HTTP 429 rate limited -- 5 req/min free tier, wait and retry"
    if resp.status_code != 200:
        return False, f"HTTP {resp.status_code} -- {resp.text[:200]}"

    try:
        data = resp.json()
    except Exception as e:
        return False, f"response not valid JSON: {e}"

    if data.get("status") not in ("OK", "DELAYED"):
        return False, f"Polygon status={data.get('status')!r} -- {data.get('error') or data}"

    results = data.get("results", [])
    if not results:
        return False, f"0 results (resultsCount={data.get('resultsCount')}) -- ticker may be delisted or timespan too short"

    # polygon_ohlcv.yaml + POL-2/POL-5 fixes: o/h/l/c must be present,
    # v (volume) nullable but should normally be present too for a real bar.
    required = {"o", "h", "l", "c"}
    latest = results[-1]
    missing = required - set(latest.keys())
    if missing:
        return False, f"latest bar missing required keys {missing} (got: {sorted(latest.keys())})"

    return True, (
        f"OK -- {len(results)} bars, latest t={latest.get('t')} "
        f"o={latest.get('o')} c={latest.get('c')} v={latest.get('v')}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=_DEFAULT_TICKER, help=f"Ticker to check (default: {_DEFAULT_TICKER})")
    parser.add_argument(
        "--timespan", default="day", choices=sorted(_TIMESPAN_LOOKBACK_DAYS),
        help="Free-tier timespan to check (default: day -- 'hour' is not on the free tier, see module docstring)",
    )
    args = parser.parse_args()

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        print("FAIL: POLYGON_API_KEY not set in .env -- cannot check live Polygon.")
        return 1

    ok, msg = _check_one(args.symbol, args.timespan, api_key)
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {args.symbol:8s} (timespan={args.timespan})  {msg}")

    print()
    if not ok:
        print("1/1 check FAILED.")
        return 1

    print("Polygon aggs endpoint confirmed live, correctly-shaped response.")
    print("(Checked 1 symbol only -- 5 req/min free tier. See module docstring.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
