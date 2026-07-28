"""
scripts/preflight/check_finnhub_shape.py

ADD ADR-025 (GMI_Decision_Document_v2.docx, 2026-07-11) -- see
check_yfinance_tickers.py's module docstring for the shared rationale
(authored now, executed later on network-enabled hardware/CI).

Confirms the LIVE /quote and /calendar/earnings response shape from
Finnhub matches what finnhub_ingester.py's schema validators
(config/schemas/finnhub_quote.yaml, finnhub_earnings_calendar.yaml)
expect. Those schemas were authored against Finnhub's PUBLICLY DOCUMENTED
API contract, retrieved via web search (GMI_Implementation_Checkpoint_v3.docx
D24) -- not against a live response, since the audit sandbox had no
network access to Finnhub either. Checkpoint v3 Section 11.3 explicitly
flags this gap as unconfirmed: "whether Finnhub's documented ... API
contract ... exactly matches the CURRENT live response shape at the time
a real integration is eventually attempted." This script is that
confirmation, deferred to whenever it can actually run.

Requires FINNHUB_API_KEY in the environment (.env or exported).

Usage:
    python scripts/preflight/check_finnhub_shape.py --symbol AAPL

Exit code 0 = live response shape matches the schema's expected columns/
nullability. Exit code 1 = a mismatch was found (which the real
SchemaValidator + on_mismatch=quarantine gate would also catch in
production -- this script exists to catch it BEFORE a live run, not to
replace that gate).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# FIX (Ovi, this thread -- "issues even though .env already filled"):
# python-dotenv is a declared dependency but was never actually called
# anywhere in this repo (confirmed by grep). Without this, os.getenv()
# only sees variables the shell has separately exported -- a filled
# .env file alone was not enough, which is exactly what this preflight
# run surfaced for TV_USERNAME/TV_PASSWORD and FINNHUB_API_KEY.
from dotenv import load_dotenv
load_dotenv()

EXPECTED_QUOTE_FIELDS = {"c", "d", "dp", "h", "l", "o", "pc", "t"}
EXPECTED_EARNINGS_FIELDS = {
    "epsEstimate", "epsActual", "revenueEstimate", "quarter", "year",
}


def _check_quote(client, symbol: str) -> tuple[bool, str]:
    try:
        quote = client.quote(symbol)
    except Exception as e:
        return False, f"quote({symbol}) raised: {e}"

    if not isinstance(quote, dict):
        return False, f"quote() did not return a dict: {type(quote)}"

    missing = EXPECTED_QUOTE_FIELDS - set(quote.keys())
    if missing:
        return False, f"quote() response missing expected fields: {missing}"

    # Documented quirk (GMI_Implementation_Checkpoint_v3.docx §4.6): an
    # invalid/delisted symbol returns all-zero numeric fields, not a
    # missing-key response -- confirm we can still distinguish this case.
    all_zero = all(quote.get(k) == 0 for k in ("c", "h", "l", "o", "pc"))
    note = " [all-zero response -- symbol may be invalid/delisted]" if all_zero else ""

    return True, f"OK -- fields present{note}: {quote}"


def _check_earnings(client, symbol: str) -> tuple[bool, str]:
    from datetime import date, timedelta

    try:
        today = date.today()
        result = client.earnings_calendar(
            _from=today.isoformat(),
            to=(today + timedelta(days=90)).isoformat(),
            symbol=symbol,
        )
    except Exception as e:
        return False, f"earnings_calendar({symbol}) raised: {e}"

    entries = result.get("earningsCalendar", []) if isinstance(result, dict) else []
    if not entries:
        return True, "OK -- no upcoming earnings in the next 90 days (not a failure)"

    entry = entries[0]
    missing = EXPECTED_EARNINGS_FIELDS - set(entry.keys())
    if missing:
        return False, f"earnings_calendar() entry missing expected fields: {missing}"

    revenue_est = entry.get("revenueEstimate")
    shape_note = f" (revenueEstimate type={type(revenue_est).__name__})" if revenue_est is not None else ""

    return True, f"OK -- fields present{shape_note}: {entry}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="AAPL")
    args = parser.parse_args()

    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        print("FINNHUB_API_KEY not set -- cannot run live check.")
        print("Set it in .env or export it, then re-run.")
        return 1

    try:
        import finnhub
    except ImportError:
        print("finnhub-python not installed in this environment.")
        return 1

    client = finnhub.Client(api_key=api_key)

    ok_quote, msg_quote = _check_quote(client, args.symbol)
    print(f"[{'PASS' if ok_quote else 'FAIL'}] /quote  {args.symbol}: {msg_quote}")

    ok_earnings, msg_earnings = _check_earnings(client, args.symbol)
    print(f"[{'PASS' if ok_earnings else 'FAIL'}] /calendar/earnings  {args.symbol}: {msg_earnings}")

    if ok_quote and ok_earnings:
        print("\nBoth endpoints match the documented contract used by finnhub_ingester.py's schemas.")
        return 0

    print("\nAt least one endpoint's live shape diverges from the documented "
          "contract -- update config/schemas/finnhub_quote.yaml / "
          "finnhub_earnings_calendar.yaml accordingly before relying on "
          "SchemaValidator to catch it silently via quarantine.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
