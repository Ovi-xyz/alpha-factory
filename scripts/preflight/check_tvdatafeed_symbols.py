"""
scripts/preflight/check_tvdatafeed_symbols.py

ADD — closes Architecture v2.1 Addendum (Commodity Universe Extension) §9,
Open Technical Decision OD-C1: "tvfeed_symbol verification for CPO, RUBBER,
TIN, COAL_NEWC ... Must be verified before production" — explicitly listed
as OPEN — BLOCKING in that document's §11, and unaddressed by any of the
three existing scripts in this directory (confirmed empirically this
thread: zero references to "tvdatafeed"/"tvfeed" anywhere under scripts/).

Same authoring/execution split as check_yfinance_tickers.py, check_bis_
cbpol_d.py, and check_finnhub_shape.py (ADR-025): this sandbox has no
network egress to tvdatafeed's backing TradingView endpoints either
(network allowlist covers only pypi.org/github.com/npmjs.com-class package
registries — the same constraint every prior GMI checkpoint has hit).
Authoring does not require network access; running it does. Run this on
the target M1 hardware using the TV_USERNAME/TV_PASSWORD already in .env.

Why the other three scripts don't cover this gap: TvDatafeedAdapter.fetch()
(src/bronze/tvdatafeed_adapter.py) hardcodes exchange="IDX" for every call
-- correct for the 30 IDX30 stocks, but there is no code path (and, as of
this thread, no config field either) to route a non-IDX exchange for the
4 commodity_context instruments the Addendum lists as tvdatafeed-primary.
Confirmed empirically: "tvfeed_exchange" has zero occurrences anywhere in
config/ or src/, and CPO/RUBBER/TIN's own `deferred_reason` fields in
config/instruments_taxonomy.yaml name this exact gap ("tvdatafeed ...
ticker/exchange verification pending (OD-C1)"). This script therefore
talks to the tvdatafeed client directly via TvDatafeedSessionManager,
bypassing the (currently IDX-only) adapter, using a routing table
transcribed from the Addendum's own §9.2 Exchange Routing Table.

The table below is duplicated here deliberately, not imported from
instruments_identity.yaml/instruments_taxonomy.yaml -- because those files
do not carry this information yet (see above), a script that imported it
would have nothing to import and would silently no-op. This mirrors
check_bis_cbpol_d.py's EXPECTED_REF_AREAS precedent: an independent check
should not depend on the very config gap it exists to help close.

COAL_NEWC is included for completeness even though its CURRENTLY LIVE
instruments_identity.yaml entry routes via yfinance (WHC.AX proxy, ADR-006)
rather than tvdatafeed -- the Addendum's own routing table lists
ICE/GLOBALCOAL_NEWC as a documented tvdatafeed-eligible alternative. A
PASS or FAIL on COAL_NEWC here is informational only (confirms whether the
alternative is viable) and never counts toward this script's exit code --
only CPO/RUBBER/TIN (the actual OD-C1 blocker) do.

Usage:
    python scripts/preflight/check_tvdatafeed_symbols.py
    python scripts/preflight/check_tvdatafeed_symbols.py --symbol TIN

Exit code 0 = every OD-C1 (tvfeed_symbol, exchange) pair returned a
non-empty, correctly-shaped result. Exit code 1 = at least one failed, the
tvDatafeed package isn't installed, or TV_USERNAME/TV_PASSWORD aren't set
(so a session can't even be attempted).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

REQUIRED_COLUMNS = {"open", "high", "low", "close", "volume"}

# Architecture v2.1 Addendum §9.2 "Exchange Routing Table" -- transcribed
# deliberately, not imported (see module docstring). OD-C1: OPEN -- BLOCKING
# as of this authoring for CPO/RUBBER/TIN. COAL_NEWC is informational only
# (see module docstring) -- flagged via INFORMATIONAL_ONLY below, not a
# blocker if it fails.
ROUTING_TABLE: dict[str, tuple[str, str]] = {
    # symbol -> (tvfeed_symbol, tvfeed_exchange)
    "CPO":       ("FCPO1!", "BMDI"),
    "RUBBER":    ("SICOM_TSR20", "SGX"),
    "TIN":       ("SN", "LME"),
    "COAL_NEWC": ("GLOBALCOAL_NEWC", "ICE"),
}
INFORMATIONAL_ONLY = frozenset({"COAL_NEWC"})


def _check_one(tvfeed_symbol: str, exchange: str, client) -> tuple[bool, str]:
    """Return (ok, message) for one (tvfeed_symbol, exchange) pair."""
    try:
        from src.bronze.tvdatafeed_session import get_tv_interval
        interval = get_tv_interval("1D")
        df = client.get_hist(
            symbol=tvfeed_symbol,
            exchange=exchange,
            interval=interval,
            n_bars=5,
        )
    except Exception as e:
        return False, f"get_hist() raised: {e}"

    if df is None or len(df) == 0:
        return False, (
            "empty result -- symbol/exchange pair may not exist on "
            "tvdatafeed, or the session died mid-call"
        )

    cols = {str(c).lower() for c in df.columns}
    missing = REQUIRED_COLUMNS - cols
    if missing:
        return False, f"missing expected OHLCV columns: {missing} (got: {sorted(cols)})"

    return True, f"OK -- {len(df)} rows, columns {sorted(cols)}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=None, help="Only check one symbol (e.g. TIN)")
    args = parser.parse_args()

    try:
        from src.bronze.tvdatafeed_session import TV_AVAILABLE, TvDatafeedSessionManager
    except ImportError as e:
        print(f"FAIL: could not import TvDatafeedSessionManager: {e}")
        return 1

    if not TV_AVAILABLE:
        print("FAIL: tvDatafeed package not installed in this environment.")
        return 1

    if not os.getenv("TV_USERNAME") or not os.getenv("TV_PASSWORD"):
        print("FAIL: TV_USERNAME / TV_PASSWORD not set -- cannot attempt a session.")
        return 1

    session = TvDatafeedSessionManager()
    client = session.get_client()
    if client is None:
        print("FAIL: tvdatafeed session could not be established (see log above).")
        return 1

    targets = list(ROUTING_TABLE.items())
    if args.symbol:
        targets = [(s, v) for s, v in targets if s == args.symbol]
        if not targets:
            print(f"No routing entry for symbol {args.symbol!r}. Known: {list(ROUTING_TABLE)}")
            return 1

    blocking_failures = []
    for symbol, (tvfeed_symbol, exchange) in sorted(targets):
        informational = symbol in INFORMATIONAL_ONLY
        flag = " [informational -- not live source]" if informational else " [OD-C1: blocking]"
        ok, msg = _check_one(tvfeed_symbol, exchange, client)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {symbol:10s} ({exchange}:{tvfeed_symbol}){flag}  {msg}")
        if not ok and not informational:
            blocking_failures.append(symbol)

    print()
    if blocking_failures:
        print(f"{len(blocking_failures)} OD-C1 symbol(s) FAILED: {blocking_failures}")
        return 1

    print("All OD-C1 blocking symbols PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
