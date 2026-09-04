"""
scripts/preflight/check_ticker_liveness.py

ADD (Ovi, this thread — 3 Sep 2026): "First, build an automated
ticker-liveness preflight. Second, use that automated ticker-liveness
preflight to verify the 41 to unblock the gate." Same authoring/execution
split as every other script in this directory (see
check_yfinance_tickers.py's module docstring) -- authored now, Tier 1
executed later on network-enabled hardware (this sandbox has no route to
finance.yahoo.com or www.alphavantage.co).

Root cause this closes (KNOWN_RISKS.md RISK-28, v1.17.5): coverage_check
sat at 92.8% (<95%) because a subset of Layer 1 symbols in
instruments_identity.yaml are permanently dead tickers -- delisted via
M&A/bankruptcy, or renamed to a new symbol -- not a transient fetch
failure. Nothing in this pipeline had ever distinguished "symbol failed
to fetch today" from "symbol will NEVER fetch again," so a stale
universe silently drained coverage_check with no diagnostic pointing at
WHY. This script is that diagnostic, designed to be re-run periodically
(quarterly SOP suggested), not just once.

Two tiers, mirroring check_alphavantage_fx.py's budget-conscious design:

  Tier 1 (default): yfinance recency check, same shape as
  check_yfinance_tickers.py's _check_one() -- does inst.yfinance_symbol
  return a non-empty, correctly-shaped OHLCV DataFrame over the last 5
  days? A FAIL here is a *symptom* (matches this thread's original
  coverage_check gap), not a diagnosis -- it does not by itself tell you
  whether the symbol is dead or just transiently rate-limited/delayed.
  That distinction is Tier 2's job.

  Tier 2 (--cross-check-delisting, opt-in, costs 1 of AlphaVantage's 25
  daily requests -- LISTING_STATUS is a single bulk call regardless of
  how many symbols you're checking against it, same "1 call however many
  symbols" shape as this project's other AV bulk-endpoint scripts):
  cross-references every Tier-1 FAIL against AlphaVantage's LISTING_STATUS
  endpoint (real HTTP call, CSV response -- symbol/name/exchange/
  assetType/ipoDate/delistingDate/status columns). A symbol found in the
  DELISTED state is classified DELISTED -- ground truth, not inference.
  A symbol found in NEITHER active nor delisted (this thread's empirical
  finding: this happens for genuine rename events -- e.g. ABC->COR,
  RE->EG, SQ->XYZ -- LISTING_STATUS's delisted records apparently do not
  reliably capture pure ticker-rename retirements the way they capture
  M&A/bankruptcy delistings) is classified UNRESOLVED, not DELISTED --
  this script does NOT guess a replacement ticker or auto-remove
  anything. A symbol found ACTIVE despite a Tier-1 fetch FAIL is
  classified LIKELY_TRANSIENT -- the universe entry itself is fine, the
  fetch pipeline has an unrelated, separate problem worth its own
  investigation (this thread found 6 such cases: ANSS, JNPR, HES, HYZN,
  RDFN, SAVA -- all confirmed ACTIVE in AV's live listing despite
  appearing in that same coverage_check gap).

  IMPORTANT, from this thread's own empirical experience: AlphaVantage's
  delistingDate field has at least one known artifact -- a single date
  (2026-09-01 at authoring time) carried 598 of ~9,457 delisted rows, a
  wildly disproportionate cluster against every other date's usual
  double digits, across an incoherent mix of SPAC warrant classes, ETFs,
  and unrelated small-caps. Treat that as "AV's most recent snapshot
  cutoff," i.e. weak evidence the exact date is real, not zero evidence
  the delisting itself is real -- the delisted/active PARTITION (which
  file the symbol appears in at all) is the reliable signal; a specific
  date within the state=delisted result is not, when it lands on
  whatever this script's most-recent cutoff date turns out to be at
  run time. This script surfaces the raw delistingDate for a human to
  judge, it does not silently trust it.

This script NEVER writes to instruments_identity.yaml, 
instruments_taxonomy.yaml, or any config file. It only ever prints a 
report. Removing or renaming a Layer 1 entry is a data decision for a 
human (or a separate, explicitly-reviewed change) given the positional
join contract between the two instrument YAML files
(src/config/yaml_split_merge.py) -- a scripted bulk edit is defensible,
a preflight *check* silently mutating production config on your behalf
is not.

Usage:
    python scripts/preflight/check_ticker_liveness.py
    python scripts/preflight/check_ticker_liveness.py --market us_stocks
    python scripts/preflight/check_ticker_liveness.py --symbol MRO
    python scripts/preflight/check_ticker_liveness.py --cross-check-delisting
    python scripts/preflight/check_ticker_liveness.py --symbols-file dead.txt --cross-check-delisting

Exit code 0 = Tier 1 all PASS (or all Tier-1 FAILs resolved ACTIVE by
Tier 2, i.e. nothing actionable). Exit code 1 = at least one symbol
classified DELISTED or UNRESOLVED after Tier 2 (or, if Tier 2 not run,
at least one Tier-1 FAIL exists needing investigation).

Out of src/, by design (this whole scripts/preflight/ directory) -- no
unit test coverage requirement applies here (see KNOWN_RISKS.md v1.17.5
convention note); this script's only "test" is being run for real, on
real network-enabled hardware.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
load_dotenv()

REQUIRED_COLUMNS = {"Open", "High", "Low", "Close", "Volume"}
RECENCY_WINDOW_DAYS = 5

AV_LISTING_STATUS_URL = "https://www.alphavantage.co/query"


def _check_one_yfinance(symbol: str, yfinance_symbol: str) -> tuple[bool, str]:
    """Tier 1 -- identical shape to check_yfinance_tickers.py's
    _check_one(), scoped to Layer 1 (OHLCV, not context anchors)."""
    try:
        import yfinance as yf
    except ImportError:
        return False, "yfinance not installed in this environment"

    try:
        df = yf.download(
            yfinance_symbol, period=f"{RECENCY_WINDOW_DAYS}d",
            interval="1d", progress=False,
        )
    except Exception as e:
        return False, f"download() raised: {e}"

    if df is None or df.empty:
        return False, "empty DataFrame -- no recent data (delisted, renamed, or transient)"

    cols = set(df.columns.get_level_values(0)) if hasattr(df.columns, "get_level_values") else set(df.columns)
    missing = REQUIRED_COLUMNS - cols
    if missing:
        return False, f"missing expected OHLCV columns: {missing} (got: {sorted(cols)})"

    return True, f"OK -- {len(df)} rows in last {RECENCY_WINDOW_DAYS}d"


def _fetch_av_listing_status(api_key: str, state: str) -> list[dict]:
    """One raw HTTP call to AlphaVantage LISTING_STATUS -- same
    raw-httpx-for-a-new-endpoint convention as check_bis_cbpol_d.py (no
    existing adapter class covers this endpoint; FX_DAILY's
    AlphaVantageForexAdapter is a different call shape entirely).
    Costs 1 of the 25 daily requests PER CALL -- this function is called
    twice by --cross-check-delisting (once for state=active, once for
    state=delisted), so that flag costs 2/25, not 1/25 -- documented in
    --help below, corrected from this docstring's earlier single-call
    assumption during authoring."""
    import httpx
    resp = httpx.get(
        AV_LISTING_STATUS_URL,
        params={"function": "LISTING_STATUS", "state": state, "apikey": api_key},
        timeout=60.0,
    )
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    rows = list(reader)
    if rows and "symbol" not in rows[0]:
        raise RuntimeError(
            f"LISTING_STATUS response did not look like the expected CSV "
            f"shape (got columns: {list(rows[0].keys()) if rows else 'none'}) "
            f"-- check ALPHAVANTAGE_API_KEY validity and AV API status."
        )
    return rows


def _cross_check_delisting(fail_symbols: list[str], api_key: str) -> dict[str, dict]:
    """Tier 2. Returns {symbol: {classification, detail}} for every
    symbol in fail_symbols. classification is one of:
      DELISTED         -- found in AV's delisted list under this exact symbol
      LIKELY_TRANSIENT -- found in AV's active list -- universe entry is
                          fine, Tier-1 failure is a separate fetch-pipeline
                          issue worth its own investigation
      UNRESOLVED        -- found in neither -- commonly a pure ticker
                          rename AV's delisted records didn't capture, or
                          a name AV simply doesn't carry. Needs a human
                          (this thread resolved several of these via
                          targeted web search, not a further API call)."""
    active_rows = _fetch_av_listing_status(api_key, "active")
    delisted_rows = _fetch_av_listing_status(api_key, "delisted")

    active_by_symbol: dict[str, list[dict]] = {}
    for r in active_rows:
        active_by_symbol.setdefault(r["symbol"], []).append(r)
    delisted_by_symbol: dict[str, list[dict]] = {}
    for r in delisted_rows:
        delisted_by_symbol.setdefault(r["symbol"], []).append(r)

    results: dict[str, dict] = {}
    for sym in fail_symbols:
        if sym in delisted_by_symbol:
            m = delisted_by_symbol[sym][0]
            results[sym] = {
                "classification": "DELISTED",
                "detail": (
                    f"AV delisted record: {m['name']} ({m['exchange']}), "
                    f"delistingDate={m['delistingDate']} -- treat the exact "
                    f"date with caution (see module docstring re: batch "
                    f"artifact dates), trust the DELISTED partition itself"
                ),
            }
        elif sym in active_by_symbol:
            m = active_by_symbol[sym][0]
            results[sym] = {
                "classification": "LIKELY_TRANSIENT",
                "detail": (
                    f"AV shows ACTIVE: {m['name']} ({m['exchange']}) -- "
                    f"universe entry is fine, investigate the fetch "
                    f"pipeline separately (rate limit? wrong yfinance "
                    f"suffix? temporary source outage?)"
                ),
            }
        else:
            results[sym] = {
                "classification": "UNRESOLVED",
                "detail": (
                    "not found in AV active or delisted lists under this "
                    "exact symbol -- commonly a ticker rename AV's "
                    "delisted records don't capture cleanly, or a name AV "
                    "doesn't carry at all. Needs manual research (targeted "
                    "web search resolved several of these in the "
                    "originating thread) before any config change."
                ),
            }
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--market", default=None, help="Only check one Layer 1 market (e.g. us_stocks, idx, forex, commodity_trading)")
    parser.add_argument("--symbol", default=None, help="Only check one symbol (e.g. MRO)")
    parser.add_argument(
        "--symbols-file", default=None,
        help="Path to a text file, one symbol per line, to check instead "
             "of the full Layer 1 universe -- e.g. a shortlist of known "
             "coverage_check gaps. Symbols are still resolved against "
             "InstrumentLoader for their yfinance_symbol; unknown symbols "
             "are skipped with a warning.",
    )
    parser.add_argument(
        "--cross-check-delisting", action="store_true",
        help="Run Tier 2 on every Tier-1 FAIL: cross-reference AlphaVantage "
             "LISTING_STATUS (active + delisted, 2 of the 25 daily "
             "AlphaVantage requests -- one bulk call per state regardless "
             "of how many symbols failed Tier 1). Off by default.",
    )
    args = parser.parse_args()

    from src.config.instrument_loader import get_loader

    loader = get_loader()
    instruments = loader.all_symbols()

    if args.symbols_file:
        wanted = {
            line.strip().upper()
            for line in Path(args.symbols_file).read_text().splitlines()
            if line.strip()
        }
        by_symbol = {i.symbol: i for i in instruments}
        resolved = [by_symbol[s] for s in wanted if s in by_symbol]
        unknown = wanted - set(by_symbol.keys())
        if unknown:
            print(f"WARNING: {len(unknown)} symbol(s) in --symbols-file not found in InstrumentLoader, skipped: {sorted(unknown)}")
        instruments = resolved
    elif args.symbol:
        instruments = [i for i in instruments if i.symbol == args.symbol]
    elif args.market:
        instruments = [i for i in instruments if i.market == args.market]

    if not instruments:
        print("No matching Layer 1 instruments found for the given filter.")
        return 1

    print(f"Tier 1: checking {len(instruments)} Layer 1 symbol(s) against yfinance (last {RECENCY_WINDOW_DAYS}d)...")
    print()

    failures: list[str] = []
    for inst in sorted(instruments, key=lambda i: i.symbol):
        ok, msg = _check_one_yfinance(inst.symbol, inst.yfinance_symbol)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {inst.symbol:8s} ({inst.yfinance_symbol:10s}) [{inst.market}]  {msg}")
        if not ok:
            failures.append(inst.symbol)

    print()
    if not failures:
        print(f"All {len(instruments)} symbols PASSED Tier 1. Nothing to cross-check.")
        return 0

    print(f"{len(failures)}/{len(instruments)} symbol(s) FAILED Tier 1: {failures}")

    if not args.cross_check_delisting:
        print()
        print("Tier 2 (--cross-check-delisting) skipped -- costs 2 of the "
              "25 daily AlphaVantage requests. Re-run with that flag to "
              "classify these as DELISTED / LIKELY_TRANSIENT / UNRESOLVED "
              "before treating any of them as a universe change.")
        return 1

    import os
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        print()
        print("FAIL: --cross-check-delisting requested but ALPHAVANTAGE_API_KEY not set in .env.")
        return 1

    print()
    print("Tier 2: cross-checking failures against AlphaVantage LISTING_STATUS...")
    print()

    try:
        classified = _cross_check_delisting(failures, api_key)
    except Exception as e:
        print(f"FAIL: Tier 2 cross-check raised: {e}")
        return 1

    by_class: dict[str, list[str]] = {"DELISTED": [], "LIKELY_TRANSIENT": [], "UNRESOLVED": []}
    for sym, info in classified.items():
        by_class[info["classification"]].append(sym)
        print(f"[{info['classification']:16s}] {sym:8s}  {info['detail']}")

    print()
    print(f"Summary: {len(by_class['DELISTED'])} DELISTED, "
          f"{len(by_class['LIKELY_TRANSIENT'])} LIKELY_TRANSIENT (universe "
          f"entry fine, investigate fetch pipeline separately), "
          f"{len(by_class['UNRESOLVED'])} UNRESOLVED (needs manual research).")

    if by_class["DELISTED"] or by_class["UNRESOLVED"]:
        print()
        print("ACTION NEEDED -- this script does not modify config. Review "
              "DELISTED and UNRESOLVED symbols above, then apply any "
              "removal/rename to instruments_identity.yaml AND "
              "instruments_taxonomy.yaml together (positional join -- see "
              "src/config/yaml_split_merge.py), update "
              "validate_instruments.py's EXPECTED_TOTAL, and re-run the "
              "full test suite before mirroring.")
        return 1

    print("All Tier-1 failures resolved to LIKELY_TRANSIENT -- no universe change indicated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
