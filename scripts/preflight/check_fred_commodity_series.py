"""
scripts/preflight/check_fred_commodity_series.py

ADD RISK-15 fix (Ovi, 8 Aug 2026) -- KNOWN_RISKS.md's own suggested next
step #3: "author or extend a preflight script to confirm all 6 [commodity
Track 2] series resolve against live FRED (mirroring
check_yfinance_tickers.py's pattern)." No existing FRED preflight script
existed to extend (scripts/preflight/ only had check_bis_cbpol_d.py,
check_bis_eer_weights.py, check_finnhub_shape.py, check_yfinance_tickers.py
-- confirmed by directory listing this thread) -- authored fresh.

Confirms, empirically, that all 6 FRED series now registered under
config/fred_series.yaml's new `commodity` domain (this thread) actually
resolve against the live FRED API and return real, monthly, IMF-sourced
observations -- not just that the series ID *string* looks plausible.

Also closes a documentation-vs-reality gap found while web-verifying
these series this thread, before writing any of them into
fred_series.yaml: Iron Ore's real FRED series ID is PIORECRUSDM -- NOT
PIORECRORECUSDM, the ID Architecture Extension v1.0 ADR-005 and this
project's own (pre-fix) KNOWN_RISKS.md RISK-15 entry both cite.
PIORECRORECUSDM does not exist on FRED (a duplicated "ORE" --
PIORE-CR-ORE-C-USDM vs the real PIORE-CR-USDM); neither prior document
was ever checked against live FRED before this thread -- exactly the gap
RISK-15 itself was tracking. All 6 IDs below were independently
confirmed (web search against fred.stlouisfed.org's own series pages,
not this sandbox's network-restricted bash tool) before being written
here or into fred_series.yaml.

Same authoring/execution split as the other four scripts in this
directory: FRED_API_KEY is a real secret (unlike BIS's key-free public
API) and api.stlouisfed.org has never been in any sandbox's network
allowlist on this project either. Authoring does not require network
access; running it does.

Usage:
    python scripts/preflight/check_fred_commodity_series.py
    python scripts/preflight/check_fred_commodity_series.py --series PTINUSDM

Exit code 0 = all 6 series return real, usable observations from live
FRED. Exit code 1 = FRED_API_KEY missing, or at least one series failed.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# FIX (same pattern as check_bis_eer_weights.py / check_bis_cbpol_d.py --
# "issues even though .env already filled"): python-dotenv is a declared
# dependency but is never invoked automatically -- os.getenv() only sees
# variables the shell has separately exported without this.
from dotenv import load_dotenv
load_dotenv()

FRED_OBSERVATIONS_ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"

# config/fred_series.yaml's new `commodity` domain block, duplicated here
# deliberately (not imported) -- same independence rationale as
# check_bis_cbpol_d.py's EXPECTED_REF_AREAS: a genuinely separate check of
# "does FRED still serve these 6 series," not a check that only
# re-validates whatever the config file itself currently says.
# FIX RISK-15 (8 Aug 2026): PIORECRUSDM corrected from the non-existent
# PIORECRORECUSDM (Architecture Extension v1.0 ADR-005 / this project's
# own prior KNOWN_RISKS.md entry both had the typo) -- verified against
# fred.stlouisfed.org/series/PIORECRUSDM directly.
EXPECTED_COMMODITY_SERIES: dict[str, str] = {
    "PIORECRUSDM": "IRON_ORE (VALE proxy, ADR-005)",
    "PCOALAUUSDM": "COAL_NEWC (WHC.AX proxy, ADR-006)",
    "PPOILUSDM":   "CPO (F34.SI proxy, ADR-030)",
    "PRUBBUSDM":   "RUBBER (STA.BK proxy, ADR-031)",
    "PTINUSDM":    "TIN (AFM.V proxy, ADR-032)",
    "PNICKUSDM":   "NICKEL (NIC.AX proxy, ADR-033)",
}


def _fetch_observations(series_id: str, api_key: str) -> dict:
    import httpx
    resp = httpx.get(
        FRED_OBSERVATIONS_ENDPOINT,
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 12,  # ~1Y of monthly obs -- existence/freshness check, not a full historical pull
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()


def _check_one(series_id: str, api_key: str) -> tuple[bool, str]:
    try:
        payload = _fetch_observations(series_id, api_key)
    except Exception as e:
        return False, f"request raised: {e}"

    obs = payload.get("observations", [])
    # FRED uses the literal string "." to mark a missing observation for
    # a given period -- distinct from an empty response entirely.
    real_obs = [o for o in obs if o.get("value") not in (None, ".", "")]
    if not real_obs:
        return False, (
            f"0 usable observations in response "
            f"({len(obs)} rows total, possibly all '.' missing-markers)"
        )

    latest = real_obs[0]  # sort_order=desc -> first row is most recent
    return True, (
        f"OK -- latest={latest['date']} value={latest['value']}, "
        f"{len(real_obs)}/{len(obs)} usable in last {len(obs)} requested"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--series", default=None,
        help="Only check one series ID (e.g. PTINUSDM)",
    )
    args = parser.parse_args()

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("FAIL: FRED_API_KEY not set in .env -- cannot check live FRED.")
        return 1

    targets = dict(EXPECTED_COMMODITY_SERIES)
    if args.series:
        if args.series not in targets:
            print(
                f"No mapping for series {args.series!r}. "
                f"Known: {list(EXPECTED_COMMODITY_SERIES)}"
            )
            return 1
        targets = {args.series: targets[args.series]}

    failures = []
    for series_id, label in sorted(targets.items()):
        ok, msg = _check_one(series_id, api_key)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {series_id:12s} ({label})  {msg}")
        if not ok:
            failures.append(series_id)

    print()
    if failures:
        print(f"{len(failures)}/{len(targets)} series FAILED: {failures}")
        return 1

    print(f"All {len(targets)} commodity Track 2 series confirmed live on FRED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
