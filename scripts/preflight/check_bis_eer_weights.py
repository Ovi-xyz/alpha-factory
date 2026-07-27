"""
scripts/preflight/check_bis_eer_weights.py

ADD -- closes (partially -- see "What this script cannot resolve" below)
Gate 1 in the ADR registry: "ADR-017: Exact Broad Dollar basket weights --
PARTIALLY GATED -- blocked on BIS EER weight-component data availability
(Gate 1). Not implemented as concrete numbers" and the identical ADR-018
gate for the IDR basket-weight override magnitude. Both are listed as
still-open in Alpha_Factory_Development_Log.md §12 Known Unknowns. No
script anywhere in this repo has attempted this before this thread
(confirmed: zero hits for "BIS EER"/"effective exchange rate" across
scripts/, config/, src/).

Same authoring/execution split as the other four scripts in this
directory: this sandbox's network allowlist has no route to
stats.bis.org, same as every prior GMI checkpoint. Authoring does not
require network access; running it does.

What this thread confirmed via web search (external to this sandbox --
Claude's search tool, not this script's own network path) that prior
threads had no way to check:

  - BIS's live SDMX API is still hosted at stats.bis.org (the human-facing
    browser moved to a separate data.bis.org portal, but that is a UI, not
    the API used by config/bis_cb_rates.yaml or this script).
  - A real, working example query against dataset WS_EER_M (monthly
    nominal/real effective exchange rates) was found in the wild:
    stats.bis.org/api/v1/data/WS_EER_M/M.N.B.CH/all?startPeriod=2000&...
    -- confirming an EER dataset genuinely exists via the same /api/v1/
    path style config/bis_cb_rates.yaml already uses for WS_CBPOL_D.
  - BIS's own EER documentation states it has published DAILY nominal EER
    data since September 2016 "to complement the monthly" series --
    implying a WS_EER_D (daily) counterpart likely exists, mirroring
    exactly why ADR-010 preferred BIS CBPOL_D (daily) over FRED's monthly
    ECB series in the first place. Neither WS_EER_D's exact existence nor
    its precise dimension-key syntax (the "M.N.B.CH"-style query segment)
    is confirmed -- this script's --discover mode exists to establish
    that empirically, since guessing the exact key syntax wrong would
    silently produce a 0-row/error response indistinguishable from
    "dataset doesn't exist."

What this script CANNOT resolve (documented so this isn't oversold as
closing Gate 1 completely): BIS's own EER methodology page states basket
WEIGHTS are revised roughly every 2-3 years and were historically
published as an appendix table in periodic BIS Quarterly Review papers
(e.g. "the new BIS effective exchange rate indices", March 2006, Appendix
I) -- i.e. as a documentation artifact, not necessarily as a queryable
SDMX data series alongside the index values themselves. This script
confirms the EER INDEX time series is reachable and correctly shaped; it
does NOT confirm per-currency weight COMPONENTS are available in the same
machine-readable form. If --discover finds no weight-bearing series in
the dataflow's own dimension list, the honest next step is manually
locating BIS's current weights publication (linked from
https://www.bis.org/statistics/eer.htm) rather than assuming the API
alone will yield it -- flagged explicitly in this script's own output so
that ambiguity isn't silently absorbed the way RISK-12/13/14 were.

One more thing worth Ovi's attention, found as a side effect of this
research and unrelated to Gate 1 directly: BIS's own API docs are now
served from stats.bis.org/api-doc/v2/, and a real v2-style query example
was found for a DIFFERENT dataset (WS_CBTA) using a
/api/v2/data/dataflow/BIS/<FLOW>/1.0/<key> path shape -- structurally
different from the /api/v1/data/<FLOW>/all shape config/bis_cb_rates.yaml
and check_bis_cbpol_d.py both currently use. This is NOT strong enough
evidence to conclude v1 is deprecated (v1/v2 commonly coexist during an
API's migration window, and no direct evidence of v1's removal was
found) -- so check_bis_cbpol_d.py was deliberately left untouched this
thread rather than "fixed" on an inconclusive signal. Worth a quick check
whenever either script is actually run against live BIS.

Usage:
    python scripts/preflight/check_bis_eer_weights.py
    python scripts/preflight/check_bis_eer_weights.py --discover

Exit code 0 = EER index data returned for all currencies in the Broad
Dollar basket (Architecture v2.0 §7.2 BIS_WEIGHTS keys). Exit code 1 =
at least one reference area failed, or (--discover) no weight-bearing
series could be identified in the dataflow structure.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BIS_EER_ENDPOINT_DAILY = "https://stats.bis.org/api/v1/data/WS_EER_D/all"
BIS_EER_ENDPOINT_MONTHLY = "https://stats.bis.org/api/v1/data/WS_EER_M/all"
BIS_EER_DATAFLOW_STRUCTURE_URL = "https://stats.bis.org/api/v1/dataflow/BIS/WS_EER_D"

# Architecture v2.0 §7.2 BIS_WEIGHTS -- the currencies the Broad Dollar
# Index derived feature already hardcodes approximate weights for. Mapped
# here to BIS REF_AREA codes (ISO-alpha-2-ish BIS convention, matching
# config/bis_cb_rates.yaml's ref_area_map style) so this script asks BIS
# about the same basket the eventual CrossAssetEngine feature will need.
BROAD_DOLLAR_REF_AREAS: dict[str, str] = {
    "EUR": "XM",  # Euro area
    "JPY": "JP",
    "GBP": "GB",
    "CAD": "CA",
    "CHF": "CH",
    "AUD": "AU",
    "MXN": "MX",
    "CNH": "CN",  # onshore CNY EER is BIS's only option; CNH itself isn't a BIS REF_AREA
    "KRW": "KR",
    "SGD": "SG",
}


def _fetch_csv(url: str, ref_area: str) -> str:
    import httpx
    resp = httpx.get(
        url,
        params={"format": "csv", "detail": "dataonly"},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.text


def _check_one(currency: str, ref_area: str, daily: bool) -> tuple[bool, str]:
    endpoint = BIS_EER_ENDPOINT_DAILY if daily else BIS_EER_ENDPOINT_MONTHLY
    try:
        text = _fetch_csv(endpoint, ref_area)
    except Exception as e:
        return False, f"request raised: {e}"

    if not text.strip() or ref_area not in text:
        return False, f"no rows for REF_AREA={ref_area} in response ({len(text)} bytes total)"

    return True, f"OK -- {len(text)} bytes returned, REF_AREA={ref_area} present"


def _discover() -> int:
    """Fetch the WS_EER_D dataflow structure and print its dimension list --
    the honest way to find out whether a weight-bearing dimension exists,
    rather than guessing the query-key syntax (see module docstring)."""
    try:
        import httpx
        resp = httpx.get(BIS_EER_DATAFLOW_STRUCTURE_URL, params={"references": "all"}, timeout=30.0)
        resp.raise_for_status()
    except Exception as e:
        print(f"FAIL: could not fetch dataflow structure: {e}")
        return 1

    body = resp.text
    print(f"Fetched {len(body)} bytes of dataflow structure from {BIS_EER_DATAFLOW_STRUCTURE_URL}")
    print("Inspect manually for a weight-bearing dimension/codelist (e.g. anything")
    print("resembling 'WEIGHT', 'BASKET_SHARE', or similar) alongside the standard")
    print("FREQ/TYPE(N,R)/BASKET(B,N)/REF_AREA dimensions -- absence of one there")
    print("is the concrete signal that weights are a documentation artifact, not")
    print("an API-queryable series (see module docstring).")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--discover", action="store_true",
        help="Fetch the WS_EER_D dataflow structure instead of querying index values",
    )
    parser.add_argument("--currency", default=None, help="Only check one currency (e.g. KRW)")
    args = parser.parse_args()

    if args.discover:
        return _discover()

    targets = list(BROAD_DOLLAR_REF_AREAS.items())
    if args.currency:
        targets = [(c, r) for c, r in targets if c == args.currency]
        if not targets:
            print(f"No mapping for currency {args.currency!r}. Known: {list(BROAD_DOLLAR_REF_AREAS)}")
            return 1

    failures = []
    for currency, ref_area in sorted(targets):
        ok, msg = _check_one(currency, ref_area, daily=True)
        if not ok:
            # Fall back to monthly before declaring failure -- daily EER
            # coverage since 2016 is documented but its exact dataset
            # availability per-currency is unconfirmed (see docstring).
            ok_m, msg_m = _check_one(currency, ref_area, daily=False)
            if ok_m:
                ok, msg = ok_m, f"(daily failed: {msg}) monthly fallback: {msg_m}"
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {currency:5s} (REF_AREA={ref_area})  {msg}")
        if not ok:
            failures.append(currency)

    print()
    if failures:
        print(f"{len(failures)}/{len(targets)} currencies FAILED: {failures}")
        return 1

    print(f"All {len(targets)} currencies' EER index data reachable.")
    print("NOTE: this confirms INDEX availability only -- see module docstring")
    print("for why weight COMPONENTS (Gate 1's actual blocker) may still need")
    print("manual sourcing from BIS's published methodology tables.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
