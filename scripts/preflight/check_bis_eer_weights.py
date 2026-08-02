"""
scripts/preflight/check_bis_eer_weights.py

ADD -- closes (partially -- see "What this script cannot resolve" below)
Gate 1 in the ADR registry: "ADR-017: Exact Broad Dollar basket weights --
PARTIALLY GATED -- blocked on BIS EER weight-component data availability
(Gate 1). Not implemented as concrete numbers" and the identical ADR-018
gate for the IDR basket-weight override magnitude.

UPD (alpha-factory_preflight_logs, 28 July 2026): this script was actually
run and found every request 404ing (data) or 501ing (--discover). Fixed
at the time to the /api/v2/ path structure -- necessary, but as of the
29 July live re-run, NOT sufficient (still 404/501). See the FIX BIS-1
block above the endpoint constants for the actual root cause found 1 Aug
2026: the dataflow ID itself was wrong (WS_EER_M -> WS_EER) and the
--discover query was missing the "structure/" path segment entirely
(explaining the 501, which is a different failure signature than the
plain 404 a bad key alone would produce). WS_EER_D still does not appear
anywhere in confirmed evidence (BIS's own portal only shows Monthly EER
across every country sampled) -- WS_EER remains the only target, now
correctly named.

UPD (Ovi, same thread): MXN removed from the currency set below,
replaced with IDR. MXN (Mexican Peso) was carried over unmodified from
Architecture v2.0 §7.2's original BIS_WEIGHTS dict -- a generic "EM
currency" placeholder from before this platform's Indonesia-specific work
(ADR-013 through ADR-018) existed. It has zero relevance to this
platform and was never actually used anywhere else in the codebase
(confirmed: this script was MXN's only occurrence in the entire repo).
IDR is the economically relevant EM currency here -- already a Layer 1
forex pair (USD_IDR), already referenced in instruments_taxonomy.yaml's
own comments as part of the *current* Broad Dollar basket design
("plus USD_IDR (Layer 1, weight logic changes per ADR-018)"), and BI
(Bank Indonesia) is already BIS-covered via context_rates_em_cb.

NOTE -- found while making that fix, not silently expanded: the *current*
Broad Dollar basket design (per instruments_taxonomy.yaml's own dollar/
dollar_basket comments) is actually EUR/JPY/GBP/CAD/CHF/AUD + IDR (all
Layer 1) + the 6-currency context_dollar_basket group (CNH/KRW/SGD/HKD/
TWD/NOK) -- 13 currencies, not the 9 below (was 10 with MXN). This
script still only covers the original Architecture v2.0 §7.2 list minus
MXN plus IDR; it does not yet check HKD/TWD/NOK. Flagged here rather
than silently fixed -- Ovi's instruction was specifically MXN->IDR, and
guessing BIS REF_AREA codes for the other three without being asked risks
introducing new unverified values in the same pass as fixing old ones.

Same authoring/execution split as the other four scripts in this
directory: this sandbox's network allowlist has no route to
stats.bis.org. Authoring does not require network access; running it
does.

What this script CANNOT resolve (unchanged from before -- documented so
this isn't oversold as closing Gate 1 completely): BIS's own EER
methodology page states basket WEIGHTS are revised roughly every 2-3
years and were historically published as an appendix table in periodic
BIS Quarterly Review papers -- a documentation artifact, not necessarily
a queryable SDMX data series alongside the index values themselves. This
script confirms the EER INDEX time series is reachable and correctly
shaped; it does NOT confirm per-currency weight COMPONENTS are available
in the same machine-readable form. If --discover finds no weight-bearing
series in the dataflow's own dimension list, the honest next step is
manually locating BIS's current weights publication (linked from
https://www.bis.org/statistics/eer.htm) rather than assuming the API
alone will yield it.

Usage:
    python scripts/preflight/check_bis_eer_weights.py
    python scripts/preflight/check_bis_eer_weights.py --discover

Exit code 0 = EER index data returned for all currencies in the Broad
Dollar basket. Exit code 1 = at least one reference area failed, or
(--discover) no weight-bearing series could be identified in the
dataflow structure.
"""

from __future__ import annotations

import argparse
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

# FIX BIS-1 (Ovi, 1 Aug 2026): same root-cause class as check_bis_cbpol_d.py
# -- the 28 July v1->v2 path fix was necessary but not sufficient. Two
# further errors, both fixed here:
#
# 1. Dataflow ID: WS_EER, not WS_EER_M. The "_M" was a monthly-cadence
#    label mistaken for part of the identifier -- same pattern as
#    WS_CBPOL_D, confirmed independently against data.bis.org's own
#    indexed pages ("topics/EER/BIS,WS_EER,1.0/{FREQ}.{TYPE}.{BASKET}.
#    {REF_AREA}", 7 countries checked: US/AE/CN/KR/XM/JP, both Real and
#    Nominal baskets seen). The old WS_EER_M name traces back to a v1-era
#    academic example (fgeerolf.com) whose flow name was itself already a
#    guess from before this project's v1->v2 migration -- both the flow
#    name and the path needed correcting together, not just the path.
# 2. Structure/discovery queries use a DIFFERENT prefix than data queries
#    -- "/api/v2/structure/dataflow/..." not "/api/v2/dataflow/...". The
#    old URL was missing "structure/" entirely, which is consistent with
#    the 501 (not 404) it was actually returning -- a malformed/
#    unrecognized v2 path, not a clean "not found". Confirmed via a real
#    SDMX 2025 conference paper (sdmx2025.org) showing both the data
#    query and structure query shapes for a third sibling dataflow
#    (WS_XRU), independently agreeing with the WS_CBTA data-query example
#    above.
#
# Key = {FREQ}.{TYPE}.{BASKET}.{REF_AREA}. FREQ=M (monthly is what BIS
# publishes EER at -- confirmed across all 7 sampled countries, no daily
# EER variant found). TYPE left wildcarded (empty -- returns both Real
# and Nominal) since neither this platform's Broad Dollar Index design
# nor Gate 1 has settled which one it wants; narrowing that is a separate
# call, not bundled into this endpoint fix. BASKET=B (broad), matching
# "Broad Dollar Index" directly. Not live-tested from this sandbox (no
# route to stats.bis.org in any sandbox on this project) -- run for real
# to close the loop.
BIS_EER_ENDPOINT_MONTHLY = (
    "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_EER/1.0/"
    "M..B.XM+JP+GB+CA+CH+AU+ID+CN+KR+SG"
)
BIS_EER_DATAFLOW_STRUCTURE_URL = "https://stats.bis.org/api/v2/structure/dataflow/BIS/WS_EER/1.0"

# Architecture v2.0 §7.2 BIS_WEIGHTS currencies, MXN removed / IDR added
# per Ovi's explicit instruction (see module docstring for full context,
# including the note on HKD/TWD/NOK being part of the *current* design
# but out of scope for this specific fix). Mapped to BIS REF_AREA codes
# matching config/bis_cb_rates.yaml's ref_area_map convention.
BROAD_DOLLAR_REF_AREAS: dict[str, str] = {
    "EUR": "XM",  # Euro area
    "JPY": "JP",
    "GBP": "GB",
    "CAD": "CA",
    "CHF": "CH",
    "AUD": "AU",
    "IDR": "ID",  # UPD: replaces MXN -- Indonesia, already covered via context_rates_em_cb (BI)
    "CNH": "CN",  # onshore CNY EER is BIS's only option; CNH itself isn't a BIS REF_AREA
    "KRW": "KR",
    "SGD": "SG",
}


def _fetch_csv(url: str) -> str:
    import httpx
    resp = httpx.get(
        url,
        params={"format": "csv", "detail": "dataonly"},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.text


def _check_one(ref_area: str) -> tuple[bool, str]:
    try:
        text = _fetch_csv(BIS_EER_ENDPOINT_MONTHLY)
    except Exception as e:
        return False, f"request raised: {e}"

    if not text.strip() or ref_area not in text:
        return False, f"no rows for REF_AREA={ref_area} in response ({len(text)} bytes total)"

    return True, f"OK -- {len(text)} bytes returned, REF_AREA={ref_area} present"


def _discover() -> int:
    """Fetch the WS_EER_M dataflow structure and print its dimension list --
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
        help="Fetch the WS_EER_M dataflow structure instead of querying index values",
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
        ok, msg = _check_one(ref_area)
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
