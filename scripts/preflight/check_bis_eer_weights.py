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

RESOLVED (Ovi, this thread, following up on the flag below): HKD/TWD/NOK
added to BROAD_DOLLAR_REF_AREAS. The *current* Broad Dollar basket design
(per instruments_taxonomy.yaml's own dollar/dollar_basket comments) is
EUR/JPY/GBP/CAD/CHF/AUD + IDR (all Layer 1) + the 6-currency
context_dollar_basket group (CNH/KRW/SGD/HKD/TWD/NOK) -- 13 currencies.
This script previously covered only 10 -- flagged as a known gap rather
than guessed at ("Ovi's instruction was specifically MXN->IDR") pending
explicit instruction, which has now been given. All 13 currencies are
covered as of this thread; BIS_EER_ENDPOINT_MONTHLY's key is now built
FROM BROAD_DOLLAR_REF_AREAS.values() rather than hand-duplicated, so this
class of drift-between-dict-and-key cannot recur (see the FIX comment
above that constant).

Same authoring/execution split as the other three scripts in this
directory: this sandbox's network allowlist has no route to
stats.bis.org. Authoring does not require network access; running it
does.

UPD (Ovi, 3 Aug 2026 thread): Gate 1 substantially advanced. BIS's own
data.bis.org/topics/EER page (server-rendered, unlike the SPA pages
elsewhere on this project) links directly, under its own "Methodology"
section, to a downloadable weights table:
https://www.bis.org/statistics/eer/weightsb.xlsx (Broad, 64 economies --
the one relevant here, since Narrow only covers 26/27 core economies and
IDR/HKD/TWD would not be in it). Confirmed reachable and genuinely an
.xlsx (mime type application/vnd.openxmlformats-officedocument.
spreadsheetml.sheet, not a redirect or error page) -- Gate 1's weight
COMPONENTS are real and machine-readable, just not via the SDMX API
check_bis_eer_weights.py otherwise uses; they're a plain file download.
Also confirmed via the same page: weights are TIME-VARYING on a 3-year
basis (vintages 1993-95 through 2017-19 per BIS's own FAQ; the 2017-19
vintage has been in continuous use for "the latest period" since, until
BIS publishes the next 3-year update) -- there is no single permanent
"exact weight," but there IS a specific, nameable current vintage, which
is what --discover-weights below is for. See _discover_weights() -- same
two-phase discover-then-extract pattern as --discover for the API
structure, since this thread cannot inspect the file's actual internal
layout (no network route to bis.org from any sandbox on this project;
Ovi's next run on the M1 is what actually reveals row/column structure
for a targeted extraction pass).

What this script still CANNOT resolve on its own: the exact current
weight VALUE for each of our 13 currencies specifically requires the
file's internal layout to be known, which --discover-weights surfaces
but does not yet parse into named values (deliberately -- guessing a
layout risks silently extracting the wrong cell, which is worse than not
extracting at all). A targeted extraction pass is the natural next step
once this has been run for real.

TYPE decision (Ovi, this thread -- was previously left wildcarded,
pending): NOMINAL, not Real. Two independent reasons converged: (1) DXY
itself -- the index this platform's Broad Dollar Index is explicitly
designed as a companion/extension of (Architecture v2.0 §7.2) -- is a
nominal currency-value index, not inflation-adjusted; using Real EER for
Broad Dollar while DXY stays Nominal would compare two conceptually
different things under one "Dollar strength" umbrella. (2) BIS's own EER
overview page states Daily-frequency EER data exists ONLY for Nominal
indices, never Real ("the latter available only as nominal indices") --
since this platform's Layer 2 anchors are specified at Daily cadence
(Architecture v2.0 §7.2: "Cadence: Daily (same as forex)"), Nominal is
the only choice that can actually deliver that. FREQ is now wildcarded
(empty segment) rather than hardcoded to M or D -- mirroring the exact
same reasoning already applied to check_bis_cbpol_d.py's key: request
whatever frequency BIS actually has per country rather than assume, and
let genuinely-available daily data come through where it exists without
risking a false failure on currencies that may only have monthly EER.

Usage:
    python scripts/preflight/check_bis_eer_weights.py
    python scripts/preflight/check_bis_eer_weights.py --discover
    python scripts/preflight/check_bis_eer_weights.py --discover-weights

Exit code 0 = EER index data returned for all currencies in the Broad
Dollar basket. Exit code 1 = at least one reference area failed, or
(--discover) no weight-bearing series could be identified in the
dataflow structure, or (--discover-weights) the weights file could not
be downloaded/parsed as xlsx.
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
# "Broad Dollar Index" directly. CONFIRMED LIVE (Ovi, M1, this thread):
# --discover fetched 568951 bytes of real dataflow structure; the data
# query returned 182410 bytes with all 10 (now 13) REF_AREA codes
# present.
#
# FIX (Ovi, this thread): endpoint key is now BUILT FROM
# BROAD_DOLLAR_REF_AREAS.values() rather than a separately hardcoded
# literal string. The literal-string version this replaces is exactly
# how the HKD/TWD/NOK gap happened in the first place -- and how it could
# have silently recurred: adding entries to the dict alone, without this
# change, would leave them permanently unfetched while _check_one() keeps
# confidently reporting "not present" -- indistinguishable from a genuine
# API failure. Building the key from the dict's own values makes this
# whole bug class structurally impossible from here on; the dict is now
# the single source of truth.
# Architecture v2.0 §7.2 BIS_WEIGHTS currencies, MXN removed / IDR added
# (28 Jul 2026); HKD/TWD/NOK added (Ovi, this thread) to complete the
# *current* Broad Dollar basket design per instruments_taxonomy.yaml's
# dollar/dollar_basket comments -- previously flagged as a known gap
# ("Ovi's instruction was specifically MXN->IDR") rather than guessed at,
# now closed on explicit instruction. This dict is the single source of
# truth for BIS_EER_ENDPOINT_MONTHLY's key below (built from its values,
# not hand-duplicated) and for every currency _check_one() validates.
# Mapped to BIS REF_AREA codes matching config/bis_cb_rates.yaml's
# ref_area_map convention.
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
    "HKD": "HK",  # NEW (Ovi, this thread) -- Hong Kong, pegged currency (reliability_flag pattern, same as SSEC/BOJ YCC)
    "TWD": "TW",  # NEW -- Taiwan
    "NOK": "NO",  # NEW -- Norway
}

# FIX (Ovi, this thread): endpoint key is built FROM BROAD_DOLLAR_REF_AREAS
# .values() -- not a separately hardcoded literal. A hardcoded literal is
# exactly how the HKD/TWD/NOK gap happened in the first place, and how it
# could silently recur: adding entries to the dict alone, without this,
# leaves them permanently unfetched while _check_one() keeps confidently
# reporting "not present" -- indistinguishable from a genuine API
# failure. This makes that whole bug class structurally impossible from
# here on; the dict above is the only place currency membership is
# declared. CONFIRMED LIVE (Ovi, M1, 1 Aug 2026, 10-currency M..B. key):
# --discover fetched 568951 bytes of real dataflow structure; the data
# query returned 182410 bytes with all 10 REF_AREA codes present.
# RE-CONFIRMED LIVE (Ovi, M1, 3 Aug 2026, full 13-currency version): all
# 13 REF_AREA codes PASS, 237188 bytes returned per check -- the HKD/TWD/
# NOK expansion is now empirically confirmed working live, not just
# code-fixed and test-verified.
#
# UPD (Ovi, same 3 Aug thread): renamed from BIS_EER_ENDPOINT_MONTHLY --
# TYPE is now fixed to N (Nominal, see module docstring for the two-part
# rationale) and FREQ is now wildcarded rather than hardcoded to M, so
# "_MONTHLY" was no longer an accurate name. Key changed from "M..B." to
# ".N.B." -- FREQ wildcarded (was fixed M), TYPE fixed to N (was
# wildcarded). Not yet re-confirmed live against this exact key shape --
# the 3 Aug confirmation above was against the prior M..B. key structure.
BIS_EER_ENDPOINT = (
    "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_EER/1.0/"
    ".N.B." + "+".join(BROAD_DOLLAR_REF_AREAS.values())
)
BIS_EER_DATAFLOW_STRUCTURE_URL = "https://stats.bis.org/api/v2/structure/dataflow/BIS/WS_EER/1.0"

# Gate 1 (ADR-017/018): the actual weight COMPONENTS, found this thread --
# not a queryable SDMX series, but a plain file download linked from BIS's
# own methodology page (data.bis.org/topics/EER). Broad (64 economies),
# not Narrow (26/27 -- would not include IDR/HKD/TWD). Confirmed reachable
# and genuinely an .xlsx this thread (mime type verified); internal layout
# (which sheet/row/column holds which currency's weight) not yet known --
# see _discover_weights().
BIS_EER_WEIGHTS_BROAD_URL = "https://www.bis.org/statistics/eer/weightsb.xlsx"


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
        text = _fetch_csv(BIS_EER_ENDPOINT)
    except Exception as e:
        return False, f"request raised: {e}"

    if not text.strip() or ref_area not in text:
        return False, f"no rows for REF_AREA={ref_area} in response ({len(text)} bytes total)"

    return True, f"OK -- {len(text)} bytes returned, REF_AREA={ref_area} present"


def _discover() -> int:
    """Fetch the WS_EER dataflow structure and print its dimension list --
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


def _discover_weights() -> int:
    """Download BIS's actual published Broad EER weights file and print its
    real internal structure -- Gate 1's answer, found this thread: weights
    are NOT in the SDMX API at all, they're a plain .xlsx download linked
    from data.bis.org/topics/EER's own Methodology section. This function
    does NOT assume a row/column layout (that would just be a new version
    of the same guessing problem this whole thread has been fixing) -- it
    downloads, opens the workbook, and reports sheet names/dimensions plus
    a structural sample and a scan for our own currency/REF_AREA codes, so
    a follow-up pass can write targeted extraction logic once the real
    layout is confirmed. Same two-phase discover-then-extract pattern as
    --discover for the API structure itself.
    """
    try:
        import httpx
        resp = httpx.get(BIS_EER_WEIGHTS_BROAD_URL, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        print(f"FAIL: could not download weights file: {e}")
        return 1

    print(f"Downloaded {len(resp.content)} bytes from {BIS_EER_WEIGHTS_BROAD_URL}")

    try:
        from io import BytesIO
        from openpyxl import load_workbook
        wb = load_workbook(BytesIO(resp.content), read_only=True, data_only=True)
    except Exception as e:
        print(f"FAIL: downloaded but could not parse as xlsx: {e}")
        return 1

    print(f"Sheets found: {wb.sheetnames}")

    # Search for our own currency/REF_AREA codes anywhere in each sheet's
    # first 200 rows -- bounded scan, since we don't know the layout and
    # some of these weight files span decades of vintages.
    target_codes = set(BROAD_DOLLAR_REF_AREAS.keys()) | set(BROAD_DOLLAR_REF_AREAS.values())
    any_matches = False

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        print(f"\n--- Sheet {sheet_name!r}: {ws.max_row} rows x {ws.max_column} cols ---")

        sample_rows = min(10, ws.max_row or 0)
        sample_cols = min(15, ws.max_column or 0)
        if sample_rows and sample_cols:
            print(f"First {sample_rows} rows x {sample_cols} cols:")
            for row in ws.iter_rows(min_row=1, max_row=sample_rows, max_col=sample_cols, values_only=True):
                print(f"  {row}")

        scan_rows = min(200, ws.max_row or 0)
        matches: list[tuple[int, int, object]] = []
        if scan_rows:
            for row_idx, row in enumerate(
                ws.iter_rows(min_row=1, max_row=scan_rows, values_only=True), start=1
            ):
                for col_idx, cell in enumerate(row, start=1):
                    if cell is not None and str(cell).strip() in target_codes:
                        matches.append((row_idx, col_idx, cell))
        if matches:
            any_matches = True
            print(f"Currency/REF_AREA code matches (row, col, value): {matches[:40]}")
        else:
            print(f"No direct currency-code matches in the first {scan_rows} rows "
                  f"-- this sheet may use full country names instead of codes, "
                  f"or the data may start further down.")

    wb.close()

    if not any_matches:
        print("\nNo sheet matched a currency/REF_AREA code directly in the scanned")
        print("range. The file may use country names or a different code convention")
        print("-- inspect the printed row samples above manually before writing any")
        print("targeted extraction logic.")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--discover", action="store_true",
        help="Fetch the WS_EER dataflow structure instead of querying index values",
    )
    parser.add_argument(
        "--discover-weights", action="store_true",
        help="Download and inspect BIS's published Broad EER weights xlsx (Gate 1)",
    )
    parser.add_argument("--currency", default=None, help="Only check one currency (e.g. KRW)")
    args = parser.parse_args()

    if args.discover_weights:
        return _discover_weights()

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
    print("NOTE: this confirms INDEX availability only. Gate 1's actual weight")
    print("COMPONENTS live in a separate file, not this API -- run with")
    print("--discover-weights to fetch and inspect BIS's published Broad weights.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
