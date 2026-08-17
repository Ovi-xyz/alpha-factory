"""
scripts/preflight/check_bea_datasets.py

ADD (Ovi, this thread — 14 Aug 2026): "start with this sequent" preflight
pass. Same authoring/execution split as every other script in this
directory — authored now, executed later on network-enabled hardware
(this sandbox has no route to apps.bea.gov).

Confirms, empirically, that all 3 BEA NIPA tables src/bronze/bea_ingester.py
depends on still resolve, AND — more load-bearing than existence alone —
that each table's response still contains a row whose LineDescription
EXACTLY matches FIX GLD-001's LINE_FILTER string. This is the specific
thing that matters: BEA NIPA tables return 27+ rows per quarter (one per
GDP component / unit variant), and _fetch_nipa() silently drops every row
whose LineDescription doesn't match the target string. If BEA ever
renames "Gross domestic product" to something else, _fetch_nipa() would
return an EMPTY DataFrame (not an error) for that table, and this is the
one check able to catch that before it happens in production.

Note on scope: KNOWN_RISKS.md / CI/CD Ops Guide v1.7.4 (NEW-7) describes
BEA NIPA unit-mixing as DEFERRED, unresolved. Direct read of the live
_fetch_nipa() this thread shows LINE_FILTER + a LineDescription equality
check already implemented (FIX GLD-001) -- the fix landed in src/ at some
point after that doc was last accurate. Flagging the doc/code drift here
since it's exactly the kind of stale-documentation gap this project's own
"empirical over documentation" principle exists to catch -- not fixing
the docs myself, since that's a separate, explicit task.

FIX ADR-039/ADR-040 (GMI_Decision_Document_v9.docx, 14 Aug 2026): this
script's own first-ever live run (14 Aug 2026) is what discovered
pce_deflator's LineDescription match failing 0/310 rows live, and
surfaced T40100 as the wrong table for trade_balance (its own returned
rows are current-account/balance-of-payments concepts, not GDP net
exports). Mirrors bea_ingester.py's fix: LineNumber-based matching
(robust to wording drift) for pce_deflator and trade_balance;
trade_balance's table switched T40100 -> T10105. real_gdp is UNCHANGED
(still matches live on LineDescription).

BEA_API_KEY is required (native NIPA path) -- if unset, BEAIngester falls
back to the FRED-mirror equivalents (A191RL1Q225SBEA, PCEPI, etc. --
already covered by check_fred_series.py), mirrored here the same way
check_bls_series.py handles the BLS/FRED-mirror branch.

Usage:
    python scripts/preflight/check_bea_datasets.py
    python scripts/preflight/check_bea_datasets.py --table real_gdp

Exit code 0 = all 3 tables resolve AND contain the expected LineDescription
row. Exit code 1 = BEA_API_KEY missing, a table failed to resolve, or the
expected LineDescription row is absent (LINE_FILTER would silently drop
everything for that table in production).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
load_dotenv()

BEA_API_URL = "https://apps.bea.gov/api/data"

# Duplicated from src/bronze/bea_ingester.py's BEA_SERIES + LINE_FILTER +
# LINE_NUMBER_FILTER -- same independence rationale as every other script
# in this directory. line_number present = LineNumber is the active match
# (FIX ADR-039/040); line_number absent = LineDescription is still the
# active match (real_gdp, unchanged).
BEA_TABLES: dict[str, dict] = {
    "real_gdp": {
        "table_name": "T10106",
        "line_description": "Gross domestic product",
        "line_number": None,
        "description": "Real GDP (quarterly)",
    },
    "pce_deflator": {
        "table_name": "T20304",
        "line_description": "Personal consumption expenditures",  # label only — see line_number
        "line_number": "1",
        "description": "PCE Price Index (quarterly)",
    },
    "trade_balance": {
        # FIX ADR-040: was T40100 (International Transactions /
        # current-account -- wrong concept). T10105 = Table 1.1.5,
        # Gross Domestic Product, the standard GDP-components table.
        "table_name": "T10105",
        "line_description": "Net exports of goods and services",  # label only — see line_number
        "line_number": "15",   # PENDING LIVE CONFIRMATION — see ADR-040
        "description": "Trade Balance — Net exports of goods and services (GDP component)",
    },
}


def _fetch_table(table_name: str, api_key: str) -> dict:
    import requests
    params = {
        "UserID": api_key,
        "method": "GetData",
        "datasetname": "NIPA",
        "TableName": table_name,
        "Frequency": "Q",
        "Year": ",".join(str(y) for y in range(date.today().year - 2, date.today().year + 1)),
        "ResultFormat": "JSON",
    }
    resp = requests.get(BEA_API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _check_one(spec_name: str, spec: dict, api_key: str) -> tuple[bool, str]:
    try:
        data = _fetch_table(spec["table_name"], api_key)
    except Exception as e:
        return False, f"request raised: {e}"

    # BEA returns HTTP 200 even for malformed requests -- errors surface
    # inside the JSON body under BEAAPI.Results.Error, not as an HTTP code.
    error = data.get("BEAAPI", {}).get("Results", {}).get("Error")
    if error:
        return False, f"BEA API error: {error}"

    rows = data.get("BEAAPI", {}).get("Results", {}).get("Data", [])
    if not rows:
        return False, "0 rows returned"

    # FIX ADR-039/040: LineNumber-first match when spec declares one
    # (robust to LineDescription wording drift); LineDescription fallback
    # otherwise (real_gdp, unchanged).
    line_number = spec.get("line_number")
    if line_number:
        matches = [r for r in rows if str(r.get("LineNumber", "")).strip() == line_number]
        match_desc = f"LineNumber {line_number!r}"
    else:
        target = spec["line_description"]
        matches = [r for r in rows if r.get("LineDescription", "").strip() == target]
        match_desc = f"LineDescription {target!r}"

    if not matches:
        seen = sorted({r.get("LineDescription", "") for r in rows})[:5]
        return False, (
            f"{len(rows)} rows returned but NONE match {match_desc} "
            f"(LINE_FILTER/LINE_NUMBER_FILTER would drop all of them). "
            f"Sample of what's actually in the response: {seen}"
        )

    latest = sorted(matches, key=lambda r: r.get("TimePeriod", ""))[-1]
    return True, (
        f"OK -- {len(matches)}/{len(rows)} rows match {match_desc}, "
        f"latest={latest.get('TimePeriod')} value={latest.get('DataValue')} "
        f"line_description={latest.get('LineDescription')!r}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--table", default=None,
        help="Only check one table (real_gdp/pce_deflator/trade_balance)",
    )
    args = parser.parse_args()

    api_key = os.getenv("BEA_API_KEY")
    if not api_key:
        print("FAIL: BEA_API_KEY not set -- cannot check the native NIPA path.")
        print("BEAIngester falls back to FRED-mirror series in this case "
              "(A191RL1Q225SBEA/PCEPI/BOPGSTB) -- run check_fred_series.py "
              "instead, or set BEA_API_KEY to check the native path.")
        print("Register free at https://apps.bea.gov/api/signup/")
        return 1

    targets = dict(BEA_TABLES)
    if args.table:
        if args.table not in targets:
            print(f"No mapping for table {args.table!r}. Known: {list(BEA_TABLES)}")
            return 1
        targets = {args.table: targets[args.table]}

    failures = []
    for name, spec in targets.items():
        ok, msg = _check_one(name, spec, api_key)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {name:14s} ({spec['table_name']}, {spec['description']})  {msg}")
        if not ok:
            failures.append(name)

    print()
    if failures:
        print(f"{len(failures)}/{len(targets)} tables FAILED: {failures}")
        return 1

    print(f"All {len(targets)} BEA NIPA tables confirmed live, with LINE_FILTER rows present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
