"""
scripts/preflight/check_bls_series.py

ADD (Ovi, this thread — 14 Aug 2026): "start with this sequent" preflight
pass. Same authoring/execution split as every other script in this
directory — authored now, executed later on network-enabled hardware
(this sandbox has no route to api.bls.gov).

Confirms, empirically, that all 6 series src/bronze/bls_ingester.py
depends on still resolve against the BLS v2 timeseries API, in a single
batch POST identical in shape to BLSIngester._fetch_batch() (BLS allows
up to 25 series per request — this script checks all 6 in one call, same
as production). Also confirms the response's `period` field still uses
the "M01".."M12" / "Qn" / "An" convention FIX BLS-1 depends on to build
observation_date -- a format change there would silently corrupt every
BLS observation_date without raising, since the ingester's own parser
just skips unrecognized period codes rather than erroring.

BLS_API_KEY is required for this script (native BLS path) -- if unset,
BLSIngester falls back to fetching the FRED-mirror equivalents instead
(CPIAUCSL, PAYEMS, UNRATE, etc. -- already covered by
check_fred_series.py), so there is nothing native to check here without a
key. This mirrors BLSIngester.run()'s own branching rather than silently
substituting the FRED mirror.

Usage:
    python scripts/preflight/check_bls_series.py
    python scripts/preflight/check_bls_series.py --series CUUR0000SA0

Exit code 0 = all 6 series return real observations with parseable period
codes. Exit code 1 = BLS_API_KEY missing, or at least one series failed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
load_dotenv()

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# Duplicated from src/bronze/bls_ingester.py's BLS_SERIES -- same
# independence rationale as every other script in this directory.
BLS_SERIES: dict[str, str] = {
    "CUUR0000SA0":    "cpi_headline",
    "CUUR0000SA0L1E": "cpi_core",
    "WPU00000000":    "ppi_headline",
    "CES0000000001":  "nfp_total",
    "LNS14000000":    "unemployment_rate",
    "LNS11000000":    "labor_force_participation",
}

# FIX BLS-1's own accepted period-code prefixes -- anything else is a
# format this pipeline doesn't know how to parse into observation_date.
_KNOWN_PERIOD_PREFIXES = ("M", "Q", "A")


def _fetch_batch(series_ids: list[str], api_key: str) -> dict:
    import requests
    payload = {
        "seriesid": series_ids,
        "startyear": "2020",
        "endyear": str(__import__("datetime").date.today().year),
        "registrationkey": api_key,
        "calculations": False,
        "annualaverage": False,
    }
    resp = requests.post(
        BLS_API_URL,
        data=json.dumps(payload),
        headers={"Content-type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--series", default=None, help="Only check one series ID (e.g. CUUR0000SA0)")
    args = parser.parse_args()

    api_key = os.getenv("BLS_API_KEY")
    if not api_key:
        print("FAIL: BLS_API_KEY not set -- cannot check the native BLS path.")
        print("BLSIngester falls back to FRED-mirror series in this case "
              "(CPIAUCSL/PAYEMS/UNRATE/etc.) -- run check_fred_series.py "
              "instead, or set BLS_API_KEY to check the native path.")
        print("Register free at https://data.bls.gov/registrationEngine/")
        return 1

    targets = dict(BLS_SERIES)
    if args.series:
        if args.series not in targets:
            print(f"No mapping for series {args.series!r}. Known: {list(BLS_SERIES)}")
            return 1
        targets = {args.series: targets[args.series]}

    try:
        data = _fetch_batch(list(targets), api_key)
    except Exception as e:
        print(f"FAILED to fetch BLS batch: {e}")
        return 1

    if data.get("status") != "REQUEST_SUCCEEDED":
        print(f"FAIL: BLS API status={data.get('status')!r} -- {data.get('message')}")
        return 1

    results_by_id = {s.get("seriesID"): s for s in data.get("Results", {}).get("series", [])}

    failures = []
    for series_id, label in targets.items():
        series = results_by_id.get(series_id)
        if series is None:
            print(f"[FAIL] {series_id:16s} ({label:26s})  not present in response")
            failures.append(series_id)
            continue

        rows = series.get("data", [])
        if not rows:
            print(f"[FAIL] {series_id:16s} ({label:26s})  0 data rows")
            failures.append(series_id)
            continue

        latest = rows[0]
        period = latest.get("period", "")
        if not period.startswith(_KNOWN_PERIOD_PREFIXES):
            print(
                f"[FAIL] {series_id:16s} ({label:26s})  unrecognized period "
                f"code {period!r} -- FIX BLS-1's parser would silently skip this row"
            )
            failures.append(series_id)
            continue

        print(
            f"[PASS] {series_id:16s} ({label:26s})  OK -- "
            f"latest period={latest.get('year')}-{period} value={latest.get('value')}"
        )

    print()
    if failures:
        print(f"{len(failures)}/{len(targets)} series FAILED: {failures}")
        return 1

    print(f"All {len(targets)} BLS series confirmed live, with parseable period codes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
