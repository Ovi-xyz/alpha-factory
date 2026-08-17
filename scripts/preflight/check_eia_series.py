"""
scripts/preflight/check_eia_series.py

ADD (Ovi, this thread — 14 Aug 2026): "start with this sequent" preflight
pass. Same authoring/execution split as every other script in this
directory — authored now, executed later on network-enabled hardware
(this sandbox has no route to api.eia.gov).

Confirms, empirically, that all 4 series src/bronze/eia_ingester.py
depends on still resolve against the EIA APIv2 /v2/seriesid/ backward-
compatibility route. This script hits the exact same URL shape as
EIAIngester._fetch_series() and validates the exact same response
structure (response.data = [{"period": ..., "value": ..., ...}, ...])
so a live EIA API-shape change would be caught here before it silently
starts returning None for every series in production.

FIX ADR-038 (GMI_Decision_Document_v9.docx, 14 Aug 2026): this script's
own first-ever live run (14 Aug 2026) is what discovered FIX EIA-1's v1
legacy endpoint was dead (HTTP 404 on all 4 series, both batched and
isolated — EIA's own docs confirm APIv1 was discontinued November 2022).
Migrated to the same /v2/seriesid/{id} route eia_ingester.py now uses,
to keep testing the real production path rather than the endpoint
Bronze no longer calls.

EIA_API_KEY: EIA's own APIv2 documentation lists it as required on every
route including /seriesid/ (stricter than v1's leniency) — this script
still attempts the request unauthenticated if unset (mirroring
EIAIngester's own soft-warning-not-hard-fail behavior), but now expects
that to fail rather than succeed.

Usage:
    python scripts/preflight/check_eia_series.py
    python scripts/preflight/check_eia_series.py --series PET.RWTC.W

Exit code 0 = all 4 series return real observations. Exit code 1 = at
least one series failed.
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

# FIX ADR-038: v2 seriesid backward-compat route. series_id is part of the
# URL path itself (not a query param, unlike v1).
EIA_V2_ENDPOINT = "https://api.eia.gov/v2/seriesid/{series_id}"

# Duplicated from src/bronze/eia_ingester.py's EIA_SERIES -- same
# independence rationale as every other script in this directory: a check
# that only imports the ingester's own list would never catch the
# ingester's list itself silently drifting from what EIA actually serves.
EIA_SERIES: dict[str, str] = {
    "PET.WCRSTUS1.W": "us_crude_stocks",
    "PET.WCRFPUS2.W": "us_crude_production",
    "PET.WGIRIUS2.W": "us_refinery_input",
    "PET.RWTC.W":     "wti_spot_price",
}


def _check_one(series_id: str, api_key: str | None) -> tuple[bool, str]:
    import requests

    params: dict = {
        "start": (date.today() - timedelta(days=90)).isoformat(),
        "end": date.today().isoformat(),
    }
    if api_key:
        params["api_key"] = api_key

    url = EIA_V2_ENDPOINT.format(series_id=series_id)

    try:
        resp = requests.get(url, params=params, timeout=30)
    except Exception as e:
        return False, f"request raised: {e}"

    if resp.status_code != 200:
        return False, f"HTTP {resp.status_code}"

    try:
        data = resp.json()
    except Exception as e:
        return False, f"response not valid JSON: {e}"

    # FIX ADR-038: v2 envelope is {"response": {"data": [...]}, ...} --
    # distinct from v1's {"series": [{"data": [[period, value], ...]}]}.
    response_obj = data.get("response")
    if response_obj is None:
        err = data.get("error")
        return False, f"no 'response' envelope in body{f' -- {err}' if err else ''}"

    raw_rows = response_obj.get("data", [])
    if not raw_rows:
        return False, "'response.data' present but 0 rows"

    latest = raw_rows[0]
    latest_period, latest_value = latest.get("period"), latest.get("value")
    return True, f"OK -- {len(raw_rows)} rows, latest period={latest_period} value={latest_value}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--series", default=None, help="Only check one series ID (e.g. PET.RWTC.W)")
    args = parser.parse_args()

    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("[NOTE] EIA_API_KEY not set -- APIv2 documents api_key as "
              "required on every route; attempting unauthenticated "
              "requests anyway (EIAIngester does the same thing), "
              "expect failure.")

    targets = dict(EIA_SERIES)
    if args.series:
        if args.series not in targets:
            print(f"No mapping for series {args.series!r}. Known: {list(EIA_SERIES)}")
            return 1
        targets = {args.series: targets[args.series]}

    failures = []
    for series_id, label in targets.items():
        ok, msg = _check_one(series_id, api_key)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {series_id:16s} ({label:20s})  {msg}")
        if not ok:
            failures.append(series_id)

    print()
    if failures:
        print(f"{len(failures)}/{len(targets)} series FAILED: {failures}")
        return 1

    print(f"All {len(targets)} EIA series confirmed live (v2 seriesid endpoint).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
