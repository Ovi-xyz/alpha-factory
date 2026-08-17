"""
scripts/preflight/check_imf_weo.py

ADD (Ovi, this thread — 14 Aug 2026): "start with this sequent" preflight
pass. Same authoring/execution split as every other script in this
directory — authored now, executed later on network-enabled hardware
(this sandbox has no route to www.imf.org).

Confirms, empirically, that all 5 IMF WEO indicators src/bronze/
imf_ingester.py depends on still resolve via the IMF DataMapper API, and
that at least the current-run-year's KEY_COUNTRIES actually have non-null
values in the response (IMF's own JSON shape nests values as
{indicator: {country: {year: value}}} -- a structurally valid response
with an empty `values` dict for an indicator is possible and would not be
an HTTP error, exactly the gap this script closes). No API key required
-- IMF DataMapper is fully public, unlike every other source in this
directory's remaining check list.

Note: imf_weo.yaml's own schema doc says IMF WEO "is ingested but not yet
read by any Silver/Gold module" -- get_latest_value() is a standalone
utility with no caller found in src/ this thread. This preflight check is
therefore lower production consequence than FRED/BIS/EIA today (nothing
downstream breaks silently if IMF drifts), but still worth confirming
before any future consumer is built on top of it.

Usage:
    python scripts/preflight/check_imf_weo.py
    python scripts/preflight/check_imf_weo.py --indicator NGDP_RPCH
    python scripts/preflight/check_imf_weo.py --country IDN

Exit code 0 = all 5 indicators return usable values for at least one of
the 12 KEY_COUNTRIES. Exit code 1 = at least one indicator failed.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
load_dotenv()

IMF_API_BASE = "https://www.imf.org/external/datamapper/api/v1"

# Duplicated from src/bronze/imf_ingester.py's IMF_INDICATORS + KEY_COUNTRIES
# -- same independence rationale as every other script in this directory.
IMF_INDICATORS: dict[str, str] = {
    "NGDP_RPCH":   "gdp_growth",
    "PCPIPCH":     "cpi_inflation",
    "BCA_NGDPD":   "current_account",
    "GGXWDG_NGDP": "govt_debt",
    "LUR":         "unemployment",
}

KEY_COUNTRIES: list[str] = [
    "USA", "CHN", "JPN", "GBR", "DEU",
    "FRA", "IND", "BRA", "CAN", "KOR", "IDN", "AUS",
]


def _fetch_indicator(indicator_id: str) -> dict:
    import requests
    countries_str = ",".join(KEY_COUNTRIES)
    url = f"{IMF_API_BASE}/{indicator_id}/{countries_str}"
    resp = requests.get(
        url,
        params={"periods": ",".join(str(y) for y in range(2015, date.today().year + 1))},
        timeout=30,
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def _check_one(indicator_id: str, label: str, country_filter: str | None) -> tuple[bool, str]:
    try:
        data = _fetch_indicator(indicator_id)
    except Exception as e:
        return False, f"request raised: {e}"

    values = data.get("values", {}).get(indicator_id, {})
    if not values:
        return False, "empty 'values' dict -- indicator_id may no longer exist in DataMapper"

    countries_present = [c for c in KEY_COUNTRIES if c in values]
    if country_filter:
        countries_present = [c for c in countries_present if c == country_filter]
        if not countries_present:
            return False, f"country {country_filter!r} not present in response"

    with_data = [c for c in countries_present if values.get(c)]
    if not with_data:
        return False, (
            f"{len(countries_present)}/{len(KEY_COUNTRIES)} countries present in "
            f"response but ALL have empty year-value maps"
        )

    sample_country = with_data[0]
    sample_years = values[sample_country]
    latest_year = max(sample_years, key=int)
    return True, (
        f"OK -- {len(with_data)}/{len(KEY_COUNTRIES)} countries have data, "
        f"e.g. {sample_country} {latest_year}={sample_years[latest_year]}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--indicator", default=None, help="Only check one indicator ID (e.g. NGDP_RPCH)")
    parser.add_argument("--country", default=None, help="Only check for one country ISO3 code (e.g. IDN)")
    args = parser.parse_args()

    targets = dict(IMF_INDICATORS)
    if args.indicator:
        if args.indicator not in targets:
            print(f"No mapping for indicator {args.indicator!r}. Known: {list(IMF_INDICATORS)}")
            return 1
        targets = {args.indicator: targets[args.indicator]}

    if args.country and args.country not in KEY_COUNTRIES:
        print(f"[NOTE] {args.country!r} is not in KEY_COUNTRIES {KEY_COUNTRIES} -- "
              f"checking anyway in case the live IMF response covers it regardless.")

    failures = []
    for indicator_id, label in targets.items():
        ok, msg = _check_one(indicator_id, label, args.country)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {indicator_id:14s} ({label:18s})  {msg}")
        if not ok:
            failures.append(indicator_id)

    print()
    if failures:
        print(f"{len(failures)}/{len(targets)} indicators FAILED: {failures}")
        return 1

    print(f"All {len(targets)} IMF WEO indicators confirmed live (no API key required).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
