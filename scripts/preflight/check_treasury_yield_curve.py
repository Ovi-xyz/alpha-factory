"""
scripts/preflight/check_treasury_yield_curve.py

ADD (Ovi, this thread — 14 Aug 2026): "start with this sequent" preflight
pass. Same authoring/execution split as every other script in this
directory — authored now, executed later on network-enabled hardware.

src/bronze/treasury_ingester.py (FIX TRES-1) does not call a Treasury API
at all — it delegates entirely to FREDIngester().run(run_date,
series_filter=TREASURY_FRED_SERIES), a 13-tenor list (DGS1MO...DGS30 +
T10Y2Y + T10Y3M + MORTGAGE30US). config/schemas/treasury_yield.yaml's own
ARCHITECTURE NOTE already documents this delegation. So "check US
Treasury" is mechanically identical to "check these 13 series on live
FRED" — same endpoint, same script shape as check_fred_series.py.

FINDING (this thread, live-code trace, not previously documented anywhere
in KNOWN_RISKS.md/CHANGELOG.md): FREDIngester.run()'s series_filter does
NOT fetch arbitrary series on request. It filters the series list already
LOADED FROM config/fred_series.yaml:

    series_list = self._registry.get("series", [])
    if series_filter:
        series_list = [s for s in series_list if s["id"] in series_filter]

A tenor absent from fred_series.yaml can never appear in series_list, so
it is silently dropped -- not fetched, not logged as skipped, no error.
Cross-referencing TREASURY_FRED_SERIES against fred_series.yaml's
monetary_policy domain (12 entries) directly: DGS2, DGS5, DGS10, DGS30,
T10Y2Y, T10Y3M, MORTGAGE30US (7) ARE registered; DGS1MO, DGS3MO, DGS6MO,
DGS1, DGS7, DGS20 (6) are NOT. Net effect: the "full 1M-30Y yield curve"
described in Grand Design v1.2 Section 3.3.3 currently only ever ingests
4 of 10 DGS tenors (2Y/5Y/10Y/30Y) in production -- the short end (1M/3M/
6M/1Y) and 7Y are never written to Bronze, silently. This script checks
both things: (1) does live FRED still serve all 13 tenors, and (2) which
of the 13 are actually reachable through the current registry-gated
ingestion path vs. which are config-invisible today. Found and flagged
here, not fixed -- fixing config/fred_series.yaml (or the filter
semantics in fred_ingester.py) is a source change out of scope for a
preflight script, and is Ovi's call, not mine to make silently.

Usage:
    python scripts/preflight/check_treasury_yield_curve.py

Exit code 0 = all 13 tenors resolve on live FRED. Exit code 1 =
FRED_API_KEY missing, or at least one tenor failed to resolve on live
FRED (independent of whether it's currently reachable via the registry
gap above -- that's a separate, additional warning printed regardless of
exit code).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import os
from dotenv import load_dotenv
load_dotenv()

FRED_OBSERVATIONS_ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"
FRED_REGISTRY_PATH = Path(__file__).resolve().parents[2] / "config" / "fred_series.yaml"

# Duplicated deliberately from src/bronze/treasury_ingester.py's
# TREASURY_FRED_SERIES -- same independence rationale as
# check_bis_cbpol_d.py's EXPECTED_REF_AREAS: a genuinely separate check of
# "does FRED still serve the 13 tenors Treasury declares," not a check
# that only re-validates whatever the ingester module currently imports.
TREASURY_TENORS: dict[str, str] = {
    "DGS1MO": "1M", "DGS3MO": "3M", "DGS6MO": "6M",
    "DGS1": "1Y", "DGS2": "2Y", "DGS5": "5Y", "DGS7": "7Y",
    "DGS10": "10Y", "DGS20": "20Y", "DGS30": "30Y",
    "T10Y2Y": "spread_10y2y", "T10Y3M": "spread_10y3m",
    "MORTGAGE30US": "mortgage_30y",
}


def _registered_series_ids() -> set[str]:
    import yaml
    data = yaml.safe_load(FRED_REGISTRY_PATH.read_text())
    return {s["id"] for s in data.get("series", [])}


def _fetch_observations(series_id: str, api_key: str) -> dict:
    import httpx
    resp = httpx.get(
        FRED_OBSERVATIONS_ENDPOINT,
        params={
            "series_id": series_id, "api_key": api_key,
            "file_type": "json", "sort_order": "desc", "limit": 5,
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
    real_obs = [o for o in obs if o.get("value") not in (None, ".", "")]
    if not real_obs:
        return False, f"0 usable observations ({len(obs)} rows total)"

    latest = real_obs[0]
    return True, f"OK -- latest={latest['date']} value={latest['value']}"


def main() -> int:
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("FAIL: FRED_API_KEY not set in .env -- cannot check live FRED.")
        print("(TreasuryIngester has no separate key -- it delegates 100% to FRED.)")
        return 1

    try:
        registered = _registered_series_ids()
    except Exception as e:
        print(f"WARNING: could not read config/fred_series.yaml ({e}) -- "
              f"skipping the registry-gap cross-check, live-fetch check only.")
        registered = None

    failures = []
    unreachable = []
    for series_id, label in TREASURY_TENORS.items():
        ok, msg = _check_one(series_id, api_key)
        status = "PASS" if ok else "FAIL"

        gap_flag = ""
        if registered is not None and series_id not in registered:
            gap_flag = " [NOT in fred_series.yaml -- silently dropped by FREDIngester.run()'s series_filter]"
            unreachable.append(series_id)

        print(f"[{status}] {series_id:14s} ({label:14s}){gap_flag}  {msg}")
        if not ok:
            failures.append(series_id)

    print()
    if unreachable:
        print(
            f"REGISTRY GAP: {len(unreachable)}/13 tenor(s) resolve on live FRED "
            f"but are NOT in config/fred_series.yaml, so TreasuryIngester "
            f"never actually ingests them in production: {unreachable}"
        )
        print(
            "This is independent of PASS/FAIL above -- a tenor can be live-"
            "confirmed here and still never reach Bronze via the real pipeline."
        )
        print()

    if failures:
        print(f"{len(failures)}/13 tenors FAILED live-fetch: {failures}")
        return 1

    print("All 13 yield-curve tenors confirmed live on FRED.")
    if unreachable:
        print(f"({len(unreachable)} of them are still registry-gapped -- see above.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
