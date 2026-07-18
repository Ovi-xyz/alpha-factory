"""
scripts/preflight/check_bis_cbpol_d.py

ADD ADR-025 (GMI_Decision_Document_v2.docx, 2026-07-11) — see
check_yfinance_tickers.py's module docstring for the shared rationale
(authored now, executed later on network-enabled hardware/CI).

Confirms, empirically, that BIS's WS_CBPOL_D dataset actually returns
DAILY-resolution TIME_PERIOD values (not monthly) for all 12 non-FED
REF_AREA codes registered in config/bis_cb_rates.yaml — closing
Data Source & Rates Adjustment v1.0 §13's Pre-Wave-1-Gate checklist items
1/2, which GMI_Implementation_Checkpoint.docx / v2 / v3 all carried
forward as still-open ("Not investigated ... unrelated to this thread's
scope" / "Not tested this thread — no network access ... from this
sandbox" applies identically to BIS).

Usage:
    python scripts/preflight/check_bis_cbpol_d.py

Exit code 0 = all 12 REF_AREA codes present with daily-resolution data.
Exit code 1 = at least one REF_AREA missing or not daily-resolution.
"""

from __future__ import annotations

import csv
import io
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BIS_ENDPOINT = "https://stats.bis.org/api/v1/data/WS_CBPOL_D/all"

# config/bis_cb_rates.yaml's ref_area_map, duplicated here deliberately
# (not imported) so this pre-flight check does not silently pass if the
# config file itself is ever corrupted/emptied -- a genuinely independent
# check of "does BIS still cover the 12 REF_AREA codes we depend on."
EXPECTED_REF_AREAS = {
    "XM": "ECB", "GB": "BOE", "JP": "BOJ", "CA": "BOC", "AU": "RBA",
    "NZ": "RBNZ", "CH": "SNB", "KR": "BOK", "NO": "NORGES",
    "SE": "RIKSBANK", "CN": "PBOC", "ID": "BI",
}


def _fetch_csv() -> str:
    import httpx
    resp = httpx.get(BIS_ENDPOINT, params={"format": "csv"}, timeout=30.0)
    resp.raise_for_status()
    return resp.text


def _daily_resolution(dates_sorted: list) -> bool:
    """True if consecutive observation dates are ~1 day apart on average
    (allowing for weekends) rather than ~30 days apart (monthly)."""
    if len(dates_sorted) < 2:
        return False
    gaps = [
        (dates_sorted[i + 1] - dates_sorted[i]).days
        for i in range(len(dates_sorted) - 1)
    ]
    avg_gap = sum(gaps) / len(gaps)
    return avg_gap <= 4.0  # daily/weekday data has small avg gaps; monthly is ~30


def main() -> int:
    try:
        raw_csv = _fetch_csv()
    except Exception as e:
        print(f"FAILED to fetch BIS CBPOL_D endpoint: {e}")
        print(f"Endpoint: {BIS_ENDPOINT}?format=csv")
        return 1

    reader = csv.DictReader(io.StringIO(raw_csv))
    by_ref_area = {k: [] for k in EXPECTED_REF_AREAS}

    for row in reader:
        ref_area = row.get("REF_AREA")
        time_period = row.get("TIME_PERIOD")
        if ref_area not in by_ref_area or not time_period:
            continue
        try:
            by_ref_area[ref_area].append(date.fromisoformat(time_period))
        except ValueError:
            continue

    failures = []
    for code, cb_name in sorted(EXPECTED_REF_AREAS.items()):
        obs = sorted(by_ref_area[code])
        if not obs:
            print(f"[FAIL] {code} ({cb_name}): 0 observations found")
            failures.append(code)
            continue
        recent = [d for d in obs if d >= date.today() - timedelta(days=90)]
        is_daily = _daily_resolution(recent if len(recent) >= 2 else obs[-30:])
        status = "PASS" if is_daily else "FAIL"
        print(
            f"[{status}] {code} ({cb_name}): {len(obs)} total obs, "
            f"latest={obs[-1]}, daily-resolution={is_daily}"
        )
        if not is_daily:
            failures.append(code)

    print()
    if failures:
        print(f"{len(failures)}/12 REF_AREA codes FAILED: {failures}")
        return 1

    print("All 12 REF_AREA codes confirmed daily-resolution.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
