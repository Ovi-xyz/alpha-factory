"""
scripts/preflight/check_fred_series.py

ADD (Ovi, this thread — 14 Aug 2026): "start with this sequent — FRED, EIA,
BLS, BEA, US Treasury, IMF, Finnhub, Polygon, AlphaVantage" preflight pass.
Same authoring/execution split as every other script in this directory
(ADR-025, GMI_Decision_Document_v2.docx): authored now, executed later on
network-enabled hardware — this sandbox has no route to api.stlouisfed.org.

Closes a real coverage gap: check_fred_commodity_series.py (RISK-15, 8 Aug
2026) only ever checked the 6 `domain: commodity` Track 2 series added that
thread. The other 62 series in config/fred_series.yaml — monetary_policy
(18), inflation (9), growth (8), labor (8), credit (9), housing (9),
volatility (1, VIXCLS) — have never had a live preflight check at all,
including the 7 series wired into macro_regime.py's regime computation via
`regime_inputs:` (VIXCLS, T10Y2Y, CPIAUCSL, A191RL1Q225SBEA, DEXUSEU, NFCI,
BAMLH0A0HYM2) — the single highest-consequence subset in the whole FRED
registry, since a silent FRED schema/ID change there degrades regime
detection with no error, not just a missing Bronze file.

Unlike check_bis_cbpol_d.py / check_fred_commodity_series.py, this script
reads config/fred_series.yaml directly rather than duplicating the list
inline — 62 series is impractical to hand-duplicate, and (unlike the BIS
REF_AREA map or the 6 commodity IDs, both authored in the same thread as
their check) fred_series.yaml is long-standing, already-live config, not
something this check needs to independently re-derive. This mirrors
check_yfinance_tickers.py's default mode (reads InstrumentLoader, not a
hand-copied instrument list).

Usage:
    python scripts/preflight/check_fred_series.py
    python scripts/preflight/check_fred_series.py --domain labor
    python scripts/preflight/check_fred_series.py --series VIXCLS
    python scripts/preflight/check_fred_series.py --regime-only
    python scripts/preflight/check_fred_series.py --include-commodity

Exit code 0 = every checked series returns real, usable FRED observations.
Exit code 1 = FRED_API_KEY missing, or at least one series failed.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Same pattern as every other script in this directory — python-dotenv is a
# declared dependency but is never invoked automatically; os.getenv() only
# sees variables the shell has separately exported without this.
from dotenv import load_dotenv
load_dotenv()

FRED_OBSERVATIONS_ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"
FRED_REGISTRY_PATH = Path(__file__).resolve().parents[2] / "config" / "fred_series.yaml"

# The 7 series feeding macro_regime.py's regime computation directly
# (config/fred_series.yaml's own `regime_inputs:` block) — flagged in
# output, not treated as a separate check, since they're a subset of
# whatever --domain/--series filter is already active.
REGIME_INPUT_SERIES = frozenset({
    "VIXCLS", "T10Y2Y", "CPIAUCSL", "A191RL1Q225SBEA",
    "DEXUSEU", "NFCI", "BAMLH0A0HYM2",
})


def _load_registry() -> list[dict]:
    import yaml
    data = yaml.safe_load(FRED_REGISTRY_PATH.read_text())
    return data.get("series", [])


def _fetch_observations(series_id: str, api_key: str) -> dict:
    import httpx
    resp = httpx.get(
        FRED_OBSERVATIONS_ENDPOINT,
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 5,  # existence/freshness check, not a full historical pull
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
    # FRED uses the literal string "." to mark a missing observation for a
    # given period — distinct from an empty response entirely (same
    # convention as check_fred_commodity_series.py).
    real_obs = [o for o in obs if o.get("value") not in (None, ".", "")]
    if not real_obs:
        return False, (
            f"0 usable observations in response "
            f"({len(obs)} rows total, possibly all '.' missing-markers)"
        )

    latest = real_obs[0]  # sort_order=desc -> first row is most recent
    return True, f"OK -- latest={latest['date']} value={latest['value']}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--domain", default=None,
        help="Only check one domain (monetary_policy/inflation/growth/labor/"
             "credit/housing/volatility)",
    )
    parser.add_argument("--series", default=None, help="Only check one series ID (e.g. VIXCLS)")
    parser.add_argument(
        "--regime-only", action="store_true",
        help="Only check the 7 series feeding macro_regime.py's regime_inputs",
    )
    parser.add_argument(
        "--include-commodity", action="store_true",
        help="Also check domain=commodity (normally left to "
             "check_fred_commodity_series.py — use that script instead "
             "unless you specifically want a single combined run)",
    )
    args = parser.parse_args()

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("FAIL: FRED_API_KEY not set in .env -- cannot check live FRED.")
        return 1

    registry = _load_registry()
    if not args.include_commodity:
        registry = [s for s in registry if s.get("domain") != "commodity"]

    if args.regime_only:
        registry = [s for s in registry if s["id"] in REGIME_INPUT_SERIES]
    elif args.series:
        registry = [s for s in registry if s["id"] == args.series]
        if not registry:
            print(f"No series {args.series!r} found in fred_series.yaml.")
            return 1
    elif args.domain:
        registry = [s for s in registry if s.get("domain") == args.domain]
        if not registry:
            print(f"No series found for domain {args.domain!r}.")
            return 1

    if not registry:
        print("No matching series found for the given filter.")
        return 1

    failures = []
    for spec in sorted(registry, key=lambda s: (s.get("domain", ""), s["id"])):
        series_id = spec["id"]
        flag = " [regime_input]" if series_id in REGIME_INPUT_SERIES else ""
        ok, msg = _check_one(series_id, api_key)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {series_id:16s} ({spec.get('domain', '?'):16s}){flag}  {msg}")
        if not ok:
            failures.append(series_id)

    print()
    if failures:
        print(f"{len(failures)}/{len(registry)} series FAILED: {failures}")
        return 1

    print(f"All {len(registry)} FRED series confirmed live.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
