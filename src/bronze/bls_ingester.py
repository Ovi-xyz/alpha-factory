"""
bls_ingester.py — Bronze BLS Ingester (GD §3.3.1)
Ingest BLS data: CPI, PPI, NFP, Unemployment Rate.

Rate limit: 500 req/day — managed conservatively.
API key: BLS_API_KEY dari .env (free registration)
Cadence: Monthly (G5: BLS CPI on day 10-15, BLS NFP first Friday)

Series ingested:
    CPI: CUUR0000SA0, CUUR0000SA0L1E (headline + core)
    PPI: WPU00000000 (headline)
    NFP: CES0000000001 (total nonfarm payroll)
    Unemployment: LNS14000000

Output: data/bronze/macro/bls/{series_id}_{ts}.parquet

Alternative: many BLS series available via FRED (FRED mirror).
FREDIngester handles PAYEMS, CPIAUCSL, UNRATE etc. as fallback.

FIX BLS-2 (HIGH): release_date was run_date.isoformat() for all observations.
    FRED-1 applied RELEASE_LAG_DAYS proxy to fred_ingester.py but bls_ingester.py
    was not updated consistently. Silver MacroProcessor PIT filter (release_date
    <= run_date) was trivially True for all BLS data — no PIT protection.
    Fix: apply the same RELEASE_LAG_DAYS proxy pattern from fred_ingester:
        proxy_release = observation_date + lag_days (clamped to run_date)
    Lag values per BLS series release schedule:
        CPI:          day 10-15 of the following month → ~35 days from month start
        PPI:          same schedule as CPI → ~35 days
        NFP:          first Friday of the following month → ~33 days
        Unemployment: same BLS Employment Situation report as NFP → ~33 days
"""

from __future__ import annotations

import json
import os
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import polars as pl
import requests
from loguru import logger

from src.bronze.base_ingester import BronzeIngester
from src.bronze.schema_validator import SchemaValidator

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
# FIX GAP-3 [P1] (Production Readiness Assessment v1.7.2, GD §3.7): BLS was
# one of 6 sources with no Bronze Schema Registry YAML. Note this only
# gates the native BLS fetch path (_fetch_batch) — the FRED-mirror fallback
# (_run_via_fred_mirror) writes under the FRED domain and is gated by
# fred_macro.yaml instead.
SCHEMA_PATH = Path("config/schemas/bls_macro.yaml")

# FIX BLS-2 (HIGH): release_date proxy via RELEASE_LAG_DAYS per series.
# release_date = observation_date + lag_days, clamped to run_date as ceiling.
# This mirrors the FRED-1 pattern from fred_ingester.py for PIT integrity.
# Lag = approximate calendar days from the START of the reference month/period
# until BLS publicly releases data for that period.
BLS_RELEASE_LAG_DAYS: dict[str, int] = {
    "CUUR0000SA0":    35,   # CPI Headline — released day 10-15 of following month
    "CUUR0000SA0L1E": 35,   # CPI Core (ex food & energy) — same BLS CPI release
    "WPU00000000":    35,   # PPI All Commodities — released day 10-15 following month
    "CES0000000001":  33,   # NFP Total Nonfarm Payroll — first Friday of following month
    "LNS14000000":    33,   # Unemployment Rate — same BLS Employment Situation report as NFP
    "LNS11000000":    33,   # Labor Force Participation — same report as NFP/Unemployment
}
_DEFAULT_BLS_LAG: int = 35   # Conservative default for any unlisted series

BLS_SERIES: dict[str, dict] = {
    "CUUR0000SA0":   {"name": "cpi_headline",     "description": "CPI All Urban Consumers"},
    "CUUR0000SA0L1E": {"name": "cpi_core",         "description": "CPI Core ex food & energy"},
    "WPU00000000":   {"name": "ppi_headline",     "description": "PPI All Commodities"},
    "CES0000000001": {"name": "nfp_total",         "description": "Nonfarm Payroll Total"},
    "LNS14000000":   {"name": "unemployment_rate", "description": "Unemployment Rate"},
    "LNS11000000":   {"name": "labor_force_participation", "description": "Labor Force Participation"},
}


class BLSIngester(BronzeIngester):
    """
    Bronze ingester untuk BLS economic data.
    Falls back to FRED mirror for most series (FRED has daily updates).
    """

    def __init__(self) -> None:
        self._api_key = os.getenv("BLS_API_KEY")
        # FIX GAP-3 [P1]: SchemaValidator gate (GD §3.7) for the native BLS
        # fetch path. FRED-mirror fallback is gated separately by FREDIngester.
        self._validator = (
            SchemaValidator(SCHEMA_PATH) if SCHEMA_PATH.exists() else None
        )

    def run(self, run_date: date, series_filter: Optional[list[str]] = None) -> None:
        """
        Ingest BLS series.
        If BLS_API_KEY not set, logs guidance to use FRED mirror instead.
        """
        if not self._api_key:
            logger.warning(
                "[BLS] BLS_API_KEY not set. Most BLS series are available"
                " via FRED (CPIAUCSL, PAYEMS, UNRATE) — FREDIngester handles these."
                " Register free at https://data.bls.gov/registrationEngine/"
            )
            # FRED mirror covers the most important BLS series
            self._run_via_fred_mirror(run_date, series_filter)
            return

        series_list = list(series_filter or BLS_SERIES.keys())
        logger.info(
            f"[BLS] Starting ingestion | {len(series_list)} series"
            f" | run_date={run_date}"
        )

        # BLS API supports batch of up to 25 series per request
        batch_size = 25
        for i in range(0, len(series_list), batch_size):
            batch = series_list[i:i + batch_size]
            self._fetch_batch(batch, run_date)
            time.sleep(2)   # Conservative throttle

    def _fetch_batch(self, series_ids: list[str], run_date: date) -> None:
        """Fetch a batch of series IDs in one BLS API request."""
        start_year = str(run_date.year - 5)
        end_year   = str(run_date.year)

        payload = {
            "seriesid":       series_ids,
            "startyear":      start_year,
            "endyear":        end_year,
            "registrationkey": self._api_key,
            "calculations":   False,
            "annualaverage":  False,
        }

        try:
            resp = requests.post(
                BLS_API_URL,
                data=json.dumps(payload),
                headers={"Content-type": "application/json"},
                timeout=30,
            )
            if resp.status_code != 200:
                logger.warning(f"[BLS] HTTP {resp.status_code}")
                return

            data = resp.json()
            if data.get("status") != "REQUEST_SUCCEEDED":
                logger.warning(f"[BLS] API status: {data.get('status')}")
                return

            for series in data.get("Results", {}).get("series", []):
                series_id = series.get("seriesID", "")
                rows = []
                for item in series.get("data", []):
                    try:
                        # FIX BLS-1 (CRITICAL): use item['period'] (e.g. 'M01')
                        # NOT item['periodName'][:3] (e.g. 'Jan') which produces
                        # "2026-Jan-01" — an invalid ISO date that crashes Silver
                        # MacroProcessor filter and DuckDB date comparisons.
                        period_code = item.get("period", "")
                        if period_code.startswith("M"):
                            month_num = int(period_code[1:])  # 'M01'→1 … 'M12'→12
                            if not 1 <= month_num <= 12:
                                # Skip annual/semi-annual (M13, A01, S01, etc.)
                                continue
                            obs_date = f"{item['year']}-{month_num:02d}-01"
                        elif period_code.startswith("Q"):
                            # Quarterly data — convert 'Q1'→'01', 'Q2'→'04', etc.
                            q_num = int(period_code[1:])
                            obs_date = f"{item['year']}-{(q_num-1)*3+1:02d}-01"
                        elif period_code.startswith("A"):
                            # Annual average — map to Jan 1st of the year
                            obs_date = f"{item['year']}-01-01"
                        else:
                            # Unknown period format — skip rather than store garbage
                            logger.debug(
                                f"[BLS] {series_id}: unknown period format "
                                f"{period_code!r} — skipping row"
                            )
                            continue

                        rows.append({
                            "series_id":        series_id,
                            "observation_date": obs_date,
                            "value":            float(item.get("value", 0) or 0),
                            "period":           period_code,
                            "year":             int(item.get("year", 0)),
                            # FIX BLS-2 (HIGH): release_date via RELEASE_LAG_DAYS proxy.
                            # Previously: run_date.isoformat() — PIT filter trivially True.
                            # Now: observation_date + lag_days, clamped to run_date.
                            # Mirrors FRED-1 pattern from fred_ingester.py.
                            "release_date":     min(
                                date.fromisoformat(obs_date)
                                + timedelta(days=BLS_RELEASE_LAG_DAYS.get(
                                    series_id, _DEFAULT_BLS_LAG
                                )),
                                run_date,
                            ).isoformat(),
                        })
                    except (ValueError, KeyError):
                        pass

                if rows:
                    df = pl.DataFrame(rows)
                    spec = BLS_SERIES.get(series_id, {"name": series_id})
                    # FIX GAP-3 [P1]: SchemaValidator gate (GD §3.7) before write.
                    if self._validator is not None:
                        ok, errors = self._validator.validate(df, series_id)
                        if not ok:
                            self._validator.handle_mismatch(
                                df, errors, series_id, on_mismatch="quarantine"
                            )
                            continue
                    self.write_macro(
                        df=df,
                        source="bls",
                        domain="labor_market",
                        series_id=spec["name"],
                    )
                    logger.debug(
                        f"[BLS] {series_id} ({spec['name']}): {len(rows)} obs"
                    )

        except Exception as e:
            logger.error(f"[BLS] Batch request failed: {e}")

    def _run_via_fred_mirror(
        self,
        run_date: date,
        series_filter: Optional[list[str]] = None,
    ) -> None:
        """
        Fetch BLS data via FRED mirror series (available without BLS key).
        FRED provides: CPIAUCSL, CPILFESL, PAYEMS, UNRATE, ICSA, etc.
        """
        fred_mirror_map = {
            "CPI":          ["CPIAUCSL", "CPILFESL"],
            "NFP":          ["PAYEMS", "ICSA"],
            "UNEMPLOYMENT": ["UNRATE", "U6RATE", "CIVPART"],
            "PPI":          ["PPIFIS"],
        }

        if os.getenv("FRED_API_KEY"):
            from src.bronze.fred_ingester import FREDIngester
            all_fred_series = []
            for series_group in fred_mirror_map.values():
                all_fred_series.extend(series_group)
            FREDIngester().run(run_date, series_filter=all_fred_series)
            logger.info("[BLS] Used FRED mirror for BLS series")
        else:
            logger.warning(
                "[BLS] Neither BLS_API_KEY nor FRED_API_KEY set."
                " BLS data unavailable. Set at least FRED_API_KEY for mirror access."
            )


def run(run_date: date) -> None:
    """Job entry point."""
    BLSIngester().run(run_date)
