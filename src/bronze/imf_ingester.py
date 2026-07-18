"""
imf_ingester.py — Bronze IMF Ingester (GD §3.3.1)
Ingest IMF World Economic Outlook (WEO) global macro data.

Rate limit: Unlimited — no API key required (public data portal)
Cadence: Semi-annual (April + October WEO release)

IMF WEO covers:
    - Real GDP growth per country
    - CPI inflation per country
    - Current Account Balance
    - Government Debt (% GDP)
    - Unemployment rates

Key indicators ingested (via IMF JSON API):
    NGDP_RPCH   — Real GDP growth (% change)
    PCPIPCH     — CPI inflation (% change)
    BCA_NGDPD   — Current Account (% GDP)
    GGXWDG_NGDP — Government debt (% GDP)
    LUR         — Unemployment rate

Countries: USA, CHN, JPN, GBR, DEU, FRA, IND, BRA, CAN, KOR, IDN, AUS

Output: data/bronze/macro/imf/{indicator}_{ts}.parquet
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import polars as pl
import requests
from loguru import logger

from src.bronze.base_ingester import BronzeIngester
from src.bronze.schema_validator import SchemaValidator

IMF_API_BASE = "https://www.imf.org/external/datamapper/api/v1"
# FIX GAP-3 [P1] (Production Readiness Assessment v1.7.2, GD §3.7): IMF was
# one of 6 sources with no Bronze Schema Registry YAML.
SCHEMA_PATH = Path("config/schemas/imf_weo.yaml")

# FIX IMF-3 (MEDIUM): release_date proxy using fixed IMF WEO publication schedule.
# IMF WEO is published in April and October each year (two per year).
# Data for year Y is first published in October of year Y (preliminary forecast)
# and revised in April of year Y+1 (updated World Economic Outlook).
# Using run_date.isoformat() was incorrect because the same observation could get
# different release_dates depending on when the pipeline happened to run.
# Fix: use the earliest WEO publication date that is <= run_date for each obs_year.
# WEO candidate dates: October 1 of obs_year, April 1 of obs_year+1, October 1 of obs_year+1.
# This provides a deterministic, reproducible release_date regardless of run_date.

_IMF_WEO_MONTHS = (
    (10, 1),   # October — primary WEO (same calendar year, preliminary)
    (4,  1),   # April of following year — updated/revised WEO
    (10, 1),   # October of following year — further revision
)


def _imf_weo_release_date(obs_year: int, run_date: date) -> date:
    """
    Return the earliest IMF WEO publication date for obs_year that is <= run_date.
    Falls back to run_date if no candidate precedes it (data not yet published).

    Candidates (in chronological order):
        October 1, obs_year      — first preliminary release for that year's data
        April   1, obs_year + 1  — revised WEO
        October 1, obs_year + 1  — second annual revision
    """
    candidates = [
        date(obs_year,     10, 1),
        date(obs_year + 1,  4, 1),
        date(obs_year + 1, 10, 1),
    ]
    for candidate in candidates:
        if candidate <= run_date:
            return candidate
    # None of the WEO dates have passed yet — data is not yet published from PIT view
    return run_date

IMF_INDICATORS = [
    {"id": "NGDP_RPCH",    "name": "gdp_growth",      "desc": "Real GDP growth %"},
    {"id": "PCPIPCH",      "name": "cpi_inflation",    "desc": "CPI inflation %"},
    {"id": "BCA_NGDPD",    "name": "current_account",  "desc": "Current Account % GDP"},
    {"id": "GGXWDG_NGDP",  "name": "govt_debt",        "desc": "Government Debt % GDP"},
    {"id": "LUR",          "name": "unemployment",     "desc": "Unemployment rate"},
]

# Key economies relevant for macro regime detection
KEY_COUNTRIES = [
    "USA", "CHN", "JPN", "GBR", "DEU",
    "FRA", "IND", "BRA", "CAN", "KOR", "IDN", "AUS",
]


class IMFIngester(BronzeIngester):
    """
    Bronze ingester for IMF World Economic Outlook data.
    No authentication required — IMF data is publicly available.
    """

    def __init__(self) -> None:
        # FIX GAP-3 [P1]: SchemaValidator gate (GD §3.7) — IMF had no
        # SchemaValidator instantiated anywhere prior to imf_weo.yaml existing.
        self._validator = (
            SchemaValidator(SCHEMA_PATH) if SCHEMA_PATH.exists() else None
        )

    def run(self, run_date: date) -> None:
        """Ingest key IMF WEO indicators for major economies."""
        logger.info(
            f"[IMF] Starting WEO ingestion | {len(IMF_INDICATORS)} indicators"
            f" | {len(KEY_COUNTRIES)} countries | run_date={run_date}"
        )

        success = failed = 0
        for spec in IMF_INDICATORS:
            try:
                df = self._fetch_indicator(spec["id"], run_date)
                if df is not None and len(df) > 0:
                    # FIX GAP-3 [P1]: SchemaValidator gate (GD §3.7) before write.
                    if self._validator is not None:
                        ok, errors = self._validator.validate(df, spec["name"])
                        if not ok:
                            self._validator.handle_mismatch(
                                df, errors, spec["name"], on_mismatch="quarantine"
                            )
                            failed += 1
                            time.sleep(1.0)
                            continue
                    self.write_macro(
                        df=df,
                        source="imf",
                        domain="world_economic_outlook",
                        series_id=spec["name"],
                    )
                    logger.info(
                        f"[IMF] {spec['id']} ({spec['name']}): {len(df)} rows"
                    )
                    success += 1
                time.sleep(1.0)    # Polite rate limiting on public endpoint
            except Exception as e:
                logger.error(f"[IMF] Failed {spec['id']}: {e}")
                failed += 1

        logger.info(f"[IMF] Complete: {success} OK, {failed} failed")

    def _fetch_indicator(
        self,
        indicator_id: str,
        run_date: date,
    ) -> Optional[pl.DataFrame]:
        """
        Fetch one IMF WEO indicator for all key countries.
        Uses IMF JSON API: /api/v1/{indicator}/{countries}
        """
        countries_str = ",".join(KEY_COUNTRIES)
        url = f"{IMF_API_BASE}/{indicator_id}/{countries_str}"

        try:
            resp = requests.get(
                url,
                # FIX IMF-1 (HIGH): generate year list dynamically from run_date.
                # Hardcoded "2000,...,2025" stops fetching at 2025 — each new year
                # requires manual code update. Dynamic generation fetches all years
                # from 2000 up to and including the current run_date year.
                params={"periods": ",".join(str(y) for y in range(2000, run_date.year + 1))},
                timeout=30,
                headers={"Accept": "application/json"},
            )

            if resp.status_code != 200:
                logger.warning(f"[IMF] HTTP {resp.status_code} for {indicator_id}")
                return None

            data    = resp.json()
            values  = data.get("values", {}).get(indicator_id, {})

            if not values:
                return None

            records = []
            for country, yearly_data in values.items():
                if country not in KEY_COUNTRIES:
                    continue
                for year_str, value in yearly_data.items():
                    if value is None:
                        continue
                    try:
                        records.append({
                            "series_id":        indicator_id,
                            "country":          country,
                            "observation_date": f"{year_str}-01-01",
                            "value":            float(value),
                            # FIX IMF-3 (MEDIUM): use WEO publication date proxy.
                            # Previously run_date.isoformat() — non-deterministic across runs.
                            # _imf_weo_release_date() returns the earliest WEO date
                            # (Oct of obs_year or Apr of obs_year+1) that is <= run_date.
                            "release_date":     _imf_weo_release_date(
                                int(year_str), run_date
                            ).isoformat(),
                            "source":           "imf_weo",
                        })
                    except (ValueError, TypeError):
                        pass

            return pl.DataFrame(records) if records else None

        except requests.exceptions.Timeout:
            logger.warning(f"[IMF] Timeout fetching {indicator_id}")
            return None
        except Exception as e:
            logger.warning(f"[IMF] Request failed for {indicator_id}: {e}")
            return None

    def get_latest_value(
        self,
        indicator: str,
        country: str = "USA",
    ) -> Optional[float]:
        """
        Utility: get latest ingested value for a specific indicator + country.
        Useful for regime detection inputs.

        FIX IMF-2 (MEDIUM): use parameterized DuckDB query to prevent SQL injection.
        `indicator` and `country` came from internal constants (low immediate risk),
        but f-string interpolation is a code smell and incorrect pattern.
        """
        import duckdb
        pattern = "data/bronze/macro/imf/**/*.parquet"
        try:
            con = duckdb.connect()
            result = con.execute("""
                SELECT value
                FROM read_parquet(?, hive_partitioning=true)
                WHERE series_id = ?
                  AND country   = ?
                ORDER BY observation_date DESC
                LIMIT 1
            """, [pattern, indicator, country]).fetchone()
            return float(result[0]) if result else None
        except Exception:
            return None


def run(run_date: date) -> None:
    """Job entry point."""
    IMFIngester().run(run_date)
