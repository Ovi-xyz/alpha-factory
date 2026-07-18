"""
fred_ingester.py — Bronze FRED Ingester (GD §3.3.1)
Ingest FRED economic series ke Bronze layer.

Rate limit: 120 req/min — managed via sleep throttle.
API key: FRED_API_KEY dari .env
Cadence: weekly (Sunday) untuk semua series, daily untuk high-freq (T10Y2Y, VIXCLS)

Output: data/bronze/macro/fred/{domain}/{series_id}_{ts}.parquet

PIT integrity: menyimpan release_date (ingestion date) sebagai metadata.
Silver MacroProcessor yang akan compute vintage_date dan detect revisions.
"""

from __future__ import annotations

import os
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import polars as pl
import yaml
from loguru import logger

from src.bronze.base_ingester import BronzeIngester
from src.bronze.schema_validator import SchemaValidator

FRED_REGISTRY_PATH  = Path("config/fred_series.yaml")
BRONZE_FRED_PATH    = Path("data/bronze/macro/fred")
THROTTLE_SECONDS    = 0.6       # ~100 req/min — safe under 120 limit
MAX_HISTORY_YEARS   = 30        # Pull 30Y of history for new series
# FIX GAP-3 [P1] (Production Readiness Assessment v1.7.2, GD §3.7): FRED was
# one of 6 sources with no Bronze Schema Registry YAML — SchemaValidator
# could never be instantiated, so the quarantine gate never fired for FRED.
SCHEMA_PATH = Path("config/schemas/fred_macro.yaml")

# FIX FRED-1 (HIGH): release_date lag proxies per series.
# FRED stores data with actual publication lag: CPI publishes ~35 days after
# period end, NFP ~40 days, etc. Storing run_date as release_date makes the
# S-F02 PIT filter (release_date <= run_date) trivially True — no protection.
# These lags approximate the typical FRED first-release publication delay.
# Source: FRED release calendars + St. Louis Fed documentation.
# Conservative: overestimates lag (earlier release_date) → safer for PIT.
RELEASE_LAG_DAYS: dict[str, int] = {
    # Inflation
    "CPIAUCSL": 35,  "CPILFESL": 35,
    "PCEPI":    35,  "PCEPILFE": 35,
    "PPIFIS":   25,  "PPIFGS":   25,  "PPIACO": 25,
    # Labor
    "PAYEMS":   40,  "ICSA":      7,  "CCSA":    7,
    "UNRATE":   40,  "U6RATE":   40,  "CIVPART": 40,
    "CES0500000003": 40,  "JTSJOL": 45,
    # GDP / Output
    "A191RL1Q225SBEA": 30,  "GDPC1": 30,
    "INDPRO": 20,  "TCU": 20,
    "RSAFS": 20,   "RSXFS": 20,
    "DGORDER": 25, "NEWORDER": 25,
    "NAPM": 3,     "NMFCI": 3,
    # Monetary / Rates (daily series — minimal lag)
    "FEDFUNDS": 3,  "DFF": 1,
    "T10Y2Y": 1,    "T10Y3M": 1,
    "DGS2": 1,      "DGS5": 1,   "DGS10": 1,  "DGS30": 1,
    "MORTGAGE30US": 3,  "IORB": 1,  "EFFR": 1,  "SOFR": 1,
    # Credit / Financial
    "BAMLH0A0HYM2": 1,  "BAMLC0A0CM": 1,
    "NFCI": 3,      "STLFSI4": 3,
    "DCOILWTICO": 1,  "GOLDAMGBD228NLBM": 1,
    "DEXUSEU": 1,   "DEXJPUS": 1,
    "M2SL": 10,     "WALCL": 5,
    "VIXCLS": 1,
    # Housing / Consumer
    "HOUST": 20,  "PERMIT": 20,  "HSN1F": 30,
    "EXHOSLUSM495S": 25,
    "UMCSENT": 3,  "CSCICP03USM665S": 5,
    "PSAVERT": 30,  "PCE": 30,
    "DRSFRMACBS": 45,  "TOTALSL": 30,
}
_DEFAULT_RELEASE_LAG = 7  # conservative default for unknown series


class FREDIngester(BronzeIngester):
    """
    Bronze ingester untuk FRED economic series.
    Supports incremental fetch: hanya ambil data yang belum ada.
    """

    def __init__(self) -> None:
        self._registry = self._load_registry()
        self._api_key  = os.getenv("FRED_API_KEY")
        # FIX GAP-3 [P1]: SchemaValidator gate (GD §3.7) — previously not
        # instantiated anywhere for FRED since fred_macro.yaml didn't exist.
        self._validator = (
            SchemaValidator(SCHEMA_PATH) if SCHEMA_PATH.exists() else None
        )

    def run(self, run_date: date, series_filter: Optional[list[str]] = None) -> None:
        """
        Ingest semua FRED series (atau subset via series_filter).

        FIX FRED-2 (HIGH): build last_known_cache via a SINGLE bulk DuckDB scan
        across all existing FRED Bronze files before the fetch loop begins.
        Previously: one scan per series (60 scans × growing files/year → slow).
        Now: one scan → dict {series_id: max_observation_date}.
        """
        if not self._api_key:
            logger.warning(
                "[FRED] FRED_API_KEY not set — skipping FRED ingestion."
                " Set FRED_API_KEY in .env to enable."
            )
            return

        series_list = self._registry.get("series", [])
        if series_filter:
            series_list = [s for s in series_list if s["id"] in series_filter]

        logger.info(
            f"[FRED] Starting ingestion: {len(series_list)} series | run_date={run_date}"
        )

        # FIX FRED-2: single bulk scan — replaces 60 per-series DuckDB scans
        last_known_cache = self._build_last_known_cache()
        logger.debug(
            f"[FRED] last_known_cache: {len(last_known_cache)} series with existing data"
        )

        success = failed = 0
        for spec in series_list:
            series_id = spec["id"]
            domain    = spec.get("domain", "other")
            cadence   = spec.get("cadence", "weekly")

            if cadence == "daily" and run_date.weekday() not in range(5):
                logger.debug(f"[FRED] Skipping daily series {series_id} — not weekday")
                continue

            try:
                df = self._fetch_series(
                    series_id, run_date, last_known_cache.get(series_id)
                )
                if df is not None and len(df) > 0:
                    # FIX GAP-3 [P1]: SchemaValidator gate (GD §3.7) before write.
                    if self._validator is not None:
                        ok, errors = self._validator.validate(df, series_id)
                        if not ok:
                            self._validator.handle_mismatch(
                                df, errors, series_id, on_mismatch="quarantine"
                            )
                            failed += 1
                            time.sleep(THROTTLE_SECONDS)
                            continue
                    self.write_macro(
                        df=df,
                        source="fred",
                        domain=domain,
                        series_id=series_id,
                    )
                    success += 1
                else:
                    logger.debug(f"[FRED] No data for {series_id}")
                time.sleep(THROTTLE_SECONDS)
            except Exception as e:
                logger.error(f"[FRED] Failed {series_id}: {e}")
                failed += 1

        logger.info(f"[FRED] Complete: {success} OK, {failed} failed")

    def _fetch_series(
        self,
        series_id: str,
        run_date: date,
        last_known: Optional[date] = None,   # FIX FRED-2: passed in from cache
    ) -> Optional[pl.DataFrame]:
        """Fetch one FRED series, incrementally from last known date.

        FIX FRED-1 (HIGH): release_date is now set to observation_date + lag_days
        instead of run_date. This gives a meaningful proxy for when each data
        point was first published, enabling the S-F02 PIT filter in Silver
        MacroProcessor to correctly exclude future releases.

        The lag is conservative (overestimate) — earlier release_date is safer
        for PIT integrity than understating the lag. Clamped to run_date as ceiling
        to prevent release_date > run_date (which would make PIT filter drop everything).
        """
        try:
            import fredapi  # type: ignore
            fred = fredapi.Fred(api_key=self._api_key)
        except ImportError:
            logger.error("[FRED] fredapi not installed. Run: pip install fredapi")
            return None

        # FIX FRED-2: use pre-built cache instead of per-series scan
        start_date = last_known or (
            run_date - timedelta(days=365 * MAX_HISTORY_YEARS)
        )

        try:
            series = fred.get_series(
                series_id,
                observation_start=start_date.isoformat(),
                observation_end=run_date.isoformat(),
            )
        except Exception as e:
            logger.warning(f"[FRED] API error for {series_id}: {e}")
            return None

        if series is None or series.empty:
            return None

        # FIX FRED-1: compute release_date per observation using lag proxy.
        # lag_days = typical publication delay for this series.
        # release_date = min(obs_date + lag_days, run_date)
        # Clamped to run_date: never store a release_date in the future.
        lag_days = RELEASE_LAG_DAYS.get(series_id, _DEFAULT_RELEASE_LAG)

        import pandas as pd
        obs_dates = series.index.date

        release_dates = [
            min(od + timedelta(days=lag_days), run_date).isoformat()
            for od in obs_dates
        ]

        df = pd.DataFrame({
            "observation_date": obs_dates,
            "value":            series.values,
            "series_id":        series_id,
            "release_date":     release_dates,  # FIX FRED-1: per-obs lag-based proxy
        })
        return pl.from_pandas(df).with_columns([
            pl.col("value").cast(pl.Float64),
        ])

    def _build_last_known_cache(self) -> dict[str, date]:
        """
        FIX FRED-2 (HIGH): Single DuckDB scan across ALL FRED Bronze files.
        Returns {series_id: max_observation_date} for all series with existing data.

        Replaces _last_known_date() which ran one scan per series:
          60 series × growing parquet files = O(n_series × n_files) per weekly run.
        Single scan: O(n_files total) — same result, far fewer filesystem opens.
        """
        pattern = str(BRONZE_FRED_PATH / "**" / "*.parquet")
        try:
            import duckdb
            con = duckdb.connect()
            rows = con.execute(
                """
                SELECT series_id, MAX(observation_date) AS last_date
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE observation_date IS NOT NULL
                GROUP BY series_id
                """,  # FIX BRZ-SQL-001: $name parameterized (GD §17.7)
                {"glob": pattern},
            ).fetchall()
            result: dict[str, date] = {}
            for series_id, last_raw in rows:
                try:
                    d = date.fromisoformat(str(last_raw)[:10])
                    result[series_id] = d
                except (ValueError, TypeError):
                    pass
            return result
        except Exception:
            return {}  # No existing data — all series start from scratch

    @staticmethod
    def _load_registry() -> dict:
        """Load fred_series.yaml registry."""
        if FRED_REGISTRY_PATH.exists():
            return yaml.safe_load(FRED_REGISTRY_PATH.read_text())
        logger.warning(f"[FRED] Registry not found: {FRED_REGISTRY_PATH}")
        return {}

    def get_regime_series(self) -> list[str]:
        """Return series IDs yang digunakan sebagai macro regime inputs."""
        return [
            s["id"]
            for s in self._registry.get("series", [])
            if s.get("regime_input", False)
        ]


def run(run_date: date) -> None:
    """Job entry point untuk job_registry."""
    FREDIngester().run(run_date)
