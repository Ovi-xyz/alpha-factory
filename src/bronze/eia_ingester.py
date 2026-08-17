"""
eia_ingester.py — Bronze EIA Ingester (GD §3.3.3)
Ingest EIA crude oil inventory, production, refinery data.

Rate limit: Unlimited (EIA is generous)
API key: EIA_API_KEY dari .env
Cadence: Weekly Wednesday (G5 schedule constraint: run_on_weekdays=[2])

Series ingested:
    PET.WCRSTUS1.W  — US Crude Oil Stocks
    PET.WCRFPUS2.W  — US Crude Oil Production
    PET.WGIRIUS2.W  — US Crude Oil Refinery Input
    PET.RWTC.W      — WTI Crude Oil Spot Price (weekly)

Output: data/bronze/commodity/eia/{series_id}_{ts}.parquet
"""

from __future__ import annotations

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

EIA_BASE_URL = "https://api.eia.gov/v2/seriesid/{series_id}"
# FIX GAP-3 [P1] (Production Readiness Assessment v1.7.2, GD §3.7): EIA was
# one of 6 sources with no Bronze Schema Registry YAML.
SCHEMA_PATH = Path("config/schemas/eia_oil.yaml")
EIA_SERIES = [
    {"id": "PET.WCRSTUS1.W",  "name": "us_crude_stocks",      "unit": "thousand_barrels"},
    {"id": "PET.WCRFPUS2.W",  "name": "us_crude_production",  "unit": "thousand_barrels_day"},
    {"id": "PET.WGIRIUS2.W",  "name": "us_refinery_input",    "unit": "thousand_barrels_day"},
    {"id": "PET.RWTC.W",      "name": "wti_spot_price",       "unit": "dollars_per_barrel"},
]


class EIAIngester(BronzeIngester):
    """Bronze ingester untuk EIA oil/gas data. Weekly cadence (Wednesday)."""

    def __init__(self) -> None:
        self._api_key = os.getenv("EIA_API_KEY")
        # FIX GAP-3 [P1]: SchemaValidator gate (GD §3.7) — previously not
        # instantiated anywhere for EIA since eia_oil.yaml didn't exist.
        self._validator = (
            SchemaValidator(SCHEMA_PATH) if SCHEMA_PATH.exists() else None
        )

    def run(self, run_date: date) -> None:
        """Ingest all EIA series."""
        if not self._api_key:
            logger.warning(
                "[EIA] EIA_API_KEY not set — APIv2 documents api_key as "
                "required on every route (including /seriesid/); "
                "attempting request anyway, expect failure"
            )

        logger.info(f"[EIA] Starting ingestion | run_date={run_date}")
        success = failed = 0

        # FIX EIA-2: build last_known_cache once before loop (avoid repeated scans)
        last_known_cache = self._build_last_known_cache()

        for spec in EIA_SERIES:
            try:
                df = self._fetch_series(
                    spec["id"], run_date,
                    # FIX EIA-4 (CRITICAL): _build_last_known_cache() keys by series_id
                    # (e.g. 'PET.WCRSTUS1.W'), not spec['name'] ('us_crude_stocks').
                    # Using spec['name'] always returned None → full 5-year history
                    # fetched every Wednesday. Fix: use spec['id'] to match cache key.
                    last_known=last_known_cache.get(spec["id"])
                )
                if df is not None and len(df) > 0:
                    df = df.with_columns([
                        pl.lit(spec["name"]).alias("series_name"),
                        pl.lit(spec["unit"]).alias("unit"),
                    ])
                    # FIX GAP-3 [P1]: SchemaValidator gate (GD §3.7) before write.
                    if self._validator is not None:
                        ok, errors = self._validator.validate(df, spec["name"])
                        if not ok:
                            self._validator.handle_mismatch(
                                df, errors, spec["name"], on_mismatch="quarantine"
                            )
                            failed += 1
                            time.sleep(0.5)
                            continue
                    self.write_macro(
                        df=df,
                        source="eia",
                        domain="crude_oil",
                        series_id=spec["name"],
                    )
                    success += 1
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"[EIA] Failed {spec['id']}: {e}")
                failed += 1

        logger.info(f"[EIA] Complete: {success} OK, {failed} failed")

    def _fetch_series(
        self,
        series_id: str,
        run_date: date,
        last_known: Optional[date] = None,
    ) -> Optional[pl.DataFrame]:
        """Fetch EIA series via REST API.

        FIX ADR-038 (GMI_Decision_Document_v9.docx, 14 Aug 2026): FIX EIA-1's
        v1 legacy endpoint (api.eia.gov/series/) was confirmed dead —
        check_eia_series.py's first-ever live run (14 Aug 2026) returned
        HTTP 404 on all 4 series, both batched and isolated, with no
        "EIA_API_KEY not set" note printed (not an auth gap). EIA's own
        documentation confirms APIv1 was fully discontinued November 2022 —
        every Wednesday bronze_eia run since deployment had been silently
        writing zero rows for ~3.75 years. FIX EIA-1's original rationale
        ("v2's category-based paths vary per dataset") is void for this
        specific route: migrated to APIv2's /v2/seriesid/{id}
        backward-compatibility route, which accepts the same legacy
        v1-style series IDs (PET.WCRSTUS1.W etc.) directly — the
        minimal-surface-area fix, not the larger v2 category/facet
        redesign FIX EIA-1 was originally avoiding.

        FIX EIA-2 (MEDIUM): use last_known date for incremental fetch instead of
        always downloading 5 years (260 rows) every Wednesday. EIA weekly data
        grows ~52 rows/year — incremental saves 260→1 row reads on subsequent runs.
        """
        # FIX EIA-2: incremental start date — only fetch new data
        if last_known is not None:
            # EIA releases Wednesday; add 14-day lookback buffer for revisions
            start = last_known - timedelta(days=14)
        else:
            start = run_date - timedelta(days=365 * 5)  # full history on first run

        # FIX ADR-038: APIv2 seriesid backward-compat route. series_id is
        # part of the URL path itself (not a query param, unlike v1) — same
        # legacy-style IDs as before, e.g. "PET.RWTC.W".
        # v2 format: https://api.eia.gov/v2/seriesid/PET.RWTC.W?api_key=KEY&start=...&end=...
        params: dict = {
            "start": start.isoformat(),
            "end":   run_date.isoformat(),
        }
        if self._api_key:
            params["api_key"] = self._api_key
        # NOTE ADR-038: unlike v1's permissive unauthenticated access, EIA's
        # own APIv2 documentation lists api_key as required on every route
        # including /seriesid/ — a request without one is expected to fail.
        # Kept as a soft warning in run() rather than a hard return here,
        # matching this ingester's pre-existing missing-key behavior.

        url = EIA_BASE_URL.format(series_id=series_id)

        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                logger.warning(
                    f"[EIA] HTTP {resp.status_code} for {series_id}"
                )
                return None

            data = resp.json()

            # FIX ADR-038: v2's response envelope is
            # {"response": {"data": [...]}, "request": {...}, "apiVersion": ...}
            # — distinct from v1's {"series": [{"data": [[period, value], ...]}]}.
            # Each row in response.data is a dict (field set varies by
            # dataset — duoarea/product/series-description/etc. for
            # petroleum series) but "period" and "value" are always present.
            # Confirmed against EIA's own APIv2 documentation and published
            # response examples this thread — this sandbox has no network
            # route to api.eia.gov to confirm against a live response for
            # these specific 4 series. Flagged pending live confirmation
            # (checklist item 4, GMI_Decision_Document_v9.docx §3), same
            # discipline as ADR-040's T10105 LineNumber.
            response_obj = data.get("response")
            if response_obj is None:
                err = data.get("error")
                logger.debug(
                    f"[EIA] No 'response' envelope for {series_id}"
                    f"{f' -- {err}' if err else ''}"
                )
                return None

            raw_rows = response_obj.get("data", [])
            if not raw_rows:
                return None

            records = []
            for row in raw_rows:
                try:
                    period = str(row.get("period"))
                    value  = row.get("value")
                    if value is None:
                        continue
                    # Normalize period to ISO date format
                    if len(period) == 8 and period.isdigit():
                        period = f"{period[:4]}-{period[4:6]}-{period[6:8]}"
                    records.append({
                        "observation_date": period,
                        "value":            float(value),
                        "series_id":        series_id,
                        # EIA-3 NOTE: release_date = run_date is a deliberate exception
                        # to the RELEASE_LAG_DAYS pattern applied to BLS/BEA/FRED.
                        # EIA crude oil inventory (PET.* series) is released every
                        # Wednesday at 10:30am ET — the same day the pipeline runs
                        # (G5: run_on_weekdays=[2]). run_date is therefore an accurate
                        # proxy for the actual release date, unlike BLS/BEA where data
                        # lags 30-120 days behind the observation period.
                        # No lag adjustment needed: PIT filter (release_date <= run_date)
                        # is correctly satisfied because data IS available on run_date.
                        "release_date":     run_date.isoformat(),
                    })
                except (AttributeError, ValueError, TypeError):
                    pass

            return pl.DataFrame(records) if records else None

        except Exception as e:
            logger.warning(f"[EIA] Request failed for {series_id}: {e}")
            return None

    def _build_last_known_cache(self) -> dict[str, date]:
        """FIX EIA-2: single scan to find last observation_date per EIA series."""
        # FIX EIA-5: was a hardcoded literal "data/bronze/commodity/eia/**"
        # — matching neither self.BASE_PATH (ignored entirely, breaking
        # test isolation and any deployment where BASE_PATH != the
        # default) nor the domain write_macro() actually uses for this
        # ingester ("macro/eia/crude_oil/", not "commodity/eia/"). The
        # glob therefore never matched any real file this ingester ever
        # wrote, so the cache was always {} and every run silently used
        # the full 5-year lookback (see FIX EIA-4's own key-mismatch fix
        # above, which corrected how the cache is READ but not this —
        # the cache was never actually populated in the first place).
        pattern = str(self.BASE_PATH / "macro" / "eia" / "crude_oil" / "**" / "*.parquet")
        cache: dict[str, date] = {}
        try:
            import duckdb
            con  = duckdb.connect()
            rows = con.execute(
                """
                SELECT series_id, MAX(observation_date) AS last_date
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE observation_date IS NOT NULL
                GROUP BY series_id
                """,  # FIX BRZ-SQL-001: $name parameterized (GD §17.7)
                {"glob": pattern},
            ).fetchall()
            for sid, last_raw in rows:
                try:
                    cache[sid] = date.fromisoformat(str(last_raw)[:10])
                except (ValueError, TypeError):
                    pass
        except Exception:
            pass
        return cache


def run(run_date: date) -> None:
    """Job entry point."""
    EIAIngester().run(run_date)
