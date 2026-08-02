"""
bis_rates_ingester.py — Bronze Layer — BIS CB Policy Rates Ingester
Data Source & Rates Adjustment v1.0 §8.1

Ingest BIS Statistics Warehouse WS_CBPOL dataset (internal label:
CBPOL_D, kept as-is for schema/path continuity — see FIX BIS-1 below
for why the dataflow ID itself is WS_CBPOL, not WS_CBPOL_D) — central
bank policy rates for 12 non-FED CBs at whatever base frequency BIS
reports per country (mixed daily/monthly, not uniformly daily).
ADR-010: ECB source corrected from FRED (monthly) to BIS. ADR-011:
DM/EM CB split established.

Hierarki: Grand Design v1.2 > Supplementary Design v1.1 > IDD v1.0 >
           Architecture v2.0 > Architecture Extension v1.0 >
           Data Source & Rates Adjustment v1.0

Architecture constraints:
  - GD §17.2: Bronze ONLY writes to data/bronze/ — never reads Silver/Gold
  - GD §17.3: Ingester berdiri sendiri, tidak import ingester lain
  - GD §17.7: Anti-pattern ingester melakukan transformasi bisnis —
    raw BIS CSV values disimpan as-is. Forward-fill, structural break
    flagging, dan rate_bps computation dilakukan di Silver layer.
  - GD §3.7: SchemaValidator gate — mismatch -> quarantine, NOT silent-fail
  - GD §3.1: Bronze is append-only, immutable audit trail
  - GD §7.1: Snappy compression untuk Bronze (kecepatan write > storage ratio)

Bronze output path:
  data/bronze/macro/bis_cb_rates/year=YYYY/month=MM/
    bis_cbpol_d_{YYYYMMDD_HHMM}.parquet

Job registry key: 'bronze_bis_rates'
Cadence: Weekly (WEEKLY_SEQUENCE — hari Minggu)
Dependency: [] (Bronze ingesters tidak saling bergantung — GD §17.3.1)
est_minutes: 3
"""

from __future__ import annotations

import io
import os
from datetime import datetime, date
from pathlib import Path

import polars as pl
import requests
from loguru import logger

from src.bronze.base_ingester import BronzeIngester
from src.bronze.schema_validator import SchemaValidator

# ── Konfigurasi ──────────────────────────────────────────────────────────────

# FIX BIS-1 (Ovi, 1 Aug 2026): the 28 July "v1->v2" fix only corrected the
# URL *path structure* -- it kept using "WS_CBPOL_D" as the dataflow ID,
# which was itself wrong, not just the path around it. That prior thread's
# claim that WS_CBPOL_D was independently confirmed correct ("a BIS SDMX
# Python client's dataflow listing") was never actually verified against
# live BIS and turned out to be false: the 29 July preflight log shows a
# 404 even after that "fix" landed.
#
# Root cause, confirmed this thread against data.bis.org's own indexed
# pages (8 countries checked: AR/BR/GB/CH/DK/NO/JP/CL, all served under
# the path "topics/CBPOL/BIS,WS_CBPOL,1.0/{FREQ}.{REF_AREA}") and a real,
# working third-party code example (jamelsaadaoui.com/EconMacro blog,
# live comments dated Aug 2024, site active through Jul 2026) for the
# sibling dataflow WS_CBTA using the identical URL shape. The dataflow ID
# is WS_CBPOL -- the "_D" was a "daily cadence" label mistaken for part of
# the identifier and propagated through this project's docs, config, and
# code since the original Data Source & Rates Adjustment v1.0. Frequency
# is a KEY dimension (FREQ.REF_AREA), not part of the flow name.
#
# Key below wildcards FREQ (leading empty segment -- confirmed wildcard
# semantics per a live blog comment: "leaving the reference area field
# empty" -- here applied to FREQ instead) and joins all 12 REF_AREA codes
# with "+", since the sampled countries show a MIX of Monthly and Daily
# base frequency (GB/CH/NO/JP -- all in our 12 -- came back Monthly in
# the sample; BR/DK came back Daily) -- hardcoding FREQ=D risked
# re-introducing a different 404/empty-result for the monthly ones.
# NOT live-tested against stats.bis.org from any sandbox on this project
# (no route in the network allowlist) -- needs confirmation via
# scripts/preflight/check_bis_cbpol_d.py on real hardware, same as every
# other BIS/tvdatafeed fix in this project's history.
_BIS_ENDPOINT = (
    "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/"
    ".XM+GB+JP+CA+AU+NZ+CH+KR+NO+SE+CN+ID"
)
_FORMAT_PARAM  = "?format=csv"
_TIMEOUT_SEC   = 60
_MAX_RETRIES   = 3
_RETRY_DELAY   = 5.0   # seconds

# REF_AREA -> internal CB identifier mapping
# Source: Data Source & Rates Adjustment v1.0 §3.2 / bis_cb_rates.yaml
_REF_AREA_MAP: dict[str, str] = {
    "XM": "ECB",       # ADR-010: BIS, bukan FRED (FRED monthly, incompatible)
    "GB": "BOE",
    "JP": "BOJ",       # YCC distortion 2016-2024 — flagged di Silver layer
    "CA": "BOC",
    "AU": "RBA",
    "NZ": "RBNZ",
    "CH": "SNB",       # Negative rate 2015-2022 — StandardScaler handles
    "KR": "BOK",       # MSCI EM — taxonomy consistency ADR-011
    "NO": "NORGES",
    "SE": "RIKSBANK",
    "CN": "PBOC",      # ADR-012: 7-day repo only (MLF/LPR not in BIS free tier)
    "ID": "BI",        # Structural break 2016-08-19 — flagged di Silver layer
}

# BIS CSV may use different casing for column headers — normalize to lowercase
_TIME_PERIOD_COL = "time_period"   # YYYY-MM-DD
_OBS_VALUE_COL   = "obs_value"     # float or ''
_REF_AREA_COL    = "ref_area"      # e.g. 'XM', 'GB'

_SCHEMA_PATH = Path("config/schemas/bis_cb_rates.yaml")
_OUTPUT_SUBPATH = Path("macro") / "bis_cb_rates"


class BISCBRatesIngester(BronzeIngester):
    """
    Ingester untuk BIS Statistics Warehouse WS_CBPOL dataset (central
    bank policy rates — internal label "CBPOL_D" unchanged, see FIX BIS-1).
    Mengimplementasikan GD §3.5 SourceAdapter pattern via requests (no chain
    needed — BIS is a single authoritative source with no free-tier fallback).
    """

    def __init__(self) -> None:
        self._validator = SchemaValidator(str(_SCHEMA_PATH))

    def run(self, run_date: date | None = None) -> None:
        """
        Fetch BIS CBPOL_D CSV, parse, validate, write Bronze Parquet.
        run_date digunakan hanya untuk logging — Bronze append-only timestamp
        berasal dari datetime.utcnow() saat write (GD §3.6 _ingested_at).
        """
        run_date = run_date or date.today()
        logger.info(f"[BISRates] Starting BIS CBPOL_D ingestion — run_date={run_date}")

        raw_text = self._fetch_csv()
        if raw_text is None:
            logger.error("[BISRates] Failed to fetch BIS data after all retries — aborting")
            return

        df = self._parse_csv(raw_text)
        if df is None or df.is_empty():
            logger.error("[BISRates] Parsed DataFrame empty or None — aborting")
            return

        logger.info(
            f"[BISRates] Parsed {len(df)} rows covering "
            f"{df['central_bank'].n_unique()} central banks"
        )

        ok, errors = self._validator.validate(df, symbol="BIS_CBPOL_D")
        if not ok:
            self._validator.handle_mismatch(
                df, errors, symbol="BIS_CBPOL_D", on_mismatch="quarantine"
            )
            return

        path = self.write(
            df,
            source="bis_cbpol_d",
            asset_class=str(_OUTPUT_SUBPATH),
            symbol="ALL_CB",   # Semua CB dalam satu file — bukan per-symbol
        )
        logger.success(
            f"[BISRates] Wrote {len(df)} rows to {path} "
            f"({df['obs_date'].min()} → {df['obs_date'].max()})"
        )

    def _fetch_csv(self) -> str | None:
        """
        Fetch BIS CBPOL_D as CSV dengan retry logic.
        BIS tidak memerlukan API key (GD §3.4: 'tidak memerlukan API key').
        """
        import time
        url = _BIS_ENDPOINT + _FORMAT_PARAM
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                logger.debug(f"[BISRates] HTTP GET attempt {attempt}/{_MAX_RETRIES}: {url}")
                resp = requests.get(url, timeout=_TIMEOUT_SEC)
                resp.raise_for_status()
                logger.debug(
                    f"[BISRates] Response: {resp.status_code},"
                    f" content_length={len(resp.content):,} bytes"
                )
                return resp.text
            except requests.RequestException as exc:
                logger.warning(
                    f"[BISRates] Attempt {attempt} failed: {exc}. "
                    f"{'Retrying...' if attempt < _MAX_RETRIES else 'Giving up.'}"
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAY * attempt)
        return None

    def _parse_csv(self, raw_text: str) -> pl.DataFrame | None:
        """
        Parse BIS CBPOL_D CSV into Bronze schema.

        BIS CSV structure (approximate — actual columns normalized to lowercase):
          FREQ,REF_AREA,CB_POLICY_RATE,UNIT_MULT,...,TIME_PERIOD,OBS_VALUE,...

        Bronze output columns (Data Source & Rates Adjustment v1.0 §8.1):
          ref_area     : STRING  — BIS REF_AREA code (e.g. 'XM', 'GB')
          central_bank : STRING  — internal identifier (e.g. 'ECB', 'BOE')
          obs_date     : DATE    — YYYY-MM-DD observation date
          rate_pct     : FLOAT64 — rate value as percentage (NULL if missing)
          _source      : STRING  — literal 'bis_cbpol_d'
          _ingested_at : STRING  — UTC ISO timestamp (added by BronzeIngester.write)

        NOTE: GD §17.7 anti-pattern — Bronze TIDAK melakukan transformasi bisnis.
        rate_bps = rate_pct * 100 dihitung di Silver global_rates_processor.py.
        Forward-fill dihitung di Silver. Structural break flags dihitung di Silver.
        """
        try:
            # BIS CSV may include comment lines or metadata headers — skip until
            # we find a line containing TIME_PERIOD (case-insensitive)
            lines = raw_text.splitlines()
            header_idx = None
            for i, line in enumerate(lines):
                if "time_period" in line.lower() or "TIME_PERIOD" in line:
                    header_idx = i
                    break

            if header_idx is None:
                logger.error("[BISRates] Cannot locate TIME_PERIOD header in BIS CSV")
                return None

            csv_text = "\n".join(lines[header_idx:])
            raw_df = pl.read_csv(
                io.StringIO(csv_text),
                infer_schema_length=0,   # toate columns as string initially
            )

            # Normalize column names to lowercase + strip whitespace
            raw_df = raw_df.rename({c: c.lower().strip() for c in raw_df.columns})

            # Locate required columns (case-insensitive, already lowercased above)
            if _REF_AREA_COL not in raw_df.columns:
                # Try alternative BIS column name patterns
                for alt in ("ref_area", "reference_area", "country"):
                    if alt in raw_df.columns:
                        raw_df = raw_df.rename({alt: _REF_AREA_COL})
                        break
                else:
                    logger.error(
                        f"[BISRates] Cannot locate REF_AREA column in BIS CSV. "
                        f"Columns found: {raw_df.columns[:10]}"
                    )
                    return None

            if _TIME_PERIOD_COL not in raw_df.columns or _OBS_VALUE_COL not in raw_df.columns:
                logger.error(
                    f"[BISRates] Required columns missing. Found: {raw_df.columns[:15]}"
                )
                return None

            # Filter to only known REF_AREA codes — exclude unknown CBs
            known_areas = list(_REF_AREA_MAP.keys())
            filtered = raw_df.filter(pl.col(_REF_AREA_COL).is_in(known_areas))

            if filtered.is_empty():
                logger.error(
                    f"[BISRates] No rows match known REF_AREA codes {known_areas}."
                    " BIS CSV format may have changed."
                )
                return None

            # Build clean DataFrame
            rows = []
            for row in filtered.iter_rows(named=True):
                ref_area = str(row[_REF_AREA_COL]).strip().upper()
                cb       = _REF_AREA_MAP.get(ref_area)
                if cb is None:
                    continue

                date_str  = str(row[_TIME_PERIOD_COL]).strip()
                value_str = str(row[_OBS_VALUE_COL]).strip()

                try:
                    obs_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    continue   # skip malformed dates silently

                try:
                    rate_pct = float(value_str) if value_str not in ("", "None", ".") else None
                except ValueError:
                    rate_pct = None

                rows.append({
                    "ref_area":    ref_area,
                    "central_bank": cb,
                    "obs_date":    obs_date,
                    "rate_pct":    rate_pct,
                    "_source":     "bis_cbpol_d",
                    "_ingested_at": datetime.utcnow().isoformat(),
                })

            if not rows:
                logger.error("[BISRates] Zero valid rows after parsing — possibly format change")
                return None

            df = pl.DataFrame(rows).with_columns([
                pl.col("obs_date").cast(pl.Date),
                pl.col("rate_pct").cast(pl.Float64),
            ])

            logger.info(
                f"[BISRates] Parsed {len(df)} rows, "
                f"{df['central_bank'].n_unique()} CBs, "
                f"date range {df['obs_date'].min()} to {df['obs_date'].max()}"
            )
            return df

        except Exception as exc:
            logger.exception(f"[BISRates] CSV parse failed: {exc}")
            return None


def run(run_date: date | None = None) -> None:
    """
    Job entry point — dikonsumsi oleh job_registry.py.
    Cadence: weekly (WEEKLY_SEQUENCE — hari Minggu bersama bronze_macro_weekly).
    """
    BISCBRatesIngester().run(run_date=run_date)
