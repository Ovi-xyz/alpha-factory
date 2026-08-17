"""
bea_ingester.py — Bronze BEA Ingester (GD §3.3.1)
Ingest BEA data: GDP, PCE, Trade Balance.

Rate limit: 100 req/min
API key: BEA_API_KEY dari .env (free registration at apps.bea.gov)
Cadence: Quarterly (G5: run_on_months=[1,4,7,10], last week)

Datasets ingested:
    NIPA Table 1.1.6: Real GDP (quarterly)
    NIPA Table 2.3.4: PCE Price Index
    NIPA Table 1.1.5: Trade Balance — Net exports of goods and services
                      (FIX ADR-040: was Table 4.1 / T40100, International
                      Transactions current-account — wrong concept, see
                      BEA_SERIES below)
    ITA:              International Transactions

Alternative: GDP and PCE available via FRED mirror (A191RL1Q225SBEA, PCEPI).

FIX BEA-2 (HIGH): release_date was run_date.isoformat() for all observations.
    BEA advance GDP/PCE estimates are released ~30 days after quarter-end.
    observation_date is the FIRST DAY of the quarter (e.g. 2025-10-01 for Q4),
    so quarter-end is observation_date + 3 months (~92 days), and advance release
    is quarter-end + ~30 days = observation_date + ~120 days total.
    Fix: apply RELEASE_LAG_DAYS proxy pattern (same as FRED-1 and BLS-2):
        proxy_release = observation_date + lag_days (clamped to run_date)
    All three BEA NIPA tables (GDP, PCE, Trade Balance) follow the same
    BEA advance estimate release schedule — 120-day lag from quarter start.

Output: data/bronze/macro/bea/{table_name}_{ts}.parquet
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

BEA_API_URL = "https://apps.bea.gov/api/data"

# FIX GLD-001: LineDescription filter per series.
# BEA NIPA tables mengembalikan 27+ baris per quarter (satu per komponen GDP).
# Tanpa filter ini, Bronze akan menyimpan seluruh baris dengan series_id yang sama,
# menyebabkan unit-mixing (level vs %-change vs kontribusi) yang meracuni
# HMM training data (CHANGELOG v1.7.4 NEW-7, audit_gold_layer_v1_7_4 GLD-001).
#
# Nilai string harus EXACT match terhadap LineDescription dari BEA API response:
#   T10106 Line 1: "Gross domestic product"              (total GDP level, billions)
#   T20304 Line 1: "Personal consumption expenditures"  (PCE total, sebagai proxy deflator)
#   T10105 Line 15: "Net exports of goods and services"  (trade balance — FIX ADR-040)
#
# FIX ADR-039 (GMI_Decision_Document_v9.docx, 14 Aug 2026): check_bea_datasets.py's
# first live run (14 Aug 2026) found pce_deflator's LineDescription match
# 0/310 rows -- T20304's actual wording doesn't match this string (table
# choice itself confirmed correct via BEA's own NIPA table register).
# pce_deflator and trade_balance below are now retained ONLY as
# human-readable labels for logging -- LINE_NUMBER_FILTER (below) is the
# active match mechanism for those two. real_gdp is UNCHANGED (still
# matches live, LineDescription remains its active match key).
LINE_FILTER: dict[str, str] = {
    "real_gdp":      "Gross domestic product",
    "pce_deflator":  "Personal consumption expenditures",
    "trade_balance": "Net exports of goods and services",
}

# FIX ADR-039/ADR-040 (GMI_Decision_Document_v9.docx, 14 Aug 2026):
# LineNumber-based matching, robust to the exact wording drift that broke
# pce_deflator's LineDescription match. BEA's NIPA table convention places
# a table's headline/aggregate row at a fixed LineNumber -- structural to
# the table format, not a label BEA reworks casually.
#   pce_deflator (T20304) Line 1  = "Personal consumption expenditures"
#     (BEA's own NIPA table register confirms T20304 is the correct table;
#     only the string match was wrong.)
#   trade_balance (T10105) Line 15 = "Net exports of goods and services"
#     (inferred from Table 1.1.x's standard line structure, shared across
#     all Table 1.1.* variants -- e.g. Table 1.1.3 confirms Line 15 =
#     "Net exports of goods and services" at this exact position. NOT yet
#     empirically confirmed against a live T10105 response for THIS
#     pipeline's own request parameters -- pending live confirmation,
#     checklist item 10, GMI_Decision_Document_v9.docx §3.)
LINE_NUMBER_FILTER: dict[str, str] = {
    "pce_deflator":  "1",
    "trade_balance": "15",   # PENDING LIVE CONFIRMATION — see ADR-040
}
# FIX GAP-3 [P1] (Production Readiness Assessment v1.7.2, GD §3.7): BEA was
# one of 6 sources with no Bronze Schema Registry YAML. Gates only the
# native NIPA fetch path (_fetch_nipa) — the FRED-mirror fallback writes
# under the FRED domain and is gated by fred_macro.yaml instead.
SCHEMA_PATH = Path("config/schemas/bea_macro.yaml")

# FIX BEA-2 (HIGH): release_date proxy via RELEASE_LAG_DAYS.
# BEA NIPA data: observation_date = first day of quarter (e.g. 2025-10-01 for Q4).
# Quarter end = observation_date + ~92 days. Advance estimate released ~30 days later.
# Total lag from observation_date start: ~120 days.
# release_date = observation_date + lag_days, clamped to run_date as ceiling.
# Mirrors FRED-1 (fred_ingester) and BLS-2 (bls_ingester) PIT proxy pattern.
BEA_RELEASE_LAG_DAYS: dict[str, int] = {
    "real_gdp":      120,   # BEA advance GDP: obs=Q-start, release ~120 days later
    "pce_deflator":  120,   # PCE deflator: same BEA release schedule as GDP
    "trade_balance": 120,   # Trade Balance current account: same quarterly schedule
}
_DEFAULT_BEA_LAG: int = 120  # Conservative default for any unlisted BEA series

# BEA NIPA dataset series to ingest
BEA_SERIES: list[dict] = [
    {
        "dataset":     "NIPA",
        "table_name":  "T10106",    # Table 1.1.6 Real GDP
        "name":        "real_gdp",
        "frequency":   "Q",
        "description": "Real Gross Domestic Product (quarterly)",
    },
    {
        "dataset":     "NIPA",
        "table_name":  "T20304",    # Table 2.3.4 PCE Price Indexes
        "name":        "pce_deflator",
        "frequency":   "Q",
        "description": "PCE Price Index (quarterly)",
    },
    {
        "dataset":     "NIPA",
        # FIX ADR-040 (GMI_Decision_Document_v9.docx, 14 Aug 2026): T40100
        # is BEA's International Transactions / current-account (balance-
        # of-payments) table -- confirmed via its own returned rows
        # ("Balance on current account, NIPAs", "Current payments to the
        # rest of the world", ...) -- a different, broader concept than a
        # GDP-component "net exports of goods and services" line (current
        # account also nets in primary/secondary income flows GDP
        # accounting excludes). Switched to T10105 (Table 1.1.5, Gross
        # Domestic Product), the standard GDP-components table. See
        # LINE_NUMBER_FILTER above for the matching LineNumber.
        "table_name":  "T10105",    # Table 1.1.5 Gross Domestic Product
        "name":        "trade_balance",
        "frequency":   "Q",
        "description": "Trade Balance — Net exports of goods and services (GDP component)",
    },
]


class BEAIngester(BronzeIngester):
    """
    Bronze ingester untuk BEA economic data.
    Falls back to FRED mirror for GDP and PCE series.
    """

    def __init__(self) -> None:
        self._api_key = os.getenv("BEA_API_KEY")
        # FIX GAP-3 [P1]: SchemaValidator gate (GD §3.7) for the native NIPA
        # fetch path. FRED-mirror fallback is gated separately by FREDIngester.
        self._validator = (
            SchemaValidator(SCHEMA_PATH) if SCHEMA_PATH.exists() else None
        )

    def run(self, run_date: date) -> None:
        """Ingest BEA quarterly data."""
        if not self._api_key:
            logger.warning(
                "[BEA] BEA_API_KEY not set. GDP and PCE data available"
                " via FRED mirror — FREDIngester handles A191RL1Q225SBEA, PCEPI."
                " Register free at https://apps.bea.gov/api/signup/"
            )
            self._run_via_fred_mirror(run_date)
            return

        logger.info(
            f"[BEA] Starting ingestion | {len(BEA_SERIES)} datasets"
            f" | run_date={run_date}"
        )

        for spec in BEA_SERIES:
            try:
                df = self._fetch_nipa(spec, run_date)
                if df is not None and len(df) > 0:
                    # FIX GAP-3 [P1]: SchemaValidator gate (GD §3.7) before write.
                    if self._validator is not None:
                        ok, errors = self._validator.validate(df, spec["name"])
                        if not ok:
                            self._validator.handle_mismatch(
                                df, errors, spec["name"], on_mismatch="quarantine"
                            )
                            time.sleep(0.7)
                            continue
                    self.write_macro(
                        df=df,
                        source="bea",
                        domain="gdp_pce",
                        series_id=spec["name"],
                    )
                    logger.info(f"[BEA] {spec['name']}: {len(df)} rows")
                time.sleep(0.7)   # ~85 req/min — under 100 limit
            except Exception as e:
                logger.error(f"[BEA] Failed {spec['name']}: {e}")

    def _fetch_nipa(self, spec: dict, run_date: date) -> Optional[pl.DataFrame]:
        """Fetch one BEA NIPA table."""
        params = {
            "UserID":      self._api_key,
            "method":      "GetData",
            "datasetname": spec["dataset"],
            "TableName":   spec["table_name"],
            "Frequency":   spec["frequency"],
            "Year":        ",".join(str(y) for y in range(run_date.year - 10, run_date.year + 1)),
            "ResultFormat": "JSON",
        }

        try:
            resp = requests.get(BEA_API_URL, params=params, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"[BEA] HTTP {resp.status_code}")
                return None

            data   = resp.json()
            result = data.get("BEAAPI", {}).get("Results", {})
            rows   = result.get("Data", [])

            if not rows:
                return None

            records = []
            for item in rows:
                try:
                    val_str = str(item.get("DataValue", "0")).replace(",", "")

                    # FIX GLD-001 (baseline) / FIX ADR-039 / FIX ADR-040:
                    # filter hanya baris yang sesuai target row. BEA NIPA
                    # tables mengembalikan 27+ baris per quarter; tanpa
                    # filter ini, komponen lain (level vs %-change vs
                    # kontribusi) akan meracuni Silver series.
                    #
                    # LineNumber-first: robust to wording drift (the exact
                    # failure mode that broke pce_deflator's old
                    # LineDescription match — 0/310 rows matched live).
                    # Falls back to LineDescription only for series with no
                    # LINE_NUMBER_FILTER entry (real_gdp — already passing
                    # live, left unchanged).
                    target_line_number = LINE_NUMBER_FILTER.get(spec["name"])
                    if target_line_number:
                        line_number = str(item.get("LineNumber", "")).strip()
                        if line_number != target_line_number:
                            continue   # skip komponen non-target
                    else:
                        target_desc = LINE_FILTER.get(spec["name"], "")
                        if target_desc:
                            line_desc = item.get("LineDescription", "").strip()
                            if line_desc != target_desc:
                                continue   # skip komponen non-target

                    # FIX BEA-1 (CRITICAL): BEA returns TimePeriod as "2025Q4", "2025A",
                    # "2025" etc. — NOT valid ISO dates. Silver MacroProcessor filter
                    # (release_date <= run_date) cannot compare "2025Q4" with "2026-06-05".
                    # Convert to first day of the period:
                    #   "2025Q1" → "2025-01-01"   "2025Q4" → "2025-10-01"
                    #   "2025A"  → "2025-01-01"   "2025"   → "2025-01-01"
                    period_raw = str(item.get("TimePeriod", ""))
                    if "Q" in period_raw:
                        yr, q   = period_raw.split("Q", 1)
                        q_month = (int(q) - 1) * 3 + 1   # Q1→1, Q2→4, Q3→7, Q4→10
                        obs_date = f"{yr}-{q_month:02d}-01"
                    elif period_raw.isdigit() and len(period_raw) == 4:
                        obs_date = f"{period_raw}-01-01"  # annual: "2025" → "2025-01-01"
                    elif len(period_raw) >= 4 and period_raw[:4].isdigit():
                        # Handles "2025A", "2025M01", etc.
                        obs_date = f"{period_raw[:4]}-01-01"
                    else:
                        logger.debug(
                            f"[BEA] {spec['name']}: unknown TimePeriod {period_raw!r} — skipping"
                        )
                        continue

                    records.append({
                        "series_id":        spec["name"],
                        "table_name":       spec["table_name"],
                        "line_description": item.get("LineDescription", ""),
                        "observation_date": obs_date,
                        "value":            float(val_str) if val_str else None,
                        "unit":             item.get("CL_UNIT", ""),
                        # FIX BEA-2 (HIGH): release_date via RELEASE_LAG_DAYS proxy.
                        # Previously: run_date.isoformat() — PIT filter trivially True.
                        # Now: observation_date (first of quarter) + lag_days (~120),
                        # clamped to run_date. Mirrors FRED-1 and BLS-2 pattern.
                        "release_date":     min(
                            date.fromisoformat(obs_date)
                            + timedelta(days=BEA_RELEASE_LAG_DAYS.get(
                                spec["name"], _DEFAULT_BEA_LAG
                            )),
                            run_date,
                        ).isoformat(),
                    })
                except (ValueError, KeyError):
                    pass

            return pl.DataFrame(records) if records else None

        except Exception as e:
            logger.warning(f"[BEA] Request failed for {spec['name']}: {e}")
            return None

    def _run_via_fred_mirror(self, run_date: date) -> None:
        """Fetch BEA data via FRED mirror series."""
        fred_bea_series = [
            "A191RL1Q225SBEA",   # Real GDP growth rate
            "GDPC1",              # Real GDP level
            "PCEPI",              # PCE Price Index
            "PCEPILFE",           # PCE Core
            "BOPGSTB",            # Trade balance goods
        ]
        if os.getenv("FRED_API_KEY"):
            from src.bronze.fred_ingester import FREDIngester
            FREDIngester().run(run_date, series_filter=fred_bea_series)
            logger.info("[BEA] Used FRED mirror for BEA series")
        else:
            logger.warning(
                "[BEA] Neither BEA_API_KEY nor FRED_API_KEY set."
                " GDP/PCE data unavailable."
            )


def run(run_date: date) -> None:
    """Job entry point."""
    BEAIngester().run(run_date)
