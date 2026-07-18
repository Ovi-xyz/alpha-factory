"""
treasury_ingester.py — Bronze US Treasury Yield Curve Ingester (GD §3.3.3)
Ingest daily yield curve per tenor (1M, 3M, 6M, 1Y, 2Y, 5Y, 7Y, 10Y, 20Y, 30Y).

FIX TRES-1 (CRITICAL): previous implementation used the wrong endpoint:
    WRONG: fiscaldata.treasury.gov/api/public/debt/rates/avg_interest_rates/
    This returns AVERAGE INTEREST RATES on all outstanding debt — a blended
    rate across all maturities already issued, NOT the daily yield curve.
    The `security_desc` field contains "Treasury Bills", "Treasury Notes" etc.
    — unparseable as standard tenor keys (2Y, 5Y, 10Y, 30Y).
    yield_curve spread (T10Y2Y, T10Y3M) CANNOT be computed from that data.

CORRECT APPROACH: delegate to FRED series which are already in fred_series.yaml.
FRED DGS series provide DAILY yield curve per tenor, already registered:
    DGS1MO  — 1-Month Treasury (daily)
    DGS3MO  — 3-Month Treasury (daily)
    DGS6MO  — 6-Month Treasury (daily)
    DGS1    — 1-Year Treasury (daily)
    DGS2    — 2-Year Treasury (daily)
    DGS5    — 5-Year Treasury (daily)
    DGS7    — 7-Year Treasury (daily)
    DGS10   — 10-Year Treasury (daily)
    DGS20   — 20-Year Treasury (daily)
    DGS30   — 30-Year Treasury (daily)
    T10Y2Y  — 10Y-2Y Spread (daily, key recession signal)
    T10Y3M  — 10Y-3M Spread (daily, key recession signal)

These are stored to: data/bronze/macro/fred/monetary_policy/

Cadence: Daily (same as other FRED series in fred_series.yaml).
No separate Treasury endpoint needed — FREDIngester handles these.

Rate limit: governed by FREDIngester (120 req/min).
API key: FRED_API_KEY from .env.
"""

from __future__ import annotations

import os
from datetime import date
from typing import Optional

import polars as pl
from loguru import logger

from src.bronze.base_ingester import BronzeIngester  # noqa: F401 — kept for potential future use

# Canonical yield curve tenors from FRED
# These are already registered in config/fred_series.yaml (monetary_policy domain)
TREASURY_FRED_SERIES: list[str] = [
    "DGS1MO",   # 1-Month Constant Maturity Treasury
    "DGS3MO",   # 3-Month Constant Maturity Treasury
    "DGS6MO",   # 6-Month Constant Maturity Treasury
    "DGS1",     # 1-Year Constant Maturity Treasury
    "DGS2",     # 2-Year Constant Maturity Treasury
    "DGS5",     # 5-Year Constant Maturity Treasury
    "DGS7",     # 7-Year Constant Maturity Treasury
    "DGS10",    # 10-Year Constant Maturity Treasury
    "DGS20",    # 20-Year Constant Maturity Treasury
    "DGS30",    # 30-Year Constant Maturity Treasury
    "T10Y2Y",   # 10Y-2Y Spread — primary recession signal (GD §8.1)
    "T10Y3M",   # 10Y-3M Spread — alternative recession signal
    "MORTGAGE30US",  # 30-Year Fixed Mortgage Rate
]

# Tenor label mapping for downstream display (Silver → Gold)
# GAP-3 (Production Readiness Assessment v1.7.2): the canonical tenor ->
# FRED series_id mapping is also registered as documentation in
# config/schemas/treasury_yield.yaml. That file is NOT an active
# SchemaValidator gate here — TreasuryIngester has no independent Bronze
# write path (see FIX TI-1 below), so the 13 series above are validated at
# write time by fred_macro.yaml via the delegated FREDIngester call. See
# the ARCHITECTURE NOTE at the top of treasury_yield.yaml for the full
# rationale.
TENOR_LABELS: dict[str, str] = {
    "DGS1MO": "1M", "DGS3MO": "3M", "DGS6MO": "6M",
    "DGS1":   "1Y", "DGS2":   "2Y", "DGS5":   "5Y",
    "DGS7":   "7Y", "DGS10":  "10Y", "DGS20":  "20Y",
    "DGS30":  "30Y",
    "T10Y2Y": "spread_10y2y", "T10Y3M": "spread_10y3m",
    "MORTGAGE30US": "mortgage_30y",
}


class TreasuryIngester:
    """
    Bronze ingester untuk US Treasury yield curve.

    FIX TI-1 (MEDIUM): Inheritance from BronzeIngester removed.
    TreasuryIngester never calls self.write() or self.write_macro() directly —
    all writes are performed by the delegated FREDIngester. Inheriting BronzeIngester
    added BASE_PATH and unused write methods that created a false impression this
    class writes data itself. Plain class is the correct pattern here (GD §17.3).

    FIX TRES-1: Delegates to FREDIngester for FRED DGS series.
    FREDIngester already handles these via fred_series.yaml — TreasuryIngester
    is a thin orchestration wrapper that ensures yield curve series are ingested
    as part of the treasury job, independently of the main weekly FRED run.

    This design follows GD §17.3 Bronze modularity: treasury_ingester.py has
    exclusive responsibility for yield curve domain — it calls FREDIngester
    with an explicit series_filter rather than duplicating fetch logic.
    """

    def run(self, run_date: date) -> None:
        """
        Ingest daily yield curve via FRED DGS series.
        Delegates to FREDIngester with explicit series_filter.
        """
        if not os.getenv("FRED_API_KEY"):
            logger.warning(
                "[Treasury] FRED_API_KEY not set — cannot fetch yield curve.\n"
                "  Daily Treasury yield curve (T10Y2Y, DGS2, DGS10, etc.) is\n"
                "  available via FRED API (free). Register at:\n"
                "  https://fredaccount.stlouisfed.org/login/secure/\n"
                "  Set FRED_API_KEY in .env to enable yield curve ingestion."
            )
            return

        logger.info(
            f"[Treasury] Fetching yield curve via FRED DGS series"
            f" | {len(TREASURY_FRED_SERIES)} series | run_date={run_date}"
        )

        try:
            from src.bronze.fred_ingester import FREDIngester
            FREDIngester().run(run_date, series_filter=TREASURY_FRED_SERIES)
            logger.info(
                f"[Treasury] Yield curve ingestion complete"
                f" ({len(TREASURY_FRED_SERIES)} FRED DGS series)"
            )
        except Exception as e:
            logger.error(f"[Treasury] Yield curve ingestion failed: {e}")

    def get_available_tenors(self) -> list[str]:
        """Return list of tenor labels available in Bronze yield curve data."""
        return list(TENOR_LABELS.values())

    def get_fred_series_ids(self) -> list[str]:
        """Return FRED series IDs for yield curve (for reference / testing)."""
        return TREASURY_FRED_SERIES.copy()


def run(run_date: date) -> None:
    """Job entry point."""
    TreasuryIngester().run(run_date)
