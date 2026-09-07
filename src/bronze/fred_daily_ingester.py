"""
fred_daily_ingester.py — Bronze FRED Daily-Cadence Ingester

FIX GMI-FRED-DAILY-01 (Ovi, 7 Sep 2026): 9 series in config/fred_series.yaml
carry cadence: daily but are NOT among treasury_ingester.py's own
TREASURY_FRED_SERIES (which already fetches its 13 tenors daily,
independent of this module). Their only other path to fred_ingester.py's
full-registry FREDIngester().run(run_date) call was bronze_macro_weekly —
which, since the 31 Aug 2026 schedule-guard fix (job_registry.py,
"run_on_weekdays": [6]), can only run on Sunday. fred_ingester.py's own
per-series cadence check requires run_date.weekday() in Mon-Fri for any
"daily" series. Sunday can never satisfy Mon-Fri — these 9 series were
consequently unreachable by any code path, deterministically, every week.

Confirmed empirically against the live repo (list_directory +
get_file_info on data/bronze/macro/fred/{volatility,credit}/): all 9 were
frozen at exactly 2 Bronze files each, both predating the 31 Aug fix. 3 are
regime_input: true in fred_series.yaml — VIXCLS (vix_proxy), DEXUSEU
(dxy_proxy), BAMLH0A0HYM2 (credit_spread) — meaning 3 of gold_regime's 7
macro regime inputs were silently stale for a week with no error, crash,
or non-debug log line anywhere in the run.

Fix (Ovi's chosen repair path — split the full FRED sweep): a dedicated
job, independent of bronze_macro_weekly, mirroring treasury_ingester.py's
own established delegate-to-FREDIngester-with-series_filter pattern.
Registered in job_registry.py as "bronze_fred_daily" in DAILY_SEQUENCE
(no schedule guard — it must be attempted every day; fred_ingester.py's
own weekday check is the correct and sufficient gate for a caller that is
itself invoked daily, exactly as it already is for bronze_treasury).

bronze_macro_weekly's full-registry Sunday call is left unchanged. It will
keep evaluating these 9 series on Sundays and skipping them via the same
weekday check (a debug log line, no API call, no write) — harmless, and
deliberately not excluded via a second hand-maintained list: that would
reintroduce the dual-source-of-truth shape GMI Decision Document v11's
ADR-047 already rejected for a different ticker table, for the same
reason. This module is the single owner of "fetch these 9 series."

Output: data/bronze/macro/fred/{domain}/{series_id}_{ts}.parquet
(same Bronze path FREDIngester always writes to — this module adds no new
Hive structure, it only adds a second, differently-scheduled caller.)
"""

from __future__ import annotations

from datetime import date

from loguru import logger

# 9 FRED series registered as cadence: daily in config/fred_series.yaml
# that are NOT already covered by treasury_ingester.py's own daily fetch
# (TREASURY_FRED_SERIES). Keep in sync with config/fred_series.yaml: if a
# new series is added there with cadence: daily and it is not already one
# of treasury_ingester.py's 13 tenors, it belongs in this list too, or it
# will silently reproduce the exact starvation this module exists to fix.
FRED_DAILY_SERIES: list[str] = [
    "VIXCLS",        # regime_input: vix_proxy
    "DEXUSEU",       # regime_input: dxy_proxy
    "BAMLH0A0HYM2",  # regime_input: credit_spread
    "BAMLC0A0CM",
    "DCOILWTICO",
    "DEXJPUS",
    "DFF",
    "T5YIE",
    "T10YIE",
]


def run(run_date: date) -> None:
    """Ingest the 9 daily-cadence FRED series orphaned by bronze_macro_weekly's
    Sunday-only schedule. Delegates entirely to FREDIngester — no separate
    fetch/write logic, no separate schema, no separate idempotency rule.
    Mirrors treasury_ingester.py's own delegate-wrapper pattern, including
    the outer try/except (a delegate failure must not abort the rest of
    DAILY_SEQUENCE)."""
    logger.info(
        f"[FRED-Daily] Fetching {len(FRED_DAILY_SERIES)} daily-cadence series "
        f"(incl. 3 macro regime inputs) | run_date={run_date}"
    )
    try:
        from src.bronze.fred_ingester import FREDIngester
        FREDIngester().run(run_date, series_filter=FRED_DAILY_SERIES)
        logger.info(
            f"[FRED-Daily] Ingestion complete ({len(FRED_DAILY_SERIES)} series)"
        )
    except Exception as e:
        logger.error(f"[FRED-Daily] Ingestion failed: {e}")
