"""
finnhub_ingester.py — Bronze Finnhub Ingester (GD §3.3.2)
Ingest real-time quote, earnings calendar, dan news sentiment dari Finnhub.

Rate limit: 60 req/min — throttled ke 50 req/min dengan safety buffer.
API key: FINNHUB_API_KEY dari .env
Cadence: Daily (setelah market close)

Domains ingested:
    fundamental/earnings    — earnings calendar per symbol
    fundamental/quote       — last real-time quote
    fundamental/financials  — key financial metrics

Output: data/bronze/market/fundamental/{domain}/{symbol}_{ts}.parquet

Digunakan oleh:
    Gold Screener → days_to_earnings, near_earnings_flag (v1.2)
    Silver Sentiment → sentiment_score (via separate SentimentProcessor)

FIX FH-1 (CRITICAL): _ingest_earnings_calendar() previously called write_macro()
    which routes to data/bronze/macro/finnhub/earnings_calendar/ — but
    get_days_to_earnings() reads from BRONZE_FUNDAMENTAL/earnings_calendar/finnhub/.
    Path mismatch caused get_days_to_earnings() to always return None, rendering
    Gold Screener days_to_earnings and near_earnings_flag permanently null (GD §5.2.4).
    Fix: use write() with asset_class='market/fundamental/earnings_calendar' so
    data lands at data/bronze/market/fundamental/earnings_calendar/finnhub/...

FIX FH-2 (HIGH): get_days_to_earnings() used f-string SQL injection:
    WHERE symbol = '{symbol}' AND ... >= '{run_date}'
    Inconsistent with IMF-2 parameterized query pattern.
    Fix: DuckDB parameterized query using ? placeholders throughout.

FIX RISK-4 (P3, KNOWN_RISKS.md) [GMI Wave 1 Bronze/Silver Solidification]:
    Both self.write() call sites (_ingest_earnings_calendar,
    _ingest_symbol) previously wrote to Bronze with ZERO SchemaValidator
    involvement — the only Bronze ingester in the repo with this gap
    (confirmed via audit across all 10 ingesters). Blocked historically on
    not having a real Finnhub response to design the schema against; this
    sandbox has no live network access to Finnhub either. Resolved by
    grounding config/schemas/finnhub_earnings_calendar.yaml and
    finnhub_quote.yaml in Finnhub's PUBLICLY DOCUMENTED API field names
    and nullability (verified via web search against Finnhub's official
    docs and community SDK type definitions, not this ingester's own
    assumptions). Both record-building methods now explicitly CAST every
    column to its declared schema type before validation — this is not
    cosmetic: letting Polars infer dtypes from raw API dict values is
    fragile in two concrete, verified ways: (1) revenueEstimate can appear
    as a whole-number JSON int or a float depending on the value, so an
    all-integer batch would infer Int64 against a Float64 schema and fail
    validation spuriously; (2) eps_actual is null for essentially EVERY
    row in practice (this ingester only fetches a 90-day FORWARD window,
    so actuals haven't happened yet) — an all-null column infers Polars'
    Null dtype, not Float64, which would make schema validation fail on
    the single most common, expected case rather than a genuine anomaly.
    Explicit casts make the schema contract stable regardless of what
    values happen to be present in a given fetch.
"""

from __future__ import annotations

import os
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import polars as pl
from loguru import logger

from src.bronze.base_ingester import BronzeIngester
from src.bronze.schema_validator import SchemaValidator  # FIX RISK-4
from src.config.instrument_loader import get_loader

THROTTLE_SECONDS      = 1.2     # 50 req/min (Finnhub limit: 60)
MAX_SYMBOLS_PER_RUN   = 200     # Only active US stocks for quota management

# FIX FH-1: canonical read path — earnings written here via write(), read here by get_days_to_earnings()
# Path structure (via write()): data/bronze/market/fundamental/earnings_calendar/finnhub/symbol=.../...
BRONZE_FUNDAMENTAL    = Path("data/bronze/market/fundamental")
_EARNINGS_ASSET_CLASS = "market/fundamental/earnings_calendar"

# FIX RISK-4: schema registry paths (GD §3.7)
_EARNINGS_SCHEMA_PATH = "config/schemas/finnhub_earnings_calendar.yaml"
_QUOTE_SCHEMA_PATH    = "config/schemas/finnhub_quote.yaml"


class FinnhubIngester(BronzeIngester):
    """
    Bronze ingester untuk Finnhub data.
    Focuses on earnings calendar and basic quote data.
    Sentiment handled separately by SentimentProcessor (Silver layer).
    """

    def __init__(self) -> None:
        self._api_key = os.getenv("FINNHUB_API_KEY")
        # FIX RISK-4: validators constructed unconditionally (cheap — just
        # parses two small YAML files) so both write paths always validate,
        # matching every other Bronze ingester's convention.
        self._earnings_validator = SchemaValidator(_EARNINGS_SCHEMA_PATH)
        self._quote_validator    = SchemaValidator(_QUOTE_SCHEMA_PATH)

    def run(self, run_date: date) -> None:
        """Ingest Finnhub data for active US symbols."""
        if not self._api_key:
            logger.warning(
                "[Finnhub] FINNHUB_API_KEY not set — skipping Finnhub ingestion."
                " Set FINNHUB_API_KEY in .env to enable."
            )
            return

        try:
            import finnhub  # type: ignore
            self._client = finnhub.Client(api_key=self._api_key)
        except ImportError:
            logger.error(
                "[Finnhub] finnhub-python not installed."
                " Run: pip install finnhub-python"
            )
            return

        loader      = get_loader()
        us_symbols  = loader.by_market("us_stocks")[:MAX_SYMBOLS_PER_RUN]

        logger.info(
            f"[Finnhub] Starting ingestion | {len(us_symbols)} symbols"
            f" | run_date={run_date}"
        )

        # 1. Earnings calendar (batch fetch — more efficient)
        self._ingest_earnings_calendar(run_date)

        # 2. Per-symbol quote + basic financials
        success = failed = 0
        for inst in us_symbols:
            try:
                self._ingest_symbol(inst.symbol, run_date)
                success += 1
            except Exception as e:
                logger.debug(f"[Finnhub] {inst.symbol}: {e}")
                failed += 1
            finally:
                time.sleep(THROTTLE_SECONDS)

        logger.info(
            f"[Finnhub] Complete: {success} OK, {failed} failed"
        )

    def _ingest_earnings_calendar(self, run_date: date) -> None:
        """
        Fetch earnings calendar for next 90 days.
        Output feeds Gold Screener days_to_earnings column (GD §5.2.4).

        FIX FH-1: use write() with asset_class=_EARNINGS_ASSET_CLASS instead of
        write_macro(). write_macro() routes to data/bronze/macro/finnhub/... but
        get_days_to_earnings() reads from data/bronze/market/fundamental/earnings_calendar/...
        write() with correct asset_class aligns the write and read paths.
        """
        from_date = run_date
        to_date   = run_date + timedelta(days=90)

        try:
            cal = self._client.earnings_calendar(
                _from=from_date.isoformat(),
                to=to_date.isoformat(),
                symbol="",          # All symbols
                international=False,
            )
            items = cal.get("earningsCalendar", [])
            if not items:
                return

            records = []
            for item in items:
                sym = item.get("symbol", "")
                if not sym:
                    continue
                records.append({
                    "symbol":            sym,
                    "earnings_date":     item.get("date", ""),
                    "eps_estimate":      item.get("epsEstimate"),
                    "eps_actual":        item.get("epsActual"),
                    "revenue_estimate":  item.get("revenueEstimate"),
                    "quarter":           item.get("quarter"),
                    "year":              item.get("year"),
                    "fetched_date":      str(run_date),
                })

            if records:
                # FIX RISK-4: explicit dtype casts BEFORE validation — see
                # module docstring for why relying on Polars' inference
                # from raw dict values is fragile for this specific data
                # (all-null eps_actual column, int-vs-float revenue values).
                df = pl.DataFrame(records).with_columns([
                    pl.col("symbol").cast(pl.Utf8),
                    pl.col("earnings_date").cast(pl.Utf8),
                    pl.col("eps_estimate").cast(pl.Float64, strict=False),
                    pl.col("eps_actual").cast(pl.Float64, strict=False),
                    pl.col("revenue_estimate").cast(pl.Float64, strict=False),
                    pl.col("quarter").cast(pl.Int64, strict=False),
                    pl.col("year").cast(pl.Int64, strict=False),
                    pl.col("fetched_date").cast(pl.Utf8),
                ])

                # FIX RISK-4: schema gate before write — mismatch -> quarantine,
                # never silent-fail (GD §3.7), matching every other ingester.
                ok, errors = self._earnings_validator.validate(
                    df, f"earnings_{run_date.isoformat()}"
                )
                if not ok:
                    self._earnings_validator.handle_mismatch(
                        df, errors,
                        symbol=f"earnings_{run_date.isoformat()}",
                        on_mismatch="quarantine",
                    )
                    logger.error(
                        f"[Finnhub] Earnings calendar QUARANTINED — schema "
                        f"mismatch: {errors}"
                    )
                    return

                # FIX FH-1: write() routes to market/fundamental/earnings_calendar/finnhub/...
                # This aligns the write path with the read path in get_days_to_earnings().
                # Idempotency is guaranteed by BronzeIngester.write() date-prefix glob check.
                self.write(
                    df=df,
                    source="finnhub",
                    asset_class=_EARNINGS_ASSET_CLASS,
                    symbol=f"earnings_{run_date.isoformat()}",
                )
                logger.info(
                    f"[Finnhub] Earnings calendar: {len(records)} events"
                )
        except Exception as e:
            logger.warning(f"[Finnhub] Earnings calendar fetch failed: {e}")

    def _ingest_symbol(self, symbol: str, run_date: date) -> None:
        """Fetch quote + basic financials for one symbol."""
        # Quote
        try:
            quote = self._client.quote(symbol)
            if quote:
                # FIX RISK-4: explicit dtype casts BEFORE validation — see
                # module docstring rationale (shared with earnings path).
                # UPD Decision Doc v2 §5 (2026-07-11): high_52w/low_52w
                # renamed to day_high/day_low — Finnhub's h/l fields are the
                # CURRENT TRADING DAY's high/low, not a 52-week range. The
                # decision doc's premise ("zero current consumers") turned
                # out to be FALSE on direct verification: fundamental_processor.py
                # ::process_quotes() reads these columns straight through
                # into Silver — updated in the same change (see that file).
                quote_df = pl.DataFrame([{
                    "symbol":           symbol,
                    "current_price":    quote.get("c"),
                    "change":           quote.get("d"),
                    "pct_change":       quote.get("dp"),
                    "day_high":         quote.get("h"),
                    "day_low":          quote.get("l"),
                    "open":             quote.get("o"),
                    "prev_close":       quote.get("pc"),
                    "timestamp":        quote.get("t"),
                    "fetched_date":     str(run_date),
                }]).with_columns([
                    pl.col("symbol").cast(pl.Utf8),
                    pl.col("current_price").cast(pl.Float64, strict=False),
                    pl.col("change").cast(pl.Float64, strict=False),
                    pl.col("pct_change").cast(pl.Float64, strict=False),
                    pl.col("day_high").cast(pl.Float64, strict=False),
                    pl.col("day_low").cast(pl.Float64, strict=False),
                    pl.col("open").cast(pl.Float64, strict=False),
                    pl.col("prev_close").cast(pl.Float64, strict=False),
                    pl.col("timestamp").cast(pl.Int64, strict=False),
                    pl.col("fetched_date").cast(pl.Utf8),
                ])

                # FIX RISK-4: schema gate before write — mismatch -> quarantine.
                ok, errors = self._quote_validator.validate(quote_df, symbol)
                if not ok:
                    self._quote_validator.handle_mismatch(
                        quote_df, errors, symbol=symbol, on_mismatch="quarantine"
                    )
                    logger.error(
                        f"[Finnhub] Quote QUARANTINED for {symbol} — "
                        f"schema mismatch: {errors}"
                    )
                    return

                self.write(
                    df=quote_df,
                    source="finnhub",
                    asset_class="market/fundamental/quote",
                    symbol=symbol,
                )
        except Exception as e:
            logger.debug(f"[Finnhub] Quote failed for {symbol}: {e}")


def run(run_date: date) -> None:
    """Job entry point."""
    FinnhubIngester().run(run_date)


def get_days_to_earnings(symbol: str, run_date: date) -> Optional[int]:
    """
    Utility: compute days_to_earnings for a symbol from Bronze earnings data.
    Used by Gold Screener to populate days_to_earnings column (GD §5.2.4).
    Returns None if no upcoming earnings found.

    FIX FH-1: glob path updated to match write() destination:
        data/bronze/market/fundamental/earnings_calendar/finnhub/**/*.parquet
        (previously pointed to market/fundamental/finnhub/earnings_calendar/** — wrong)

    FIX FH-2 (HIGH): parameterized DuckDB query replaces f-string interpolation.
        Previous: WHERE symbol = '{symbol}' AND ... >= '{run_date}' — SQL injection risk.
        Fix: DuckDB ? placeholder binding for all variable inputs.
        Consistent with IMF-2 parameterized query pattern.
    """
    # FIX FH-3 (HIGH): FH-1 fixed the write path to use write() which produces
    # Hive path: earnings_calendar/source=finnhub/symbol=earnings_{date}/year/month/
    # However this glob still pointed to the old non-Hive path:
    #   earnings_calendar/finnhub/**/*.parquet  ← WRONG (never matched written files)
    # Fix: use wildcard ** without assuming source= prefix — covers both layouts
    # and is forward-compatible if write() path changes.
    earnings_glob = str(
        BRONZE_FUNDAMENTAL / "earnings_calendar" / "**" / "*.parquet"
    )
    try:
        import duckdb
        con = duckdb.connect()
        # FIX FH-2: parameterized query — no f-string SQL variable interpolation
        result = con.execute(
            """
            SELECT MIN(DATEDIFF('day', CAST(? AS DATE), CAST(earnings_date AS DATE)))
                AS days_to_earnings
            FROM read_parquet(?, hive_partitioning=true)
            WHERE symbol = ?
              AND CAST(earnings_date AS DATE) >= CAST(? AS DATE)
            """,
            [str(run_date), earnings_glob, symbol, str(run_date)],
        ).fetchone()

        if result and result[0] is not None:
            return int(result[0])
    except Exception:
        pass
    return None
