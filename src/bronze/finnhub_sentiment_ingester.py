"""
finnhub_sentiment_ingester.py — Bronze Finnhub Sentiment Ingester (GD §3.3.2, §3.7)

Eliminates architectural debt: Silver calling Finnhub API directly (GD §17.7 anti-pattern).

Refactor summary:
    BEFORE (v1.5 debt):
        silver/sentiment_processor.py → finnhub.Client() → writes Silver directly
        Scope: ~200 active symbols (artifact of debt — Silver needed active_symbols to limit calls)
        Violation: GD §17.7 "Silver memanggil external API untuk enrichment utama"

    AFTER (this module):
        bronze/finnhub_sentiment_ingester.py → finnhub.Client() → Bronze Parquet
        Scope: 643 instrument universe via InstrumentLoader — no Silver dependency
        Compliant: GD §17.3 "Bronze adalah satu-satunya layer dengan mandate memanggil external API"

Scope   : 643 instrument universe via InstrumentLoader (depends_on=[]).
Cadence : Daily Bronze job.
Output  : data/bronze/market/fundamental/sentiment/source=finnhub/
          symbol=sentiment_{date}/year={Y}/month={M}/sentiment_{date}_raw_{ts}.parquet
Schema  : config/schemas/finnhub_sentiment.yaml (GD §3.7 SchemaValidator gate)

GD References:
    §3.1  — Idempotency: BronzeIngester.write() date-prefix check prevents duplicate writes.
    §3.7  — Bronze Schema Registry: SchemaValidator gate before every write.
    §17.2 — Layer Independence: Bronze reads no Silver or Gold data.
    §17.3 — Bronze sole external API gateway.
    §17.7 — Anti-pattern eliminated: Silver no longer calls finnhub.Client().

Silver consumer:
    src/silver/sentiment_processor.py reads from BRONZE_SENTIMENT_PATH via scan_parquet.
    Silver filters by fetched_date column — no hive partition coupling.
"""

from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path
from typing import Optional

import polars as pl
from loguru import logger

from src.bronze.base_ingester import BronzeIngester
from src.bronze.schema_validator import SchemaValidator
from src.config.instrument_loader import get_loader

# Rate limiting: Finnhub 60 req/min → 50 req/min (safety buffer of 1 req/1.2s)
THROTTLE_SECONDS: float = 1.2

# Bronze path constants — Silver consumer reads from this root path
BRONZE_SENTIMENT_PATH: Path = Path("data/bronze/market/fundamental/sentiment")

# Internal config
_SCHEMA_PATH: str = "config/schemas/finnhub_sentiment.yaml"
_ASSET_CLASS: str = "market/fundamental/sentiment"


class FinnhubSentimentIngester(BronzeIngester):
    """
    Bronze ingester for Finnhub news sentiment.

    Scope: 643 instrument universe via InstrumentLoader.
    depends_on: [] — independent Bronze job, no Silver dependency whatsoever.

    Finnhub API coverage is limited to US stocks. For IDX, forex, and commodity
    symbols, _fetch_one() returns None and the symbol is silently skipped.
    This is NOT a schema mismatch — only structural schema errors trigger quarantine.

    Records are batched into a single DataFrame per run_date to minimize
    the number of Parquet files and simplify Silver consumption.

    Idempotency: BronzeIngester.write() performs a date-prefix check (GD §3.1).
    If a sentiment file for this run_date already exists in the partition,
    write() skips the write and returns None. Re-running on the same date
    is safe.
    """

    def __init__(self) -> None:
        self._api_key: Optional[str] = os.getenv("FINNHUB_API_KEY")
        self._validator: SchemaValidator = SchemaValidator(_SCHEMA_PATH)
        self._client = None  # initialized lazily in run()

    def run(self, run_date: date) -> None:
        """
        Fetch Finnhub sentiment for all 643 instruments and write to Bronze.

        Flow:
            1. Validate FINNHUB_API_KEY is set.
            2. Instantiate finnhub.Client().
            3. Loop 643 symbols from InstrumentLoader (not Silver active_symbols).
            4. Call _fetch_one() for each — None returned for uncovered symbols.
            5. Batch all records into a single pl.DataFrame.
            6. SchemaValidator gate (GD §3.7) — quarantine on mismatch.
            7. BronzeIngester.write() — idempotent, Snappy compressed.
        """
        if not self._api_key:
            logger.warning(
                "[BronzeSentiment] FINNHUB_API_KEY not set — skipping sentiment ingestion. "
                "Set FINNHUB_API_KEY in .env to enable."
            )
            return

        try:
            import finnhub  # type: ignore
            self._client = finnhub.Client(api_key=self._api_key)
        except ImportError:
            logger.error(
                "[BronzeSentiment] finnhub-python not installed. "
                "Run: pip install finnhub-python"
            )
            return

        # 643 instrument universe — InstrumentLoader, NO Silver dependency
        loader  = get_loader()
        symbols = loader.all_symbols()

        logger.info(
            f"[BronzeSentiment] Fetching {len(symbols)} symbols | run_date={run_date}"
        )

        records: list[dict] = []
        skipped = 0

        for i, inst in enumerate(symbols):
            record = self._fetch_one(inst.symbol, run_date)
            if record is not None:
                records.append(record)
            else:
                skipped += 1

            time.sleep(THROTTLE_SECONDS)

            if (i + 1) % 100 == 0:
                logger.info(
                    f"[BronzeSentiment] Progress: {i + 1}/{len(symbols)} | "
                    f"{len(records)} records collected"
                )

        if not records:
            logger.warning(
                f"[BronzeSentiment] No sentiment records collected for {run_date}. "
                f"{skipped} symbols skipped (not covered by Finnhub or API errors). "
                f"Check FINNHUB_API_KEY and network connectivity."
            )
            return

        df = pl.DataFrame(records)

        # SchemaValidator gate (GD §3.7) — Finnhub schema change → quarantine
        symbol_key = f"sentiment_{run_date.isoformat()}"
        ok, errors = self._validator.validate(df, symbol_key)

        if ok:
            result_path = self.write(
                df=df,
                source="finnhub",
                asset_class=_ASSET_CLASS,
                symbol=symbol_key,
            )
            if result_path is not None:
                logger.info(
                    f"[BronzeSentiment] Written {len(df)} records | "
                    f"{skipped} skipped | run_date={run_date} | {result_path}"
                )
            else:
                # BronzeIngester.write() returned None → idempotent skip
                logger.info(
                    f"[BronzeSentiment] Idempotent skip — Bronze sentiment for "
                    f"{run_date} already written."
                )
        else:
            self._validator.handle_mismatch(
                df,
                errors,
                symbol=symbol_key,
                on_mismatch="quarantine",
            )
            logger.error(
                f"[BronzeSentiment] Schema mismatch — data quarantined for {run_date}. "
                f"Errors: {errors}"
            )

    def _fetch_one(self, symbol: str, run_date: date) -> Optional[dict]:
        """
        Fetch Finnhub news sentiment for one symbol.

        Returns:
            dict with all Bronze schema fields if Finnhub API returns valid data.
            None if:
                - Symbol not covered by Finnhub (IDX, forex, commodity instruments)
                - API error, timeout, or rate limit exceeded
                - Response is not a dict (malformed)

        Silently returns None — does NOT raise. Caller skips None returns.
        """
        try:
            data = self._client.news_sentiment(symbol)

            # Malformed response guard — Finnhub returns {} for uncovered symbols
            if not isinstance(data, dict):
                return None

            # Guard None values from API — default to 0.0 / 0
            buzz_data        = data.get("buzz") or {}
            sentiment_score  = float(data.get("companyNewsScore") or 0.0)
            buzz_score       = float(buzz_data.get("buzz") or 0.0)
            news_volume_7d   = int(buzz_data.get("articlesInLastWeek") or 0)

            return {
                "symbol":          symbol,
                "sentiment_score": sentiment_score,
                "buzz_score":      buzz_score,
                "news_volume_7d":  news_volume_7d,
                "source":          "finnhub",
                "fetched_date":    str(run_date),
            }

        except Exception as e:
            logger.debug(
                f"[BronzeSentiment] Skipped {symbol}: {type(e).__name__}: {e}"
            )
            return None


def run(run_date: date) -> None:
    """Job entry point — called by job_registry.py _bronze_finnhub_sentiment()."""
    FinnhubSentimentIngester().run(run_date)
