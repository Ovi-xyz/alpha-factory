"""
sentiment_processor.py — Silver Sentiment Processor (GD §4.6)
Pure Bronze→Silver transform. Zero external API calls.

REFACTORED: Eliminates architectural debt per GD §17.7 anti-pattern.

    BEFORE (v1.5 — debt):
        SentimentProcessor directly called finnhub.Client().news_sentiment().
        Violation: GD §17.7 "Silver memanggil external API untuk enrichment utama".
        Scope: ~200 active symbols — artificial constraint from debt
               (Silver needed silver_active_symbols output to limit API calls).
        Idempotency FAILED: Silver output depended on Finnhub API state at run time,
               not on Bronze snapshot. Replay from Bronze → different result.

    AFTER (this module — post-refactor):
        SentimentProcessor reads Bronze Parquet written by bronze_finnhub_sentiment.
        Zero network access — pure filesystem read + transform.
        Scope: all symbols covered by Bronze (643 universe, Finnhub covers US stocks).
        Idempotency GUARANTEED: identical Bronze input → identical Silver output.
        Replay guarantee: Silver can run from Bronze snapshot with no network access.

depends_on: [bronze_finnhub_sentiment]
    NOT silver_active_symbols — eliminating lateral Silver coupling (GD §17.7).

Input  : data/bronze/market/fundamental/sentiment/**/*.parquet
         (written by FinnhubSentimentIngester via BronzeIngester.write())
         Filter: fetched_date == run_date (in-data column, not hive partition)

Output : data/silver/sentiment/date={date}/sentiment_silver.parquet

Silver Sentiment Schema (GD §4.6 — UNCHANGED from v1.5):
    symbol          : String   — normalized ticker
    date            : String   — run_date ISO format (YYYY-MM-DD)
    sentiment_score : Float64  — companyNewsScore from Finnhub
    buzz_score      : Float64  — relative news volume vs 30D average
    news_volume_7d  : Int64    — articles in last 7 days
    source          : String   — "finnhub"

Gold Screener (GD §5.2.4) uses LEFT JOIN on this output.
Schema is unchanged — Gold Screener requires NO modification after this refactor.

GD References:
    §4.6  — Sentiment Silver Sub-layer spec (schema definition)
    §17.2 — Layer Independence Guarantee (Silver reads only from Bronze)
    §17.4 — Silver catalog: SentimentProcessor responsibility definition
    §17.7 — Anti-pattern eliminated: Silver no longer calls external API
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

# Bronze read path — root of BronzeIngester.write() output for sentiment
# BronzeIngester.write(asset_class="market/fundamental/sentiment", source="finnhub",
#                      symbol="sentiment_{date}") produces:
#   data/bronze/market/fundamental/sentiment/source=finnhub/
#   symbol=sentiment_{date}/year={Y}/month={M}/sentiment_{date}_raw_{ts}.parquet
# Silver reads with ** glob and filters by fetched_date (in-data column).
BRONZE_SENTIMENT_PATH: Path = Path("data/bronze/market/fundamental/sentiment")

# Silver write path (GD §4.6, §4.2 — unchanged from v1.5)
SILVER_SENTIMENT_PATH: Path = Path("data/silver/sentiment")


class SentimentProcessor:
    """
    Pure Bronze→Silver transform for Finnhub news sentiment data.

    Zero API calls. Zero Silver-to-Silver coupling.
    Silver is idempotent: re-run with same Bronze data produces identical output.
    Replay guarantee: can run from Bronze snapshot without any network access.
    """

    def run(self, run_date: date) -> None:
        """
        Read Bronze sentiment for run_date, transform to Silver schema, and write.

        Bronze path is scanned with ** glob (no hive_partitioning to avoid
        column name conflicts with the 'symbol' data column vs hive partition key).
        Filtering by fetched_date (in-data string column) selects the correct run_date.
        """
        if not BRONZE_SENTIMENT_PATH.exists():
            logger.warning(
                f"[SilverSentiment] Bronze sentiment root path does not exist: "
                f"{BRONZE_SENTIMENT_PATH}. "
                f"Run bronze_finnhub_sentiment first to populate Bronze."
            )
            return

        bronze_glob = str(BRONZE_SENTIMENT_PATH / "**" / "*.parquet")

        try:
            df = (
                pl.scan_parquet(bronze_glob, hive_partitioning=False)
                .filter(pl.col("fetched_date") == str(run_date))
                .collect()
            )
        except Exception as e:
            logger.warning(
                f"[SilverSentiment] Cannot read Bronze sentiment for {run_date}: {e}. "
                f"Ensure bronze_finnhub_sentiment ran successfully."
            )
            return

        if df.is_empty():
            logger.warning(
                f"[SilverSentiment] No Bronze sentiment data found for {run_date}. "
                f"Ensure bronze_finnhub_sentiment ran successfully for this date."
            )
            return

        df_silver = self._transform(df, run_date)

        if df_silver.is_empty():
            logger.warning(
                f"[SilverSentiment] Transform produced empty DataFrame for {run_date}. "
                f"All records may have had null sentiment_score."
            )
            return

        self._write(df_silver, run_date)

        logger.info(
            f"[SilverSentiment] Written {len(df_silver)} records | run_date={run_date}"
        )

    def _transform(self, df: pl.DataFrame, run_date: date) -> pl.DataFrame:
        """
        Normalize Bronze schema → Silver sentiment schema (GD §4.6).

        Operations:
            1. Validate required Bronze columns are present.
            2. Filter out records where sentiment_score is null.
            3. Select Silver schema columns — drop Bronze metadata (_source, _ingested_at, _symbol).
            4. Add 'date' column (Silver schema) = str(run_date).
            5. Cast to exact Silver schema types.

        Returns:
            pl.DataFrame with Silver sentiment schema columns.
            Returns empty DataFrame (correct schema) if required columns are missing.
        """
        required_cols = {"symbol", "sentiment_score", "buzz_score",
                         "news_volume_7d", "source"}
        available     = set(df.columns)
        missing       = required_cols - available

        if missing:
            logger.error(
                f"[SilverSentiment] Bronze data missing required columns: {missing}. "
                f"Available columns: {list(available)}. "
                f"Check finnhub_sentiment.yaml schema and Bronze ingester output."
            )
            # Return empty DataFrame with correct Silver schema (GD §17.6 contract)
            return pl.DataFrame(schema={
                "symbol":          pl.String,
                "date":            pl.String,
                "sentiment_score": pl.Float64,
                "buzz_score":      pl.Float64,
                "news_volume_7d":  pl.Int64,
                "source":          pl.String,
            })

        return (
            df
            # Filter: skip symbols where Finnhub returned no score
            .filter(pl.col("sentiment_score").is_not_null())
            # Select and cast to exact Silver schema — Bronze metadata columns dropped
            .select([
                pl.col("symbol").cast(pl.String),
                pl.lit(str(run_date)).alias("date"),
                pl.col("sentiment_score").cast(pl.Float64),
                pl.col("buzz_score").cast(pl.Float64),
                pl.col("news_volume_7d").cast(pl.Int64),
                pl.col("source").cast(pl.String),
            ])
        )

    def _write(self, df: pl.DataFrame, run_date: date) -> None:
        """
        Write Silver sentiment Parquet (date-partitioned).
        Overwrites existing file on re-run — Silver is idempotent by design.
        """
        out_dir = SILVER_SENTIMENT_PATH / f"date={run_date.isoformat()}"
        out_dir.mkdir(parents=True, exist_ok=True)

        # FIX SIL-AIO-004: atomic write — sentiment consumed by Gold Screener
        # use_pyarrow=False: original write had no pyarrow dep; preserve behaviour
        from src.utils.atomic_io import atomic_write_parquet
        atomic_write_parquet(
            df,
            out_dir / "sentiment_silver.parquet",
            compression="zstd",
            compression_level=3,
            use_pyarrow=False,
        )


def run(run_date: date) -> None:
    """Job entry point — called by job_registry.py _silver_sentiment()."""
    SentimentProcessor().run(run_date)
