"""
macro_processor.py — Silver Macro Processor (GD §4.5)
Clean Bronze macro series → Silver dengan Point-in-Time integrity.

PIT Integrity (critical for backtest anti-lookahead):
    - vintage_date:  tanggal data ini diingesti (pipeline run date)
    - release_date:  tanggal data pertama kali dipublikasikan
    - is_revision:   True jika nilai berubah dari release sebelumnya
    - revision_seq:  0=initial, 1=first revision, dst.

Prinsip: vintage_date <= trade_date untuk PIT query di backtest.
Silver hanya menyimpan data yang diketahui pada saat itu.

Output: data/silver/macro_enriched/{domain}_{series_id}_silver.parquet
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from src.utils.atomic_io import atomic_write_parquet  # FIX SIL-AIO-002
from loguru import logger

CURRENT_SILVER_VERSION = "1.2"
BRONZE_MACRO_PATH      = Path("data/bronze/macro")
SILVER_MACRO_PATH      = Path("data/silver/macro_enriched")

# FIX F-MP-02 [P2]: Float comparison tolerance for revision detection.
# Direct float equality (value != value_prev) causes false-positive is_revision
# flags due to floating-point round-trip precision loss, especially for rate
# series with many decimal places (e.g. 0.0025 → 0.00250000000001 after
# Parquet serialisation round-trip).  Using absolute tolerance 1e-9 eliminates
# all spurious revisions while still catching genuine BLS/BEA data revisions
# which are typically ≥ 0.01 in their native units.
REVISION_TOLERANCE: float = 1e-9


class MacroProcessor:
    """
    Process Bronze macro Parquet → Silver dengan PIT metadata.
    Satu call per domain (fred, bls, bea, eia, treasury).
    """

    def process_fred(self, run_date: date) -> None:
        """Process semua FRED series dari Bronze."""
        self._process_domain(
            source="fred",
            domain_glob="data/bronze/macro/fred/**/*.parquet",
            run_date=run_date,
        )

    def process_treasury(self, run_date: date) -> None:
        """No-op — Treasury yield curve data is already covered by process_fred().

        FIX MP-1 (HIGH): TreasuryIngester.run() delegates to FREDIngester which
        writes DGS series (T10Y2Y, DGS2, DGS5, DGS10, DGS30, etc.) to:
            data/bronze/macro/fred/monetary_policy/*.parquet
        process_fred() reads data/bronze/macro/fred/**/*.parquet and already
        processes all DGS / yield-curve series in that directory.

        Calling process_treasury() with glob 'data/bronze/bond/treasury/**/*.parquet'
        was dead code — that path is never written by any ingester. Removing this
        call eliminates confusion and avoids misleading 'No data found' warnings.
        process_fred() is the authoritative handler for yield curve series.
        """
        logger.debug(
            "[MacroProcessor] process_treasury() skipped — "
            "DGS series already processed by process_fred() (GD §3.3.1, MP-1 fix)"
        )

    def process_bls(self, run_date: date) -> None:
        """Process BLS data (CPI, PPI, NFP, Unemployment Rate, JOLTS) dari Bronze.

        FIX F-MP-01 [P1]: BLS Bronze data was never processed into Silver.
        BLSIngester (bronze) writes to data/bronze/macro/bls/ but run() never
        called process_bls(), causing a dead-end: Gold MacroRegime could not
        use CPI or NFP for STAGFLATION / REFLATION regime classification.
        These are key regime inputs per GD §5.2.1 and fred_series.yaml.
        """
        self._process_domain(
            source="bls",
            domain_glob="data/bronze/macro/bls/**/*.parquet",
            run_date=run_date,
        )

    def process_bea(self, run_date: date) -> None:
        """Process BEA data (GDP, PCE, Trade Balance) dari Bronze.

        FIX F-MP-01 [P1]: BEA Bronze data was never processed into Silver.
        BEAIngester (bronze) writes to data/bronze/macro/bea/ but run() never
        called process_bea(), leaving GDP Advance Estimate unavailable for
        Gold MacroRegime (GD §5.2.1: GDP score is a primary regime input).
        GDP quarterly release is the key driver of STAGFLATION vs REFLATION
        classification — missing this was a silent critical failure.
        """
        self._process_domain(
            source="bea",
            domain_glob="data/bronze/macro/bea/**/*.parquet",
            run_date=run_date,
        )

    def process_eia(self, run_date: date) -> None:
        """Process EIA crude oil data dari Bronze."""
        self._process_domain(
            source="eia",
            domain_glob="data/bronze/commodity/eia/**/*.parquet",
            run_date=run_date,
        )

    def _process_domain(
        self,
        source: str,
        domain_glob: str,
        run_date: date,
    ) -> None:
        """Generic domain processor.

        FIX S-F02: Filter release_date <= run_date sebelum write ke Silver.
        Data yang release_date-nya di masa depan relatif terhadap run_date
        adalah lookahead bias — HARUS dieksklusi dari Silver output.
        Jika kolom release_date tidak ada di Bronze, gunakan observation_date
        sebagai konservatif proxy (GD §4.5).
        """
        try:
            con = duckdb.connect()
            df = con.execute(
                """
                SELECT *
                FROM read_parquet($glob, hive_partitioning=true)
                ORDER BY series_id, observation_date
                """,  # FIX SIL-SQL-002: $name parameterized (GD §17.7)
                {"glob": domain_glob},
            ).pl()
        except Exception as e:
            logger.warning(f"[MacroProcessor] {source}: no data yet — {e}")
            return

        if df.is_empty():
            logger.debug(f"[MacroProcessor] {source}: empty, skipping")
            return

        # FIX S-F02: PIT lookahead prevention
        # Gunakan release_date jika tersedia; fallback ke _ingested_at atau observation_date
        if "release_date" in df.columns:
            before = len(df)
            df = df.filter(pl.col("release_date") <= pl.lit(run_date.isoformat()))
            dropped = before - len(df)
            if dropped > 0:
                logger.info(
                    f"[MacroProcessor] {source}: PIT filter dropped {dropped} rows "
                    f"(release_date > {run_date})"
                )
        elif "observation_date" in df.columns:
            # Conservative proxy: exclude obs dates beyond run_date
            before = len(df)
            df = df.filter(pl.col("observation_date") <= pl.lit(run_date.isoformat()))
            dropped = before - len(df)
            if dropped > 0:
                logger.debug(
                    f"[MacroProcessor] {source}: observation_date PIT filter "
                    f"dropped {dropped} rows (no release_date column)"
                )

        if df.is_empty():
            logger.debug(f"[MacroProcessor] {source}: empty after PIT filter, skipping")
            return

        # Add PIT metadata
        df = df.with_columns([
            pl.lit(run_date.isoformat()).alias("vintage_date"),
            pl.lit(CURRENT_SILVER_VERSION).alias("processing_version"),
        ])

        # Detect revisions per (series_id, observation_date)
        df = self._detect_revisions(df, source, run_date)

        # Write Silver
        SILVER_MACRO_PATH.mkdir(parents=True, exist_ok=True)
        out_path = SILVER_MACRO_PATH / f"{source}_{run_date.isoformat()}_silver.parquet"
        # FIX SIL-AIO-002: atomic write — corrupt macro Silver breaks regime detection
        atomic_write_parquet(
            df, out_path,
            compression="zstd", compression_level=3, row_group_size=50_000,
        )
        logger.info(
            f"[MacroProcessor] {source}: {len(df):,} rows → {out_path.name}"
        )

    def _detect_revisions(
        self,
        df: pl.DataFrame,
        source: str,
        run_date: date,
    ) -> pl.DataFrame:
        """
        Compare current values against last known Silver vintage.
        Mark is_revision=True, revision_seq where value changed.
        """
        # Find most recent Silver vintage for this source
        prev_path = self._find_latest_silver(source)

        if prev_path is None:
            # Initial release — all rows are revision_seq=0
            return df.with_columns([
                pl.lit(False).alias("is_revision"),
                pl.lit(0).cast(pl.Int16).alias("revision_seq"),
            ])

        try:
            # FIX MP-3 [discovered while writing GAP-8 tests, Production
            # Readiness Assessment v1.7.2 follow-up]: explicitly rename
            # 'prev' columns before the join instead of relying on
            # df.join(..., suffix="_prev"). Polars' `suffix` only applies to
            # columns that COLLIDE between the two frames being joined. At
            # this point `df` (the incoming Bronze data being processed)
            # does not yet have a `revision_seq` column — it's computed
            # below — so there is no collision for `revision_seq`, and
            # Polars left it as the bare name `revision_seq` instead of
            # `revision_seq_prev`. The line below referencing
            # `revision_seq_prev` then raised ColumnNotFoundError on every
            # single call where a previous vintage existed, was caught by
            # the broad `except Exception`, and silently fell back to
            # is_revision=False for every row, every run. This made F-MP-02's
            # REVISION_TOLERANCE comparison dead code — it was never reached.
            # Explicit pre-join renaming removes the ambiguity entirely:
            # 'value' and 'revision_seq' are renamed up front, so the join
            # needs no suffix and the joined frame always has exactly the
            # columns this method expects, regardless of what `df` contains.
            # FIX SIL-RPQ-001: lazy scan for single Silver vintage file
            prev = pl.scan_parquet(str(prev_path)).collect().select([
                "series_id", "observation_date", "value", "revision_seq"
            ]).rename({"value": "value_prev", "revision_seq": "revision_seq_prev"})

            # Join on (series_id, observation_date) to detect value changes
            joined = df.join(
                prev,
                on=["series_id", "observation_date"],
                how="left",
            )

            # is_revision: value changed from previous vintage
            # FIX F-MP-02 [P2]: use REVISION_TOLERANCE instead of direct !=
            # to prevent false-positive revisions from float round-trip error.
            # BEFORE: pl.col("value") != pl.col("value_prev")
            # AFTER:  abs(value - value_prev) > REVISION_TOLERANCE
            joined = joined.with_columns([
                (
                    pl.col("value_prev").is_not_null()
                    & (
                        (pl.col("value") - pl.col("value_prev")).abs()
                        > REVISION_TOLERANCE
                    )
                ).alias("is_revision"),
                (
                    pl.col("revision_seq_prev")
                    .fill_null(0)
                    .cast(pl.Int16)
                    + pl.when(
                        pl.col("value_prev").is_not_null()
                        & (
                            (pl.col("value") - pl.col("value_prev")).abs()
                            > REVISION_TOLERANCE
                        )
                    ).then(1).otherwise(0).cast(pl.Int16)
                ).alias("revision_seq"),
            ]).drop(["value_prev", "revision_seq_prev"])

            return joined

        except Exception as e:
            logger.warning(f"[MacroProcessor] Revision detection failed: {e}")
            return df.with_columns([
                pl.lit(False).alias("is_revision"),
                pl.lit(0).cast(pl.Int16).alias("revision_seq"),
            ])

    def _find_latest_silver(self, source: str) -> Optional[Path]:
        """Find most recent Silver file for this source."""
        pattern = SILVER_MACRO_PATH.glob(f"{source}_*_silver.parquet")
        files   = sorted(pattern)
        return files[-1] if files else None


def run(run_date: date) -> None:
    """Job entry point.

    FIX F-MP-01 [P1]: Added process_bls() and process_bea() calls.
    Previously run() only called process_fred() and process_eia(), leaving
    BLS (CPI, PPI, NFP) and BEA (GDP, PCE) dead-ends in Bronze that were
    never promoted to Silver — silently breaking MacroRegime classification.

    Order: FRED first (largest dataset, DGS series), then BLS, BEA, EIA.
    process_treasury() is a documented no-op (DGS series already in FRED).
    """
    proc = MacroProcessor()
    proc.process_fred(run_date)
    proc.process_bls(run_date)      # FIX F-MP-01: was missing
    proc.process_bea(run_date)      # FIX F-MP-01: was missing
    proc.process_treasury(run_date)
    proc.process_eia(run_date)
    logger.info(f"[silver_macro] Complete for {run_date}")
