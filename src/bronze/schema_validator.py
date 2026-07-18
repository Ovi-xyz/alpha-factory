"""
schema_validator.py — GD §3.7 (NEW v1.2)
Schema Registry validation untuk Bronze ingestion.

Setiap ingestion divalidasi terhadap expected schema sebelum write ke Bronze.
Mismatch → quarantine + alert, bukan silent-fail atau data corrupt di Gold.

Motivation: yfinance, tvdatafeed, Finnhub pernah mengubah nama/tipe kolom
output tanpa notice. Tanpa schema registry, Silver processor akan crash atau
— lebih buruk — menghasilkan data corrupt yang lolos ke Gold layer.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl
from src.utils.atomic_io import atomic_write_parquet  # FIX BRZ-AIO-001
import yaml
from loguru import logger


class SchemaValidationError(Exception):
    """Raised saat schema mismatch dengan on_mismatch='fail'."""
    pass


class SchemaValidator:
    """
    Validate DataFrame incoming terhadap YAML schema registry.
    Quarantine jika mismatch. Tidak tahu apa-apa tentang Silver.

    Usage:
        validator = SchemaValidator("config/schemas/yfinance_ohlcv.yaml")
        ok, errors = validator.validate(df, symbol="AAPL")
        if not ok:
            validator.handle_mismatch(df, errors, "AAPL", on_mismatch="quarantine")
    """

    QUARANTINE_PATH: Path = Path("data/quarantine")

    def __init__(self, schema_path: str | Path) -> None:
        with open(schema_path) as f:
            self.schema = yaml.safe_load(f)
        self._source = self.schema.get("source", "unknown")
        self._on_mismatch = self.schema.get("on_mismatch", "quarantine")

    def validate(
        self,
        df: pl.DataFrame,
        symbol: str,
    ) -> tuple[bool, list[str]]:
        """
        Return (is_valid, error_list).
        is_valid=True means schema matches.
        """
        errors: list[str] = []

        for col_spec in self.schema.get("expected_columns", []):
            name     = col_spec["name"]
            nullable = col_spec.get("nullable", True)

            # Check column existence
            if name not in df.columns:
                errors.append(f"Missing column: '{name}'")
                continue

            # FIX SV-1 (HIGH): exact type match (case-insensitive), not prefix match.
            # Previous: actual.startswith(expected.split("64")[0]) where prefix="float"
            # matched Float32 for Float64 — 7 vs 15-16 digit precision difference.
            # For financial data (OHLCV), Float32 can cause rounding errors on
            # arithmetic operations (dollar_volume = close * volume at large volumes).
            actual_norm   = str(df[name].dtype).lower()
            expected_norm = str(col_spec["type"]).lower()
            # Polars dtype strings: "float64", "int64", etc. (already lowercase)
            if actual_norm != expected_norm:
                errors.append(
                    f"Column '{name}': expected {col_spec['type']},"
                    f" got {df[name].dtype} — exact type required (not prefix match)"
                )

            # Check nullability
            if not nullable and df[name].null_count() > 0:
                null_count = df[name].null_count()
                errors.append(
                    f"Column '{name}': not nullable but has {null_count} nulls"
                )

        return len(errors) == 0, errors

    def handle_mismatch(
        self,
        df: pl.DataFrame,
        errors: list[str],
        symbol: str,
        on_mismatch: Optional[str] = None,
    ) -> Optional[pl.DataFrame]:
        """
        Handle schema mismatch berdasarkan on_mismatch strategy.

        Strategies:
          'quarantine': Write ke data/quarantine/, log ERROR, return None
          'warn':       Log WARNING, return df as-is (use cautiously)
          'fail':       Raise SchemaValidationError (use in testing)
        """
        strategy = on_mismatch or self._on_mismatch

        if strategy == "quarantine":
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            quarantine_file = (
                self.QUARANTINE_PATH
                / f"{symbol}_{self._source}_{ts}.parquet"
            )
            quarantine_file.parent.mkdir(parents=True, exist_ok=True)

            # Annotate quarantined file with error info
            error_str = " | ".join(errors)
            # FIX BRZ-AIO-001: atomic quarantine write (Bronze snappy, GD §7.1)
            atomic_write_parquet(
                df.with_columns(pl.lit(error_str).alias("_quarantine_reason")),
                quarantine_file,
                compression="snappy", compression_level=None,
                row_group_size=100_000, statistics=False, use_pyarrow=False,
            )

            logger.error(
                f"[SchemaValidator] QUARANTINED {symbol} ({self._source})"
                f" → {quarantine_file.name}"
            )
            for e in errors:
                logger.error(f"  Schema error: {e}")
            return None

        elif strategy == "warn":
            logger.warning(
                f"[SchemaValidator] Schema mismatch for {symbol}"
                f" ({self._source}): {errors}"
            )
            return df

        elif strategy == "fail":
            raise SchemaValidationError(
                f"Schema validation failed for {symbol}: {errors}"
            )

        else:
            logger.error(
                f"[SchemaValidator] Unknown on_mismatch strategy: {strategy!r}"
                " — defaulting to quarantine"
            )
            return self.handle_mismatch(df, errors, symbol, "quarantine")
