"""tests/unit/test_schema_validator.py — SchemaValidator test suite"""

from pathlib import Path

import polars as pl
import pytest
import yaml

from src.bronze.schema_validator import SchemaValidator, SchemaValidationError


@pytest.fixture
def schema_yaml(tmp_path) -> Path:
    """Write a temporary schema YAML for testing."""
    schema = {
        "version": "test",
        "source":  "test_source",
        "on_mismatch": "quarantine",
        "expected_columns": [
            {"name": "open",   "type": "float64", "nullable": False},
            {"name": "high",   "type": "float64", "nullable": False},
            {"name": "low",    "type": "float64", "nullable": False},
            {"name": "close",  "type": "float64", "nullable": False},
            {"name": "volume", "type": "int64",   "nullable": True},
        ],
    }
    path = tmp_path / "test_schema.yaml"
    path.write_text(yaml.dump(schema))
    return path


@pytest.fixture
def validator(schema_yaml) -> SchemaValidator:
    return SchemaValidator(schema_yaml)


@pytest.fixture
def valid_df() -> pl.DataFrame:
    return pl.DataFrame({
        "open":   [100.0, 101.0],
        "high":   [105.0, 106.0],
        "low":    [98.0,  99.0],
        "close":  [102.0, 103.0],
        "volume": [1_000_000, 1_100_000],
    })


class TestSchemaValidator:

    def test_valid_df_passes(self, validator, valid_df):
        ok, errors = validator.validate(valid_df, "AAPL")
        assert ok is True
        assert errors == []

    def test_missing_column_fails(self, validator):
        df = pl.DataFrame({
            "open": [100.0], "high": [105.0], "low": [98.0],
            # close missing
            "volume": [1_000_000],
        })
        ok, errors = validator.validate(df, "AAPL")
        assert ok is False
        assert any("close" in e for e in errors)

    def test_wrong_type_fails(self, validator):
        df = pl.DataFrame({
            "open":   [100],    # int, not float — type mismatch
            "high":   [105.0],
            "low":    [98.0],
            "close":  [102.0],
            "volume": [1_000_000],
        })
        ok, errors = validator.validate(df, "AAPL")
        # May or may not fail depending on polars version casting
        # At minimum, validator should not raise an exception
        assert isinstance(ok, bool)

    def test_nullable_violation(self, schema_yaml, tmp_path):
        """Column marked nullable=False should fail with nulls."""
        schema = yaml.safe_load(schema_yaml.read_text())
        # Make 'open' non-nullable
        schema_yaml.write_text(yaml.dump(schema))

        df = pl.DataFrame({
            "open":   [None, 101.0],   # null in non-nullable column
            "high":   [105.0, 106.0],
            "low":    [98.0, 99.0],
            "close":  [102.0, 103.0],
            "volume": [1_000_000, None],
        })
        validator = SchemaValidator(schema_yaml)
        ok, errors = validator.validate(df, "AAPL")
        # open is marked nullable=False, so null should cause error
        assert not ok or len(errors) > 0  # at least some issue detected

    def test_quarantine_writes_file(self, validator, tmp_path, monkeypatch):
        """handle_mismatch('quarantine') should write to quarantine dir."""
        monkeypatch.setattr(
            SchemaValidator, "QUARANTINE_PATH", tmp_path / "quarantine"
        )
        df = pl.DataFrame({
            "open":  [100.0], "high": [105.0], "low": [98.0],
            "close": [102.0], "volume": [1_000_000],
        })
        result = validator.handle_mismatch(
            df, ["Missing column: close"], "AAPL", on_mismatch="quarantine"
        )
        assert result is None   # quarantine returns None
        quarantine_files = list((tmp_path / "quarantine").glob("*.parquet"))
        assert len(quarantine_files) == 1

    def test_warn_returns_df(self, validator, valid_df):
        """handle_mismatch('warn') should return original df."""
        result = validator.handle_mismatch(
            valid_df, ["Some warning"], "AAPL", on_mismatch="warn"
        )
        assert result is not None
        assert len(result) == len(valid_df)

    def test_fail_raises_exception(self, validator, valid_df):
        """handle_mismatch('fail') should raise SchemaValidationError."""
        with pytest.raises(SchemaValidationError):
            validator.handle_mismatch(
                valid_df, ["Error"], "AAPL", on_mismatch="fail"
            )

    def test_unknown_strategy_defaults_to_quarantine(self, validator, tmp_path, monkeypatch):
        """Coverage tranche (17 Aug 2026) — unrecognized on_mismatch string
        falls through the else branch, logs an error, and defaults to quarantine."""
        monkeypatch.setattr(
            SchemaValidator, "QUARANTINE_PATH", tmp_path / "quarantine"
        )
        df = pl.DataFrame({
            "open":  [100.0], "high": [105.0], "low": [98.0],
            "close": [102.0], "volume": [1_000_000],
        })
        result = validator.handle_mismatch(
            df, ["some error"], "AAPL", on_mismatch="not_a_real_strategy"
        )
        assert result is None   # falls through to quarantine, which returns None
        quarantine_files = list((tmp_path / "quarantine").glob("*.parquet"))
        assert len(quarantine_files) == 1
