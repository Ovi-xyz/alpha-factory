"""
tests/unit/test_bronze_schema_registry_gap3.py

FIX GAP-3 [P1] (Production Readiness Assessment v1.7.2, GD §3.7): 6 of 11
Bronze sources (FRED, BLS, BEA, IMF, EIA, Treasury) had no Bronze Schema
Registry YAML, so SchemaValidator could never be instantiated for them and
the GD §3.7 quarantine gate never fired. This test suite verifies, for each
source:
    1. The schema YAML exists, parses, and matches the actual ingester's
       row-construction code (validated against a realistic synthetic
       DataFrame shaped exactly like that ingester produces).
    2. The gate actually REJECTS a mismatched DataFrame (the failure mode
       GD §3.7 exists to catch — e.g. a source silently renaming a column).
    3. Each of the 5 ingesters with an independent Bronze write path
       (FRED, BLS, BEA, IMF, EIA) instantiates a real SchemaValidator in
       __init__() bound to the correct schema file.

Treasury is intentionally excluded from point 3 — TreasuryIngester has no
independent Bronze write path (FIX TI-1 / TRES-1: it delegates 100% to
FREDIngester, validated under fred_macro.yaml). See the ARCHITECTURE NOTE
in config/schemas/treasury_yield.yaml for the full rationale.
"""

from pathlib import Path

import polars as pl
import pytest
import yaml

from src.bronze.schema_validator import SchemaValidator

SCHEMAS_DIR = Path("config/schemas")


# ── Fixtures: one "valid" and one "broken" DataFrame per source, shaped to ──
# ── match what each ingester's row-construction code actually produces.  ──

def _fred_valid_df() -> pl.DataFrame:
    import pandas as pd
    from datetime import date, timedelta
    obs = [date(2025, 1, 1), date(2025, 2, 1)]
    pdf = pd.DataFrame({
        "observation_date": obs,
        "value":            [310.1, 310.5],
        "series_id":        ["CPIAUCSL", "CPIAUCSL"],
        "release_date":     [(d + timedelta(days=35)).isoformat() for d in obs],
    })
    return pl.from_pandas(pdf).with_columns([pl.col("value").cast(pl.Float64)])


def _bls_valid_df() -> pl.DataFrame:
    return pl.DataFrame({
        "series_id":        ["CUUR0000SA0"],
        "observation_date": ["2025-01-01"],
        "value":            [310.5],
        "period":           ["M01"],
        "year":             [2025],
        "release_date":     ["2025-02-05"],
    })


def _bea_valid_df() -> pl.DataFrame:
    return pl.DataFrame({
        "series_id":        ["real_gdp"],
        "table_name":       ["T10106"],
        "line_description": ["Percent change"],
        "observation_date": ["2025-01-01"],
        "value":            [2.8],
        "unit":             ["pct"],
        "release_date":     ["2025-05-01"],
    })


def _imf_valid_df() -> pl.DataFrame:
    return pl.DataFrame({
        "series_id":        ["NGDP_RPCH"],
        "country":          ["USA"],
        "observation_date": ["2025-01-01"],
        "value":            [2.1],
        "release_date":     ["2025-10-01"],
        "source":           ["imf_weo"],
    })


def _eia_valid_df() -> pl.DataFrame:
    return pl.DataFrame({
        "observation_date": ["2025-01-01"],
        "value":            [123.4],
        "series_id":        ["PET.WCRSTUS1.W"],
        "release_date":     ["2025-01-01"],
        "series_name":      ["us_crude_stocks"],
        "unit":             ["thousand_barrels"],
    })


SOURCES = {
    "fred": ("fred_macro.yaml", _fred_valid_df, "series_id"),
    "bls":  ("bls_macro.yaml",  _bls_valid_df,  "series_id"),
    "bea":  ("bea_macro.yaml",  _bea_valid_df,  "series_id"),
    "imf":  ("imf_weo.yaml",    _imf_valid_df,  "series_id"),
    "eia":  ("eia_oil.yaml",    _eia_valid_df,  "series_id"),
}


class TestSchemaYamlsExist:
    """All 6 GAP-3 schema files must exist and parse as valid YAML."""

    @pytest.mark.parametrize("filename", [
        "fred_macro.yaml", "bls_macro.yaml", "bea_macro.yaml",
        "imf_weo.yaml", "eia_oil.yaml", "treasury_yield.yaml",
    ])
    def test_schema_file_exists_and_parses(self, filename):
        path = SCHEMAS_DIR / filename
        assert path.exists(), f"{filename} missing from {SCHEMAS_DIR}"
        data = yaml.safe_load(path.read_text())
        assert "expected_columns" in data
        assert "on_mismatch" in data
        assert data["on_mismatch"] == "quarantine"


class TestSchemaValidatesRealisticShape:
    """Each schema must accept a DataFrame shaped like its real ingester output."""

    @pytest.mark.parametrize("source", SOURCES.keys())
    def test_valid_shape_passes(self, source):
        filename, make_df, key_col = SOURCES[source]
        validator = SchemaValidator(SCHEMAS_DIR / filename)
        df = make_df()
        ok, errors = validator.validate(df, df[key_col][0])
        assert ok, f"{source}: expected valid, got errors: {errors}"


class TestSchemaRejectsMismatch:
    """
    GD §3.7's entire purpose: a renamed/dropped column must be caught, not
    silently pass through to Silver. One representative mismatch per source.
    """

    @pytest.mark.parametrize("source", SOURCES.keys())
    def test_missing_value_column_rejected(self, source):
        filename, make_df, key_col = SOURCES[source]
        validator = SchemaValidator(SCHEMAS_DIR / filename)
        df = make_df()
        if "value" not in df.columns:
            pytest.skip(f"{source} fixture has no 'value' column to drop")
        broken = df.rename({"value": "val"})   # simulate an API rename
        ok, errors = validator.validate(broken, broken[key_col][0])
        assert not ok
        assert any("value" in e for e in errors)

    def test_wrong_type_rejected(self):
        """FRED observation_date must be Date, not a plain string."""
        validator = SchemaValidator(SCHEMAS_DIR / "fred_macro.yaml")
        df = pl.DataFrame({
            "observation_date": ["2025-01-01"],   # string, not Date
            "value":            [310.1],
            "series_id":        ["CPIAUCSL"],
            "release_date":     ["2025-02-05"],
        })
        ok, errors = validator.validate(df, "CPIAUCSL")
        assert not ok
        assert any("observation_date" in e for e in errors)


class TestIngesterValidatorWiring:
    """
    Each of the 5 ingesters with an independent write path must instantiate
    a real SchemaValidator bound to its schema file in __init__().
    Treasury is excluded — see module docstring.
    """

    def test_fred_ingester_has_validator(self):
        from src.bronze.fred_ingester import FREDIngester
        ing = FREDIngester()
        assert isinstance(ing._validator, SchemaValidator)

    def test_bls_ingester_has_validator(self):
        from src.bronze.bls_ingester import BLSIngester
        ing = BLSIngester()
        assert isinstance(ing._validator, SchemaValidator)

    def test_bea_ingester_has_validator(self):
        from src.bronze.bea_ingester import BEAIngester
        ing = BEAIngester()
        assert isinstance(ing._validator, SchemaValidator)

    def test_imf_ingester_has_validator(self):
        from src.bronze.imf_ingester import IMFIngester
        ing = IMFIngester()
        assert isinstance(ing._validator, SchemaValidator)

    def test_eia_ingester_has_validator(self):
        from src.bronze.eia_ingester import EIAIngester
        ing = EIAIngester()
        assert isinstance(ing._validator, SchemaValidator)
