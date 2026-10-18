"""tests/unit/test_audit_bronze_schema_drift.py — FIX SIL-BRZ-SCHEMA-01 census script."""

from datetime import date, timedelta

import polars as pl

from scripts.audit_bronze_schema_drift import (
    find_drift, find_silver_gaps, group_bronze_files, main,
)


def _bronze(root, market, tf, sym, src, fname, ts):
    d = root / market / f"timeframe={tf}" / f"source={src}" / f"symbol={sym}" / "year=2026" / "month=09"
    d.mkdir(parents=True, exist_ok=True)
    n = len(ts)
    pl.DataFrame({"timestamp": ts, "open": [1.0] * n, "high": [1.0] * n,
                  "low": [1.0] * n, "close": [1.0] * n}).write_parquet(d / fname)


def _dates(n=3):
    return [date(2026, 9, 1) + timedelta(days=i) for i in range(n)]


def test_groups_span_sources_like_silver_glob(tmp_path):
    _bronze(tmp_path, "forex", "1D", "EUR_USD", "yfinance", "a.parquet", _dates())
    _bronze(tmp_path, "forex", "1D", "EUR_USD", "alphavantage", "b.parquet", _dates())
    groups = group_bronze_files(tmp_path)
    assert list(groups) == [("forex", "1D", "EUR_USD")]
    assert len(groups[("forex", "1D", "EUR_USD")]) == 2


def test_detects_string_vs_date_drift_and_ignores_uniform(tmp_path):
    _bronze(tmp_path, "context", "1D", "CNH", "alphavantage", "a.parquet", ["2026-09-01"])
    _bronze(tmp_path, "context", "1D", "CNH", "alphavantage", "b.parquet", _dates())
    _bronze(tmp_path, "context", "1D", "OK", "yfinance", "a.parquet", _dates())
    _bronze(tmp_path, "context", "1D", "OK", "yfinance", "b.parquet", _dates())
    drift = find_drift(group_bronze_files(tmp_path))
    assert [d["key"] for d in drift] == [("context", "1D", "CNH")]
    types = set(drift[0]["columns"]["timestamp"])   # polars writes large_string; arrow name varies
    assert len(types) == 2 and "date32[day]" in types


def test_silver_gap_found_and_registry_filter_applied(tmp_path):
    bronze, silver = tmp_path / "b", tmp_path / "s"
    _bronze(bronze, "context", "1D", "CNH", "alphavantage", "a.parquet", _dates())
    _bronze(bronze, "context", "1D", "VIX", "yfinance", "a.parquet", _dates())
    _bronze(bronze, "context", "1D", "OLD", "yfinance", "a.parquet", _dates())
    (silver / "context" / "symbol=VIX").mkdir(parents=True)
    (silver / "context" / "symbol=VIX" / "VIX_1D_silver.parquet").write_bytes(b"x")
    groups = group_bronze_files(bronze)
    assert find_silver_gaps(groups, silver) == [("context", "1D", "CNH"), ("context", "1D", "OLD")]
    assert find_silver_gaps(groups, silver, registry_symbols={"CNH", "VIX"}) == [("context", "1D", "CNH")]


def test_main_exit_codes(tmp_path):
    bronze, silver = tmp_path / "b", tmp_path / "s"
    _bronze(bronze, "context", "1D", "VIX", "yfinance", "a.parquet", _dates())
    args = ["--bronze-root", str(bronze), "--silver-root", str(silver), "--all-symbols"]
    assert main(args) == 1                       # Bronze but no Silver
    (silver / "context" / "symbol=VIX").mkdir(parents=True)
    (silver / "context" / "symbol=VIX" / "VIX_1D_silver.parquet").write_bytes(b"x")
    assert main(args) == 0
