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


def _bronze_tz(root, sym, fname, zone, days=(1, 2, 3), tf="1D", market="context"):
    from datetime import datetime
    d = root / market / f"timeframe={tf}" / "source=yfinance" / f"symbol={sym}" / "year=2026" / "month=09"
    d.mkdir(parents=True, exist_ok=True)
    ts = pl.Series("timestamp", [datetime(2026, 9, x) for x in days], dtype=pl.Datetime("ms")).dt.replace_time_zone(zone)
    n = len(days)
    pl.DataFrame({"timestamp": ts, "open": [1.0] * n, "high": [1.0] * n,
                  "low": [1.0] * n, "close": [1.0] * n}).write_parquet(d / fname)


def test_names_the_minority_zone_files(tmp_path):
    """FIX SIL-BRZ-SCHEMA-02: two tz-aware zones in one group -> the odd file(s) are named."""
    for i in range(3):
        _bronze_tz(tmp_path, "VIX", f"chi_{i}.parquet", "America/Chicago")
    _bronze_tz(tmp_path, "VIX", "ny_0.parquet", "America/New_York")
    _bronze_tz(tmp_path, "SPX", "a.parquet", "America/New_York")       # uniform group: not reported
    _bronze_tz(tmp_path, "SPX", "b.parquet", "America/New_York")
    drift = find_drift(group_bronze_files(tmp_path))
    assert [d["key"] for d in drift] == [("context", "1D", "VIX")]
    assert drift[0]["zone_minority"] == [("ny_0.parquet", "timestamp[ms, tz=America/New_York]")]


def test_zone_minority_tie_is_alphabetical_and_absent_without_two_zones(tmp_path):
    _bronze_tz(tmp_path, "T", "a.parquet", "America/Chicago")
    _bronze_tz(tmp_path, "T", "b.parquet", "America/New_York")
    (drift,) = find_drift(group_bronze_files(tmp_path))
    assert [n for n, _ in drift["zone_minority"]] == ["b.parquet"]     # Chicago sorts first -> majority
    # String vs Date drift has no tz-aware zone at all -> no zone_minority key
    _bronze(tmp_path, "context", "1D", "CNH", "alphavantage", "a.parquet", ["2026-09-01"])
    _bronze(tmp_path, "context", "1D", "CNH", "alphavantage", "b.parquet", _dates())
    cnh = [d for d in find_drift(group_bronze_files(tmp_path)) if d["key"][2] == "CNH"][0]
    assert "zone_minority" not in cnh


def test_main_prints_the_minority_file(tmp_path, capsys):
    bronze, silver = tmp_path / "b", tmp_path / "s"
    for i in range(2):
        _bronze_tz(bronze, "VIX", f"chi_{i}.parquet", "America/Chicago")
    _bronze_tz(bronze, "VIX", "ny_odd.parquet", "America/New_York")
    main(["--bronze-root", str(bronze), "--silver-root", str(silver), "--all-symbols"])
    out = capsys.readouterr().out
    assert "minority zone: ny_odd.parquet" in out and "tz=America/New_York" in out
