"""
FIX SIL-AUDIT-OHLCV-01 [chat thread, 14 Nov 2026]: scripts/audit_silver_ohlcv.py — the thorough, read-only audit
of the Silver OHLCV tree. Fixtures are HEALTHY by default so every test reaches exactly one branch; the two
shapes found on live Silver (7 symbols with 1M fragments and no month-start bar, ABBV with a Tuesday-anchored 1W
series) are pinned as regressions.
"""
from __future__ import annotations

import ast
import json
import math
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import audit_silver_ohlcv as A  # noqa: E402

NY = "America/New_York"
CURRENT = A.CURRENT_SILVER_VERSION


# ── fixtures ────────────────────────────────────────────────────────────────────────────────────

def silver_frame(stamps, o=None, h=None, l=None, c=None, v=None, *, tz=NY, tf="1D", symbol="X",
                 version=CURRENT) -> pl.DataFrame:
    """A GD §4.3 Silver frame that passes every check: sane OHLC, derived fields recomputed from OHLCV."""
    n = len(stamps)
    c = list(c) if c is not None else [100.0 + 0.5 * i for i in range(n)]
    o = list(o) if o is not None else c
    h = list(h) if h is not None else [x * 1.01 for x in c]
    l = list(l) if l is not None else [x * 0.99 for x in c]
    v = list(v) if v is not None else [1000] * n
    ts = pl.Series("timestamp", [datetime.fromisoformat(s) if isinstance(s, str) else s for s in stamps],
                   dtype=pl.Datetime("ms"))
    if tz:
        ts = ts.dt.replace_time_zone(tz)
    df = pl.DataFrame({"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v},
                      schema_overrides={"volume": pl.Int64, "open": pl.Float64, "high": pl.Float64,
                                        "low": pl.Float64, "close": pl.Float64})
    return df.with_columns(
        (pl.col("close") / pl.col("close").shift(1)).log().alias("log_return"),
        (pl.col("close") * pl.col("volume")).alias("dollar_volume"),
        ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("spread_hl"),
        pl.col("close").alias("vwap"), pl.lit(True).alias("is_adjusted"), pl.lit(1.0).alias("adj_factor"),
        pl.lit(True).alias("is_clean"), pl.lit(symbol).alias("symbol"), pl.lit(tf).alias("timeframe"),
        pl.lit("yfinance").alias("data_source"), pl.lit(version).alias("processing_version"))


def put(root: Path, market: str, sym: str, tf: str, df: pl.DataFrame, name_sym: str | None = None,
        identity: bool = True) -> Path:
    if identity:     # the symbol / timeframe columns follow the path unless a test is about the mismatch
        df = df.with_columns(pl.lit(sym).alias("symbol"), pl.lit(tf).alias("timeframe"))
    d = root / market / f"symbol={sym}"
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{name_sym or sym}_{tf}_silver.parquet"
    df.write_parquet(path)
    return path


def put_frame(root, market, sym, tf, stamps, **kw):
    kw.setdefault("symbol", sym)
    return put(root, market, sym, tf, silver_frame(stamps, tf=tf, **kw))


def business_days(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def days_until(start: date, end: date) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def daily_rows(days: list[date]) -> list[dict]:
    rows = []
    for i, d in enumerate(days):
        c = 100 + i * 0.37 + ((i * 7) % 5) * 0.2
        o = c - 0.1
        rows.append(dict(day=d, o=o, h=max(o, c) + 0.3, l=min(o, c) - 0.3, c=c, v=1000 + i))
    return rows


def aggregate(rows: list[dict], key_fn) -> dict:
    groups: dict = {}
    for r in rows:
        groups.setdefault(key_fn(r["day"]), []).append(r)
    stamps = sorted(groups)
    return dict(
        stamps=[datetime.combine(k, time(0)) for k in stamps],
        o=[groups[k][0]["o"] for k in stamps], h=[max(x["h"] for x in groups[k]) for k in stamps],
        l=[min(x["l"] for x in groups[k]) for k in stamps], c=[groups[k][-1]["c"] for k in stamps],
        v=[sum(x["v"] for x in groups[k]) for k in stamps])


def monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def tuesday_block(d: date) -> date:          # Tue..Mon blocks stamped Tuesday (the ABBV shape)
    return d - timedelta(days=(d.weekday() - 1) % 7)


def sunday_week(d: date) -> date:            # Sun..Sat week stamped Sunday (forex)
    return d - timedelta(days=(d.weekday() + 1) % 7)


def first_of_month(d: date) -> date:
    return d.replace(day=1)


def build_symbol(root: Path, market: str, sym: str, key_fn=monday, tf="1W", n_days=330, tz=NY,
                 start=date(2026, 1, 5), tamper=None):
    """1D + a coarse file aggregated from it with ``key_fn``; ``tamper(agg)`` may edit the bars."""
    days = business_days(start, n_days)
    rows = daily_rows(days)
    put_frame(root, market, sym, "1D", [datetime.combine(d, time(0)) for d in days], tz=tz,
              o=[r["o"] for r in rows], h=[r["h"] for r in rows], l=[r["l"] for r in rows],
              c=[r["c"] for r in rows], v=[r["v"] for r in rows])
    agg = aggregate(rows, key_fn)
    if tamper:
        tamper(agg)
    put_frame(root, market, sym, tf, agg["stamps"], tz=tz, o=agg["o"], h=agg["h"], l=agg["l"], c=agg["c"],
              v=agg["v"])
    return days, agg


def audit(root: Path, checks=A.CHECK_ORDER, tfs=A.COARSE_TFS, **kw) -> A.AuditResult:
    opts = A.Options(silver_root=root, checks=tuple(checks), use_registry=False, **kw)
    return A.run_audit(opts, A.discover(root, tfs))


def got(res: A.AuditResult, check: str, sev: str | None = None, symbol: str | None = None):
    return [f for f in res.findings if f.check == check and (sev is None or f.severity == sev)
            and (symbol is None or f.symbol == symbol)]


# ── discovery ───────────────────────────────────────────────────────────────────────────────────

class TestDiscover:

    def test_symbols_with_underscores_keep_their_timeframe(self, tmp_path):
        put_frame(tmp_path, "forex", "EUR_USD", "1W", ["2026-11-02"])
        put_frame(tmp_path, "index_futures", "YM_F", "1M", ["2026-11-01"])
        files = A.discover(tmp_path, None)
        assert [(f.symbol, f.timeframe) for f in files] == [("EUR_USD", "1W"), ("YM_F", "1M")]
        assert all(f.name_symbol == f.symbol for f in files)

    def test_default_timeframes_are_the_coarse_ones(self, tmp_path):
        for tf in ("1H", "4H", "1D", "1W", "1M"):
            put_frame(tmp_path, "us_stocks", "AAPL", tf, ["2026-11-02"])
        assert {f.timeframe for f in A.discover(tmp_path)} == {"1D", "1W", "1M"}
        assert {f.timeframe for f in A.discover(tmp_path, None)} == {"1H", "4H", "1D", "1W", "1M"}

    def test_market_and_symbol_filters(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02"])
        put_frame(tmp_path, "idx", "BBCA", "1D", ["2026-11-02"])
        assert [f.symbol for f in A.discover(tmp_path, market="idx")] == ["BBCA"]
        assert [f.symbol for f in A.discover(tmp_path, symbols={"AAPL"})] == ["AAPL"]

    def test_prepare_uses_the_stamps_own_local_date(self):
        """A Jakarta midnight is the previous day in UTC; the audit must not convert (trade_date.py)."""
        df = silver_frame(["2026-11-01T00:00"], tz="Asia/Jakarta")
        assert A.prepare(df)["_stamp"][0] == date(2026, 11, 1)


# ── schema ──────────────────────────────────────────────────────────────────────────────────────

class TestSchema:

    def test_healthy_file_has_no_schema_finding(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02", "2026-11-03"])
        assert got(audit(tmp_path, ["schema"]), "schema") == []

    def test_missing_column_fails_and_a_missing_price_blocks_the_other_checks(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03"]).drop("close")
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        res = audit(tmp_path, ["schema", "ohlc", "derived"])
        (f,) = got(res, "schema", A.FAIL)
        assert "close" in f.message
        assert any("remaining checks skipped" in x.message for x in got(res, "schema", A.INFO))
        assert got(res, "ohlc") == [] and got(res, "derived") == []

    def test_a_missing_non_price_column_fails_without_blocking(self, tmp_path):
        put(tmp_path, "us_stocks", "AAPL", "1D", silver_frame(["2026-11-02"]).drop("vwap"))
        res = audit(tmp_path, ["schema"])
        (f,) = got(res, "schema", A.FAIL)
        assert "vwap" in f.message and not f.detail.get("blocking")

    def test_naive_timestamp_fails(self, tmp_path):
        put(tmp_path, "us_stocks", "AAPL", "1D", silver_frame(["2026-11-02"], tz=None))
        assert any("tz-naive" in f.message for f in got(audit(tmp_path, ["schema"]), "schema", A.FAIL))

    def test_non_datetime_timestamp_blocks(self, tmp_path):
        df = silver_frame(["2026-11-02"]).with_columns(pl.col("timestamp").cast(pl.String))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        (f,) = got(audit(tmp_path, ["schema"]), "schema", A.FAIL)
        assert "not Datetime" in f.message and f.detail["blocking"]

    def test_a_blocking_failure_runs_nothing_else_on_the_file(self, tmp_path):
        """A String timestamp would crash the timestamp / period / derived checks; with every check selected
        the only findings must still be the schema ones (no 'internal' crash, no cascade)."""
        df = silver_frame(["2026-11-02", "2026-11-03"]).with_columns(pl.col("timestamp").cast(pl.String))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        res = audit(tmp_path)
        assert {f.check for f in res.findings} == {"schema"}

    def test_empty_file_fails_and_blocks(self, tmp_path):
        put(tmp_path, "us_stocks", "AAPL", "1D", silver_frame(["2026-11-02"]).head(0))
        (f,) = got(audit(tmp_path, ["schema"]), "schema", A.FAIL)
        assert "no rows" in f.message

    def test_symbol_and_timeframe_columns_must_match_the_path(self, tmp_path):
        df = silver_frame(["2026-11-02"], symbol="OTHER", tf="1W")
        put(tmp_path, "us_stocks", "AAPL", "1D", df, identity=False)
        msgs = " | ".join(f.message for f in got(audit(tmp_path, ["schema"]), "schema", A.FAIL))
        assert "symbol column" in msgs and "timeframe column" in msgs

    def test_file_name_symbol_must_match_the_directory(self, tmp_path):
        put(tmp_path, "us_stocks", "AAPL", "1D", silver_frame(["2026-11-02"], symbol="AAPL"), name_sym="AAPX")
        assert any("file name symbol" in f.message for f in got(audit(tmp_path, ["schema"]), "schema", A.FAIL))

    def test_first_bar_null_is_clean_is_info_only(self, tmp_path):
        """Measured on 10 of 10 real files: row 0, where log_return is null (Kleene True & null = null)."""
        df = silver_frame(["2026-11-02", "2026-11-03", "2026-11-04"]).with_columns(
            pl.Series("is_clean", [None, True, True], dtype=pl.Boolean))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        res = audit(tmp_path, ["schema"])
        assert got(res, "schema", A.FAIL) == [] and len(got(res, "schema", A.INFO)) == 1

    def test_null_is_clean_elsewhere_fails(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03", "2026-11-04"]).with_columns(
            pl.Series("is_clean", [None, None, True], dtype=pl.Boolean))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        (f,) = got(audit(tmp_path, ["schema"]), "schema", A.FAIL)
        assert "1 null(s) other than the first bar's" in f.message

    def test_null_is_clean_on_a_bar_with_a_log_return_fails(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03"]).with_columns(
            pl.Series("is_clean", [True, None], dtype=pl.Boolean))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        assert got(audit(tmp_path, ["schema"]), "schema", A.FAIL)

    def test_unreadable_file_is_a_fail_not_a_skip(self, tmp_path):
        d = tmp_path / "us_stocks" / "symbol=BAD"
        d.mkdir(parents=True)
        (d / "BAD_1D_silver.parquet").write_bytes(b"not parquet")
        res = audit(tmp_path)
        (f,) = got(res, "schema", A.FAIL)
        assert "unreadable" in f.message and res.n_files == 1 and res.n_read == 0


# ── timestamp ───────────────────────────────────────────────────────────────────────────────────

class TestTimestamp:

    def test_healthy_file_has_no_timestamp_finding(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02", "2026-11-03"])
        assert got(audit(tmp_path, ["timestamp"]), "timestamp") == []

    def test_unsorted_stamps_fail(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-03", "2026-11-02", "2026-11-04"])
        (f,) = got(audit(tmp_path, ["timestamp"]), "timestamp", A.FAIL)
        assert "1 out-of-order" in f.message

    def test_exact_duplicate_stamps_fail(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02", "2026-11-03", "2026-11-03"])
        (f,) = got(audit(tmp_path, ["timestamp"]), "timestamp", A.FAIL)
        assert "2 row(s) share 1 exact stamp(s)" in f.message

    def test_null_stamps_fail(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03"]).with_columns(
            pl.Series("timestamp", [datetime(2026, 11, 2), None],
                      dtype=pl.Datetime("ms")).dt.replace_time_zone(NY))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        assert any("null stamp" in f.message for f in got(audit(tmp_path, ["timestamp"]), "timestamp", A.FAIL))

    @pytest.mark.parametrize("tf", ["1D", "1W", "1M"])
    def test_coarse_stamp_after_the_local_hour_limit_fails(self, tmp_path, tf):
        stamp = "2026-11-02T07:00" if tf != "1M" else "2026-11-01T07:00"
        put_frame(tmp_path, "us_stocks", "AAPL", tf, [stamp])
        (f,) = got(audit(tmp_path, ["timestamp"]), "timestamp", A.FAIL)
        assert f"later than local hour {A._COARSE_STAMP_MAX_LOCAL_HOUR}" in f.message

    def test_the_hour_limit_is_inclusive_and_intraday_is_exempt(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", [f"2026-11-02T{A._COARSE_STAMP_MAX_LOCAL_HOUR:02d}:00"])
        put_frame(tmp_path, "us_stocks", "AAPX", "1H", ["2026-11-02T15:00", "2026-11-02T16:00"], symbol="AAPX")
        res = audit(tmp_path, ["timestamp"], tfs=None)
        assert got(res, "timestamp") == []

    def test_weekend_dated_1d_bars_warn_with_the_flat_count(self, tmp_path):
        stamps = ["2026-10-30", "2026-10-31", "2026-11-01", "2026-11-02"]      # Fri, Sat, Sun, Mon
        c = [100.0, 101.0, 102.0, 103.0]
        put_frame(tmp_path, "forex", "EUR_USD", "1D", stamps, tz="Europe/London", c=c, o=c,
                  h=[101.0, 101.0, 102.0, 104.0], l=[99.0, 101.0, 102.0, 102.0])
        (f,) = got(audit(tmp_path, ["timestamp"]), "timestamp", A.WARN)
        assert "2 weekend-dated 1D bar(s) (2026-10-31..2026-11-01), 2 fully flat" in f.message

    def test_weekend_rule_is_for_1d_only(self, tmp_path):
        put_frame(tmp_path, "forex", "EUR_USD", "1W", ["2026-11-01"], tz="Europe/London")     # a Sunday week
        assert got(audit(tmp_path, ["timestamp"]), "timestamp") == []


# ── period / anchor ─────────────────────────────────────────────────────────────────────────────

class TestPeriodAndAnchor:

    def test_the_period_key_is_the_processors(self):
        import src.silver.ohlcv_processor as proc
        assert A.period_key_expr is proc.period_key_expr
        assert A._PERIOD_DEDUP_TIMEFRAMES is proc._PERIOD_DEDUP_TIMEFRAMES
        assert A.CURRENT_SILVER_VERSION == proc.CURRENT_SILVER_VERSION
        assert A._COARSE_STAMP_MAX_LOCAL_HOUR == proc._COARSE_STAMP_MAX_LOCAL_HOUR

    def test_two_bars_in_one_month_fail_period_and_anchor(self, tmp_path):
        """The 149-symbol case: a month-to-date bar stamped 11-01 plus a one-day fragment stamped 11-03."""
        put_frame(tmp_path, "us_stocks", "AAPL", "1M", ["2026-10-01", "2026-11-01", "2026-11-03"])
        res = audit(tmp_path, ["period", "anchor"])
        (p,) = got(res, "period", A.FAIL)
        assert "first 2026-11-01: 2 bars stamped 2026-11-01, 2026-11-03" in p.message
        (a,) = got(res, "anchor", A.FAIL)
        assert "1 of 3 monthly stamp(s) are not on day 1" in a.message

    def test_a_clean_month_series_has_no_finding(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1M", ["2026-09-01", "2026-10-01", "2026-11-01"])
        assert got(audit(tmp_path, ["period", "anchor"]), "period") == []
        assert got(audit(tmp_path, ["period", "anchor"]), "anchor") == []

    def test_a_period_of_exactly_two_bars_is_caught(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1W", ["2026-08-17", "2026-08-18", "2026-08-24"])
        assert got(audit(tmp_path, ["period"]), "period", A.FAIL)

    def test_sunday_stamped_weeks_and_local_dates_are_not_false_positives(self, tmp_path):
        put_frame(tmp_path, "forex", "EUR_USD", "1W", ["2026-11-01", "2026-11-08", "2026-11-15"], tz="Europe/London")
        put_frame(tmp_path, "idx", "BBCA", "1M", ["2026-10-01", "2026-11-01"], tz="Asia/Jakarta")
        res = audit(tmp_path, ["period", "anchor"])
        assert got(res, "period") == [] and got(res, "anchor", A.FAIL) == []

    def _weekly_market(self, tmp_path, stamps_by_symbol):
        for sym, stamps in stamps_by_symbol.items():
            put_frame(tmp_path, "us_stocks", sym, "1W", stamps)

    def test_abbv_shape_a_tuesday_series_in_a_monday_market_fails(self, tmp_path):
        """Live: 713 of 726 weekly stamps Tuesday (2013-01-01..2026-08-25), 13 Monday from 2026-08-17."""
        mondays = ["2026-08-10", "2026-08-17", "2026-08-24"]
        self._weekly_market(tmp_path, {"AAPL": mondays, "MSFT": mondays, "NVDA": mondays,
                                       "ABBV": ["2026-08-04", "2026-08-11", "2026-08-18", "2026-08-25",
                                                "2026-09-01", "2026-09-07"]})
        res = audit(tmp_path, ["anchor"])
        (f,) = got(res, "anchor", A.FAIL)
        assert f.symbol == "ABBV"
        assert "5 of 6 1W stamp(s) are Tue (2026-08-04..2026-09-01) but us_stocks is Mon (3 of 4 symbols)" in f.message
        assert "1 on Mon (2026-09-07..2026-09-07)" in f.message
        assert res.facts["census"][0]["us_stocks/1W"] == {"symbols": 4, "mode": "Mon",
                                                          "dominant": {"Mon": 3, "Tue": 1}, "judged": True}

    def test_a_stray_stamp_on_another_weekday_warns(self, tmp_path):
        mondays = ["2026-08-10", "2026-08-17", "2026-08-24"]
        self._weekly_market(tmp_path, {"AAPL": mondays, "MSFT": mondays, "NVDA": mondays,
                                       "AMD": ["2026-08-10", "2026-08-17", "2026-08-25"]})
        res = audit(tmp_path, ["anchor"])
        (f,) = got(res, "anchor", A.WARN)
        assert f.symbol == "AMD" and "1 of 3 1W stamp(s) fall on Tue instead of Mon" in f.message
        assert got(res, "anchor", A.FAIL) == []

    def test_a_market_without_three_agreeing_symbols_is_not_judged(self, tmp_path):
        self._weekly_market(tmp_path, {"AAPL": ["2026-08-10"], "MSFT": ["2026-08-10"], "ABBV": ["2026-08-11"]})
        res = audit(tmp_path, ["anchor"])
        assert got(res, "anchor", A.FAIL) == []
        (f,) = got(res, "anchor", A.INFO)
        assert "anchor not judged" in f.message
        assert res.facts["census"][0]["us_stocks/1W"]["judged"] is False

    def test_the_agreeing_symbols_must_be_at_least_half_the_market(self, tmp_path):
        mondays = ["2026-08-10", "2026-08-17"]
        tue = ["2026-08-11", "2026-08-18"]
        wed = ["2026-08-12", "2026-08-19"]
        self._weekly_market(tmp_path, {"A1": mondays, "A2": mondays, "A3": mondays, "B1": tue, "B2": tue,
                                       "B3": wed, "B4": wed, "B5": wed})
        assert got(audit(tmp_path, ["anchor"]), "anchor", A.FAIL) == []

    def test_a_forex_market_of_sunday_weeks_has_sunday_as_its_anchor(self, tmp_path):
        for s in ("EUR_USD", "GBP_USD", "AUD_USD"):
            put_frame(tmp_path, "forex", s, "1W", ["2026-11-01", "2026-11-08"], tz="Europe/London")
        res = audit(tmp_path, ["anchor"])
        assert got(res, "anchor", A.FAIL) == [] and res.facts["census"][0]["forex/1W"]["mode"] == "Sun"


# ── window reconciliation ───────────────────────────────────────────────────────────────────────

class TestWindow:

    def test_monday_weeks_reconcile_both_ways(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "AAPL")
        res = audit(tmp_path, ["window"])
        assert got(res, "window") == []
        (_, _, _, best, cal, variant) = res.facts["recon"][0]
        assert variant == "exclusive" and best["close"] == 1.0 and cal["close"] == 1.0 and best["n"] > 40

    def test_months_reconcile(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "AAPL", key_fn=first_of_month, tf="1M")
        assert got(audit(tmp_path, ["window"]), "window") == []

    def test_tuesday_to_monday_weeks_are_an_anchor_defect_not_a_value_defect(self, tmp_path):
        """ABBV: the values are right (own window 1.0) and the calendar is shifted (calendar week ~0)."""
        build_symbol(tmp_path, "us_stocks", "ABBV", key_fn=tuesday_block)
        res = audit(tmp_path, ["window"])
        (f,) = got(res, "window", A.WARN)
        assert f.message.startswith("anchor defect: close equals the daily close of the bar's own stamp window")
        assert f.detail["implied"]["close"] == 1.0 and f.detail["calendar"]["close"] < 0.3

    def test_the_friday_close_check_alone_would_have_called_it_a_value_defect(self, tmp_path):
        """The two reconciliations disagree on ABBV; that disagreement is the diagnosis."""
        days, agg = build_symbol(tmp_path, "us_stocks", "ABBV", key_fn=tuesday_block)
        bars = A.prepare(pl.read_parquet(tmp_path / "us_stocks/symbol=ABBV/ABBV_1W_silver.parquet"))
        daily = A.prepare(pl.read_parquet(tmp_path / "us_stocks/symbol=ABBV/ABBV_1D_silver.parquet"))
        rec = A.reconcile_windows(bars, daily, "1W")
        assert A.match_rates(rec, "i")["close"] == 1.0 and A.match_rates(rec, "c")["close"] < 0.3

    def test_a_tampered_close_is_a_value_defect(self, tmp_path):
        def tamper(agg):
            for i in range(0, len(agg["c"]), 2):
                agg["c"][i] *= 1.05
        build_symbol(tmp_path, "us_stocks", "AAPL", tamper=tamper)
        (f,) = got(audit(tmp_path, ["window"]), "window", A.WARN)
        assert f.message.startswith("close does not equal the daily close of the bar's own window (exclusive)")
        assert "by year: 2026 " in f.message and f.detail["by_year"]

    def test_open_high_low_and_volume_are_reported_not_judged(self, tmp_path):
        """AAPL control: close agrees to 6e-7 while open misses 6% of weeks (ex-dividend weeks)."""
        def tamper(agg):
            agg["o"] = [x * 1.02 for x in agg["o"]]
            agg["h"] = [x * 1.02 for x in agg["h"]]
            agg["l"] = [x * 0.98 for x in agg["l"]]
            agg["v"] = [x * 3 for x in agg["v"]]
        build_symbol(tmp_path, "us_stocks", "AAPL", tamper=tamper)
        res = audit(tmp_path, ["window"])
        assert got(res, "window") == []
        assert res.facts["recon"][0][3]["open"] == 0.0 and res.facts["recon"][0][3]["close"] == 1.0

    def test_a_missing_bar_does_not_inflate_the_previous_bars_window(self, tmp_path):
        """YM_F: 27 of 180 months have no bar. Without the one-period cap the bar before a gap would be
        compared with two months of daily bars and fail."""
        def tamper(agg):
            for k in (3, 6):
                for key in ("stamps", "o", "h", "l", "c", "v"):
                    del agg[key][k]
        build_symbol(tmp_path, "us_stocks", "YM_F", key_fn=first_of_month, tf="1M", tamper=tamper)
        res = audit(tmp_path, ["window", "gaps"])
        assert got(res, "window") == []
        (g,) = got(res, "gaps", A.WARN)
        assert "2 of " in g.message and g.detail["n_missing"] == 2

    def test_the_newest_bar_is_open_ended_and_left_out_of_the_rates(self, tmp_path):
        def tamper(agg):
            agg["c"][-1] *= 1.10
        build_symbol(tmp_path, "us_stocks", "AAPL", key_fn=first_of_month, tf="1M", n_days=190, tamper=tamper)
        res = audit(tmp_path, ["window"])
        assert got(res, "window") == []
        assert res.facts["recon"][0][3]["close"] == 1.0

    def test_fx_style_end_inclusive_weeks_are_recognised(self, tmp_path):
        """EUR_USD 2016: the Monday-stamped bar closes at the NEXT Monday's daily close."""
        days = business_days(date(2026, 1, 5), 330)
        rows = daily_rows(days)
        by_day = {r["day"]: r for r in rows}
        agg = aggregate(rows, monday)
        mondays = [s.date() for s in agg["stamps"]]
        for i, m in enumerate(mondays[:-1]):
            nxt = by_day.get(mondays[i + 1])
            if nxt:
                agg["c"][i] = nxt["c"]
        put_frame(tmp_path, "forex", "EUR_USD", "1D", [datetime.combine(d, time(0)) for d in days],
                  tz="Europe/London", o=[r["o"] for r in rows], h=[r["h"] for r in rows],
                  l=[r["l"] for r in rows], c=[r["c"] for r in rows], v=[r["v"] for r in rows])
        put_frame(tmp_path, "forex", "EUR_USD", "1W", agg["stamps"], tz="Europe/London", o=agg["o"], h=agg["h"],
                  l=agg["l"], c=agg["c"], v=agg["v"])
        res = audit(tmp_path, ["window"])
        assert got(res, "window", A.WARN) == []
        (f,) = got(res, "window", A.INFO)
        assert "window end INCLUDED" in f.message and res.facts["recon"][0][5] == "end-inclusive"

    def test_the_end_inclusive_variant_is_chosen_only_when_it_is_better(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "AAPL")
        assert audit(tmp_path, ["window"]).facts["recon"][0][5] == "exclusive"

    def test_no_1d_file_is_an_info_not_a_crash(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1W", ["2026-11-02"])
        res = audit(tmp_path, ["window"])
        assert [f.severity for f in got(res, "window")] == [A.INFO]

    def test_min_match_and_tol_are_honoured(self, tmp_path):
        def tamper(agg):
            agg["c"] = [x * 1.0005 for x in agg["c"]]            # 5 bps off on every bar
        build_symbol(tmp_path, "us_stocks", "AAPL", tamper=tamper)
        assert got(audit(tmp_path, ["window"], tol=1e-3), "window") == []
        assert got(audit(tmp_path, ["window"], tol=1e-4), "window", A.WARN)

    def test_reconcile_windows_marks_exactly_one_open_ended_bar(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "AAPL")
        bars = A.prepare(pl.read_parquet(tmp_path / "us_stocks/symbol=AAPL/AAPL_1W_silver.parquet"))
        daily = A.prepare(pl.read_parquet(tmp_path / "us_stocks/symbol=AAPL/AAPL_1D_silver.parquet"))
        rec = A.reconcile_windows(bars, daily, "1W")
        assert rec.height == bars.height and rec["open_ended"].sum() == 1 and rec["open_ended"][-1]


# ── current period / gaps ───────────────────────────────────────────────────────────────────────

class TestCurrentPeriodAndGaps:

    def test_ym_f_shape_fragments_and_no_month_start_bar_fail(self, tmp_path):
        """Live: 152 of 158 stamps on day 1, six fragments stamped 6, 9..13 Nov, no 2026-11-01 bar."""
        days = days_until(date(2026, 1, 5), date(2026, 11, 13))
        assert days[-1] == date(2026, 11, 13)
        put_frame(tmp_path, "index_futures", "YM_F", "1D", [datetime.combine(d, time(0)) for d in days])
        put_frame(tmp_path, "index_futures", "YM_F", "1M",
                  ["2026-09-01", "2026-10-01", "2026-11-06", "2026-11-09", "2026-11-10", "2026-11-11",
                   "2026-11-12", "2026-11-13"])
        res = audit(tmp_path, ["current_period", "period", "anchor"])
        (f,) = got(res, "current_period", A.FAIL)
        assert "no bar stamped at the start of the current period 2026-11-01" in f.message
        assert "only 6 bar(s) stamped 2026-11-06, 2026-11-09" in f.message
        assert got(res, "period", A.FAIL) and got(res, "anchor", A.FAIL)

    def test_a_single_fragment_is_not_a_period_duplicate_but_still_fails_current_period(self, tmp_path):
        days = days_until(date(2026, 1, 5), date(2026, 11, 13))
        put_frame(tmp_path, "index_futures", "YM_F", "1D", [datetime.combine(d, time(0)) for d in days])
        put_frame(tmp_path, "index_futures", "YM_F", "1M", ["2026-10-01", "2026-11-13"])
        res = audit(tmp_path, ["current_period", "period"])
        assert got(res, "period") == [] and got(res, "current_period", A.FAIL)

    def test_a_month_start_bar_passes_even_with_a_later_fragment(self, tmp_path):
        days = days_until(date(2026, 1, 5), date(2026, 11, 13))
        put_frame(tmp_path, "index_futures", "YM_F", "1D", [datetime.combine(d, time(0)) for d in days])
        put_frame(tmp_path, "index_futures", "YM_F", "1M", ["2026-10-01", "2026-11-01", "2026-11-03"])
        assert got(audit(tmp_path, ["current_period"]), "current_period") == []

    def test_no_bar_at_all_for_the_current_period_is_a_warn(self, tmp_path):
        days = days_until(date(2026, 1, 5), date(2026, 11, 13))
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", [datetime.combine(d, time(0)) for d in days])
        put_frame(tmp_path, "us_stocks", "AAPL", "1M", ["2026-09-01", "2026-10-01"])
        (f,) = got(audit(tmp_path, ["current_period"]), "current_period")
        assert f.severity == A.WARN and "no 1M bar for the current period 2026-11-01" in f.message

    def test_a_sunday_stamped_forex_week_counts_as_the_week_start_bar(self, tmp_path):
        days = business_days(date(2026, 10, 5), 30)               # ends Fri 2026-11-13
        put_frame(tmp_path, "forex", "EUR_USD", "1D", [datetime.combine(d, time(0)) for d in days],
                  tz="Europe/London")
        put_frame(tmp_path, "forex", "EUR_USD", "1W", ["2026-11-01", "2026-11-08"], tz="Europe/London")
        assert got(audit(tmp_path, ["current_period"]), "current_period") == []

    def test_a_week_bar_stamped_after_the_monday_fails_current_period(self, tmp_path):
        days = business_days(date(2026, 10, 5), 30)               # ends Fri 2026-11-13
        put_frame(tmp_path, "us_stocks", "ABBV", "1D", [datetime.combine(d, time(0)) for d in days])
        put_frame(tmp_path, "us_stocks", "ABBV", "1W", ["2026-11-03", "2026-11-10"])
        (f,) = got(audit(tmp_path, ["current_period"]), "current_period", A.FAIL)
        assert "2026-11-09" in f.message and "week-to-date" in f.message

    def test_idx_local_midnight_month_start_is_not_a_false_positive(self, tmp_path):
        days = days_until(date(2026, 1, 5), date(2026, 11, 13))
        put_frame(tmp_path, "idx", "BBCA", "1D", [datetime.combine(d, time(0)) for d in days], tz="Asia/Jakarta")
        put_frame(tmp_path, "idx", "BBCA", "1M", ["2026-10-01", "2026-11-01"], tz="Asia/Jakarta")
        res = audit(tmp_path, ["current_period", "anchor", "period"])
        assert got(res, "current_period") == [] and got(res, "anchor") == [] and got(res, "period") == []

    def test_gaps_report_the_missing_months(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1M", ["2026-01-01", "2026-02-01", "2026-05-01", "2026-06-01"])
        (f,) = got(audit(tmp_path, ["gaps"]), "gaps", A.WARN)
        assert "2 of 6 1M period(s) between 2026-01-01 and 2026-06-01 have no bar, e.g. 2026-03-01, 2026-04-01" in f.message

    def test_gaps_report_the_missing_weeks_by_period_key(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1W", ["2026-10-05", "2026-10-12", "2026-10-26"])
        (f,) = got(audit(tmp_path, ["gaps"]), "gaps", A.WARN)
        assert f.detail["missing"] == ["2026-10-19"]

    def test_a_contiguous_series_has_no_gap(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1W", ["2026-10-05", "2026-10-12", "2026-10-19"])
        assert got(audit(tmp_path, ["gaps"]), "gaps") == []


# ── ohlc / derived / meta ───────────────────────────────────────────────────────────────────────

class TestOhlc:

    def test_healthy_bars_have_no_finding(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02", "2026-11-03"])
        assert got(audit(tmp_path, ["ohlc"]), "ohlc") == []

    def test_a_sanity_violation_flagged_clean_fails(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03"]).with_columns(
            pl.Series("high", [90.0, 102.0]))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        (f,) = got(audit(tmp_path, ["ohlc"]), "ohlc", A.FAIL)
        assert "carry is_clean=True" in f.message

    def test_a_sanity_violation_correctly_flagged_is_info(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03"]).with_columns(
            pl.Series("high", [90.0, 102.0]), pl.Series("is_clean", [False, True]))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        res = audit(tmp_path, ["ohlc"])
        assert got(res, "ohlc", A.FAIL) == []
        assert any("violate the price-sanity rules, all flagged is_clean=False" in f.message
                   for f in got(res, "ohlc", A.INFO))

    def test_a_null_price_flagged_clean_fails_and_flagged_dirty_is_info(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03"]).with_columns(pl.Series("close", [None, 101.0]))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        assert got(audit(tmp_path, ["ohlc"]), "ohlc", A.FAIL)
        put(tmp_path, "us_stocks", "AAPL", "1D", df.with_columns(pl.Series("is_clean", [False, True])))
        res = audit(tmp_path, ["ohlc"])
        assert got(res, "ohlc", A.FAIL) == []
        assert any("null/non-finite OHLC, all flagged" in f.message for f in got(res, "ohlc", A.INFO))

    def test_a_nan_price_is_non_finite(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03"]).with_columns(pl.Series("low", [float("nan"), 99.0]))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        assert got(audit(tmp_path, ["ohlc"]), "ohlc", A.FAIL)

    def test_a_non_positive_price_warns_cl_style(self, tmp_path):
        c = [10.0, -37.0, 5.0]
        put_frame(tmp_path, "commodity", "CL", "1D", ["2020-04-17", "2020-04-20", "2020-04-21"], c=c, o=c,
                  h=[11.0, 5.0, 6.0], l=[-40.0, -40.0, 4.0])
        (f,) = got(audit(tmp_path, ["ohlc"]), "ohlc", A.WARN)
        assert "price <= 0" in f.message and "CL, 2020-04-20" in f.message

    def test_negative_volume_fails(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02", "2026-11-03"], v=[1000, -5])
        (f,) = got(audit(tmp_path, ["ohlc"]), "ohlc", A.FAIL)
        assert "negative volume" in f.message

    def test_flat_and_dirty_bars_are_counted_as_info(self, tmp_path):
        c = [100.0, 100.0, 101.0]
        df = silver_frame(["2026-11-02", "2026-11-03", "2026-11-04"], c=c, o=c, h=c, l=c).with_columns(
            pl.Series("is_clean", [True, False, True]))
        put(tmp_path, "forex", "EUR_USD", "1D", df.with_columns(pl.lit("EUR_USD").alias("symbol")))
        msgs = [f.message for f in got(audit(tmp_path, ["ohlc"]), "ohlc", A.INFO)]
        assert any("3 of 3 bar(s) are fully flat" in m for m in msgs)
        assert any("1 of 3 bar(s) flagged is_clean=False" in m for m in msgs)


class TestDerived:

    def test_healthy_derived_fields_have_no_finding(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02", "2026-11-03", "2026-11-04"])
        assert got(audit(tmp_path, ["derived"]), "derived") == []

    @pytest.mark.parametrize("col", ["log_return", "dollar_volume", "spread_hl"])
    def test_a_tampered_derived_field_fails(self, tmp_path, col):
        df = silver_frame(["2026-11-02", "2026-11-03", "2026-11-04"])
        vals = df[col].to_list()
        vals[2] = (vals[2] or 0.0) + 0.5
        put(tmp_path, "us_stocks", "AAPL", "1D", df.with_columns(pl.Series(col, vals)))
        (f,) = got(audit(tmp_path, ["derived"]), "derived", A.FAIL)
        assert f.detail["field"] == col and "2026-11-04" in f.message

    def test_a_sign_crossing_close_with_a_matching_nan_is_not_a_mismatch(self, tmp_path):
        """CL 2020-04-20: ln(negative / positive) is NaN in the processor and in the recomputation."""
        c = [10.0, -37.0, 5.0]
        put_frame(tmp_path, "commodity", "CL", "1D", ["2020-04-17", "2020-04-20", "2020-04-21"], c=c, o=c,
                  h=[11.0, 5.0, 6.0], l=[-40.0, -40.0, 4.0])
        assert got(audit(tmp_path, ["derived"]), "derived") == []

    def test_a_stored_nan_where_the_recomputation_is_finite_fails(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03", "2026-11-04"])
        lr = df["log_return"].to_list()
        lr[2] = float("nan")
        put(tmp_path, "us_stocks", "AAPL", "1D", df.with_columns(pl.Series("log_return", lr)))
        assert got(audit(tmp_path, ["derived"]), "derived", A.FAIL)

    def test_the_first_bars_null_log_return_is_not_judged(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02"])
        assert got(audit(tmp_path, ["derived"]), "derived") == []

    def test_float_noise_below_the_tolerance_is_absorbed(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03", "2026-11-04"])
        put(tmp_path, "us_stocks", "AAPL", "1D", df.with_columns((pl.col("log_return") + 1e-12).alias("log_return")))
        assert got(audit(tmp_path, ["derived"]), "derived") == []


class TestMeta:

    def test_current_version_has_no_finding(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02"])
        assert got(audit(tmp_path, ["meta"]), "meta") == []

    def test_a_stale_or_mixed_processing_version_warns(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02"], version="1.1")
        (f,) = got(audit(tmp_path, ["meta"]), "meta", A.WARN)
        assert "rebuild pending" in f.message
        df = silver_frame(["2026-11-02", "2026-11-03"]).with_columns(
            pl.Series("processing_version", [CURRENT, "1.1"]))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        assert got(audit(tmp_path, ["meta"]), "meta", A.WARN)

    def test_several_data_sources_and_adjustment_factors_are_info(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03"]).with_columns(
            pl.Series("data_source", ["yfinance", "polygon"]), pl.Series("adj_factor", [1.0, 0.98]))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        res = audit(tmp_path, ["meta"])
        assert got(res, "meta", A.WARN) == [] and len(got(res, "meta", A.INFO)) == 2

    def test_null_adjustment_columns_warn(self, tmp_path):
        df = silver_frame(["2026-11-02", "2026-11-03"]).with_columns(
            pl.Series("is_adjusted", [True, None], dtype=pl.Boolean))
        put(tmp_path, "us_stocks", "AAPL", "1D", df)
        assert got(audit(tmp_path, ["meta"]), "meta", A.WARN)


# ── freshness / coverage ────────────────────────────────────────────────────────────────────────

class TestFreshnessAndCoverage:

    def _market(self, tmp_path, last_by_symbol):
        for sym, last in last_by_symbol.items():
            put_frame(tmp_path, "us_stocks", sym, "1D", ["2026-09-01", last])

    def test_a_symbol_trailing_its_market_by_more_than_the_window_warns(self, tmp_path):
        self._market(tmp_path, {"AAPL": "2026-11-13", "MSFT": "2026-11-13", "WBD": "2026-10-02"})
        (f,) = got(audit(tmp_path, ["freshness"], stale_days=4), "freshness", A.WARN)
        assert f.symbol == "WBD" and f.detail["lag_days"] == 42

    def test_a_lag_equal_to_the_window_is_fine(self, tmp_path):
        self._market(tmp_path, {"AAPL": "2026-11-13", "MSFT": "2026-11-09"})
        assert got(audit(tmp_path, ["freshness"], stale_days=4), "freshness") == []
        assert got(audit(tmp_path, ["freshness"], stale_days=3), "freshness", A.WARN)

    def test_freshness_is_judged_against_each_market_separately(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-09-01", "2026-11-13"])
        put_frame(tmp_path, "idx", "BBCA", "1D", ["2026-09-01", "2026-10-30"])
        assert got(audit(tmp_path, ["freshness"]), "freshness") == []

    def test_coverage_registry_missing_orphan_and_missing_timeframe(self, tmp_path):
        for sym in ("AAPL", "MSFT", "NVDA"):
            put_frame(tmp_path, "us_stocks", sym, "1D", ["2026-11-02"])
            put_frame(tmp_path, "us_stocks", sym, "1W", ["2026-11-02"])
        put_frame(tmp_path, "us_stocks", "PARA", "1D", ["2026-11-02"])           # not in the registry, no 1W
        opts = A.Options(silver_root=tmp_path, checks=("coverage",), use_registry=False)
        facts = A.run_audit(opts, A.discover(tmp_path)).facts
        opts = A.Options(silver_root=tmp_path, checks=("coverage",), use_registry=True)
        out = A.post_coverage(facts, opts, registry={"AAPL", "MSFT", "NVDA", "GOOG"})
        by = {(f.severity, f.symbol): f.message for f in out}
        assert by[(A.FAIL, "GOOG")].startswith("registered symbol has NO Silver file")
        para = [f.message for f in out if f.symbol == "PARA"]
        assert len(para) == 2 and any("not in the instrument registry" in m for m in para)
        assert any("missing timeframe(s) 1W" in m for m in para)
        assert not [f for f in out if f.symbol in ("AAPL", "MSFT", "NVDA")]

    def test_coverage_is_skipped_when_the_run_is_narrowed(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02"])
        opts = A.Options(silver_root=tmp_path, checks=("coverage",), use_registry=True, filtered=True)
        res = A.run_audit(opts, A.discover(tmp_path))
        (f,) = got(res, "coverage")
        assert f.severity == A.INFO and "skipped" in f.message

    def test_a_registry_that_cannot_load_is_reported_not_silently_skipped(self, tmp_path, monkeypatch):
        def boom():
            raise RuntimeError("no config here")
        monkeypatch.setattr(A, "_registry_symbols", boom)
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02"])
        opts = A.Options(silver_root=tmp_path, checks=("coverage",), use_registry=True)
        res = A.run_audit(opts, A.discover(tmp_path))
        assert any("registry unavailable (RuntimeError: no config here)" in f.message for f in got(res, "coverage"))

    def test_small_markets_get_no_missing_timeframe_judgement(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02"])
        put_frame(tmp_path, "us_stocks", "AAPL", "1W", ["2026-11-02"])
        put_frame(tmp_path, "us_stocks", "MSFT", "1D", ["2026-11-02"])
        opts = A.Options(silver_root=tmp_path, checks=("coverage",), use_registry=False)
        assert got(A.run_audit(opts, A.discover(tmp_path)), "coverage") == []


# ── driver, CLI, safety ─────────────────────────────────────────────────────────────────────────

@pytest.fixture
def tree(tmp_path):
    build_symbol(tmp_path, "us_stocks", "AAPL")                                  # healthy 1D + 1W
    put_frame(tmp_path, "us_stocks", "BAD", "1D", ["2026-11-03", "2026-11-02"])  # unsorted -> FAIL
    return tmp_path


class TestDriver:

    def test_exit_1_on_a_fail_and_0_on_a_clean_tree(self, tree, tmp_path_factory, capsys):
        assert A.main(["--silver-root", str(tree), "--no-registry"]) == 1
        clean = tmp_path_factory.mktemp("clean")
        build_symbol(clean, "us_stocks", "AAPL")
        assert A.main(["--silver-root", str(clean), "--no-registry"]) == 0

    def test_strict_makes_a_warn_fatal(self, tmp_path, capsys):
        put_frame(tmp_path, "us_stocks", "AAPL", "1M", ["2026-01-01", "2026-03-01"])   # a gap -> WARN only
        assert A.main(["--silver-root", str(tmp_path), "--no-registry", "--checks", "gaps"]) == 0
        assert A.main(["--silver-root", str(tmp_path), "--no-registry", "--checks", "gaps", "--strict"]) == 1

    def test_checks_selection_and_unknown_check(self, tree, capsys):
        assert A.main(["--silver-root", str(tree), "--no-registry", "--checks", "gaps"]) == 0
        with pytest.raises(SystemExit) as e:
            A.main(["--silver-root", str(tree), "--checks", "nope"])
        assert e.value.code == 2

    def test_list_checks_prints_every_check(self, capsys):
        assert A.main(["--list-checks"]) == 0
        out = capsys.readouterr().out
        assert all(name in out for name in A.CHECKS) and len(A.CHECKS) == 17

    def test_tf_all_includes_the_intraday_files(self, tmp_path, capsys):
        for tf in ("1H", "1D"):
            put_frame(tmp_path, "us_stocks", "AAPL", tf, ["2026-11-02"])
        A.main(["--silver-root", str(tmp_path), "--no-registry"])
        assert "files=1 (read 1)" in capsys.readouterr().out
        A.main(["--silver-root", str(tmp_path), "--no-registry", "--tf", "all"])
        assert "files=2 (read 2)" in capsys.readouterr().out

    def test_market_and_symbol_filters_and_coverage_skip(self, tree, capsys):
        A.main(["--silver-root", str(tree), "--symbol", "AAPL", "--market", "us_stocks"])
        out = capsys.readouterr().out
        assert "symbols=1" in out and "BAD" not in out and "skipped: --market / --symbol" in out

    def test_json_output_is_valid_and_complete(self, tree, capsys):
        assert A.main(["--silver-root", str(tree), "--no-registry", "--format", "json"]) == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["files"] == 3 and payload["files_read"] == 3 and payload["symbols"] == 2
        assert payload["summary"]["timestamp"]["FAIL"] == 1
        assert payload["thresholds"] == {"tol": A.DEFAULT_TOL, "vol_tol": A.DEFAULT_VOL_TOL,
                                         "min_match": A.DEFAULT_MIN_MATCH, "stale_days": A.DEFAULT_STALE_DAYS}
        assert {f["check"] for f in payload["findings"]} >= {"timestamp"}
        assert "reconciliation" in payload and "anchor_census" in payload

    def test_text_report_has_the_summary_census_and_reconciliation_blocks(self, tree, capsys):
        A.main(["--silver-root", str(tree), "--no-registry"])
        out = capsys.readouterr().out
        assert "CHECK" in out and "ANCHOR CENSUS" in out and "RECONCILIATION vs 1D" in out
        assert "RESULT: " in out and "[timestamp]" in out and "us_stocks/BAD 1D" in out

    def test_limit_caps_the_printed_findings(self, tmp_path, capsys):
        for i in range(5):
            put_frame(tmp_path, "us_stocks", f"S{i}", "1D", ["2026-11-03", "2026-11-02"])
        A.main(["--silver-root", str(tmp_path), "--no-registry", "--checks", "timestamp", "--limit", "2"])
        out = capsys.readouterr().out
        assert out.count("FAIL us_stocks/S") == 2 and "+3 more" in out

    def test_a_crashing_check_becomes_an_internal_fail_never_a_silent_skip(self, tree, monkeypatch):
        def boom(c):
            raise ValueError("kaboom")
        func, tfs, doc = A.CHECKS["ohlc"]
        monkeypatch.setitem(A.CHECKS, "ohlc", (boom, tfs, doc))
        res = audit(tree, ["ohlc"])
        fails = got(res, "internal", A.FAIL)
        assert fails and "check 'ohlc' crashed: ValueError: kaboom" in fails[0].message
        assert A.exit_code(res, strict=False) == 1

    def test_findings_are_sorted_check_then_severity(self, tree):
        res = audit(tree)
        order = [(A.CHECK_ORDER.index(f.check) if f.check in A.CHECK_ORDER else 99, A._SEVERITY_ORDER[f.severity])
                 for f in res.findings]
        assert order == sorted(order)

    def test_results_do_not_depend_on_the_worker_order(self, tmp_path):
        for s in ("A1", "B2", "C3", "D4", "E5"):
            put_frame(tmp_path, "us_stocks", s, "1D", ["2026-11-03", "2026-11-02"])
        a = [(f.symbol, f.message) for f in audit(tmp_path, ["timestamp"]).findings]
        b = [(f.symbol, f.message) for f in audit(tmp_path, ["timestamp"]).findings]
        assert a == b and [s for s, _ in a] == ["A1", "B2", "C3", "D4", "E5"]

    def test_the_run_never_modifies_the_tree(self, tree, capsys):
        before = {p: (p.stat().st_size, p.stat().st_mtime_ns) for p in tree.rglob("*")}
        A.main(["--silver-root", str(tree), "--no-registry", "--tf", "all"])
        A.main(["--silver-root", str(tree), "--no-registry", "--format", "json"])
        assert {p: (p.stat().st_size, p.stat().st_mtime_ns) for p in tree.rglob("*")} == before


# ── isolation pins (AST on imports and calls, never a name scan) ────────────────────────────────

_WRITE_ATTRS = {"write_parquet", "write_csv", "write_json", "write_text", "write_bytes", "write_ndjson",
                "sink_parquet", "sink_csv", "to_parquet", "to_csv", "mkdir", "touch", "unlink", "rmdir", "rmtree",
                "copyfile", "makedirs", "symlink_to"}
_OS_ONLY_ATTRS = {"replace", "rename", "remove", "move", "copy"}       # also str / list / set methods


def _write_calls(source: str) -> list[str]:
    hits = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = fn.attr if isinstance(fn, ast.Attribute) else (fn.id if isinstance(fn, ast.Name) else "")
        if name in _WRITE_ATTRS:
            hits.append(name)
        elif (name in _OS_ONLY_ATTRS and isinstance(fn, ast.Attribute) and isinstance(fn.value, ast.Name)
              and fn.value.id in ("os", "shutil")):
            hits.append(f"{fn.value.id}.{name}")
        elif name == "open":
            mode = ""
            if len(node.args) > 1 and isinstance(node.args[1], ast.Constant):
                mode = str(node.args[1].value)
            for kw in node.keywords:
                if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
                    mode = str(kw.value.value)
            if any(ch in mode for ch in "wax+"):
                hits.append(f"open({mode})")
    return hits


def _fstring_sql(source: str) -> list[str]:
    words = ("SELECT ", "READ_PARQUET(", "INSERT INTO", "CREATE TABLE", "DELETE FROM")   # not bare FROM / WHERE:
    # the script's English messages say "differs from ..."
    hits = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.JoinedStr):
            text = "".join(p.value for p in node.values if isinstance(p, ast.Constant)).upper()
            if any(w in text for w in words):
                hits.append(text.strip()[:40])
    return hits


class TestIsolation:

    SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "audit_silver_ohlcv.py"

    def test_the_scanner_catches_the_bad_spellings(self):
        for bad in ("df.write_parquet('x')", "p.write_text('x')", "open('f','w')", "open('f', mode='a')",
                    "os.replace(a, b)", "shutil.rmtree(d)", "d.mkdir()", "os.makedirs(d)", "p.unlink()",
                    "open('f','r+')", "lf.sink_parquet('x')", "os.remove(f)", "shutil.move(a, b)"):
            assert _write_calls(bad), bad
        for ok in ("open('f')", "open('f','r')", "s.replace('a','b')", "s.replace(a, b)", "lst.remove(x)",
                   "df.write", "x = {'write_parquet': 1}", "print('write_parquet(x)')", "dt.replace_time_zone('UTC')"):
            assert not _write_calls(ok), ok

    def test_the_sql_scanner_catches_an_f_string_query(self):
        assert _fstring_sql("q = f\"SELECT * FROM read_parquet('{p}')\"")
        assert not _fstring_sql("q = \"SELECT * FROM t\"; m = f\"{a} rows\"")
        assert not _fstring_sql("m = f\"{n} row(s) differ from the recomputation where {c} is null\"")

    def test_the_script_is_read_only(self):
        assert _write_calls(self.SCRIPT.read_text()) == []

    def test_no_src_module_imports_the_audit_script(self):
        root = Path(__file__).resolve().parents[2] / "src"
        offenders = []
        for py in root.rglob("*.py"):
            tree = ast.parse(py.read_text())
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""] + [a.name for a in node.names]
                if any("audit_silver_ohlcv" in n for n in names):
                    offenders.append(str(py))
        assert offenders == []

    def test_the_script_imports_only_the_processor_constants_and_the_registry(self):
        """Layer rule: Silver audit, so no Bronze / Gold / scheduler import. The registry import is lazy and
        read-only (instrument symbols for the coverage check)."""
        mods = []
        for node in ast.walk(ast.parse(self.SCRIPT.read_text())):
            if isinstance(node, ast.ImportFrom) and node.module:
                mods.append(node.module)
            elif isinstance(node, ast.Import):
                mods += [a.name for a in node.names]
        assert sorted(m for m in mods if m.startswith("src")) == ["src.config.instrument_loader",
                                                                  "src.silver.ohlcv_processor"]

    def test_no_f_string_sql_and_no_sql_engine(self):
        source = self.SCRIPT.read_text()
        assert _fstring_sql(source) == []
        assert "import duckdb" not in source


# ═════════════════════════════════════════════════════════════════════════════════════════════════
# FIX SIL-AUDIT-OHLCV-02 [chat thread, 17 Nov 2026] — the Silver -> Gold gate. Shapes measured on live Silver:
# ALL / CSX 1W (constant 1.00416 / 1.0027 for 519 of 533 weeks, then 1.0), MNST 1M (exactly 0.5 for 118 of 123
# months) and MNST 1D (split-factor jumps alternating 07-20..08-11), ABBV 1W (Tue..Mon windows end on the day the
# technical builder calls them available), KOSPI 1M (low = 0.0 with is_clean = TRUE).
# ═════════════════════════════════════════════════════════════════════════════════════════════════

def wednesday_block(d: date) -> date:        # Wed..Tue blocks stamped Wednesday (the MNTS shape)
    return d - timedelta(days=(d.weekday() - 2) % 7)


def scale_first(n: int, factor: float):
    def tamper(agg):
        for i in range(min(n, len(agg["c"]))):
            agg["c"][i] = agg["c"][i] * factor
    return tamper


class TestBasis:

    def test_a_constant_ratio_over_many_bars_is_a_basis_step_not_a_window_defect(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "MNST", tamper=scale_first(40, 0.5))
        (f,) = got(audit(tmp_path, ["basis"]), "basis", A.FAIL)
        assert f.detail["ratio"] == pytest.approx(0.5, rel=1e-6)
        assert f.detail["n_constant"] == 40 and f.detail["matches_again_from"] is not None
        assert "split-like, factor 2" in f.message and "lower than 1D" in f.message
        assert "which side is stale is not decided here" in f.message

    def test_a_dividend_sized_step_is_a_warn(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "ALL", tamper=scale_first(40, 1.00416))
        (f,) = got(audit(tmp_path, ["basis"]), "basis")
        assert f.severity == A.WARN and "dividend-sized" in f.message and "higher than 1D" in f.message

    def test_a_varying_ratio_is_a_window_or_staleness_defect_not_a_basis_step(self, tmp_path):
        def tamper(agg):
            for i in range(40):
                agg["c"][i] = agg["c"][i] * (1.02 + 0.01 * i)
        build_symbol(tmp_path, "us_stocks", "X", tamper=tamper)
        assert got(audit(tmp_path, ["basis"]), "basis") == []

    def test_fewer_than_twelve_mismatching_bars_is_not_enough(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "X", tamper=scale_first(BASIS_MIN := A.BASIS_MIN_PERIODS - 1, 0.5))
        assert got(audit(tmp_path, ["basis"]), "basis") == []

    def test_healthy_and_abbv_shaped_series_have_no_basis_finding(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "AAPL")
        build_symbol(tmp_path, "us_stocks", "ABBV", key_fn=tuesday_block)
        assert got(audit(tmp_path, ["basis"]), "basis") == []

    def test_the_window_warn_points_at_the_basis_check(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "MNST", tamper=scale_first(40, 0.5))
        (f,) = got(audit(tmp_path, ["window"]), "window", A.WARN)
        assert "see the basis check" in f.message

    def test_a_file_with_no_1d_is_skipped(self, tmp_path):
        put_frame(tmp_path, "us_stocks", "X", "1W", [f"2026-01-{d:02d}" for d in (5, 12, 19, 26)])
        assert got(audit(tmp_path, ["basis"]), "basis") == []


def _seam_closes(n=120, jumps=()):
    """Smooth closes with (index, factor) multiplicative level jumps applied from that index on."""
    c = [100.0 + 0.3 * i for i in range(n)]
    for idx, k in jumps:
        c = [x * k if i >= idx else x for i, x in enumerate(c)]
    return c


def _put_1d(tmp_path, closes, market="us_stocks", sym="MNST"):
    days = business_days(date(2026, 1, 5), len(closes))
    put_frame(tmp_path, market, sym, "1D", [datetime.combine(d, time(0)) for d in days], c=closes,
              h=[x * 1.001 for x in closes], l=[x * 0.999 for x in closes])


class TestSeam:

    def test_jumps_that_reverse_quickly_are_a_vendor_glitch(self, tmp_path):
        _put_1d(tmp_path, _seam_closes(jumps=[(40, 0.5), (43, 2.0), (50, 0.5), (53, 2.0)]))
        (f,) = got(audit(tmp_path, ["seam"], tfs=("1D",)), "seam", A.FAIL)
        assert f.detail["n_jumps"] == 4 and f.detail["n_reversing"] == 4 and "reversing" in f.message

    def test_one_persistent_jump_is_a_warn(self, tmp_path):
        _put_1d(tmp_path, _seam_closes(jumps=[(60, 0.5)]))
        (f,) = got(audit(tmp_path, ["seam"], tfs=("1D",)), "seam")
        assert f.severity == A.WARN and f.detail["n_reversing"] == 0

    def test_a_reversal_later_than_the_window_is_two_persistent_jumps(self, tmp_path):
        assert A.SEAM_REVERT_BARS < 40                      # the literal gap below must sit outside the window
        _put_1d(tmp_path, _seam_closes(n=160, jumps=[(30, 0.5), (70, 2.0)]))
        (f,) = got(audit(tmp_path, ["seam"], tfs=("1D",)), "seam")
        assert f.severity == A.WARN and f.detail["n_jumps"] == 2 and f.detail["n_reversing"] == 0

    def test_an_ordinary_large_move_is_not_a_split_factor(self, tmp_path):
        _put_1d(tmp_path, _seam_closes(jumps=[(60, 1.5)]))             # +50%: not within 8% of 2 or 1/2
        assert got(audit(tmp_path, ["seam"], tfs=("1D",)), "seam") == []

    def test_a_smooth_series_has_no_seam(self, tmp_path):
        _put_1d(tmp_path, _seam_closes())
        assert got(audit(tmp_path, ["seam"], tfs=("1D",)), "seam") == []

    def test_the_seam_check_runs_on_every_timeframe_but_a_coarse_reversal_is_only_a_warn(self, tmp_path):
        """FIX SIL-SEAM-SCOPE-01: CVNA 1M has six reversing 2x jumps and no 1D seam; 25 monthly bars are two years."""
        stamps = [datetime.combine(first_of_month(date(2024, 1, 1) + timedelta(days=31 * i)), time(0))
                  for i in range(30)]
        c = _seam_closes(n=len(stamps), jumps=[(10, 0.5), (12, 2.0)])
        put_frame(tmp_path, "us_stocks", "X", "1M", stamps, c=c, h=[x * 1.001 for x in c], l=[x * 0.999 for x in c])
        res = audit(tmp_path, ["seam"], tfs=("1M",))
        assert not got(res, "seam", A.FAIL)
        (f,) = got(res, "seam", A.WARN)
        assert f.timeframe == "1M" and f.detail["n_reversing"] == 2 and "ordinary move" in f.message

    def test_a_weekly_reversal_is_a_warn_and_a_daily_one_a_fail(self, tmp_path):
        mondays = [datetime.combine(date(2026, 1, 5) + timedelta(days=7 * i), time(0)) for i in range(40)]
        c = _seam_closes(n=40, jumps=[(20, 0.5), (22, 2.0)])
        put_frame(tmp_path, "us_stocks", "W", "1W", mondays, c=c, h=[x * 1.001 for x in c], l=[x * 0.999 for x in c])
        _put_1d(tmp_path, _seam_closes(jumps=[(40, 0.5), (43, 2.0)]), sym="D")
        res = audit(tmp_path, ["seam"])
        assert [f.symbol for f in got(res, "seam", A.FAIL)] == ["D"]
        assert [(f.symbol, f.timeframe) for f in got(res, "seam", A.WARN)] == [("W", "1W")]

    def test_the_jump_ratio_uses_the_tolerance_band(self, tmp_path):
        # 1.93x is inside 8% of 2 (a split riding on a -3.5% move); 1.80x is outside
        _put_1d(tmp_path, _seam_closes(jumps=[(60, 1.93)]))
        assert got(audit(tmp_path, ["seam"], tfs=("1D",)), "seam")
        shutil_tmp = tmp_path / "b"
        shutil_tmp.mkdir()
        _put_1d(shutil_tmp, _seam_closes(jumps=[(60, 1.80)]))
        assert got(audit(shutil_tmp, ["seam"], tfs=("1D",)), "seam") == []


class TestLeak:

    def test_tuesday_blocks_end_on_the_day_the_technical_builder_calls_them_available(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "ABBV", key_fn=tuesday_block)
        (f,) = got(audit(tmp_path, ["leak"]), "leak")
        assert f.severity == A.FAIL and f.detail["lookahead_days"] == 1 and f.detail["n_leak"] > 0
        assert "features_technical" in f.message

    def test_wednesday_blocks_look_two_days_ahead(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "MNTS", key_fn=wednesday_block)
        (f,) = got(audit(tmp_path, ["leak"]), "leak", A.FAIL)
        assert f.detail["lookahead_days"] == 2

    def test_monday_weeks_and_first_of_month_bars_do_not_leak(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "AAPL")
        build_symbol(tmp_path, "us_stocks", "BBB", key_fn=first_of_month, tf="1M", n_days=420)
        assert got(audit(tmp_path, ["leak"]), "leak") == []

    def test_outside_the_technical_markets_it_is_informational(self, tmp_path):
        build_symbol(tmp_path, "context", "SPX", key_fn=tuesday_block)
        (f,) = got(audit(tmp_path, ["leak"]), "leak")
        assert f.severity == A.INFO and "no technical consumer" in f.message

    def test_a_bar_that_does_not_reconcile_is_not_judged_here(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "ABBV", key_fn=tuesday_block, tamper=scale_first(10_000, 1.5))
        assert got(audit(tmp_path, ["leak"]), "leak") == []

    def test_the_audit_pins_the_availability_rule_of_features_technical(self):
        src = (Path(__file__).resolve().parents[2] / "src" / "gold" / "intelligence" / "features_technical.py"
               ).read_text()
        assert 'avail = pl.col("bar_date") + pl.duration(days=(7 - pl.col("bar_date").dt.weekday())' \
               '.cast(pl.Int64) + 1)' in src
        assert 'avail = pl.col("bar_date").dt.month_end() + pl.duration(days=1)' in src
        import re
        markets = re.search(r"^MARKETS = \((.*?)\)", src, re.M | re.S).group(1)
        assert set(re.findall(r'"(\w+)"', markets)) == set(A.LAYER1_MARKETS)


class TestLastBar:

    def test_a_stale_in_progress_bar_warns(self, tmp_path):
        def tamper(agg):
            agg["c"][-1] = agg["c"][-1] * 1.05
        build_symbol(tmp_path, "commodity", "AG", key_fn=first_of_month, tf="1M", n_days=300, tamper=tamper)
        (f,) = got(audit(tmp_path, ["last_bar"]), "last_bar")
        assert f.severity == A.WARN and f.detail["gap"] == pytest.approx(0.05, abs=1e-6)

    def test_a_current_bar_is_silent(self, tmp_path):
        build_symbol(tmp_path, "commodity", "AG", key_fn=first_of_month, tf="1M", n_days=300)
        assert got(audit(tmp_path, ["last_bar"]), "last_bar") == []


class TestGapsExplainedBy1D:

    def _week_series(self, tmp_path, drop_from_1d=(), drop_from_bars=(), n_days=330):
        days = [d for d in business_days(date(2026, 1, 5), n_days) if monday(d) not in set(drop_from_1d)]
        rows = daily_rows(days)
        put_frame(tmp_path, "idx", "JKSE", "1D", [datetime.combine(d, time(0)) for d in days], tz="Asia/Jakarta",
                  o=[r["o"] for r in rows], h=[r["h"] for r in rows], l=[r["l"] for r in rows],
                  c=[r["c"] for r in rows], v=[r["v"] for r in rows])
        agg = aggregate(rows, monday)
        keep = [i for i, st in enumerate(agg["stamps"])
                if st.date() not in set(drop_from_1d) | set(drop_from_bars)]
        put_frame(tmp_path, "idx", "JKSE", "1W", [agg["stamps"][i] for i in keep], tz="Asia/Jakarta",
                  o=[agg["o"][i] for i in keep], h=[agg["h"][i] for i in keep], l=[agg["l"][i] for i in keep],
                  c=[agg["c"][i] for i in keep], v=[agg["v"][i] for i in keep])

    def test_a_week_with_no_1d_bar_either_is_a_closure_not_a_gap(self, tmp_path):
        self._week_series(tmp_path, drop_from_1d=(date(2026, 3, 16),))
        (f,) = got(audit(tmp_path, ["gaps"]), "gaps")
        assert f.severity == A.INFO and f.detail["n_closed"] == 1 and f.detail["n_holes"] == 0

    def test_a_week_that_holds_1d_bars_is_a_rebuildable_hole(self, tmp_path):
        self._week_series(tmp_path, drop_from_bars=(date(2026, 3, 16),))
        (f,) = got(audit(tmp_path, ["gaps"]), "gaps")
        assert f.severity == A.WARN and f.detail["n_holes"] == 1 and "rebuildable" in f.message

    def test_a_period_before_the_1d_history_cannot_be_explained(self, tmp_path):
        days = business_days(date(2026, 3, 2), 120)
        rows = daily_rows(days)
        put_frame(tmp_path, "idx", "JKSE", "1D", [datetime.combine(d, time(0)) for d in days], tz="Asia/Jakarta",
                  o=[r["o"] for r in rows], h=[r["h"] for r in rows], l=[r["l"] for r in rows],
                  c=[r["c"] for r in rows], v=[r["v"] for r in rows])
        agg = aggregate(rows, monday)
        stamps = [datetime.combine(date(2026, 1, 5), time(0)), datetime.combine(date(2026, 2, 2), time(0))] + agg["stamps"]
        n = len(stamps)
        put_frame(tmp_path, "idx", "JKSE", "1W", stamps, tz="Asia/Jakarta")
        (f,) = got(audit(tmp_path, ["gaps"]), "gaps")
        assert f.severity == A.WARN and f.detail["n_pre_1d"] > 0 and f.detail["n_holes"] == 0

    def test_without_a_1d_file_the_old_warn_stands(self, tmp_path):
        put_frame(tmp_path, "idx", "JKSE", "1W", ["2026-01-05", "2026-01-12", "2026-02-02"], tz="Asia/Jakarta")
        (f,) = got(audit(tmp_path, ["gaps"]), "gaps")
        assert f.severity == A.WARN and "n_holes" not in f.detail


class TestNonPositivePrice:

    def _frame_with_low(self, is_clean):
        df = silver_frame(["2026-11-02", "2026-11-03", "2026-11-04"], tf="1M")
        return df.with_columns(pl.Series("low", [99.0, 0.0, 99.0]),
                               pl.Series("is_clean", [True, is_clean, True]))

    def test_a_price_of_zero_that_is_clean_fails(self, tmp_path):
        put(tmp_path, "context", "KOSPI", "1M", self._frame_with_low(True))
        res = audit(tmp_path, ["ohlc"])
        (f,) = got(res, "ohlc", A.FAIL)
        assert "carry is_clean=True" in f.message

    def test_a_flagged_non_positive_bar_stays_a_warn(self, tmp_path):
        put(tmp_path, "commodity", "CL", "1M", self._frame_with_low(False))
        res = audit(tmp_path, ["ohlc"])
        assert not [f for f in got(res, "ohlc", A.FAIL) if "price <= 0" in f.message]
        assert any("price <= 0" in f.message for f in got(res, "ohlc", A.WARN))


class TestGate:

    def test_a_clean_tree_passes_both_consumers(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "AAPL")
        gate = audit(tmp_path).facts["gate"][0]
        assert gate["technical_xgboost"]["verdict"] == "PASS" and gate["cross_asset_xgboost"]["verdict"] == "PASS"

    def test_a_1w_leak_blocks_the_technical_model_only(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "ABBV", key_fn=tuesday_block)
        gate = audit(tmp_path).facts["gate"][0]
        assert gate["technical_xgboost"]["verdict"] == "BLOCK"
        assert ["us_stocks", "ABBV", "1W", "leak"] in gate["technical_xgboost"]["blocking_series"]
        assert gate["cross_asset_xgboost"]["verdict"] == "PASS"       # the cross-asset engine reads 1D only

    def test_a_1d_seam_blocks_both(self, tmp_path):
        _put_1d(tmp_path, _seam_closes(jumps=[(40, 0.5), (43, 2.0)]))
        gate = audit(tmp_path).facts["gate"][0]
        assert gate["technical_xgboost"]["verdict"] == gate["cross_asset_xgboost"]["verdict"] == "BLOCK"

    def test_a_layer2_1d_fail_blocks_only_the_cross_asset_engine(self, tmp_path):
        _put_1d(tmp_path, _seam_closes(jumps=[(40, 0.5), (43, 2.0)]), market="context", sym="SPX")
        gate = audit(tmp_path).facts["gate"][0]
        assert gate["technical_xgboost"]["verdict"] == "PASS"
        assert gate["cross_asset_xgboost"]["verdict"] == "BLOCK"

    def test_a_warn_of_an_attention_check_is_attention_not_block(self, tmp_path):
        days = [date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 7)]          # a Saturday-dated 1D bar
        put_frame(tmp_path, "forex", "EUR_USD", "1D", [datetime.combine(d, time(0)) for d in days],
                  tz="Europe/London")
        gate = audit(tmp_path, ["timestamp"]).facts["gate"][0]
        assert gate["technical_xgboost"]["verdict"] == "ATTENTION" and gate["technical_xgboost"]["fail"] == 0

    def test_unaudited_timeframes_are_reported_as_partial(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "AAPL")
        gate = audit(tmp_path, tfs=("1W",)).facts["gate"][0]
        assert gate["technical_xgboost"]["partial"] == ["1D", "1M"]
        assert gate["cross_asset_xgboost"]["partial"] == ["1D"]

    def test_whole_tree_findings_carry_no_series_and_are_not_counted(self):
        f = A.Finding("coverage", A.FAIL, "us_stocks", "*", "1D", "x")
        gate = A.build_gate([f], ("1D",))
        assert gate["technical_xgboost"]["verdict"] == "PASS"

    def test_the_gate_is_in_the_text_and_json_reports(self, tmp_path, capsys):
        build_symbol(tmp_path, "us_stocks", "ABBV", key_fn=tuesday_block)
        assert A.main(["--silver-root", str(tmp_path), "--no-registry"]) == 1
        text = capsys.readouterr().out
        assert "GATE (Silver -> Gold readiness" in text and "technical_xgboost" in text and "BLOCK" in text
        A.main(["--silver-root", str(tmp_path), "--no-registry", "--format", "json"])
        payload = json.loads(capsys.readouterr().out)
        assert payload["gate"]["technical_xgboost"]["verdict"] == "BLOCK"


# -- FIX SIL-AUDIT-OHLCV-03: a gate must not PASS because no check ran ----------------------------------------

class TestAnEmptyAuditCannotPass:

    def test_the_default_options_run_every_check(self):
        assert A.Options().checks == tuple(A.CHECKS)

    def test_the_python_api_without_a_checks_argument_is_not_vacuous(self, tmp_path):
        """The 19 Nov slip: Options(silver_root=..., use_registry=False) ran zero checks and the gate said PASS."""
        build_symbol(tmp_path, "us_stocks", "ABBV", key_fn=tuesday_block)
        res = A.run_audit(A.Options(silver_root=tmp_path, use_registry=False), A.discover(tmp_path, ("1D", "1W")))
        assert res.checks == tuple(A.CHECKS) and got(res, "leak", A.FAIL)
        assert res.facts["gate"][0]["technical_xgboost"]["verdict"] == "BLOCK"

    def test_an_explicitly_empty_selection_is_refused(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "AAPL")
        with pytest.raises(ValueError, match="no check"):
            A.run_audit(A.Options(silver_root=tmp_path, checks=(), use_registry=False),
                        A.discover(tmp_path, ("1D", "1W")))


# -- FIX SIL-AUDIT-OHLCV-03: the recent-period check (frozen months the 0.95 overall share cannot see) --------

def _bump(idx_from_end, factor):
    """Scale the close of the bar ``idx_from_end`` bars before the newest one (the newest is open-ended)."""
    def tamper(agg):
        for i in ([idx_from_end] if isinstance(idx_from_end, int) else idx_from_end):
            agg["c"][-1 - i] = agg["c"][-1 - i] * factor
    return tamper


class TestRecent:
    N = 420          # ~19 monthly bars: more than RECENT_PERIODS["1M"] = 12

    def _monthly(self, tmp_path, tamper=None, market="us_stocks", sym="X", tz=NY):
        return build_symbol(tmp_path, market, sym, key_fn=first_of_month, tf="1M", n_days=self.N, tz=tz,
                            tamper=tamper)

    def test_a_clean_series_has_no_recent_finding(self, tmp_path):
        self._monthly(tmp_path)
        assert got(audit(tmp_path, ["recent"]), "recent") == []

    def test_two_frozen_months_pass_the_overall_share_but_fail_the_recent_window(self, tmp_path):
        """MARA / CLSK 1M: Aug and Sep 2026 frozen at a first snapshot, up to 14% off, 2 of 122 bars."""
        build_symbol(tmp_path, "us_stocks", "X", key_fn=first_of_month, tf="1M", n_days=2500,
                     tamper=_bump([2, 3], 1.14))                         # ~115 months, as on live Silver
        res = audit(tmp_path, ["window", "recent"])
        assert not got(res, "window")                                    # ~98% of bars match: above --min-match
        (f,) = got(res, "recent")
        assert f.severity == A.FAIL and f.detail["n_bad"] == 2 and f.detail["max_gap"] > 0.10

    def test_a_small_gap_is_a_warn(self, tmp_path):
        self._monthly(tmp_path, _bump([2], 1.005))                      # AAPL 1M: 0.89% off
        (f,) = got(audit(tmp_path, ["recent"]), "recent")
        assert f.severity == A.WARN and f.detail["n_bad"] == 1

    def test_a_layer2_market_is_only_a_warn_whatever_the_gap(self, tmp_path):
        self._monthly(tmp_path, _bump([2, 3], 1.14), market="context")
        (f,) = got(audit(tmp_path, ["recent"]), "recent")
        assert f.severity == A.WARN

    def test_a_bad_bar_older_than_the_window_is_not_recent(self, tmp_path):
        def old(agg):
            agg["c"][1] = agg["c"][1] * 1.14
        self._monthly(tmp_path, old)
        assert got(audit(tmp_path, ["recent"]), "recent") == []

    def test_the_newest_open_ended_bar_is_not_judged_here(self, tmp_path):
        self._monthly(tmp_path, _bump([0], 1.14))
        assert got(audit(tmp_path, ["recent"]), "recent") == []          # `last_bar` owns it

    def test_fx_is_not_judged_it_is_deferred(self, tmp_path):
        self._monthly(tmp_path, _bump([2, 3], 1.14), market="forex", sym="EUR_USD", tz="Europe/London")
        assert got(audit(tmp_path, ["recent"]), "recent") == []

    def test_a_convention_difference_is_not_a_stale_value(self, tmp_path):
        """ABBV: Tuesday-stamped blocks reconcile on the implied window; that is `anchor` / `leak`, not `recent`."""
        build_symbol(tmp_path, "us_stocks", "ABBV", key_fn=tuesday_block)
        assert got(audit(tmp_path, ["recent"]), "recent") == []

    def test_an_end_inclusive_fx_style_convention_is_not_a_stale_value(self, tmp_path):
        """Context FX series (HKD, THB ...) close the Monday-stamped week at the NEXT Monday's daily close; a bar
        counts as reconciled on ANY of the three windows, so this is `window`'s business, not `recent`'s."""
        days = business_days(date(2026, 1, 5), 420)
        rows = daily_rows(days)
        by_day = {r["day"]: r for r in rows}
        agg = aggregate(rows, monday)
        mondays = [s.date() for s in agg["stamps"]]
        for i in range(len(mondays) - 1):
            if by_day.get(mondays[i + 1]):
                agg["c"][i] = by_day[mondays[i + 1]]["c"]
        put_frame(tmp_path, "context", "HKD", "1D", [datetime.combine(d, time(0)) for d in days], tz="Europe/London",
                  o=[r["o"] for r in rows], h=[r["h"] for r in rows], l=[r["l"] for r in rows],
                  c=[r["c"] for r in rows], v=[r["v"] for r in rows])
        put_frame(tmp_path, "context", "HKD", "1W", agg["stamps"], tz="Europe/London", o=agg["o"], h=agg["h"],
                  l=agg["l"], c=agg["c"], v=agg["v"])
        assert got(audit(tmp_path, ["recent"]), "recent") == []

    def test_weekly_uses_its_own_window(self, tmp_path):
        assert A.RECENT_PERIODS == {"1W": 26, "1M": 12}
        build_symbol(tmp_path, "us_stocks", "W", key_fn=monday, tf="1W", n_days=330, tamper=_bump([30], 1.14))
        assert got(audit(tmp_path, ["recent"]), "recent") == []          # 30 bars back: outside 26
        build_symbol(tmp_path, "us_stocks", "V", key_fn=monday, tf="1W", n_days=330, tamper=_bump([5], 1.14))
        (f,) = got(audit(tmp_path, ["recent"]), "recent", symbol="V")
        assert f.timeframe == "1W" and f.severity == A.FAIL

    def test_recent_is_an_attention_check_for_the_gate(self):
        assert "recent" in A.GATE_ATTENTION and "recent" in A.CHECKS

    def test_a_layer1_frozen_month_blocks_the_technical_model(self, tmp_path):
        self._monthly(tmp_path, _bump([2, 3], 1.14))
        gate = audit(tmp_path, ["recent"]).facts["gate"][0]
        assert gate["technical_xgboost"]["verdict"] == "BLOCK"
        assert ["us_stocks", "X", "1M", "recent"] in gate["technical_xgboost"]["blocking_series"]
        assert gate["cross_asset_xgboost"]["verdict"] == "PASS"          # reads 1D only


# -- FIX SIL-AUDIT-OHLCV-03: the gate states the cost of quarantine and refuses PASS while FX is deferred -----

class TestGateCostAndDeferral:

    def test_the_cost_of_a_quarantine_is_a_number(self, tmp_path):
        for sym in ("MNST", "OTHR"):
            _put_1d(tmp_path, _seam_closes(jumps=[(40, 0.5), (43, 2.0)]), sym=sym)
            _quarantine(tmp_path, "us_stocks", sym)
        g = audit(tmp_path).facts["gate"][0]["technical_xgboost"]
        assert g["quarantined_count"] == 2 and g["quarantined_symbols"] == 2

    def test_series_and_symbols_are_counted_separately(self, tmp_path):
        """One symbol with two quarantined series (MNST 1D + 1W) is 2 series and 1 symbol."""
        build_symbol(tmp_path, "us_stocks", "MNST")                      # 1D + 1W
        _quarantine(tmp_path, "us_stocks", "MNST", "1D")
        _quarantine(tmp_path, "us_stocks", "MNST", "1W")
        g = audit(tmp_path, ["schema"]).facts["gate"][0]["technical_xgboost"]
        assert g["quarantined_count"] == 2 and g["quarantined_symbols"] == 1

    def test_the_cost_is_in_the_text_report(self, tmp_path, capsys):
        for sym in ("MNST", "OTHR"):
            _put_1d(tmp_path, _seam_closes(jumps=[(40, 0.5), (43, 2.0)]), sym=sym)
            _quarantine(tmp_path, "us_stocks", sym)
        A.main(["--silver-root", str(tmp_path), "--no-registry"])
        assert "quarantined series=2 (symbols=2" in capsys.readouterr().out

    def test_no_quarantine_means_zero(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "AAPL")
        g = audit(tmp_path).facts["gate"][0]["technical_xgboost"]
        assert g["quarantined_count"] == 0 and g["quarantined_symbols"] == 0 and g["deferred_series"] == []

    def test_fx_coarse_series_are_deferred_and_the_technical_model_cannot_pass(self, tmp_path):
        build_symbol(tmp_path, "forex", "EUR_USD", key_fn=monday, tf="1W", tz="Europe/London")
        gate = audit(tmp_path, ["schema"]).facts["gate"][0]               # no finding at all: only the deferral
        t = gate["technical_xgboost"]
        assert t["fail"] == 0 and t["attention"] == 0
        assert t["verdict"] == "ATTENTION" and ["forex", "EUR_USD", "1W"] in t["deferred_series"]
        assert "GHR-11" in t["deferred_reason"]
        assert gate["cross_asset_xgboost"]["verdict"] == "PASS" and gate["cross_asset_xgboost"]["deferred_series"] == []

    def test_a_blocked_consumer_stays_blocked_when_fx_is_deferred(self, tmp_path):
        build_symbol(tmp_path, "forex", "EUR_USD", key_fn=monday, tf="1W", tz="Europe/London")
        build_symbol(tmp_path, "us_stocks", "ABBV", key_fn=tuesday_block)
        assert audit(tmp_path).facts["gate"][0]["technical_xgboost"]["verdict"] == "BLOCK"

    def test_deferred_fx_is_in_the_text_report(self, tmp_path, capsys):
        build_symbol(tmp_path, "forex", "EUR_USD", key_fn=monday, tf="1W", tz="Europe/London")
        A.main(["--silver-root", str(tmp_path), "--no-registry", "--checks", "schema"])
        assert "deferred FX series=1" in capsys.readouterr().out


# ── FIX SIL-SEAM-QUARANTINE-01: a quarantined series (no is_clean row) is not a Gold blocker ─────────────────

def _quarantine(root: Path, market: str, sym: str, tf: str = "1D") -> None:
    path = root / market / f"symbol={sym}" / f"{sym}_{tf}_silver.parquet"
    pl.read_parquet(path).with_columns(pl.lit(False).alias("is_clean")).write_parquet(path)


class TestGateQuarantine:

    def test_a_seam_series_with_no_clean_row_is_listed_not_blocking(self, tmp_path):
        _put_1d(tmp_path, _seam_closes(jumps=[(40, 0.5), (43, 2.0)]))
        _quarantine(tmp_path, "us_stocks", "MNST")
        res = audit(tmp_path)
        assert got(res, "seam", A.FAIL)                                   # the finding itself stays a FAIL
        gate = res.facts["gate"][0]
        for name in ("technical_xgboost", "cross_asset_xgboost"):
            assert gate[name]["verdict"] != "BLOCK" and gate[name]["fail"] == 0
            assert ["us_stocks", "MNST", "1D"] in gate[name]["quarantined_series"]

    def test_the_same_series_with_clean_rows_still_blocks(self, tmp_path):
        _put_1d(tmp_path, _seam_closes(jumps=[(40, 0.5), (43, 2.0)]))
        gate = audit(tmp_path).facts["gate"][0]
        assert gate["technical_xgboost"]["verdict"] == "BLOCK" and gate["technical_xgboost"]["quarantined_series"] == []

    def test_a_basis_finding_on_a_coarse_series_whose_1d_is_quarantined_does_not_block(self, tmp_path):
        """MNST: the 1M is right and 1D is the stale side; once 1D is out of Gold the 1M basis FAIL is moot."""
        build_symbol(tmp_path, "us_stocks", "MNST", tamper=scale_first(40, 0.5))
        assert got(audit(tmp_path, ["basis"]), "basis", A.FAIL)
        _quarantine(tmp_path, "us_stocks", "MNST", "1D")
        res = audit(tmp_path, ["basis", "seam"])          # the tampered fixture also breaks OHLC order: isolate basis
        assert got(res, "basis", A.FAIL)
        gate = res.facts["gate"][0]["technical_xgboost"]
        assert gate["fail"] == 0 and ["us_stocks", "MNST", "1D"] in gate["quarantined_series"]

    def test_a_basis_finding_is_not_neutralised_when_only_the_coarse_series_is_odd(self, tmp_path):
        build_symbol(tmp_path, "us_stocks", "MNST", tamper=scale_first(40, 0.5))
        gate = audit(tmp_path, ["basis", "seam"]).facts["gate"][0]["technical_xgboost"]
        assert gate["fail"] >= 1 and ["us_stocks", "MNST", "1W", "basis"] in gate["blocking_series"]

    def test_one_clean_row_is_enough_for_gold_to_read_the_series(self, tmp_path):
        _put_1d(tmp_path, _seam_closes(jumps=[(40, 0.5), (43, 2.0)]))
        path = tmp_path / "us_stocks" / "symbol=MNST" / "MNST_1D_silver.parquet"
        df = pl.read_parquet(path)
        df.with_columns((pl.int_range(pl.len()) == df.height - 1).alias("is_clean")).write_parquet(path)
        gate = audit(tmp_path).facts["gate"][0]["technical_xgboost"]
        assert gate["verdict"] == "BLOCK" and gate["quarantined_series"] == []

    def test_the_quarantine_is_visible_in_the_text_report(self, tmp_path, capsys):
        _put_1d(tmp_path, _seam_closes(jumps=[(40, 0.5), (43, 2.0)]))
        _quarantine(tmp_path, "us_stocks", "MNST")
        A.main(["--silver-root", str(tmp_path), "--no-registry"])
        text = capsys.readouterr().out
        assert "quarantined series=1" in text and "[quarantined: no is_clean row" in text
