"""tests/unit/test_trade_date.py — FIX TS-DATE-01 [chat thread, 16 Oct 2026]

Silver 1D bars are stamped at 00:00 LOCAL exchange time in each instrument's own
timezone (measured live: AAPL New York, BBCA Jakarta, EUR_USD London, VIX
Chicago) — not UTC as Grand Design §4.3 says. The stamp's own calendar date is
the trading date. DuckDB reads the column as TIMESTAMP WITH TIME ZONE and drops
the zone name, so `CAST(timestamp AS DATE)` converted into the *session* zone
(the host's): right on a WIB host for zones <= +7, one day early for IDX under
UTC/New York and for the Layer 2 Asia-Pacific indices even under WIB. See
src/utils/trade_date.py.
"""

import datetime as dt
import re
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import pytest

from src.utils.trade_date import PROBE_TZ, trade_date_sql, with_trade_date

# Every zone a Silver 1D file is stamped in, plus the Layer 2 indices whose
# stamps were wrong even on the WIB host.
UNIVERSE_ZONES = [
    "America/New_York", "America/Chicago", "Europe/London", "Asia/Jakarta",
    "Asia/Tokyo", "Asia/Seoul", "Asia/Hong_Kong", "Asia/Shanghai",
    "Asia/Taipei", "Australia/Sydney",
]
# Includes both sides of the US/UK/AU DST changes in 2026.
TRADING_DATES = [
    dt.date(2026, 1, 5), dt.date(2026, 3, 6), dt.date(2026, 3, 9),
    dt.date(2026, 3, 30), dt.date(2026, 10, 15), dt.date(2026, 11, 2),
]
# Host zones a run can land on: the M1 (WIB), a CI/UTC host, a New York host,
# and an east-of-everything host.
SESSION_ZONES = ["Asia/Jakarta", "UTC", "America/New_York", "Pacific/Auckland"]


def _con(session_tz: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET TimeZone='{session_tz}'")  # test-only literal, not src SQL
    return con


def _local_midnight_frame(zone: str, dates) -> pl.DataFrame:
    """Silver-shaped 1D stamps: 00:00 local time, tz-aware in `zone`."""
    return pl.DataFrame({
        "truth": list(dates),
        "timestamp": [
            dt.datetime(d.year, d.month, d.day, tzinfo=ZoneInfo(zone))
            for d in dates
        ],
    }).with_columns(pl.col("timestamp").dt.convert_time_zone(zone))


class TestTradeDateSql:

    @pytest.mark.parametrize("session_tz", SESSION_ZONES)
    @pytest.mark.parametrize("zone", UNIVERSE_ZONES)
    def test_local_midnight_stamp_yields_its_own_date(self, tmp_path, zone, session_tz):
        """The core contract: whatever zone the bar is stamped in and whatever
        zone the session happens to be in, the date is the stamp's own."""
        path = tmp_path / "bars.parquet"
        _local_midnight_frame(zone, TRADING_DATES).write_parquet(path)
        sql = "SELECT COUNT(*) FROM read_parquet($f) WHERE (" + trade_date_sql() + ") != truth"
        assert _con(session_tz).execute(sql, {"f": str(path)}).fetchone()[0] == 0

    @pytest.mark.parametrize("session_tz", SESSION_ZONES)
    def test_naive_datetime_and_date_columns_keep_plain_cast(self, tmp_path, session_tz):
        """Test fixtures and any non-tz column never involved a zone; the
        helper must not introduce one."""
        pl.DataFrame({
            "truth": TRADING_DATES,
            "timestamp": [dt.datetime(d.year, d.month, d.day) for d in TRADING_DATES],
        }).write_parquet(tmp_path / "naive.parquet")
        pl.DataFrame({"truth": TRADING_DATES, "timestamp": TRADING_DATES}
                     ).write_parquet(tmp_path / "dateonly.parquet")
        con = _con(session_tz)
        sql = "SELECT COUNT(*) FROM read_parquet($f) WHERE (" + trade_date_sql() + ") != truth"
        for name in ("naive.parquet", "dateonly.parquet"):
            assert con.execute(sql, {"f": str(tmp_path / name)}).fetchone()[0] == 0, name

    def test_raw_cast_is_session_dependent_documenting_the_bug(self, tmp_path):
        """Pins WHY the helper exists: the plain cast on the same data is wrong
        for IDX under a UTC session and right under WIB. If DuckDB ever stops
        doing this, this test fails and the helper can be reconsidered."""
        path = tmp_path / "bbca.parquet"
        _local_midnight_frame("Asia/Jakarta", TRADING_DATES).write_parquet(path)
        raw = "SELECT COUNT(*) FROM read_parquet($f) WHERE CAST(timestamp AS DATE) != truth"
        assert _con("Asia/Jakarta").execute(raw, {"f": str(path)}).fetchone()[0] == 0
        assert _con("UTC").execute(raw, {"f": str(path)}).fetchone()[0] == len(TRADING_DATES)

    def test_column_name_is_validated_not_trusted(self):
        for bad in ("timestamp; DROP TABLE x", "a b", "", "ts)--", "s..timestamp"):
            with pytest.raises(ValueError):
                trade_date_sql(bad)
        assert "s.timestamp" in trade_date_sql("s.timestamp")

    def test_probe_zone_is_a_fixed_offset_ahead_of_every_universe_zone(self):
        """Validity range of the trick (module docstring): the probe zone must
        be a fixed offset and sit strictly less than 24h ahead of every stamp
        zone, i.e. offset(stamp) > -10h with the probe at +14h."""
        assert PROBE_TZ == "Etc/GMT-14"
        probe = ZoneInfo(PROBE_TZ)
        for d in TRADING_DATES:
            noon = dt.datetime(d.year, d.month, d.day, 12, tzinfo=dt.timezone.utc)
            assert noon.astimezone(probe).utcoffset() == dt.timedelta(hours=14)
            for z in UNIVERSE_ZONES:
                assert noon.astimezone(ZoneInfo(z)).utcoffset() > dt.timedelta(hours=-10)


class TestWithTradeDate:

    def test_default_token_expands_to_timestamp_column(self):
        out = with_trade_date("SELECT __TRADE_DATE__ AS d FROM t")
        assert out == "SELECT " + trade_date_sql("timestamp") + " AS d FROM t"

    def test_aliased_token_expands_to_that_column(self):
        out = with_trade_date("SELECT __TRADE_DATE(s.timestamp)__ AS ts_date FROM s")
        assert out == "SELECT " + trade_date_sql("s.timestamp") + " AS ts_date FROM s"

    def test_every_occurrence_is_expanded(self):
        out = with_trade_date("__TRADE_DATE__ >= $a AND __TRADE_DATE__ < $b")
        assert "__TRADE_DATE" not in out
        assert out.count("Etc/GMT-14") == 2

    def test_missing_token_raises_instead_of_silently_passing_through(self):
        with pytest.raises(ValueError):
            with_trade_date("SELECT CAST(timestamp AS DATE) FROM t")


def _bar_frame(symbol, zone, dates):
    n = len(dates)
    return pl.DataFrame({
        "symbol": [symbol] * n,
        "timestamp": [dt.datetime(d.year, d.month, d.day, tzinfo=ZoneInfo(zone)) for d in dates],
        "timeframe": ["1D"] * n,
        "open": [100.0] * n, "high": [105.0] * n, "low": [95.0] * n, "close": [102.0] * n,
        "volume": [1_000_000] * n, "log_return": [0.001] * n, "vwap": [101.0] * n,
        "dollar_volume": [1.0e8] * n, "spread_hl": [0.1] * n,
        "is_adjusted": [True] * n, "adj_factor": [1.0] * n,
        "is_clean": [True] * n, "data_source": ["yfinance"] * n,
    }).with_columns(pl.col("timestamp").dt.convert_time_zone(zone))


IDX_BARS = [dt.date(2026, 10, d) for d in (8, 9, 12, 13, 14, 15)]


class TestPitGuardIsSessionIndependent:
    """The PIT guard `< trade_date` is where 'one day early' becomes
    look-ahead: IDX bar D (00:00+07 = D-1 17:00 UTC) read as D-1 under a UTC
    session is returned for trade_date D."""

    @pytest.mark.parametrize("session_tz", SESSION_ZONES)
    def test_bar_stamped_trade_date_is_never_returned(self, tmp_path, monkeypatch, session_tz):
        import src.backtest.pit_data as pit

        monkeypatch.setattr(pit, "SILVER_OHLCV_ROOT", tmp_path)
        path = tmp_path / "idx" / "symbol=BBCA" / "BBCA_1D_silver.parquet"
        path.parent.mkdir(parents=True)
        _bar_frame("BBCA", "Asia/Jakarta", IDX_BARS).write_parquet(path)

        loader = pit.PITDataLoader()
        loader._con.execute(f"SET TimeZone='{session_tz}'")
        out = loader.get_ohlcv("BBCA", "1D", dt.date(2026, 10, 15))

        # DuckDB hands the column back in the SESSION zone (zone name is dropped on
        # read), so take the stamp's own date by converting back to the zone the
        # test wrote it in — the very thing the SQL cannot know.
        own = out["timestamp"].dt.convert_time_zone("Asia/Jakarta").dt.date().to_list()
        assert own == [d for d in IDX_BARS if d < dt.date(2026, 10, 15)], (
            f"session={session_tz}: PIT guard returned {own}"
        )

    @pytest.mark.parametrize("session_tz", SESSION_ZONES)
    def test_universe_loader_applies_the_same_guard(self, tmp_path, monkeypatch, session_tz):
        import src.backtest.pit_data as pit

        monkeypatch.setattr(pit, "SILVER_OHLCV_ROOT", tmp_path)
        path = tmp_path / "idx" / "symbol=BBCA" / "BBCA_1D_silver.parquet"
        path.parent.mkdir(parents=True)
        _bar_frame("BBCA", "Asia/Jakarta", IDX_BARS).write_parquet(path)

        loader = pit.PITDataLoader()
        loader._con.execute(f"SET TimeZone='{session_tz}'")
        out = loader.get_ohlcv_universe(["BBCA"], "1D", dt.date(2026, 10, 15))

        own = out["timestamp"].dt.convert_time_zone("Asia/Jakarta").dt.date().to_list()
        assert own == [d for d in IDX_BARS if d < dt.date(2026, 10, 15)], (
            f"session={session_tz}: universe PIT guard returned {own}"
        )


class TestQualityValidatorFreshnessIsSessionIndependent:

    @pytest.mark.parametrize("session_tz", SESSION_ZONES)
    def test_idx_lag_is_measured_from_the_stamps_own_date(self, tmp_path, monkeypatch, session_tz):
        """Latest IDX bar stamped 2026-10-15; run_date 2026-10-20 is a lag of
        exactly 5 days -> passes (threshold is > 5). Under a UTC/New York
        session the old cast read the bar as 10-14, lag 6, a spurious fail."""
        import src.silver.quality_validator as qv_mod

        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        path = tmp_path / "idx" / "symbol=BBCA" / "BBCA_1D_silver.parquet"
        path.parent.mkdir(parents=True)
        _bar_frame("BBCA", "Asia/Jakarta", IDX_BARS).write_parquet(path)

        real_connect = duckdb.connect

        def _connect_in_session_zone(*a, **k):
            con = real_connect(*a, **k)
            con.execute(f"SET TimeZone='{session_tz}'")
            return con

        monkeypatch.setattr(qv_mod.duckdb, "connect", _connect_in_session_zone)
        qv = qv_mod.QualityValidator()
        assert qv._check_freshness(dt.date(2026, 10, 20)) is True
        assert qv._check_freshness(dt.date(2026, 10, 21)) is False   # lag 6: real staleness still fails


SRC = Path(__file__).resolve().parents[2] / "src"
RAW_CAST = re.compile(r"CAST\((?:\w+\.)?timestamp AS DATE\)")

# Cross-asset modules that pivot returns on a per-day index ACROSS markets.
# Their date alignment is a methodology decision (lead-lag lag convention,
# XAE-CAL-01 calibration, ADR expectation 'TWSE -> IXIC at lag 1'), not a
# bug-fix: see KNOWN_RISKS RISK-37 decision D1. Remove an entry here in the
# same change that converts the module — the test below fails if one is left
# behind after its casts are gone.
PENDING_DECISION_D1 = {
    "gold/cross_asset/correlation_module.py",
    "gold/cross_asset/lead_lag_module.py",
    "gold/cross_asset/forecast_module.py",
    "gold/cross_asset/broad_dollar.py",
}


class TestNoRawSessionZoneCastInSrc:

    @staticmethod
    def _files_with_raw_cast():
        hits = set()
        for p in SRC.rglob("*.py"):
            rel = p.relative_to(SRC).as_posix()
            if rel == "utils/trade_date.py":   # documents the pattern it replaces
                continue
            if RAW_CAST.search(p.read_text()):
                hits.add(rel)
        return hits

    def test_no_new_raw_cast_against_silver_timestamp(self):
        stray = self._files_with_raw_cast() - PENDING_DECISION_D1
        assert not stray, (
            f"{sorted(stray)}: CAST(timestamp AS DATE) depends on the DuckDB session "
            "TimeZone; use __TRADE_DATE__ + with_trade_date() (src/utils/trade_date.py)"
        )

    def test_pending_decision_list_has_no_stale_entries(self):
        stale = PENDING_DECISION_D1 - self._files_with_raw_cast()
        assert not stale, f"converted already — drop from PENDING_DECISION_D1: {sorted(stale)}"
