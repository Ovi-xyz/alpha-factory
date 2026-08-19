"""
tests/unit/test_ohlcv_aggregator.py — Silver OHLCV Aggregator unit tests

v1.5 refactoring: ohlcv_aggregator dipindah Bronze → Silver.
Import path diupdate: src.silver.ohlcv_aggregator (bukan src.bronze).
TestAggregateWeekly + TestAggregateMonthly DIHAPUS:
    - 1W dan 1M adalah raw Bronze data dari yfinance (interval=1wk/1mo)
    - _aggregate_weekly() dan _aggregate_monthly() adalah dead code yang dihapus
TestCanAggregate diupdate: hanya 1H→4H yang valid di Silver layer.
"""

from datetime import datetime, timedelta

import polars as pl
import pytest

# v1.5: import dari Silver layer — bukan Bronze
from src.silver.ohlcv_aggregator import (
    aggregate_ohlcv,
    can_aggregate,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_1h_bars(n_days: int = 5, symbol: str = "AAPL") -> pl.DataFrame:
    """Generate n_days × 8 trading hours = n_days*8 1H bars (Silver schema subset)."""
    rows = []
    base  = datetime(2025, 1, 6, 9, 0)   # Monday 09:00 US market open
    price = 150.0

    for d in range(n_days):
        for h in range(8):   # 9:00–16:00 = 8 bars
            ts = base + timedelta(days=d, hours=h)
            price += 0.1
            rows.append({
                "symbol":    symbol,
                "timestamp": ts,
                "open":      round(price - 0.05, 4),
                "high":      round(price + 0.20, 4),
                "low":       round(price - 0.15, 4),
                "close":     round(price, 4),
                "volume":    100_000 + h * 10_000,
            })
    return pl.DataFrame(rows)


# ── can_aggregate — v1.5 scope ────────────────────────────────────────────────

class TestCanAggregate:

    def test_1h_to_4h_supported(self):
        """Satu-satunya kombinasi yang valid di Silver layer."""
        assert can_aggregate("1H", "4H") is True

    def test_1d_to_1w_not_supported(self):
        """1W adalah raw Bronze data dari yfinance — Silver tidak mensintesis."""
        assert can_aggregate("1D", "1W") is False

    def test_1d_to_1m_not_supported(self):
        """1M adalah raw Bronze data dari yfinance — Silver tidak mensintesis."""
        assert can_aggregate("1D", "1M") is False

    def test_5m_to_4h_not_supported(self):
        assert can_aggregate("5m", "4H") is False

    def test_1h_to_1d_not_supported(self):
        assert can_aggregate("1H", "1D") is False

    def test_invalid_raises_on_aggregate_call(self):
        """aggregate_ohlcv() harus raise ValueError untuk kombinasi tidak valid."""
        from datetime import date
        df = pl.DataFrame({
            "timestamp": [datetime(2025, 1, 6, 9)],
            "open": [150.0], "high": [151.0],
            "low": [149.0],  "close": [150.5],
            "volume": [100_000],
        })
        with pytest.raises(ValueError, match="unsupported aggregation"):
            aggregate_ohlcv(df, "1D", "1W", "AAPL")


# ── 4H Aggregation ────────────────────────────────────────────────────────────

class TestAggregate4H:

    def test_produces_fewer_bars(self):
        df1h = _make_1h_bars(n_days=5)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        assert len(df4h) < len(df1h)

    def test_open_is_first(self):
        """4H open must equal first 1H bar open in that block."""
        df1h = _make_1h_bars(n_days=1)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        assert len(df4h) > 0
        first_1h_open = df1h.sort("timestamp").row(0, named=True)["open"]
        first_4h_open = df4h.sort("timestamp").row(0, named=True)["open"]
        assert first_4h_open == first_1h_open

    def test_close_is_last(self):
        """4H close must equal last 1H bar close in that block."""
        df1h = _make_1h_bars(n_days=1)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        last_1h_close = df1h.sort("timestamp").row(-1, named=True)["close"]
        last_4h_close = df4h.sort("timestamp").row(-1, named=True)["close"]
        assert last_4h_close == last_1h_close

    def test_high_is_max(self):
        """4H high must be >= any 1H high in that block."""
        df1h = _make_1h_bars(n_days=2)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        assert df4h["high"].max() == df1h["high"].max()

    def test_low_is_min(self):
        """4H low must be <= any 1H low in that block."""
        df1h = _make_1h_bars(n_days=2)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        assert df4h["low"].min() == df1h["low"].min()

    def test_volume_is_sum(self):
        """4H volume must equal sum of all 1H volumes."""
        df1h = _make_1h_bars(n_days=1)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        assert df4h["volume"].sum() == df1h["volume"].sum()

    def test_ohlc_sanity(self):
        """high >= low, open and close within [low, high] in 4H bars."""
        df1h = _make_1h_bars(n_days=5)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        assert (df4h["high"] >= df4h["low"]).all()
        assert (df4h["open"] >= df4h["low"]).all()
        assert (df4h["open"] <= df4h["high"]).all()
        assert (df4h["close"] >= df4h["low"]).all()
        assert (df4h["close"] <= df4h["high"]).all()

    def test_vwap_added(self):
        """4H aggregation must include vwap column."""
        df1h = _make_1h_bars(n_days=2)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        assert "vwap" in df4h.columns

    def test_vwap_in_range(self):
        """VWAP (typical price weighted) must be between low and high."""
        df1h = _make_1h_bars(n_days=3)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        valid = df4h.filter(pl.col("vwap").is_not_null())
        assert len(valid) > 0
        assert (valid["vwap"] >= valid["low"]).all()
        assert (valid["vwap"] <= valid["high"]).all()

    def test_bar_count_added(self):
        """bar_count column must be present — FIX Bug 4."""
        df1h = _make_1h_bars(n_days=2)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        assert "bar_count" in df4h.columns

    def test_is_incomplete_bar_added(self):
        """is_incomplete_bar flag must be present — FIX Bug 4."""
        df1h = _make_1h_bars(n_days=2)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        assert "is_incomplete_bar" in df4h.columns

    def test_canonical_block_start_timestamp(self):
        """
        Timestamp harus UTC block-start, bukan first() dari sub-bar — FIX Bug 3.
        Block [09:00, 10:00, 11:00, 12:00] → canonical 08:00 (block [08-11]).
        """
        df1h = _make_1h_bars(n_days=1)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        hours = [ts.hour for ts in df4h.sort("timestamp")["timestamp"].to_list()]
        for h in hours:
            assert h % 4 == 0, f"Timestamp hour {h} is not a UTC block start (must be multiple of 4)"

    def test_passthrough_symbol_column(self):
        """symbol column harus dipreserve setelah group_by — FIX Bug 5."""
        df1h = _make_1h_bars(n_days=2, symbol="MSFT")
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "MSFT")
        assert "symbol" in df4h.columns
        assert df4h["symbol"].unique().to_list() == ["MSFT"]

    def test_sorted_ascending(self):
        """Output harus sorted by timestamp ascending."""
        df1h = _make_1h_bars(n_days=5)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        ts = df4h["timestamp"].to_list()
        assert ts == sorted(ts)

    def test_empty_input_returns_empty(self):
        df4h = aggregate_ohlcv(pl.DataFrame(), "1H", "4H", "AAPL")
        assert len(df4h) == 0

    def test_input_from_silver_schema(self):
        """
        Aggregator harus bisa menerima Silver 1H DataFrame (full Silver schema).
        Silver 1H memiliki extra columns (is_clean, log_return, dll) yang harus
        tidak mempengaruhi hasil agregasi.
        """
        df1h = _make_1h_bars(n_days=3)
        # Tambah Silver-style extra columns
        df1h = df1h.with_columns([
            pl.lit(True).alias("is_clean"),
            pl.lit(1.0).alias("adj_factor"),
            pl.lit(True).alias("is_adjusted"),
            pl.lit("yfinance").alias("data_source"),
        ])
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")
        assert len(df4h) > 0
        # Core OHLCV columns harus ada
        for col in ["open", "high", "low", "close", "volume", "vwap"]:
            assert col in df4h.columns

    def test_vwap_uses_subar_typical_price(self):
        """
        FIX Bug 2: VWAP = sum(tp_i * vol_i) / sum(vol_i) per sub-bar.
        Bukan (H+L+C)/3 dari aggregated bar — itu menggunakan block max(H), min(L).
        """
        # Buat 4 bars dengan volume berbeda untuk memastikan VWAP weighted benar
        rows = [
            {"symbol": "AAPL", "timestamp": datetime(2025, 1, 6, 8),
             "open": 100.0, "high": 110.0, "low": 90.0,  "close": 105.0, "volume": 1_000},
            {"symbol": "AAPL", "timestamp": datetime(2025, 1, 6, 9),
             "open": 105.0, "high": 115.0, "low": 95.0,  "close": 110.0, "volume": 2_000},
            {"symbol": "AAPL", "timestamp": datetime(2025, 1, 6, 10),
             "open": 110.0, "high": 120.0, "low": 100.0, "close": 115.0, "volume": 3_000},
            {"symbol": "AAPL", "timestamp": datetime(2025, 1, 6, 11),
             "open": 115.0, "high": 125.0, "low": 105.0, "close": 120.0, "volume": 4_000},
        ]
        df1h = pl.DataFrame(rows)
        df4h = aggregate_ohlcv(df1h, "1H", "4H", "AAPL")

        # Manual VWAP calculation: sum(tp_i * vol_i) / sum(vol_i)
        tp_vols = sum(
            ((r["high"] + r["low"] + r["close"]) / 3) * r["volume"]
            for r in rows
        )
        total_vol = sum(r["volume"] for r in rows)
        expected_vwap = tp_vols / total_vol

        actual_vwap = df4h["vwap"][0]
        assert abs(actual_vwap - expected_vwap) < 1e-6, (
            f"VWAP mismatch: expected {expected_vwap:.6f}, got {actual_vwap:.6f}"
        )


# ── FIX NEW-3 [P1 HIGH] — session-aware completeness ───────────────────────────
# audit_v1_7_3_uncovered_findings.docx Section 4, Opsi A.
# All expected_bars/is_incomplete_bar figures below were verified empirically
# against the real implementation (not hand-derived) before being asserted.

def _hourly_bars(symbol: str, day: "date", hours: list[int]) -> pl.DataFrame:
    """One 1H bar per UTC hour-of-day listed, on a single calendar day."""
    rows = []
    price = 150.0
    for h in hours:
        ts = datetime(day.year, day.month, day.day, h, 0)
        price += 0.1
        rows.append({
            "symbol": symbol, "timestamp": ts,
            "open": price - 0.05, "high": price + 0.2,
            "low": price - 0.15, "close": price,
            "volume": 100_000,
        })
    return pl.DataFrame(rows)


class TestSessionAwareCompleteness:

    def test_default_market_preserves_legacy_flat_four(self):
        """
        market="" (omitted — legacy callers / existing tests) must reproduce
        the exact pre-fix flat EXPECTED_BARS=4 behavior. UTC hours 13-20 split
        into blocks [12-15](3 bars),[16-19](4 bars),[20-23](1 bar) under a flat
        threshold of 4 → 2 of 3 blocks incomplete, regardless of market.
        """
        from datetime import date as _date
        df = _hourly_bars("TEST", _date(2025, 1, 6), list(range(13, 21)))
        out = aggregate_ohlcv(df, "1H", "4H", "TEST")   # no market kwarg
        flags = out.sort("timestamp")["is_incomplete_bar"].to_list()
        assert flags == [True, False, True]

    def test_us_stocks_session_overlap_replaces_flat_threshold(self):
        """
        Same fixture as above, but with market='us_stocks': all 3 blocks
        become NOT incomplete because each block's actual bar_count meets or
        exceeds the session-overlap-derived expected count (NYSE 09:30-16:00
        ET → on this date, EST/winter, block[12-15] expects 2, block[16-19]
        expects 4, block[20-23] expects 1 — all satisfied by 3/4/1 actual bars).
        """
        from datetime import date as _date
        df = _hourly_bars("TEST", _date(2025, 1, 6), list(range(13, 21)))
        out = aggregate_ohlcv(df, "1H", "4H", "TEST", market="us_stocks")
        assert out.sort("timestamp")["is_incomplete_bar"].to_list() == [False, False, False]

    def test_genuine_intraday_gap_still_flagged_incomplete(self):
        """
        Critical regression guard distinguishing Opsi A from the rejected
        Opsi C ("lower the threshold"): a REAL gap during active session hours
        must still be flagged. Block [16-19] UTC on 2025-01-06 is fully within
        the NYSE session (expected=4); providing only 3 of its 4 hours (17:00
        missing) must still produce is_incomplete_bar=True.
        """
        from datetime import date as _date
        df = _hourly_bars("TEST", _date(2025, 1, 6), [16, 18, 19])   # 17 missing
        out = aggregate_ohlcv(df, "1H", "4H", "TEST", market="us_stocks")
        assert len(out) == 1
        assert out["bar_count"][0] == 3
        assert out["is_incomplete_bar"][0] is True

    def test_off_session_block_with_no_data_not_flagged(self):
        """
        A block with zero overlap with the trading session (e.g. [00-03] UTC,
        entirely overnight for NYSE) structurally has expected_bars=0 — even
        if it happens to have 0 actual sub-bars (no data, since no symbol-level
        group would even be produced for an empty block in practice), the
        guard `expected > 0` must prevent a False is_incomplete_bar verdict.
        Exercised directly via _expected_bars_by_block to check the underlying
        expected count used by the guard.
        """
        from datetime import date as _date
        from src.silver.ohlcv_aggregator import _expected_bars_by_block
        expected = _expected_bars_by_block(
            pl.Series([_date(2025, 1, 6)]),
            pl.Series([0], dtype=pl.Int32),
            "us_stocks",
        )
        assert expected.to_list() == [0]

    def test_idx_session_window(self):
        """
        IDX session 09:00-14:30 Asia/Jakarta (fixed UTC+7, no DST) ==
        02:00-07:30 UTC. Block[00-03] expects 2 in-session hours (02,03);
        block[04-07] expects 4 (04,05,06,07 all overlap); later blocks expect 0.
        Reuses the exact session window already established by FIX TVA-3.
        """
        from datetime import date as _date
        from src.silver.ohlcv_aggregator import _expected_bars_by_block
        dates  = pl.Series([_date(2025, 1, 6)] * 6)
        blocks = pl.Series([0, 4, 8, 12, 16, 20], dtype=pl.Int32)
        assert _expected_bars_by_block(dates, blocks, "idx").to_list() == \
            [2, 4, 0, 0, 0, 0]

    def test_forex_and_commodity_keep_flat_fallback(self):
        """NEAR_24H_MARKETS (forex, commodity) are unaffected by NEW-3 — audit
        §4 explicitly notes these are not materially impacted."""
        from datetime import date as _date
        from src.silver.ohlcv_aggregator import _expected_bars_by_block, EXPECTED_BARS
        dates  = pl.Series([_date(2025, 1, 6)] * 6)
        blocks = pl.Series([0, 4, 8, 12, 16, 20], dtype=pl.Int32)
        for market in ("forex", "commodity"):
            result = _expected_bars_by_block(dates, blocks, market)
            assert result.to_list() == [EXPECTED_BARS["4H"]] * 6

    def test_unrecognized_market_falls_back_safely(self):
        """A market string not in MARKET_SESSION_LOCAL/NEAR_24H_MARKETS must
        fall back to the flat default rather than raise or silently zero out."""
        from datetime import date as _date
        from src.silver.ohlcv_aggregator import _expected_bars_by_block, EXPECTED_BARS
        result = _expected_bars_by_block(
            pl.Series([_date(2025, 1, 6)]), pl.Series([12], dtype=pl.Int32),
            "totally_unknown_market_xyz",
        )
        assert result.to_list() == [EXPECTED_BARS["4H"]]

    def test_dst_shifts_expected_bars_for_same_utc_block(self):
        """
        DST-correctness: the SAME UTC block_hour=12 produces a DIFFERENT
        expected_bars count in winter (EST, UTC-5) vs summer (EDT, UTC-4) —
        proof that conversion goes through the real IANA tz database (matching
        OHLCVProcessor._normalize_timestamps) rather than a fixed UTC offset.
        """
        from datetime import date as _date
        from src.silver.ohlcv_aggregator import _expected_bars_by_block
        dates  = pl.Series([_date(2025, 1, 6), _date(2025, 7, 7)])   # winter, summer
        blocks = pl.Series([12, 12], dtype=pl.Int32)
        result = _expected_bars_by_block(dates, blocks, "us_stocks").to_list()
        assert result == [2, 3]
        assert result[0] != result[1]

    def test_empty_series_returns_empty(self):
        """Defensive: empty block list must not raise."""
        from src.silver.ohlcv_aggregator import _expected_bars_by_block
        result = _expected_bars_by_block(
            pl.Series([], dtype=pl.Date), pl.Series([], dtype=pl.Int32), "us_stocks"
        )
        assert len(result) == 0


class TestRestorePassthroughEmptyDict:
    """Coverage tranche (17 Aug 2026) — the not passthrough_vals early-return
    branch (no passthrough columns captured, e.g. source df had none of the
    _PASSTHROUGH_COLS present)."""

    def test_empty_passthrough_vals_returns_agg_unchanged(self):
        from src.silver.ohlcv_aggregator import _restore_passthrough
        agg = pl.DataFrame({"symbol": ["AAPL"], "close": [150.0]})
        result = _restore_passthrough(agg, {})
        assert result is agg   # early return — same object, not a copy
