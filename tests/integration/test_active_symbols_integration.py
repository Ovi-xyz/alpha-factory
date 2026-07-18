"""
test_active_symbols_integration.py — ActiveSymbolsResolver Integration Tests
Precision Audit v1.7.1 — end-to-end validation with realistic Silver data

Integration coverage:
  - Full resolve cycle with multi-symbol Silver Parquet
  - Always-in guarantee under LIMIT pressure (AS-5)
  - 20D ROW_NUMBER window with clean-only data (AS-3 + AS-4)
  - Output schema completeness (AS-6)
  - Load/load_full roundtrip (AS-6)
  - Fallback path (AS-2)
  - Dirty row isolation (AS-4)
  - Multiple run dates (reproducibility)
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.silver.active_symbols import (
    RESOLVER_VERSION,
    THRESHOLDS,
    ActiveSymbolsResolver,
    _ALWAYS_IN_MARKETS,
    _SCREENED_LIMIT,
)


# ── Shared fixture helpers ─────────────────────────────────────────────────────

def _make_parquet(
    tmp_path: Path,
    configs: list[tuple],   # (symbol, close, volume, is_clean)
    n_days: int = 45,
    base_date: date = date(2025, 1, 2),
) -> str:
    """Write a multi-symbol Silver Parquet and return glob path."""
    rows: list[dict] = []
    for sym, close_price, vol, is_clean in configs:
        for i in range(n_days):
            rows.append({
                "symbol":    sym,
                "timestamp": base_date + timedelta(days=i),
                "close":     float(close_price) + i * 0.01,
                "volume":    int(vol),
                "is_clean":  is_clean,
            })

    df = pl.DataFrame(rows).with_columns(pl.col("timestamp").cast(pl.Date))
    out = tmp_path / "silver_1d" / "data.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out)
    return str(out.parent / "*.parquet")


def _make_market_mock(market_map: dict[str, str]) -> MagicMock:
    """Build a minimal InstrumentLoader mock."""
    mock = MagicMock()
    mock.market_map.return_value = market_map
    mock.symbol_list.return_value = list(market_map.keys())
    return mock


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def full_silver(tmp_path) -> tuple[str, dict[str, str]]:
    """
    Realistic multi-symbol Silver 1D Parquet.
    Returns (glob_path, market_map).

    Symbols:
      AAPL ($7.6B/day)   → us_stocks → should PASS threshold
      MSFT ($10.5B/day)  → us_stocks → should PASS threshold
      LOWVOL ($5k/day)   → us_stocks → should FAIL threshold
      BBCA (IDR 1.7T/day) → idx      → should PASS threshold
      EUR_USD             → forex    → always_in (threshold=0)
      SPX                 → index    → always_in
      AU                  → commodity → always_in
    """
    configs = [
        ("AAPL",    152.0,  50_000_000,   True),
        ("MSFT",    350.0,  30_000_000,   True),
        ("LOWVOL",    5.0,      1_000,    True),
        ("BBCA",  8_500.0, 200_000_000,   True),
        ("EUR_USD",   1.08,         0,    True),
        ("SPX",   4_800.0,          0,    True),
        ("AU",    2_000.0,          0,    True),
    ]
    market_map = {
        "AAPL": "us_stocks", "MSFT": "us_stocks", "LOWVOL": "us_stocks",
        "BBCA": "idx", "EUR_USD": "forex", "SPX": "index", "AU": "commodity",
    }
    path = _make_parquet(tmp_path, configs)
    return path, market_map


# ── Core integration tests ─────────────────────────────────────────────────────

class TestResolveCore:

    def test_forex_always_included(self, full_silver, tmp_path, monkeypatch):
        """Integration: EUR_USD (forex) always in active symbols."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            symbols = resolver.resolve(silver_path, date(2025, 2, 28))

        assert "EUR_USD" in symbols, "forex must always be in active symbols"

    def test_index_always_included(self, full_silver, tmp_path, monkeypatch):
        """Integration: SPX (index) always in active symbols."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            symbols = resolver.resolve(silver_path, date(2025, 2, 28))

        assert "SPX" in symbols, "index must always be in active symbols"

    def test_commodity_always_included(self, full_silver, tmp_path, monkeypatch):
        """Integration: AU (commodity) always in active symbols."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            symbols = resolver.resolve(silver_path, date(2025, 2, 28))

        assert "AU" in symbols, "commodity must always be in active symbols"

    def test_high_volume_us_stock_included(self, full_silver, tmp_path, monkeypatch):
        """Integration: AAPL ($7.6B/day) must pass US threshold ($10M)."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            symbols = resolver.resolve(silver_path, date(2025, 2, 28))

        assert "AAPL" in symbols

    def test_low_volume_us_stock_excluded(self, full_silver, tmp_path, monkeypatch):
        """Integration: LOWVOL ($5k/day) must fail US threshold ($10M)."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            symbols = resolver.resolve(silver_path, date(2025, 2, 28))

        assert "LOWVOL" not in symbols

    def test_idx_high_volume_included(self, full_silver, tmp_path, monkeypatch):
        """Integration: BBCA (IDR 1.7T/day) must pass IDX threshold (IDR 5B)."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            symbols = resolver.resolve(silver_path, date(2025, 2, 28))

        assert "BBCA" in symbols

    def test_resolve_returns_list_of_strings(self, full_silver, tmp_path, monkeypatch):
        """Integration: resolve() return type must be list[str]."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            result = resolver.resolve(silver_path, date(2025, 2, 28))

        assert isinstance(result, list)
        assert all(isinstance(s, str) for s in result)
        assert len(result) > 0


# ── AS-3 + AS-4: 20D window with clean-only data ──────────────────────────────

class TestTwentyDayCleanWindow:

    def test_exactly_20_day_window(self, tmp_path, monkeypatch):
        """
        AS-3: dollar_volume_20d must use exactly 20 most-recent trading days.
        Test: give AAPL 25 clean days in the window. Row 1-20 (most recent)
        have high volume. Rows 21-25 (older) have very low volume.
        Result: AAPL should pass threshold (20D avg = high volume).
        """
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        base_date = date(2025, 1, 2)
        run_date  = date(2025, 2, 15)

        rows = []
        # Most recent 20 days: high volume $500M/day
        for i in range(20):
            d = run_date - timedelta(days=i)
            rows.append({"symbol": "AAPL", "timestamp": d,
                         "close": 150.0, "volume": 3_333_333, "is_clean": True})
        # Older 5 days inside window: very low volume (should NOT affect 20D avg)
        for i in range(20, 25):
            d = run_date - timedelta(days=i)
            rows.append({"symbol": "AAPL", "timestamp": d,
                         "close": 150.0, "volume": 1, "is_clean": True})

        df = pl.DataFrame(rows).with_columns(pl.col("timestamp").cast(pl.Date))
        out = tmp_path / "silver_1d" / "test.parquet"
        out.parent.mkdir(parents=True)
        df.write_parquet(out)

        market_map = {"AAPL": "us_stocks"}
        resolver   = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            symbols = resolver.resolve(str(out.parent / "*.parquet"), run_date)

        # 20D avg = (150 * 3_333_333) = $500M >> $10M threshold
        assert "AAPL" in symbols, (
            "AS-3: AAPL with 20 high-volume days must pass, "
            "older low-volume days must not drag average down."
        )

    def test_dirty_rows_excluded_from_20d_avg(self, tmp_path, monkeypatch):
        """AS-4: dirty rows excluded even from ROW_NUMBER window calculation."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        base_date = date(2025, 1, 2)
        run_date  = date(2025, 2, 15)

        rows = []
        # 20 clean days with moderate volume — should pass
        for i in range(20):
            d = run_date - timedelta(days=i)
            rows.append({"symbol": "AAPL", "timestamp": d,
                         "close": 150.0, "volume": 100_000, "is_clean": True})
        # 1 dirty day with enormous spike — must NOT count
        rows.append({"symbol": "AAPL", "timestamp": run_date - timedelta(days=21),
                     "close": 150.0, "volume": 999_999_999, "is_clean": False})

        df = pl.DataFrame(rows).with_columns(pl.col("timestamp").cast(pl.Date))
        out = tmp_path / "silver_1d" / "dirty_test.parquet"
        out.parent.mkdir(parents=True)
        df.write_parquet(out)

        market_map = {"AAPL": "us_stocks"}
        resolver   = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            symbols = resolver.resolve(str(out.parent / "*.parquet"), run_date)

        # Clean 20D avg = 150 * 100_000 = $15M > $10M threshold → included
        assert "AAPL" in symbols

        # Verify dollar_volume_20d is not inflated by dirty spike
        df_out = resolver.load_full(run_date)
        aapl   = df_out.filter(pl.col("symbol") == "AAPL")
        if aapl.height > 0:
            dvol = aapl["dollar_volume_20d"][0]
            # Should be ~$15M; dirty spike ($150B) would inflate dramatically
            assert dvol < 1_000_000_000, (
                f"AS-4: dirty volume spike inflated dvol to {dvol:.0f}. "
                "is_clean filter must exclude dirty rows from AVG."
            )


# ── AS-5: LIMIT pressure test ─────────────────────────────────────────────────

class TestAlwaysInUnderLimitPressure:

    def test_always_in_not_truncated_when_screened_at_limit(
        self, tmp_path, monkeypatch
    ):
        """
        AS-5: when screened fills exactly _SCREENED_LIMIT slots,
        always-in markets must STILL be in the output (not truncated).
        """
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        n_screened = _SCREENED_LIMIT + 5   # More than the screened LIMIT

        # Build many high-volume US stocks + always-in symbols
        configs = []
        market_map = {}
        for i in range(n_screened):
            sym = f"STOCK{i:03d}"
            configs.append((sym, 100.0, 10_000_000, True))   # $1B/day each >> $10M
            market_map[sym] = "us_stocks"

        # Always-in symbols
        for sym, mkt in [("EUR_USD", "forex"), ("SPX", "index"), ("AU", "commodity")]:
            configs.append((sym, 1.0, 0, True))
            market_map[sym] = mkt

        silver_path = _make_parquet(tmp_path, configs)
        resolver    = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            symbols = resolver.resolve(silver_path, date(2025, 2, 28))

        sym_set = set(symbols)
        assert "EUR_USD" in sym_set, "AS-5: EUR_USD must not be truncated by LIMIT"
        assert "SPX"     in sym_set, "AS-5: SPX must not be truncated by LIMIT"
        assert "AU"      in sym_set, "AS-5: AU must not be truncated by LIMIT"

    def test_screened_limited_to_175(self, tmp_path, monkeypatch):
        """AS-5: screened us_stocks capped at _SCREENED_LIMIT = 175."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))

        # Build 200 high-volume US stocks
        configs = []
        market_map = {}
        for i in range(200):
            sym = f"STK{i:03d}"
            configs.append((sym, 100.0, 10_000_000, True))
            market_map[sym] = "us_stocks"

        silver_path = _make_parquet(tmp_path, configs)
        resolver    = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            symbols = resolver.resolve(silver_path, date(2025, 2, 28))

        screened_count = sum(
            1 for s in symbols
            if market_map.get(s) == "us_stocks"
        )
        assert screened_count <= _SCREENED_LIMIT, (
            f"AS-5: screened count {screened_count} > limit {_SCREENED_LIMIT}"
        )


# ── AS-6: Output schema roundtrip ─────────────────────────────────────────────

class TestOutputSchemaRoundtrip:

    def test_load_full_schema(self, full_silver, tmp_path, monkeypatch):
        """AS-6: load_full() returns all required audit columns."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            resolver.resolve(silver_path, date(2025, 2, 28))

        df = resolver.load_full(date(2025, 2, 28))
        required = {
            "symbol", "market", "dollar_volume_20d", "clean_days",
            "last_close", "eligibility_reason", "resolved_date",
            "resolver_version", "unknown_market_count", "is_fallback",
        }
        assert required.issubset(set(df.columns)), (
            f"Missing columns: {required - set(df.columns)}"
        )

    def test_load_returns_same_symbols_as_resolve(
        self, full_silver, tmp_path, monkeypatch
    ):
        """AS-6: load() must return same symbols as resolve()."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            resolved_syms = resolver.resolve(silver_path, date(2025, 2, 28))

        loaded_syms = resolver.load(date(2025, 2, 28))
        assert sorted(resolved_syms) == sorted(loaded_syms)

    def test_resolver_version_in_output(self, full_silver, tmp_path, monkeypatch):
        """AS-6: resolver_version must match RESOLVER_VERSION constant."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            resolver.resolve(silver_path, date(2025, 2, 28))

        df = resolver.load_full(date(2025, 2, 28))
        assert all(v == RESOLVER_VERSION for v in df["resolver_version"].to_list())

    def test_is_fallback_false_on_normal_resolve(
        self, full_silver, tmp_path, monkeypatch
    ):
        """AS-6 + AS-2: is_fallback=False when Silver data is available."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            resolver.resolve(silver_path, date(2025, 2, 28))

        df = resolver.load_full(date(2025, 2, 28))
        assert not any(df["is_fallback"].to_list()), (
            "AS-2: is_fallback must be False for normal (non-fallback) resolve."
        )

    def test_eligibility_reason_values_correct(
        self, full_silver, tmp_path, monkeypatch
    ):
        """AS-6: eligibility_reason must be always_in or liquidity_screened only."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            resolver.resolve(silver_path, date(2025, 2, 28))

        df = resolver.load_full(date(2025, 2, 28))
        allowed = {"always_in", "liquidity_screened"}
        actual  = set(df["eligibility_reason"].to_list())
        assert actual.issubset(allowed), f"Unexpected eligibility values: {actual - allowed}"

    def test_always_in_symbols_have_correct_reason(
        self, full_silver, tmp_path, monkeypatch
    ):
        """AS-5 + AS-6: forex/index/commodity get eligibility_reason='always_in'."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            resolver.resolve(silver_path, date(2025, 2, 28))

        df = resolver.load_full(date(2025, 2, 28))
        always_in_df = df.filter(
            pl.col("market").is_in(list(_ALWAYS_IN_MARKETS))
        )
        reasons = set(always_in_df["eligibility_reason"].to_list())
        assert reasons == {"always_in"}, (
            f"AS-5/AS-6: always-in markets must have reason='always_in', got {reasons}"
        )


# ── AS-2: Fallback path integration ──────────────────────────────────────────

class TestFallbackPath:

    def test_fallback_full_universe_when_silver_missing(
        self, tmp_path, monkeypatch
    ):
        """AS-2: when Silver not ready, returns full universe with is_fallback=True."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()
        missing  = str(tmp_path / "does_not_exist" / "*.parquet")

        expected_syms = ["AAPL", "MSFT", "EUR_USD"]
        with patch("src.silver.active_symbols.get_loader") as ml:
            mock_inst = _make_market_mock(
                {"AAPL": "us_stocks", "MSFT": "us_stocks", "EUR_USD": "forex"}
            )
            mock_inst.symbol_list.return_value = expected_syms
            ml.return_value = mock_inst
            symbols = resolver.resolve(missing, date(2025, 1, 22))

        assert sorted(symbols) == sorted(expected_syms)
        df = resolver.load_full(date(2025, 1, 22))
        assert all(df["is_fallback"].to_list())

    def test_fallback_output_has_fallback_reason(self, tmp_path, monkeypatch):
        """AS-2: fallback output rows have eligibility_reason='fallback_full_universe'."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()
        missing  = str(tmp_path / "not_here" / "*.parquet")

        with patch("src.silver.active_symbols.get_loader") as ml:
            mock_inst = _make_market_mock({"EUR_USD": "forex"})
            mock_inst.symbol_list.return_value = ["EUR_USD"]
            ml.return_value = mock_inst
            resolver.resolve(missing, date(2025, 1, 22))

        df = resolver.load_full(date(2025, 1, 22))
        reasons = set(df["eligibility_reason"].to_list())
        assert reasons == {"fallback_full_universe"}


# ── Reproducibility across run dates ──────────────────────────────────────────

class TestReproducibility:

    def test_same_run_date_same_result(self, full_silver, tmp_path, monkeypatch):
        """Two calls with same run_date must return identical sorted results."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            s1 = resolver.resolve(silver_path, date(2025, 2, 28))
            s2 = resolver.resolve(silver_path, date(2025, 2, 28))

        assert sorted(s1) == sorted(s2)

    def test_different_run_dates_independent(self, full_silver, tmp_path, monkeypatch):
        """Resolves for different dates must produce independent outputs."""
        silver_path, market_map = full_silver
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        d1 = date(2025, 2, 10)
        d2 = date(2025, 2, 28)

        with patch("src.silver.active_symbols.get_loader") as ml:
            ml.return_value = _make_market_mock(market_map)
            s1 = resolver.resolve(silver_path, d1)
            s2 = resolver.resolve(silver_path, d2)

        # Both should complete without error
        assert isinstance(s1, list)
        assert isinstance(s2, list)
        # Verify two separate files exist
        assert resolver.OUTPUT_PATH.joinpath(
            f"active_{d1.isoformat()}.parquet"
        ).exists()
        assert resolver.OUTPUT_PATH.joinpath(
            f"active_{d2.isoformat()}.parquet"
        ).exists()


# ── Threshold range sanity ─────────────────────────────────────────────────────

class TestThresholdSanity:

    def test_us_stocks_threshold_financial_range(self):
        """THRESHOLDS us_stocks must be in reasonable financial range."""
        us = THRESHOLDS["us_stocks"]
        assert us["dollar_volume_20d"] >= 1_000_000, "At least $1M/day"
        assert us["price_floor"]       >= 0.01
        assert us["min_days"]          >= 1

    def test_idx_threshold_financial_range(self):
        """THRESHOLDS idx must be in reasonable IDR range."""
        idx = THRESHOLDS["idx"]
        assert idx["dollar_volume_20d"] >= 1_000_000_000, "At least IDR 1B/day"

    def test_always_in_thresholds_zero(self):
        """forex/commodity/index thresholds must all be zero (always-in)."""
        for mkt in ("forex", "commodity", "index"):
            assert THRESHOLDS[mkt]["dollar_volume_20d"] == 0, (
                f"AS-5: {mkt} threshold must be 0 — enforced via UNION policy."
            )
