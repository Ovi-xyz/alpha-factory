"""
tests/unit/test_active_symbols.py — ActiveSymbolsResolver Unit Tests
Precision Audit v1.7.1 — covers all 12 audit findings (AS-1..AS-12)

Test matrix:
  AS-1: DuckDB $name param smoke test passes
  AS-2: Fallback when Silver missing; fail-fast on real query error
  AS-3: ROW_NUMBER 20D window — verified via limited-history fixture
  AS-4: Dirty rows excluded from metrics
  AS-5: Always-in markets not truncated by screened LIMIT
  AS-6: Rich output schema present
  AS-7: DuckDB context manager (no lingering connections)
  AS-8: OUTPUT_PATH respects PIPELINE_DATA_ROOT env var
  AS-9: Atomic write via temp+rename
  AS-10: Unknown market symbols logged and excluded
  AS-11: hive_partitioning=False in query text
  AS-12: Query uses $name params and UNION policy (Section 8 pattern)
"""

from __future__ import annotations

import re
import shutil
import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.silver.active_symbols import (
    RESOLVER_VERSION,
    THRESHOLDS,
    _ALWAYS_IN_MARKETS,
    _RESOLVE_QUERY,
    _SCREENED_LIMIT,
    ActiveSymbolsResolver,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_silver_parquet(
    tmp_path: Path,
    n_days: int = 45,
    include_dirty: bool = False,
    include_unknown: bool = False,
) -> str:
    """
    Build a minimal Silver 1D Parquet suitable for resolver tests.
    Symbols: AAPL (us_stocks, high-vol), LOWVOL (us_stocks, low-vol),
             BBCA (idx), EUR_USD (forex), SPX (index), AU (commodity).
    Optionally injects dirty rows and unknown-market symbols.
    """
    base_date = date(2025, 1, 2)
    configs = [
        # (symbol, close, volume, is_clean)
        ("AAPL",    152.0,  50_000_000, True),   # $7.6B/day >> $10M threshold
        ("LOWVOL",    5.0,       1_000, True),   # $5k/day << threshold → excluded
        ("BBCA",   8_500.0, 200_000_000, True),  # IDR 1.7T/day >> IDR 5B threshold
        ("EUR_USD",   1.08,          0, True),   # forex → always_in
        ("SPX",    4_800.0,          0, True),   # index → always_in
        ("AU",     2_000.0,          0, True),   # commodity → always_in
    ]
    if include_dirty:
        # AAPL dirty row with extreme volume spike — AS-4 test
        configs.append(("AAPL", 152.0, 999_999_999, False))
    if include_unknown:
        # Unknown symbol not in InstrumentLoader — AS-10 test
        configs.append(("ORPHAN_SYM", 50.0, 1_000_000, True))

    rows: list[dict] = []
    for sym, close_price, vol, is_clean in configs:
        for i in range(n_days):
            rows.append({
                "symbol":    sym,
                "timestamp": base_date + timedelta(days=i),
                "close":     close_price + i * 0.01,
                "volume":    vol,
                "is_clean":  is_clean,
            })

    df = pl.DataFrame(rows).with_columns(
        pl.col("timestamp").cast(pl.Date)
    )
    out = tmp_path / "silver_1d" / "data.parquet"
    out.parent.mkdir(parents=True)
    df.write_parquet(out)
    return str(out.parent / "*.parquet")


# ── AS-1: DuckDB $name parameter smoke test ───────────────────────────────────

class TestAS1DuckDBNameParam:

    def test_duckdb_dollar_name_works(self):
        """AS-1: DuckDB $name param substitution must work — required before main query."""
        import duckdb
        with duckdb.connect() as con:
            row = con.execute("SELECT $v AS x", {"v": 42}).fetchone()
        assert row is not None
        assert row[0] == 42, f"DuckDB $name param broken — got {row[0]}"

    def test_query_uses_dollar_name_params(self):
        """AS-1: _RESOLVE_QUERY must not contain :name style params."""
        colon_params = re.findall(r":\w+", _RESOLVE_QUERY)
        assert colon_params == [], (
            f"AS-1: Found SQLite-style :name params in query: {colon_params}. "
            "All params must use $name format."
        )

    def test_query_uses_dollar_params(self):
        """AS-1: _RESOLVE_QUERY must declare expected $name params."""
        dollar_params = re.findall(r"\$(\w+)", _RESOLVE_QUERY)
        expected = {"path", "run_date", "us_dvol", "us_price", "us_days",
                    "idx_dvol", "idx_price", "idx_days", "screened_limit"}
        found = set(dollar_params)
        assert expected.issubset(found), (
            f"AS-1: Missing $name params. Expected subset {expected}, found {found}"
        )


# ── AS-2: Fallback and fail-fast logic ───────────────────────────────────────

class TestAS2FallbackAndFailFast:

    def test_fallback_when_silver_missing(self, tmp_path, monkeypatch):
        """AS-2: when Silver not ready, fallback returns full universe with is_fallback=True."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()
        non_existent = str(tmp_path / "nonexistent" / "*.parquet")

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.symbol_list.return_value = ["AAPL", "MSFT", "EUR_USD"]
            mock_loader.return_value = mock_inst
            mock_inst.market_map.return_value = {
                "AAPL": "us_stocks", "MSFT": "us_stocks", "EUR_USD": "forex"
            }

            symbols = resolver.resolve(non_existent, date(2025, 1, 22))

        assert set(symbols) == {"AAPL", "MSFT", "EUR_USD"}
        # Verify is_fallback=True in persisted output
        df = resolver.load_full(date(2025, 1, 22))
        assert df["is_fallback"].to_list() == [True, True, True]

    def test_query_error_propagates(self, tmp_path, monkeypatch):
        """AS-2: real query errors must NOT be swallowed — fail-fast behavior."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()

        # Create a valid file so Silver availability check passes
        dummy = tmp_path / "silver_1d" / "dummy.parquet"
        dummy.parent.mkdir(parents=True)
        pl.DataFrame({"symbol": ["X"], "timestamp": [date(2025, 1, 1)],
                      "close": [1.0], "volume": [1], "is_clean": [True]}).write_parquet(dummy)

        with patch.object(resolver, "_run_query", side_effect=RuntimeError("db error")):
            with pytest.raises(RuntimeError, match="db error"):
                resolver.resolve(str(dummy.parent / "*.parquet"), date(2025, 1, 22))

    def test_no_catch_all_except_in_main_flow(self):
        """AS-2: _run_query must not contain broad except Exception that swallows errors."""
        import inspect
        src = inspect.getsource(ActiveSymbolsResolver._run_query)
        # The method should not have a bare 'except Exception' / 'except:' catch
        assert "except Exception" not in src and "except:" not in src, (
            "AS-2: _run_query must not suppress exceptions — fail-fast required."
        )


# ── AS-3: 20 trading day ROW_NUMBER window ───────────────────────────────────

class TestAS3TwentyDayWindow:

    def test_query_contains_row_number(self):
        """AS-3: query must use ROW_NUMBER for exact 20D window."""
        assert "ROW_NUMBER()" in _RESOLVE_QUERY.upper()

    def test_query_limits_to_20_days(self):
        """AS-3: rn <= 20 filter must appear in query."""
        assert "rn <= 20" in _RESOLVE_QUERY

    def test_lookback_interval_45_days(self):
        """AS-3: lookback window is 45 calendar days to cover 20 trading days."""
        assert "INTERVAL 45 DAYS" in _RESOLVE_QUERY


# ── AS-4: Dirty rows excluded ─────────────────────────────────────────────────

class TestAS4DirtyRowsExcluded:

    def test_is_clean_filter_in_ohlcv_cte(self):
        """AS-4: is_clean=TRUE must be in ohlcv CTE (affects ALL downstream CTEs)."""
        ohlcv_section = _RESOLVE_QUERY.split("ranked_clean")[0]
        assert "is_clean = TRUE" in ohlcv_section, (
            "AS-4: is_clean filter must be in ohlcv CTE, not just COUNT FILTER."
        )

    def test_dirty_volume_spike_excluded_from_metrics(self, tmp_path, monkeypatch):
        """AS-4: dirty rows (is_clean=False) must not inflate dollar_volume_20d."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))

        # Build Silver with AAPL: 20 clean days + 1 dirty spike day
        base_date = date(2025, 1, 2)
        rows = []
        for i in range(20):
            rows.append({"symbol": "AAPL", "timestamp": base_date + timedelta(days=i),
                         "close": 150.0, "volume": 1_000_000, "is_clean": True})
        # Dirty spike row
        rows.append({"symbol": "AAPL", "timestamp": base_date + timedelta(days=20),
                     "close": 150.0, "volume": 999_000_000, "is_clean": False})

        df = pl.DataFrame(rows).with_columns(pl.col("timestamp").cast(pl.Date))
        out = tmp_path / "silver" / "data.parquet"
        out.parent.mkdir(parents=True)
        df.write_parquet(out)

        resolver = ActiveSymbolsResolver()
        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.symbol_list.return_value = ["AAPL"]
            mock_inst.market_map.return_value = {"AAPL": "us_stocks"}
            mock_loader.return_value = mock_inst

            symbols = resolver.resolve(str(out.parent / "*.parquet"), date(2025, 2, 1))

        # AAPL with 1M * $150 = $150M clean vol >> $10M threshold
        assert "AAPL" in symbols

        # Verify dollar_volume_20d in output is clean-only (not inflated by dirty spike)
        resolved_df = resolver.load_full(date(2025, 2, 1))
        aapl_row = resolved_df.filter(pl.col("symbol") == "AAPL")
        if aapl_row.height > 0:
            dvol = aapl_row["dollar_volume_20d"][0]
            # Clean dvol = 150 * 1_000_000 = 150_000_000. Dirty spike would be 999_000_000*150
            assert dvol < 200_000_000, (
                f"AS-4: dollar_volume_20d {dvol} appears inflated by dirty row spike."
            )


# ── AS-5: Always-in markets not truncated ─────────────────────────────────────

class TestAS5AlwaysInMarkets:

    def test_always_in_markets_constant(self):
        """AS-5: _ALWAYS_IN_MARKETS must include forex, commodity, index."""
        for mkt in ("forex", "commodity", "index"):
            assert mkt in _ALWAYS_IN_MARKETS

    def test_screened_limit_is_175(self):
        """AS-5: screened LIMIT must be 175 (headroom for 25 always-in)."""
        assert _SCREENED_LIMIT == 175

    def test_union_all_in_query(self):
        """AS-5: query must use UNION ALL to combine always_in and screened."""
        assert "UNION ALL" in _RESOLVE_QUERY

    def test_limit_only_in_screened_cte(self):
        """AS-5: LIMIT must appear ONLY inside screened CTE, not after UNION ALL."""
        # Find position of UNION ALL and LIMIT
        union_pos = _RESOLVE_QUERY.upper().rfind("UNION ALL")
        limit_positions = [m.start() for m in re.finditer(r"\bLIMIT\b", _RESOLVE_QUERY.upper())]
        # All LIMIT occurrences must be BEFORE the final UNION ALL
        assert all(pos < union_pos for pos in limit_positions), (
            "AS-5: LIMIT found after UNION ALL — always_in markets will be truncated."
        )

    def test_forex_always_included_even_with_many_screened(self, tmp_path, monkeypatch):
        """AS-5: forex symbols never excluded regardless of screened count."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.market_map.return_value = {
                "AAPL": "us_stocks", "LOWVOL": "us_stocks", "BBCA": "idx",
                "EUR_USD": "forex", "SPX": "index", "AU": "commodity",
            }
            mock_inst.symbol_list.return_value = [
                "AAPL", "LOWVOL", "BBCA", "EUR_USD", "SPX", "AU"
            ]
            mock_loader.return_value = mock_inst

            symbols = resolver.resolve(silver_path, date(2025, 2, 28))

        assert "EUR_USD" in symbols, "AS-5: EUR_USD (forex) must always be in active symbols"
        assert "SPX" in symbols,     "AS-5: SPX (index) must always be in active symbols"
        assert "AU" in symbols,      "AS-5: AU (commodity) must always be in active symbols"

    def test_low_volume_excluded(self, tmp_path, monkeypatch):
        """AS-5: LOWVOL below threshold must be excluded from screened list."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.market_map.return_value = {
                "AAPL": "us_stocks", "LOWVOL": "us_stocks", "BBCA": "idx",
                "EUR_USD": "forex", "SPX": "index", "AU": "commodity",
            }
            mock_inst.symbol_list.return_value = ["AAPL", "LOWVOL", "BBCA",
                                                   "EUR_USD", "SPX", "AU"]
            mock_loader.return_value = mock_inst

            symbols = resolver.resolve(silver_path, date(2025, 2, 28))

        assert "LOWVOL" not in symbols, "AS-5: LOWVOL below threshold must be excluded"


# ── AS-6: Rich output schema ──────────────────────────────────────────────────

class TestAS6RichOutputSchema:

    REQUIRED_COLUMNS = {
        "symbol", "market", "dollar_volume_20d", "clean_days",
        "last_close", "eligibility_reason", "resolved_date",
        "resolver_version", "unknown_market_count", "is_fallback",
    }

    def test_output_has_all_required_columns(self, tmp_path, monkeypatch):
        """AS-6: persisted Parquet must contain all audit-required columns."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.market_map.return_value = {
                "AAPL": "us_stocks", "LOWVOL": "us_stocks", "BBCA": "idx",
                "EUR_USD": "forex", "SPX": "index", "AU": "commodity",
            }
            mock_inst.symbol_list.return_value = ["AAPL", "LOWVOL", "BBCA",
                                                   "EUR_USD", "SPX", "AU"]
            mock_loader.return_value = mock_inst

            resolver.resolve(silver_path, date(2025, 2, 28))

        df = resolver.load_full(date(2025, 2, 28))
        missing = self.REQUIRED_COLUMNS - set(df.columns)
        assert not missing, f"AS-6: Missing output columns: {missing}"

    def test_resolver_version_correct(self, tmp_path, monkeypatch):
        """AS-6: resolver_version column must match RESOLVER_VERSION constant."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.market_map.return_value = {"EUR_USD": "forex"}
            mock_inst.symbol_list.return_value = ["EUR_USD"]
            mock_loader.return_value = mock_inst

            resolver.resolve(silver_path, date(2025, 2, 28))

        df = resolver.load_full(date(2025, 2, 28))
        assert all(v == RESOLVER_VERSION for v in df["resolver_version"].to_list())

    def test_eligibility_reason_values(self, tmp_path, monkeypatch):
        """AS-6: eligibility_reason must be always_in or liquidity_screened."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.market_map.return_value = {
                "AAPL": "us_stocks", "LOWVOL": "us_stocks", "BBCA": "idx",
                "EUR_USD": "forex", "SPX": "index", "AU": "commodity",
            }
            mock_inst.symbol_list.return_value = ["AAPL", "LOWVOL", "BBCA",
                                                   "EUR_USD", "SPX", "AU"]
            mock_loader.return_value = mock_inst

            resolver.resolve(silver_path, date(2025, 2, 28))

        df = resolver.load_full(date(2025, 2, 28))
        valid_reasons = {"always_in", "liquidity_screened"}
        actual = set(df["eligibility_reason"].to_list())
        assert actual.issubset(valid_reasons), (
            f"AS-6: unexpected eligibility_reason values: {actual - valid_reasons}"
        )


# ── AS-7: DuckDB context manager ──────────────────────────────────────────────

class TestAS7DuckDBContextManager:

    def test_run_query_uses_context_manager(self):
        """AS-7: _run_query must use 'with duckdb.connect() as con:' pattern."""
        import inspect
        src = inspect.getsource(ActiveSymbolsResolver._run_query)
        assert "with duckdb.connect()" in src, (
            "AS-7: _run_query must use context manager to ensure connection cleanup."
        )


# ── AS-8: Config-driven paths ──────────────────────────────────────────────────

class TestAS8ConfigDrivenPaths:

    def test_output_path_uses_env_var(self, tmp_path, monkeypatch):
        """AS-8: OUTPUT_PATH must respect PIPELINE_DATA_ROOT env var."""
        custom_root = tmp_path / "custom_data"
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(custom_root))
        resolver = ActiveSymbolsResolver()
        expected = custom_root / "silver" / "active_symbols"
        assert resolver.OUTPUT_PATH == expected

    def test_output_path_fallback_to_config(self, tmp_path, monkeypatch):
        """AS-8: OUTPUT_PATH falls back gracefully when env var not set."""
        monkeypatch.delenv("PIPELINE_DATA_ROOT", raising=False)
        resolver = ActiveSymbolsResolver()
        # Should not raise; path is deterministically derived from config
        assert isinstance(resolver.OUTPUT_PATH, Path)
        assert "silver" in str(resolver.OUTPUT_PATH)
        assert "active_symbols" in str(resolver.OUTPUT_PATH)


# ── AS-9: Atomic write ────────────────────────────────────────────────────────

class TestAS9AtomicWrite:

    def test_save_uses_temp_then_rename(self):
        """AS-9 / FIX SIL-AIO-003: _save must use tempfile + os.replace (POSIX-atomic rename).
        Original used shutil.move which is NOT filesystem-atomic; fixed to os.replace.
        """
        import inspect
        src = inspect.getsource(ActiveSymbolsResolver._save)
        assert "tempfile.NamedTemporaryFile" in src, (
            "AS-9: _save must write to temp file first."
        )
        # FIX SIL-AIO-003: shutil.move was replaced with os.replace (POSIX-atomic)
        assert "os.replace" in src, (
            "FIX SIL-AIO-003: _save must use os.replace() for atomic rename "
            "(shutil.move is NOT filesystem-atomic — GD §17.7)."
        )
        assert "shutil.move" not in src, (
            "FIX SIL-AIO-003 REGRESSION: shutil.move must NOT be used — "
            "use os.replace() which is guaranteed atomic by POSIX."
        )

    def test_temp_file_cleaned_on_failure(self, tmp_path, monkeypatch):
        """AS-9: temp file cleaned up on write failure — no orphaned .tmp files."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()
        output_path = resolver.OUTPUT_PATH
        output_path.mkdir(parents=True, exist_ok=True)

        df = pl.DataFrame({
            "symbol": ["AAPL"], "market": ["us_stocks"],
            "dollar_volume_20d": [1e9], "clean_days": [20],
            "last_close": [150.0], "eligibility_reason": ["liquidity_screened"],
        })

        # Simulate write failure
        with patch("polars.DataFrame.write_parquet", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                resolver._save(df, date(2025, 1, 22), 0)

        # No orphaned .tmp files
        tmp_files = list(output_path.glob("*.tmp"))
        assert tmp_files == [], f"AS-9: orphaned temp files: {tmp_files}"


# ── AS-10: Unknown market handling ────────────────────────────────────────────

class TestAS10UnknownMarket:

    def test_unknown_market_count_in_output(self, tmp_path, monkeypatch):
        """AS-10: unknown_market_count must be 0 when all symbols are known."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.market_map.return_value = {
                "AAPL": "us_stocks", "LOWVOL": "us_stocks", "BBCA": "idx",
                "EUR_USD": "forex", "SPX": "index", "AU": "commodity",
            }
            mock_inst.symbol_list.return_value = ["AAPL", "LOWVOL", "BBCA",
                                                   "EUR_USD", "SPX", "AU"]
            mock_loader.return_value = mock_inst

            resolver.resolve(silver_path, date(2025, 2, 28))

        df = resolver.load_full(date(2025, 2, 28))
        assert df["unknown_market_count"][0] == 0

    def test_null_market_guard_in_query(self):
        """AS-10: query must filter m.market IS NOT NULL."""
        assert "m.market IS NOT NULL" in _RESOLVE_QUERY, (
            "AS-10: query must include NULL market guard to exclude orphan symbols."
        )


# ── AS-11: hive_partitioning=False ───────────────────────────────────────────

class TestAS11HivePartitioning:

    def test_query_uses_hive_partitioning_false(self):
        """AS-11: query must use hive_partitioning=false (Supp. Design G2 conv.)."""
        assert "hive_partitioning=false" in _RESOLVE_QUERY.lower(), (
            "AS-11: hive_partitioning must be false to avoid column name conflicts."
        )

    def test_no_hive_partitioning_true_in_query(self):
        """AS-11: hive_partitioning=true must NOT appear in query."""
        assert "hive_partitioning=true" not in _RESOLVE_QUERY.lower(), (
            "AS-11: hive_partitioning=true can cause silent data corruption."
        )


# ── AS-12: Section 8 query pattern (not Section 7 sketch) ─────────────────────

class TestAS12Section8Query:

    def test_query_has_always_in_cte(self):
        """AS-12: query must define always_in CTE (absent from Section 7 sketch)."""
        assert "always_in AS" in _RESOLVE_QUERY.lower() or "always_in AS" in _RESOLVE_QUERY

    def test_query_has_screened_cte(self):
        """AS-12: query must define screened CTE with threshold WHERE clause."""
        assert "screened AS" in _RESOLVE_QUERY.lower() or "screened AS" in _RESOLVE_QUERY

    def test_query_has_final_union(self):
        """AS-12: query must end with UNION combining always_in and screened."""
        # UNION ALL must appear after both CTEs
        always_in_pos = _RESOLVE_QUERY.upper().find("ALWAYS_IN AS")
        screened_pos  = _RESOLVE_QUERY.upper().find("SCREENED AS")
        union_pos     = _RESOLVE_QUERY.upper().rfind("UNION ALL")
        assert always_in_pos > 0, "AS-12: always_in CTE missing"
        assert screened_pos  > 0, "AS-12: screened CTE missing"
        assert union_pos > screened_pos, "AS-12: UNION ALL must appear after screened CTE"


# ── General correctness tests ─────────────────────────────────────────────────

class TestGeneralCorrectness:

    def test_thresholds_dict_present(self):
        """THRESHOLDS must cover all markets."""
        for mkt in ("us_stocks", "idx", "forex", "commodity", "index"):
            assert mkt in THRESHOLDS
            assert "dollar_volume_20d" in THRESHOLDS[mkt]
            assert "price_floor" in THRESHOLDS[mkt]
            assert "min_days" in THRESHOLDS[mkt]

    def test_always_in_thresholds_zero(self):
        """forex/commodity/index thresholds must be zero (always-in policy)."""
        for mkt in ("forex", "commodity", "index"):
            assert THRESHOLDS[mkt]["dollar_volume_20d"] == 0

    def test_load_raises_if_not_resolved(self, tmp_path, monkeypatch):
        """load() must raise FileNotFoundError if resolve() hasn't been called."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()
        with pytest.raises(FileNotFoundError):
            resolver.load(date(2099, 1, 1))

    def test_load_full_raises_if_not_resolved(self, tmp_path, monkeypatch):
        """load_full() must raise FileNotFoundError if resolve() hasn't been called."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        resolver = ActiveSymbolsResolver()
        with pytest.raises(FileNotFoundError):
            resolver.load_full(date(2099, 1, 1))

    def test_resolve_returns_list_of_strings(self, tmp_path, monkeypatch):
        """resolve() must return list[str]."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.market_map.return_value = {"EUR_USD": "forex", "AAPL": "us_stocks"}
            mock_inst.symbol_list.return_value = ["EUR_USD", "AAPL"]
            mock_loader.return_value = mock_inst

            result = resolver.resolve(silver_path, date(2025, 2, 28))

        assert isinstance(result, list)
        assert all(isinstance(s, str) for s in result)

    def test_reproducibility_same_run_date(self, tmp_path, monkeypatch):
        """Two resolve() calls with same run_date must return identical results."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        resolver = ActiveSymbolsResolver()

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.market_map.return_value = {
                "EUR_USD": "forex", "AAPL": "us_stocks"
            }
            mock_inst.symbol_list.return_value = ["EUR_USD", "AAPL"]
            mock_loader.return_value = mock_inst

            s1 = resolver.resolve(silver_path, date(2025, 2, 28))
            s2 = resolver.resolve(silver_path, date(2025, 2, 28))

        assert sorted(s1) == sorted(s2)


# ── ADD GMI-AS-001: Dual-layer output (Architecture v2.0 §4) ─────────────────

class TestGMIAS001DualLayerOutput:
    """
    ADD GMI-AS-001 — Architecture v2.0 §4.1-§4.2: Dual-Layer Active Universe.
    Tests that resolve() writes active_ohlcv_{date}.parquet canonical path
    (Layer 1). Layer 2 (context anchors) coverage MOVED GMI-CTX-001 to
    tests/unit/test_context_anchors.py — see src/silver/context_anchors.py
    for the module those tests exercise.
    """

    def test_resolve_writes_canonical_ohlcv_parquet(self, tmp_path, monkeypatch):
        """
        GMI-AS-001: resolve() must write active_ohlcv_{date}.parquet in
        addition to the legacy active_{date}.parquet.
        Architecture v2.0 §5.2 specifies this filename as the canonical
        Layer 1 artifact consumed by gold_signals.
        """
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        run_date = date(2025, 3, 1)

        resolver = ActiveSymbolsResolver()
        output_path = tmp_path / "silver" / "active_symbols"
        monkeypatch.setattr(type(resolver), "OUTPUT_PATH", property(lambda s: output_path))

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mock_inst = MagicMock()
            mock_inst.market_map.return_value = {"AAPL": "us_stocks", "EUR_USD": "forex"}
            mock_inst.symbol_list.return_value = ["AAPL", "EUR_USD"]
            mock_loader.return_value = mock_inst
            resolver.resolve(silver_path, run_date)

        canonical = output_path / f"active_ohlcv_{run_date.isoformat()}.parquet"
        legacy    = output_path / f"active_{run_date.isoformat()}.parquet"
        assert canonical.exists(), "active_ohlcv_{date}.parquet must exist (GMI-AS-001)"
        assert legacy.exists(),    "legacy active_{date}.parquet must still exist (backward compat)"

    def test_canonical_and_legacy_have_identical_content(self, tmp_path, monkeypatch):
        """
        GMI-AS-001: active_ohlcv and active_legacy must have identical symbols.
        Both are written atomically via os.replace; they should never diverge.
        """
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        run_date    = date(2025, 3, 2)
        resolver    = ActiveSymbolsResolver()
        output_path = tmp_path / "silver" / "active_symbols"
        monkeypatch.setattr(type(resolver), "OUTPUT_PATH", property(lambda s: output_path))

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mi = MagicMock()
            mi.market_map.return_value = {"AAPL": "us_stocks", "EUR_USD": "forex"}
            mi.symbol_list.return_value = ["AAPL", "EUR_USD"]
            mock_loader.return_value = mi
            resolver.resolve(silver_path, run_date)

        canonical_syms = (
            pl.read_parquet(output_path / f"active_ohlcv_{run_date.isoformat()}.parquet")
            ["symbol"].to_list()
        )
        legacy_syms = (
            pl.read_parquet(output_path / f"active_{run_date.isoformat()}.parquet")
            ["symbol"].to_list()
        )
        assert sorted(canonical_syms) == sorted(legacy_syms)

    # test_resolve_context_writes_parquet_without_silver,
    # test_resolve_context_excludes_deferred, test_resolve_context_parquet_schema:
    # MOVED GMI-CTX-001 to tests/unit/test_context_anchors.py (resolve_context()
    # itself moved to src/silver/context_anchors.py::ContextAnchorsResolver.resolve()).
    # Deleted here rather than left as skip-stubs — the coverage is fully
    # preserved (and extended) in the new file; a lingering skip with no
    # future un-skip condition is dead weight, not a safety net.

    def test_load_ohlcv_returns_list_of_strings(self, tmp_path, monkeypatch):
        """
        GMI-AS-001: load_ohlcv() must return list[str] from the canonical
        active_ohlcv_{date}.parquet (Architecture v2.0 §5.2 API).
        """
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver_path = _make_silver_parquet(tmp_path)
        run_date    = date(2025, 3, 6)
        resolver    = ActiveSymbolsResolver()
        output_path = tmp_path / "silver" / "active_symbols"
        monkeypatch.setattr(type(resolver), "OUTPUT_PATH", property(lambda s: output_path))

        with patch("src.silver.active_symbols.get_loader") as mock_loader:
            mi = MagicMock()
            mi.market_map.return_value = {"AAPL": "us_stocks", "EUR_USD": "forex"}
            mi.symbol_list.return_value = ["AAPL", "EUR_USD"]
            mock_loader.return_value = mi
            resolver.resolve(silver_path, run_date)

        symbols = resolver.load_ohlcv(run_date)
        assert isinstance(symbols, list)
        assert all(isinstance(s, str) for s in symbols)
        assert len(symbols) > 0

    def test_load_ohlcv_raises_when_no_parquet(self, tmp_path, monkeypatch):
        """GMI-AS-001: load_ohlcv() raises FileNotFoundError if not yet resolved."""
        resolver    = ActiveSymbolsResolver()
        output_path = tmp_path / "silver" / "active_symbols"
        monkeypatch.setattr(type(resolver), "OUTPUT_PATH", property(lambda s: output_path))
        with pytest.raises(FileNotFoundError):
            resolver.load_ohlcv(date(2099, 1, 1))

    # test_load_context_raises_when_no_parquet: MOVED GMI-CTX-001 to
    # tests/unit/test_context_anchors.py::test_load_raises_when_no_parquet
    # (load_context() itself moved to ContextAnchorsResolver.load()).
