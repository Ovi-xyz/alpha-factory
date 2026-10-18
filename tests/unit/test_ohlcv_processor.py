"""tests/unit/test_ohlcv_processor.py — Silver OHLCVProcessor test suite

v1.5: Tambah TestSynthesize4H untuk synthesize_4h() dan _add_4h_derived_fields().
"""

from datetime import date, datetime, timedelta

import polars as pl
import pytest

from src.silver.ohlcv_processor import OHLCVProcessor, CURRENT_SILVER_VERSION


@pytest.fixture
def proc():
    return OHLCVProcessor()


@pytest.fixture
def raw_ohlcv():
    """Minimal raw Bronze-style OHLCV DataFrame."""
    base = date(2025, 1, 2)
    return pl.DataFrame({
        "timestamp": [base + timedelta(days=i) for i in range(30)],
        "open":      [100.0 + i * 0.5 for i in range(30)],
        "high":      [105.0 + i * 0.5 for i in range(30)],
        "low":       [98.0  + i * 0.5 for i in range(30)],
        "close":     [102.0 + i * 0.5 for i in range(30)],
        "volume":    [1_000_000 + i * 10_000 for i in range(30)],
    })


class TestOHLCVProcessor:

    def test_derived_fields_present(self, proc, raw_ohlcv):
        result = proc.process_symbol(
            raw_ohlcv, "AAPL", "us_stocks", "1D"
        )
        assert "log_return"    in result.columns
        assert "dollar_volume" in result.columns
        assert "spread_hl"     in result.columns
        assert "vwap"          in result.columns

    def test_vwap_uses_typical_price(self, proc, raw_ohlcv):
        """CRITICAL FIX v1.2: VWAP = (H+L+C)/3 weighted by volume."""
        result = proc.process_symbol(
            raw_ohlcv, "AAPL", "us_stocks", "1D"
        )
        assert "vwap" in result.columns
        # VWAP should be between low and high (typical price is in this range)
        valid = result.filter(pl.col("vwap").is_not_null())
        if len(valid) > 0:
            assert (valid["vwap"] >= valid["low"]).all()
            assert (valid["vwap"] <= valid["high"]).all()

    def test_vwap_not_equal_to_close(self, proc, raw_ohlcv):
        """v1.2 fix: VWAP should NOT equal close (old incorrect formula)."""
        result = proc.process_symbol(
            raw_ohlcv, "AAPL", "us_stocks", "1D"
        )
        valid = result.filter(
            pl.col("vwap").is_not_null() & pl.col("close").is_not_null()
        )
        if len(valid) > 1:
            # At least some VWAP values should differ from close
            diff = (valid["vwap"] - valid["close"]).abs().sum()
            assert diff > 0, "VWAP equals close — old formula bug not fixed"

    def test_adjustment_flags(self, proc, raw_ohlcv):
        """NEW v1.2: is_adjusted + adj_factor columns."""
        result = proc.process_symbol(
            raw_ohlcv, "AAPL", "us_stocks", "1D",
            is_adjusted=True, adj_factor=1.0
        )
        assert "is_adjusted" in result.columns
        assert "adj_factor"  in result.columns
        assert result["is_adjusted"].to_list()[0] is True
        assert result["adj_factor"].to_list()[0]  == 1.0

    def test_is_clean_flag_present(self, proc, raw_ohlcv):
        result = proc.process_symbol(
            raw_ohlcv, "AAPL", "us_stocks", "1D"
        )
        assert "is_clean" in result.columns

    def test_price_sanity_flags_bad_rows(self, proc):
        """Rows where high < low should be is_clean=False."""
        bad_df = pl.DataFrame({
            "timestamp": [date(2025, 1, 2)],
            "open":      [100.0],
            "high":      [95.0],    # high < low → bad
            "low":       [98.0],
            "close":     [97.0],
            "volume":    [1_000_000],
        })
        result = proc.process_symbol(bad_df, "TEST", "us_stocks", "1D")
        if "is_clean" in result.columns and len(result) > 0:
            assert result["is_clean"].to_list()[0] is False

    def test_metadata_columns(self, proc, raw_ohlcv):
        """Silver metadata columns must be present."""
        result = proc.process_symbol(
            raw_ohlcv, "AAPL", "us_stocks", "1D"
        )
        assert "symbol"             in result.columns
        assert "timeframe"          in result.columns
        assert "processing_version" in result.columns
        assert result["processing_version"].to_list()[0] == CURRENT_SILVER_VERSION

    def test_deduplication(self, proc):
        """Duplicate timestamps should be deduplicated."""
        dup_df = pl.DataFrame({
            "timestamp": [date(2025, 1, 2), date(2025, 1, 2), date(2025, 1, 3)],
            "open":      [100.0, 100.5, 101.0],
            "high":      [105.0, 105.5, 106.0],
            "low":       [98.0,  98.5,  99.0],
            "close":     [102.0, 102.5, 103.0],
            "volume":    [1_000_000, 1_100_000, 1_200_000],
        })
        result = proc.process_symbol(dup_df, "AAPL", "us_stocks", "1D")
        assert len(result) == 2   # Deduplicated to 2 unique dates

    def test_empty_input_returns_empty(self, proc):
        result = proc.process_symbol(
            pl.DataFrame(), "AAPL", "us_stocks", "1D"
        )
        assert len(result) == 0


# ── Synthesize 4H (v1.5 refactoring) ─────────────────────────────────────────

def _make_silver_1h(n_days: int = 5, symbol: str = "AAPL") -> pl.DataFrame:
    """
    Silver 1H fixture: n_days × 8 bars (09:00-16:00 US session).
    Mencerminkan output dari OHLCVProcessor.process_symbol(..., '1H').
    """
    rows  = []
    base  = datetime(2025, 1, 6, 13, 0)   # 09:00 ET = 13:00 UTC
    price = 150.0

    for d in range(n_days):
        for h in range(8):
            ts = base + timedelta(days=d, hours=h)
            price += 0.1
            rows.append({
                "symbol":             symbol,
                "timestamp":          ts,
                "timeframe":          "1H",
                "open":               round(price - 0.05, 4),
                "high":               round(price + 0.20, 4),
                "low":                round(price - 0.15, 4),
                "close":              round(price, 4),
                "volume":             100_000 + h * 10_000,
                "is_adjusted":        True,
                "adj_factor":         1.0,
                "vwap":               round(price + 0.02, 4),
                "log_return":         0.001,
                "dollar_volume":      float((100_000 + h * 10_000) * price),
                "spread_hl":          0.0023,
                "is_clean":           True,
                "data_source":        "yfinance",
                "processing_version": "1.2",
            })
    return pl.DataFrame(rows)


class TestNormalizeTimestampsUtf8Fallback:
    """FIX AV-CNH-01 defense-in-depth [chat thread, 8/9 Sep 2026]. The
    actual root cause (AlphaVantageForexAdapter writing a raw date string
    instead of a date object) is fixed at the source — these tests cover
    the separate, deliberately-added safety net in _normalize_timestamps()
    itself, for whatever future adapter makes the identical mistake.
    Before this fix, a Utf8 timestamp column matched neither the pl.Date
    nor pl.Datetime branch and passed through completely untouched, only
    to fail later and opaquely inside _add_derived_fields()'s VWAP calc."""

    def test_utf8_date_strings_are_parsed_and_utc_localized(self):
        df = pl.DataFrame({"timestamp": ["2025-01-15", "2025-01-16"]})
        out = OHLCVProcessor._normalize_timestamps(df, market="forex")
        assert out["timestamp"].dtype.time_zone == "UTC"
        assert out["timestamp"].dt.date().to_list() == [date(2025, 1, 15), date(2025, 1, 16)]

    def test_utf8_datetime_strings_are_parsed(self):
        df = pl.DataFrame({"timestamp": ["2025-01-15 09:30:00"]})
        out = OHLCVProcessor._normalize_timestamps(df, market="us_stocks")
        assert out["timestamp"].dtype.time_zone == "UTC"

    def test_downstream_vwap_no_longer_fails_on_string_timestamp(self):
        """The actual symptom this whole fix chain addresses: VWAP must
        compute normally once the string has been parsed, not silently
        fall back to all-null."""
        df = pl.DataFrame({
            "timestamp": ["2025-01-15", "2025-01-16"],
            "open":  [1.10, 1.11], "high": [1.12, 1.13],
            "low":   [1.09, 1.10], "close": [1.11, 1.12],
            "volume": [1000, 1200],
        })
        df = OHLCVProcessor._normalize_timestamps(df, market="forex")
        df = OHLCVProcessor._add_derived_fields(df, timeframe="1D")
        assert "vwap" in df.columns
        assert df["vwap"].null_count() == 0

    def test_mixed_valid_and_invalid_strings_nulls_only_the_bad_rows(self):
        """When most of the column parses, format inference succeeds and
        the specific unparseable row nulls out individually — same
        graceful-degradation contract as the Date/Datetime branches."""
        df = pl.DataFrame({"timestamp": ["2025-01-15", "garbage", "2025-01-17"]})
        out = OHLCVProcessor._normalize_timestamps(df, market="forex")
        result = out["timestamp"].to_list()
        assert result[1] is None
        assert result[0] is not None and result[2] is not None

    def test_wholly_unparseable_column_left_untouched_not_crashed(self):
        """When NO row in the column matches any recognizable format,
        Polars' format inference itself fails (raises internally) rather
        than nulling row-by-row — caught by the outer try/except in
        _normalize_timestamps(), which logs a debug note and returns the
        DataFrame unchanged. Verified empirically: this is a materially
        different failure mode from the mixed-validity case above, not a
        bug in either branch — the point is the SAME as the Date/Datetime
        branches' contract: never raise out of this method and take down
        a whole ingestion run over one column's data quality."""
        df = pl.DataFrame({"timestamp": ["not-a-date"]})
        out = OHLCVProcessor._normalize_timestamps(df, market="forex")
        assert out["timestamp"].to_list() == ["not-a-date"]


class TestFlagIsCleanOutlierIsolation:
    """
    FIX OP-LR-01 [chat thread, 2 Sep 2026]: a single sign-crossing close
    (e.g. CL/WTI negative on 2020-04-20/21 — a real historical event, not
    bad data) makes ln(close/prev_close) NaN/Inf. Pre-fix, that one row's
    NaN propagated into mean_/std_ computed from the FULL log_return
    column, silently disabling outlier detection for the symbol's ENTIRE
    history (`std_ and std_ > 0` evaluates False for NaN, no exception, no
    log line). These tests build a small, hand-crafted frame — not a full
    process_symbol() pipeline — directly exercising _flag_is_clean()'s
    z-score branch in isolation.
    """

    @staticmethod
    def _frame(log_returns: list[float], n: int) -> pl.DataFrame:
        base = date(2025, 1, 2)
        return pl.DataFrame({
            "timestamp":  [base + timedelta(days=i) for i in range(n)],
            "open":       [100.0] * n, "high": [101.0] * n, "low": [99.0] * n,
            "close":      [100.0] * n, "volume": [1_000_000] * n,
            "log_return": log_returns,
        })

    def test_infinite_log_return_no_longer_poisons_symbol_baseline(self, proc):
        # 50 quiet rows (~0 return), one genuine finite outlier (idx 10),
        # one non-finite sign-crossing row (idx 25, the CL/WTI scenario).
        lr = [0.001 if i % 2 == 0 else -0.001 for i in range(50)]
        lr[10] = 0.5          # genuine extreme, finite — should be caught
        lr[25] = float("inf")  # sign-crossing artifact — should be isolated
        df = self._frame(lr, 50)

        result = proc._flag_is_clean(df)

        assert result["is_clean"][10] == False, (
            "Pre-fix this stayed True: NaN baseline made the whole "
            "`std_ and std_ > 0` guard silently skip outlier detection"
        )
        assert result["is_clean"][25] == False, (
            "The non-finite row itself must still end up flagged — "
            "isolating it must not accidentally exempt it"
        )
        # Every other quiet row must remain clean — proves the baseline
        # wasn't dragged down by only 2/50 rows being flagged.
        untouched = [i for i in range(50) if i not in (10, 25)]
        assert all(result["is_clean"][i] for i in untouched)

    def test_nan_log_return_same_isolation(self, proc):
        """Same guarantee for NaN (e.g. 0/0 division), not just Infinity."""
        lr = [0.001 if i % 2 == 0 else -0.001 for i in range(50)]
        lr[30] = float("nan")
        df = self._frame(lr, 50)

        result = proc._flag_is_clean(df)

        assert result["is_clean"][30] == False
        untouched = [i for i in range(50) if i != 30]
        assert all(result["is_clean"][i] for i in untouched)

    def test_no_nonfinite_values_unaffected(self, proc):
        """Regression guard: normal data with no NaN/Inf must behave
        identically to before this fix."""
        lr = [0.001 if i % 2 == 0 else -0.001 for i in range(50)]
        lr[10] = 0.5  # genuine outlier, finite
        df = self._frame(lr, 50)

        result = proc._flag_is_clean(df)

        assert result["is_clean"][10] == False
        untouched = [i for i in range(50) if i != 10]
        assert all(result["is_clean"][i] for i in untouched)


class TestSynthesize4H:
    """
    Tests untuk OHLCVProcessor.synthesize_4h() — v1.5 refactoring.
    4H disintesis dari Silver 1H (GD §4.1 Enrichment, §17.7 Anti-Patterns).
    """

    @pytest.fixture
    def proc(self):
        return OHLCVProcessor()

    @pytest.fixture
    def silver_1h(self):
        return _make_silver_1h(n_days=5)

    # ── Basic output ──────────────────────────────────────────────────────────

    def test_returns_dataframe(self, proc, silver_1h):
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0

    def test_fewer_bars_than_1h(self, proc, silver_1h):
        """4H output harus lebih sedikit bar dari 1H input."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert len(result) < len(silver_1h)

    def test_empty_input_returns_empty(self, proc):
        result = proc.synthesize_4h(pl.DataFrame(), "AAPL", "us_stocks")
        assert len(result) == 0

    def test_none_input_returns_empty(self, proc):
        result = proc.synthesize_4h(None, "AAPL", "us_stocks")
        assert len(result) == 0

    # ── Full Silver schema ────────────────────────────────────────────────────

    SILVER_4H_REQUIRED_COLS = [
        "symbol", "timeframe", "open", "high", "low", "close", "volume",
        "vwap", "log_return", "dollar_volume", "spread_hl",
        "is_adjusted", "adj_factor", "is_clean",
        "data_source", "processing_version",
    ]

    def test_full_silver_schema_present(self, proc, silver_1h):
        """Silver 4H harus memiliki semua kolom Silver schema GD §4.3."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        for col in self.SILVER_4H_REQUIRED_COLS:
            assert col in result.columns, f"Missing Silver column: {col}"

    def test_timeframe_is_4h(self, proc, silver_1h):
        """timeframe kolom harus '4H'."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert result["timeframe"].unique().to_list() == ["4H"]

    def test_symbol_correct(self, proc, silver_1h):
        """symbol kolom harus match input symbol."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert result["symbol"].unique().to_list() == ["AAPL"]

    def test_data_source_is_aggregated(self, proc, silver_1h):
        """data_source harus 'yfinance_aggregated' (derived product)."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert result["data_source"].unique().to_list() == ["yfinance_aggregated"]

    def test_processing_version_matches(self, proc, silver_1h):
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert result["processing_version"].unique().to_list() == [CURRENT_SILVER_VERSION]

    # ── OHLCV correctness ─────────────────────────────────────────────────────

    def test_ohlc_sanity(self, proc, silver_1h):
        """high >= low, open/close in [low, high] — price sanity."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert (result["high"] >= result["low"]).all()
        assert (result["open"] >= result["low"]).all()
        assert (result["open"] <= result["high"]).all()
        assert (result["close"] >= result["low"]).all()
        assert (result["close"] <= result["high"]).all()

    def test_volume_sum_preserved(self, proc, silver_1h):
        """Total volume 4H harus = total volume 1H (sum preserved)."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert result["volume"].sum() == silver_1h["volume"].sum()

    def test_vwap_present_and_in_range(self, proc, silver_1h):
        """VWAP (dari aggregator, per-block) harus antara low dan high."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert "vwap" in result.columns
        valid = result.filter(pl.col("vwap").is_not_null())
        if len(valid) > 0:
            assert (valid["vwap"] >= valid["low"]).all()
            assert (valid["vwap"] <= valid["high"]).all()

    def test_dollar_volume_computed(self, proc, silver_1h):
        """dollar_volume = close * volume (G2 FIX)."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert "dollar_volume" in result.columns
        valid = result.filter(
            pl.col("dollar_volume").is_not_null()
            & pl.col("close").is_not_null()
            & pl.col("volume").is_not_null()
        )
        if len(valid) > 0:
            expected = valid["close"] * valid["volume"].cast(pl.Float64)
            diff = (expected - valid["dollar_volume"]).abs().max()
            assert diff < 1.0, f"dollar_volume mismatch: max diff={diff}"

    def test_spread_hl_computed(self, proc, silver_1h):
        """spread_hl = (high - low) / close."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert "spread_hl" in result.columns
        valid = result.filter(pl.col("spread_hl").is_not_null())
        if len(valid) > 0:
            # spread_hl harus >= 0 (high selalu >= low)
            assert (valid["spread_hl"] >= 0).all()

    def test_log_return_present(self, proc, silver_1h):
        """log_return harus ada (first bar boleh null)."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert "log_return" in result.columns
        # Semua kecuali bar pertama harus non-null
        non_null = result["log_return"].drop_nulls()
        assert len(non_null) >= len(result) - 1

    # ── Adjustment flags ──────────────────────────────────────────────────────

    def test_is_adjusted_propagated(self, proc, silver_1h):
        """is_adjusted diwariskan dari parameter (default True)."""
        result = proc.synthesize_4h(
            silver_1h, "AAPL", "us_stocks", is_adjusted=True, adj_factor=1.0
        )
        assert result["is_adjusted"].to_list()[0] is True

    def test_adj_factor_propagated(self, proc, silver_1h):
        """adj_factor diwariskan dari parameter."""
        result = proc.synthesize_4h(
            silver_1h, "AAPL", "us_stocks", is_adjusted=True, adj_factor=0.95
        )
        assert abs(result["adj_factor"].to_list()[0] - 0.95) < 1e-9

    # ── is_clean flag ─────────────────────────────────────────────────────────

    def test_is_clean_present(self, proc, silver_1h):
        """is_clean harus ada di output."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert "is_clean" in result.columns

    def test_clean_input_mostly_clean(self, proc, silver_1h):
        """Silver 1H yang bersih harus menghasilkan 4H yang mostly clean."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        clean_pct = result["is_clean"].mean()
        assert clean_pct >= 0.8, (
            f"Expected >= 80% clean 4H bars from clean 1H input, got {clean_pct:.1%}"
        )

    # ── Working columns not in Silver schema ──────────────────────────────────

    def test_bar_count_not_in_output(self, proc, silver_1h):
        """bar_count (working col dari aggregator) TIDAK boleh ada di Silver output."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert "bar_count" not in result.columns

    def test_is_incomplete_bar_not_in_output(self, proc, silver_1h):
        """is_incomplete_bar (working col) TIDAK boleh ada di Silver output GD §4.3."""
        result = proc.synthesize_4h(silver_1h, "AAPL", "us_stocks")
        assert "is_incomplete_bar" not in result.columns

    # ── Missing required columns ──────────────────────────────────────────────

    def test_missing_required_columns_returns_empty(self, proc):
        """Jika Silver 1H tidak punya kolom wajib, synthesize_4h return empty."""
        incomplete_df = pl.DataFrame({
            "timestamp": [datetime(2025, 1, 6, 9)],
            "open": [150.0],
            # missing: high, low, close, volume
        })
        result = proc.synthesize_4h(incomplete_df, "AAPL", "us_stocks")
        assert len(result) == 0

    # ── IDX market ────────────────────────────────────────────────────────────

    def test_idx_market(self, proc):
        """IDX market juga bisa disintesis — UTC timestamps dari Silver 1H."""
        silver_1h = _make_silver_1h(n_days=3, symbol="BBCA")
        result = proc.synthesize_4h(silver_1h, "BBCA", "idx")
        assert len(result) > 0
        assert result["timeframe"].unique().to_list() == ["4H"]


class TestRunEntryPoint:
    """
    FIX GAP-6 [P1] (Production Readiness Assessment v1.7.2, GD §14.3.2):
    coverage for the new module-level run(run_date) — the job_registry.py
    entry point that previously existed only as an inline copy in
    job_registry.py::_silver_ohlcv(). Exercises both passes end-to-end
    against real Parquet fixtures on disk (tmp_path), not mocks, so the
    wildcard Bronze glob (FIX MI-1) and the 1H->4H handoff are both
    actually exercised.
    """

    @staticmethod
    def _fake_instrument(symbol="AAPL", market="us_stocks"):
        from src.config.instrument_loader import Instrument
        return Instrument(
            symbol=symbol, raw_symbol=symbol, market=market, sector="Technology",
            yfinance_symbol=symbol, polygon_symbol=symbol, tvfeed_symbol=None,
            eia_series=None, timezone="America/New_York",
        )

    @staticmethod
    def _fake_loader(instruments):
        class _FakeLoader:
            def all_symbols(self_inner):
                return instruments
            def symbol_list(self_inner, market=None):
                return [i.symbol for i in instruments if market is None or i.market == market]
        return _FakeLoader()

    def _write_bronze_fixture(self, tmp_path, symbol, market, timeframe, source="yfinance"):
        from src.silver.ohlcv_processor import BRONZE_OHLCV_PATH
        base = date(2024, 1, 1)
        df = pl.DataFrame({
            "timestamp": [base + timedelta(days=i) for i in range(40)],
            "open":      [100.0 + i * 0.5 for i in range(40)],
            "high":      [105.0 + i * 0.5 for i in range(40)],
            "low":       [98.0  + i * 0.5 for i in range(40)],
            "close":     [102.0 + i * 0.5 for i in range(40)],
            "volume":    [1_000_000 + i * 10_000 for i in range(40)],
        })
        # FIX ADR-045 companion (GMI_Decision_Document_v11.docx §2): Bronze
        # is now timeframe-partitioned (market/timeframe={tf}/source={src}/
        # symbol={sym}/...) — fixtures must be written per-timeframe to
        # exercise the real production path, not a single shared blob.
        out_dir = (
            tmp_path / BRONZE_OHLCV_PATH / market / f"timeframe={timeframe}"
            / f"source={source}" / f"symbol={symbol}" / "year=2024" / "month=01"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out_dir / f"{symbol}_raw_fixture.parquet")

    def test_run_produces_silver_for_all_bronze_timeframes(self, tmp_path, monkeypatch):
        """PASS 1: every Bronze raw TF must produce a Silver file when Bronze data exists.

        FIX ADR-045 companion: each TF now needs its OWN timeframe-scoped
        Bronze fixture — this is the corrected version of what used to be a
        single shared fixture (see git history), which encoded the exact
        pre-fix bug this test now guards against: a single Bronze blob no
        longer silently satisfies all 6 declared timeframes at once.
        """
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = self._fake_instrument("AAPL", "us_stocks")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        for tf in ohlcv_mod._RUN_BRONZE_TFS:
            self._write_bronze_fixture(tmp_path, "AAPL", "us_stocks", tf)

        ohlcv_mod.run(date(2025, 6, 1))

        silver_dir = tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "us_stocks" / "symbol=AAPL"
        for tf in ohlcv_mod._RUN_BRONZE_TFS:
            out_path = silver_dir / f"AAPL_{tf}_silver.parquet"
            assert out_path.exists(), f"Missing Silver output for TF={tf}"

    def test_run_does_not_blend_timeframes_across_bronze_partitions(self, tmp_path, monkeypatch):
        """Regression guard for the timeframe-blind glob bug discovered
        during ADR-045 implementation: empirically confirmed against a
        real production Bronze file that, pre-fix, this glob matched
        identically for every declared _RUN_BRONZE_TFS entry regardless of
        which timeframe was actually being processed — every Silver TF
        file for a symbol would silently contain the SAME underlying rows.
        Two Bronze fixtures with DIFFERENT row counts (hence different date
        ranges) at 1D vs 1W must produce two Silver files with DIFFERENT
        row counts — proving PASS 1 does not union them.
        """
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = self._fake_instrument("AAPL", "us_stocks")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        self._write_bronze_fixture(tmp_path, "AAPL", "us_stocks", "1D")
        # A deliberately SMALLER, distinct fixture for 1W — if the glob were
        # still timeframe-blind, PASS 1 would read the UNION of both (or
        # whichever glob resolves first) for both TFs, and the two Silver
        # outputs would end up with identical row counts.
        from src.silver.ohlcv_processor import BRONZE_OHLCV_PATH
        base = date(2024, 6, 1)
        small_df = pl.DataFrame({
            "timestamp": [base + timedelta(days=i) for i in range(5)],
            "open":      [200.0] * 5, "high": [201.0] * 5,
            "low":       [199.0] * 5, "close": [200.5] * 5,
            "volume":    [500_000] * 5,
        })
        out_dir = (
            tmp_path / BRONZE_OHLCV_PATH / "us_stocks" / "timeframe=1W"
            / "source=yfinance" / "symbol=AAPL" / "year=2024" / "month=06"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        small_df.write_parquet(out_dir / "AAPL_raw_fixture.parquet")

        ohlcv_mod.run(date(2025, 6, 1))

        silver_dir = tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "us_stocks" / "symbol=AAPL"
        rows_1d = len(pl.read_parquet(silver_dir / "AAPL_1D_silver.parquet"))
        rows_1w = len(pl.read_parquet(silver_dir / "AAPL_1W_silver.parquet"))
        assert rows_1d == 40
        assert rows_1w == 5
        assert rows_1d != rows_1w

    def test_run_pass2_synthesizes_4h_from_silver_1h(self, tmp_path, monkeypatch):
        """PASS 2: Silver 1H written by PASS 1 must drive a Silver 4H synthesis."""
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = self._fake_instrument("AAPL", "us_stocks")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        self._write_bronze_fixture(tmp_path, "AAPL", "us_stocks", "1H")

        ohlcv_mod.run(date(2025, 6, 1))

        silver_4h = (
            tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "us_stocks"
            / "symbol=AAPL" / "AAPL_4H_silver.parquet"
        )
        assert silver_4h.exists()
        out = pl.read_parquet(silver_4h)
        assert out["timeframe"].unique().to_list() == ["4H"]

    def test_run_no_bronze_data_marks_done_without_error(self, tmp_path, monkeypatch):
        """Symbol with zero Bronze data must not raise — checkpoint marks it done."""
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = self._fake_instrument("MSFT", "us_stocks")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        # No Bronze fixture written for MSFT at all.
        ohlcv_mod.run(date(2025, 6, 1))   # must not raise

        silver_dir = tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "us_stocks" / "symbol=MSFT"
        assert not silver_dir.exists() or len(list(silver_dir.glob("*.parquet"))) == 0

    def test_run_resumable_via_checkpoint(self, tmp_path, monkeypatch):
        """A second run() call for the same run_date must skip already-done symbols."""
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = self._fake_instrument("AAPL", "us_stocks")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        self._write_bronze_fixture(tmp_path, "AAPL", "us_stocks", "1D")

        run_date = date(2025, 6, 1)
        ohlcv_mod.run(run_date)   # first run: processes everything

        silver_1d = (
            tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "us_stocks"
            / "symbol=AAPL" / "AAPL_1D_silver.parquet"
        )
        mtime_after_first = silver_1d.stat().st_mtime_ns

        ohlcv_mod.run(run_date)   # second run, same run_date: should skip (checkpoint)
        assert silver_1d.stat().st_mtime_ns == mtime_after_first, (
            "File was rewritten on the second run — ProgressCheckpoint not honored"
        )


class TestRunContextEntryPoint:
    """
    ADD GMI-SIL-001: coverage for run_context(run_date) — the Silver-layer
    counterpart to Bronze's run_context() (GMI-BRZ-001). Mirrors
    TestRunEntryPoint's exact tmp_path/monkeypatch pattern above, adapted
    for Layer 2: all_context() instead of all_symbols(), market='context',
    single-pass (no 4H synthesis — see _RUN_CONTEXT_TFS comment in
    ohlcv_processor.py for why no Layer 2 consumer needs 4H in this cycle).
    """

    @staticmethod
    def _fake_context_instrument(symbol="VIX", timezone="America/New_York"):
        from src.config.instrument_loader import Instrument
        return Instrument(
            symbol=symbol, raw_symbol=symbol, market="context", sector=None,
            yfinance_symbol=f"^{symbol}", polygon_symbol="", tvfeed_symbol=None,
            eia_series=None, timezone=timezone, is_active=True,
            layer=2, context_category="context_volatility", context_group="equity",
            context_available=True, include_in_forecast=True,
        )

    @staticmethod
    def _fake_loader(instruments):
        class _FakeLoader:
            def all_context(self_inner, include_deferred=False):
                return instruments
        return _FakeLoader()

    def _write_bronze_context_fixture(self, tmp_path, symbol, timeframe, source="yfinance"):
        from src.silver.ohlcv_processor import BRONZE_OHLCV_PATH
        base = date(2024, 1, 1)
        df = pl.DataFrame({
            "timestamp": [base + timedelta(days=i) for i in range(40)],
            "open":      [100.0 + i * 0.5 for i in range(40)],
            "high":      [105.0 + i * 0.5 for i in range(40)],
            "low":       [98.0  + i * 0.5 for i in range(40)],
            "close":     [102.0 + i * 0.5 for i in range(40)],
            "volume":    [1_000_000 + i * 10_000 for i in range(40)],
        })
        out_dir = (
            tmp_path / BRONZE_OHLCV_PATH / "context" / f"timeframe={timeframe}"
            / f"source={source}" / f"symbol={symbol}" / "year=2024" / "month=01"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out_dir / f"{symbol}_raw_fixture.parquet")

    def test_run_context_produces_silver_for_context_timeframes(self, tmp_path, monkeypatch):
        """Every _RUN_CONTEXT_TFS entry must produce a Silver file when Bronze context data exists."""
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = self._fake_context_instrument("VIX")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        for tf in ohlcv_mod._RUN_CONTEXT_TFS:
            self._write_bronze_context_fixture(tmp_path, "VIX", tf)

        ohlcv_mod.run_context(date(2025, 6, 1))

        silver_dir = tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "context" / "symbol=VIX"
        for tf in ohlcv_mod._RUN_CONTEXT_TFS:
            out_path = silver_dir / f"VIX_{tf}_silver.parquet"
            assert out_path.exists(), f"Missing Silver context output for TF={tf}"

    def test_run_context_uses_per_instrument_timezone(self, tmp_path, monkeypatch):
        """tz_hint=inst.timezone must be honored, not a market-keyed fallback
        dict — 'context' is not a key in _normalize_timestamps' internal
        market->timezone dict, so passing tz_hint correctly is load-bearing."""
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = self._fake_context_instrument("N225", timezone="Asia/Tokyo")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        self._write_bronze_context_fixture(tmp_path, "N225", "1D")

        ohlcv_mod.run_context(date(2025, 6, 1))

        out = pl.read_parquet(
            tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "context"
            / "symbol=N225" / "N225_1D_silver.parquet"
        )
        # UTC-normalized timestamp column must exist and be tz-aware —
        # confirms _normalize_timestamps ran using tz_hint, not silently no-op.
        assert out["timestamp"].dtype.time_zone == "UTC"

    def test_run_context_no_4h_synthesis(self, tmp_path, monkeypatch):
        """Layer 2 is single-pass — no 4H file should ever be produced."""
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = self._fake_context_instrument("VIX")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        self._write_bronze_context_fixture(tmp_path, "VIX", "1D")

        ohlcv_mod.run_context(date(2025, 6, 1))

        silver_dir = tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "context" / "symbol=VIX"
        assert not (silver_dir / "VIX_4H_silver.parquet").exists()

    def test_run_context_no_bronze_data_marks_done_without_error(self, tmp_path, monkeypatch):
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = self._fake_context_instrument("DXY")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        ohlcv_mod.run_context(date(2025, 6, 1))   # no Bronze fixture — must not raise

        silver_dir = tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "context" / "symbol=DXY"
        assert not silver_dir.exists() or len(list(silver_dir.glob("*.parquet"))) == 0

    def test_run_context_resumable_via_checkpoint(self, tmp_path, monkeypatch):
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = self._fake_context_instrument("VIX")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        self._write_bronze_context_fixture(tmp_path, "VIX", "1D")

        run_date = date(2025, 6, 1)
        ohlcv_mod.run_context(run_date)
        out_path = (
            tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "context"
            / "symbol=VIX" / "VIX_1D_silver.parquet"
        )
        mtime_first = out_path.stat().st_mtime_ns

        ohlcv_mod.run_context(run_date)   # second call — must skip, not rewrite
        assert out_path.stat().st_mtime_ns == mtime_first

    def test_run_context_checkpoint_namespace_isolated_from_layer1(self, tmp_path, monkeypatch):
        """silver_ohlcv_context's checkpoint must not collide with, or be
        satisfiable by, Layer 1's silver_ohlcv_p1 namespace."""
        import src.silver.ohlcv_processor as ohlcv_mod
        from src.utils.progress_checkpoint import ProgressCheckpoint

        monkeypatch.chdir(tmp_path)
        inst = self._fake_context_instrument("VIX")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: self._fake_loader([inst]),
        )
        self._write_bronze_context_fixture(tmp_path, "VIX", "1D")

        ohlcv_mod.run_context(date(2025, 6, 1))

        ctx_ckpt = ProgressCheckpoint("silver_ohlcv_context", date(2025, 6, 1))
        assert ctx_ckpt.is_done("VIX", timeframe="1D")

        layer1_ckpt = ProgressCheckpoint("silver_ohlcv_p1", date(2025, 6, 1))
        assert not layer1_ckpt.is_done("VIX", timeframe="1D")


class TestBronzeSchemaDrift:
    """
    FIX SIL-BRZ-SCHEMA-01 [chat thread, 17 Oct 2026]: regression guard for the
    CNH silent drop. Bronze is append-only, so a symbol whose adapter changed
    its `timestamp` dtype (String until 8 Sep 2026, Date from 9 Sep — FIX
    AV-CNH-01) carries both on disk. `pl.scan_parquet(glob)` over the mix raises
    SchemaError and the former bare `except Exception` in run()/run_context()
    recorded that as "no Bronze data" -> no Silver file, no error.

    Fixtures mirror the live CNH shape measured on 17 Oct 2026: String
    timestamp + Null volume (AlphaVantage FX has no volume) in the old files,
    Date timestamp + Null volume in the new ones.
    """

    @staticmethod
    def _write(tmp_path, market, tf, symbol, fname, ts, close_dtype=pl.Float64,
               source="alphavantage", volume_null=True):
        from src.silver.ohlcv_processor import BRONZE_OHLCV_PATH
        n = len(ts)
        df = pl.DataFrame(
            {
                "timestamp": ts,
                "open":   [6.70 + i * 0.001 for i in range(n)],
                "high":   [6.72 + i * 0.001 for i in range(n)],
                "low":    [6.68 + i * 0.001 for i in range(n)],
                "close":  pl.Series([6.71 + i * 0.001 for i in range(n)]).cast(close_dtype),
                "volume": pl.Series([None] * n, dtype=pl.Null) if volume_null
                          else pl.Series([1000] * n, dtype=pl.Int64),
                "_source": [source] * n,
            }
        )
        out_dir = (
            tmp_path / BRONZE_OHLCV_PATH / market / f"timeframe={tf}"
            / f"source={source}" / f"symbol={symbol}" / "year=2026" / "month=09"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out_dir / fname)
        return out_dir / fname

    @staticmethod
    def _pattern(tmp_path, market, tf, symbol):
        from src.silver.ohlcv_processor import BRONZE_OHLCV_PATH
        return str(
            tmp_path / BRONZE_OHLCV_PATH / market / f"timeframe={tf}"
            / "**" / f"symbol={symbol}" / "**" / "*.parquet"
        )

    def _write_cnh_mixed(self, tmp_path, tf="1D"):
        """8 'old' String-era rows then 'new' Date-era rows, one file each."""
        old = [f"2026-08-{d:02d}" for d in range(20, 28)]
        new = [date(2026, 8, 28) + timedelta(days=i) for i in range(10)]
        self._write(tmp_path, "context", tf, "CNH", "CNH_raw_20260908.parquet", old)
        self._write(tmp_path, "context", tf, "CNH", "CNH_raw_20260909.parquet", new)
        return len(old) + len(new)

    # ── _read_bronze_symbol ────────────────────────────────────────────────────────────

    def test_mixed_string_and_date_reproduces_original_failure(self, tmp_path):
        """Pins the root cause: the OLD read raises SchemaError on this fixture."""
        self._write_cnh_mixed(tmp_path)
        with pytest.raises(pl.exceptions.SchemaError):
            pl.scan_parquet(self._pattern(tmp_path, "context", "1D", "CNH")).collect()

    def test_no_files_returns_none(self, tmp_path):
        from src.silver.ohlcv_processor import _read_bronze_symbol
        assert _read_bronze_symbol(self._pattern(tmp_path, "context", "1D", "NOPE")) is None

    def test_mixed_string_and_date_is_reconciled(self, tmp_path):
        from src.silver.ohlcv_processor import _read_bronze_symbol
        n = self._write_cnh_mixed(tmp_path)
        df = _read_bronze_symbol(self._pattern(tmp_path, "context", "1D", "CNH"))
        assert df is not None and len(df) == n, "no Bronze row may be dropped by reconciliation"
        assert df.schema["timestamp"] == pl.Datetime("us")
        assert df["timestamp"].null_count() == 0
        assert df["timestamp"].min().date() == date(2026, 8, 20)
        assert df["timestamp"].max().date() == date(2026, 9, 6)

    def test_uniform_schema_takes_fast_path_unchanged(self, tmp_path):
        """A symbol with one dtype must come back exactly as scan_parquet returns it
        (Date stays Date) — the fix may not alter symbols that already work."""
        from src.silver.ohlcv_processor import _read_bronze_symbol
        self._write(tmp_path, "context", "1D", "EUR", "a.parquet",
                    [date(2026, 9, 1) + timedelta(days=i) for i in range(5)])
        self._write(tmp_path, "context", "1D", "EUR", "b.parquet",
                    [date(2026, 9, 6) + timedelta(days=i) for i in range(5)])
        pat = self._pattern(tmp_path, "context", "1D", "EUR")
        got = _read_bronze_symbol(pat)
        assert got.schema["timestamp"] == pl.Date
        assert got.equals(pl.scan_parquet(pat).collect())

    def test_non_numeric_price_after_reconciliation_raises(self, tmp_path):
        """String `close` in one file + Float64 in another must NOT be relaxed to a
        String price column and passed on as if it were fine."""
        from src.silver.ohlcv_processor import _read_bronze_symbol
        self._write(tmp_path, "context", "1D", "BAD", "a.parquet",
                    [date(2026, 9, 1) + timedelta(days=i) for i in range(3)])
        self._write(tmp_path, "context", "1D", "BAD", "b.parquet",
                    ["2026-09-10", "2026-09-11", "2026-09-12"], close_dtype=pl.Utf8)
        with pytest.raises((ValueError, pl.exceptions.PolarsError)):
            _read_bronze_symbol(self._pattern(tmp_path, "context", "1D", "BAD"))

    def test_corrupt_file_raises_instead_of_looking_like_no_data(self, tmp_path):
        from src.silver.ohlcv_processor import _read_bronze_symbol
        f = self._write(tmp_path, "context", "1D", "CRP", "a.parquet",
                        [date(2026, 9, 1) + timedelta(days=i) for i in range(3)])
        f.write_bytes(b"not a parquet file")
        with pytest.raises(Exception):
            _read_bronze_symbol(self._pattern(tmp_path, "context", "1D", "CRP"))

    # ── run_context() ────────────────────────────────────────────────────────────────

    def _ctx_instrument(self, symbol="CNH", timezone="UTC"):
        return TestRunContextEntryPoint._fake_context_instrument(symbol, timezone=timezone)

    def test_run_context_produces_silver_for_mixed_dtype_bronze(self, tmp_path, monkeypatch):
        """THE CNH regression: all three context TFs, String-era + Date-era Bronze."""
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: TestRunContextEntryPoint._fake_loader([self._ctx_instrument()]),
        )
        n = 0
        for tf in ohlcv_mod._RUN_CONTEXT_TFS:
            n = self._write_cnh_mixed(tmp_path, tf=tf)

        ohlcv_mod.run_context(date(2026, 10, 17))

        silver_dir = tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "context" / "symbol=CNH"
        for tf in ohlcv_mod._RUN_CONTEXT_TFS:
            out = silver_dir / f"CNH_{tf}_silver.parquet"
            assert out.exists(), f"CNH {tf} silently dropped — original bug"
        d1 = pl.read_parquet(silver_dir / "CNH_1D_silver.parquet")
        assert len(d1) == n
        assert d1["timestamp"].dtype.time_zone == "UTC"
        # CNH tz is UTC: both eras must land on the SAME midnight convention.
        assert d1["timestamp"].dt.hour().unique().to_list() == [0]
        assert d1["timestamp"].is_unique().all()

    def test_run_context_corrupt_bronze_is_marked_failed_not_done(self, tmp_path, monkeypatch):
        import src.silver.ohlcv_processor as ohlcv_mod
        from src.utils.progress_checkpoint import ProgressCheckpoint

        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: TestRunContextEntryPoint._fake_loader([self._ctx_instrument("CRP")]),
        )
        f = self._write(tmp_path, "context", "1D", "CRP", "a.parquet",
                        [date(2026, 9, 1) + timedelta(days=i) for i in range(3)])
        f.write_bytes(b"not a parquet file")

        ohlcv_mod.run_context(date(2026, 10, 17))   # must not raise out of the job

        ckpt = ProgressCheckpoint("silver_ohlcv_context", date(2026, 10, 17))
        assert not ckpt.is_done("CRP", timeframe="1D")
        assert [r["symbol"] for r in ckpt.failed_report()] == ["CRP"]

    # ── Layer 1 run() shares the same helper ────────────────────────────────────────────────

    def test_run_layer1_produces_silver_for_mixed_dtype_bronze(self, tmp_path, monkeypatch):
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = TestRunEntryPoint._fake_instrument("EUR_USD", market="forex")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: TestRunEntryPoint._fake_loader([inst]),
        )
        old = [f"2026-08-{d:02d}" for d in range(20, 28)]
        new = [date(2026, 8, 28) + timedelta(days=i) for i in range(10)]
        self._write(tmp_path, "forex", "1D", "EUR_USD", "a.parquet", old, source="yfinance")
        self._write(tmp_path, "forex", "1D", "EUR_USD", "b.parquet", new, source="alphavantage")

        ohlcv_mod.run(date(2026, 10, 17))

        out = tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "forex" / "symbol=EUR_USD" / "EUR_USD_1D_silver.parquet"
        assert out.exists(), "Layer 1 PASS 1 shares the swallow bug — must be fixed too"
        assert len(pl.read_parquet(out)) == len(old) + len(new)


class TestBronzePolygonNaiveVsYfinanceTzAware:
    """
    FIX SIL-BRZ-SCHEMA-01, second finding (census 18 Oct 2026): us_stocks/LHX 1M
    has 46 yfinance files (tz-aware Datetime[ms, America/New_York], local
    midnight) plus ONE polygon file from the day yfinance had none (naive
    Datetime[ms], volume Float64, extra vwap). tz-aware + naive has no supertype,
    so LHX stayed without Silver (loudly, after the first fix). The polygon
    adapter writes pd.Timestamp(ms, unit="ms") = naive EPOCH-UTC, measured as
    04:00:00 on 1 Aug/1 Sep 2026 (= New York midnight under EDT).
    """

    NY = "America/New_York"

    @staticmethod
    def _yf(tmp_path, fname, stamps, close):
        from datetime import datetime
        from src.silver.ohlcv_processor import BRONZE_OHLCV_PATH
        n = len(stamps)
        ts = pl.Series("timestamp", [datetime(*s) for s in stamps],
                       dtype=pl.Datetime("ms")).dt.replace_time_zone("America/New_York")
        df = pl.DataFrame({
            "timestamp": ts, "open": [100.0] * n, "high": [101.0] * n, "low": [99.0] * n,
            "close": [close] * n, "volume": pl.Series([1_000_000] * n, dtype=pl.Int64),
            "_source": ["yfinance"] * n,
        })
        d = (tmp_path / BRONZE_OHLCV_PATH / "us_stocks" / "timeframe=1D" / "source=yfinance"
             / "symbol=LHX" / "year=2026" / "month=09")
        d.mkdir(parents=True, exist_ok=True)
        df.write_parquet(d / fname)

    @staticmethod
    def _poly(tmp_path, fname, stamps, close, source="polygon"):
        from datetime import datetime
        from src.silver.ohlcv_processor import BRONZE_OHLCV_PATH
        n = len(stamps)
        df = pl.DataFrame({
            "timestamp": pl.Series("timestamp", [datetime(*s) for s in stamps], dtype=pl.Datetime("ms")),
            "open": [100.0] * n, "high": [101.0] * n, "low": [99.0] * n,
            "close": [close] * n, "volume": pl.Series([2_000_000.0] * n, dtype=pl.Float64),
            "vwap": [100.0] * n, "_source": [source] * n,
        })
        d = (tmp_path / BRONZE_OHLCV_PATH / "us_stocks" / "timeframe=1D" / f"source={source}"
             / "symbol=LHX" / "year=2026" / "month=09")
        d.mkdir(parents=True, exist_ok=True)
        df.write_parquet(d / fname)

    @staticmethod
    def _pattern(tmp_path):
        from src.silver.ohlcv_processor import BRONZE_OHLCV_PATH
        return str(tmp_path / BRONZE_OHLCV_PATH / "us_stocks" / "timeframe=1D"
                   / "**" / "symbol=LHX" / "**" / "*.parquet")

    def _write_lhx_shape(self, tmp_path):
        # polygon: winter bar (05:00 UTC = 00:00 EST), Aug + Sep bars (04:00 UTC = 00:00 EDT)
        self._poly(tmp_path, "LHX_raw_20260911.parquet",
                   [(2026, 1, 1, 5), (2026, 8, 1, 4), (2026, 9, 1, 4)], close=111.0)
        # yfinance: same Sep bar at local midnight, later files win on equal keys
        self._yf(tmp_path, "LHX_raw_20260910.parquet", [(2026, 9, 1)], close=222.0)
        self._yf(tmp_path, "LHX_raw_20260912.parquet", [(2026, 9, 1)], close=333.0)

    def test_polygon_naive_utc_lands_on_the_same_instant_as_yfinance(self, tmp_path):
        from src.silver.ohlcv_processor import _read_bronze_symbol
        self._write_lhx_shape(tmp_path)
        df = _read_bronze_symbol(self._pattern(tmp_path), tz_hint=self.NY)
        assert df.schema["timestamp"].time_zone == self.NY
        local = df["timestamp"].dt.replace_time_zone(None)       # wall clock in New York
        assert local.dt.hour().unique().to_list() == [0], "every bar must be NY-local midnight (EST and EDT)"
        sep = df.filter(pl.col("_source") == "polygon").filter(pl.col("timestamp").dt.month() == 9)
        yf = df.filter(pl.col("_source") == "yfinance")
        assert sep["timestamp"][0] == yf["timestamp"][0], "same instant -> dedupe key collides"

    def test_reconciled_bars_dedupe_to_one_per_month_and_yfinance_wins(self, tmp_path):
        from src.silver.ohlcv_processor import _read_bronze_symbol, OHLCVProcessor
        self._write_lhx_shape(tmp_path)
        df = _read_bronze_symbol(self._pattern(tmp_path), tz_hint=self.NY)
        out = OHLCVProcessor().process_symbol(
            df=df, symbol="LHX", market="us_stocks", timeframe="1D", tz_hint=self.NY)
        assert len(out) == 3, "Jan, Aug, Sep — no duplicate Sep bar from the polygon file"
        assert out["timestamp"].is_unique().all()
        sep_close = out.filter(pl.col("timestamp").dt.month() == 9)["close"][0]
        assert sep_close == 333.0, "primary source (latest yfinance file) must win over polygon"

    def test_without_the_source_rule_naive_would_be_mislabelled(self, tmp_path):
        """Pins WHY the rule exists: the processor's own naive branch localises 04:00
        as New York time (-> 08:00 UTC) and misses the yfinance instant (04:00 UTC)."""
        from datetime import datetime, timezone
        from src.silver.ohlcv_processor import OHLCVProcessor
        naive = pl.DataFrame({"timestamp": pl.Series([datetime(2026, 9, 1, 4)], dtype=pl.Datetime("ms")),
                              "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0]})
        got = OHLCVProcessor._normalize_timestamps(naive, "us_stocks", self.NY)["timestamp"][0]
        assert got == datetime(2026, 9, 1, 8, tzinfo=timezone.utc)
        assert got != datetime(2026, 9, 1, 4, tzinfo=timezone.utc)

    def test_naive_from_unknown_source_in_a_tz_aware_mix_still_raises(self, tmp_path):
        """Only sources KNOWN to write epoch-UTC are reinterpreted; for any other the
        meaning of a naive stamp is unknown and guessing would mis-stamp silently."""
        from src.silver.ohlcv_processor import _read_bronze_symbol
        self._poly(tmp_path, "a.parquet", [(2026, 9, 1, 4)], close=1.0, source="tvdatafeed")
        self._yf(tmp_path, "b.parquet", [(2026, 9, 1)], close=2.0)
        with pytest.raises(pl.exceptions.PolarsError):
            _read_bronze_symbol(self._pattern(tmp_path), tz_hint=self.NY)

    def test_run_layer1_lhx_shape_produces_silver(self, tmp_path, monkeypatch):
        import src.silver.ohlcv_processor as ohlcv_mod

        monkeypatch.chdir(tmp_path)
        inst = TestRunEntryPoint._fake_instrument("LHX", market="us_stocks")
        monkeypatch.setattr(
            "src.config.instrument_loader.get_loader",
            lambda: TestRunEntryPoint._fake_loader([inst]),
        )
        self._write_lhx_shape(tmp_path)

        ohlcv_mod.run(date(2026, 10, 18))

        out = tmp_path / ohlcv_mod.SILVER_OHLCV_PATH / "us_stocks" / "symbol=LHX" / "LHX_1D_silver.parquet"
        assert out.exists(), "LHX shape (tz-aware yfinance + naive polygon) silently/loudly dropped"
        silver = pl.read_parquet(out)
        assert len(silver) == 3 and silver["timestamp"].is_unique().all()
