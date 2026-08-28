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
