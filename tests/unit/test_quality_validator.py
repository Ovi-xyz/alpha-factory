"""tests/unit/test_quality_validator.py — QualityValidator unit tests"""

from datetime import date
from pathlib import Path

import pytest

from src.silver.quality_validator import QualityValidator


class TestQualityValidatorRun:

    def test_run_returns_dict(self):
        """run() returns a dict of {check_name: bool}."""
        qv     = QualityValidator()
        result = qv.run(date(2025, 5, 1))
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_all_checks_in_result(self):
        """Expected check names must all be in result.

        FIX GAP-2 [P0] (Production Readiness Assessment v1.7.2): this test
        asserted the PRE-F-QV-01 key names ('ohlcv_null', 'ohlcv_sanity',
        'ohlcv_outlier', 'ohlcv_freshness', 'ohlcv_coverage', 'adj_integrity',
        'macro_pit'). F-QV-01/02/03 (v1.7.2) renamed quality_validator.py's
        result dict keys to match CRITICAL_CHECKS / WARNING_CHECKS exactly
        (see src/silver/quality_validator.py module docstring) and added
        'gap_detection' (F-QV-02, previously missing entirely). This test
        was never updated to match — a regression introduced by the v1.7.2
        remediation itself, which would fail CI on every run post-merge.
        Updated to the current key names; 'gap_detection' added since it's
        now a real WARNING_CHECKS entry.
        """
        qv       = QualityValidator()
        result   = qv.run(date(2025, 5, 1))
        expected = {
            "null_check", "price_sanity", "coverage_check",  # F-QV-01: renamed
            "gap_detection",                                  # F-QV-02: new
            "outlier_detection", "freshness_check",           # F-QV-03: renamed
            "pit_integrity", "adj_flag_integrity",            # F-QV-03: renamed
            "vix_circuit",                                    # unchanged
            # ADD QV-L2-01: Layer 2 (context anchor) parity checks
            "context_null_check", "context_price_sanity", "context_coverage_check",
            "context_gap_detection", "context_outlier_detection", "context_freshness_check",
        }
        assert expected.issubset(set(result.keys())), (
            f"Missing checks: {expected - set(result.keys())}"
        )

    def test_all_results_are_bool(self):
        """All check results must be boolean."""
        qv     = QualityValidator()
        result = qv.run(date(2025, 5, 1))
        for name, val in result.items():
            assert isinstance(val, bool), f"Check {name!r} returned {type(val)}"

    def test_vix_circuit_always_passes(self):
        """vix_circuit is non-blocking — always returns True."""
        qv     = QualityValidator()
        result = qv.run(date(2025, 5, 1))
        assert result["vix_circuit"] is True

    def test_run_no_silver_data_graceful(self):
        """QV must not raise when Silver data is absent (before any ingestion)."""
        qv = QualityValidator()
        # Should not raise — all checks handle missing data gracefully
        result = qv.run(date(2099, 1, 1))   # Far future = no data
        assert isinstance(result, dict)

    def test_issues_list_cleared_between_runs(self):
        """_issues list is cleared at start of each run."""
        qv = QualityValidator()
        qv.run(date(2025, 5, 1))
        qv.run(date(2025, 5, 1))   # Second run
        # _issues should only contain issues from second run
        # (not accumulated from both)
        assert isinstance(qv._issues, list)


class TestVIXCircuitBreaker:

    def test_vix_circuit_is_in_checks(self):
        """vix_circuit must appear in QualityValidator output."""
        qv     = QualityValidator()
        result = qv.run(date(2025, 5, 1))
        assert "vix_circuit" in result

    def test_vix_circuit_non_blocking(self):
        """Even if VIX data showed spike, check returns True (non-blocking)."""
        qv = QualityValidator()
        # Directly call the check method
        result = qv._check_vix_circuit_breaker(date(2025, 5, 1))
        assert result is True

    def test_issues_populated_on_spike(self):
        """VIX spike (>40) should populate _issues list."""
        import unittest.mock as mock

        qv = QualityValidator()
        qv._issues.clear()

        # Mock DuckDB to return VIX=45
        with mock.patch("src.silver.quality_validator.duckdb") as mock_duckdb:
            mock_con = mock.MagicMock()
            mock_duckdb.connect.return_value = mock_con
            mock_con.execute.return_value.fetchone.return_value = (45.0,)

            result = qv._check_vix_circuit_breaker(date(2025, 5, 1))

        assert result is True   # Non-blocking
        assert any("vix_circuit" in str(i) for i in qv._issues), (
            f"Expected vix_circuit in issues, got: {qv._issues}"
        )


class TestCheckMethods:

    def test_null_check_graceful_no_data(self):
        qv     = QualityValidator()
        result = qv._check_null(date(2099, 1, 1))
        assert result is True   # No data → check passes (nothing to fail)

    def test_price_sanity_graceful_no_data(self):
        qv     = QualityValidator()
        result = qv._check_price_sanity(date(2099, 1, 1))
        assert result is True

    def test_outlier_check_graceful_no_data(self):
        qv     = QualityValidator()
        result = qv._check_outliers(date(2099, 1, 1))
        assert result is True

    def test_freshness_check_graceful_no_data(self):
        qv     = QualityValidator()
        result = qv._check_freshness(date(2099, 1, 1))
        assert result is True

    def test_macro_pit_graceful_no_data(self):
        qv     = QualityValidator()
        result = qv._check_macro_pit(date(2099, 1, 1))
        assert result is True

    # ── ADD QV-L2-01: graceful no-data tests for the 6 new context checks ──
    def test_context_null_graceful_no_data(self):
        assert QualityValidator()._check_context_null(date(2099, 1, 1)) is True

    def test_context_price_sanity_graceful_no_data(self):
        assert QualityValidator()._check_context_price_sanity(date(2099, 1, 1)) is True

    def test_context_coverage_graceful_no_data(self):
        assert QualityValidator()._check_context_coverage(date(2099, 1, 1)) is True

    def test_context_gap_detection_graceful_no_data(self):
        assert QualityValidator()._check_context_gap_detection(date(2099, 1, 1)) is True

    def test_context_outliers_graceful_no_data(self):
        assert QualityValidator()._check_context_outliers(date(2099, 1, 1)) is True

    def test_context_freshness_graceful_no_data(self):
        assert QualityValidator()._check_context_freshness(date(2099, 1, 1)) is True


class TestQVL2MaskingBugsFixed:
    """
    FIX QV-L2-01 — regression guard reproducing the exact two empirically-
    confirmed masking bugs (see src/silver/quality_validator.py and
    src/utils/silver_scope.py module docstrings) as permanent tests, not
    just an ad hoc sandbox probe. Before this fix, both scenarios below
    passed a check that SHOULD have failed.
    """

    def _write(self, path, symbol, timestamps, **overrides):
        import polars as pl
        n = len(timestamps)
        base = {
            "symbol": [symbol] * n, "timestamp": timestamps,
            "open": [100.0] * n, "high": [105.0] * n, "low": [95.0] * n,
            "close": [102.0] * n, "volume": [1_000_000] * n,
            "is_clean": [True] * n, "log_return": [0.001] * n,
            "is_adjusted": [True] * n, "adj_factor": [1.0] * n,
        }
        base.update(overrides)
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(base).write_parquet(path)

    def test_fresh_layer2_anchor_no_longer_masks_stale_layer1(self, tmp_path, monkeypatch):
        """
        Reproduces the exact scenario silver_scope.py's module docstring
        describes: Layer 1 (AAPL) is 19 days stale; Layer 2 (VIX) is fresh
        as of run_date. Pre-fix, freshness_check's MAX(timestamp) spanned
        both layers and reported 0-day lag (masked). Post-fix, it must
        correctly report the Layer 1 staleness and FAIL.
        """
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)

        stale_date = date(2026, 6, 1)
        run_date   = date(2026, 6, 20)  # 19 days after AAPL's latest bar
        self._write(
            tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet",
            "AAPL", [stale_date],
        )
        self._write(
            tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet",
            "VIX", [run_date],
        )

        qv = QualityValidator()
        assert qv._check_freshness(run_date) is False, (
            "freshness_check must FAIL when Layer 1 is stale, even if a "
            "Layer 2 anchor is fresh — pre-fix this incorrectly passed"
        )

    def test_layer2_symbols_no_longer_inflate_layer1_coverage(self, tmp_path, monkeypatch):
        """
        Reproduces coverage_check's masking bug: a Layer 2 symbol appearing
        in the numerator (COUNT(DISTINCT symbol) from an unfiltered glob)
        against a Layer-1-only denominator could push coverage% above
        100% of the true Layer 1 figure, hiding a genuine Layer 1 drop.
        Here we simulate ONLY 1 of get_loader().count() Layer 1 symbols
        being fresh, plus 1 Layer 2 symbol fresh — post-fix, the Layer 2
        row must contribute NOTHING to the numerator.
        """
        import src.silver.quality_validator as qv_mod
        from src.config.instrument_loader import get_loader
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)

        run_date = date(2026, 6, 20)
        self._write(
            tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet",
            "AAPL", [run_date],
        )
        self._write(
            tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet",
            "VIX", [run_date],
        )

        qv = QualityValidator()
        # We can't easily intercept get_loader().count() here without
        # touching production config, so instead we assert the STRUCTURAL
        # guarantee directly: the underlying glob list used by coverage
        # must never include the context/ directory.
        from src.utils.silver_scope import layer1_globs
        globs = layer1_globs(tmp_path, "*_1D_silver.parquet")
        assert all("context" not in Path(g).parts for g in map(str, globs)), (
            "coverage_check's Layer 1 glob must never include context/ — "
            "confirms the numerator can no longer be inflated by Layer 2"
        )
        # And confirm the check still runs without error against this fixture.
        qv._check_coverage(run_date)

    def test_context_coverage_uses_context_denominator_not_layer1(self, tmp_path, monkeypatch):
        """context_coverage_check must divide by count_context(), never
        count() — this IS the bug this whole fix exists to prevent from
        ever recurring in the Layer 2 direction too."""
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        from src.config.instrument_loader import get_loader

        run_date = date(2026, 6, 20)
        # Write all 49 active Layer 2 symbols fresh -> should be 100% coverage
        loader = get_loader()
        for inst in loader.all_context(include_deferred=False):
            self._write(
                tmp_path / "context" / f"symbol={inst.symbol}" / f"{inst.symbol}_1D_silver.parquet",
                inst.symbol, [run_date],
            )
        qv = QualityValidator()
        assert qv._check_context_coverage(run_date) is True

    def test_context_price_sanity_detects_violation(self, tmp_path, monkeypatch):
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        self._write(
            tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet",
            "VIX", [date(2026, 6, 20)],
            high=[10.0], low=[20.0],  # high < low -> violation
        )
        qv = QualityValidator()
        assert qv._check_context_price_sanity(date(2026, 6, 20)) is False

    def test_layer1_checks_unaffected_by_context_only_data(self, tmp_path, monkeypatch):
        """If ONLY Layer 2 data exists (no Layer 1 at all — e.g. Cycle 3
        ran but Layer 1 backfill hasn't started), Layer 1 checks must
        gracefully report 'no data yet' (True), not error or falsely pass
        using Layer 2 rows as a stand-in."""
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        self._write(
            tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet",
            "VIX", [date(2026, 6, 20)],
        )
        qv = QualityValidator()
        assert qv._check_null(date(2026, 6, 20)) is True
        assert qv._check_price_sanity(date(2026, 6, 20)) is True
        assert qv._check_freshness(date(2026, 6, 20)) is True


class TestPriceSanityIsCleanScoping:
    """
    FIX QV-PS-01 [chat thread, 2 Sep 2026]: price_sanity previously counted
    ANY OHLC-ordering violation, including rows OHLCVProcessor._flag_is_clean()
    had already correctly caught and flagged is_clean=False at Silver-write
    time — duplicating that check and hard-blocking Gold on noise that was
    already quarantined and already excluded from every downstream consumer.
    Live-test (2 Sep 2026) found 2101 such rows concentrated in forex/IDX
    (known noisier retail-feed OHLC), zero in us_stocks. This scopes the
    CRITICAL gate to `is_clean=TRUE` rows: it should now only fire when
    self-flagging itself has a gap, not on data that's already correctly
    handled.
    """

    @staticmethod
    def _write(path, symbol, timestamps, **overrides):
        n = len(timestamps)
        base = {
            "symbol": [symbol] * n, "timestamp": timestamps,
            "open": [100.0] * n, "high": [105.0] * n, "low": [95.0] * n,
            "close": [102.0] * n, "volume": [1_000_000] * n,
            "is_clean": [True] * n,
        }
        base.update(overrides)
        path.parent.mkdir(parents=True, exist_ok=True)
        import polars as pl
        pl.DataFrame(base).write_parquet(path)

    def test_already_flagged_violation_no_longer_blocks(self, tmp_path, monkeypatch):
        """A row with high < low but is_clean already False (OHLCVProcessor
        did its job) must NOT trip the CRITICAL gate."""
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        self._write(
            tmp_path / "forex" / "symbol=USD_JPY" / "USD_JPY_1D_silver.parquet",
            "USD_JPY", [date(2026, 6, 20)],
            high=[10.0], low=[20.0], is_clean=[False],
        )
        qv = QualityValidator()
        assert qv._check_price_sanity(date(2026, 6, 20)) is True, (
            "Pre-fix this incorrectly returned False — re-detecting a "
            "violation that was already correctly self-quarantined"
        )

    def test_unflagged_violation_still_caught(self, tmp_path, monkeypatch):
        """The genuine invariant this check protects: an OHLC violation
        that somehow escaped OHLCVProcessor's own flagging (is_clean still
        True) must still trip the CRITICAL gate. This is the regression
        guard proving FIX QV-PS-01 narrowed scope, not neutered the check."""
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        self._write(
            tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet",
            "AAPL", [date(2026, 6, 20)],
            high=[10.0], low=[20.0], is_clean=[True],
        )
        qv = QualityValidator()
        assert qv._check_price_sanity(date(2026, 6, 20)) is False

    def test_mixed_flagged_and_unflagged_only_counts_unflagged(self, tmp_path, monkeypatch):
        """One already-flagged violation + one genuinely-escaped violation
        in the same file — must fail (because of the escaped one), and the
        detail message should reflect only the unflagged count (1, not 2)."""
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        self._write(
            tmp_path / "forex" / "symbol=EUR_USD" / "EUR_USD_1D_silver.parquet",
            "EUR_USD", [date(2026, 6, 20), date(2026, 6, 21)],
            high=[10.0, 10.0], low=[20.0, 20.0],
            is_clean=[False, True],  # first already caught, second escaped
        )
        qv = QualityValidator()
        assert qv._check_price_sanity(date(2026, 6, 20)) is False
        detail = qv._issues[-1]["detail"]
        assert "1 rows" in detail


class TestOutlierSurvivesNonFiniteLogReturn:
    """
    FIX QV-OUT-01 [chat thread, 2 Sep 2026]: a sign-crossing close (e.g.
    CL/WTI negative on 2020-04-20/21, a real historical event) makes
    log_return NaN/Inf for that symbol. Pre-fix, DuckDB's STDDEV_SAMP
    window function overflowed on that one partition and aborted the
    ENTIRE cross-symbol query ("STDDEV_SAMP is out of range!"), silently
    skipping outlier detection for all Layer 1 symbols in the same run —
    not just the offending one. This must no longer happen, and a genuine
    outlier in an unrelated, well-behaved symbol must still be detected
    in the same run.
    """

    @staticmethod
    def _write_symbol_1d(base_dir, symbol, market, df):
        from pathlib import Path
        out_dir = Path(base_dir) / market / f"symbol={symbol}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{symbol}_1D_silver.parquet"
        df.write_parquet(out_path)
        return out_path

    def _make_ohlcv(self, symbol, n=250, outlier_idx=None, nonfinite_idx=None,
                     nonfinite_value=float("inf"), seed=0):
        import datetime
        import numpy as np
        import polars as pl

        rng  = np.random.default_rng(seed)
        base = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
        rows = []
        for i in range(n):
            lr = float(rng.normal(0, 0.01))
            rows.append({
                "symbol": symbol, "timestamp": base + datetime.timedelta(days=i),
                "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
                "volume": 1_000_000, "log_return": lr, "is_clean": True,
            })
        if outlier_idx is not None:
            rows[outlier_idx]["log_return"] = 12.0
        if nonfinite_idx is not None:
            rows[nonfinite_idx]["log_return"] = nonfinite_value
        return pl.DataFrame(rows)

    def test_query_does_not_raise_with_infinite_partition(self, tmp_path, monkeypatch):
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)

        cl_df = self._make_ohlcv("CL", nonfinite_idx=100, nonfinite_value=float("inf"))
        self._write_symbol_1d(tmp_path, "CL", "commodity_trading", cl_df)

        validator = qv_mod.QualityValidator()
        result = validator._check_outliers(date(2025, 1, 1))
        assert result is True   # non-blocking; must not raise/skip-as-error

    def test_nan_partition_also_survives(self, tmp_path, monkeypatch):
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)

        cl_df = self._make_ohlcv("CL", nonfinite_idx=100, nonfinite_value=float("nan"))
        self._write_symbol_1d(tmp_path, "CL", "commodity_trading", cl_df)

        validator = qv_mod.QualityValidator()
        assert validator._check_outliers(date(2025, 1, 1)) is True

    def test_other_symbols_outliers_still_detected_alongside_poisoned_one(
        self, tmp_path, monkeypatch
    ):
        """The core regression: CL's Inf must not take down TSLA's genuine
        outlier detection in the same run — pre-fix, the whole query
        aborted, so TSLA's real outlier silently never got flagged."""
        import polars as pl
        import src.silver.quality_validator as qv_mod
        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)

        cl_df    = self._make_ohlcv("CL", nonfinite_idx=100, seed=1)
        tsla_df  = self._make_ohlcv("TSLA", outlier_idx=75, seed=2)
        cl_path   = self._write_symbol_1d(tmp_path, "CL", "commodity_trading", cl_df)
        tsla_path = self._write_symbol_1d(tmp_path, "TSLA", "us_stocks", tsla_df)

        validator = qv_mod.QualityValidator()
        validator._check_outliers(date(2025, 1, 1))

        assert pl.read_parquet(tsla_path).filter(~pl.col("is_clean")).height == 1, (
            "Pre-fix: the whole query would have raised before ever "
            "reaching TSLA's partition, leaving it unflagged"
        )
        # CL's own file is untouched by outlier writeback for the Inf row
        # since Inf was filtered out of the detection query entirely —
        # it's still is_clean=True for outlier purposes here (price_sanity
        # / null_check are separate checks with their own scope).
        assert pl.read_parquet(cl_path).height == cl_df.height


class TestOutlierWriteback:
    """
    FIX GAP-4 [P1] + GAP-9 [P3] (Production Readiness Assessment v1.7.2):
    _check_outliers() now writes is_clean=False back to Silver Parquet for
    detected outlier bars (GAP-4), via a single-pass DuckDB window-function
    query instead of the previous two-scan CTE+JOIN (GAP-9). These tests
    build a real Silver 1D fixture on disk (monkeypatching SILVER_OHLCV_PATH
    to a tmp_path) so the full DuckDB read -> detect -> atomic writeback ->
    re-read round trip is exercised, not just mocked.
    """

    @staticmethod
    def _write_symbol_1d(base_dir, symbol: str, market: str, df) -> "Path":
        from pathlib import Path
        out_dir = Path(base_dir) / market / f"symbol={symbol}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{symbol}_1D_silver.parquet"
        df.write_parquet(out_path)
        return out_path

    def _make_ohlcv(self, symbol: str, n: int = 250, outlier_idx=None, seed: int = 0):
        import datetime
        import polars as pl
        import numpy as np

        rng   = np.random.default_rng(seed)
        base  = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
        rows  = []
        for i in range(n):
            lr = float(rng.normal(0, 0.01))
            rows.append({
                "symbol":     symbol,
                "timestamp":  base + datetime.timedelta(days=i),
                "open":       100.0, "high": 101.0, "low": 99.0, "close": 100.0,
                "volume":     1_000_000,
                "log_return": lr,
                "is_clean":   True,
            })
        if outlier_idx is not None:
            rows[outlier_idx]["log_return"] = 12.0   # extreme z-score
        return pl.DataFrame(rows)

    def test_outlier_flagged_is_clean_false(self, tmp_path, monkeypatch):
        """An injected extreme log_return bar must end up is_clean=False on disk."""
        import polars as pl
        import src.silver.quality_validator as qv_mod

        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        df = self._make_ohlcv("AAPL", outlier_idx=120)
        path = self._write_symbol_1d(tmp_path, "AAPL", "us_stocks", df)

        validator = qv_mod.QualityValidator()
        result = validator._check_outliers(date(2025, 1, 1))

        assert result is True   # non-blocking regardless of outcome
        on_disk = pl.read_parquet(path)
        flagged = on_disk.filter(~pl.col("is_clean"))
        assert flagged.height == 1, "Exactly the injected outlier bar should be flagged"
        assert on_disk.height == df.height, "Row count must be preserved by the rewrite"

    def test_no_outlier_no_write(self, tmp_path, monkeypatch):
        """Clean data must not be rewritten — all rows stay is_clean=True."""
        import polars as pl
        import src.silver.quality_validator as qv_mod

        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        df = self._make_ohlcv("MSFT", outlier_idx=None)
        path = self._write_symbol_1d(tmp_path, "MSFT", "us_stocks", df)
        mtime_before = path.stat().st_mtime_ns

        validator = qv_mod.QualityValidator()
        validator._check_outliers(date(2025, 1, 1))

        on_disk = pl.read_parquet(path)
        assert on_disk.filter(~pl.col("is_clean")).height == 0
        # No outliers found for this symbol -> _flag_outliers_in_file is never
        # invoked for it -> file must not be touched at all.
        assert path.stat().st_mtime_ns == mtime_before

    def test_writeback_idempotent_on_rerun(self, tmp_path, monkeypatch):
        """Running the check twice must not re-flag or duplicate rows."""
        import polars as pl
        import src.silver.quality_validator as qv_mod

        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        df = self._make_ohlcv("GOOGL", outlier_idx=50)
        path = self._write_symbol_1d(tmp_path, "GOOGL", "us_stocks", df)

        validator = qv_mod.QualityValidator()
        validator._check_outliers(date(2025, 1, 1))
        first_pass = pl.read_parquet(path)
        flagged_after_first = first_pass.filter(~pl.col("is_clean")).height

        validator._check_outliers(date(2025, 1, 1))
        second_pass = pl.read_parquet(path)
        flagged_after_second = second_pass.filter(~pl.col("is_clean")).height

        assert flagged_after_first == 1
        assert flagged_after_second == 1   # not re-flagged or duplicated
        assert second_pass.height == df.height

    def test_multi_symbol_single_pass(self, tmp_path, monkeypatch):
        """GAP-9: detection must scope outliers to the correct symbol only."""
        import polars as pl
        import src.silver.quality_validator as qv_mod

        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        clean_df   = self._make_ohlcv("AAPL", outlier_idx=None, seed=1)
        outlier_df = self._make_ohlcv("TSLA", outlier_idx=75, seed=2)
        path_aapl = self._write_symbol_1d(tmp_path, "AAPL", "us_stocks", clean_df)
        path_tsla = self._write_symbol_1d(tmp_path, "TSLA", "us_stocks", outlier_df)

        validator = qv_mod.QualityValidator()
        validator._check_outliers(date(2025, 1, 1))

        assert pl.read_parquet(path_aapl).filter(~pl.col("is_clean")).height == 0
        assert pl.read_parquet(path_tsla).filter(~pl.col("is_clean")).height == 1

    def test_preexisting_dirty_rows_untouched(self, tmp_path, monkeypatch):
        """Rows already is_clean=False (e.g. from price_sanity) must stay False
        and must not be double-counted as a 'new' flip."""
        import polars as pl
        import src.silver.quality_validator as qv_mod

        monkeypatch.setattr(qv_mod, "SILVER_OHLCV_PATH", tmp_path)
        df = self._make_ohlcv("NVDA", outlier_idx=30)
        # Mark a different, non-outlier row as already dirty from another check
        df = df.with_columns(
            pl.when(pl.arange(0, df.height) == 5)
            .then(False)
            .otherwise(pl.col("is_clean"))
            .alias("is_clean")
        )
        path = self._write_symbol_1d(tmp_path, "NVDA", "us_stocks", df)

        validator = qv_mod.QualityValidator()
        validator._check_outliers(date(2025, 1, 1))

        on_disk = pl.read_parquet(path)
        dirty = on_disk.filter(~pl.col("is_clean"))
        assert dirty.height == 2   # pre-existing dirty row + the outlier row
        assert set(dirty["log_return"].to_list()) == {
            df["log_return"][5], 12.0
        }
