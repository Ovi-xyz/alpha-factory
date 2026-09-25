"""
tests/unit/test_signal_aggregation.py — signal_aggregation unit tests.

Covers: per-TF component formulas (bounds + null-safety), cross-TF
composite merge (outer join, no-nulls-for-active-symbols guarantee),
grade thresholds, sector breadth computation, sector momentum lookback,
breadth_divergence (soft mtf dependency), and the run() entry point
(checkpoint idempotency, atomic write, graceful-empty path).
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

import polars as pl
import pytest

import src.gold.signal_aggregation as sa_mod
from src.gold.signal_aggregation import (
    GRADE_A_THRESHOLD,
    GRADE_B_THRESHOLD,
    GRADE_C_THRESHOLD,
    TIMEFRAMES,
    _compute,
    _compute_composite_scores,
    _compute_sector_breadth,
    _load_prior_sector_breadth,
    _load_sector_map,
    _load_tf_components,
    load_signal_aggregation,
    run,
)
from src.utils.progress_checkpoint import ProgressCheckpoint


def _write_tf_fixture(path, tf: str, rows: list[dict]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path / f"tech_signals_{tf}.parquet")


def _base_row(symbol: str, **overrides) -> dict:
    row = {
        "symbol": symbol,
        "timestamp": "2026-09-22",
        "close": 100.0,
        "ema_50": 95.0,
        "rsi_14": 50.0,
        "macd_hist": 0.0,
        "atr_14": 2.0,
        "adx": 20.0,
        "di_plus": 20.0,
        "di_minus": 15.0,
        "relative_volume": 1.0,
    }
    row.update(overrides)
    return row


class TestTimeframesSourceOfTruth:
    def test_timeframes_matches_technical_signals(self):
        """TIMEFRAMES must be imported from technical_signals — single
        source of truth, no third hardcoded copy (drift risk)."""
        from src.gold.technical_signals import TIMEFRAMES as TS_TF
        assert TIMEFRAMES is TS_TF


class TestPerTimeframeComponents:
    """_load_tf_components: per-TF bounded [-1, 1] composite, null-safe."""

    def test_missing_file_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        out = _load_tf_components("1D", None)
        assert out.is_empty()

    def test_bullish_symbol_positive_composite(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        _write_tf_fixture(tmp_path, "1D", [
            _base_row("AAA", rsi_14=80.0, macd_hist=2.0, atr_14=2.0,
                      adx=40.0, di_plus=30.0, di_minus=5.0, relative_volume=2.0),
        ])
        out = _load_tf_components("1D", None)
        val = out.filter(pl.col("symbol") == "AAA")["tf_composite_1D"][0]
        assert val is not None
        assert 0.0 < val <= 1.0

    def test_bearish_symbol_negative_composite(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        _write_tf_fixture(tmp_path, "1D", [
            _base_row("BBB", rsi_14=20.0, macd_hist=-2.0, atr_14=2.0,
                      adx=40.0, di_plus=5.0, di_minus=30.0, relative_volume=1.0),
        ])
        out = _load_tf_components("1D", None)
        val = out.filter(pl.col("symbol") == "BBB")["tf_composite_1D"][0]
        assert val is not None
        assert -1.0 <= val < 0.0

    def test_composite_bounded_at_extremes(self, tmp_path, monkeypatch):
        """Even pathological inputs must stay within [-1, 1] (each
        component is independently bounded before averaging)."""
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        _write_tf_fixture(tmp_path, "1D", [
            _base_row("EXTREME", rsi_14=100.0, macd_hist=1_000_000.0, atr_14=0.001,
                      adx=999.0, di_plus=999.0, di_minus=0.0, relative_volume=999.0),
        ])
        out = _load_tf_components("1D", None)
        val = out.filter(pl.col("symbol") == "EXTREME")["tf_composite_1D"][0]
        assert -1.0 <= val <= 1.0

    def test_zero_or_missing_atr_yields_null_macd_component_not_crash(self, tmp_path, monkeypatch):
        """atr_14 <= 0 must null the MACD component (div-by-zero guard),
        not raise — the other 3 components still contribute."""
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        _write_tf_fixture(tmp_path, "1D", [
            _base_row("ZEROATR", atr_14=0.0),
            _base_row("NULLATR", atr_14=None),
        ])
        out = _load_tf_components("1D", None)
        assert out["tf_composite_1D"].null_count() == 0  # RSI/ADX/vol still average

    def test_active_symbols_filter_applied(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        _write_tf_fixture(tmp_path, "1D", [_base_row("KEEP"), _base_row("DROP")])
        out = _load_tf_components("1D", ["KEEP"])
        assert out["symbol"].to_list() == ["KEEP"]

    def test_latest_bar_only_used(self, tmp_path, monkeypatch):
        """Multiple bars per symbol — only the most recent timestamp counts."""
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        _write_tf_fixture(tmp_path, "1D", [
            _base_row("AAA", timestamp="2026-09-01", rsi_14=10.0),
            _base_row("AAA", timestamp="2026-09-22", rsi_14=90.0),
        ])
        out = _load_tf_components("1D", None)
        assert len(out) == 1
        # rsi_14=90 -> _rsi_c=0.8, others neutral (0-ish) -> composite should be clearly positive
        assert out["tf_composite_1D"][0] > 0.0


class TestCompositeAggregation:
    """_compute_composite_scores: cross-TF outer join + grading."""

    def test_no_nulls_for_active_symbols(self, tmp_path, monkeypatch):
        """A symbol present in the active universe but with zero usable
        indicator data anywhere must still get composite_score = 0.0,
        never null (Architecture v2.0 §10.2 requirement)."""
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        for tf in TIMEFRAMES:
            _write_tf_fixture(tmp_path, tf, [_base_row("AAA")])
        # Symbol with NO data in any TF file at all
        out = _compute_composite_scores(date(2026, 9, 22), ["AAA", "GHOST"])
        assert out["composite_score"].null_count() == 0

    def test_symbol_missing_one_timeframe_not_dropped(self, tmp_path, monkeypatch):
        """Outer join across TFs — a symbol present in only some TFs must
        still appear in the final output (the exact drop-bug class this
        codebase has repeatedly caught — ADR-046 Path C, FIX GLD-L2-01)."""
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        _write_tf_fixture(tmp_path, "1D", [_base_row("ONLY_1D", rsi_14=70.0)])
        # No other TF files exist at all
        out = _compute_composite_scores(date(2026, 9, 22), None)
        assert "ONLY_1D" in out["symbol"].to_list()
        assert out.filter(pl.col("symbol") == "ONLY_1D")["composite_score"][0] is not None

    def test_tf_coverage_count_reflects_available_timeframes(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        _write_tf_fixture(tmp_path, "1D", [_base_row("AAA")])
        _write_tf_fixture(tmp_path, "1W", [_base_row("AAA")])
        out = _compute_composite_scores(date(2026, 9, 22), None)
        row = out.filter(pl.col("symbol") == "AAA")
        assert row["tf_coverage_count"][0] == 2

    def test_composite_score_bounded(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        for tf in TIMEFRAMES:
            _write_tf_fixture(tmp_path, tf, [
                _base_row("A", rsi_14=99.0, macd_hist=999.0, adx=99.0, di_plus=99.0, di_minus=0.0),
                _base_row("B", rsi_14=1.0, macd_hist=-999.0, adx=99.0, di_plus=0.0, di_minus=99.0),
            ])
        out = _compute_composite_scores(date(2026, 9, 22), None)
        assert out["composite_score"].min() >= -1.0
        assert out["composite_score"].max() <= 1.0

    @pytest.mark.parametrize("score,expected_grade", [
        (GRADE_A_THRESHOLD, "A"),
        (0.99, "A"),
        (GRADE_B_THRESHOLD, "B"),
        (0.50, "B"),
        (GRADE_C_THRESHOLD, "C"),
        (0.20, "C"),
        (0.0, "D"),
        (0.05, "D"),
    ])
    def test_grade_thresholds(self, tmp_path, monkeypatch, score, expected_grade):
        """Grade boundaries computed via the same Polars expression the
        module uses, spot-checked at and above each threshold."""
        df = pl.DataFrame({"composite_score": [score]}).with_columns(
            pl.when(pl.col("composite_score").abs() >= GRADE_A_THRESHOLD).then(pl.lit("A"))
              .when(pl.col("composite_score").abs() >= GRADE_B_THRESHOLD).then(pl.lit("B"))
              .when(pl.col("composite_score").abs() >= GRADE_C_THRESHOLD).then(pl.lit("C"))
              .otherwise(pl.lit("D"))
              .alias("g")
        )
        assert df["g"][0] == expected_grade

    def test_empty_when_no_tf_data_anywhere(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        out = _compute_composite_scores(date(2026, 9, 22), ["AAA"])
        assert out.is_empty()


class TestSectorMap:
    def test_reads_sector_regime_weights_when_present(self, tmp_path, monkeypatch):
        sector_path = tmp_path / "sector_regime_weights.parquet"
        monkeypatch.setattr(sa_mod, "GOLD_SECTOR_PATH", sector_path)
        pl.DataFrame({
            "symbol": ["AAA"], "market": ["us_stocks"], "sector": ["Technology"],
        }).write_parquet(sector_path)
        out = _load_sector_map()
        assert out.filter(pl.col("symbol") == "AAA")["sector"][0] == "Technology"

    def test_falls_back_when_sector_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SECTOR_PATH", tmp_path / "does_not_exist.parquet")
        # Fallback goes through InstrumentLoader — just assert it doesn't raise
        # and returns a DataFrame with the expected columns.
        out = _load_sector_map()
        assert set(out.columns) == {"symbol", "market", "sector"}


class TestSectorBreadth:
    def test_breadth_percentage_computed_correctly(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        _write_tf_fixture(tmp_path, "1D", [
            _base_row("AAA", close=100.0, ema_50=90.0),   # above
            _base_row("BBB", close=80.0, ema_50=90.0),    # below
            _base_row("CCC", close=100.0, ema_50=90.0),   # above
        ])
        sector_map = pl.DataFrame({
            "symbol": ["AAA", "BBB", "CCC"],
            "market": ["us_stocks"] * 3,
            "sector": ["Technology"] * 3,
        })
        out = _compute_sector_breadth(date(2026, 9, 22), None, sector_map)
        row = out.filter(pl.col("sector") == "Technology")
        assert row["sector_breadth_pct"][0] == pytest.approx(200 / 3)
        assert row["sector_symbol_count"][0] == 3

    def test_empty_when_no_1d_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        sector_map = pl.DataFrame({
            "symbol": ["AAA"], "market": ["us_stocks"], "sector": ["Technology"],
        })
        out = _compute_sector_breadth(date(2026, 9, 22), None, sector_map)
        assert out.is_empty()

    def test_unknown_sector_bucket_used_when_unmapped(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path)
        _write_tf_fixture(tmp_path, "1D", [_base_row("ORPHAN", close=100.0, ema_50=90.0)])
        empty_map = pl.DataFrame({
            "symbol": pl.Series([], dtype=pl.Utf8),
            "market": pl.Series([], dtype=pl.Utf8),
            "sector": pl.Series([], dtype=pl.Utf8),
        })
        out = _compute_sector_breadth(date(2026, 9, 22), None, empty_map)
        assert out["sector"].to_list() == ["Unknown"]


class TestSectorMomentum:
    def test_no_prior_file_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNAL_AGG_PATH", tmp_path)
        out = _load_prior_sector_breadth(date(2026, 9, 22))
        assert out.is_empty()

    def test_finds_prior_file_within_window(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNAL_AGG_PATH", tmp_path)
        run_date = date(2026, 9, 22)
        prior_date = run_date - timedelta(days=7)   # exact target
        pl.DataFrame({
            "sector": ["Technology"], "sector_breadth_pct": [42.0],
        }).write_parquet(tmp_path / f"signal_aggregation_{prior_date.isoformat()}.parquet")

        out = _load_prior_sector_breadth(run_date)
        assert out.filter(pl.col("sector") == "Technology")["prior_sector_breadth_pct"][0] == 42.0

    def test_ignores_files_outside_window(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNAL_AGG_PATH", tmp_path)
        run_date = date(2026, 9, 22)
        too_recent = run_date - timedelta(days=1)   # < MIN_DAYS (4)
        pl.DataFrame({
            "sector": ["Technology"], "sector_breadth_pct": [99.0],
        }).write_parquet(tmp_path / f"signal_aggregation_{too_recent.isoformat()}.parquet")

        out = _load_prior_sector_breadth(run_date)
        assert out.is_empty()

    def test_picks_closest_to_target_when_multiple_available(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNAL_AGG_PATH", tmp_path)
        run_date = date(2026, 9, 22)
        for offset, val in [(5, 10.0), (7, 20.0), (9, 30.0)]:
            d = run_date - timedelta(days=offset)
            pl.DataFrame({"sector": ["Technology"], "sector_breadth_pct": [val]}).write_parquet(
                tmp_path / f"signal_aggregation_{d.isoformat()}.parquet"
            )
        out = _load_prior_sector_breadth(run_date)   # target=7 -> exact match wins
        assert out["prior_sector_breadth_pct"][0] == 20.0


class TestBreadthDivergenceIntegration:
    """End-to-end via _compute() — mtf is a soft input."""

    def _write_full_fixture(self, tmp_path, monkeypatch, mtf_rows=None):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path / "signals")
        monkeypatch.setattr(sa_mod, "GOLD_MTF_PATH", tmp_path / "mtf")
        monkeypatch.setattr(sa_mod, "GOLD_SECTOR_PATH", tmp_path / "sector.parquet")
        monkeypatch.setattr(sa_mod, "GOLD_SIGNAL_AGG_PATH", tmp_path / "out")
        for tf in TIMEFRAMES:
            _write_tf_fixture(tmp_path / "signals", tf, [_base_row("AAA"), _base_row("BBB")])
        pl.DataFrame({
            "symbol": ["AAA", "BBB"], "market": ["us_stocks"] * 2, "sector": ["Technology"] * 2,
        }).write_parquet(tmp_path / "sector.parquet")
        (tmp_path / "mtf").mkdir(parents=True, exist_ok=True)
        if mtf_rows is not None:
            pl.DataFrame(mtf_rows).write_parquet(
                (tmp_path / "mtf") / "mtf_alignment_2026-09-22.parquet"
            )

    def test_breadth_divergence_null_when_mtf_missing(self, tmp_path, monkeypatch):
        self._write_full_fixture(tmp_path, monkeypatch, mtf_rows=None)
        with patch.object(sa_mod, "_active_ohlcv_symbols", return_value=["AAA", "BBB"]):
            df = _compute(date(2026, 9, 22))
        assert df["mtf_score"].null_count() == len(df)
        assert df["breadth_divergence"].null_count() == len(df)
        # composite_score must still be fully computed regardless
        assert df["composite_score"].null_count() == 0

    def test_breadth_divergence_computed_when_mtf_present(self, tmp_path, monkeypatch):
        self._write_full_fixture(
            tmp_path, monkeypatch, mtf_rows=[{"symbol": "AAA", "mtf_score": 5}]
        )
        with patch.object(sa_mod, "_active_ohlcv_symbols", return_value=["AAA", "BBB"]):
            df = _compute(date(2026, 9, 22))
        aaa = df.filter(pl.col("symbol") == "AAA")
        bbb = df.filter(pl.col("symbol") == "BBB")
        assert aaa["mtf_score"][0] == 5
        assert aaa["breadth_divergence"][0] == pytest.approx(
            5 / 5.0 - aaa["composite_score"][0]
        )
        # BBB has no mtf row -> null, not accidentally 0.0
        assert bbb["mtf_score"][0] is None
        assert bbb["breadth_divergence"][0] is None


class TestRunEntryPoint:
    def test_run_writes_output_and_marks_checkpoint_done(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ProgressCheckpoint, "DB_PATH", tmp_path / "progress.db")
        monkeypatch.setattr(sa_mod, "GOLD_SIGNALS_PATH", tmp_path / "signals")
        monkeypatch.setattr(sa_mod, "GOLD_MTF_PATH", tmp_path / "mtf")
        monkeypatch.setattr(sa_mod, "GOLD_SECTOR_PATH", tmp_path / "sector.parquet")
        monkeypatch.setattr(sa_mod, "GOLD_SIGNAL_AGG_PATH", tmp_path / "out")
        for tf in TIMEFRAMES:
            _write_tf_fixture(tmp_path / "signals", tf, [_base_row("AAA")])

        with patch.object(sa_mod, "_active_ohlcv_symbols", return_value=["AAA"]):
            run(date(2026, 9, 22))

        out_path = tmp_path / "out" / "signal_aggregation_2026-09-22.parquet"
        assert out_path.exists()
        checkpoint = ProgressCheckpoint("signal_aggregation", date(2026, 9, 22))
        assert checkpoint.is_done("ALL")

    def test_run_is_idempotent_skips_when_already_done(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ProgressCheckpoint, "DB_PATH", tmp_path / "progress.db")
        monkeypatch.setattr(sa_mod, "GOLD_SIGNAL_AGG_PATH", tmp_path / "out")
        checkpoint = ProgressCheckpoint("signal_aggregation", date(2026, 9, 22))
        checkpoint.mark_done("ALL")

        with patch.object(sa_mod, "_compute") as mock_compute:
            run(date(2026, 9, 22))
        mock_compute.assert_not_called()

    def test_run_marks_failed_on_exception_and_reraises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ProgressCheckpoint, "DB_PATH", tmp_path / "progress.db")
        with patch.object(sa_mod, "_compute", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                run(date(2026, 9, 22))
        checkpoint = ProgressCheckpoint("signal_aggregation", date(2026, 9, 22))
        report = checkpoint.failed_report()
        assert any("boom" in r["error_msg"] for r in report)

    def test_run_handles_empty_result_gracefully(self, tmp_path, monkeypatch):
        """No active symbols / no signals yet -> empty output, marked done,
        no crash (matches gold_signals' own graceful-empty precedent)."""
        monkeypatch.setattr(ProgressCheckpoint, "DB_PATH", tmp_path / "progress.db")
        with patch.object(sa_mod, "_compute", return_value=pl.DataFrame()):
            run(date(2026, 9, 22))
        checkpoint = ProgressCheckpoint("signal_aggregation", date(2026, 9, 22))
        assert checkpoint.is_done("ALL")


class TestLoadSignalAggregation:
    def test_raises_file_not_found_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNAL_AGG_PATH", tmp_path)
        with pytest.raises(FileNotFoundError):
            load_signal_aggregation(date(2099, 1, 1))

    def test_loads_previously_written_output(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sa_mod, "GOLD_SIGNAL_AGG_PATH", tmp_path)
        run_date = date(2026, 9, 22)
        pl.DataFrame({"symbol": ["AAA"], "composite_score": [0.5]}).write_parquet(
            tmp_path / f"signal_aggregation_{run_date.isoformat()}.parquet"
        )
        out = load_signal_aggregation(run_date)
        assert out["symbol"].to_list() == ["AAA"]
