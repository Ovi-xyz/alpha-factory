"""tests/unit/test_mtf_alignment.py — MTF alignment unit tests"""

from datetime import date
from unittest.mock import patch

import polars as pl
import pytest

import src.gold.mtf_alignment as mtf_mod
from src.gold.mtf_alignment import (
    _apply_regime_compatible,
    _compute_mtf_alignment,
    get_mtf_summary,
    run,
)
from src.utils.progress_checkpoint import ProgressCheckpoint


class TestMTFSummaryHelper:

    def test_summary_returns_empty_when_no_file(self):
        """get_mtf_summary returns {} when no file exists."""
        result = get_mtf_summary(date(2099, 1, 1))   # Far future = no file
        assert result == {}


class TestMTFScoreLogic:
    """Test MTF score and quality grading logic in isolation."""

    @staticmethod
    def _grade(score: int) -> str:
        if abs(score) >= 6:
            return "A"
        if abs(score) == 5:
            return "B"
        if abs(score) == 4:
            return "C"
        return "D"

    def test_score_plus_7_is_grade_a(self):
        assert self._grade(7) == "A"

    def test_score_minus_7_is_grade_a(self):
        assert self._grade(-7) == "A"

    def test_score_5_is_grade_b(self):
        assert self._grade(5)  == "B"
        assert self._grade(-5) == "B"

    def test_score_4_is_grade_c(self):
        assert self._grade(4)  == "C"
        assert self._grade(-4) == "C"

    def test_score_3_is_grade_d(self):
        assert self._grade(3)  == "D"
        assert self._grade(0)  == "D"
        assert self._grade(-3) == "D"

    def test_score_range_is_minus7_to_plus7(self):
        """Valid MTF scores are -7 to +7 (7 timeframes, each ±1 or 0)."""
        for score in range(-7, 8):
            grade = self._grade(score)
            assert grade in {"A", "B", "C", "D"}, \
                f"Score {score} produced invalid grade {grade}"

    def test_grade_a_threshold(self):
        """Grade A requires |score| >= 6."""
        assert self._grade(6)  == "A"
        assert self._grade(-6) == "A"
        assert self._grade(5)  != "A"

    def test_polars_grade_expression(self):
        """Verify grade assignment via Polars expression."""
        df = pl.DataFrame({"mtf_score": list(range(-7, 8))})
        df = df.with_columns([
            pl.when(pl.col("mtf_score").abs() >= 6).then(pl.lit("A"))
              .when(pl.col("mtf_score").abs() == 5).then(pl.lit("B"))
              .when(pl.col("mtf_score").abs() == 4).then(pl.lit("C"))
              .otherwise(pl.lit("D"))
              .alias("signal_quality")
        ])

        for row in df.iter_rows(named=True):
            expected = self._grade(row["mtf_score"])
            assert row["signal_quality"] == expected, \
                f"Score {row['mtf_score']}: expected {expected}, got {row['signal_quality']}"


class TestRegiomeCompatible:
    """Test regime_compatible logic from _apply_regime_compatible."""

    def _apply_mock(self, df: pl.DataFrame, regime: str) -> pl.DataFrame:
        """Replicate _apply_regime_compatible logic without file I/O."""
        if regime == "RISK_ON":
            compatible_expr = pl.col("mtf_score") > 0
        elif regime == "RISK_OFF":
            compatible_expr = pl.col("mtf_score") < 0
        else:
            compatible_expr = pl.lit(True)

        return df.with_columns([
            compatible_expr.alias("regime_compatible"),
            pl.lit(regime).alias("active_regime"),
        ])

    def test_risk_on_positive_score_compatible(self):
        df = pl.DataFrame({"mtf_score": [5, -5, 0]})
        result = self._apply_mock(df, "RISK_ON")
        compatible = result["regime_compatible"].to_list()
        assert compatible == [True, False, False]

    def test_risk_off_negative_score_compatible(self):
        df = pl.DataFrame({"mtf_score": [5, -5, 0]})
        result = self._apply_mock(df, "RISK_OFF")
        compatible = result["regime_compatible"].to_list()
        assert compatible == [False, True, False]

    def test_stagflation_all_compatible(self):
        """Non risk-on/off regimes: all symbols compatible."""
        df = pl.DataFrame({"mtf_score": [7, -7, 3, 0]})
        result = self._apply_mock(df, "STAGFLATION")
        assert result["regime_compatible"].all()

    def test_reflation_all_compatible(self):
        df = pl.DataFrame({"mtf_score": [4, -4, 1]})
        result = self._apply_mock(df, "REFLATION")
        assert result["regime_compatible"].all()

    def test_active_regime_column_added(self):
        df = pl.DataFrame({"mtf_score": [5]})
        result = self._apply_mock(df, "RISK_ON")
        assert "active_regime" in result.columns
        assert result["active_regime"].to_list()[0] == "RISK_ON"


# ─────────────────────────────────────────────────────────────────────────
# Decision C (GMI_Decision_Document_v5.docx §3, tranche item #1) —
# real-function coverage. Everything above tests grading/regime logic in
# isolation via hand-duplicated copies (_grade, _apply_mock) that never
# call the actual module code. The classes below exercise the REAL
# _compute_mtf_alignment(), _apply_regime_compatible(), run(), and
# get_mtf_summary() against fixture Parquet files, following the
# monkeypatch/tmp_path conventions established in test_macro_regime.py and
# test_technical_signals.py.
# ─────────────────────────────────────────────────────────────────────────


def _row(
    symbol: str,
    close: float,
    ema_9: float,
    ema_21: float,
    ema_50: float,
    atr_14: float = 2.0,
    rsi_14: float = 50.0,
    macd_hist: float = 0.0,
    signal_date: str = "2026-06-01",
    timestamp: date | None = None,
) -> dict:
    """One tech_signals_{TF}.parquet row — only the 9 columns
    _compute_mtf_alignment's SQL actually SELECTs (GD §5.2.2 defines a much
    wider schema; the extra columns are irrelevant to this query)."""
    return {
        "symbol": symbol,
        "timestamp": timestamp or date(2026, 6, 1),
        "close": close,
        "ema_9": ema_9,
        "ema_21": ema_21,
        "ema_50": ema_50,
        "atr_14": atr_14,
        "rsi_14": rsi_14,
        "macd_hist": macd_hist,
        "signal_date": signal_date,
    }


def _write_tech_signals(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


class TestComputeMtfAlignmentNoData:

    def test_no_signal_files_returns_empty_dataframe(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", tmp_path / "signals")
        result = _compute_mtf_alignment(date(2026, 6, 1))
        assert result.is_empty()


class TestComputeMtfAlignmentScoring:
    """mtf_score / signal_quality computed from real per-TF DuckDB queries."""

    def test_single_tf_bullish_score_1_grade_d(self, tmp_path, monkeypatch):
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        _write_tech_signals(
            sig_dir / "tech_signals_1D.parquet",
            [_row("AAPL", close=110.0, ema_9=12.0, ema_21=10.0, ema_50=100.0)],
        )
        result = _compute_mtf_alignment(date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["mtf_score"] == 1
        assert row["signal_quality"] == "D"
        assert row["5m"] == 0   # TFs with no file fill neutral, not null

    def test_all_seven_tf_bullish_score_7_grade_a(self, tmp_path, monkeypatch):
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        for tf in mtf_mod.TIMEFRAMES:
            _write_tech_signals(
                sig_dir / f"tech_signals_{tf}.parquet",
                [_row("AAPL", close=110.0, ema_9=12.0, ema_21=10.0, ema_50=100.0)],
            )
        result = _compute_mtf_alignment(date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["mtf_score"] == 7
        assert row["signal_quality"] == "A"

    def test_five_bullish_two_neutral_score_5_grade_b(self, tmp_path, monkeypatch):
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        for tf in ["5m", "15m", "1H", "4H", "1D"]:
            _write_tech_signals(
                sig_dir / f"tech_signals_{tf}.parquet",
                [_row("AAPL", close=110.0, ema_9=12.0, ema_21=10.0, ema_50=100.0)],
            )
        for tf in ["1W", "1M"]:
            # close == ema_50 -> neither CASE branch fires -> neutral (0)
            _write_tech_signals(
                sig_dir / f"tech_signals_{tf}.parquet",
                [_row("AAPL", close=100.0, ema_9=12.0, ema_21=10.0, ema_50=100.0)],
            )
        result = _compute_mtf_alignment(date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["mtf_score"] == 5
        assert row["signal_quality"] == "B"

    def test_four_bearish_three_neutral_score_minus4_grade_c(self, tmp_path, monkeypatch):
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        for tf in ["5m", "15m", "1H", "4H"]:
            _write_tech_signals(
                sig_dir / f"tech_signals_{tf}.parquet",
                [_row("AAPL", close=90.0, ema_9=8.0, ema_21=10.0, ema_50=100.0)],
            )
        for tf in ["1D", "1W", "1M"]:
            _write_tech_signals(
                sig_dir / f"tech_signals_{tf}.parquet",
                [_row("AAPL", close=100.0, ema_9=8.0, ema_21=10.0, ema_50=100.0)],
            )
        result = _compute_mtf_alignment(date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["mtf_score"] == -4
        assert row["signal_quality"] == "C"

    def test_multiple_symbols_scored_independently(self, tmp_path, monkeypatch):
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        _write_tech_signals(
            sig_dir / "tech_signals_1D.parquet",
            [
                _row("AAPL", close=110.0, ema_9=12.0, ema_21=10.0, ema_50=100.0),  # bullish
                _row("TSLA", close=90.0,  ema_9=8.0,  ema_21=10.0, ema_50=100.0),  # bearish
            ],
        )
        result = _compute_mtf_alignment(date(2026, 6, 1))
        scores = dict(zip(result["symbol"].to_list(), result["mtf_score"].to_list()))
        assert scores == {"AAPL": 1, "TSLA": -1}

    def test_most_recent_bar_used_not_stale_bar(self, tmp_path, monkeypatch):
        """ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC)
        must select the latest bar per symbol, not whichever row happens
        to be written first in the file."""
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        stale = _row("AAPL", close=90.0, ema_9=8.0, ema_21=10.0, ema_50=100.0,
                      timestamp=date(2026, 5, 1))    # bearish, old
        fresh = _row("AAPL", close=110.0, ema_9=12.0, ema_21=10.0, ema_50=100.0,
                      timestamp=date(2026, 6, 1))    # bullish, new
        _write_tech_signals(sig_dir / "tech_signals_1D.parquet", [stale, fresh])
        result = _compute_mtf_alignment(date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["mtf_score"] == 1

    def test_one_malformed_tf_file_skipped_others_still_scored(self, tmp_path, monkeypatch):
        """A single corrupt/unreadable tech_signals_{TF}.parquet must be
        caught and skipped (treated as neutral/absent) — it must not take
        down the whole computation for TFs whose files are fine."""
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        sig_dir.mkdir(parents=True, exist_ok=True)
        (sig_dir / "tech_signals_15m.parquet").write_text("not a real parquet file")
        _write_tech_signals(
            sig_dir / "tech_signals_1D.parquet",
            [_row("AAPL", close=110.0, ema_9=12.0, ema_21=10.0, ema_50=100.0)],
        )
        result = _compute_mtf_alignment(date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["mtf_score"] == 1   # 1D contributes +1; 15m skipped -> 0
        assert row["15m"] == 0


class TestComputeMtfAlignmentEntryStopZones:
    """entry_zone_low/high, stop_zone_1H, reward_risk_ratio — computed from
    1H ATR with a documented fallback when 1H signals are unavailable."""

    def test_1h_atr_drives_entry_stop_and_rrr(self, tmp_path, monkeypatch):
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        _write_tech_signals(
            sig_dir / "tech_signals_1H.parquet",
            [_row("AAPL", close=100.0, ema_9=12.0, ema_21=10.0, ema_50=90.0, atr_14=4.0)],
        )
        result = _compute_mtf_alignment(date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["entry_zone_low"]  == pytest.approx(98.0)
        assert row["entry_zone_high"] == pytest.approx(102.0)
        assert row["stop_zone_1H"]    == pytest.approx(94.0)
        assert row["reward_risk_ratio"] == pytest.approx(1.2)

    def test_reward_risk_ratio_is_atr_invariant(self, tmp_path, monkeypatch):
        """OBSERVATION, flagged not fixed (out of Decision C's test-only
        scope — see thread report): (1.5*atr)/(1.25*atr) algebraically
        cancels to a constant 1.2 for ANY atr > 0, so reward_risk_ratio
        currently carries no symbol- or volatility-specific information
        despite its name and the per-row computation. This test locks in
        and documents the CURRENT behavior as a regression guard; it is
        not an endorsement that the formula is doing what its name implies."""
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        _write_tech_signals(
            sig_dir / "tech_signals_1H.parquet",
            [_row("AAPL", close=50.0, ema_9=12.0, ema_21=10.0, ema_50=40.0, atr_14=25.0)],
        )
        result = _compute_mtf_alignment(date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["reward_risk_ratio"] == pytest.approx(1.2)

    def test_fallback_to_non_1h_atr_when_1h_absent(self, tmp_path, monkeypatch):
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        _write_tech_signals(
            sig_dir / "tech_signals_4H.parquet",
            [_row("AAPL", close=100.0, ema_9=12.0, ema_21=10.0, ema_50=90.0, atr_14=3.0)],
        )
        result = _compute_mtf_alignment(date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["stop_zone_1H"] == pytest.approx(100.0 - 1.5 * 3.0)
        assert row["reward_risk_ratio"] == pytest.approx(1.2)


class TestApplyRegimeCompatibleReal:
    """Exercises the REAL _apply_regime_compatible — including its file
    I/O, staleness ordering, and exception-swallowing branches — as
    opposed to TestRegiomeCompatible above, which only ever calls a
    hand-duplicated copy of the branching logic."""

    @staticmethod
    def _write_regime_store(path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(rows).write_parquet(path)

    def test_risk_on_from_real_regime_file(self, tmp_path, monkeypatch):
        store = tmp_path / "regime_store.parquet"
        monkeypatch.setattr(mtf_mod, "REGIME_STORE_PATH", store)
        self._write_regime_store(store, [{"date": date(2026, 5, 30), "regime": "RISK_ON"}])
        df = pl.DataFrame({"mtf_score": [5, -5, 0]})
        result = _apply_regime_compatible(df, date(2026, 6, 1))
        assert result["active_regime"].to_list() == ["RISK_ON"] * 3
        assert result["regime_compatible"].to_list() == [True, False, False]

    def test_risk_off_from_real_regime_file(self, tmp_path, monkeypatch):
        store = tmp_path / "regime_store.parquet"
        monkeypatch.setattr(mtf_mod, "REGIME_STORE_PATH", store)
        self._write_regime_store(store, [{"date": date(2026, 5, 30), "regime": "RISK_OFF"}])
        df = pl.DataFrame({"mtf_score": [5, -5, 0]})
        result = _apply_regime_compatible(df, date(2026, 6, 1))
        assert result["regime_compatible"].to_list() == [False, True, False]

    def test_most_recent_regime_row_selected(self, tmp_path, monkeypatch):
        """ORDER BY date DESC LIMIT 1 must pick the latest row <= run_date."""
        store = tmp_path / "regime_store.parquet"
        monkeypatch.setattr(mtf_mod, "REGIME_STORE_PATH", store)
        self._write_regime_store(store, [
            {"date": date(2026, 5, 1),  "regime": "RISK_OFF"},
            {"date": date(2026, 5, 30), "regime": "RISK_ON"},
        ])
        df = pl.DataFrame({"mtf_score": [1]})
        result = _apply_regime_compatible(df, date(2026, 6, 1))
        assert result["active_regime"].to_list() == ["RISK_ON"]

    def test_missing_file_defaults_neutral_all_compatible(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mtf_mod, "REGIME_STORE_PATH", tmp_path / "nonexistent.parquet")
        df = pl.DataFrame({"mtf_score": [7, -7, 0]})
        result = _apply_regime_compatible(df, date(2026, 6, 1))
        assert result["active_regime"].to_list() == ["NEUTRAL"] * 3
        assert result["regime_compatible"].all()

    def test_no_row_before_run_date_defaults_neutral(self, tmp_path, monkeypatch):
        """Regime rows exist but all postdate run_date — the query matches
        nothing, fetchone() is None, must fall back to NEUTRAL not raise."""
        store = tmp_path / "regime_store.parquet"
        monkeypatch.setattr(mtf_mod, "REGIME_STORE_PATH", store)
        self._write_regime_store(store, [{"date": date(2026, 7, 1), "regime": "RISK_ON"}])
        df = pl.DataFrame({"mtf_score": [3]})
        result = _apply_regime_compatible(df, date(2026, 6, 1))
        assert result["active_regime"].to_list() == ["NEUTRAL"]

    def test_corrupt_regime_file_swallows_exception_defaults_neutral(self, tmp_path, monkeypatch):
        """A malformed file at REGIME_STORE_PATH must degrade gracefully —
        regime join is optional context for MTF alignment, not a hard
        dependency, so the broad except/pass here is intentional."""
        store = tmp_path / "regime_store.parquet"
        store.parent.mkdir(parents=True, exist_ok=True)
        store.write_text("not a parquet file")
        monkeypatch.setattr(mtf_mod, "REGIME_STORE_PATH", store)
        df = pl.DataFrame({"mtf_score": [2]})
        result = _apply_regime_compatible(df, date(2026, 6, 1))
        assert result["active_regime"].to_list() == ["NEUTRAL"]


class TestRunIntegration:
    """Full run() lifecycle: happy path, checkpoint-skip, no-data early
    return, and the write-failure → mark_failed → re-raise path."""

    @staticmethod
    def _setup(tmp_path, monkeypatch):
        sig_dir = tmp_path / "signals"
        mtf_dir = tmp_path / "mtf"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        monkeypatch.setattr(mtf_mod, "GOLD_MTF_PATH", mtf_dir)
        monkeypatch.setattr(mtf_mod, "REGIME_STORE_PATH", tmp_path / "no_regime.parquet")
        monkeypatch.setattr(ProgressCheckpoint, "DB_PATH", tmp_path / "health" / "progress.db")
        return sig_dir, mtf_dir

    def test_happy_path_writes_output_and_marks_done(self, tmp_path, monkeypatch):
        sig_dir, mtf_dir = self._setup(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_tech_signals(
            sig_dir / "tech_signals_1D.parquet",
            [_row("AAPL", close=110.0, ema_9=12.0, ema_21=10.0, ema_50=100.0)],
        )
        run(run_date)
        out = mtf_dir / f"mtf_alignment_{run_date.isoformat()}.parquet"
        assert out.exists()
        written = pl.read_parquet(out)
        assert written.filter(pl.col("symbol") == "AAPL")["mtf_score"].to_list() == [1]
        assert ProgressCheckpoint("gold_mtf", run_date).is_done("ALL") is True

    def test_already_done_skips_recompute(self, tmp_path, monkeypatch):
        sig_dir, mtf_dir = self._setup(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_tech_signals(
            sig_dir / "tech_signals_1D.parquet",
            [_row("AAPL", close=110.0, ema_9=12.0, ema_21=10.0, ema_50=100.0)],
        )
        run(run_date)   # first call — computes and marks done
        with patch(
            "src.gold.mtf_alignment._compute_mtf_alignment",
            wraps=mtf_mod._compute_mtf_alignment,
        ) as spy:
            run(run_date)   # second call — must skip, not recompute
            spy.assert_not_called()

    def test_no_signal_data_returns_without_writing_or_marking_done(self, tmp_path, monkeypatch):
        sig_dir, mtf_dir = self._setup(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        # sig_dir intentionally left empty
        run(run_date)
        out = mtf_dir / f"mtf_alignment_{run_date.isoformat()}.parquet"
        assert not out.exists()
        # Not marked done -> a later run once data lands is free to proceed
        assert ProgressCheckpoint("gold_mtf", run_date).is_done("ALL") is False

    def test_write_failure_marks_failed_and_reraises(self, tmp_path, monkeypatch):
        sig_dir, mtf_dir = self._setup(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_tech_signals(
            sig_dir / "tech_signals_1D.parquet",
            [_row("AAPL", close=110.0, ema_9=12.0, ema_21=10.0, ema_50=100.0)],
        )
        # Block GOLD_MTF_PATH.mkdir(): a plain FILE sits where a directory
        # is expected, so mkdir(parents=True, exist_ok=True) raises —
        # exist_ok only suppresses FileExistsError when the existing path
        # IS already a directory.
        blocker = tmp_path / "blocked_mtf_dir"
        blocker.write_text("i am a file, not a directory")
        monkeypatch.setattr(mtf_mod, "GOLD_MTF_PATH", blocker)

        with pytest.raises((FileExistsError, NotADirectoryError)):
            run(run_date)

        failed = ProgressCheckpoint("gold_mtf", run_date).failed_report()
        assert len(failed) == 1
        assert failed[0]["symbol"] == "ALL"


class TestGetMtfSummaryFullPath:

    def test_summary_counts_grades_from_written_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mtf_mod, "GOLD_MTF_PATH", tmp_path)
        run_date = date(2026, 6, 1)
        path = tmp_path / f"mtf_alignment_{run_date.isoformat()}.parquet"
        pl.DataFrame({
            "symbol":         ["A", "B", "C", "D"],
            "signal_quality": ["A", "A", "B", "D"],
        }).write_parquet(path)
        summary = get_mtf_summary(run_date)
        assert summary == {
            "total_symbols": 4, "grade_A": 2, "grade_B": 1, "grade_C": 0, "grade_D": 1,
        }


class TestNoAtrColumnAnywhere:
    """Coverage tranche (17 Aug 2026) — the atr_1h_df is None branch (both
    the primary 1H lookup and the fallback-across-all-TFs lookup come up
    empty). The real DuckDB query explicitly SELECTs atr_14 by name, so a
    successful query always yields that column — this branch is only
    reachable if atr_14 is genuinely absent from every per-TF result,
    which we construct by mocking the DuckDB connection layer directly
    rather than writing malformed fixture Parquet (which would just raise
    inside the query's own try/except and be skipped before reaching
    tf_dfs at all)."""

    def test_missing_atr_everywhere_fills_null_zone_columns(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock, patch
        sig_dir = tmp_path / "signals"
        monkeypatch.setattr(mtf_mod, "GOLD_SIGNALS_PATH", sig_dir)
        _write_tech_signals(
            sig_dir / "tech_signals_1D.parquet",
            [_row("AAPL", close=110.0, ema_9=12.0, ema_21=10.0, ema_50=100.0)],
        )
        no_atr_df = pl.DataFrame({
            "symbol": ["AAPL"], "timeframe": ["1D"], "trend_dir": [1],
            "rsi_14": [50.0], "macd_hist": [0.0], "close": [110.0],
            "signal_date": ["2026-06-01"],
        })   # deliberately missing 'atr_14'
        mock_con = MagicMock()
        mock_con.execute.return_value.pl.return_value = no_atr_df
        with patch("duckdb.connect", return_value=mock_con):
            result = _compute_mtf_alignment(date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["reward_risk_ratio"] is None
        assert row["entry_zone_low"] is None
