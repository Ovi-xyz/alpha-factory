"""tests/unit/test_mtf_alignment.py — MTF alignment unit tests"""

from datetime import date

import polars as pl
import pytest

from src.gold.mtf_alignment import get_mtf_summary


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
