"""
tests/unit/test_macro_regime_gld002.py — Test suite untuk GLD-002 fix.

FIX GLD-002: DXY score hardcoded 0.5 → computed dari actual DXY level.

_classify() adalah method dari MacroRegimeDetector.
Returns: (regime_name: str, scores: dict, confidence: float)
  scores keys: vix, yield_curve, cpi, gdp, dxy, composite

Formula post-fix: dxy_score = max(0, min(1, (110 - dxy) / 20))
  DXY = 90  → score = 1.0 (weak dollar = risk-on)
  DXY = 100 → score = 0.5 (neutral)
  DXY = 110 → score = 0.0 (strong dollar = risk-off)
"""
from __future__ import annotations
import pytest
from src.gold.macro_regime import MacroRegimeDetector


def classify(indicators: dict) -> dict:
    """Helper: wrap _classify() return values into a flat dict."""
    det = MacroRegimeDetector()
    regime, scores, confidence = det._classify(indicators)
    return {
        "regime":         regime,
        "confidence":     confidence,
        "dxy_score":      scores.get("dxy", None),
        "composite_score": scores.get("composite", None),
        **{k: v for k, v in scores.items()},
    }


class TestDXYScoreComputation:
    """GLD-002: dxy_score harus dihitung dari actual DXY level."""

    def _base(self, dxy: float) -> dict:
        return {
            "vix": 20.0, "yield_spread": 0.5,
            "cpi": 2.5, "gdp": 2.0, "dxy": dxy,
        }

    def test_neutral_dxy_100_gives_score_050(self):
        """DXY=100 harus menghasilkan dxy_score tepat 0.5."""
        result = classify(self._base(100.0))
        assert result["dxy_score"] == pytest.approx(0.5, abs=1e-3), (
            "GLD-002: DXY=100 harus score=0.5 (neutral)"
        )

    def test_weak_dollar_dxy_90_gives_high_score(self):
        """DXY=90 (weak dollar) → dxy_score = 1.0."""
        result = classify(self._base(90.0))
        assert result["dxy_score"] == pytest.approx(1.0, abs=1e-3)

    def test_strong_dollar_dxy_110_gives_zero_score(self):
        """DXY=110 (strong dollar) → dxy_score = 0.0."""
        result = classify(self._base(110.0))
        assert result["dxy_score"] == pytest.approx(0.0, abs=1e-3)

    def test_very_strong_dollar_capped_at_zero(self):
        """DXY=125 (extreme) → clamped to 0.0."""
        result = classify(self._base(125.0))
        assert result["dxy_score"] == pytest.approx(0.0, abs=1e-3)

    def test_very_weak_dollar_capped_at_one(self):
        """DXY=80 (extreme weak) → clamped to 1.0."""
        result = classify(self._base(80.0))
        assert result["dxy_score"] == pytest.approx(1.0, abs=1e-3)

    def test_dxy_score_varies_not_hardcoded(self):
        """Regression: dxy_score BUKAN hardcoded 0.5 untuk semua input."""
        score_90  = classify(self._base(90.0))["dxy_score"]
        score_110 = classify(self._base(110.0))["dxy_score"]
        assert score_90 != score_110, (
            "GLD-002 REGRESSION: dxy_score identik untuk DXY=90 dan DXY=110 — "
            "kemungkinan kembali ke hardcoded 0.5"
        )

    def test_dxy_score_monoton_decreasing(self):
        """dxy_score harus monoton menurun seiring DXY naik."""
        s90  = classify(self._base(90.0))["dxy_score"]
        s100 = classify(self._base(100.0))["dxy_score"]
        s110 = classify(self._base(110.0))["dxy_score"]
        assert s90 > s100 > s110

    def test_default_dxy_fallback_neutral(self):
        """Jika 'dxy' tidak ada di indicators, default 100.0 → score 0.5."""
        ind = {"vix": 20.0, "yield_spread": 0.5, "cpi": 2.5, "gdp": 2.0}
        result = classify(ind)
        assert result["dxy_score"] == pytest.approx(0.5, abs=1e-3)

    def test_dxy_score_in_output(self):
        """_classify() harus return scores dict dengan key 'dxy'."""
        det = MacroRegimeDetector()
        _, scores, _ = det._classify({"vix": 20.0, "yield_spread": 0.5,
                                       "cpi": 2.5, "gdp": 2.0, "dxy": 100.0})
        assert "dxy" in scores


class TestRegimeCompositeScoreWithDXY:
    """GLD-002: composite_score harus mencerminkan DXY level secara aktual."""

    def test_composite_score_changes_with_dxy(self):
        """composite_score harus berbeda antara DXY=90 dan DXY=110."""
        base = {"vix": 20.0, "yield_spread": 0.5, "cpi": 2.5, "gdp": 2.0}
        score_weak   = classify({**base, "dxy": 90.0})["composite_score"]
        score_strong = classify({**base, "dxy": 110.0})["composite_score"]
        assert score_weak > score_strong, (
            "GLD-002: composite_score dengan DXY=90 harus lebih tinggi dari DXY=110"
        )

    def test_risk_off_detected_with_high_vix_strong_dollar(self):
        """VIX=35 + DXY=115 → RISK_OFF (VIX trigger memastikan ini)."""
        ind = {"vix": 35.0, "yield_spread": -0.3,
               "cpi": 5.5, "gdp": 0.5, "dxy": 115.0}
        result = classify(ind)
        assert result["regime"] == "RISK_OFF"

    def test_risk_on_detected_with_low_vix_weak_dollar(self):
        """VIX=14 + DXY=92 + cpi=3.0 (above disinflation threshold) → RISK_ON."""
        # cpi harus > 2.5 agar tidak trigger DISINFLATION rule terlebih dahulu
        ind = {"vix": 14.0, "yield_spread": 1.2,
               "cpi": 3.0, "gdp": 3.5, "dxy": 92.0}
        result = classify(ind)
        assert result["regime"] == "RISK_ON"
