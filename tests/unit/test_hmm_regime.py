"""tests/unit/test_hmm_regime.py — HMMRegimeDetector unit tests"""

import numpy as np
import pytest

from src.gold.hmm_regime import (
    FEATURE_COLS,
    REGIME_LABELS,
    HMMRegimeDetector,
)


class TestHMMRegimeDetector:

    def test_regime_labels_complete(self):
        """All 5 regimes must be represented."""
        regimes = set(REGIME_LABELS.values())
        expected = {"RISK_ON", "RISK_OFF", "STAGFLATION", "REFLATION", "DISINFLATION"}
        assert regimes == expected

    def test_feature_cols_count(self):
        """Must have exactly 5 feature columns."""
        assert len(FEATURE_COLS) == 5

    def test_unfitted_falls_back_to_rule_based(self):
        """Unfitted HMM delegates to rule-based MacroRegimeDetector."""
        detector = HMMRegimeDetector()
        # Don't load any pre-trained model
        detector._is_fitted = False
        detector._model     = None

        indicators = {"vix": 35.0, "yield_spread": -0.5, "cpi": 5.0, "gdp": 1.5}
        regime, scores, confidence = detector.classify(indicators)

        # Should return valid regime from rule-based fallback
        assert regime in REGIME_LABELS.values()
        assert 0.0 <= confidence <= 1.0
        assert "composite" in scores

    def test_dict_to_features_shape(self):
        """Feature vector must have shape (5,)."""
        indicators = {
            "vix": 20.0, "yield_spread": 0.5,
            "cpi": 3.0, "gdp": 2.5, "dxy": 100.0,
        }
        features = HMMRegimeDetector._dict_to_features(indicators)
        assert features.shape == (5,)
        assert features.dtype == float

    def test_dict_to_features_uses_defaults(self):
        """Missing indicators use sensible defaults."""
        features = HMMRegimeDetector._dict_to_features({})
        assert features.shape == (5,)
        # VIX default 20, yield_spread 0.5, CPI 3.0, GDP 2.0, DXY 100.0
        assert features[0] == 20.0    # vix

    def test_n_states_matches_labels(self):
        """N_STATES must match number of regime labels."""
        assert HMMRegimeDetector.N_STATES == len(REGIME_LABELS)

    def test_fit_with_synthetic_data(self):
        """HMM fits without raising on synthetic data."""
        try:
            from hmmlearn import hmm  # noqa
        except ImportError:
            pytest.skip("hmmlearn not installed — skipping HMM fit test")

        detector = HMMRegimeDetector()
        # Generate sufficient synthetic data (5 states × 20 samples each)
        np.random.seed(42)
        n_samples = 200
        features  = np.random.randn(n_samples, 5)

        # Should fit without raising
        detector.fit(features)
        assert detector.is_trained

    def test_predict_sequence_without_model(self):
        """predict_sequence returns safe defaults when unfitted."""
        detector = HMMRegimeDetector()
        detector._is_fitted = False
        detector._model     = None

        features = np.random.randn(10, 5)
        result   = detector.predict_sequence(features)
        assert len(result) == 10
        assert all(r == "RISK_ON" for r in result)

    def test_classify_returns_valid_tuple(self):
        """classify() always returns (str, dict, float)."""
        detector = HMMRegimeDetector()
        detector._is_fitted = False

        indicators = {"vix": 22.0, "yield_spread": 0.3, "cpi": 3.5, "gdp": 2.0}
        regime, scores, confidence = detector.classify(indicators)

        assert isinstance(regime, str)
        assert isinstance(scores, dict)
        assert isinstance(confidence, float)
        assert regime in REGIME_LABELS.values()
        assert 0.0 <= confidence <= 1.0


class TestHMMVsRuleBased:
    """Verify HMM fallback produces same classifications as rule-based for clear cases."""

    CLEAR_CASES = [
        ({"vix": 38.0, "yield_spread": -0.5, "cpi": 5.0, "gdp": 1.5}, "RISK_OFF"),
        ({"vix": 15.0, "yield_spread": 0.8,  "cpi": 3.0, "gdp": 3.5}, "RISK_ON"),    # CPI=3.0 > 2.5 → not DISINFLATION
        ({"vix": 22.0, "yield_spread": 0.2,  "cpi": 7.0, "gdp": 0.3}, "STAGFLATION"),
        ({"vix": 16.0, "yield_spread": 0.4,  "cpi": 1.5, "gdp": 2.0}, "DISINFLATION"),
    ]

    def test_fallback_matches_rule_based(self):
        """Unfitted HMM fallback must match rule-based for unambiguous cases."""
        from src.gold.macro_regime import MacroRegimeDetector

        hmm_det  = HMMRegimeDetector()
        hmm_det._is_fitted = False
        hmm_det._model     = None
        rule_det = MacroRegimeDetector()

        for indicators, expected in self.CLEAR_CASES:
            hmm_regime,  _, _ = hmm_det.classify(indicators)
            rule_regime, _, _ = rule_det._classify(indicators)
            assert hmm_regime == rule_regime, (
                f"For {indicators}: HMM={hmm_regime}, Rule={rule_regime}"
            )
            assert hmm_regime == expected, (
                f"Expected {expected}, got {hmm_regime}"
            )
