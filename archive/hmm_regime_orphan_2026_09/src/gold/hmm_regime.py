"""
hmm_regime.py — GD §8.2 (HMM Regime Detector — v1.3 upgrade path)
Hidden Markov Model regime detection as drop-in replacement for rule-based.

Phase 1 (current): rule-based threshold (macro_regime.py)
Phase 2 (v1.3):    HMM — regime as hidden state, avoids signal churn
                   at borderline zones (composite_score ≈ 0)

Interface contract: same output schema as MacroRegimeDetector._classify()
Returns: (regime_name, scores_dict, confidence_float)

HMM features (GD §8.2):
    - 5 hidden states = 5 regimes
    - Observations: [vix, yield_spread, cpi, gdp, dxy]
    - Covariance: "full" (captures cross-indicator correlations)
    - n_iter: 200 for EM convergence

Usage:
    from src.gold.hmm_regime import HMMRegimeDetector
    detector = HMMRegimeDetector()
    detector.fit(historical_features)            # Train on Silver macro data
    regime, scores, conf = detector.classify(current_features)

Upgrade path (GD §14.5 pattern — same interface, swap implementation):
    # In macro_regime.py, replace:
    from src.gold.macro_regime import MacroRegimeDetector
    # With:
    from src.gold.hmm_regime import HMMRegimeDetector as MacroRegimeDetector
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
from loguru import logger

# Regime label mapping (state index → regime name)
REGIME_LABELS = {
    0: "RISK_ON",
    1: "RISK_OFF",
    2: "STAGFLATION",
    3: "REFLATION",
    4: "DISINFLATION",
}

HMM_MODEL_PATH = Path("data/health/hmm_regime_model.pkl")

# Feature columns (must match Silver macro query order)
FEATURE_COLS = ["vix", "yield_spread", "cpi", "gdp", "dxy"]


class HMMRegimeDetector:
    """
    HMM-based macro regime detector.
    Drop-in replacement for rule-based MacroRegimeDetector._classify().

    GD §8.2: uses hmmlearn GaussianHMM with 5 hidden states.
    Trains on historical Silver macro features.
    Predicts most-likely state + probability vector as confidence.

    Fallback: if model not trained, delegates to rule-based classifier.
    """

    N_STATES: int = 5

    def __init__(self) -> None:
        self._model     = None
        self._scaler    = None   # FIX G-F06: persist alongside _model
        self._is_fitted = False
        self._load_model()

    # ── Training ──────────────────────────────────────────────────────────────

    def fit(self, features: np.ndarray) -> "HMMRegimeDetector":
        """
        Train HMM on historical macro features.
        features: (n_samples, 5) array of [vix, yield_spread, cpi, gdp, dxy]
        """
        try:
            from hmmlearn import hmm  # type: ignore
        except ImportError:
            logger.error(
                "[HMM] hmmlearn not installed. Run: pip install hmmlearn"
            )
            return self

        if features.shape[0] < self.N_STATES * 10:
            logger.warning(
                f"[HMM] Too few samples ({features.shape[0]}) for reliable"
                f" training. Need at least {self.N_STATES * 10}."
            )

        logger.info(
            f"[HMM] Training on {features.shape[0]} samples,"
            f" {self.N_STATES} states..."
        )

        self._model = hmm.GaussianHMM(
            n_components=self.N_STATES,
            covariance_type="full",
            n_iter=200,
            random_state=42,
            verbose=False,
        )

        # FIX G-F06: StandardScaler harus di-fit DAN di-persist bersama model.
        # Tanpa ini, classify() menggunakan skala berbeda dari fit() jika
        # features range berubah — menghasilkan state prediction yang salah.
        try:
            from sklearn.preprocessing import StandardScaler
            self._scaler = StandardScaler()
            features_scaled = self._scaler.fit_transform(features)
        except ImportError:
            logger.warning("[HMM] sklearn not available — training without scaling")
            self._scaler = None
            features_scaled = features

        self._model.fit(features_scaled)
        self._is_fitted = True
        self._save_model()   # FIX G-F06: _save_model() sekarang persist scaler juga

        logger.success(
            f"[HMM] Training complete."
            f" Converged: {self._model.monitor_.converged}"
        )
        return self

    def fit_from_silver(self, run_date=None) -> "HMMRegimeDetector":
        """
        Build features from Silver macro enriched data and train.
        Convenience method for CLI usage.
        """
        features = self._load_features(run_date)
        if features is not None and len(features) > self.N_STATES * 5:
            self.fit(features)
        else:
            logger.warning(
                "[HMM] Insufficient Silver macro data for training."
                " Using rule-based fallback."
            )
        return self

    # ── Inference ─────────────────────────────────────────────────────────────

    def classify(
        self,
        indicators: dict,
    ) -> tuple[str, dict, float]:
        """
        Classify current macro state.
        Same interface as MacroRegimeDetector._classify().

        Returns: (regime_name, scores_dict, confidence_0_to_1)
        """
        if not self._is_fitted or self._model is None:
            logger.debug(
                "[HMM] Model not fitted — delegating to rule-based classifier"
            )
            from src.gold.macro_regime import MacroRegimeDetector
            return MacroRegimeDetector()._classify(indicators)

        feature_vec = self._dict_to_features(indicators)

        try:
            # FIX G-F06: apply SAME scaler used during fit() before predict
            obs = feature_vec.reshape(1, -1)
            if self._scaler is not None:
                try:
                    obs = self._scaler.transform(obs)
                except Exception as scale_err:
                    logger.warning(
                        f"[HMM] Scaler transform failed: {scale_err} — "
                        "using unscaled features. Recommend retraining."
                    )
            # Predict state sequence (single observation)
            state  = self._model.predict(obs)[0]
            proba  = self._model.predict_proba(obs)[0]

            regime     = REGIME_LABELS.get(state, "RISK_ON")
            confidence = float(proba[state])

            # Build scores dict for schema compatibility
            scores = {
                "vix":         1 - indicators.get("vix", 20) / 50,
                "yield_curve": indicators.get("yield_spread", 0) + 0.5,
                "cpi":         1 - indicators.get("cpi", 3) / 8,
                "gdp":         indicators.get("gdp", 2) / 4,
                "dxy":         0.5,
                "composite":   float(proba[state]),
                "state_proba": proba.tolist(),
            }

            logger.debug(
                f"[HMM] State={state} ({regime}), confidence={confidence:.3f},"
                f" proba={[f'{p:.2f}' for p in proba]}"
            )
            return regime, scores, round(confidence, 3)

        except Exception as e:
            logger.warning(f"[HMM] Inference failed: {e} — using rule-based")
            from src.gold.macro_regime import MacroRegimeDetector
            return MacroRegimeDetector()._classify(indicators)

    def predict_sequence(self, features: np.ndarray) -> list[str]:
        """
        Predict regime labels for a sequence of feature vectors.
        Useful for backtesting historical regime assignments.
        """
        if not self._is_fitted or self._model is None:
            return ["RISK_ON"] * len(features)
        states = self._model.predict(features)
        return [REGIME_LABELS.get(s, "RISK_ON") for s in states]

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save_model(self) -> None:
        """Persist trained model AND scaler to disk.

        FIX G-F06: scaler di-save bersama model dalam satu dict.
        Memastikan classify() menggunakan scaling yang sama dengan fit().
        """
        try:
            import pickle
            HMM_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "model":  self._model,
                "scaler": self._scaler,   # FIX G-F06: persist scaler
            }
            with open(HMM_MODEL_PATH, "wb") as f:
                pickle.dump(payload, f)
            logger.debug(f"[HMM] Model+scaler saved → {HMM_MODEL_PATH}")
        except Exception as e:
            logger.warning(f"[HMM] Could not save model: {e}")

    def _load_model(self) -> None:
        """Load persisted model AND scaler from disk if available.

        FIX G-F06: restore scaler — jika payload berupa dict (format baru),
        restore scaler juga. Backward-compat: jika payload bukan dict
        (format lama tanpa scaler), set scaler=None dan log warning.
        """
        if not HMM_MODEL_PATH.exists():
            return
        try:
            import pickle
            with open(HMM_MODEL_PATH, "rb") as f:
                payload = pickle.load(f)
            # FIX G-F06: handle both old (model only) and new (dict) formats
            if isinstance(payload, dict):
                self._model  = payload.get("model")
                self._scaler = payload.get("scaler")   # may be None if old format
                if self._scaler is None:
                    logger.warning(
                        "[HMM] Loaded model has no scaler (old format) — "
                        "classify() will run unscaled. Recommend retraining."
                    )
            else:
                # Legacy format: raw model object
                self._model  = payload
                self._scaler = None
                logger.warning(
                    "[HMM] Loaded legacy model without scaler — "
                    "classify() will run unscaled. Recommend retraining."
                )
            self._is_fitted = True
            logger.info(f"[HMM] Loaded pre-trained model from {HMM_MODEL_PATH}")
        except Exception as e:
            logger.debug(f"[HMM] Could not load model: {e}")

    # ── Feature Engineering ───────────────────────────────────────────────────

    @staticmethod
    def _dict_to_features(indicators: dict) -> np.ndarray:
        """Convert indicators dict to numpy feature vector."""
        return np.array([
            indicators.get("vix",          20.0),
            indicators.get("yield_spread",  0.5),
            indicators.get("cpi",           3.0),
            indicators.get("gdp",           2.0),
            indicators.get("dxy",         100.0),
        ], dtype=float)

    def _load_features(self, run_date=None) -> Optional[np.ndarray]:
        """Load and reshape Silver macro data as training features."""
        import duckdb
        macro_glob = "data/silver/macro_enriched/fred_*_silver.parquet"
        series_map = {
            "vix":         "VIXCLS",
            "yield_spread":"T10Y2Y",
            "cpi":         "CPIAUCSL",
            "gdp":         "A191RL1Q225SBEA",
            "dxy":         "DEXUSEU",
        }
        try:
            con     = duckdb.connect()
            frames  = {}
            for feat, series_id in series_map.items():
                # FIX GLD-003: $name parameterized query — f-string SQL dilarang GD §17.7
                result = con.execute(
                    """
                    SELECT CAST(observation_date AS DATE) AS obs_date, value
                    FROM read_parquet($glob, hive_partitioning=true)
                    WHERE series_id = $series_id
                    ORDER BY obs_date
                    """,
                    {"glob": macro_glob, "series_id": series_id},
                ).pl()
                if not result.is_empty():
                    frames[feat] = dict(
                        zip(result["obs_date"].to_list(),
                            result["value"].to_list())
                    )

            if len(frames) < 3:
                return None

            # Intersect dates
            common_dates = set(frames[list(frames.keys())[0]].keys())
            for f in frames.values():
                common_dates &= set(f.keys())
            common_dates = sorted(common_dates)

            if len(common_dates) < self.N_STATES * 10:
                return None

            features = np.array([
                [frames.get(feat, {}).get(d, 0.0) for feat in FEATURE_COLS]
                for d in common_dates
            ], dtype=float)

            # Normalize: z-score per feature
            mean = features.mean(axis=0)
            std  = features.std(axis=0)
            std[std == 0] = 1.0
            return (features - mean) / std

        except Exception as e:
            logger.warning(f"[HMM] Feature loading failed: {e}")
            return None

    @property
    def is_trained(self) -> bool:
        return self._is_fitted
