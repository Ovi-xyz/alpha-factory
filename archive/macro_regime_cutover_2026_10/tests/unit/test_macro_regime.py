"""tests/unit/test_macro_regime.py — Gold MacroRegimeDetector test suite"""

from datetime import date

import pytest

from src.gold.macro_regime import MacroRegimeDetector, compute_regime_transition
import polars as pl


class TestMacroRegimeDetector:

    def setup_method(self):
        self.detector = MacroRegimeDetector()
        self.run_date = date(2025, 6, 1)

    def test_risk_off_high_vix(self):
        """VIX > 30 → RISK_OFF."""
        indicators = {"vix": 38.0, "yield_spread": -0.5, "cpi": 4.5, "gdp": 2.0}
        regime, scores, conf = self.detector._classify(indicators)
        assert regime == "RISK_OFF"
        assert conf > 0

    def test_risk_on_low_vix(self):
        """VIX < 20, positive yield spread → RISK_ON."""
        indicators = {"vix": 15.0, "yield_spread": 0.8, "cpi": 3.0, "gdp": 3.0}
        regime, scores, conf = self.detector._classify(indicators)
        assert regime == "RISK_ON"
        assert conf > 0

    def test_stagflation_high_cpi_low_gdp(self):
        """CPI > 5%, GDP < 1% → STAGFLATION."""
        indicators = {"vix": 22.0, "yield_spread": 0.2, "cpi": 6.5, "gdp": 0.5}
        regime, scores, conf = self.detector._classify(indicators)
        assert regime == "STAGFLATION"

    def test_disinflation_low_cpi(self):
        """CPI < 2.5% → DISINFLATION."""
        indicators = {"vix": 18.0, "yield_spread": 0.4, "cpi": 1.8, "gdp": 2.5}
        regime, scores, conf = self.detector._classify(indicators)
        assert regime == "DISINFLATION"

    def test_reflation_positive_spread_moderate_cpi(self):
        """Yield spread > 0.5, CPI < 4% → REFLATION."""
        indicators = {"vix": 21.0, "yield_spread": 0.9, "cpi": 3.2, "gdp": 3.0}
        regime, scores, conf = self.detector._classify(indicators)
        assert regime == "REFLATION"

    def test_confidence_range(self):
        """Confidence must be in [0, 1]."""
        for indicators in [
            {"vix": 15.0, "yield_spread": 1.0, "cpi": 2.0, "gdp": 3.0},
            {"vix": 45.0, "yield_spread": -1.0, "cpi": 7.0, "gdp": -1.0},
            {"vix": 25.0, "yield_spread": 0.3, "cpi": 3.5, "gdp": 2.0},
        ]:
            _, _, conf = self.detector._classify(indicators)
            assert 0.0 <= conf <= 1.0

    def test_composite_scores_present(self):
        """Score dict must contain all expected keys."""
        indicators = {"vix": 20.0, "yield_spread": 0.5, "cpi": 3.0, "gdp": 2.0}
        _, scores, _ = self.detector._classify(indicators)
        for key in ["vix", "yield_curve", "cpi", "gdp", "composite"]:
            assert key in scores

    def test_detect_returns_full_record(self, tmp_path, monkeypatch):
        """detect() must return all required schema fields."""
        import src.gold.macro_regime as mr
        monkeypatch.setattr(mr, "REGIME_STORE_PATH", tmp_path / "regime_store.parquet")

        record = self.detector.detect(self.run_date)
        required_fields = [
            "date", "regime", "vix_score", "yield_curve_score",
            "cpi_score", "gdp_score", "dxy_score", "composite_score",
            "confidence", "prev_regime", "regime_persistence_days",
            "regime_transition", "transition_alert",
        ]
        for field in required_fields:
            assert field in record, f"Missing field: {field}"

    def test_regime_transition_detection(self):
        """compute_regime_transition should flag True when regime changes."""
        df = pl.DataFrame({
            "regime":      ["RISK_ON", "RISK_ON", "RISK_OFF"],
            "prev_regime": ["RISK_ON", "RISK_ON", "RISK_ON"],
        })
        result = compute_regime_transition(df)
        transitions = result["regime_transition"].to_list()
        assert transitions[0] is False
        assert transitions[1] is False
        assert transitions[2] is True
        assert "RISK_ON -> RISK_OFF" in result["transition_alert"].to_list()[2]


class TestLoadIndicatorsGapFix:
    """
    FIX GAP-1 [P0] (Production Readiness Assessment v1.7.2, GD §5.2.1, §8.1):
    F-MP-01 (v1.7.2) made BLS/BEA Silver output exist, but
    _load_indicators() only ever globbed 'fred_*_silver.parquet' — a glob
    that silently ignores bls_*_silver.parquet / bea_*_silver.parquet.
    These tests build real Silver macro fixtures on disk (monkeypatching
    SILVER_MACRO_PATH to tmp_path) to prove the fix actually reads from
    BLS as a fallback and still prefers FRED when both are present, rather
    than just asserting the glob string changed.
    """

    def setup_method(self):
        self.detector = MacroRegimeDetector()
        self.run_date = date(2025, 6, 15)

    def _write_domain(self, tmp_path, domain: str, rows: dict):
        import polars as pl
        path = tmp_path / f"{domain}_2025-06-01_silver.parquet"
        pl.DataFrame(rows).write_parquet(path)
        return path

    def test_bls_fallback_used_when_fred_absent(self, tmp_path, monkeypatch):
        """CPI must be sourced from BLS native series_id when no FRED file exists."""
        import src.gold.macro_regime as mr_mod

        monkeypatch.setattr(mr_mod, "SILVER_MACRO_PATH", tmp_path)
        self._write_domain(tmp_path, "bls", {
            "series_id":        ["CUUR0000SA0"],
            "observation_date": ["2025-05-01"],
            "value":            [314.5],
        })

        indicators = self.detector._load_indicators(self.run_date)
        assert indicators["cpi"] == 314.5

    def test_fred_takes_priority_over_bls(self, tmp_path, monkeypatch):
        """When both FRED and BLS have CPI data, FRED must win (priority order)."""
        import src.gold.macro_regime as mr_mod

        monkeypatch.setattr(mr_mod, "SILVER_MACRO_PATH", tmp_path)
        self._write_domain(tmp_path, "bls", {
            "series_id":        ["CUUR0000SA0"],
            "observation_date": ["2025-05-01"],
            "value":            [314.5],
        })
        self._write_domain(tmp_path, "fred", {
            "series_id":        ["CPIAUCSL"],
            "observation_date": ["2025-05-01"],
            "value":            [309.1],
        })

        indicators = self.detector._load_indicators(self.run_date)
        assert indicators["cpi"] == 309.1

    def test_gdp_not_aliased_to_bea_native(self, tmp_path, monkeypatch):
        """GDP must stay at neutral default if only BEA native 'real_gdp' exists —
        BEA NIPA rows are multi-LineNumber / mixed-unit (see _load_indicators
        docstring), so no blind BEA alias is wired in."""
        import src.gold.macro_regime as mr_mod

        monkeypatch.setattr(mr_mod, "SILVER_MACRO_PATH", tmp_path)
        self._write_domain(tmp_path, "bea", {
            "series_id":        ["real_gdp", "real_gdp"],
            "observation_date": ["2025-04-01", "2025-04-01"],
            "value":            [2.8, 23_500_000.0],   # mixed-unit rows, by design
        })

        indicators = self.detector._load_indicators(self.run_date)
        assert indicators["gdp"] == 2.0   # neutral default, untouched

    def test_no_silver_macro_data_uses_defaults(self, tmp_path, monkeypatch):
        """No Silver macro files at all -> every indicator stays at its neutral default."""
        import src.gold.macro_regime as mr_mod

        monkeypatch.setattr(mr_mod, "SILVER_MACRO_PATH", tmp_path)
        indicators = self.detector._load_indicators(self.run_date)
        assert indicators == {
            "vix": 20.0, "yield_spread": 0.5, "cpi": 3.0,
            "gdp": 2.0, "dxy": 100.0,
        }

    def test_future_observation_excluded_pit(self, tmp_path, monkeypatch):
        """Observations dated after run_date must not leak into indicators (PIT)."""
        import src.gold.macro_regime as mr_mod

        monkeypatch.setattr(mr_mod, "SILVER_MACRO_PATH", tmp_path)
        self._write_domain(tmp_path, "fred", {
            "series_id":        ["CPIAUCSL"],
            "observation_date": ["2099-01-01"],   # far future
            "value":            [999.0],
        })

        indicators = self.detector._load_indicators(self.run_date)
        assert indicators["cpi"] == 3.0   # default, future row excluded
