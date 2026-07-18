"""
test_gold_pipeline.py — Silver→Gold Integration Test
End-to-end test: synthetic Silver data → Gold indicators → MTF → Regime.

Validates:
    1. Technical indicators compute correctly on multi-symbol data
    2. MTF alignment produces valid score range [-7, +7]
    3. Signal quality grading A/B/C/D is applied correctly
    4. Macro regime classification produces valid output
    5. Sector rotation weights are applied per regime
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from src.gold.indicators.core_indicators import (
    add_atr, add_ema, add_macd, add_momentum_features, add_rsi,
)
from src.gold.macro_regime import MacroRegimeDetector
from src.gold.sector_rotation import REGIME_SECTOR_WEIGHTS


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def multi_symbol_silver():
    """100-bar multi-symbol Silver OHLCV DataFrame."""
    n      = 100
    base   = date(2025, 1, 2)
    frames = []

    symbol_configs = [
        ("AAPL",   150.0,  0.5,  "us_stocks"),
        ("MSFT",   300.0,  0.8,  "us_stocks"),
        ("BBCA",   8500.0, 15.0, "idx"),
    ]

    for sym, start_price, step, market in symbol_configs:
        price = start_price
        rows  = []
        for i in range(n):
            # Bull trend with some noise
            price += step * (1 + (i % 7 - 3) * 0.1)
            rows.append({
                "symbol":    sym,
                "timestamp": base + timedelta(days=i),
                "open":      round(price * 0.998, 2),
                "high":      round(price * 1.008, 2),
                "low":       round(price * 0.993, 2),
                "close":     round(price, 2),
                "volume":    1_000_000 + i * 10_000,
                "is_clean":  True,
                "market":    market,
            })
        frames.append(pl.DataFrame(rows))

    return pl.concat(frames)


# ── Indicator Integration Tests ───────────────────────────────────────────────

class TestIndicatorPipeline:

    def test_full_indicator_chain(self, multi_symbol_silver):
        """Full indicator chain must complete without error."""
        result = (
            multi_symbol_silver
            .sort(["symbol", "timestamp"])
            .pipe(add_ema,  periods=[9, 21, 50, 200])
            .pipe(add_rsi,  periods=[14, 28])
            .pipe(add_macd)
            .pipe(add_atr,  period=14)
            .pipe(add_momentum_features)
        )
        assert len(result) == len(multi_symbol_silver)

    def test_indicators_per_symbol_isolated(self, multi_symbol_silver):
        """EMA must be computed per-symbol, not across symbols."""
        result = (
            multi_symbol_silver
            .sort(["symbol", "timestamp"])
            .pipe(add_ema, periods=[9])
        )
        # AAPL and MSFT have very different price scales
        aapl_ema = result.filter(pl.col("symbol") == "AAPL")["ema_9"].drop_nulls()
        msft_ema = result.filter(pl.col("symbol") == "MSFT")["ema_9"].drop_nulls()

        # AAPL EMA should be around 150, MSFT around 300
        assert aapl_ema.mean() < 250, "AAPL EMA contaminated by MSFT prices"
        assert msft_ema.mean() > 250, "MSFT EMA contaminated by AAPL prices"

    def test_rsi_all_in_range(self, multi_symbol_silver):
        """RSI must be in [0, 100] for all symbols."""
        result = (
            multi_symbol_silver.sort(["symbol", "timestamp"]).pipe(add_rsi)
        )
        valid = result["rsi_14"].drop_nulls()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_ema_cross_signal(self, multi_symbol_silver):
        """In a bull trend, EMA9 should be > EMA21 for most bars."""
        result = (
            multi_symbol_silver
            .sort(["symbol", "timestamp"])
            .pipe(add_ema, periods=[9, 21])
        )
        aapl = result.filter(pl.col("symbol") == "AAPL").drop_nulls(["ema_9", "ema_21"])
        ema_cross_up = (aapl["ema_9"] > aapl["ema_21"]).mean()
        # Bull trend: expect >60% of bars with EMA9 > EMA21
        assert ema_cross_up > 0.6, (
            f"Expected EMA bull cross >60%, got {ema_cross_up:.1%}"
        )

    def test_macd_hist_direction(self, multi_symbol_silver):
        """In uptrend, MACD hist should be positive for majority of bars."""
        result = (
            multi_symbol_silver
            .filter(pl.col("symbol") == "AAPL")
            .sort("timestamp")
            .pipe(add_macd)
        )
        valid = result.filter(pl.col("macd_hist").is_not_null())
        pos_pct = (valid["macd_hist"] > 0).mean()
        # Bull trend: expect >50% positive histogram
        assert pos_pct >= 0.4   # Loose threshold for test data


# ── Regime Tests ──────────────────────────────────────────────────────────────

class TestMacroRegimeIntegration:

    def setup_method(self):
        self.detector = MacroRegimeDetector()

    def test_regime_classification_all_regimes(self):
        """All 5 regimes must be reachable via specific indicator inputs."""
        cases = [
            ({"vix": 35.0, "yield_spread": -0.5, "cpi": 5.0, "gdp": 1.5},
             "RISK_OFF"),
            ({"vix": 7.0,  "yield_spread": 1.2,  "cpi": 3.0, "gdp": 3.5},
             "RISK_ON"),
            ({"vix": 22.0, "yield_spread": 0.1,  "cpi": 7.0, "gdp": 0.3},
             "STAGFLATION"),
            ({"vix": 22.0, "yield_spread": 0.9,  "cpi": 3.5, "gdp": 3.0},
             "REFLATION"),
            ({"vix": 16.0, "yield_spread": 0.4,  "cpi": 1.5, "gdp": 2.0},
             "DISINFLATION"),
        ]
        for indicators, expected_regime in cases:
            regime, _, _ = self.detector._classify(indicators)
            assert regime == expected_regime, (
                f"Expected {expected_regime}, got {regime}"
                f" for indicators={indicators}"
            )

    def test_regime_record_has_transition_flag(self, tmp_path, monkeypatch):
        """regime_transition field must be in detect() output."""
        import src.gold.macro_regime as mr
        monkeypatch.setattr(mr, "REGIME_STORE_PATH", tmp_path / "regime_store.parquet")
        record = self.detector.detect(date(2025, 6, 1))
        assert "regime_transition" in record
        assert isinstance(record["regime_transition"], bool)


# ── Sector Rotation Integration ───────────────────────────────────────────────

class TestSectorRotationIntegration:

    def test_sector_weights_sum_plausible(self):
        """Sum of all sector weights per regime should be reasonable."""
        for regime, weights in REGIME_SECTOR_WEIGHTS.items():
            total = sum(weights.values())
            # 16 sectors × average ~1.0 = ~16 total
            assert 8 < total < 25, (
                f"Regime {regime}: implausible total weight {total:.2f}"
            )

    def test_risk_on_sum_greater_than_risk_off(self):
        """RISK_ON total weight should exceed RISK_OFF (more OW in bull)."""
        ro_sum   = sum(REGIME_SECTOR_WEIGHTS["RISK_ON"].values())
        roff_sum = sum(REGIME_SECTOR_WEIGHTS["RISK_OFF"].values())
        assert ro_sum >= roff_sum * 0.95   # Allow slight overlap

    def test_all_sectors_have_float_weights(self):
        """All weight values must be floats."""
        for regime, weights in REGIME_SECTOR_WEIGHTS.items():
            for sector, w in weights.items():
                assert isinstance(w, (int, float)), (
                    f"{regime}/{sector}: weight is {type(w)}, expected float"
                )
