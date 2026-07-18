"""tests/unit/test_slippage.py — SlippageModel unit tests"""

import pytest

from src.backtest.slippage import (
    FixedSlippage,
    MarketImpactSlippage,
    SlippagePreset,
    SpreadSlippage,
    VolatilitySlippage,
)


class TestFixedSlippage:

    def test_estimate_5bps(self):
        s      = FixedSlippage(bps=5.0)
        result = s.estimate(100.0, 1.0, 1_000_000, "long", 10)
        assert abs(result - 0.05) < 1e-9   # 100 × 5/10000 = 0.05

    def test_estimate_10bps(self):
        s      = FixedSlippage(bps=10.0)
        result = s.estimate(200.0, 1.5, 5_000_000, "short", 5)
        assert abs(result - 0.20) < 1e-9   # 200 × 10/10000 = 0.20

    def test_total_cost_multiplies_by_qty(self):
        s    = FixedSlippage(bps=5.0)
        unit = s.estimate(100.0, 1.0, 1_000_000, "long", 10)
        total = s.total_cost(100.0, 1.0, 1_000_000, "long", 10)
        assert abs(total - unit * 10) < 1e-9

    def test_positive_slippage(self):
        """Slippage is always a cost (positive)."""
        s = FixedSlippage(bps=5.0)
        assert s.estimate(150.0, 2.0, 1_000_000, "long", 100) > 0

    def test_baseline_preset_is_5bps(self):
        """GD §12.4 baseline: 5 bps per side."""
        assert SlippagePreset.BASELINE.bps == 5.0


class TestSpreadSlippage:

    def test_higher_atr_means_higher_slippage(self):
        s = SpreadSlippage(atr_fraction=0.15)
        low_atr  = s.estimate(100.0, 0.5, 1_000_000, "long", 10)
        high_atr = s.estimate(100.0, 2.0, 1_000_000, "long", 10)
        assert high_atr > low_atr

    def test_zero_atr_uses_fallback(self):
        """Zero ATR falls back to fixed 5 bps."""
        s      = SpreadSlippage()
        result = s.estimate(100.0, 0.0, 1_000_000, "long", 10)
        assert result == pytest.approx(0.05, abs=1e-6)

    def test_spread_is_proportional_to_atr_fraction(self):
        price = 100.0
        atr   = 2.0
        s1    = SpreadSlippage(atr_fraction=0.10)
        s2    = SpreadSlippage(atr_fraction=0.20)
        assert s2.estimate(price, atr, 1_000_000, "long", 10) == pytest.approx(
            s1.estimate(price, atr, 1_000_000, "long", 10) * 2, rel=1e-6
        )


class TestVolatilitySlippage:

    def test_volatile_market_higher_slippage(self):
        s        = VolatilitySlippage(atr_mult=0.10)
        low_vol  = s.estimate(100.0, 0.5, 1_000_000, "long", 10)   # ATR 0.5%
        high_vol = s.estimate(100.0, 3.0, 1_000_000, "long", 10)   # ATR 3%
        assert high_vol > low_vol

    def test_clamped_to_min_bps(self):
        """Very stable markets clamped at min_bps."""
        s      = VolatilitySlippage(atr_mult=0.01, min_bps=2.0, max_bps=50.0)
        result = s.estimate(100.0, 0.001, 1_000_000, "long", 10)  # near-zero ATR
        assert result >= 100.0 * 2.0 / 10_000   # At least min_bps

    def test_clamped_to_max_bps(self):
        """Extremely volatile instruments clamped at max_bps."""
        s      = VolatilitySlippage(atr_mult=10.0, min_bps=2.0, max_bps=50.0)
        result = s.estimate(100.0, 50.0, 1_000_000, "long", 10)  # 50% ATR
        assert result <= 100.0 * 50.0 / 10_000   # At most max_bps

    def test_zero_price_returns_zero(self):
        s      = VolatilitySlippage()
        result = s.estimate(0.0, 1.0, 1_000_000, "long", 10)
        assert result == 0.0


class TestMarketImpactSlippage:

    def test_larger_order_more_impact(self):
        """Bigger position size → more market impact."""
        s    = MarketImpactSlippage(impact_factor=0.5)
        small = s.total_cost(100.0, 1.0, 1_000_000, "long", 100)
        large = s.total_cost(100.0, 1.0, 1_000_000, "long", 10_000)
        assert large > small

    def test_zero_dollar_volume_uses_fallback(self):
        s      = MarketImpactSlippage()
        result = s.estimate(100.0, 1.0, 0, "long", 10)
        assert result == pytest.approx(100.0 * 5 / 10_000, rel=1e-6)

    def test_impact_capped_at_200bps(self):
        """Impact is capped to prevent unrealistic slippage."""
        s      = MarketImpactSlippage(impact_factor=10.0)
        result = s.estimate(100.0, 1.0, 1_000, "long", 100_000)
        assert result <= 100.0 * 200 / 10_000   # Max 200 bps


class TestSlippagePresets:

    def test_all_presets_are_slippage_models(self):
        from src.backtest.slippage import SlippageModel
        presets = [
            SlippagePreset.BASELINE,
            SlippagePreset.CONSERVATIVE,
            SlippagePreset.SPREAD_LIQUID,
            SlippagePreset.SPREAD_MIDCAP,
            SlippagePreset.VOLATILITY_ADJUSTED,
            SlippagePreset.IDX,
        ]
        for preset in presets:
            assert isinstance(preset, SlippageModel)

    def test_conservative_higher_than_baseline(self):
        price  = 100.0
        args   = (price, 1.0, 1_000_000, "long", 10)
        base   = SlippagePreset.BASELINE.estimate(*args)
        cons   = SlippagePreset.CONSERVATIVE.estimate(*args)
        assert cons > base

    def test_idx_spread_wider_than_liquid(self):
        """IDX (emerging market) has wider spread than liquid US."""
        args    = (8500.0, 85.0, 5_000_000_000, "long", 1000)
        liquid  = SlippagePreset.SPREAD_LIQUID.estimate(*args)
        idx     = SlippagePreset.IDX.estimate(*args)
        assert idx > liquid
