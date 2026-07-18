"""
slippage.py — GD §12.4 (Backtest Slippage Model)
Realistic market impact / slippage estimation for backtest.

GD §12.4 cost model: 0.1% round-trip estimate (adjustable).
This module provides more sophisticated alternatives.

Models available:
    FixedSlippage:      Constant bps on every trade
    SpreadSlippage:     Half-spread based on bid-ask spread proxy (ATR/2)
    VolatilitySlippage: Linear in daily ATR% — wider spread in volatile markets

Usage in BacktestEngine:
    engine = BacktestEngine(config, slippage=SpreadSlippage(bps=5))
    # or
    engine = BacktestEngine(config, slippage=VolatilitySlippage(atr_mult=0.1))
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from loguru import logger


class SlippageModel(ABC):
    """Abstract base for all slippage models."""

    @abstractmethod
    def estimate(
        self,
        price: float,
        atr: float,
        dollar_volume: float,
        direction: str,
        qty: float,
    ) -> float:
        """
        Estimate slippage cost per share.

        Args:
            price:        Current bar close price
            atr:          14-period ATR for the symbol
            dollar_volume: 20-day average dollar volume
            direction:    'long' or 'short'
            qty:          Number of shares

        Returns:
            Slippage cost per share (positive = cost, deducted from PnL)
        """
        ...

    def total_cost(
        self,
        price: float,
        atr: float,
        dollar_volume: float,
        direction: str,
        qty: float,
    ) -> float:
        """Return total slippage cost for the full position."""
        return self.estimate(price, atr, dollar_volume, direction, qty) * qty


class FixedSlippage(SlippageModel):
    """
    Constant basis-points slippage on every trade.
    Simplest model — appropriate when spread data is unavailable.

    Default: 5 bps = 0.05% per side.
    Round-trip cost: 10 bps = 0.1% (GD §12.4 baseline).
    """

    def __init__(self, bps: float = 5.0) -> None:
        self.bps = bps   # basis points per side

    def estimate(self, price, atr, dollar_volume, direction, qty) -> float:
        return price * self.bps / 10_000


class SpreadSlippage(SlippageModel):
    """
    Half-spread slippage based on ATR proxy for bid-ask spread.
    Higher ATR (volatility) → wider spread → more slippage.

    Spread estimate: atr_fraction × ATR / 2
    Conservative for liquid US stocks (ATR fraction ≈ 0.1-0.3).
    """

    def __init__(self, atr_fraction: float = 0.15) -> None:
        """
        Args:
            atr_fraction: Fraction of ATR as spread estimate (0.15 = 15% of ATR)
        """
        self.atr_fraction = atr_fraction

    def estimate(self, price, atr, dollar_volume, direction, qty) -> float:
        if atr <= 0 or price <= 0:
            return price * 5 / 10_000   # Fallback: 5 bps
        spread_estimate = atr * self.atr_fraction
        half_spread     = spread_estimate / 2
        # Normalize to per-share cost
        return half_spread


class VolatilitySlippage(SlippageModel):
    """
    Volatility-adjusted slippage: linear in ATR%.
    More realistic for different volatility regimes.

    High-vol markets (VIX spike, RISK_OFF): wider spreads.
    Low-vol markets (RISK_ON): tighter spreads.

    Slippage = price × atr_mult × (ATR/price)
    """

    def __init__(
        self,
        atr_mult:   float = 0.10,   # 10% of ATR% as slippage
        min_bps:    float = 2.0,    # Minimum 2 bps (very liquid)
        max_bps:    float = 50.0,   # Maximum 50 bps (very illiquid)
    ) -> None:
        self.atr_mult = atr_mult
        self.min_bps  = min_bps
        self.max_bps  = max_bps

    def estimate(self, price, atr, dollar_volume, direction, qty) -> float:
        if price <= 0:
            return 0.0

        atr_pct   = atr / price if atr > 0 else 0.002   # Fallback 0.2%
        raw_bps   = atr_pct * 10_000 * self.atr_mult
        clamped   = max(self.min_bps, min(self.max_bps, raw_bps))
        return price * clamped / 10_000


class MarketImpactSlippage(SlippageModel):
    """
    Market impact model: larger orders move the price more.
    Proportional to sqrt(order_size / avg_daily_volume).

    Appropriate for large positions relative to ADV.
    Negligible for retail-scale trades (< 0.1% of ADV).
    """

    def __init__(self, impact_factor: float = 0.5) -> None:
        """
        Args:
            impact_factor: Scaling constant (0.5 is a common empirical value)
        """
        self.impact_factor = impact_factor

    def estimate(self, price, atr, dollar_volume, direction, qty) -> float:
        if dollar_volume <= 0 or price <= 0:
            return price * 5 / 10_000   # Fallback

        order_value    = price * qty
        participation  = order_value / dollar_volume   # fraction of ADV
        impact_bps     = self.impact_factor * (participation ** 0.5) * 10_000
        clamped        = min(impact_bps, 200)   # Cap at 200 bps
        return price * clamped / 10_000


# ── Default / Named Presets ───────────────────────────────────────────────────

class SlippagePreset:
    """Named presets for common backtest scenarios."""

    # GD §12.4 baseline: 0.1% round-trip = 5 bps per side
    BASELINE    = FixedSlippage(bps=5.0)

    # Conservative: 10 bps per side (includes wider spreads / impact)
    CONSERVATIVE = FixedSlippage(bps=10.0)

    # Spread-based: realistic for liquid US large-caps
    SPREAD_LIQUID = SpreadSlippage(atr_fraction=0.10)

    # Spread-based: for small/mid-caps with wider spreads
    SPREAD_MIDCAP = SpreadSlippage(atr_fraction=0.25)

    # Volatility-adjusted: adapts to market regime
    VOLATILITY_ADJUSTED = VolatilitySlippage(atr_mult=0.10)

    # IDX stocks: wider spreads (emerging market liquidity)
    IDX = SpreadSlippage(atr_fraction=0.30)
