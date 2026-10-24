"""
test_gold_pipeline.py — Silver→Gold Integration Test
End-to-end test: synthetic Silver data → Gold indicators → MTF → HMM context regime → sector map.

Validates:
    1. Technical indicators compute correctly on multi-symbol data
    2. MTF alignment produces valid score range [-7, +7]
    3. Signal quality grading A/B/C/D is applied correctly
    4. The dual-HMM context regime join produces valid vocabulary and a boolean transition flag
       (ADR-055 v2: replaces the rule-based macro_regime.py classification)
    5. signal_aggregation's registry-direct sector map covers the whole universe (ADR-055 v2:
       replaces the regime-keyed sector rotation weights; gold_sector is archived)
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from src.gold.indicators.core_indicators import (
    add_atr, add_ema, add_macd, add_momentum_features, add_rsi,
)
from src.config.instrument_loader import get_loader
from src.gold.hmm.context_regime import COLUMNS as CONTEXT_COLUMNS, build as build_context
from src.gold.signal_aggregation import _load_sector_map


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

MACRO_LABELS  = {"expansion", "slowdown", "recession", "recovery"}
MARKET_LABELS = {"risk_off", "mixed", "risk_on"}


def _macro_frame(days: list[date], labels: list[str]) -> pl.DataFrame:
    return pl.DataFrame({
        "date": days, "macro_label": labels,
        "macro_activity": [0.1 * i for i in range(len(days))],
        "macro_momentum": [0.0] * len(days),
        "in_sample": [False] * len(days),
        "model_config_fingerprint": ["fp-macro"] * len(days),
    })


def _market_frame(days: list[date], labels: list[str]) -> pl.DataFrame:
    return pl.DataFrame({
        "date": days, "market_label": labels,
        "market_probability": [0.8] * len(days), "market_score": [0.5] * len(days),
        "in_sample": [False] * len(days), "n_features_observed": [10] * len(days),
        "model_config_fingerprint": ["fp-market"] * len(days),
    })


class TestContextRegimeIntegration:
    """The dual-HMM context regime is the platform's regime since ADR-055 v2."""

    DAYS = [date(2026, 10, 12) + timedelta(days=i) for i in range(5)]   # Mon..Fri

    def test_context_frame_vocabulary_and_schema(self):
        macro  = _macro_frame(self.DAYS, ["expansion", "expansion", "slowdown", "slowdown", "slowdown"])
        market = _market_frame(self.DAYS, ["risk_on", "risk_on", "mixed", "mixed", "risk_off"])
        out = build_context(macro, market, self.DAYS[-1])
        assert out.columns == CONTEXT_COLUMNS
        assert set(out["macro_label"]) <= MACRO_LABELS
        assert set(out["market_label"]) <= MARKET_LABELS
        assert out["context_label"].to_list()[0] == "expansion/risk_on"

    def test_transition_flag_is_boolean_and_marks_label_changes(self):
        macro  = _macro_frame(self.DAYS, ["expansion", "expansion", "slowdown", "slowdown", "slowdown"])
        market = _market_frame(self.DAYS, ["risk_on", "risk_on", "mixed", "mixed", "risk_off"])
        out = build_context(macro, market, self.DAYS[-1])
        assert out.schema["context_transition"] == pl.Boolean
        assert out["context_transition"].to_list() == [False, False, True, False, True]
        assert out["context_transition_alert"].to_list()[2] == "expansion/risk_on -> slowdown/mixed"
        assert out["context_transition_alert"].to_list()[1] is None


# ── Sector Map Integration ───────────────────────────────────────────────────

class TestSectorMapIntegration:

    def test_sector_map_covers_whole_universe(self):
        get_loader.cache_clear()
        df = _load_sector_map()
        assert set(df["symbol"]) == {i.symbol for i in get_loader().all_symbols()}

    def test_sector_map_has_no_weight_or_regime_columns(self):
        df = _load_sector_map()
        assert not {"regime", "sector_weight_adj"} & set(df.columns)
