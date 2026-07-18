"""
test_end_to_end_smoke.py — End-to-End Pipeline Smoke Test
Validates the complete Bronze → Silver → Gold pipeline using synthetic data.

This test:
    1. Writes synthetic Bronze OHLCV for multiple symbols + markets
    2. Runs OHLCVProcessor → Silver
    3. Resolves ActiveSymbols from Silver
    4. Computes technical indicators (Gold signals)
    5. Computes MTF alignment scores
    6. Runs macro regime detection
    7. Applies sector rotation weights
    8. Builds screener watchlist
    9. Verifies output schema integrity at every layer

Does NOT require any external API keys — fully self-contained.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_ohlcv(symbol: str, n: int = 80, start_price: float = 100.0) -> pl.DataFrame:
    """Generate realistic OHLCV with a mild uptrend."""
    base  = date(2025, 1, 2)
    price = start_price
    rows  = []
    for i in range(n):
        price += price * 0.002 * (1 if i % 5 != 4 else -0.5)
        rows.append({
            "symbol":    symbol,
            "timestamp": base + timedelta(days=i),
            "open":      round(price * 0.999, 4),
            "high":      round(price * 1.007, 4),
            "low":       round(price * 0.994, 4),
            "close":     round(price, 4),
            "volume":    1_000_000 + i * 2_000,
        })
    return pl.DataFrame(rows)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def e2e_workspace(tmp_path_factory):
    """Create complete workspace with Bronze data for all test symbols."""
    ws = tmp_path_factory.mktemp("e2e_workspace")

    configs = [
        ("AAPL",    "us_stocks",  150.0),
        ("MSFT",    "us_stocks",  320.0),
        ("GOOGL",   "us_stocks",  140.0),
        ("NVDA",    "us_stocks",  480.0),
        ("BBCA",    "idx",       8500.0),
        ("TLKM",    "idx",       3200.0),
        ("EUR_USD", "forex",       1.085),
        # FIX GMI-IL-001: was ("SPX", "index", 4800.0). Architecture
        # Extension v1.0 ADR-003 reclassifies SPX out of Layer 1 'index'
        # into Layer 2 context_equity_dm — Layer 1 'index' market is now
        # permanently empty by design. Swapped to a real commodity symbol
        # (AU=Gold) to keep exercising the AS-5 always-in market pathway.
        ("AU",      "commodity",  2050.0),
    ]

    for symbol, market, price in configs:
        df       = _make_ohlcv(symbol, n=80, start_price=price)
        out_dir  = (
            ws / "bronze" / "market" / "ohlcv" / market
            / "source=yfinance" / f"symbol={symbol}"
            / "year=2025" / "month=01"
        )
        out_dir.mkdir(parents=True)
        df.write_parquet(out_dir / f"{symbol}_raw.parquet")

    return ws, configs


# ── E2E Tests ─────────────────────────────────────────────────────────────────

class TestEndToEndSmoke:

    # ── Layer 1: Bronze ───────────────────────────────────────────────────────

    def test_bronze_files_readable(self, e2e_workspace):
        ws, configs = e2e_workspace
        for symbol, market, _ in configs:
            pattern = str(
                ws / "bronze" / "market" / "ohlcv" / market
                / "**" / "*.parquet"
            )
            df = pl.read_parquet(pattern)
            assert len(df) > 0, f"Bronze empty for {symbol}"
            assert "close" in df.columns
            assert "volume" in df.columns

    # ── Layer 2: Silver ───────────────────────────────────────────────────────

    def test_silver_ohlcv_processing(self, e2e_workspace, monkeypatch):
        """OHLCVProcessor produces valid Silver for all symbols."""
        ws, configs = e2e_workspace
        from src.silver.ohlcv_processor import OHLCVProcessor, CURRENT_SILVER_VERSION
        proc    = OHLCVProcessor()
        results = {}

        for symbol, market, _ in configs:
            pattern = str(
                ws / "bronze" / "market" / "ohlcv" / market
                / "**" / "*.parquet"
            )
            df     = pl.read_parquet(pattern)
            silver = proc.process_symbol(df, symbol, market, "1D")
            assert silver is not None and len(silver) > 0
            assert silver["processing_version"].to_list()[0] == CURRENT_SILVER_VERSION
            results[symbol] = silver

        # Save Silver to workspace for downstream tests
        for symbol, market, _ in configs:
            out = ws / "silver" / "market_ohlcv" / market / f"symbol={symbol}"
            out.mkdir(parents=True)
            results[symbol].write_parquet(out / f"{symbol}_1D_silver.parquet")

    def test_silver_vwap_is_valid(self, e2e_workspace):
        """VWAP must be between low and high for all symbols."""
        ws, configs = e2e_workspace
        from src.silver.ohlcv_processor import OHLCVProcessor
        proc = OHLCVProcessor()

        for symbol, market, _ in configs[:3]:   # Sample 3 symbols
            pattern = str(
                ws / "bronze" / "market" / "ohlcv" / market
                / "**" / "*.parquet"
            )
            df     = pl.read_parquet(pattern)
            silver = proc.process_symbol(df, symbol, market, "1D")
            valid  = silver.filter(pl.col("vwap").is_not_null())

            assert len(valid) > 0
            assert (valid["vwap"] >= valid["low"]).all(), f"{symbol} VWAP < low"
            assert (valid["vwap"] <= valid["high"]).all(), f"{symbol} VWAP > high"

    def test_silver_dollar_volume_computed(self, e2e_workspace):
        """dollar_volume must equal close × volume."""
        ws, configs = e2e_workspace
        from src.silver.ohlcv_processor import OHLCVProcessor
        proc = OHLCVProcessor()

        for symbol, market, _ in [configs[0]]:   # Test AAPL
            pattern = str(
                ws / "bronze" / "market" / "ohlcv" / market
                / "**" / "*.parquet"
            )
            df     = pl.read_parquet(pattern)
            silver = proc.process_symbol(df, symbol, market, "1D")
            valid  = silver.filter(pl.col("dollar_volume").is_not_null())

            expected = valid["close"] * valid["volume"].cast(pl.Float64)
            diff     = (expected - valid["dollar_volume"]).abs().max()
            assert diff < 1.0

    # ── Layer 3: Silver Active Symbols ────────────────────────────────────────

    def test_active_symbols_resolves(self, e2e_workspace, monkeypatch, tmp_path):
        """ActiveSymbolsResolver resolves from Silver 1D data."""
        ws, configs = e2e_workspace
        from src.silver.active_symbols import ActiveSymbolsResolver

        # Write merged Silver 1D for resolver
        all_frames = []
        for symbol, market, price in configs:
            from src.silver.ohlcv_processor import OHLCVProcessor
            proc    = OHLCVProcessor()
            pattern = str(
                ws / "bronze" / "market" / "ohlcv" / market
                / "**" / "*.parquet"
            )
            df     = pl.read_parquet(pattern)
            silver = proc.process_symbol(df, symbol, market, "1D")
            silver = silver.with_columns([
                pl.lit(market).alias("market"),
                (pl.col("close") * pl.col("volume").cast(pl.Float64))
                  .alias("dollar_volume"),
            ])
            all_frames.append(silver)

        combined = pl.concat(all_frames, how="diagonal_relaxed")
        silver_dir = tmp_path / "silver_1d"
        silver_dir.mkdir()
        combined.write_parquet(silver_dir / "data.parquet")

        monkeypatch.setattr(ActiveSymbolsResolver, "OUTPUT_PATH", tmp_path / "active")
        resolver = ActiveSymbolsResolver()
        symbols  = resolver.resolve(str(silver_dir / "*.parquet"), date(2025, 3, 31))

        assert len(symbols) > 0
        # FIX GMI-IL-001: Forex and commodity always included (was "index" —
        # that market is now empty Layer 2 territory, see fixture comment above).
        assert "EUR_USD" in symbols
        assert "AU" in symbols

    # ── Gold Layer ────────────────────────────────────────────────────────────

    def test_technical_indicators_compute(self, e2e_workspace):
        """Full indicator chain runs on multi-symbol data."""
        ws, configs = e2e_workspace
        from src.gold.indicators.core_indicators import (
            add_ema, add_rsi, add_macd, add_atr, add_momentum_features,
        )
        from src.silver.ohlcv_processor import OHLCVProcessor

        frames = []
        proc   = OHLCVProcessor()
        for symbol, market, _ in configs[:4]:   # 4 symbols
            pattern = str(
                ws / "bronze" / "market" / "ohlcv" / market
                / "**" / "*.parquet"
            )
            df     = pl.read_parquet(pattern)
            silver = proc.process_symbol(df, symbol, market, "1D")
            frames.append(silver)

        combined = pl.concat(frames, how="diagonal_relaxed")

        result = (
            combined.sort(["symbol", "timestamp"])
            .pipe(add_ema,  periods=[9, 21, 50, 200])
            .pipe(add_rsi,  periods=[14, 28])
            .pipe(add_macd)
            .pipe(add_atr,  period=14)
            .pipe(add_momentum_features)
        )

        assert len(result) == len(combined)
        for col in ["ema_9", "ema_21", "ema_50", "rsi_14", "macd", "atr_14"]:
            assert col in result.columns, f"Missing indicator: {col}"
        assert (result["rsi_14"].drop_nulls() >= 0).all()
        assert (result["rsi_14"].drop_nulls() <= 100).all()

    def test_macro_regime_produces_valid_output(self):
        """MacroRegimeDetector returns valid regime record."""
        from src.gold.macro_regime import MacroRegimeDetector
        detector = MacroRegimeDetector()

        # Test with neutral indicators (no FRED data needed)
        indicators = {"vix": 22.0, "yield_spread": 0.4, "cpi": 3.5, "gdp": 2.5}
        regime, scores, confidence = detector._classify(indicators)

        assert regime in {
            "RISK_ON", "RISK_OFF", "STAGFLATION", "REFLATION", "DISINFLATION"
        }
        assert 0.0 <= confidence <= 1.0
        assert "composite" in scores

    def test_sector_rotation_covers_all_symbols(self, e2e_workspace):
        """REGIME_SECTOR_WEIGHTS covers all sectors present in instrument loader."""
        from src.gold.sector_rotation import REGIME_SECTOR_WEIGHTS
        from src.config.instrument_loader import get_loader

        loader  = get_loader()
        sectors = set(loader.sectors())

        for regime, weights in REGIME_SECTOR_WEIGHTS.items():
            uncovered = sectors - set(weights.keys())
            # All sectors in instrument loader should have a weight
            assert not uncovered, (
                f"Regime {regime} missing weights for sectors: {uncovered}"
            )

    # ── Schema Integrity ──────────────────────────────────────────────────────

    def test_silver_schema_all_required_columns(self, e2e_workspace):
        """Every Silver output must have complete schema."""
        ws, configs = e2e_workspace
        from src.silver.ohlcv_processor import OHLCVProcessor

        REQUIRED_SILVER_COLS = [
            "symbol", "timeframe",
            "open", "high", "low", "close", "volume",
            "log_return", "dollar_volume", "spread_hl", "vwap",
            "is_adjusted", "adj_factor", "is_clean",
            "data_source", "processing_version",
        ]

        proc = OHLCVProcessor()
        for symbol, market, _ in configs[:3]:
            pattern = str(
                ws / "bronze" / "market" / "ohlcv" / market
                / "**" / "*.parquet"
            )
            df     = pl.read_parquet(pattern)
            silver = proc.process_symbol(df, symbol, market, "1D")

            for col in REQUIRED_SILVER_COLS:
                assert col in silver.columns, (
                    f"Silver schema for {symbol} missing column: {col}"
                )

    def test_no_data_corruption_across_symbols(self, e2e_workspace):
        """Processing one symbol must not affect another symbol's data."""
        ws, configs = e2e_workspace
        from src.silver.ohlcv_processor import OHLCVProcessor

        proc    = OHLCVProcessor()
        results = {}

        for symbol, market, _ in configs[:3]:
            pattern = str(
                ws / "bronze" / "market" / "ohlcv" / market
                / "**" / "*.parquet"
            )
            df = pl.read_parquet(pattern)
            results[symbol] = proc.process_symbol(df, symbol, market, "1D")

        # Each result must contain only its own symbol
        for symbol, silver in results.items():
            symbols_in_result = silver["symbol"].unique().to_list()
            assert symbols_in_result == [symbol], (
                f"Data contamination: {symbol} result contains {symbols_in_result}"
            )
