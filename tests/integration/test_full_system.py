"""
test_full_system.py — Full System Integration Test
Tests the complete pipeline as a single coherent system:
Bronze → Silver → Active Symbols → Gold Signals → MTF → Regime → Sector → Screener

All components wired together with synthetic data — no API calls required.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest


# ── Synthetic Data Helpers ────────────────────────────────────────────────────

def _ohlcv(symbol: str, n: int, price: float, market: str) -> pl.DataFrame:
    base = date(2025, 1, 2)
    rows = []
    p    = price
    for i in range(n):
        p += p * 0.003 * (1 if i % 4 != 3 else -0.5)
        rows.append({
            "symbol":    symbol,
            "timestamp": base + timedelta(days=i),
            "open":      round(p * 0.999, 4),
            "high":      round(p * 1.008, 4),
            "low":       round(p * 0.993, 4),
            "close":     round(p, 4),
            "volume":    2_000_000 + i * 5_000,
            "market":    market,
        })
    return pl.DataFrame(rows)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def system_workspace(tmp_path_factory):
    """Full workspace with Bronze + Silver for complete system test."""
    ws = tmp_path_factory.mktemp("system")

    symbols = [
        ("AAPL",    150.0, "us_stocks"),
        ("MSFT",    320.0, "us_stocks"),
        ("GOOGL",   140.0, "us_stocks"),
        ("NVDA",    480.0, "us_stocks"),
        ("JPM",     185.0, "us_stocks"),
        ("BBCA",   8500.0, "idx"),
        ("EUR_USD",   1.08, "forex"),
        # FIX GMI-IL-001: was ("SPX", 4800.0, "index"). Architecture Extension
        # v1.0 ADR-003 reclassifies SPX out of Layer 1 'index' into Layer 2
        # context_equity_dm — Layer 1 'index' market is now permanently empty.
        # Swapped to a real commodity symbol (AU=Gold) to keep exercising the
        # AS-5 always-in market pathway and per-symbol indicator isolation.
        ("AU",     2050.0, "commodity"),
    ]

    from src.silver.ohlcv_processor import OHLCVProcessor
    proc = OHLCVProcessor()

    silver_frames = []
    for sym, price, market in symbols:
        raw    = _ohlcv(sym, 80, price, market)
        silver = proc.process_symbol(raw, sym, market, "1D")
        silver = silver.with_columns([
            pl.lit(market).alias("market"),
            (pl.col("close") * pl.col("volume").cast(pl.Float64)).alias("dollar_volume"),
        ])
        silver_frames.append(silver)

    combined = pl.concat(silver_frames, how="diagonal_relaxed")
    silver_dir = ws / "silver_1d"
    silver_dir.mkdir()
    combined.write_parquet(silver_dir / "data.parquet")

    return ws, silver_dir, symbols


# ── Layer Tests ───────────────────────────────────────────────────────────────

class TestFullSystemPipeline:

    # Layer 1: Silver OHLCV quality
    def test_l1_silver_schema_complete(self, system_workspace):
        _, silver_dir, _ = system_workspace
        df = pl.read_parquet(str(silver_dir / "*.parquet"))
        required = [
            "symbol", "open", "high", "low", "close", "volume",
            "vwap", "log_return", "dollar_volume", "is_clean",
            "is_adjusted", "adj_factor", "processing_version",
        ]
        for col in required:
            assert col in df.columns, f"Missing Silver column: {col}"

    def test_l1_vwap_in_ohlc_range(self, system_workspace):
        _, silver_dir, _ = system_workspace
        df    = pl.read_parquet(str(silver_dir / "*.parquet"))
        valid = df.filter(pl.col("vwap").is_not_null())
        assert (valid["vwap"] >= valid["low"]).all()
        assert (valid["vwap"] <= valid["high"]).all()

    def test_l1_dollar_volume_correct(self, system_workspace):
        _, silver_dir, _ = system_workspace
        df    = pl.read_parquet(str(silver_dir / "*.parquet"))
        valid = df.filter(
            pl.col("dollar_volume").is_not_null()
            & pl.col("close").is_not_null()
        )
        expected = valid["close"] * valid["volume"].cast(pl.Float64)
        diff     = (expected - valid["dollar_volume"]).abs().max()
        assert diff < 1.0, f"dollar_volume mismatch: max diff={diff}"

    def test_l1_no_cross_symbol_contamination(self, system_workspace):
        _, silver_dir, symbols = system_workspace
        from src.silver.ohlcv_processor import OHLCVProcessor
        proc = OHLCVProcessor()
        for sym, price, market in symbols[:4]:
            raw    = _ohlcv(sym, 40, price, market)
            silver = proc.process_symbol(raw, sym, market, "1D")
            syms   = silver["symbol"].unique().to_list()
            assert syms == [sym], f"{sym} result has other symbols: {syms}"

    # Layer 2: Active Symbols
    def test_l2_active_symbols_resolves(self, system_workspace, tmp_path, monkeypatch):
        _, silver_dir, _ = system_workspace
        from src.silver.active_symbols import ActiveSymbolsResolver
        monkeypatch.setattr(ActiveSymbolsResolver, "OUTPUT_PATH", tmp_path / "active")
        resolver = ActiveSymbolsResolver()
        symbols  = resolver.resolve(str(silver_dir / "*.parquet"), date(2025, 3, 28))
        assert len(symbols) > 0

    def test_l2_forex_always_included(self, system_workspace, tmp_path, monkeypatch):
        _, silver_dir, _ = system_workspace
        from src.silver.active_symbols import ActiveSymbolsResolver
        monkeypatch.setattr(ActiveSymbolsResolver, "OUTPUT_PATH", tmp_path / "active2")
        resolver = ActiveSymbolsResolver()
        symbols  = resolver.resolve(str(silver_dir / "*.parquet"), date(2025, 3, 28))
        assert "EUR_USD" in symbols

    def test_l2_commodity_always_included(self, system_workspace, tmp_path, monkeypatch):
        """
        FIX GMI-IL-001: was test_l2_index_always_included, asserted SPX (market=
        'index') always-in. Layer 1 'index' is now permanently empty (ADR-003);
        AU (commodity) exercises the same AS-5 always-in pathway.
        """
        _, silver_dir, _ = system_workspace
        from src.silver.active_symbols import ActiveSymbolsResolver
        monkeypatch.setattr(ActiveSymbolsResolver, "OUTPUT_PATH", tmp_path / "active3")
        resolver = ActiveSymbolsResolver()
        symbols  = resolver.resolve(str(silver_dir / "*.parquet"), date(2025, 3, 28))
        assert "AU" in symbols

    # Layer 3: Technical Indicators
    def test_l3_indicators_all_compute(self, system_workspace):
        _, silver_dir, _ = system_workspace
        from src.gold.indicators.core_indicators import (
            add_atr, add_ema, add_macd, add_momentum_features, add_rsi,
        )
        df = pl.read_parquet(str(silver_dir / "*.parquet")).sort(["symbol", "timestamp"])
        result = (
            df.pipe(add_ema,  periods=[9, 21, 50, 200])
              .pipe(add_rsi,  periods=[14, 28])
              .pipe(add_macd)
              .pipe(add_atr,  period=14)
              .pipe(add_momentum_features)
        )
        for col in ["ema_9", "ema_50", "rsi_14", "macd", "atr_14", "momentum_score"]:
            assert col in result.columns, f"Missing indicator: {col}"

    def test_l3_rsi_in_valid_range(self, system_workspace):
        _, silver_dir, _ = system_workspace
        from src.gold.indicators.core_indicators import add_rsi
        df   = pl.read_parquet(str(silver_dir / "*.parquet")).sort(["symbol", "timestamp"])
        rsi  = add_rsi(df)["rsi_14"].drop_nulls()
        assert (rsi >= 0).all() and (rsi <= 100).all()

    def test_l3_ema_per_symbol_correct(self, system_workspace):
        _, silver_dir, _ = system_workspace
        from src.gold.indicators.core_indicators import add_ema
        df   = pl.read_parquet(str(silver_dir / "*.parquet")).sort(["symbol", "timestamp"])
        ema  = add_ema(df, periods=[9])
        # FIX GMI-IL-001: was SPX (~4800) — see system_workspace fixture comment.
        # AAPL (~150) and AU (~2050) EMA should be very different.
        aapl_ema = ema.filter(pl.col("symbol") == "AAPL")["ema_9"].drop_nulls().mean()
        au_ema   = ema.filter(pl.col("symbol") == "AU")["ema_9"].drop_nulls().mean()
        if aapl_ema and au_ema:
            assert au_ema > aapl_ema * 10, "EMA not isolated per symbol"

    # Layer 4: Macro Regime
    def test_l4_regime_classification_stable(self):
        from src.gold.macro_regime import MacroRegimeDetector
        det     = MacroRegimeDetector()
        results = set()
        for _ in range(5):
            indicators = {"vix": 18.0, "yield_spread": 0.6, "cpi": 3.0, "gdp": 2.5}
            r, _, _    = det._classify(indicators)
            results.add(r)
        assert len(results) == 1, "Regime classification is not deterministic"

    def test_l4_regime_record_has_all_fields(self, tmp_path, monkeypatch):
        import src.gold.macro_regime as mr
        monkeypatch.setattr(mr, "REGIME_STORE_PATH", tmp_path / "regime_store.parquet")
        det    = mr.MacroRegimeDetector()
        record = det.detect(date(2025, 3, 28))
        for field in ["date", "regime", "confidence", "regime_transition", "composite_score"]:
            assert field in record, f"Missing regime field: {field}"

    # Layer 5: Sector Rotation
    def test_l5_all_instrument_symbols_have_weights(self):
        from src.gold.sector_rotation import REGIME_SECTOR_WEIGHTS
        from src.config.instrument_loader import get_loader
        loader  = get_loader()
        sectors = set(loader.sectors())
        for regime, weights in REGIME_SECTOR_WEIGHTS.items():
            missing = sectors - set(weights.keys())
            assert not missing, f"{regime} missing sectors: {missing}"

    def test_l5_weights_sum_reasonable(self):
        from src.gold.sector_rotation import REGIME_SECTOR_WEIGHTS
        for regime, weights in REGIME_SECTOR_WEIGHTS.items():
            total = sum(weights.values())
            assert 8 < total < 30, f"{regime}: total weight={total:.1f} out of range"

    # Layer 6: MTF Score Logic
    def test_l6_mtf_grade_complete_coverage(self):
        """All 15 possible |score| values (0-7) produce valid grades."""
        grades = set()
        for score in range(-7, 8):
            if abs(score) >= 6:
                g = "A"
            elif abs(score) == 5:
                g = "B"
            elif abs(score) == 4:
                g = "C"
            else:
                g = "D"
            grades.add(g)
        assert grades == {"A", "B", "C", "D"}

    def test_l6_regime_compatible_logic(self):
        """RISK_ON: positive scores compatible; RISK_OFF: negative."""
        df = pl.DataFrame({"mtf_score": [7, -7, 3, -3, 0]})

        # scores: [7, -7, 3, -3, 0]
        for regime, expected_compat in [
            ("RISK_ON",     [True, False, True, False, False]),   # > 0
            ("RISK_OFF",    [False, True, False, True, False]),    # < 0
            ("STAGFLATION", [True, True, True, True, True]),       # all
        ]:
            if regime == "RISK_ON":
                expr = pl.col("mtf_score") > 0
            elif regime == "RISK_OFF":
                expr = pl.col("mtf_score") < 0
            else:
                expr = pl.lit(True)

            result = df.with_columns(expr.alias("compat"))["compat"].to_list()
            assert result == expected_compat, f"{regime}: got {result}"

    # Layer 7: System Integrity
    def test_l7_instrument_count_unchanged(self):
        """
        FIX GMI-IL-001: was == 643. Architecture Extension v1.0 ADR-003
        reclassifies SPX, VIX, DXY out of Layer 1 into Layer 2 context.
        Layer 1 'unchanged' now means 640 (the new stable baseline) plus
        a separate, independently-asserted Layer 2 count of 59 active / 0
        deferred (FIX ADR-030-033, 30 Jul 2026) — see
        test_l7_layer2_context_universe_present.
        """
        from src.config.instrument_loader import get_loader
        assert get_loader().count() == 640

    def test_l7_layer2_context_universe_present(self):
        """ADD GMI-IL-001: Layer 2 context anchors — Extension v1.0 §3.1 total
        52, extended to 59 by GMI_Decision_Document_v1.docx ADR-014
        (context_dollar_basket, +6) and GMI_Decision_Document_v2.docx
        ADR-024 (context_fx_normalization, +1). FIX ADR-030-033
        (GMI_Decision_Document_v7.docx, 30 Jul 2026): deferred_count() now 0,
        not 4 — tvdatafeed retired entirely (ADR-029); CPO, RUBBER, TIN,
        NICKEL all un-deferred via yfinance equity proxies."""
        from src.config.instrument_loader import get_loader
        loader = get_loader()
        assert loader.count_context() == 59
        assert loader.count_context(include_deferred=True) == 59
        assert loader.deferred_count() == 0

    def test_l7_all_markets_represented(self):
        """
        FIX GMI-IL-001: 'index' market is intentionally empty post ADR-003
        (SPX, VIX moved to Layer 2 context_equity_dm / context_volatility).
        Tradeable Layer 1 markets must still be non-empty; 'index' is
        asserted == 0 explicitly rather than silently excluded, so any
        future regression that accidentally repopulates it is caught too.
        """
        from src.config.instrument_loader import get_loader
        loader = get_loader()
        for market in ["us_stocks", "idx", "forex", "commodity"]:
            count = len(loader.by_market(market))
            assert count > 0, f"Market {market!r} has no instruments"
        assert len(loader.by_market("index")) == 0, (
            "Layer 1 'index' must be empty — SPX/VIX reclassified to Layer 2 (ADR-003)"
        )

    def test_l7_pipeline_config_sane(self):
        from src.config.pipeline_config import get_config
        cfg = get_config()
        assert cfg.duckdb_memory_limit_gb <= 8   # Fits in M1 8GB
        assert cfg.min_symbol_coverage_pct >= 90
        assert cfg.screener_top_n <= 50
        assert cfg.max_per_cluster >= 1

    def test_l7_job_registry_all_layers_present(self):
        from src.scheduler.job_registry import JOB_REGISTRY
        layers = {job["layer"] for job in JOB_REGISTRY.values()}
        for expected in ["bronze", "silver", "gold", "util"]:
            assert expected in layers, f"Layer {expected!r} missing from registry"

    def test_l7_pipeline_sequence_comprehensive(self):
        """
        FIX NEW-2 (audit_v1_7_3_uncovered_findings.docx §3, Opsi A): floor lowered
        from >= 14 to >= 13.

        The previous >= 14 floor baked in the assumption that silver_fundamental
        would be part of PIPELINE_SEQUENCE (DAILY_SEQUENCE) — true only once
        bronze_finnhub gets a real implementation (Opsi B, not yet done;
        bronze_finnhub is presently an intentional NotImplementedError stub,
        FIX R-F04). Per NEW-2 Opsi A, silver_fundamental is deliberately kept
        out of the daily sequence and gold_screener's hard dependency on it
        removed — DAILY_SEQUENCE's correct current length is 13. The floor
        (rather than an exact ==13) still guards against accidental step removal.
        """
        from src.scheduler.job_registry import PIPELINE_SEQUENCE
        assert len(PIPELINE_SEQUENCE) >= 13, (
            f"PIPELINE_SEQUENCE has only {len(PIPELINE_SEQUENCE)} steps"
        )

    # Layer 8: Data Flow Contracts
    def test_l8_silver_is_clean_flag_correct(self, system_workspace):
        _, silver_dir, _ = system_workspace
        df = pl.read_parquet(str(silver_dir / "*.parquet"))
        # is_clean must be boolean
        assert df["is_clean"].dtype == pl.Boolean
        # For clean synthetic data: most rows should be clean
        clean_pct = df["is_clean"].mean()
        assert clean_pct >= 0.85, f"Expected >= 85% clean, got {clean_pct:.1%}"

    def test_l8_log_return_is_small(self, system_workspace):
        _, silver_dir, _ = system_workspace
        df   = pl.read_parquet(str(silver_dir / "*.parquet"))
        lrs  = df["log_return"].drop_nulls()
        # Daily log returns should be < 10% for normal market data
        assert (lrs.abs() < 0.10).mean() >= 0.95, "Too many extreme log returns"

    def test_l8_no_negative_prices(self, system_workspace):
        _, silver_dir, _ = system_workspace
        df = pl.read_parquet(str(silver_dir / "*.parquet"))
        for col in ["open", "high", "low", "close"]:
            assert (df[col] > 0).all(), f"Negative {col} prices found"

    def test_l8_high_gte_low_always(self, system_workspace):
        _, silver_dir, _ = system_workspace
        df = pl.read_parquet(str(silver_dir / "*.parquet"))
        assert (df["high"] >= df["low"]).all(), "high < low violation found"
