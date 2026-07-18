"""
tests/unit/test_technical_signals.py — gold_signals core logic unit tests

Previously ZERO test coverage existed for _process_timeframe() / run() /
the active_ohlcv resolution — only the narrow _get_latest_vix() path fix
had a dedicated test file (test_technical_signals_vix_path.py). This file
covers:

  - FIX GLD-L2-01: Layer 2 (context) rows must NEVER appear in
    tech_signals_{TF}.parquet — reproduces the exact pollution bug (RSI/
    MACD computed for VIX as if tradeable) as a permanent regression guard.
  - ADD GLD-ACTIVE-001: active_ohlcv filtering — only symbols in the
    resolved list appear in output; graceful fallback to full Layer 1
    universe when active_ohlcv is unavailable.
  - _resolve_active_ohlcv_symbols(): success, FileNotFoundError fallback,
    empty-result fallback.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

import src.gold.technical_signals as ts_mod
from src.gold.technical_signals import (
    _process_timeframe,
    _resolve_active_ohlcv_symbols,
    run,
)


def _write_ohlcv(path: Path, symbol: str, n_days: int = 30) -> None:
    from datetime import timedelta
    base = date(2026, 5, 1)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol":    [symbol] * n_days,
        "timestamp": [base + timedelta(days=d) for d in range(n_days)],
        "open":      [150.0 + d for d in range(n_days)],
        "high":      [155.0 + d for d in range(n_days)],
        "low":       [145.0 + d for d in range(n_days)],
        "close":     [152.0 + d for d in range(n_days)],
        "volume":    [1_000_000] * n_days,
        "is_clean":  [True] * n_days,
    }).write_parquet(path)


class TestResolveActiveOhlcvSymbols:
    def test_returns_symbols_when_available(self, monkeypatch):
        with patch(
            "src.silver.active_symbols.ActiveSymbolsResolver.load_ohlcv",
            return_value=["AAPL", "MSFT"],
        ):
            result = _resolve_active_ohlcv_symbols(date(2026, 6, 1))
        assert result == ["AAPL", "MSFT"]

    def test_returns_none_when_not_yet_resolved(self, monkeypatch):
        with patch(
            "src.silver.active_symbols.ActiveSymbolsResolver.load_ohlcv",
            side_effect=FileNotFoundError("not resolved"),
        ):
            result = _resolve_active_ohlcv_symbols(date(2026, 6, 1))
        assert result is None

    def test_returns_none_when_resolved_but_empty(self, monkeypatch):
        with patch(
            "src.silver.active_symbols.ActiveSymbolsResolver.load_ohlcv",
            return_value=[],
        ):
            result = _resolve_active_ohlcv_symbols(date(2026, 6, 1))
        assert result is None


class TestProcessTimeframeLayer1Scoping:
    """FIX GLD-L2-01 — Layer 2 pollution must be eliminated."""

    def test_context_symbols_excluded_from_output(self, tmp_path, monkeypatch):
        """Reproduces the exact bug: VIX (Layer 2, context market) must
        NEVER receive RSI/MACD/ADX computation or appear in gold_signals
        output — pre-fix it did, because the glob had no market filter."""
        monkeypatch.setattr(ts_mod, "SILVER_OHLCV_PATH", tmp_path)
        monkeypatch.setattr(ts_mod, "GOLD_SIG_PATH", tmp_path / "gold" / "signals")

        _write_ohlcv(tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet", "AAPL")
        _write_ohlcv(tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet", "VIX")

        rows = _process_timeframe("1D", date(2026, 6, 1), active_symbols=None)
        assert rows > 0

        out = pl.read_parquet(tmp_path / "gold" / "signals" / "tech_signals_1D.parquet")
        assert "VIX" not in out["symbol"].to_list(), (
            "Layer 2 (context) symbols must never appear in tech_signals "
            "output — this is exactly the bug ADR-003 rationale warns "
            "about (RSI on VIX is not meaningful)"
        )
        assert "AAPL" in out["symbol"].to_list()

    def test_returns_zero_when_no_layer1_data(self, tmp_path, monkeypatch):
        """Even if Layer 2 data exists, absence of ANY Layer 1 data must
        gracefully return 0, never fall back to using Layer 2 rows."""
        monkeypatch.setattr(ts_mod, "SILVER_OHLCV_PATH", tmp_path)
        monkeypatch.setattr(ts_mod, "GOLD_SIG_PATH", tmp_path / "gold" / "signals")
        _write_ohlcv(tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet", "VIX")

        rows = _process_timeframe("1D", date(2026, 6, 1), active_symbols=None)
        assert rows == 0


class TestProcessTimeframeActiveOhlcvFilter:
    """ADD GLD-ACTIVE-001 — Architecture v2.0 §5.2."""

    def test_filters_to_active_symbols_when_provided(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ts_mod, "SILVER_OHLCV_PATH", tmp_path)
        monkeypatch.setattr(ts_mod, "GOLD_SIG_PATH", tmp_path / "gold" / "signals")

        _write_ohlcv(tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet", "AAPL")
        _write_ohlcv(tmp_path / "us_stocks" / "symbol=LOWVOL" / "LOWVOL_1D_silver.parquet", "LOWVOL")

        rows = _process_timeframe("1D", date(2026, 6, 1), active_symbols=["AAPL"])
        assert rows > 0

        out = pl.read_parquet(tmp_path / "gold" / "signals" / "tech_signals_1D.parquet")
        assert set(out["symbol"].unique().to_list()) == {"AAPL"}, (
            "Only symbols in the active_ohlcv list must appear when a "
            "filter list is provided"
        )

    def test_processes_full_layer1_universe_when_none(self, tmp_path, monkeypatch):
        """Graceful degradation: active_symbols=None must still process
        ALL Layer 1 symbols (just unfiltered by liquidity), not zero."""
        monkeypatch.setattr(ts_mod, "SILVER_OHLCV_PATH", tmp_path)
        monkeypatch.setattr(ts_mod, "GOLD_SIG_PATH", tmp_path / "gold" / "signals")

        _write_ohlcv(tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet", "AAPL")
        _write_ohlcv(tmp_path / "us_stocks" / "symbol=LOWVOL" / "LOWVOL_1D_silver.parquet", "LOWVOL")

        rows = _process_timeframe("1D", date(2026, 6, 1), active_symbols=None)
        assert rows > 0
        out = pl.read_parquet(tmp_path / "gold" / "signals" / "tech_signals_1D.parquet")
        assert set(out["symbol"].unique().to_list()) == {"AAPL", "LOWVOL"}


class TestRunEntryPoint:
    def test_run_resolves_active_ohlcv_once(self, tmp_path, monkeypatch):
        """active_ohlcv must be resolved ONCE per run(), not once per
        timeframe (7x) — same list is valid across all TFs for one run_date."""
        monkeypatch.setattr(ts_mod, "TIMEFRAMES", ["1D", "1W"])
        monkeypatch.setattr(ts_mod, "SILVER_OHLCV_PATH", tmp_path)
        monkeypatch.setattr(ts_mod, "GOLD_SIG_PATH", tmp_path / "gold" / "signals")

        with patch(
            "src.gold.technical_signals._resolve_active_ohlcv_symbols",
            return_value=["AAPL"],
        ) as mock_resolve, patch(
            "src.gold.technical_signals._process_timeframe", return_value=1
        ):
            run(date(2026, 6, 1))

        mock_resolve.assert_called_once_with(date(2026, 6, 1))

    def test_run_passes_active_symbols_to_every_timeframe(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ts_mod, "TIMEFRAMES", ["1D", "1W"])
        with patch(
            "src.gold.technical_signals._resolve_active_ohlcv_symbols",
            return_value=["AAPL"],
        ), patch(
            "src.gold.technical_signals._process_timeframe", return_value=1
        ) as mock_process:
            run(date(2026, 6, 1))

        for call in mock_process.call_args_list:
            assert call.args[2] == ["AAPL"] or call.kwargs.get("active_symbols") == ["AAPL"]
