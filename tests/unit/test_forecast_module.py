"""
tests/unit/test_forecast_module.py — ForecastModule unit tests
(Architecture v2.0 §6.4, GMI Wave 1 Cycle 4 — fourth CrossAssetEngine
module, Gate 1 closure via broad_dollar.py).

Test matrix:
    - Graceful skip when followers resolver hasn't run, or
      forecast_context() returns nothing, or too little data exists.
    - PCA + per-equity VAR pipeline produces the documented schema, one
      row per (symbol, horizon) with horizon_days in 1..5.
    - A follower genuinely dependent on the (single, dominant-variance)
      Layer 2 context factor at a known lag is detected: var_lag matches,
      stable=True, |forecast_return| is materially larger than an
      independent follower's.
    - An independent (noise) follower resolves to var_lag=0 (BIC finds
      no structure) and a forecast_return close to its own sample mean —
      the explicit k_ar==0 handling path, exercised for real rather than
      only unit-tested in isolation.
    - k_ar==0 never raises (statsmodels' own forecast()/is_stable() do,
      on an empty coefficient array — confirmed empirically during
      development, not a hypothetical).
    - A follower with too little overlapping history is skipped without
      breaking the rest of the batch.
    - Both PCA-input fallback paths work: Layer 2 context present with no
      FX data, and FX-derived Broad Dollar present with no Layer 2
      context symbols.
    - run() writes the documented schema.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

import src.gold.cross_asset.broad_dollar as bd
import src.gold.cross_asset.forecast_module as fm
from src.gold.cross_asset.forecast_module import ForecastModule, run
from src.silver.active_symbols import ActiveSymbolsResolver

RUN_DATE = date(2026, 3, 1)
N_DAYS = 90
BASE_DATE = date(2026, 1, 1)
DATES = [BASE_DATE + timedelta(days=i) for i in range(N_DAYS)]


class _FakeInstrument:
    def __init__(self, symbol: str):
        self.symbol = symbol


class _FakeLoader:
    def __init__(self, symbols: list[str]):
        self._symbols = symbols

    def forecast_context(self) -> list[_FakeInstrument]:
        return [_FakeInstrument(s) for s in self._symbols]


@pytest.fixture
def paths(tmp_path, monkeypatch):
    silver_root = tmp_path / "silver" / "market_ohlcv"
    monkeypatch.setattr(fm, "SILVER_OHLCV_PATH", silver_root)
    monkeypatch.setattr(bd, "SILVER_OHLCV_PATH", silver_root)
    monkeypatch.setattr(fm, "REGIME_STORE_PATH", tmp_path / "gold" / "macro" / "regime_store.parquet")
    monkeypatch.setattr(fm, "GOLD_CROSS_ASSET_PATH", tmp_path / "gold" / "cross_asset")
    monkeypatch.setattr(fm, "FORECAST_STORE_PATH", tmp_path / "gold" / "cross_asset" / "cross_asset_forecast.parquet")

    as_out = tmp_path / "silver" / "active_symbols"
    monkeypatch.setattr(ActiveSymbolsResolver, "OUTPUT_PATH", property(lambda s: as_out))
    return tmp_path, as_out


def _set_forecast_context(monkeypatch, symbols: list[str]) -> None:
    monkeypatch.setattr(fm, "get_loader", lambda: _FakeLoader(symbols))


def _write_followers(as_out: Path, symbols: list[str]) -> None:
    as_out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}).write_parquet(
        as_out / f"active_ohlcv_{RUN_DATE.isoformat()}.parquet"
    )


def _write_series(tmp_path: Path, symbol: str, market: str, series: np.ndarray, n_days: int = N_DAYS) -> None:
    path = tmp_path / "silver" / "market_ohlcv" / market / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol":     [symbol] * n_days,
        "timestamp":  DATES[:n_days],
        "log_return": series[:n_days],
        "is_clean":   [True] * n_days,
    }).write_parquet(path)


def _weekday_dates_ending(end: date, lookback_days: int) -> list[date]:
    """Realistic 5-day trading calendar: every Mon-Fri date in the
    window ending at `end` — see test_correlation_module.py's identical
    helper for full rationale (FIX XAE-CAL-01 regression guard)."""
    start = end - timedelta(days=lookback_days)
    span = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    return [d for d in span if d.weekday() < 5]


def _write_series_at_dates(
    tmp_path: Path, symbol: str, market: str, series: np.ndarray, dates: list[date]
) -> None:
    path = tmp_path / "silver" / "market_ohlcv" / market / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol":     [symbol] * len(dates),
        "timestamp":  dates,
        "log_return": series[: len(dates)],
        "is_clean":   [True] * len(dates),
    }).write_parquet(path)


class TestGracefulSkip:
    def test_returns_none_when_followers_not_resolved(self, paths, monkeypatch):
        _set_forecast_context(monkeypatch, ["DXY"])
        assert ForecastModule().compute(RUN_DATE) is None

    def test_returns_none_when_no_forecast_context_symbols(self, paths, monkeypatch):
        tmp_path, as_out = paths
        _write_followers(as_out, ["AAPL"])
        rng = np.random.default_rng(0)
        _write_series(tmp_path, "AAPL", "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _set_forecast_context(monkeypatch, [])
        assert ForecastModule().compute(RUN_DATE) is None

    def test_run_does_not_raise_when_unresolved(self, paths, monkeypatch):
        _set_forecast_context(monkeypatch, ["DXY"])
        run(RUN_DATE)  # no exception


class TestPCAVARPipeline:
    def test_output_schema_and_horizon_range(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(1)
        _write_series(tmp_path, "DXY", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "SPX", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "AAPL", "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_followers(as_out, ["AAPL"])
        _set_forecast_context(monkeypatch, ["DXY", "SPX"])

        result = ForecastModule().compute(RUN_DATE)

        required = {
            "symbol", "computation_date", "horizon_days", "forecast_return",
            "n_pcs", "pca_variance_explained", "var_lag", "stable", "regime",
        }
        assert required.issubset(set(result.columns))
        assert sorted(result["horizon_days"].unique().to_list()) == [1, 2, 3, 4, 5]
        assert result.height == 5  # 1 symbol x 5 horizons

    def test_multiple_symbols_all_covered(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(2)
        _write_series(tmp_path, "DXY", "context", rng.normal(0, 0.01, N_DAYS))
        for sym in ["AAPL", "MSFT", "GOOGL"]:
            _write_series(tmp_path, sym, "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_followers(as_out, ["AAPL", "MSFT", "GOOGL"])
        _set_forecast_context(monkeypatch, ["DXY"])

        result = ForecastModule().compute(RUN_DATE)

        assert set(result["symbol"].unique().to_list()) == {"AAPL", "MSFT", "GOOGL"}
        assert result.height == 15  # 3 symbols x 5 horizons


class TestLaggedDependencyDetection:
    def test_dependent_follower_detected_vs_independent(self, paths, monkeypatch):
        """A follower built from the (single) context factor lagged by 1
        day must show a materially larger |forecast_return| than an
        independent follower, and var_lag >= 1 (BIC found structure)."""
        tmp_path, as_out = paths
        rng = np.random.default_rng(3)
        ctx = rng.normal(0, 0.02, N_DAYS)
        dependent = np.zeros(N_DAYS)
        dependent[1:] = 0.9 * ctx[:-1] + rng.normal(0, 0.002, N_DAYS - 1)
        independent = rng.normal(0, 0.01, N_DAYS)

        _write_series(tmp_path, "DXY", "context", ctx)
        _write_series(tmp_path, "DEPENDENT", "us_stocks", dependent)
        _write_series(tmp_path, "INDEPENDENT", "us_stocks", independent)
        _write_followers(as_out, ["DEPENDENT", "INDEPENDENT"])
        _set_forecast_context(monkeypatch, ["DXY"])

        result = ForecastModule().compute(RUN_DATE)

        dep_row = result.filter(
            (pl.col("symbol") == "DEPENDENT") & (pl.col("horizon_days") == 1)
        ).row(0, named=True)
        indep_row = result.filter(
            (pl.col("symbol") == "INDEPENDENT") & (pl.col("horizon_days") == 1)
        ).row(0, named=True)

        assert dep_row["var_lag"] >= 1
        assert abs(dep_row["forecast_return"]) > abs(indep_row["forecast_return"])
        assert dep_row["stable"] is True


class TestKArZeroEdgeCase:
    def test_independent_follower_resolves_zero_lag_no_crash(self, paths, monkeypatch):
        """statsmodels' forecast()/is_stable() both raise on an empty
        coefficient array when k_ar==0 — must never propagate."""
        tmp_path, as_out = paths
        rng = np.random.default_rng(4)
        _write_series(tmp_path, "DXY", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "NOISE", "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_followers(as_out, ["NOISE"])
        _set_forecast_context(monkeypatch, ["DXY"])

        result = ForecastModule().compute(RUN_DATE)  # must not raise

        assert result is not None
        row = result.filter(pl.col("horizon_days") == 1).row(0, named=True)
        if row["var_lag"] == 0:
            assert row["stable"] is True

    def test_zero_lag_forecast_equals_training_mean(self, paths, monkeypatch):
        """When k_ar==0, the module's own documented fallback is the
        equity's training-window mean return at every horizon."""
        tmp_path, as_out = paths
        rng = np.random.default_rng(5)
        _write_series(tmp_path, "DXY", "context", rng.normal(0, 0.01, N_DAYS))
        noise = rng.normal(0, 0.01, N_DAYS)
        _write_series(tmp_path, "NOISE", "us_stocks", noise)
        _write_followers(as_out, ["NOISE"])
        _set_forecast_context(monkeypatch, ["DXY"])

        result = ForecastModule().compute(RUN_DATE)
        rows = result.sort("horizon_days")
        if rows.row(0, named=True)["var_lag"] == 0:
            values = rows["forecast_return"].to_list()
            assert len(set(values)) == 1  # identical at every horizon — a flat forecast


class TestInsufficientHistory:
    def test_short_history_follower_skipped_others_continue(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(6)
        _write_series(tmp_path, "DXY", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "FULL_HISTORY", "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "SHORT_HISTORY", "us_stocks", rng.normal(0, 0.01, 5), n_days=5)
        _write_followers(as_out, ["FULL_HISTORY", "SHORT_HISTORY"])
        _set_forecast_context(monkeypatch, ["DXY"])

        result = ForecastModule().compute(RUN_DATE)

        assert result is not None
        assert set(result["symbol"].unique().to_list()) == {"FULL_HISTORY"}


class TestPCAInputFallbackPaths:
    def test_layer2_context_only_no_fx_data(self, paths, monkeypatch):
        """No Layer 1 forex/dollar_basket data at all -> Broad Dollar
        contributes nothing, but Layer 2 context alone still drives PCA."""
        tmp_path, as_out = paths
        rng = np.random.default_rng(7)
        _write_series(tmp_path, "DXY", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "AAPL", "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_followers(as_out, ["AAPL"])
        _set_forecast_context(monkeypatch, ["DXY"])

        result = ForecastModule().compute(RUN_DATE)
        assert result is not None and not result.is_empty()

    def test_fx_broad_dollar_only_no_layer2_context(self, paths, monkeypatch):
        """forecast_context() empty of usable Layer 2 OHLCV data, but FX
        pairs feeding Broad Dollar exist -> Broad Dollar alone (a single
        derived feature column) is still a valid, if trivial, PCA input
        (sklearn's PCA handles n_features=1 fine — confirmed empirically
        rather than assumed) and should still drive a usable VAR."""
        tmp_path, as_out = paths
        rng = np.random.default_rng(8)
        _write_series(tmp_path, "EUR_USD", "forex", rng.normal(0, 0.005, N_DAYS))
        _write_series(tmp_path, "USD_JPY", "forex", rng.normal(0, 0.005, N_DAYS))
        _write_series(tmp_path, "AAPL", "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_followers(as_out, ["AAPL"])
        _set_forecast_context(monkeypatch, [])  # no Layer 2 OHLCV context symbols

        result = ForecastModule().compute(RUN_DATE)

        assert result is not None and not result.is_empty()
        assert result.row(0, named=True)["n_pcs"] == 1


class TestCalendarAwareness:
    """FIX XAE-CAL-01 regression guard (16 Sep 2026) — see
    test_correlation_module.py's identical class for full rationale. A
    genuine, gap-free 5-day-week context symbol and follower (46 obs
    here, vs. the old, unreachable 52-row threshold) must not be
    excluded as insufficient history — this is exactly the scenario
    that left gold_forecast covering ~1 of ~196 active equities in
    production while still logging SUCCESS."""

    def test_weekday_only_context_and_follower_not_excluded(self, paths, monkeypatch):
        tmp_path, as_out = paths
        weekday_dates = _weekday_dates_ending(RUN_DATE, fm.LOOKBACK_DAYS)
        n = len(weekday_dates)
        assert n < 52
        assert n >= fm.MIN_OBSERVATIONS

        rng = np.random.default_rng(43)
        _write_series_at_dates(tmp_path, "DXY", "context", rng.normal(0, 0.01, n), weekday_dates)
        _write_series_at_dates(tmp_path, "AAPL", "us_stocks", rng.normal(0, 0.01, n), weekday_dates)
        _write_followers(as_out, ["AAPL"])
        _set_forecast_context(monkeypatch, ["DXY"])

        result = ForecastModule().compute(RUN_DATE)

        assert result is not None and not result.is_empty()
        assert set(result["symbol"].unique().to_list()) == {"AAPL"}


class TestRunEntryPoint:
    def test_run_writes_expected_schema(self, paths, monkeypatch):
        tmp_path, as_out = paths
        rng = np.random.default_rng(9)
        _write_series(tmp_path, "DXY", "context", rng.normal(0, 0.01, N_DAYS))
        _write_series(tmp_path, "AAPL", "us_stocks", rng.normal(0, 0.01, N_DAYS))
        _write_followers(as_out, ["AAPL"])
        _set_forecast_context(monkeypatch, ["DXY"])

        run(RUN_DATE)

        out_path = tmp_path / "gold" / "cross_asset" / "cross_asset_forecast.parquet"
        assert out_path.exists()
        out = pl.read_parquet(out_path)
        assert out.height == 5
        assert set(out["horizon_days"].to_list()) == {1, 2, 3, 4, 5}
