"""tests/unit/test_backtest_metrics.py — BacktestMetrics unit tests"""

import math
from datetime import date

import pytest

from src.backtest.metrics import (
    BacktestMetrics,
    MetricsComputer,
    analyse_walk_forward,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_trades(n_win: int, n_lose: int,
                 avg_win: float = 100.0, avg_loss: float = -60.0) -> list[dict]:
    trades = []
    for _ in range(n_win):
        trades.append({
            "pnl": avg_win, "pnl_pct": avg_win / 10_000,
            "hold_days": 5, "symbol": "AAPL",
            "entry_date": "2025-01-02", "exit_date": "2025-01-09",
            "entry_price": 150.0, "exit_price": 151.0,
            "direction": "long", "exit_reason": "signal_exit",
        })
    for _ in range(n_lose):
        trades.append({
            "pnl": avg_loss, "pnl_pct": avg_loss / 10_000,
            "hold_days": 3, "symbol": "MSFT",
            "entry_date": "2025-01-10", "exit_date": "2025-01-13",
            "entry_price": 300.0, "exit_price": 298.0,
            "direction": "long", "exit_reason": "signal_exit",
        })
    return trades


def _make_equity(values: list[float]) -> list[dict]:
    return [{"date": f"2025-01-{i+2:02d}", "equity": v}
            for i, v in enumerate(values)]


# ── BacktestMetrics Tests ─────────────────────────────────────────────────────

class TestBacktestMetrics:

    def test_to_dict_returns_dict(self):
        m = BacktestMetrics(n_trades=10, win_rate=0.6)
        d = m.to_dict()
        assert isinstance(d, dict)
        assert "n_trades"     in d
        assert "win_rate"     in d
        assert "sharpe"       in d
        assert "max_drawdown" in d

    def test_summary_returns_string(self):
        m = BacktestMetrics(n_trades=50, win_rate=0.55, sharpe=1.2, max_drawdown=0.08)
        s = m.summary()
        assert isinstance(s, str)
        assert "Trades=" in s
        assert "Sharpe=" in s

    def test_default_zero_values(self):
        m = BacktestMetrics()
        assert m.n_trades      == 0
        assert m.win_rate      == 0.0
        assert m.sharpe        == 0.0
        assert m.max_drawdown  == 0.0


# ── MetricsComputer Tests ─────────────────────────────────────────────────────

class TestMetricsComputer:

    def setup_method(self):
        self.computer   = MetricsComputer()
        self.config_dict = {
            "initial_capital": 100_000.0,
            "timeframe": "1D",
            "symbols": ["AAPL", "MSFT"],
            "start_date": "2025-01-01",
            "end_date": "2025-12-31",
            "test_months": 1,
        }

    def test_empty_trades_returns_zero(self):
        m = self.computer.compute([], [], [], self.config_dict)
        assert m.n_trades == 0

    def test_win_rate_correct(self):
        trades = _make_trades(n_win=7, n_lose=3)
        m = self.computer.compute(trades, [], [{"final_equity": 100_700}], self.config_dict)
        assert m.n_trades == 10
        assert m.n_winning == 7
        assert abs(m.win_rate - 0.7) < 1e-9

    def test_total_pnl_correct(self):
        trades = _make_trades(n_win=5, n_lose=5, avg_win=100.0, avg_loss=-60.0)
        m = self.computer.compute(trades, [], [{"final_equity": 100_200}], self.config_dict)
        expected = 5 * 100.0 + 5 * (-60.0)   # 500 - 300 = 200
        assert abs(m.total_pnl - expected) < 1e-6

    def test_profit_factor(self):
        trades = _make_trades(n_win=5, n_lose=5, avg_win=100.0, avg_loss=-50.0)
        m = self.computer.compute(trades, [], [{"final_equity": 100_250}], self.config_dict)
        # gross_profit=500, gross_loss=250 → PF=2.0
        assert abs(m.profit_factor - 2.0) < 1e-6

    def test_expectancy_positive_for_good_system(self):
        trades = _make_trades(n_win=7, n_lose=3, avg_win=100.0, avg_loss=-50.0)
        m = self.computer.compute(trades, [], [{"final_equity": 100_550}], self.config_dict)
        # E = 0.7×100 + 0.3×(-50) = 70 - 15 = 55
        assert m.expectancy > 0

    def test_avg_hold_days(self):
        trades = _make_trades(n_win=3, n_lose=2)
        m = self.computer.compute(trades, [], [], self.config_dict)
        # win=5 days, lose=3 days: (5*3 + 3*2)/5 = 21/5 = 4.2
        assert abs(m.avg_hold_days - 4.2) < 0.01

    def test_max_drawdown_computed(self):
        equity = _make_equity([100_000, 102_000, 99_000, 101_000, 98_000, 103_000])
        trades = _make_trades(n_win=2, n_lose=1)
        m = self.computer.compute(trades, equity, [], self.config_dict)
        # Peak=102k, trough=98k: DD=(102-98)/102 ≈ 3.92%
        assert 0.0 < m.max_drawdown < 0.10

    def test_sharpe_positive_for_winning_system(self):
        trades = _make_trades(n_win=8, n_lose=2, avg_win=120.0, avg_loss=-30.0)
        m = self.computer.compute(trades, [], [{"final_equity": 101_020}], self.config_dict)
        assert m.sharpe >= 0   # Positive system → non-negative Sharpe

    def test_recovery_factor_computed(self):
        equity = _make_equity([100_000, 105_000, 103_000, 108_000])
        trades = _make_trades(n_win=3, n_lose=0, avg_win=100.0)
        m = self.computer.compute(trades, equity, [], self.config_dict)
        assert m.recovery_factor >= 0


# ── Max Drawdown Tests ────────────────────────────────────────────────────────

class TestMaxDrawdown:

    def test_no_drawdown(self):
        equities = [100_000, 101_000, 102_000, 103_000]
        dd = MetricsComputer._max_drawdown(equities)
        assert dd == 0.0

    def test_full_drawdown(self):
        equities = [100_000, 50_000]
        dd = MetricsComputer._max_drawdown(equities)
        assert abs(dd - 0.5) < 1e-9

    def test_partial_drawdown(self):
        equities = [100_000, 110_000, 99_000]
        dd = MetricsComputer._max_drawdown(equities)
        # Peak=110k, trough=99k: DD=11/110=10%
        assert abs(dd - 11_000/110_000) < 1e-9

    def test_empty_returns_zero(self):
        assert MetricsComputer._max_drawdown([]) == 0.0

    def test_single_value_returns_zero(self):
        assert MetricsComputer._max_drawdown([100_000]) == 0.0

    def test_recovery_not_negative(self):
        """Drawdown after recovery should not count as larger DD."""
        equities = [100_000, 90_000, 105_000, 95_000, 110_000]
        dd = MetricsComputer._max_drawdown(equities)
        assert dd == pytest.approx(0.1, abs=1e-9)   # Only the first 10% drop counts as max


# ── Analyse Walk-Forward Tests ────────────────────────────────────────────────

class TestAnalyseWalkForward:

    def test_empty_returns_empty(self):
        result = analyse_walk_forward([])
        assert result == {}

    def test_all_profitable_windows(self):
        windows = [
            {"final_equity": 101_000},
            {"final_equity": 103_000},
            {"final_equity": 102_500},
        ]
        r = analyse_walk_forward(windows)
        assert r["pct_profitable_win"] == 1.0

    def test_mixed_windows(self):
        windows = [
            {"final_equity": 102_000},
            {"final_equity": 98_000},
            {"final_equity": 101_000},
            {"final_equity": 99_000},
        ]
        r = analyse_walk_forward(windows)
        assert r["n_windows"]          == 4
        assert r["pct_profitable_win"] == 0.5

    def test_best_worst_windows(self):
        windows = [
            {"final_equity": 115_000},
            {"final_equity": 95_000},
            {"final_equity": 105_000},
        ]
        r = analyse_walk_forward(windows)
        assert r["best_window_return"]  > r["worst_window_return"]
        assert r["best_window_return"]  == pytest.approx(0.15, abs=0.001)
        assert r["worst_window_return"] == pytest.approx(-0.05, abs=0.001)

    def test_consistency_score_range(self):
        windows = [{"final_equity": 102_000 + i * 500} for i in range(6)]
        r = analyse_walk_forward(windows)
        assert 0 <= r["consistency_score"] <= 1.0
