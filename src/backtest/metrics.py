"""
metrics.py — GD §12.4 (Backtest Metrics)
Comprehensive performance metrics for walk-forward backtest results.

Metrics computed on OOS (out-of-sample) periods only:
    Sharpe Ratio:    annualized return / annualized std of returns
    Calmar Ratio:    annualized return / max drawdown
    Max Drawdown:    peak-to-trough decline in equity curve
    Win Rate:        fraction of profitable trades
    Profit Factor:   gross profit / gross loss
    Avg Hold Days:   mean trade duration
    Expectancy:      avg PnL per trade (win_rate × avg_win - loss_rate × avg_loss)
    Recovery Factor: total PnL / max drawdown
    Avg Win / Loss:  average winning and losing trade PnL
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import polars as pl
from loguru import logger


# ── Metrics Dataclass ─────────────────────────────────────────────────────────

@dataclass
class BacktestMetrics:
    """
    Complete backtest performance metrics.
    All metrics computed exclusively on OOS (test) periods.
    """
    # Trade statistics
    n_trades:       int   = 0
    n_winning:      int   = 0
    n_losing:       int   = 0
    win_rate:       float = 0.0    # n_winning / n_trades
    profit_factor:  float = 0.0    # gross_profit / gross_loss

    # PnL
    total_pnl:      float = 0.0
    avg_pnl:        float = 0.0
    avg_win:        float = 0.0
    avg_loss:       float = 0.0
    expectancy:     float = 0.0    # win_rate × avg_win + loss_rate × avg_loss

    # Duration
    avg_hold_days:  float = 0.0
    min_hold_days:  int   = 0
    max_hold_days:  int   = 0

    # Risk-adjusted returns
    sharpe:         float = 0.0
    calmar:         float = 0.0
    max_drawdown:   float = 0.0    # Fraction (e.g. 0.15 = 15%)
    recovery_factor:float = 0.0    # total_pnl / max_drawdown_abs

    # Walk-forward
    n_windows:      int   = 0
    avg_window_pnl: float = 0.0

    # Metadata
    start_date:     Optional[str] = None
    end_date:       Optional[str] = None
    symbols:        list[str]     = field(default_factory=list)
    timeframe:      str           = "1D"
    initial_capital:float         = 100_000.0

    def to_dict(self) -> dict:
        """Return metrics as dict for Parquet serialization."""
        return {
            "n_trades":        self.n_trades,
            "n_winning":       self.n_winning,
            "n_losing":        self.n_losing,
            "win_rate":        round(self.win_rate, 4),
            "profit_factor":   round(self.profit_factor, 3),
            "total_pnl":       round(self.total_pnl, 2),
            "avg_pnl":         round(self.avg_pnl, 2),
            "avg_win":         round(self.avg_win, 2),
            "avg_loss":        round(self.avg_loss, 2),
            "expectancy":      round(self.expectancy, 2),
            "avg_hold_days":   round(self.avg_hold_days, 1),
            "sharpe":          round(self.sharpe, 3),
            "calmar":          round(self.calmar, 3),
            "max_drawdown":    round(self.max_drawdown, 4),
            "recovery_factor": round(self.recovery_factor, 3),
            "n_windows":       self.n_windows,
            "avg_window_pnl":  round(self.avg_window_pnl, 2),
            "start_date":      self.start_date or "",
            "end_date":        self.end_date or "",
            "timeframe":       self.timeframe,
        }

    def summary(self) -> str:
        """Return human-readable one-line summary."""
        return (
            f"Trades={self.n_trades} | "
            f"WinRate={self.win_rate:.1%} | "
            f"PF={self.profit_factor:.2f} | "
            f"Sharpe={self.sharpe:.2f} | "
            f"MaxDD={self.max_drawdown:.1%} | "
            f"Calmar={self.calmar:.2f}"
        )


# ── Metrics Computer ──────────────────────────────────────────────────────────

class MetricsComputer:
    """
    Computes BacktestMetrics from trade records and equity curve.
    Designed for walk-forward backtest output from BacktestEngine.
    """

    TRADING_DAYS_PER_YEAR: int = 252

    def compute(
        self,
        trades: list[dict],
        equity_curve: list[dict],
        window_results: list[dict],
        config_dict: dict,
    ) -> BacktestMetrics:
        """
        Compute full metrics from backtest output.

        Args:
            trades:         List of trade record dicts with pnl, pnl_pct, hold_days
            equity_curve:   List of {date, equity} dicts across all windows
            window_results: Per-window summary from BacktestEngine
            config_dict:    BacktestConfig attributes as dict

        Returns:
            BacktestMetrics with all fields populated.
        """
        m = BacktestMetrics(
            initial_capital = config_dict.get("initial_capital", 100_000.0),
            timeframe       = config_dict.get("timeframe", "1D"),
            symbols         = config_dict.get("symbols", []),
            n_windows       = len(window_results),
            start_date      = str(config_dict.get("start_date", "")),
            end_date        = str(config_dict.get("end_date", "")),
        )

        if not trades:
            return m

        # Filter to closed trades only
        closed = [t for t in trades if t.get("pnl") is not None]
        if not closed:
            return m

        pnls      = [t["pnl"]      for t in closed]
        pnl_pcts  = [t["pnl_pct"]  for t in closed if t.get("pnl_pct") is not None]
        hold_days = [t["hold_days"] for t in closed if t.get("hold_days") is not None]

        # ── Trade Counts ──────────────────────────────────────────────────────
        m.n_trades  = len(closed)
        m.n_winning = sum(1 for p in pnls if p > 0)
        m.n_losing  = sum(1 for p in pnls if p <= 0)
        m.win_rate  = m.n_winning / m.n_trades if m.n_trades > 0 else 0.0

        # ── PnL Statistics ────────────────────────────────────────────────────
        m.total_pnl = sum(pnls)
        m.avg_pnl   = m.total_pnl / m.n_trades if m.n_trades else 0.0

        winning_pnls = [p for p in pnls if p > 0]
        losing_pnls  = [p for p in pnls if p <= 0]

        m.avg_win  = statistics.mean(winning_pnls) if winning_pnls else 0.0
        m.avg_loss = statistics.mean(losing_pnls)  if losing_pnls  else 0.0

        # Profit Factor
        gross_profit = sum(winning_pnls)
        gross_loss   = abs(sum(losing_pnls))
        m.profit_factor = (
            gross_profit / gross_loss if gross_loss > 0 else float("inf")
        )

        # Expectancy: avg$ per trade
        loss_rate   = m.n_losing / m.n_trades if m.n_trades else 0.0
        m.expectancy = (m.win_rate * m.avg_win) + (loss_rate * m.avg_loss)

        # ── Hold Duration ─────────────────────────────────────────────────────
        if hold_days:
            m.avg_hold_days = statistics.mean(hold_days)
            m.min_hold_days = min(hold_days)
            m.max_hold_days = max(hold_days)

        # ── Sharpe Ratio ──────────────────────────────────────────────────────
        m.sharpe = self._sharpe(pnl_pcts)

        # ── Max Drawdown ──────────────────────────────────────────────────────
        if equity_curve:
            equities = [e["equity"] for e in equity_curve if "equity" in e]
            m.max_drawdown  = self._max_drawdown(equities)

            # Recovery Factor
            max_dd_abs = m.max_drawdown * m.initial_capital
            m.recovery_factor = (
                m.total_pnl / max_dd_abs if max_dd_abs > 0 else 0.0
            )

            # Calmar Ratio
            m.calmar = self._calmar(
                equity_curve  = equities,
                initial        = m.initial_capital,
                n_windows      = m.n_windows,
                test_months    = config_dict.get("test_months", 1),
            )

        # ── Window Statistics ─────────────────────────────────────────────────
        window_pnls = [
            w.get("final_equity", m.initial_capital) - m.initial_capital
            for w in window_results
        ]
        m.avg_window_pnl = statistics.mean(window_pnls) if window_pnls else 0.0

        logger.info(f"[Metrics] {m.summary()}")
        return m

    def _sharpe(self, returns: list[float]) -> float:
        """Annualized Sharpe Ratio from per-trade return series."""
        if len(returns) < 2:
            return 0.0
        try:
            mean_r = statistics.mean(returns)
            std_r  = statistics.stdev(returns)
            if std_r <= 0:
                return 0.0
            # Annualize: assume trades are distributed uniformly across year
            return mean_r / std_r * math.sqrt(self.TRADING_DAYS_PER_YEAR)
        except Exception:
            return 0.0

    @staticmethod
    def _max_drawdown(equity_curve: list[float]) -> float:
        """Peak-to-trough max drawdown as fraction of peak equity."""
        if not equity_curve:
            return 0.0
        peak   = equity_curve[0]
        max_dd = 0.0
        for eq in equity_curve:
            peak   = max(peak, eq)
            dd     = (peak - eq) / peak if peak > 0 else 0.0
            max_dd = max(max_dd, dd)
        return max_dd

    def _calmar(
        self,
        equity_curve: list[float],
        initial: float,
        n_windows: int,
        test_months: int,
    ) -> float:
        """Calmar Ratio: annualized return / max drawdown."""
        if not equity_curve or initial <= 0:
            return 0.0

        total_return = (equity_curve[-1] - initial) / initial
        n_years      = max((n_windows * test_months) / 12, 0.1)
        ann_return   = (1 + total_return) ** (1 / n_years) - 1
        max_dd       = self._max_drawdown(equity_curve)

        return ann_return / max_dd if max_dd > 0 else 0.0


# ── Walk-Forward Analysis ─────────────────────────────────────────────────────

def analyse_walk_forward(window_results: list[dict]) -> dict:
    """
    Analyse consistency across walk-forward windows.
    Returns stability metrics: pct profitable windows, std of window returns.
    """
    if not window_results:
        return {}

    final_equities = [
        w.get("final_equity", 100_000.0) for w in window_results
    ]
    initial = 100_000.0   # Assume standard initial capital
    window_returns = [(eq - initial) / initial for eq in final_equities]

    profitable_windows = sum(1 for r in window_returns if r > 0)
    pct_profitable     = profitable_windows / len(window_returns)

    return {
        "n_windows":           len(window_results),
        "pct_profitable_win":  round(pct_profitable, 3),
        "avg_window_return":   round(statistics.mean(window_returns), 4),
        "std_window_return":   round(statistics.stdev(window_returns), 4)
                               if len(window_returns) > 1 else 0.0,
        "best_window_return":  round(max(window_returns), 4),
        "worst_window_return": round(min(window_returns), 4),
        "consistency_score":   round(
            pct_profitable * (1 - statistics.stdev(window_returns))
            if len(window_returns) > 1 else pct_profitable, 3
        ),
    }
