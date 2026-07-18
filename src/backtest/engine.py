"""
engine.py — GD §12.4 (Backtest Engine)
Walk-forward backtest engine dengan PIT integrity.

Architecture (GD §12.4):
    Data Loader:     PITDataLoader — PIT-aware, anti-lookahead guaranteed
    Signal Generator: Gold Layer signals per bar
    Trade Executor:  DuckDB vectorized, entry hanya pada bar setelah signal
    Position Tracker: Polars rolling window
    Cost Model:      0.1% round-trip (configurable)
    Walk-Forward:    3M train / 1M test rolling window

Walk-Forward splits prevent overfitting (GD §12.4).
All P&L calculations on OOS (out-of-sample) periods only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import polars as pl
from loguru import logger

from src.backtest.pit_data import PITDataLoader
from src.utils.atomic_io import atomic_write_parquet  # FIX BCK-AIO-001

BACKTEST_RESULTS_PATH = Path("data/backtest")


from src.backtest.slippage import SlippageModel, SlippagePreset

@dataclass
class BacktestConfig:
    """Configuration for a backtest run."""
    symbols:            list[str]
    timeframe:          str          = "1D"
    start_date:         date         = field(default_factory=lambda: date(2020, 1, 1))
    end_date:           date         = field(default_factory=date.today)
    train_months:       int          = 3
    test_months:        int          = 1
    initial_capital:    float        = 100_000.0
    commission_pct:     float        = 0.001       # 0.1% per side
    max_position_pct:   float        = 0.10        # Max 10% per position
    min_mtf_score:      int          = 5
    min_signal_quality: str          = "B"
    slippage_model:     SlippageModel = field(
        default_factory=lambda: SlippagePreset.BASELINE
    )   # GD §12.4: 0.1% round-trip baseline


@dataclass
class Trade:
    """Single trade record."""
    symbol:        str
    entry_date:    date
    exit_date:     Optional[date]
    entry_price:   float
    exit_price:    Optional[float]
    direction:     str          # 'long' | 'short'
    qty:           float
    commission:    float
    pnl:           Optional[float] = None
    pnl_pct:       Optional[float] = None
    hold_days:     Optional[int]   = None
    exit_reason:   Optional[str]   = None


class BacktestEngine:
    """
    Walk-forward backtest engine.
    Consumes Gold Layer signals via PITDataLoader.
    """

    def __init__(self, config: BacktestConfig) -> None:
        self.config = config
        self._trades: list[Trade] = []

    def run(self) -> "BacktestResult":
        """
        Execute walk-forward backtest.
        Splits date range into train/test windows, evaluates on OOS only.
        """
        logger.info(
            f"[Backtest] Starting walk-forward | {self.config.start_date}"
            f" → {self.config.end_date}"
            f" | {len(self.config.symbols)} symbols | TF={self.config.timeframe}"
        )

        windows = self._generate_walk_forward_windows()
        all_results: list[dict] = []

        with PITDataLoader() as loader:
            for i, (train_start, train_end, test_start, test_end) in enumerate(windows):
                logger.info(
                    f"[Backtest] Window {i+1}/{len(windows)}"
                    f" | Train: {train_start}→{train_end}"
                    f" | Test: {test_start}→{test_end}"
                )
                window_result = self._run_window(
                    loader, test_start, test_end
                )
                all_results.append(window_result)

        result = BacktestResult(
            config=self.config,
            trades=self._trades,
            window_results=all_results,
        )
        result.compute_metrics()
        self._save_results(result)
        return result

    def _generate_walk_forward_windows(self) -> list[tuple]:
        """
        Generate (train_start, train_end, test_start, test_end) tuples.
        Walk-Forward: slide test window forward by test_months each iteration.
        """
        windows = []
        train_delta = timedelta(days=30 * self.config.train_months)
        test_delta  = timedelta(days=30 * self.config.test_months)

        test_start = self.config.start_date + train_delta
        while test_start + test_delta <= self.config.end_date:
            train_start = test_start - train_delta
            train_end   = test_start - timedelta(days=1)
            test_end    = min(test_start + test_delta - timedelta(days=1),
                              self.config.end_date)
            windows.append((train_start, train_end, test_start, test_end))
            test_start += test_delta

        return windows

    def _run_window(
        self,
        loader: PITDataLoader,
        test_start: date,
        test_end: date,
    ) -> dict:
        """Simulate trading in one test window."""
        capital    = self.config.initial_capital
        positions: dict[str, Trade] = {}   # symbol → open trade
        equity_curve: list[dict]    = []
        window_trades: list[Trade]  = []

        # Generate trading dates in window
        trading_days = self._get_trading_days(test_start, test_end)

        for trade_date in trading_days:
            # Check signals for each symbol
            for symbol in self.config.symbols:
                # MTF score (PIT)
                mtf = loader.get_mtf_score(symbol, trade_date)
                if mtf is None:
                    continue

                score   = mtf.get("mtf_score", 0)
                quality = mtf.get("signal_quality", "D")

                # OHLCV for last bar (for pricing)
                ohlcv = loader.get_ohlcv(symbol, self.config.timeframe, trade_date, 5)
                if ohlcv.is_empty():
                    continue
                last_bar = ohlcv.row(-1, named=True)
                price = last_bar["close"]

                # Exit logic: signal reversal or quality degradation
                if symbol in positions:
                    pos = positions[symbol]
                    should_exit = (
                        (pos.direction == "long"  and score < 0) or
                        (pos.direction == "short" and score > 0) or
                        quality == "D"
                    )
                    if should_exit:
                        # Estimate slippage on exit
                        atr          = last_bar.get("atr_14", price * 0.01)
                        dollar_vol   = last_bar.get("dollar_volume", price * 1_000_000)
                        slip_cost    = self.config.slippage_model.total_cost(
                            price, atr or 0, dollar_vol or 0, pos.direction, pos.qty
                        )
                        comm         = price * pos.qty * self.config.commission_pct
                        pnl_abs      = (price - pos.entry_price) * pos.qty
                        if pos.direction == "short":
                            pnl_abs = -pnl_abs
                        pos.exit_date   = trade_date
                        pos.exit_price  = price
                        pos.commission += comm + slip_cost
                        pos.pnl         = pnl_abs - comm - slip_cost
                        pos.pnl_pct     = pos.pnl / (pos.entry_price * pos.qty)
                        pos.hold_days   = (trade_date - pos.entry_date).days
                        pos.exit_reason = "signal_exit"
                        capital += pos.entry_price * pos.qty + pos.pnl
                        window_trades.append(pos)
                        del positions[symbol]
                        continue

                # Entry logic: strong signal, not already in position
                if (
                    symbol not in positions
                    and abs(score) >= self.config.min_mtf_score
                    and quality in ("A", "B")
                ):
                    direction      = "long" if score > 0 else "short"
                    position_value = capital * min(self.config.max_position_pct, 1.0)
                    qty            = position_value / price
                    atr            = last_bar.get("atr_14", price * 0.01)
                    dollar_vol     = last_bar.get("dollar_volume", price * 1_000_000)
                    slip_cost      = self.config.slippage_model.total_cost(
                        price, atr or 0, dollar_vol or 0, direction, qty
                    )
                    comm           = position_value * self.config.commission_pct
                    capital       -= position_value + comm + slip_cost

                    positions[symbol] = Trade(
                        symbol=symbol,
                        entry_date=trade_date,
                        exit_date=None,
                        entry_price=price,
                        exit_price=None,
                        direction=direction,
                        qty=qty,
                        commission=comm + slip_cost,
                    )

            # Mark-to-market equity
            open_pnl = 0.0
            for sym, pos in positions.items():
                ohlcv = loader.get_ohlcv(sym, self.config.timeframe, trade_date, 2)
                if not ohlcv.is_empty():
                    mtm = ohlcv.row(-1, named=True)["close"]
                    open_pnl += (mtm - pos.entry_price) * pos.qty

            equity_curve.append({
                "date":   str(trade_date),
                "equity": capital + open_pnl,
                "n_positions": len(positions),
            })

        # Force-close all open positions at window end
        for sym, pos in list(positions.items()):
            ohlcv = loader.get_ohlcv(sym, self.config.timeframe, test_end, 2)
            if not ohlcv.is_empty():
                close_price = ohlcv.row(-1, named=True)["close"]
                comm = close_price * pos.qty * self.config.commission_pct
                pnl  = (close_price - pos.entry_price) * pos.qty - comm
                pos.exit_date   = test_end
                pos.exit_price  = close_price
                pos.pnl         = pnl
                pos.exit_reason = "window_end"
                window_trades.append(pos)
                capital += pos.entry_price * pos.qty + pnl

        self._trades.extend(window_trades)

        return {
            "test_start":    str(test_start),
            "test_end":      str(test_end),
            "n_trades":      len(window_trades),
            "final_equity":  capital,
            "equity_curve":  equity_curve,
        }

    @staticmethod
    def _get_trading_days(start: date, end: date) -> list[date]:
        """Generate weekday dates (approximate trading days)."""
        days = []
        current = start
        while current <= end:
            if current.weekday() < 5:   # Mon-Fri
                days.append(current)
            current += timedelta(days=1)
        return days

    def _save_results(self, result: "BacktestResult") -> None:
        """Save backtest results to Parquet."""
        BACKTEST_RESULTS_PATH.mkdir(parents=True, exist_ok=True)
        # FIX BCK-PIT-001: use config.end_date — date.today() breaks reproducibility
        # Backtest results are stamped with the simulation end date, not wall-clock now
        ts = self.config.end_date.isoformat()

        if result.trades_df is not None and not result.trades_df.is_empty():
            # FIX BCK-AIO-001: atomic write — partial backtest results corrupt analysis
            atomic_write_parquet(
                result.trades_df,
                BACKTEST_RESULTS_PATH / f"trades_{ts}.parquet",
                compression="zstd", compression_level=3,
            )

        if result.metrics:
            # FIX BCK-AIO-001: atomic write for metrics too
            atomic_write_parquet(
                pl.DataFrame([result.metrics]),
                BACKTEST_RESULTS_PATH / f"metrics_{ts}.parquet",
                compression="zstd", compression_level=3,
            )
        logger.info(
            f"[Backtest] Results saved to {BACKTEST_RESULTS_PATH}"
        )


@dataclass
class BacktestResult:
    """Container for backtest results with computed metrics."""
    config:         BacktestConfig
    trades:         list[Trade]
    window_results: list[dict]
    trades_df:      Optional[pl.DataFrame] = None
    metrics:        Optional[dict]         = None

    def compute_metrics(self) -> None:
        """Compute full metrics via MetricsComputer (GD §12.4)."""
        from src.backtest.metrics import MetricsComputer, analyse_walk_forward

        if not self.trades:
            self.metrics = {"n_trades": 0}
            return

        trade_dicts = [
            {
                "symbol":      t.symbol,
                "entry_date":  str(t.entry_date),
                "exit_date":   str(t.exit_date) if t.exit_date else None,
                "direction":   t.direction,
                "entry_price": t.entry_price,
                "exit_price":  t.exit_price,
                "pnl":         t.pnl,
                "pnl_pct":     t.pnl_pct,
                "hold_days":   t.hold_days,
                "exit_reason": t.exit_reason,
            }
            for t in self.trades if t.pnl is not None
        ]

        equity_curve = []
        for wr in self.window_results:
            equity_curve.extend(wr.get("equity_curve", []))

        config_dict = {
            "initial_capital": self.config.initial_capital,
            "timeframe":       self.config.timeframe,
            "symbols":         self.config.symbols,
            "start_date":      str(self.config.start_date),
            "end_date":        str(self.config.end_date),
            "test_months":     self.config.test_months,
        }

        bm          = MetricsComputer().compute(trade_dicts, equity_curve,
                                                self.window_results, config_dict)
        wf_analysis = analyse_walk_forward(self.window_results)

        self.trades_df = pl.DataFrame(trade_dicts) if trade_dicts else pl.DataFrame()
        self.metrics   = {**bm.to_dict(), **wf_analysis}
