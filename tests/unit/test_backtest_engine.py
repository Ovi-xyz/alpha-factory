"""tests/unit/test_backtest_engine.py — BacktestEngine + PITDataLoader unit tests"""

from datetime import date, timedelta

import polars as pl
import pytest

from src.backtest.engine import BacktestConfig, BacktestResult, BacktestEngine, Trade


class TestBacktestConfig:

    def test_default_config_valid(self):
        cfg = BacktestConfig(symbols=["AAPL", "MSFT"])
        assert cfg.timeframe         == "1D"
        assert cfg.train_months      == 3
        assert cfg.test_months       == 1
        assert cfg.initial_capital   == 100_000.0
        assert cfg.commission_pct    == 0.001
        assert cfg.max_position_pct  == 0.10
        assert cfg.min_mtf_score     == 3
        assert cfg.min_signal_quality == "B"

    def test_custom_config(self):
        cfg = BacktestConfig(
            symbols=["AAPL"],
            timeframe="1H",
            initial_capital=50_000.0,
            commission_pct=0.0005,
        )
        assert cfg.timeframe       == "1H"
        assert cfg.initial_capital == 50_000.0
        assert cfg.commission_pct  == 0.0005


class TestBacktestResult:

    def test_empty_trades_metrics(self):
        cfg    = BacktestConfig(symbols=["AAPL"])
        result = BacktestResult(config=cfg, trades=[], window_results=[])
        result.compute_metrics()
        assert result.metrics["n_trades"] == 0

    def test_metrics_with_trades(self):
        cfg = BacktestConfig(symbols=["AAPL"])
        trades = [
            Trade(
                symbol="AAPL", entry_date=date(2025, 1, 2),
                exit_date=date(2025, 1, 10), entry_price=150.0,
                exit_price=158.0, direction="long", qty=10.0,
                commission=1.50, pnl=78.5, pnl_pct=0.0523,
                hold_days=8, exit_reason="signal_exit",
            ),
            Trade(
                symbol="MSFT", entry_date=date(2025, 1, 5),
                exit_date=date(2025, 1, 12), entry_price=300.0,
                exit_price=295.0, direction="long", qty=5.0,
                commission=1.50, pnl=-26.5, pnl_pct=-0.0177,
                hold_days=7, exit_reason="signal_exit",
            ),
        ]
        result = BacktestResult(
            config=cfg,
            trades=trades,
            window_results=[{"equity_curve": [
                {"date": "2025-01-02", "equity": 100_000},
                {"date": "2025-01-12", "equity": 100_050},
            ]}],
        )
        result.compute_metrics()

        assert result.metrics["n_trades"]  == 2
        assert result.metrics["win_rate"]  == 0.5     # 1 win / 2 trades
        assert result.metrics["total_pnl"] == 52.0    # 78.5 - 26.5

    def test_sharpe_is_float(self):
        cfg    = BacktestConfig(symbols=["AAPL"])
        trades = [
            Trade("AAPL", date(2025,1,2), date(2025,1,5), 150.0, 153.0,
                  "long", 10.0, 1.5, pnl=28.5, pnl_pct=0.019,
                  hold_days=3, exit_reason="exit"),
            Trade("AAPL", date(2025,1,6), date(2025,1,9), 153.0, 151.0,
                  "long", 10.0, 1.5, pnl=-21.5, pnl_pct=-0.014,
                  hold_days=3, exit_reason="exit"),
        ]
        result = BacktestResult(cfg, trades, [{"equity_curve": []}])
        result.compute_metrics()
        assert isinstance(result.metrics["sharpe"], float)


class TestWalkForwardWindows:

    def test_generates_windows(self):
        cfg = BacktestConfig(
            symbols=["AAPL"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            train_months=3,
            test_months=1,
        )
        engine  = BacktestEngine(cfg)
        windows = engine._generate_walk_forward_windows()
        assert len(windows) >= 6    # ~9 test months in 2024 (after 3M train)

    def test_window_structure(self):
        cfg = BacktestConfig(
            symbols=["AAPL"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 6, 30),
            train_months=3,
            test_months=1,
        )
        engine  = BacktestEngine(cfg)
        windows = engine._generate_walk_forward_windows()

        for train_start, train_end, test_start, test_end in windows:
            assert train_start < train_end
            assert train_end   < test_start
            assert test_start  <= test_end

    def test_no_overlapping_windows(self):
        cfg = BacktestConfig(
            symbols=["AAPL"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            train_months=3,
            test_months=1,
        )
        engine  = BacktestEngine(cfg)
        windows = engine._generate_walk_forward_windows()

        for i in range(len(windows) - 1):
            _, _, _, test_end_i   = windows[i]
            _, _, test_start_next, _ = windows[i + 1]
            # Test windows should not overlap
            assert test_end_i < test_start_next


class TestTradingDays:

    def test_weekdays_only(self):
        days = BacktestEngine._get_trading_days(
            date(2025, 1, 6),    # Monday
            date(2025, 1, 12),   # Sunday
        )
        for d in days:
            assert d.weekday() < 5, f"{d} is a weekend"

    def test_correct_count(self):
        # Jan 6 (Mon) → Jan 10 (Fri) = 5 trading days
        days = BacktestEngine._get_trading_days(
            date(2025, 1, 6),
            date(2025, 1, 10),
        )
        assert len(days) == 5
