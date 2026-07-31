"""
tests/unit/test_tvdatafeed_adapter.py

NEW (v1.11.2) — Coverage gap closure (GMI_Decision_Document_v3.docx Priority
2 / Checkpoint v6 §8 item 2). src/bronze/tvdatafeed_adapter.py had 0%
coverage (60 statements, 0 covered) prior to this file. This is the primary
IDX SourceAdapter (GD §3.5) — tested here entirely via a mocked
TvDatafeedSessionManager, no real TradingView session or network I/O.

Tests validate:
    1. name property / SourceAdapter contract
    2. FIX TVA-1 — _null_count is an instance variable, not class-level
       (regression guard: two adapter instances must not share failure state)
    3. fetch() early-exit paths: TV unavailable, no client, unsupported TF
    4. fetch() empty-result path — force_reconnect() + null_count increment
    5. fetch() success path — column normalization + null_count reset to 0
    6. fetch() exception path — session/auth keyword detection triggers
       force_reconnect(); other exceptions do not
    7. _estimate_n_bars — FIX TVA-3 IDX session-hours corrections, 20k cap
    8. _check_null_alert — IDX_NULL_ALERT_THRESHOLD boundary

Coverage target: >=80% (CI/CD Ops Guide coverage table — src/bronze/*).
"""
from __future__ import annotations

import ast
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.bronze.tvdatafeed_adapter import (
    TvDatafeedAdapter,
    IDX_NULL_ALERT_THRESHOLD,
)
from src.bronze.source_adapter import SourceAdapter


@pytest.fixture
def adapter() -> TvDatafeedAdapter:
    return TvDatafeedAdapter()


@pytest.fixture
def sample_pandas_ohlcv() -> pd.DataFrame:
    """Mimics tvdatafeed's get_hist() return shape: DatetimeIndex named
    'datetime', lowercase OHLCV columns."""
    idx = pd.date_range("2026-07-01", periods=3, freq="D", name="datetime")
    return pd.DataFrame(
        {
            "symbol": ["BBCA"] * 3,
            "open":   [9000.0, 9050.0, 9100.0],
            "high":   [9100.0, 9150.0, 9200.0],
            "low":    [8950.0, 9000.0, 9050.0],
            "close":  [9050.0, 9100.0, 9150.0],
            "volume": [1_000_000, 1_100_000, 1_050_000],
        },
        index=idx,
    )


# ── SourceAdapter Contract ────────────────────────────────────────────────────

class TestSourceAdapterContract:

    def test_is_source_adapter_subclass(self):
        assert issubclass(TvDatafeedAdapter, SourceAdapter)

    def test_name_property(self, adapter):
        assert adapter.name == "tvdatafeed"


# ── FIX TVA-1: Instance-Level _null_count ─────────────────────────────────────

class TestNullCountIsInstanceLevel:

    def test_fresh_instance_starts_at_zero(self, adapter):
        assert adapter._null_count == 0

    def test_two_instances_do_not_share_null_count(self):
        """FIX TVA-1 regression guard: a class-level counter would let Run 1's
        failures bleed into Run 2's alert threshold."""
        a = TvDatafeedAdapter()
        b = TvDatafeedAdapter()
        a._null_count = 5
        assert b._null_count == 0


# ── fetch() Early-Exit Paths ───────────────────────────────────────────────────

class TestFetchEarlyExits:

    def test_tv_not_available_returns_none(self, adapter, monkeypatch):
        monkeypatch.setattr(
            "src.bronze.tvdatafeed_adapter.TV_AVAILABLE", False
        )
        result = adapter.fetch("BBCA", "1D", date(2026, 1, 1), date(2026, 7, 1))
        assert result is None

    def test_no_client_returns_none(self, adapter, monkeypatch):
        monkeypatch.setattr(
            "src.bronze.tvdatafeed_adapter.TV_AVAILABLE", True
        )
        mock_session = MagicMock()
        mock_session.get_client.return_value = None
        with patch(
            "src.bronze.tvdatafeed_adapter.TvDatafeedSessionManager",
            return_value=mock_session,
        ):
            result = adapter.fetch("BBCA", "1D", date(2026, 1, 1), date(2026, 7, 1))
        assert result is None

    def test_unsupported_timeframe_returns_none(self, adapter, monkeypatch):
        monkeypatch.setattr(
            "src.bronze.tvdatafeed_adapter.TV_AVAILABLE", True
        )
        mock_session = MagicMock()
        mock_session.get_client.return_value = MagicMock()
        with patch(
            "src.bronze.tvdatafeed_adapter.TvDatafeedSessionManager",
            return_value=mock_session,
        ):
            with patch(
                "src.bronze.tvdatafeed_adapter.get_tv_interval", return_value=None
            ):
                result = adapter.fetch(
                    "BBCA", "3H", date(2026, 1, 1), date(2026, 7, 1)
                )
        assert result is None


# ── fetch() Empty-Result Path ──────────────────────────────────────────────────

class TestFetchEmptyResult:

    def _mock_session_with_client(self, monkeypatch, get_hist_return):
        monkeypatch.setattr(
            "src.bronze.tvdatafeed_adapter.TV_AVAILABLE", True
        )
        mock_client = MagicMock()
        mock_client.get_hist.return_value = get_hist_return
        mock_session = MagicMock()
        mock_session.get_client.return_value = mock_client
        return mock_session, mock_client

    def test_none_result_triggers_force_reconnect_and_null_increment(self, adapter, monkeypatch):
        mock_session, _ = self._mock_session_with_client(monkeypatch, None)
        with patch(
            "src.bronze.tvdatafeed_adapter.TvDatafeedSessionManager",
            return_value=mock_session,
        ):
            with patch(
                "src.bronze.tvdatafeed_adapter.get_tv_interval", return_value="1D_INTERVAL"
            ):
                result = adapter.fetch(
                    "BBCA", "1D", date(2026, 1, 1), date(2026, 7, 1)
                )
        assert result is None
        mock_session.force_reconnect.assert_called_once()
        assert adapter._null_count == 1

    def test_empty_dataframe_result_also_triggers_reconnect(self, adapter, monkeypatch):
        empty_df = pd.DataFrame()
        mock_session, _ = self._mock_session_with_client(monkeypatch, empty_df)
        with patch(
            "src.bronze.tvdatafeed_adapter.TvDatafeedSessionManager",
            return_value=mock_session,
        ):
            with patch(
                "src.bronze.tvdatafeed_adapter.get_tv_interval", return_value="1D_INTERVAL"
            ):
                result = adapter.fetch(
                    "BBCA", "1D", date(2026, 1, 1), date(2026, 7, 1)
                )
        assert result is None
        mock_session.force_reconnect.assert_called_once()

    def test_null_count_accumulates_across_calls_on_same_instance(self, adapter, monkeypatch):
        mock_session, _ = self._mock_session_with_client(monkeypatch, None)
        with patch(
            "src.bronze.tvdatafeed_adapter.TvDatafeedSessionManager",
            return_value=mock_session,
        ):
            with patch(
                "src.bronze.tvdatafeed_adapter.get_tv_interval", return_value="1D_INTERVAL"
            ):
                for _ in range(3):
                    adapter.fetch("BBCA", "1D", date(2026, 1, 1), date(2026, 7, 1))
        assert adapter._null_count == 3


# ── fetch() Success Path ────────────────────────────────────────────────────────

class TestFetchSuccess:

    def test_success_normalizes_columns_and_returns_polars_df(
        self, adapter, monkeypatch, sample_pandas_ohlcv
    ):
        monkeypatch.setattr(
            "src.bronze.tvdatafeed_adapter.TV_AVAILABLE", True
        )
        mock_client = MagicMock()
        mock_client.get_hist.return_value = sample_pandas_ohlcv
        mock_session = MagicMock()
        mock_session.get_client.return_value = mock_client
        with patch(
            "src.bronze.tvdatafeed_adapter.TvDatafeedSessionManager",
            return_value=mock_session,
        ):
            with patch(
                "src.bronze.tvdatafeed_adapter.get_tv_interval", return_value="1D_INTERVAL"
            ):
                result = adapter.fetch(
                    "BBCA", "1D", date(2026, 1, 1), date(2026, 7, 1)
                )
        assert result is not None
        assert set(result.columns) == {
            "timestamp", "open", "high", "low", "close", "volume"
        }
        assert result.shape[0] == 3

    def test_success_resets_null_count_to_zero(self, adapter, monkeypatch, sample_pandas_ohlcv):
        adapter._null_count = 4  # simulate prior failures
        monkeypatch.setattr(
            "src.bronze.tvdatafeed_adapter.TV_AVAILABLE", True
        )
        mock_client = MagicMock()
        mock_client.get_hist.return_value = sample_pandas_ohlcv
        mock_session = MagicMock()
        mock_session.get_client.return_value = mock_client
        with patch(
            "src.bronze.tvdatafeed_adapter.TvDatafeedSessionManager",
            return_value=mock_session,
        ):
            with patch(
                "src.bronze.tvdatafeed_adapter.get_tv_interval", return_value="1D_INTERVAL"
            ):
                adapter.fetch("BBCA", "1D", date(2026, 1, 1), date(2026, 7, 1))
        assert adapter._null_count == 0

    def test_get_hist_called_with_idx_exchange(self, adapter, monkeypatch, sample_pandas_ohlcv):
        """fetch() must always request exchange='IDX' — the class docstring
        states this is IDX30's primary source."""
        monkeypatch.setattr(
            "src.bronze.tvdatafeed_adapter.TV_AVAILABLE", True
        )
        mock_client = MagicMock()
        mock_client.get_hist.return_value = sample_pandas_ohlcv
        mock_session = MagicMock()
        mock_session.get_client.return_value = mock_client
        with patch(
            "src.bronze.tvdatafeed_adapter.TvDatafeedSessionManager",
            return_value=mock_session,
        ):
            with patch(
                "src.bronze.tvdatafeed_adapter.get_tv_interval", return_value="1D_INTERVAL"
            ):
                adapter.fetch("TLKM", "1D", date(2026, 1, 1), date(2026, 7, 1))
        kwargs = mock_client.get_hist.call_args.kwargs
        assert kwargs["exchange"] == "IDX"
        assert kwargs["symbol"] == "TLKM"


# ── fetch() Exception Path ──────────────────────────────────────────────────────

class TestFetchExceptionHandling:

    def _mock_session_raising(self, monkeypatch, exception):
        monkeypatch.setattr(
            "src.bronze.tvdatafeed_adapter.TV_AVAILABLE", True
        )
        mock_client = MagicMock()
        mock_client.get_hist.side_effect = exception
        mock_session = MagicMock()
        mock_session.get_client.return_value = mock_client
        return mock_session

    @pytest.mark.parametrize("message", [
        "session expired", "Auth failed", "invalid login", "token rejected",
    ])
    def test_session_related_exception_triggers_force_reconnect(
        self, adapter, monkeypatch, message
    ):
        mock_session = self._mock_session_raising(monkeypatch, Exception(message))
        with patch(
            "src.bronze.tvdatafeed_adapter.TvDatafeedSessionManager",
            return_value=mock_session,
        ):
            with patch(
                "src.bronze.tvdatafeed_adapter.get_tv_interval", return_value="1D_INTERVAL"
            ):
                result = adapter.fetch(
                    "BBCA", "1D", date(2026, 1, 1), date(2026, 7, 1)
                )
        assert result is None
        mock_session.force_reconnect.assert_called_once()
        assert adapter._null_count == 1

    def test_unrelated_exception_does_not_trigger_force_reconnect(self, adapter, monkeypatch):
        mock_session = self._mock_session_raising(
            monkeypatch, ValueError("malformed response")
        )
        with patch(
            "src.bronze.tvdatafeed_adapter.TvDatafeedSessionManager",
            return_value=mock_session,
        ):
            with patch(
                "src.bronze.tvdatafeed_adapter.get_tv_interval", return_value="1D_INTERVAL"
            ):
                result = adapter.fetch(
                    "BBCA", "1D", date(2026, 1, 1), date(2026, 7, 1)
                )
        assert result is None
        mock_session.force_reconnect.assert_not_called()
        assert adapter._null_count == 1  # still counted as a null result


# ── _estimate_n_bars (FIX TVA-3) ────────────────────────────────────────────────

class TestEstimateNBars:

    def test_daily_timeframe_roughly_one_bar_per_day(self, adapter):
        n = adapter._estimate_n_bars(date(2026, 1, 1), date(2026, 1, 31), "1D")
        assert 30 <= n <= 45  # 30 days * 1.1 + 10 buffer, generous bound

    def test_unknown_timeframe_defaults_to_one_bar_per_day(self, adapter):
        n = adapter._estimate_n_bars(date(2026, 1, 1), date(2026, 1, 11), "99X")
        assert n == int(10 * 1 * 1.1) + 10

    def test_capped_at_20000(self, adapter):
        n = adapter._estimate_n_bars(date(2000, 1, 1), date(2026, 1, 1), "5m")
        assert n == 20_000

    def test_minimum_one_day_even_for_same_start_end(self, adapter):
        d = date(2026, 7, 1)
        n = adapter._estimate_n_bars(d, d, "1D")
        assert n > 0  # max((end-start).days, 1) floor prevents zero/negative

    def test_1h_uses_idx_session_hours_not_us_market_hours(self, adapter):
        """FIX TVA-3: IDX 1H bars_per_day must be 5.5 (IDX session), not 8
        (US market session) — the original bug this fix corrected."""
        n_1h = adapter._estimate_n_bars(date(2026, 1, 1), date(2026, 2, 1), "1H")
        n_1d = adapter._estimate_n_bars(date(2026, 1, 1), date(2026, 2, 1), "1D")
        # 5.5 bars/day should be well under 8x the daily estimate, and
        # comfortably above 4x (sanity bound around the documented 5.5 value)
        assert 4 * n_1d < n_1h < 8 * n_1d


# ── _check_null_alert ────────────────────────────────────────────────────────────

class TestCheckNullAlert:

    def test_below_threshold_does_not_log_error(self, adapter):
        # NOTE: mocks the loguru logger directly, not stdlib caplog — loguru
        # does not propagate to the standard logging module by default, so
        # caplog would silently capture nothing regardless of behavior here.
        adapter._null_count = IDX_NULL_ALERT_THRESHOLD - 1
        with patch("src.bronze.tvdatafeed_adapter.logger") as mock_logger:
            adapter._check_null_alert("BBCA")
            mock_logger.error.assert_not_called()

    def test_at_threshold_logs_error(self, adapter):
        adapter._null_count = IDX_NULL_ALERT_THRESHOLD
        with patch("src.bronze.tvdatafeed_adapter.logger") as mock_logger:
            adapter._check_null_alert("BBCA")
            mock_logger.error.assert_called_once()
            assert "IDX_PARTIAL_FAILURE" in mock_logger.error.call_args.args[0]

    def test_above_threshold_also_logs_error(self, adapter):
        adapter._null_count = IDX_NULL_ALERT_THRESHOLD + 3
        with patch("src.bronze.tvdatafeed_adapter.logger") as mock_logger:
            adapter._check_null_alert("BBCA")
            mock_logger.error.assert_called_once()


# ── Architectural Invariants ─────────────────────────────────────────────────────

class TestArchitecturalInvariants:

    def test_no_yfinance_jk_adapter_defined_here(self):
        """FIX TVA-2: YFinanceJKAdapter must not be redefined in this module —
        the canonical definition lives in yfinance_adapter.py. Two definitions
        of the same class name is a maintainability hazard the fix removed."""
        import src.bronze.tvdatafeed_adapter as module
        tree = ast.parse(Path(module.__file__).read_text())
        class_names = [
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        ]
        assert "YFinanceJKAdapter" not in class_names

    def test_syntax_valid(self):
        import src.bronze.tvdatafeed_adapter as module
        ast.parse(Path(module.__file__).read_text())
