"""
tests/unit/test_tvdatafeed_session.py

NEW (v1.11.2) — Coverage gap closure (GMI_Decision_Document_v3.docx Priority
2 / Checkpoint v6 §8 item 2). src/bronze/tvdatafeed_session.py had 0%
coverage (99 statements, 0 covered) prior to this file, despite being the
primary IDX data-source reliability mechanism (GD §9.1, IDD §6). All tests
mock away the network-dependent TvDatafeed client entirely — no real
TradingView session is ever created.

FIX TVS-2 (found while writing this file, not from re-reading the
docstring): the health-check-failure branch of _connect() had NO backoff
sleep, unlike the exception branch a few lines below it — three back-to-back
TvDatafeed() constructor calls with zero delay whenever login succeeds but
the lightweight health-check bar fetch fails. Fixed in tvdatafeed_session.py
alongside this test file; see test_health_check_failure_backs_off_between_attempts.

Coverage target: >=80% (CI/CD Ops Guide coverage table — src/bronze/*).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

import src.bronze.tvdatafeed_session as tvs_module
from src.bronze.tvdatafeed_session import (
    TvDatafeedSessionManager,
    get_tv_interval,
    MAX_SESSION_AGE,
    MAX_RETRY,
    RECONNECT_COOLDOWN,
)


@pytest.fixture(autouse=True)
def reset_singleton():
    """
    _instance is a CLASS attribute on TvDatafeedSessionManager — it persists
    across tests unless explicitly reset. Without this fixture, whichever
    test runs first "wins" the singleton for the rest of the test session
    (classic singleton test-pollution hazard).
    """
    TvDatafeedSessionManager._instance = None
    yield
    TvDatafeedSessionManager._instance = None


@pytest.fixture
def manager() -> TvDatafeedSessionManager:
    return TvDatafeedSessionManager()


# ── Singleton Pattern ─────────────────────────────────────────────────────────

class TestSingletonPattern:

    def test_two_instantiations_return_same_object(self):
        a = TvDatafeedSessionManager()
        b = TvDatafeedSessionManager()
        assert a is b

    def test_fresh_instance_has_no_session_state(self, manager):
        assert manager._tv is None
        assert manager._connected_at is None
        assert manager._fail_count == 0
        assert manager._last_failed_at is None


# ── is_session_stale ──────────────────────────────────────────────────────────

class TestIsSessionStale:

    def test_never_connected_is_stale(self, manager):
        assert manager.is_session_stale is True

    def test_recently_connected_not_stale(self, manager):
        manager._connected_at = datetime.utcnow()
        assert manager.is_session_stale is False

    def test_older_than_max_age_is_stale(self, manager):
        manager._connected_at = datetime.utcnow() - MAX_SESSION_AGE - timedelta(minutes=1)
        assert manager.is_session_stale is True

    def test_exactly_at_max_age_boundary_not_yet_stale(self, manager):
        """Boundary is strictly '>' MAX_SESSION_AGE, not '>='."""
        manager._connected_at = datetime.utcnow() - MAX_SESSION_AGE + timedelta(seconds=5)
        assert manager.is_session_stale is False


# ── is_available ───────────────────────────────────────────────────────────────

class TestIsAvailable:

    def test_no_tv_client_not_available(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", True)
        manager._tv = None
        assert manager.is_available is False

    def test_tv_not_installed_not_available_even_with_client(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", False)
        manager._tv = MagicMock()
        assert manager.is_available is False

    def test_tv_installed_and_client_set_is_available(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", True)
        manager._tv = MagicMock()
        assert manager.is_available is True


# ── get_client ─────────────────────────────────────────────────────────────────

class TestGetClient:

    def test_tv_not_installed_returns_none(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", False)
        assert manager.get_client() is None

    def test_fresh_session_triggers_connect(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", True)
        with patch.object(manager, "_connect") as mock_connect:
            manager.get_client()
            mock_connect.assert_called_once()

    def test_healthy_session_skips_connect_returns_client(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", True)
        fake_client = MagicMock()
        manager._tv = fake_client
        manager._connected_at = datetime.utcnow()
        with patch.object(manager, "_connect") as mock_connect:
            client = manager.get_client()
            mock_connect.assert_not_called()
            assert client is fake_client

    def test_cooldown_active_skips_reconnect_returns_none(self, manager, monkeypatch):
        """FIX TVS-1: within cooldown window after a failure, get_client()
        must return None immediately WITHOUT attempting another reconnect."""
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", True)
        manager._tv = None
        manager._last_failed_at = datetime.utcnow()  # just failed
        with patch.object(manager, "_connect") as mock_connect:
            result = manager.get_client()
            assert result is None
            mock_connect.assert_not_called()

    def test_cooldown_expired_attempts_reconnect(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", True)
        manager._tv = None
        manager._last_failed_at = (
            datetime.utcnow() - RECONNECT_COOLDOWN - timedelta(seconds=1)
        )
        with patch.object(manager, "_connect") as mock_connect:
            manager.get_client()
            mock_connect.assert_called_once()

    def test_stale_session_with_no_prior_failure_reconnects_immediately(self, manager, monkeypatch):
        """No _last_failed_at set (never failed before) — cooldown branch
        must not be entered at all."""
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", True)
        manager._tv = None
        manager._last_failed_at = None
        with patch.object(manager, "_connect") as mock_connect:
            manager.get_client()
            mock_connect.assert_called_once()


# ── force_reconnect / reset ───────────────────────────────────────────────────

class TestForceReconnect:

    def test_clears_session_state_and_reconnects(self, manager):
        manager._tv = MagicMock()
        manager._connected_at = datetime.utcnow()
        with patch.object(manager, "_connect") as mock_connect:
            manager.force_reconnect()
            mock_connect.assert_called_once()
        # _connect is mocked (no-op), so _tv should remain cleared, not repopulated
        assert manager._tv is None


class TestReset:

    def test_reset_clears_all_state_including_cooldown(self, manager):
        manager._tv = MagicMock()
        manager._connected_at = datetime.utcnow()
        manager._fail_count = 3
        manager._last_failed_at = datetime.utcnow()
        manager.reset()
        assert manager._tv is None
        assert manager._connected_at is None
        assert manager._fail_count == 0
        assert manager._last_failed_at is None  # FIX TVS-1: cooldown also cleared


# ── _connect (retry / backoff / credentials) ──────────────────────────────────

class TestConnect:

    def test_missing_credentials_returns_early_no_tvdatafeed_call(self, manager, monkeypatch):
        monkeypatch.delenv("TV_USERNAME", raising=False)
        monkeypatch.delenv("TV_PASSWORD", raising=False)
        with patch.object(tvs_module, "TvDatafeed") as mock_tv_cls:
            manager._connect()
            mock_tv_cls.assert_not_called()
        assert manager._tv is None

    def test_missing_password_only_also_returns_early(self, manager, monkeypatch):
        monkeypatch.setenv("TV_USERNAME", "user")
        monkeypatch.delenv("TV_PASSWORD", raising=False)
        with patch.object(tvs_module, "TvDatafeed") as mock_tv_cls:
            manager._connect()
            mock_tv_cls.assert_not_called()

    @patch("time.sleep")
    def test_success_on_first_attempt(self, mock_sleep, manager, monkeypatch):
        monkeypatch.setenv("TV_USERNAME", "user")
        monkeypatch.setenv("TV_PASSWORD", "pass")
        fake_client = MagicMock()
        with patch.object(tvs_module, "TvDatafeed", return_value=fake_client):
            with patch.object(manager, "_health_check", return_value=True):
                manager._connect()
        assert manager._tv is fake_client
        assert manager._connected_at is not None
        assert manager._fail_count == 0
        assert manager._last_failed_at is None
        mock_sleep.assert_not_called()  # no retries needed

    @patch("time.sleep")
    def test_health_check_failure_backs_off_between_attempts(self, mock_sleep, manager, monkeypatch):
        """FIX TVS-2 regression guard: health-check failure (no exception)
        must still sleep with exponential backoff between attempts, exactly
        like the exception path does."""
        monkeypatch.setenv("TV_USERNAME", "user")
        monkeypatch.setenv("TV_PASSWORD", "pass")
        fake_client = MagicMock()
        with patch.object(tvs_module, "TvDatafeed", return_value=fake_client):
            with patch.object(manager, "_health_check", return_value=False):
                manager._connect()
        assert manager._tv is None
        assert manager._fail_count == 1
        assert manager._last_failed_at is not None
        assert mock_sleep.call_count == MAX_RETRY, (
            "FIX TVS-2: health-check-failure branch must back off once per "
            "attempt, same as the exception branch"
        )

    @patch("time.sleep")
    def test_constructor_exception_retries_then_gives_up(self, mock_sleep, manager, monkeypatch):
        monkeypatch.setenv("TV_USERNAME", "user")
        monkeypatch.setenv("TV_PASSWORD", "pass")
        with patch.object(
            tvs_module, "TvDatafeed", side_effect=ConnectionError("timeout")
        ):
            manager._connect()
        assert manager._tv is None
        assert manager._fail_count == 1
        assert manager._last_failed_at is not None
        assert mock_sleep.call_count == MAX_RETRY

    @patch("time.sleep")
    def test_second_attempt_succeeds_after_first_exception(self, mock_sleep, manager, monkeypatch):
        monkeypatch.setenv("TV_USERNAME", "user")
        monkeypatch.setenv("TV_PASSWORD", "pass")
        fake_client = MagicMock()
        with patch.object(
            tvs_module, "TvDatafeed",
            side_effect=[ConnectionError("timeout"), fake_client],
        ):
            with patch.object(manager, "_health_check", return_value=True):
                manager._connect()
        assert manager._tv is fake_client
        assert manager._fail_count == 0
        assert manager._last_failed_at is None

    @patch("time.sleep")
    def test_backoff_uses_exponential_base(self, mock_sleep, manager, monkeypatch):
        """Sleep durations must follow RETRY_BACKOFF_BASE ** attempt (1, 2, 3...)."""
        monkeypatch.setenv("TV_USERNAME", "user")
        monkeypatch.setenv("TV_PASSWORD", "pass")
        with patch.object(
            tvs_module, "TvDatafeed", side_effect=ConnectionError("down")
        ):
            manager._connect()
        expected_calls = [
            ((tvs_module.RETRY_BACKOFF_BASE ** attempt,),)
            for attempt in range(1, MAX_RETRY + 1)
        ]
        actual = [call.args for call in mock_sleep.call_args_list]
        assert actual == [c[0] for c in expected_calls]


# ── _health_check ──────────────────────────────────────────────────────────────

class TestHealthCheck:

    def test_no_client_returns_false(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "Interval", MagicMock())
        manager._tv = None
        assert manager._health_check() is False

    def test_interval_none_returns_false(self, manager, monkeypatch):
        manager._tv = MagicMock()
        monkeypatch.setattr(tvs_module, "Interval", None)
        assert manager._health_check() is False

    def test_nonempty_result_is_healthy(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "Interval", MagicMock())
        manager._tv = MagicMock()
        manager._tv.get_hist.return_value = [1, 2, 3, 4, 5]
        assert manager._health_check() is True

    def test_empty_result_is_unhealthy(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "Interval", MagicMock())
        manager._tv = MagicMock()
        manager._tv.get_hist.return_value = []
        assert manager._health_check() is False

    def test_none_result_is_unhealthy(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "Interval", MagicMock())
        manager._tv = MagicMock()
        manager._tv.get_hist.return_value = None
        assert manager._health_check() is False

    def test_exception_is_unhealthy_not_propagated(self, manager, monkeypatch):
        monkeypatch.setattr(tvs_module, "Interval", MagicMock())
        manager._tv = MagicMock()
        manager._tv.get_hist.side_effect = Exception("network boom")
        assert manager._health_check() is False

    def test_calls_get_hist_with_health_check_constants(self, manager, monkeypatch):
        fake_interval = MagicMock()
        monkeypatch.setattr(tvs_module, "Interval", fake_interval)
        manager._tv = MagicMock()
        manager._tv.get_hist.return_value = [1]
        manager._health_check()
        kwargs = manager._tv.get_hist.call_args.kwargs
        assert kwargs["symbol"] == tvs_module.HEALTH_CHECK_SYM
        assert kwargs["exchange"] == tvs_module.HEALTH_CHECK_EXCH
        assert kwargs["n_bars"] == 5


# ── get_tv_interval ────────────────────────────────────────────────────────────

class TestGetTvInterval:

    def test_not_available_returns_none(self, monkeypatch):
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", False)
        assert get_tv_interval("1D") is None

    def test_interval_none_returns_none(self, monkeypatch):
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", True)
        monkeypatch.setattr(tvs_module, "Interval", None)
        assert get_tv_interval("1D") is None

    def test_known_timeframes_map_to_non_none(self, monkeypatch):
        fake_interval = MagicMock()
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", True)
        monkeypatch.setattr(tvs_module, "Interval", fake_interval)
        for tf in ["1m", "5m", "15m", "30m", "1H", "4H", "1D", "1W", "1M"]:
            assert get_tv_interval(tf) is not None, f"tf={tf} unexpectedly None"

    def test_unknown_timeframe_returns_none(self, monkeypatch):
        fake_interval = MagicMock()
        monkeypatch.setattr(tvs_module, "TV_AVAILABLE", True)
        monkeypatch.setattr(tvs_module, "Interval", fake_interval)
        assert get_tv_interval("3H") is None
        assert get_tv_interval("") is None
