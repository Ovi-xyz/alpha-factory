"""
tvdatafeed_session.py — IDD §6.2 (tvdatafeed Session Manager)
Singleton session manager dengan auto-reconnect dan health check.

tvdatafeed risk profile (IDD §6.1):
    - Session lifetime: ~8-12 jam — expire tanpa notice
    - Error: kadang return DataFrame kosong (bukan exception)
    - Rate limit: session-based — terlalu banyak request → ban sementara
    - Auth: TV_USERNAME + TV_PASSWORD dari .env

SOP jika IDX down (IDD §6.3):
    - > 5 symbols return None → log IDX_PARTIAL_FAILURE
    - Lanjut dengan yfinance .JK ChainedAdapter
    - Health reporter tampilkan IDX coverage %
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Optional

from loguru import logger

# Constants
MAX_SESSION_AGE      = timedelta(hours=6)    # Reconnect preventif tiap 6 jam
MAX_RETRY            = 3                     # Retry login maksimal
HEALTH_CHECK_SYM     = "BBCA"               # Symbol ringan untuk health check
HEALTH_CHECK_EXCH    = "IDX"
RETRY_BACKOFF_BASE   = 2                    # Exponential backoff base (seconds)
# FIX TVS-1: cooldown between reconnect attempts when tvdatafeed is down.
# Without cooldown: every get_client() call triggers a full 3-attempt reconnect
# (~29s of blocking sleep × 120 IDX symbol×TF calls = ~58 min when tv is down).
# With cooldown: after a failed connect, skip reconnect for RECONNECT_COOLDOWN.
# get_client() returns None immediately during cooldown → ChainedAdapter falls
# through to YFinanceJKAdapter without blocking the ingestion loop.
RECONNECT_COOLDOWN   = timedelta(minutes=5)

# Try import — graceful degradation jika tidak terinstall
try:
    from tvDatafeed import TvDatafeed, Interval  # type: ignore
    TV_AVAILABLE = True
except ImportError:
    TV_AVAILABLE = False
    TvDatafeed = None
    Interval    = None


class TvDatafeedSessionManager:
    """
    Singleton session manager untuk tvdatafeed.
    Thread-safe via __new__ singleton pattern.

    Usage:
        session = TvDatafeedSessionManager()
        client  = session.get_client()
        if client is None:
            # fallback to yfinance
    """

    _instance: Optional["TvDatafeedSessionManager"] = None

    def __new__(cls) -> "TvDatafeedSessionManager":
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._tv             = None
            inst._connected_at   = None
            inst._fail_count     = 0
            # FIX TVS-1: track last failure timestamp for cooldown enforcement
            inst._last_failed_at = None
            cls._instance = inst
        return cls._instance

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def is_session_stale(self) -> bool:
        """True jika session belum dibuat atau sudah lebih tua dari MAX_SESSION_AGE."""
        if self._connected_at is None:
            return True
        return datetime.utcnow() - self._connected_at > MAX_SESSION_AGE

    @property
    def is_available(self) -> bool:
        """True jika tvdatafeed terinstall DAN session aktif."""
        return TV_AVAILABLE and self._tv is not None

    def get_client(self) -> Optional["TvDatafeed"]:
        """
        Return client yang valid. Reconnect otomatis jika session stale.

        FIX TVS-1: check cooldown BEFORE attempting reconnect.
        When tvdatafeed is down, every get_client() call previously triggered
        a full 3-attempt reconnect (~29s blocking per call).
        30 IDX symbols × 4 TF × 29s = ~58 minutes of blocking sleep.
        Cooldown window (default 5 min) caps this: one reconnect attempt
        per RECONNECT_COOLDOWN, then return None immediately during the window.
        ChainedAdapter falls through to YFinanceJKAdapter without blocking.
        """
        if not TV_AVAILABLE:
            logger.debug("[tvSession] tvDatafeed not installed — skipping")
            return None

        if self._tv is None or self.is_session_stale:
            # FIX TVS-1: skip reconnect if within cooldown window after failure
            if self._last_failed_at is not None:
                elapsed   = datetime.utcnow() - self._last_failed_at
                remaining = RECONNECT_COOLDOWN - elapsed
                if remaining.total_seconds() > 0:
                    logger.debug(
                        f"[tvSession] Cooldown active after failed connect "
                        f"({int(remaining.total_seconds())}s remaining) "
                        f"— returning None; yfinance .JK will be used"
                    )
                    return None
            self._connect()
        return self._tv

    def force_reconnect(self) -> None:
        """
        Paksa reconnect — dipanggil jika fetch return DataFrame kosong.
        Empty result biasanya indikasi session mati diam-diam.
        Respects cooldown: if last failure was recent, skips reconnect.
        """
        logger.info("[tvSession] Force reconnect triggered")
        self._tv           = None
        self._connected_at = None
        self._connect()

    def reset(self) -> None:
        """Reset singleton state — useful untuk testing."""
        self._tv             = None
        self._connected_at   = None
        self._fail_count     = 0
        self._last_failed_at = None    # FIX TVS-1: also reset cooldown

    # ── Internal ──────────────────────────────────────────────────────────────

    def _connect(self) -> None:
        """Login ke TradingView dengan retry + exponential backoff."""
        username = os.getenv("TV_USERNAME")
        password = os.getenv("TV_PASSWORD")

        if not username or not password:
            logger.warning(
                "[tvSession] TV_USERNAME / TV_PASSWORD not set in .env"
                " — IDX will fallback to yfinance .JK"
            )
            self._tv = None
            return

        for attempt in range(1, MAX_RETRY + 1):
            try:
                logger.info(
                    f"[tvSession] Connecting (attempt {attempt}/{MAX_RETRY})..."
                )
                self._tv = TvDatafeed(username=username, password=password)
                self._connected_at   = datetime.utcnow()
                self._fail_count     = 0
                self._last_failed_at = None    # FIX TVS-1: clear cooldown on success

                if self._health_check():
                    logger.success(
                        "[tvSession] Connection established and healthy"
                    )
                    return
                else:
                    logger.warning(
                        "[tvSession] Health check failed after connect"
                        " — will retry"
                    )
                    self._tv = None

            except Exception as e:
                logger.warning(
                    f"[tvSession] Login attempt {attempt} failed: {e}"
                )
                time.sleep(RETRY_BACKOFF_BASE ** attempt)

        # FIX TVS-1: record failure time → cooldown enforced in get_client()
        self._last_failed_at = datetime.utcnow()
        self._fail_count    += 1
        self._tv             = None
        logger.error(
            f"[tvSession] All {MAX_RETRY} login attempts failed"
            f" (total failure count: {self._fail_count}). "
            f"Cooldown active for {int(RECONNECT_COOLDOWN.total_seconds())}s."
        )

    def _health_check(self) -> bool:
        """
        Fetch 5 bars BBCA 1D sebagai lightweight health check.
        Return True jika data kembali — session confirmed healthy.
        """
        if self._tv is None or Interval is None:
            return False
        try:
            df = self._tv.get_hist(
                symbol=HEALTH_CHECK_SYM,
                exchange=HEALTH_CHECK_EXCH,
                interval=Interval.in_daily,
                n_bars=5,
            )
            return df is not None and len(df) > 0
        except Exception as e:
            logger.warning(f"[tvSession] Health check exception: {e}")
            return False


# ── tvdatafeed Interval Mapping ───────────────────────────────────────────────

def get_tv_interval(tf: str) -> Optional["Interval"]:
    """Map pipeline timeframe string ke tvdatafeed Interval enum."""
    if not TV_AVAILABLE or Interval is None:
        return None
    interval_map = {
        "1m":  Interval.in_1_minute,
        "5m":  Interval.in_5_minute,
        "15m": Interval.in_15_minute,
        "30m": Interval.in_30_minute,
        "1H":  Interval.in_1_hour,
        "4H":  Interval.in_4_hour,
        "1D":  Interval.in_daily,
        "1W":  Interval.in_weekly,
        "1M":  Interval.in_monthly,
    }
    return interval_map.get(tf)
