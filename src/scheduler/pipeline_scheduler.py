"""
pipeline_scheduler.py — GD §14.5 (APScheduler Upgrade Path)
Automated scheduler untuk ketika pipeline sudah stabil dan siap berjalan
tanpa pengawasan (target: setelah Phase P7 production deployment).

GD §14.5: karena semua logika job sudah ada di job_registry.py yang shared,
migrasi ke APScheduler hanya perlu ~50 baris ini — tanpa perubahan di
Bronze, Silver, Gold, atau utility layer manapun.

Usage (ketika siap auto):
    python -m src.scheduler.pipeline_scheduler

Requirements:
    pip install apscheduler pytz

Cron schedule (WIB — Asia/Jakarta):
    02:00  bronze_ohlcv_daily    (45m)
    02:45  bronze_treasury       (2m)
    03:00  silver_ohlcv          (60m)
    03:45  silver_validate       (10m)
    03:55  silver_active_symbols (5m)
    04:15  gold_signals          (90m)
    05:45  gold_mtf              (30m)
    06:00  gold_regime           (5m)
    06:05  gold_sector           (2m)
    06:15  gold_screener         (5m)
    07:30  health_report         (2m)

    Weekly (Sunday):
    02:00  bronze_macro_weekly   (15m)
    08:00  gold_correlation      (10m)

    Wednesday only:
    03:00  bronze_eia            (2m)

    FIX ADR-043 (GMI_Decision_Document_v10.docx): bronze_finnhub and
    silver_sentiment removed from this schedule — Finnhub retired in full
    (sentiment: 403 plan-tier gate on every symbol; earnings/quotes: never
    activated, NotImplementedError stub). Neither job exists in
    JOB_REGISTRY any more.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from loguru import logger

from src.scheduler.dependency_guard import DependencyGuard
from src.scheduler.job_registry import JOB_REGISTRY

SENTINEL_DIR = Path("data/.sentinels")


def _make_job(name: str, guard: DependencyGuard):
    """Wrap job function with dependency check + sentinel write."""
    def _job():
        run_date = date.today()
        job      = JOB_REGISTRY[name]
        missing  = guard.check_dependencies(job.get("depends_on", []), run_date)

        if missing:
            logger.warning(
                f"[Scheduler] {name}: skipped — missing deps {missing}"
            )
            return

        if guard.is_done(name, run_date):
            logger.debug(f"[Scheduler] {name}: already done — skipping")
            return

        logger.info(f"[Scheduler] {name}: starting...")
        try:
            job["fn"](run_date)
            guard.mark_done(name, run_date)
            logger.success(f"[Scheduler] {name}: done")
        except Exception as e:
            logger.error(f"[Scheduler] {name}: FAILED — {e}")

    _job.__name__ = f"job_{name}"
    return _job


def create_scheduler():
    """
    Build and return configured APScheduler instance.
    Import done lazily — apscheduler is optional dependency.
    """
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
        import pytz
    except ImportError:
        raise ImportError(
            "APScheduler not installed. Run:\n"
            "    pip install apscheduler pytz\n"
            "Manual runner (runner.py) is the recommended approach during development."
        )

    WIB   = pytz.timezone("Asia/Jakarta")
    guard = DependencyGuard(sentinel_dir=SENTINEL_DIR)
    sched = BlockingScheduler(timezone=WIB)

    # ── Daily jobs ────────────────────────────────────────────────────────────
    # FIX ADR-043: bronze_finnhub, silver_sentiment removed — retired in full.
    daily_schedule = [
        ("bronze_ohlcv_daily",   2,  0),
        ("bronze_treasury",      2, 45),
        ("silver_ohlcv",         3,  0),
        ("silver_macro",         3,  0),   # runs parallel-ish
        ("silver_validate",      3, 45),
        ("silver_active_symbols", 3, 55),
        ("gold_signals",         4, 15),
        ("gold_mtf",             5, 45),
        ("gold_regime",          6,  0),
        ("gold_sector",          6,  5),
        ("gold_screener",        6, 15),
        ("health_report",        7, 30),
    ]

    for job_name, hour, minute in daily_schedule:
        sched.add_job(
            _make_job(job_name, guard),
            CronTrigger(day_of_week="mon-fri", hour=hour, minute=minute,
                        timezone=WIB),
            id=job_name,
            name=job_name,
            misfire_grace_time=3600,   # 1h grace window
        )

    # ── Weekly jobs (Sunday) ──────────────────────────────────────────────────
    sched.add_job(
        _make_job("bronze_macro_weekly", guard),
        CronTrigger(day_of_week="sun", hour=2, minute=0, timezone=WIB),
        id="bronze_macro_weekly",
    )
    sched.add_job(
        _make_job("gold_correlation", guard),
        CronTrigger(day_of_week="sun", hour=8, minute=0, timezone=WIB),
        id="gold_correlation",
    )

    # ── Wednesday only (EIA) ──────────────────────────────────────────────────
    sched.add_job(
        _make_job("bronze_eia", guard),
        CronTrigger(day_of_week="wed", hour=3, minute=0, timezone=WIB),
        id="bronze_eia",
    )

    return sched


def main() -> None:
    """Entry point — run automated scheduler."""
    logger.info(
        "[Scheduler] Starting APScheduler pipeline automation"
        " (WIB timezone)..."
    )
    logger.warning(
        "[Scheduler] This is the production auto-mode scheduler."
        " For development, use: python src/runner.py --job <job_name>"
    )

    sched = create_scheduler()
    logger.info(
        f"[Scheduler] {len(sched.get_jobs())} jobs registered. Starting..."
    )

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[Scheduler] Scheduler stopped by user")
        sched.shutdown()


if __name__ == "__main__":
    main()
