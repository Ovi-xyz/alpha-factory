"""
runner.py — GD §14.3.1 (Manual Runner CLI Entry Point)
CLI entry point untuk Alpha Factory.

Usage:
    python runner.py --job bronze_ohlcv_daily
    python runner.py --job all
    python runner.py --job silver_ohlcv --force
    python runner.py --job gold_regime --date 2026-05-13
    python runner.py --list
    python runner.py --status
    python runner.py --reset gold_regime
    python runner.py --reset-all

G5: schedule guard via _passes_schedule() — beberapa job hanya berjalan pada
    hari tertentu (EIA=Rabu, BLS NFP=Jumat pertama, dll).

G6: --reset flag untuk clear ProgressCheckpoint sebelum force-rerun.
"""

import argparse
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

# FIX (Ovi, this thread — "issues in API/ticker/data sources even though
# .env already filled"): python-dotenv has been a declared dependency
# since Grand Design v1.2 §12.3, but nothing in src/ or scripts/ ever
# called load_dotenv() — confirmed by grep, zero hits repo-wide before
# this fix. A filled .env file was never actually reaching os.getenv()
# in ANY bronze ingester unless the shell had separately exported those
# variables. This is the single load-bearing fix: every job dispatched
# through this CLI (i.e. all of production) now gets .env loaded once,
# here, before any job function runs. Preflight scripts are separate
# entry points that bypass runner.py entirely — each needs its own
# load_dotenv() call too (added directly to each of the 5 scripts).
load_dotenv()

from src.scheduler.dependency_guard import DependencyGuard
from src.scheduler.job_registry import (
    JOB_REGISTRY,
    PIPELINE_SEQUENCE,
    DAILY_SEQUENCE,
    WEEKLY_SEQUENCE,
    _passes_schedule,
)
from src.utils.pipeline_logger import PipelineLogger
from src.utils.progress_checkpoint import ProgressCheckpoint

# ── Global instances ──────────────────────────────────────────────────────────
pipeline_logger = PipelineLogger()
guard = DependencyGuard(sentinel_dir=Path("data/.sentinels"))


# ── Core Runner ───────────────────────────────────────────────────────────────

def run_job(
    job_name: str,
    force: bool = False,
    reset: bool = False,
    run_date: date | None = None,
) -> None:
    """
    Execute satu job dengan dependency check dan schedule guard.

    Args:
        job_name:  Job key dari JOB_REGISTRY
        force:     Skip dependency check jika True
        reset:     Clear ProgressCheckpoint sebelum run (G1×G6)
        run_date:  Override date; default = today
    """
    run_date = run_date or date.today()

    if job_name not in JOB_REGISTRY:
        pipeline_logger.error(
            f"Job '{job_name}' tidak ditemukan. Gunakan --list untuk melihat daftar."
        )
        sys.exit(1)

    job = JOB_REGISTRY[job_name]

    # G5: Schedule guard — skip jika hari ini bukan jadwal job
    if not force and not _passes_schedule(job, run_date):
        pipeline_logger.warn(
            f"Job '{job_name}' skipped — schedule constraint tidak terpenuhi"
            f" untuk {run_date}. Gunakan --force untuk bypass."
        )
        return

    # G6: Reset ProgressCheckpoint jika --reset flag
    if reset:
        checkpoint = ProgressCheckpoint(job_name, run_date)
        checkpoint.clear(run_date)
        logger.info(f"[Runner] Checkpoint cleared for {job_name} on {run_date}")

    # Dependency check
    # FIX NEW-1: pass job's stale_tolerance (if configured) so cross-cadence
    # dependencies (e.g. silver_macro, weekly cadence per GD §3.3.1) use a
    # staleness window instead of exact-date match. See dependency_guard.py.
    if not force and job.get("depends_on"):
        missing = guard.check_dependencies(
            job["depends_on"],
            run_date,
            stale_tolerance=job.get("stale_tolerance"),
        )
        if missing:
            pipeline_logger.warn(
                f"Dependency belum selesai untuk '{job_name}': {missing}\n"
                f"         Jalankan upstream job terlebih dahulu,"
                f" atau gunakan --force untuk bypass."
            )
            sys.exit(1)

    # Execute
    pipeline_logger.start(job_name, job["description"])
    try:
        job["fn"](run_date)
        guard.mark_done(job_name, run_date)
        pipeline_logger.success(
            job_name,
            layer=job.get("layer", ""),
        )
    except Exception as e:
        pipeline_logger.failure(
            job_name,
            str(e),
            layer=job.get("layer", ""),
        )
        raise


def run_all(force: bool = False, run_date: date | None = None) -> None:
    """
    Jalankan seluruh PIPELINE_SEQUENCE secara sequential.
    Jobs dengan schedule constraint yang tidak terpenuhi di-skip otomatis.

    FIX R-F02: parameter force diteruskan ke setiap run_job() dalam loop.
    Sebelumnya, run_all(force=True) tidak berpengaruh karena loop memanggil
    run_job dengan force=False hardcoded — membuat --force all tidak berfungsi.
    """
    run_date = run_date or date.today()
    pipeline_logger.banner(f"FULL PIPELINE RUN — {run_date}")

    for job_name in PIPELINE_SEQUENCE:
        run_job(job_name, force=force, run_date=run_date)  # FIX R-F02


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Alpha Factory — Manual Runner (GD §14.3.1)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python runner.py --job bronze_ohlcv_daily
  python runner.py --job all
  python runner.py --job silver_ohlcv --force
  python runner.py --job all --date 2026-05-13
  python runner.py --list
  python runner.py --status
  python runner.py --dashboard
  python runner.py --views
  python runner.py --reset gold_regime
  python runner.py --reset-all
        """,
    )
    p.add_argument("--job",       type=str,  help="Job name to run (or 'all')")
    p.add_argument("--list",      action="store_true", help="List all available jobs")
    p.add_argument("--status",    action="store_true", help="Show today's job completion status")
    p.add_argument("--force",     action="store_true", help="Skip dependency + schedule check")
    p.add_argument("--reset",     type=str,  default=None, help="Reset sentinel for one job")
    p.add_argument("--reset-all", action="store_true",    help="Reset all sentinels for today")
    p.add_argument("--dashboard", action="store_true",    help="Show pipeline health dashboard")
    p.add_argument("--views",     action="store_true",    help="Register DuckDB views")
    p.add_argument(
        "--date",
        type=str,
        default=None,
        help="Override run date YYYY-MM-DD (default: today)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run_date = (
        date.fromisoformat(args.date) if args.date else date.today()
    )

    if args.list:
        pipeline_logger.print_job_list(JOB_REGISTRY)

    elif args.status:
        statuses = guard.get_all_statuses(
            list(JOB_REGISTRY.keys()), run_date
        )
        pipeline_logger.print_status_table(statuses, run_date)

    elif args.dashboard:
        from src.utils.pipeline_dashboard import render_dashboard
        render_dashboard(run_date)

    elif args.views:
        from src.gold.views import register_views
        register_views(run_date)

    elif args.reset_all:
        count = guard.reset_all(run_date)
        pipeline_logger.info(f"Reset {count} sentinels untuk {run_date}")

    elif args.reset:
        guard.reset_job(args.reset, run_date)

    elif args.job == "all":
        run_all(force=args.force, run_date=run_date)
        # Auto-register DuckDB views after full pipeline run
        try:
            from src.gold.views import register_views
            register_views(run_date)
        except Exception as e:
            pipeline_logger.warn(f"Views registration skipped: {e}")

    elif args.job:
        run_job(
            args.job,
            force=args.force,
            run_date=run_date,
        )

    else:
        pipeline_logger.error(
            "Tidak ada perintah. Gunakan --help untuk bantuan."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
