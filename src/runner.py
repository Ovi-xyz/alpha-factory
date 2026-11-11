"""
runner.py — GD §14.3.1 (Manual Runner CLI Entry Point)
CLI entry point untuk Alpha Factory.

Usage:
    python runner.py --job bronze_ohlcv_daily
    python runner.py --job all
    python runner.py --job silver_ohlcv --force
    python runner.py --job gold_context_regime --date 2026-05-13
    python runner.py --list
    python runner.py --status
    python runner.py --reset gold_context_regime
    python runner.py --reset-all

G5: schedule guard via _passes_schedule() — beberapa job hanya berjalan pada
    hari tertentu (EIA=Rabu, BLS NFP=Jumat pertama, dll).

G6: --reset flag untuk clear ProgressCheckpoint sebelum force-rerun.
"""

import argparse
import os
import sys
from datetime import date

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
    LAYER_JOB_NAMES,
    WEEKLY_TAIL,
    jobs_for,
    layer_jobs_for,
    _passes_schedule,
)
from src.utils.pipeline_logger import PipelineLogger
from src.utils.data_paths import sentinels_dir
from src.utils.job_result import JobResult
from src.utils.progress_checkpoint import ProgressCheckpoint

# ── Global instances ──────────────────────────────────────────────────────────
pipeline_logger = PipelineLogger()
guard = DependencyGuard(sentinel_dir=sentinels_dir())   # GHR-01: $ALPHA_FACTORY_DATA_DIR-aware


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
        result = job["fn"](run_date)
        guard.mark_done(job_name, run_date)
        # ADR-059 GHR-03: a job may report what it wrote (JobResult); anything else is "unknown"
        # (NULL), never a silent 0.
        for warning in JobResult.warnings_from(result):
            pipeline_logger.warn(f"{job_name}: {warning}")
        pipeline_logger.success(
            job_name,
            rows_written=JobResult.rows_from(result),
            layer=job.get("layer", ""),
        )
    except Exception as e:
        pipeline_logger.failure(
            job_name,
            str(e),
            layer=job.get("layer", ""),
        )
        raise


def _run_chain(job_names: list[str], *, force: bool, run_date: date) -> None:
    """Run `job_names` in order with the failure semantics shared by `--job all` and the layer runs
    (ADR-060: one implementation, so the commands cannot drift apart); raise the FIRST failure at the end.

    ADD GMI-GATE-001 (ADR-050) — run_on_failure. Sebelumnya run_all() berhenti di job gagal pertama
    (SystemExit dari dependency guard, atau exception dari job itu sendiri), sehingga job yang
    tugasnya MENGAMATI pipeline yang gagal (gatekeeper) justru tidak pernah jalan di hari yang paling
    membutuhkannya. Sekarang: setelah kegagalan pertama, job lain di-skip KECUALI yang ber-flag
    "run_on_failure" di JOB_REGISTRY; kegagalan pertama tetap di-raise di akhir (exit code / tipe
    exception tidak berubah), dan kegagalan job run_on_failure setelahnya TIDAK menimpanya.
    KeyboardInterrupt (BaseException) sengaja tidak ditangkap — operator yang membatalkan tidak
    boleh memicu job apa pun.

    ADD ADR-060 — weekly tail isolation. The weekly tail (cross-asset weeklies, HMM fits, specialist
    fits) runs AFTER the daily chain, so its jobs cannot block daily output; they are also independent
    of each other. A first failure INSIDE the tail therefore does not skip the other tail jobs (one
    failed fit must not hide the rest — a job whose own dependency failed is still refused by the
    dependency guard). A first failure OUTSIDE the tail skips the tail like every other job.
    """
    tail = frozenset(WEEKLY_TAIL)
    first_failure: BaseException | None = None
    first_failure_in_tail = False
    for job_name in job_names:
        if first_failure is not None and not JOB_REGISTRY[job_name].get("run_on_failure", False):
            if not (first_failure_in_tail and job_name in tail):
                continue
        try:
            run_job(job_name, force=force, run_date=run_date)  # FIX R-F02
        except (SystemExit, Exception) as exc:
            if first_failure is None:
                first_failure = exc
                first_failure_in_tail = job_name in tail
                if first_failure_in_tail:
                    pipeline_logger.warn(
                        f"'{job_name}' (weekly tail) gagal — job tail lain tetap dijalankan; "
                        f"kegagalan ini tetap di-raise di akhir."
                    )
                else:
                    pipeline_logger.warn(
                        f"'{job_name}' gagal — sisa job di-skip kecuali yang ber-flag "
                        f"run_on_failure (gatekeeper, health_report). Kegagalan ini tetap di-raise di akhir."
                    )
            else:
                logger.error(
                    f"[Runner] job '{job_name}' juga gagal ({exc!r}) — "
                    f"kegagalan pertama dipertahankan, tidak ditimpa."
                )

    if first_failure is not None:
        raise first_failure


def run_all(force: bool = False, run_date: date | None = None) -> None:
    """
    Jalankan rantai kanonik hari itu — job_registry.jobs_for(run_date): weekly head (due) ->
    DAILY_SEQUENCE -> weekly tail (due) -> util (gatekeeper, health_report). Jobs dengan schedule
    constraint yang tidak terpenuhi tidak masuk daftar.

    ADR-060 (GMI-JOB-ALIGN-001): `--job all` kini menjalankan HIMPUNAN YANG SAMA dengan
    `--job bronze` + `--job silver` + `--job gold` (+ util), dalam urutan yang sama. Sebelumnya
    silver_macro / silver_global_rates dan seluruh job weekly Gold tidak pernah jalan di bawah `all`
    (tebing 7 hari di silver_validate) dan Gatekeeper menilai job weekly yang belum jadwalnya.

    FIX R-F02: parameter force diteruskan ke setiap run_job() dalam loop.
    Sebelumnya, run_all(force=True) tidak berpengaruh karena loop memanggil
    run_job dengan force=False hardcoded — membuat --force all tidak berfungsi.
    `force` tidak menambah job weekly yang tidak jatuh tempo ke daftar `all`.
    """
    run_date = run_date or date.today()
    pipeline_logger.banner(f"FULL PIPELINE RUN — {run_date}")
    _run_chain(jobs_for(run_date), force=force, run_date=run_date)


def run_layer(layer: str, force: bool = False, run_date: date | None = None) -> None:
    """
    ADD GMI-JR-003: jalankan seluruh job pada satu layer (bronze/silver/gold)
    secara sequential — untuk live testing per-layer tanpa `--job all`.

    ADR-060: tanpa --force, daftar job = job_registry.layer_jobs_for(layer, run_date) — rantai kanonik
    yang SAMA dengan `--job all`, dibatasi ke satu layer (urutan sama, hanya job yang jatuh tempo hari
    itu). Dengan --force, daftar = LAYER_JOB_NAMES[layer] (seluruh anggota layer, perilaku lama:
    --force melewati schedule guard). Aturan kegagalan = `_run_chain` (satu implementasi dengan `all`).

    Dependency check antar-layer TETAP berlaku tanpa --force: `--job silver`
    akan sys.exit(1) jika bronze belum jalan hari ini, `--job gold` akan
    sys.exit(1) jika silver belum jalan — ini disengaja (bukan bug), karena
    memverifikasi urutan Bronze -> Silver -> Gold yang benar (GD §17.2 Layer
    Independence Guarantee), persis seperti perilaku menjalankan job
    individual apapun tanpa --force hari ini.
    """
    members = LAYER_JOB_NAMES.get(layer)
    if not members:
        pipeline_logger.error(
            f"Layer '{layer}' tidak dikenali atau tidak punya job terdaftar."
        )
        sys.exit(1)

    run_date = run_date or date.today()
    pipeline_logger.banner(f"{layer.upper()} LAYER RUN — {run_date}")

    if force:
        job_names = list(members)
    else:
        job_names = layer_jobs_for(layer, run_date)
        not_due = [j for j in members if j not in job_names]
        if not_due:
            pipeline_logger.info(
                f"{len(not_due)} job layer '{layer}' tidak jatuh tempo pada {run_date} "
                f"(schedule guard): {', '.join(not_due)}"
            )
    _run_chain(job_names, force=force, run_date=run_date)


def _register_views(run_date: date) -> None:
    """Register the DuckDB views; a failure is a warning, never a failed run (as `--job all` always did)."""
    try:
        from src.gold.views import register_views
        register_views(run_date)
    except Exception as e:
        pipeline_logger.warn(f"Views registration skipped: {e}")


def _after_layer_run(layer: str, run_date: date, *, failed: bool, gate: bool) -> None:
    """ADR-060: the post-steps `--job all` has always run implicitly, for `--job <layer>`.

      - Gatekeeper after a FAILED layer run (record the bad day — until now an operator had to remember
        `--job gatekeeper`) and after a completed `gold` layer run (the chain is whole). NEVER after a
        successful bronze/silver run: that would record the layers not yet run as missing/block for a
        day that is simply not finished. A Gatekeeper failure is logged, never raised (it must not mask
        the layer run's own outcome); `gate=False` (--no-gate) skips it.
      - DuckDB views after a completed `gold` layer run (`all` registers them after a full run).
    """
    if gate and (failed or layer == "gold"):
        try:
            run_job("gatekeeper", force=False, run_date=run_date)
        except (SystemExit, Exception) as exc:
            logger.error(f"[Runner] gatekeeper after '{layer}' layer run failed ({exc!r}) — ignored")
    if layer == "gold" and not failed:
        _register_views(run_date)


def run_layer_command(layer: str, *, force: bool, run_date: date, gate: bool = True) -> None:
    """`--job <layer>`: run_layer + _after_layer_run. The layer run's own exception/SystemExit is
    re-raised unchanged after the post-steps; KeyboardInterrupt triggers nothing."""
    try:
        run_layer(layer, force=force, run_date=run_date)
    except (SystemExit, Exception):
        _after_layer_run(layer, run_date, failed=True, gate=gate)
        raise
    _after_layer_run(layer, run_date, failed=False, gate=gate)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Alpha Factory — Manual Runner (GD §14.3.1)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python runner.py --job bronze_ohlcv_daily
  python runner.py --job bronze
  python runner.py --job silver
  python runner.py --job gold
  python runner.py --job gold --no-gate
  python runner.py --job all
  python runner.py --job silver_ohlcv --force
  python runner.py --job all --date 2026-05-13
  python runner.py --list
  python runner.py --status
  python runner.py --dashboard
  python runner.py --views
  python runner.py --reset gold_context_regime
  python runner.py --reset-all
        """,
    )
    p.add_argument("--job",       type=str,  help="Job name, layer ('bronze'/'silver'/'gold'), or 'all'")
    p.add_argument("--list",      action="store_true", help="List all available jobs")
    p.add_argument("--status",    action="store_true", help="Show today's job completion status")
    p.add_argument("--force",     action="store_true", help="Skip dependency + schedule check")
    p.add_argument("--reset",     type=str,  default=None, help="Reset sentinel for one job")
    p.add_argument("--reset-all", action="store_true",    help="Reset all sentinels for today")
    p.add_argument("--dashboard", action="store_true",    help="Show pipeline health dashboard")
    p.add_argument("--views",     action="store_true",    help="Register DuckDB views")
    p.add_argument("--no-gate",   action="store_true",
                   help="Layer runs: do not auto-run the Gatekeeper after a failed layer run / a completed gold run")
    p.add_argument(
        "--date",
        type=str,
        default=None,
        help="Override run date YYYY-MM-DD (default: today)",
    )
    # ADD GHR-09 (ADR-059): deliberate-pause markers, so a paused chain is `skipped`, not `block`.
    p.add_argument("--pause",  type=str, default=None,
                   help="Pause layer scopes (comma list of bronze,silver,gold) from --date (default today)")
    p.add_argument("--reason", type=str, default="", help="Reason recorded with --pause")
    p.add_argument("--expected-resume", type=str, default=None,
                   help="Expected resume date YYYY-MM-DD recorded with --pause")
    p.add_argument("--resume", type=str, default=None,
                   help="Close the open pause of these layer scopes at --date (default today)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run_date = (
        date.fromisoformat(args.date) if args.date else date.today()
    )

    if args.pause or args.resume:
        from src.utils import pause_markers

        if args.pause:
            pause_markers.add_pause(
                [s.strip() for s in args.pause.split(",") if s.strip()], from_date=run_date,
                reason=args.reason, created_by=os.environ.get("USER", ""),
                expected_resume=date.fromisoformat(args.expected_resume) if args.expected_resume else None,
            )
            pipeline_logger.info(f"Paused {args.pause} from {run_date} ({args.reason or 'no reason given'})")
        if args.resume:
            pause_markers.end_pause([s.strip() for s in args.resume.split(",") if s.strip()], resumed_on=run_date)
            pipeline_logger.info(f"Resumed {args.resume} on {run_date}; earlier paused days stay 'skipped'")

    elif args.list:
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

    elif args.job in LAYER_JOB_NAMES:
        run_layer_command(args.job, force=args.force, run_date=run_date, gate=not args.no_gate)

    elif args.job == "all":
        run_all(force=args.force, run_date=run_date)
        # Auto-register DuckDB views after full pipeline run
        _register_views(run_date)

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
