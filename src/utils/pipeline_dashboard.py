"""
pipeline_dashboard.py — Pipeline Health Dashboard
Rich terminal dashboard: layer coverage, data freshness, storage, job history.

Usage:
    python -m src.utils.pipeline_dashboard
    python -m src.utils.pipeline_dashboard --date 2026-05-22
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

from src.utils.silver_scope import layer1_globs, context_glob

DB_PATH          = Path("data/health/pipeline_runs.db")
PROGRESS_DB_PATH = Path("data/health/progress.db")

# Storage thresholds (GD §15.2)
STORAGE_RED_GB   = 70.0
STORAGE_WARN_GB  = 150.0


def _c(text: str, code: str) -> str:
    """Apply ANSI colour only if stdout is a TTY."""
    import sys
    return f"{code}{text}\033[0m" if sys.stdout.isatty() else text


GREEN  = "\033[92m"
RED    = "\033[91m"
AMBER  = "\033[93m"
BLUE   = "\033[94m"
GRAY   = "\033[90m"
BOLD   = "\033[1m"
CYAN   = "\033[96m"


def _bar(fraction: float, width: int = 20, filled: str = "█", empty: str = "░") -> str:
    filled_n = int(round(fraction * width))
    return filled * filled_n + empty * (width - filled_n)


def render_dashboard(run_date: date) -> None:
    """Print full dashboard to terminal."""
    print()
    print(_c("=" * 70, BOLD))
    print(_c(f"  PIPELINE HEALTH DASHBOARD — {run_date}", BOLD + BLUE))
    print(_c("=" * 70, BOLD))

    _section_job_status(run_date)
    _section_layer_coverage()
    _section_storage()
    _section_recent_failures(run_date)
    _section_data_freshness()

    print(_c("=" * 70, BOLD))
    print()


def _section_job_status(run_date: date) -> None:
    """Show today's job completion status from DependencyGuard sentinels."""
    from src.scheduler.dependency_guard import DependencyGuard
    from src.scheduler.job_registry import JOB_REGISTRY, PIPELINE_SEQUENCE

    print(_c("\n  JOB STATUS", BOLD + CYAN))
    print("  " + "-" * 66)

    guard    = DependencyGuard()
    statuses = guard.get_all_statuses(list(JOB_REGISTRY.keys()), run_date)

    # Show in pipeline sequence order, then remaining
    ordered  = PIPELINE_SEQUENCE + [j for j in JOB_REGISTRY if j not in PIPELINE_SEQUENCE]

    done_count = 0
    for job_name in ordered:
        if job_name not in statuses:
            continue
        done     = statuses[job_name]
        job      = JOB_REGISTRY[job_name]
        layer    = job.get("layer", "")
        est      = job.get("est_minutes", 0)

        layer_colors = {
            "bronze": AMBER,
            "silver": BLUE,
            "gold":   "\033[38;5;220m",
            "util":   GRAY,
        }
        layer_str = _c(f"[{layer.upper():<7}]", layer_colors.get(layer, GRAY))

        if done:
            status_str = _c("✓ DONE   ", GREEN)
            done_count += 1
        else:
            status_str = _c("○ PENDING", AMBER)

        print(f"  {status_str} {layer_str} {job_name:<35} ~{est}m")

    total = len(statuses)
    pct   = done_count / total if total else 0
    bar   = _bar(pct, width=30)
    colour = GREEN if pct >= 0.9 else (AMBER if pct >= 0.5 else RED)
    print(f"\n  {_c(bar, colour)} {done_count}/{total} completed ({pct:.0%})\n")


def _section_layer_coverage() -> None:
    """Show data coverage per layer.

    FIX ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8,
    2026-07-11): "Silver OHLCV" previously used a single unfiltered
    'market_ohlcv/**/*.parquet' glob — the same RISK-6 defect class fixed
    elsewhere, here lower-severity (a display-only diagnostic count, not a
    gate or computation) but still worth splitting out for accuracy: an
    operator reading "Silver OHLCV: N files" has no way to tell how much
    of that is Layer 1 (tradeable) vs. Layer 2 (context anchors, added in
    GMI Cycle 3) without this split — genuinely more informative, not
    just gate-compliance busywork.
    """
    print(_c("  DATA COVERAGE", BOLD + CYAN))
    print("  " + "-" * 66)

    silver_root = Path("data/silver/market_ohlcv")
    layer1_ohlcv_globs = layer1_globs(silver_root, "*.parquet")
    layer2_ohlcv_glob = context_glob(silver_root, "*.parquet")

    layers = [
        ("Bronze OHLCV",  ["data/bronze/market/ohlcv/**/*.parquet"], AMBER),
        ("Silver OHLCV (Layer 1)", layer1_ohlcv_globs, BLUE),
        ("Silver OHLCV (Layer 2 context)",
         [layer2_ohlcv_glob] if layer2_ohlcv_glob else [], BLUE),
        ("Silver Macro",  ["data/silver/macro_enriched/**/*.parquet"], BLUE),
        ("Gold Signals",  ["data/gold/signals/*.parquet"], "\033[38;5;220m"),
        ("Gold Regime",   ["data/gold/macro/regime_store.parquet"], "\033[38;5;220m"),
        ("Gold Watchlist", ["data/gold/screener/*.parquet"], "\033[38;5;220m"),
    ]

    for label, glob_strs, colour in layers:
        files = [f for glob_str in glob_strs for f in Path(".").glob(glob_str)]
        if files:
            total_size = sum(f.stat().st_size for f in files if f.exists())
            size_mb    = total_size / 1024 / 1024
            file_count = len(files)
            status     = _c(f"{file_count:4d} files, {size_mb:7.1f} MB", colour)
        else:
            status = _c("  no data yet", GRAY)
        print(f"  {label:<32} {status}")

    print()


def _section_storage() -> None:
    """Show disk usage and storage health."""
    print(_c("  STORAGE", BOLD + CYAN))
    print("  " + "-" * 66)

    try:
        usage  = shutil.disk_usage("/")
        free   = usage.free  / 1024 ** 3
        total  = usage.total / 1024 ** 3
        used   = usage.used  / 1024 ** 3
        pct    = usage.used  / usage.total

        bar_colour = GREEN if free > STORAGE_WARN_GB else (
            AMBER if free > STORAGE_RED_GB else RED
        )
        bar = _bar(pct, width=30)

        print(f"  Disk:  {_c(bar, bar_colour)} {used:.1f}GB used / {total:.1f}GB total")
        if free <= STORAGE_RED_GB:
            print(_c(f"  ⚠  ALERT: Only {free:.1f}GB free! Archive old Bronze data.", RED))
        elif free <= STORAGE_WARN_GB:
            print(_c(f"  ⚠  Warning: {free:.1f}GB free (below 150GB threshold)", AMBER))
        else:
            print(_c(f"  ✓  {free:.1f}GB free", GREEN))

        # Pipeline data directory sizes
        data_path = Path("data")
        if data_path.exists():
            for subdir in ["bronze", "silver", "gold"]:
                p = data_path / subdir
                if p.exists():
                    size = sum(
                        f.stat().st_size for f in p.rglob("*") if f.is_file()
                    ) / 1024 ** 3
                    print(f"  data/{subdir:<8} {size:.2f} GB")
    except Exception as e:
        print(_c(f"  Storage check failed: {e}", RED))

    print()


def _section_recent_failures(run_date: date) -> None:
    """Show failed jobs from ProgressCheckpoint."""
    print(_c("  RECENT FAILURES", BOLD + CYAN))
    print("  " + "-" * 66)

    if not PROGRESS_DB_PATH.exists():
        print(_c("  No progress database yet", GRAY))
        print()
        return

    try:
        con  = sqlite3.connect(PROGRESS_DB_PATH)
        rows = con.execute("""
            SELECT job_name, run_date, symbol, timeframe, error_msg,
                   ts
            FROM symbol_progress
            WHERE status = 'failed'
              AND run_date >= ?
            ORDER BY ts DESC
            LIMIT 15
        """, [(run_date - timedelta(days=7)).isoformat()]).fetchall()
        con.close()

        if not rows:
            print(_c("  ✓ No failures in last 7 days", GREEN))
        else:
            print(_c(f"  {len(rows)} failures in last 7 days:", AMBER))
            for r in rows[:10]:
                tf_str    = f"/{r[3]}" if r[3] else ""
                err_short = (r[4] or "")[:60]
                print(
                    f"  {_c('✗', RED)} {r[1]} | {r[0]} | {r[2]}{tf_str}"
                )
                if err_short:
                    print(_c(f"      {err_short}", GRAY))
    except Exception as e:
        print(_c(f"  Progress DB read failed: {e}", RED))

    print()


def _section_data_freshness() -> None:
    """Show latest data timestamps across key datasets."""
    print(_c("  DATA FRESHNESS", BOLD + CYAN))
    print("  " + "-" * 66)

    checks = [
        ("Silver 1D (AAPL)",
         "data/silver/market_ohlcv/us_stocks/symbol=AAPL/*_1D_silver.parquet"),
        ("Gold Regime",
         "data/gold/macro/regime_store.parquet"),
        ("Gold Watchlist",
         "data/gold/screener/watchlist_*.parquet"),
        ("Active Symbols",
         "data/silver/active_symbols/active_*.parquet"),
    ]

    today = date.today()
    for label, glob_str in checks:
        files = sorted(Path(".").glob(glob_str))
        if not files:
            print(f"  {label:<25} {_c('not found', GRAY)}")
            continue

        latest = max(files, key=lambda f: f.stat().st_mtime)
        import datetime
        mtime    = datetime.datetime.fromtimestamp(latest.stat().st_mtime)
        age_days = (datetime.datetime.now() - mtime).days
        age_str  = f"{age_days}d ago" if age_days > 0 else "today"

        colour = GREEN if age_days <= 1 else (AMBER if age_days <= 3 else RED)
        print(
            f"  {label:<25} {_c(mtime.strftime('%Y-%m-%d %H:%M'), colour)}"
            f"  {_c(age_str, colour)}"
        )

    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Health Dashboard")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Override date YYYY-MM-DD (default: today)",
    )
    args     = parser.parse_args()
    run_date = date.fromisoformat(args.date) if args.date else date.today()
    render_dashboard(run_date)


if __name__ == "__main__":
    main()
