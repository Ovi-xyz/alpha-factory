"""
pipeline_logger.py — IDD §2 (Implementation Detail Document v1.0)
Rich terminal output + SQLite audit trail untuk Manual Runner.

Semua job events ditulis ke pipeline_runs.db yang dikonsumsi oleh health_reporter.
Menyediakan semua method yang direferensikan di runner.py:
  start(), success(), failure(), banner(), warn(), error(),
  print_job_list(), print_status_table()
"""

import sqlite3
import sys
import time
from datetime import date, datetime
from pathlib import Path

from loguru import logger as loguru_logger


# ── ANSI Color Codes ──────────────────────────────────────────────────────────
# Graceful degradation ke plain text di non-TTY environments

BOLD  = "\033[1m"
GREEN = "\033[92m"
RED   = "\033[91m"
AMBER = "\033[93m"
BLUE  = "\033[94m"
GRAY  = "\033[90m"
RESET = "\033[0m"

_LAYER_COLORS = {
    "bronze": "\033[38;5;172m",    # orange
    "silver": "\033[38;5;110m",    # light blue
    "gold":   "\033[38;5;220m",    # yellow-gold
    "util":   GRAY,
}

DB_PATH = Path("data/health/pipeline_runs.db")


def _c(text: str, code: str) -> str:
    """Apply ANSI color only if stdout is a TTY."""
    return f"{code}{text}{RESET}" if sys.stdout.isatty() else text


class PipelineLogger:
    """
    Rich terminal output + SQLite audit trail.

    - Terminal: colored status lines per job event
    - SQLite:   pipeline_runs table (GD Section 13.2) untuk health_reporter
    """

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._start_times: dict[str, float] = {}

    # ── Schema Init ───────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id       TEXT NOT NULL,
                    run_date     TEXT NOT NULL,
                    layer        TEXT,
                    source       TEXT,
                    symbol       TEXT,
                    timeframe    TEXT,
                    status       TEXT,
                    rows_written INTEGER DEFAULT 0,
                    duration_sec REAL,
                    error_msg    TEXT,
                    created_at   TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

    # ── Public API ────────────────────────────────────────────────────────────

    def banner(self, title: str) -> None:
        """Print section header."""
        line = "=" * 60
        print(_c(f"\n{line}", BOLD))
        print(_c(f"  {title}", BOLD + BLUE))
        print(_c(line, BOLD))

    def start(self, job_name: str, description: str) -> None:
        """Log job start — records timestamp for duration tracking."""
        self._start_times[job_name] = time.monotonic()
        ts = datetime.now().strftime("%H:%M:%S")
        print(
            _c(f"[{ts}]", GRAY)
            + f"  {_c('RUNNING', BLUE)}  {job_name}"
        )
        print(_c(f"         {description}", GRAY))

    def success(
        self,
        job_name: str,
        rows_written: int = 0,
        layer: str = "",
        source: str = "",
    ) -> None:
        """Log job success with duration."""
        dur = self._elapsed(job_name)
        ts  = datetime.now().strftime("%H:%M:%S")
        print(
            _c(f"[{ts}]", GRAY)
            + f"  {_c('SUCCESS', GREEN)}  {job_name}"
            + _c(f"  ({dur:.1f}s)", GRAY)
        )
        if rows_written:
            print(_c(f"         {rows_written:,} rows written", GRAY))
        self._write_db(job_name, "success", rows_written, dur, None, layer, source)

    def failure(
        self,
        job_name: str,
        error_msg: str,
        layer: str = "",
        source: str = "",
    ) -> None:
        """Log job failure with error message."""
        dur = self._elapsed(job_name)
        ts  = datetime.now().strftime("%H:%M:%S")
        print(
            _c(f"[{ts}]", GRAY)
            + f"  {_c('FAILED ', RED)}  {job_name}"
        )
        print(_c(f"         ERROR: {error_msg[:120]}", RED))
        self._write_db(job_name, "failed", 0, dur, error_msg[:500], layer, source)

    def warn(self, message: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        print(_c(f"[{ts}]", GRAY) + f"  {_c('WARN   ', AMBER)}  {message}")

    def error(self, message: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        print(_c(f"[{ts}]", GRAY) + f"  {_c('ERROR  ', RED)}  {message}")

    def info(self, message: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        print(_c(f"[{ts}]", GRAY) + f"         {message}")

    def print_job_list(self, registry: dict) -> None:
        """Tampilkan semua job dengan dependency dan estimasi durasi."""
        self.banner("AVAILABLE JOBS")
        print(
            f"{'Job Name':<35} {'Layer':<10} {'Est.':<8} Dependencies"
        )
        print("-" * 85)
        for name, job in registry.items():
            deps  = ", ".join(job.get("depends_on", [])) or "—"
            layer = job.get("layer", "")
            layer_colored = _c(layer.upper()[:6], _LAYER_COLORS.get(layer, RESET))
            est   = job.get("est_minutes", "?")
            print(
                f"{name:<35} {layer_colored:<20} {est}m{'':<5} {deps}"
            )

    def print_status_table(
        self,
        statuses: dict[str, bool],
        run_date: date,
    ) -> None:
        """Tampilkan status DONE / PENDING semua job untuk run_date."""
        self.banner(f"PIPELINE STATUS — {run_date}")
        for name, done in statuses.items():
            icon = _c("DONE   ", GREEN) if done else _c("PENDING", AMBER)
            print(f"  {icon}  {name}")
        done_count = sum(statuses.values())
        total = len(statuses)
        print(_c(f"\n  {done_count}/{total} jobs completed", BOLD))

    # ── Internal ──────────────────────────────────────────────────────────────

    def _elapsed(self, job_name: str) -> float:
        start = self._start_times.pop(job_name, time.monotonic())
        return time.monotonic() - start

    def _write_db(
        self,
        job_name: str,
        status: str,
        rows: int,
        dur: float,
        error: str | None,
        layer: str,
        source: str,
    ) -> None:
        run_date = date.today().isoformat()
        with sqlite3.connect(self.db_path) as con:
            con.execute(
                "INSERT INTO pipeline_runs"
                " (run_id, run_date, layer, source, status,"
                "  rows_written, duration_sec, error_msg)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (
                    job_name,
                    run_date,
                    layer,
                    source,
                    status,
                    rows,
                    round(dur, 2),
                    error,
                ),
            )
