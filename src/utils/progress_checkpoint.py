"""
progress_checkpoint.py — G6 Supplementary Design v1.1
Track symbol-level (dan opsional timeframe-level) progress untuk partial run recovery.

FIX v1.1:
  - timeframe sebagai optional key dimension (kritis untuk gold_signals)
  - error_msg column untuk diagnostik (tidak perlu scan log manual)
  - clear() method untuk force-rerun

Usage:
    checkpoint = ProgressCheckpoint('gold_signals', run_date)
    for tf in TIMEFRAMES:
        for symbol in pending_symbols:
            if checkpoint.is_done(symbol, timeframe=tf):
                continue
            try:
                process(symbol, tf)
                checkpoint.mark_done(symbol, timeframe=tf)
            except Exception as e:
                checkpoint.mark_failed(symbol, e, timeframe=tf)
"""

import sqlite3
from datetime import date
from pathlib import Path

from loguru import logger


class ProgressCheckpoint:
    """
    Track symbol-level (dan opsional timeframe-level) progress.
    Backed by SQLite — persists across process restarts.

    Primary key: (job_name, run_date, symbol, timeframe)
    """

    DB_PATH: Path = Path("data/health/progress.db")

    def __init__(self, job_name: str, run_date: date) -> None:
        self.job_name = job_name
        self.run_date = run_date.isoformat()
        self.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ── Schema Init ───────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with sqlite3.connect(self.DB_PATH) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS symbol_progress (
                    job_name  TEXT NOT NULL,
                    run_date  TEXT NOT NULL,
                    symbol    TEXT NOT NULL,
                    timeframe TEXT NOT NULL DEFAULT '',
                    status    TEXT NOT NULL,
                    error_msg TEXT,
                    ts        TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (job_name, run_date, symbol, timeframe)
                )
            """)
            # Index for quick pending_symbols lookup
            con.execute("""
                CREATE INDEX IF NOT EXISTS idx_progress_lookup
                ON symbol_progress (job_name, run_date, timeframe, status)
            """)

    # ── Core API ──────────────────────────────────────────────────────────────

    def is_done(self, symbol: str, timeframe: str = "") -> bool:
        """Return True jika (symbol, timeframe) sudah selesai di job/run_date ini."""
        with sqlite3.connect(self.DB_PATH) as con:
            row = con.execute(
                "SELECT 1 FROM symbol_progress"
                " WHERE job_name=? AND run_date=? AND symbol=?"
                "   AND timeframe=? AND status='done'",
                (self.job_name, self.run_date, symbol, timeframe),
            ).fetchone()
        return row is not None

    def mark_done(self, symbol: str, timeframe: str = "") -> None:
        """Mark (symbol, timeframe) sebagai selesai."""
        with sqlite3.connect(self.DB_PATH) as con:
            con.execute(
                "INSERT OR REPLACE INTO symbol_progress"
                " VALUES (?,?,?,?,'done',NULL,CURRENT_TIMESTAMP)",
                (self.job_name, self.run_date, symbol, timeframe),
            )

    def mark_failed(
        self,
        symbol: str,
        error: Exception,
        timeframe: str = "",
    ) -> None:
        """
        Mark (symbol, timeframe) sebagai failed.
        FIX v1.1: simpan error message untuk diagnostik — tidak perlu scan log.
        """
        with sqlite3.connect(self.DB_PATH) as con:
            con.execute(
                "INSERT OR REPLACE INTO symbol_progress"
                " VALUES (?,?,?,?,'failed',?,CURRENT_TIMESTAMP)",
                (
                    self.job_name,
                    self.run_date,
                    symbol,
                    timeframe,
                    str(error)[:500],  # truncate untuk DB safety
                ),
            )
        logger.warning(
            f"[Checkpoint] {self.job_name} | {symbol}"
            f"{'/' + timeframe if timeframe else ''} → FAILED: {error}"
        )

    def pending_symbols(
        self,
        all_symbols: list[str],
        timeframe: str = "",
    ) -> list[str]:
        """
        Return symbols yang belum done untuk timeframe ini.
        Gunakan untuk resume setelah crash.
        """
        return [s for s in all_symbols if not self.is_done(s, timeframe)]

    # ── Maintenance ───────────────────────────────────────────────────────────

    def clear(self, run_date: date | None = None) -> None:
        """
        Reset checkpoint.
        - run_date specified: hapus hanya run_date tersebut
        - run_date None: hapus SEMUA checkpoint untuk job ini

        Gunakan sebelum force-rerun seluruh job pada hari yang sama.
        G1×G6 Cross-gap: ini adalah --reset flag handler di runner.py.
        """
        with sqlite3.connect(self.DB_PATH) as con:
            if run_date is not None:
                con.execute(
                    "DELETE FROM symbol_progress"
                    " WHERE job_name=? AND run_date=?",
                    (self.job_name, run_date.isoformat()),
                )
                logger.info(
                    f"[Checkpoint] Cleared {self.job_name} for {run_date}"
                )
            else:
                con.execute(
                    "DELETE FROM symbol_progress WHERE job_name=?",
                    (self.job_name,),
                )
                logger.info(
                    f"[Checkpoint] Cleared ALL checkpoints for {self.job_name}"
                )

    # ── Reporting ─────────────────────────────────────────────────────────────

    def summary(self) -> dict[str, int]:
        """Return {status: count} untuk job/run_date ini."""
        with sqlite3.connect(self.DB_PATH) as con:
            rows = con.execute(
                "SELECT status, COUNT(*) FROM symbol_progress"
                " WHERE job_name=? AND run_date=? GROUP BY status",
                (self.job_name, self.run_date),
            ).fetchall()
        return {r[0]: r[1] for r in rows}

    def failed_report(self) -> list[dict]:
        """
        Return list of {symbol, timeframe, error_msg} untuk semua failed symbols.
        Dipanggil oleh health reporter setiap akhir pipeline run.
        """
        with sqlite3.connect(self.DB_PATH) as con:
            rows = con.execute(
                "SELECT symbol, timeframe, error_msg FROM symbol_progress"
                " WHERE job_name=? AND run_date=? AND status='failed'",
                (self.job_name, self.run_date),
            ).fetchall()
        return [
            {"symbol": r[0], "timeframe": r[1], "error_msg": r[2]}
            for r in rows
        ]

    def coverage_pct(self, total_expected: int) -> float:
        """
        Return % coverage (done / total_expected).
        Threshold minimum 95% dikonfigurasi di pipeline.yaml.
        """
        s = self.summary()
        done = s.get("done", 0)
        if total_expected == 0:
            return 100.0
        return round(done / total_expected * 100, 2)
