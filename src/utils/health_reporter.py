"""
health_reporter.py — GD §13.4 (NEW v1.2)
Daily pipeline health summary: run stats, storage check, optional Telegram alert.

Membaca dari pipeline_runs.db (diisi oleh PipelineLogger).
Berjalan sebagai job terakhir setelah gold_screener selesai.

Output:
    - Terminal: formatted summary
    - SQLite log: sudah ada via PipelineLogger
    - Telegram: optional alert jika TELEGRAM_BOT_TOKEN di-set

FIX GAP-10 [P3] (Production Readiness Assessment v1.7.2, GD §9.1, §3.3.2):
tvdatafeed WAS a reverse-engineered, unofficial TradingView API (ToS risk,
could break without warning). IDX30 (30 of 643 instruments) depended on it
as primary source, falling back to yfinance .JK. This mitigation originally
surfaced how many of the 30 IDX symbols came from tvdatafeed vs. fell back
to yfinance_jk vs. were missing entirely.

FIX ADR-029 (GMI_Decision_Document_v7.docx, 30 Jul 2026): tvdatafeed
RETIRED entirely (KNOWN_RISKS.md RISK-1 -> RESOLVED). yfinance .JK is now
IDX30's SOLE source, not a fallback -- a tvdatafeed-vs-fallback source
distinction is no longer meaningful; every present symbol would show as
"fallback" under the old schema, permanently over-tripping the alert
regardless of actual health (every healthy run would show ~30 "fallback"
symbols > IDX_COVERAGE_ALERT_THRESHOLD=5, firing IDX_PARTIAL_FAILURE on
EVERY run). _check_idx_coverage() reworked from tvdatafeed-vs-fallback to
presence-vs-missing: it reads Bronze IDX metadata (the `_symbol` column
BronzeIngester always writes, GD §3.6) and surfaces how many of the 30 IDX
symbols have data at all today vs. are missing entirely. Source-of-origin
is no longer tracked since there is only one source.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
from datetime import date
from pathlib import Path

from loguru import logger

DB_PATH = Path("data/health/pipeline_runs.db")
BRONZE_IDX_GLOB = "data/bronze/market/ohlcv/idx/**/*.parquet"

# System health thresholds (GD §15.2)
STORAGE_ALERT_GB   = 70.0   # Alert jika free space < 70 GB
STORAGE_WARN_GB    = 150.0  # Warning zone
FAILED_ALERT_COUNT = 3      # Alert jika > 3 jobs failed

# FIX GAP-10 [P3]: mitigation #2 from the assessment — "Tambahkan
# IDX_COVERAGE_ALERT di health_reporter.py: jika tvdatafeed return None
# untuk > 5 symbols dalam satu run, kirim alert dan log coverage percentage."
IDX_COVERAGE_ALERT_THRESHOLD = 5   # > N symbols not on tvdatafeed -> alert


def run(run_date: date) -> None:
    """Entry point untuk health_report job."""
    report = generate_daily_report(run_date)
    _print_report(report)

    # Optional Telegram alert
    token   = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            send_telegram_alert(report, token, chat_id)
        except Exception as e:
            logger.warning(f"[HealthReporter] Telegram alert failed: {e}")


def generate_daily_report(run_date: date) -> dict:
    """
    Build daily health report dict.
    Return structured report untuk display dan alerting.
    """
    today = run_date.isoformat()
    report: dict = {
        "date":             today,
        "pipeline_summary": [],
        "storage_free_gb":  None,
        "storage_alert":    False,
        "storage_warn":     False,
        "total_runs":       0,
        "total_failed":     0,
        "total_rows":       0,
        "avg_duration_sec": 0.0,
        # FIX GAP-10 [P3] + FIX ADR-029: IDX coverage fields, populated by
        # _check_idx_coverage(). Reworked to presence-vs-missing (30 Jul 2026) --
        # idx_tvdatafeed_count/idx_fallback_count REMOVED (source-of-origin no
        # longer meaningful, single source only). See module docstring.
        "idx_total":          0,
        "idx_present_count":  0,
        "idx_missing_count":  0,
        "idx_coverage_pct":   None,
        "idx_coverage_alert": False,
    }

    # ── Pipeline summary from SQLite ──────────────────────────────────────────
    if DB_PATH.exists():
        try:
            con = sqlite3.connect(DB_PATH)
            rows = con.execute("""
                SELECT
                    run_id    AS job,
                    layer,
                    COUNT(*)                      AS total_runs,
                    SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success,
                    SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END) AS failed,
                    AVG(duration_sec)             AS avg_duration_sec,
                    SUM(COALESCE(rows_written, 0)) AS total_rows
                FROM pipeline_runs
                WHERE run_date = ?
                GROUP BY run_id, layer
                ORDER BY layer, job
            """, [today]).fetchall()
            con.close()

            report["pipeline_summary"] = [
                {
                    "job":             r[0],
                    "layer":           r[1],
                    "total_runs":      r[2],
                    "success":         r[3],
                    "failed":          r[4],
                    "avg_duration_sec": round(r[5] or 0, 1),
                    "total_rows":      r[6],
                }
                for r in rows
            ]

            # Aggregate totals
            report["total_runs"]      = sum(r["total_runs"] for r in report["pipeline_summary"])
            report["total_failed"]    = sum(r["failed"] for r in report["pipeline_summary"])
            report["total_rows"]      = sum(r["total_rows"] for r in report["pipeline_summary"])
            if report["pipeline_summary"]:
                report["avg_duration_sec"] = round(
                    sum(r["avg_duration_sec"] for r in report["pipeline_summary"])
                    / len(report["pipeline_summary"]), 1
                )

        except Exception as e:
            logger.warning(f"[HealthReporter] DB read failed: {e}")

    # ── Storage check ─────────────────────────────────────────────────────────
    try:
        usage = shutil.disk_usage("/")
        free_gb = usage.free / 1024 ** 3
        report["storage_free_gb"] = round(free_gb, 1)
        report["storage_alert"]   = free_gb < STORAGE_ALERT_GB
        report["storage_warn"]    = free_gb < STORAGE_WARN_GB
    except Exception as e:
        logger.warning(f"[HealthReporter] Storage check failed: {e}")

    # ── IDX coverage check (FIX GAP-10 [P3]) ────────────────────────────────
    report.update(_check_idx_coverage(run_date))

    return report


def _check_idx_coverage(run_date: date) -> dict:
    """
    FIX ADR-029 (GMI_Decision_Document_v7.docx, 30 Jul 2026): reworked from
    tvdatafeed-vs-fallback to presence-vs-missing after tvdatafeed's
    retirement (KNOWN_RISKS.md RISK-1 -> RESOLVED). yfinance .JK is now
    IDX30's SOLE source -- a source-of-origin distinction is no longer
    meaningful, and the old schema would have permanently over-tripped the
    alert (every present symbol showing as "fallback", tripping
    IDX_COVERAGE_ALERT_THRESHOLD on every single healthy run).

    Counts, for today's Bronze IDX ingestion, how many of the 30 IDX symbols
    have data at all (idx_present_count) vs. have none (idx_missing_count).

    Reads the `_symbol` column every Bronze file already carries
    (BronzeIngester.write(), GD §3.6) — no new write path or schema change
    required.

    Returns a dict merged into generate_daily_report()'s report. Mirrors
    the original IDD §6.3 SOP note ("Jika lebih dari 5 IDX symbols return
    None dalam satu run ... log warning 'IDX_PARTIAL_FAILURE'"), now
    applied to missing-only rather than missing+fallback.
    """
    result = {
        "idx_total":          0,
        "idx_present_count":  0,
        "idx_missing_count":  0,
        "idx_coverage_pct":   None,
        "idx_coverage_alert": False,
    }

    try:
        from src.config.instrument_loader import get_loader
        idx_symbols = {inst.symbol for inst in get_loader().by_market("idx")}
    except Exception as e:
        logger.debug(f"[HealthReporter] IDX coverage check skipped (InstrumentLoader): {e}")
        return result

    if not idx_symbols:
        return result

    result["idx_total"] = len(idx_symbols)

    try:
        import duckdb

        con = duckdb.connect()
        rows = con.execute(
            """
            SELECT DISTINCT _symbol AS symbol
            FROM read_parquet($glob, hive_partitioning=true)
            WHERE CAST(_ingested_at AS DATE) = $run_date
            """,
            {"glob": BRONZE_IDX_GLOB, "run_date": run_date},
        ).fetchall()
    except Exception as e:
        logger.debug(f"[HealthReporter] IDX coverage check skipped (no Bronze data yet): {e}")
        return result

    present_symbols = {r[0] for r in rows} & idx_symbols
    present_count   = len(present_symbols)
    missing_count   = len(idx_symbols - present_symbols)

    result["idx_present_count"] = present_count
    result["idx_missing_count"] = missing_count
    result["idx_coverage_pct"]  = round(
        present_count / result["idx_total"] * 100, 1
    ) if result["idx_total"] else None

    result["idx_coverage_alert"] = missing_count > IDX_COVERAGE_ALERT_THRESHOLD

    if result["idx_coverage_alert"]:
        logger.warning(
            "IDX_PARTIAL_FAILURE: "
            f"{missing_count} of {result['idx_total']} IDX symbols missing "
            f"today — coverage={result['idx_coverage_pct']}% (ADR-029)"
        )
    elif missing_count > 0:
        logger.debug(
            f"[HealthReporter] IDX coverage: {missing_count} symbols missing "
            f"(below alert threshold of {IDX_COVERAGE_ALERT_THRESHOLD})"
        )

    return result


def _print_report(report: dict) -> None:
    """Print formatted health report ke terminal."""
    logger.info("=" * 60)
    logger.info(f"PIPELINE HEALTH REPORT — {report['date']}")
    logger.info("=" * 60)

    # Job summary
    if report["pipeline_summary"]:
        logger.info(
            f"{'Job':<35} {'Layer':<8} {'Success':<9} {'Failed':<8} {'Rows':>10}"
        )
        logger.info("-" * 75)
        for r in report["pipeline_summary"]:
            status = "✓" if r["failed"] == 0 else "✗"
            logger.info(
                f"{status} {r['job']:<33} {r['layer']:<8}"
                f" {r['success']:<9} {r['failed']:<8} {r['total_rows']:>10,}"
            )

    logger.info("-" * 60)
    logger.info(
        f"Total: {report['total_runs']} runs | "
        f"{report['total_failed']} failed | "
        f"{report['total_rows']:,} rows"
    )

    # Storage
    if report["storage_free_gb"] is not None:
        storage_status = ""
        if report["storage_alert"]:
            storage_status = " ⚠️  ALERT — Below 70GB threshold!"
        elif report["storage_warn"]:
            storage_status = " ⚠️  Warning — Below 150GB"
        logger.info(
            f"Storage free: {report['storage_free_gb']:.1f} GB{storage_status}"
        )

    # FIX GAP-10 [P3] + FIX ADR-029: IDX coverage line (presence-vs-missing)
    if report.get("idx_total"):
        idx_status = ""
        if report["idx_coverage_alert"]:
            idx_status = " ⚠️  IDX_PARTIAL_FAILURE — see KNOWN_RISKS.md RISK-1"
        logger.info(
            f"IDX coverage: {report['idx_present_count']}/{report['idx_total']} "
            f"present ({report['idx_coverage_pct']}%) | "
            f"{report['idx_missing_count']} missing{idx_status}"
        )

    logger.info("=" * 60)


def send_telegram_alert(report: dict, token: str, chat_id: str) -> None:
    """Send summary ke Telegram bot (optional)."""
    import httpx

    if report["storage_alert"]:
        msg = (
            f"⚠️ Pipeline {report['date']} | "
            f"Storage ALERT: {report['storage_free_gb']}GB free"
        )
    elif report.get("idx_coverage_alert"):
        # FIX GAP-10 [P3] + FIX ADR-029: IDX coverage alert takes priority over
        # the generic success message, same tier as storage/failed-job alerts.
        # Reworked to presence-vs-missing -- idx_fallback_count no longer exists.
        msg = (
            f"⚠️ Pipeline {report['date']} | "
            f"IDX_PARTIAL_FAILURE: {report['idx_missing_count']}"
            f"/{report['idx_total']} IDX symbols missing "
            f"(coverage={report['idx_coverage_pct']}%)"
        )
    elif report["total_failed"] >= FAILED_ALERT_COUNT:
        msg = (
            f"🔴 Pipeline {report['date']} | "
            f"{report['total_failed']} jobs FAILED"
        )
    else:
        msg = (
            f"✅ Pipeline {report['date']} | "
            f"{report['total_runs']} runs | "
            f"{report['total_failed']} failed | "
            f"{report['total_rows']:,} rows"
        )

    httpx.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": msg},
        timeout=10,
    )
    logger.debug(f"[HealthReporter] Telegram alert sent: {msg}")
