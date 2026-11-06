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
from datetime import date, datetime, time, timedelta

from loguru import logger
from src.utils.data_paths import health_dir

DB_PATH = health_dir() / "pipeline_runs.db"   # GHR-01: $ALPHA_FACTORY_DATA_DIR-aware
# ADD GMI-GATE-001 (ADR-050): Gatekeeper's output, surfaced alongside the
# existing summary (ADR-050 Consequences). Soft read — see _read_gate_status().
GATE_STATUS_PATH = health_dir() / "gate_status.parquet"
BRONZE_IDX_GLOB = "data/bronze/market/ohlcv/idx/**/*.parquet"

# System health thresholds (GD §15.2)
STORAGE_ALERT_GB   = 70.0   # Alert jika free space < 70 GB
STORAGE_WARN_GB    = 150.0  # Warning zone
FAILED_ALERT_COUNT = 3      # Alert jika > 3 jobs failed

# FIX GAP-10 [P3]: mitigation #2 from the assessment — "Tambahkan
# IDX_COVERAGE_ALERT di health_reporter.py: jika tvdatafeed return None
# untuk > 5 symbols dalam satu run, kirim alert dan log coverage percentage."
IDX_COVERAGE_ALERT_THRESHOLD = 5   # > N symbols not on tvdatafeed -> alert

# GHR-02 (ADR-059 HR1): the operating timezone of the machine that runs the chain. `run_date` is
# `date.today()` on that machine (src/runner.py), i.e. the WIB calendar day, while Bronze's
# `_ingested_at` is a NAIVE UTC timestamp. WIB = UTC+7, no DST. Must match the timezone the runner's
# `date.today()` uses; a chain that starts 21:16 UTC (04:16 WIB the next day) belongs to the WIB day.
LOCAL_UTC_OFFSET_HOURS = 7


def local_day_window_utc(run_date: date) -> tuple[datetime, datetime]:
    """[start, end) of `run_date`'s LOCAL calendar day expressed as NAIVE UTC datetimes.

    WIB midnight of run_date = 17:00 UTC the day before. Naive on purpose: it is compared against
    Bronze's naive-UTC `_ingested_at`."""
    start = datetime.combine(run_date, time.min) - timedelta(hours=LOCAL_UTC_OFFSET_HOURS)
    return start, start + timedelta(days=1)


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


def _zero_row_jobs(con: sqlite3.Connection, run_date: date) -> list[str]:
    """Jobs flagged `must_write_rows` in the registry that SUCCEEDED today yet reported a total of
    0 rows (ADR-059 GHR-03). Weekdays only — a weekend/holiday with nothing new is not a defect.
    A job that never reported a count (rows_known = 0 on every row) is unknown, not zero."""
    if run_date.weekday() >= 5:
        return []
    from src.scheduler.job_registry import JOB_REGISTRY

    flagged = sorted(n for n, j in JOB_REGISTRY.items() if j.get("must_write_rows"))
    out: list[str] = []
    for name in flagged:
        row = con.execute(
            "SELECT COUNT(*), COALESCE(SUM(rows_written), 0) FROM pipeline_runs"
            " WHERE run_date = ? AND run_id = ? AND status = 'success' AND rows_known = 1",
            [run_date.isoformat(), name],
        ).fetchone()
        if row and row[0] > 0 and row[1] == 0:
            out.append(name)
    return out


def _paused_scopes(run_date: date) -> dict:
    """{scope: {since, reason, expected_resume}} for every layer scope paused on run_date
    (ADR-059 GHR-09), so the report says PAUSED rather than looking stale. Never raises."""
    try:
        from src.utils.pause_markers import active_pause, load_pauses

        pauses = load_pauses()
        out = {}
        for scope in pauses:
            p = active_pause(pauses, scope, run_date)
            if p is not None:
                out[scope] = {
                    "since": p.from_date.isoformat(), "reason": p.reason,
                    "expected_resume": p.expected_resume.isoformat() if p.expected_resume else None,
                }
        return out
    except Exception as e:                                   # noqa: BLE001 — a report must not die on this
        logger.warning(f"[HealthReporter] pause markers unreadable ({e})")
        return {}


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
        # ADD GMI-GATE-001 (ADR-050): {scope: {gate_status, layer_health,
        # streak_days}} for run_date, or {} when Gatekeeper hasn't written
        # a row for it (see _read_gate_status()).
        "gate_status":      {},
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
            # ADR-059 GHR-03: only REPORTED counts (rows_known = 1) are summed; a legacy 0 or a job
            # that reported nothing is UNKNOWN, not zero. A DB created before the column existed
            # (no PRAGMA hit) keeps the legacy sum so older fixtures/DBs still read.
            has_known = "rows_known" in {r[1] for r in con.execute("PRAGMA table_info(pipeline_runs)")}
            if has_known:
                rows = con.execute("""
                    SELECT
                        run_id    AS job,
                        layer,
                        COUNT(*)                      AS total_runs,
                        SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success,
                        SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END) AS failed,
                        AVG(duration_sec)             AS avg_duration_sec,
                        SUM(CASE WHEN rows_known = 1 THEN COALESCE(rows_written, 0) ELSE 0 END) AS total_rows,
                        SUM(CASE WHEN rows_known = 1 THEN 1 ELSE 0 END) AS known_runs
                    FROM pipeline_runs
                    WHERE run_date = ?
                    GROUP BY run_id, layer
                    ORDER BY layer, job
                """, [today]).fetchall()
            else:
                rows = con.execute("""
                    SELECT
                        run_id    AS job,
                        layer,
                        COUNT(*)                      AS total_runs,
                        SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success,
                        SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END) AS failed,
                        AVG(duration_sec)             AS avg_duration_sec,
                        SUM(COALESCE(rows_written, 0)) AS total_rows,
                        NULL                           AS known_runs
                    FROM pipeline_runs
                    WHERE run_date = ?
                    GROUP BY run_id, layer
                    ORDER BY layer, job
                """, [today]).fetchall()
            zero_row_jobs = _zero_row_jobs(con, run_date) if has_known else []
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
                    "known_runs":      r[7],     # None = legacy schema, cannot tell
                }
                for r in rows
            ]
            report["zero_row_jobs"] = zero_row_jobs
            if has_known:
                report["rows_known_runs"] = sum(r["known_runs"] or 0 for r in report["pipeline_summary"])

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

    # ── Gatekeeper status (ADD GMI-GATE-001, ADR-050) ────────────────────────
    report["gate_status"] = _read_gate_status(run_date)

    # ── Deliberate pauses (ADD GHR-09, ADR-059) ──────────────────────────────
    report["paused"] = _paused_scopes(run_date)

    return report


def _read_gate_status(run_date: date) -> dict:
    """
    Soft read of data/health/gate_status.parquet for run_date. Returns
    {scope: {gate_status, layer_health, streak_days}}, or {} if the file
    or that day's rows don't exist (Gatekeeper not yet run, or its own
    persistence failed non-fatally) — health_report must never fail
    because a companion job's output is missing.
    """
    if not GATE_STATUS_PATH.exists():
        return {}
    try:
        import polars as pl
        df = pl.read_parquet(GATE_STATUS_PATH).filter(
            pl.col("run_date") == run_date.isoformat()
        )
        return {
            r["scope"]: {
                "gate_status":  r["gate_status"],
                "layer_health": r["layer_health"],
                "streak_days":  r["streak_days"],
            }
            for r in df.to_dicts()
        }
    except Exception as e:
        logger.warning(f"[HealthReporter] gate_status read failed: {e}")
        return {}


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

    # GHR-02 (HR1): the WIB calendar day in UTC — NOT CAST(_ingested_at AS DATE) = run_date, which
    # compared a naive-UTC date with a WIB date and read 0/30 for any chain that starts before 07:00
    # WIB (all 30 files of the 18 Oct run were ingested 21:22 UTC on 17 Oct).
    win_start, win_end = local_day_window_utc(run_date)

    try:
        import duckdb

        con = duckdb.connect()
        rows = con.execute(
            """
            SELECT DISTINCT _symbol AS symbol
            FROM read_parquet($glob, hive_partitioning=true)
            WHERE TRY_CAST(_ingested_at AS TIMESTAMP) >= $win_start
              AND TRY_CAST(_ingested_at AS TIMESTAMP) <  $win_end
            """,
            {"glob": BRONZE_IDX_GLOB, "win_start": win_start, "win_end": win_end},
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
            rows_cell = f"{r['total_rows']:>10,}" if r.get("known_runs", 1) != 0 else f"{'n/a':>10}"
            logger.info(
                f"{status} {r['job']:<33} {r['layer']:<8}"
                f" {r['success']:<9} {r['failed']:<8} {rows_cell}"
            )

    logger.info("-" * 60)
    logger.info(
        f"Total: {report['total_runs']} runs | "
        f"{report['total_failed']} failed | "
        f"{_rows_text(report)}"
    )
    if report.get("zero_row_jobs"):
        logger.warning("ZERO_ROWS (succeeded, reported 0 rows): " + ", ".join(report["zero_row_jobs"]))

    # ADD GHR-09 (ADR-059): a deliberate pause is reported as such, not as staleness
    for scope, info in (report.get("paused") or {}).items():
        resume = f", expected resume {info['expected_resume']}" if info.get("expected_resume") else ""
        logger.info(f"PAUSED: {scope} since {info['since']} ({info.get('reason') or 'no reason given'}{resume})")

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

    # ADD GMI-GATE-001 (ADR-050): Gatekeeper line, only when a row exists.
    gate = report.get("gate_status") or {}
    if gate:
        parts = [
            f"{scope}={gate[scope]['gate_status']}"
            f" (raw {gate[scope]['layer_health']}, streak {gate[scope]['streak_days']}d)"
            for scope in ("bronze", "silver", "gold") if scope in gate
        ]
        logger.info("Gatekeeper: " + " | ".join(parts))

    logger.info("=" * 60)


def collect_alerts(report: dict) -> list[str]:
    """Every ACTIVE alert line for this report, in fixed severity order (storage, IDX, failed jobs,
    gate block, stale/paused). GHR-02 (ADR-059 HR2): the previous `if/elif` chain let one permanent
    alert (the IDX false alarm) hide every later one — failed jobs and the success line included."""
    alerts: list[str] = []
    if report.get("storage_alert"):
        alerts.append(f"⚠️ Storage ALERT: {report.get('storage_free_gb')}GB free")
    if report.get("idx_coverage_alert"):
        alerts.append(
            f"⚠️ IDX_PARTIAL_FAILURE: {report['idx_missing_count']}"
            f"/{report['idx_total']} IDX symbols missing "
            f"(coverage={report['idx_coverage_pct']}%)"
        )
    if report.get("total_failed", 0) >= FAILED_ALERT_COUNT:
        alerts.append(f"🔴 {report['total_failed']} jobs FAILED")
    if report.get("zero_row_jobs"):
        alerts.append("⚠️ ZERO_ROWS: " + ", ".join(report["zero_row_jobs"]) + " succeeded but wrote 0 rows")
    return alerts


def _rows_text(report: dict) -> str:
    """'n/a' when no job reported a count (an unknown total must not read as 0 rows)."""
    if report.get("rows_known_runs") == 0:
        return "rows n/a"
    return f"{report['total_rows']:,} rows"


def compose_alert_message(report: dict) -> str:
    """One Telegram message from all active alerts; the success line ONLY when none is active.

    A single alert keeps the legacy one-line shape ("<icon> Pipeline <date> | <alert>")."""
    alerts = collect_alerts(report)
    date_str = report["date"]
    if not alerts:
        return (
            f"✅ Pipeline {date_str} | "
            f"{report['total_runs']} runs | "
            f"{report['total_failed']} failed | "
            f"{_rows_text(report)}"
        )
    if len(alerts) == 1:
        icon, _, rest = alerts[0].partition(" ")
        return f"{icon} Pipeline {date_str} | {rest}"
    return "\n".join([f"🚨 Pipeline {date_str} | {len(alerts)} alerts", *alerts])


def send_telegram_alert(report: dict, token: str, chat_id: str) -> None:
    """Send summary ke Telegram bot (optional)."""
    import httpx

    msg = compose_alert_message(report)

    httpx.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": msg},
        timeout=10,
    )
    logger.debug(f"[HealthReporter] Telegram alert sent: {msg}")
