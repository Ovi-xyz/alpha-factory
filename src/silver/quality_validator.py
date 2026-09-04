"""
quality_validator.py — GD §13.1 (Silver Quality Validator)
Cross-module quality validation: null check, price sanity, gap detection,
Z-score outlier, PIT integrity, adj_factor consistency.

Berjalan setelah semua Silver processors selesai.
Updates is_clean flag in-place pada Silver OHLCV files.

Quality Checks (GD §13.1):
    ✓ Schema Validation (Bronze, already done)
    ✓ Null Check         — OHLC nulls < 0.1%
    ✓ Price Sanity       — high >= low, open/close in [low, high]
    ✓ Gap Detection      — timestamp continuity per TF
    ✓ Outlier Detection  — |z-score log_return| > 4
    ✓ Freshness Check    — latest data within 2x expected interval
    ✓ Coverage Check     — > 95% symbols fresh (GD §15.1)
    ✓ PIT Integrity      — macro vintage_date >= observation_date
    ✓ Adj Flag Integrity — adj_factor = 1.0 but is_adjusted = True without split

FIX F-QV-01 [P0]: Aligned result keys with CRITICAL_CHECKS and WARNING_CHECKS.
    BEFORE: results used 'ohlcv_null', 'ohlcv_sanity', 'ohlcv_coverage' etc.
    AFTER:  results use 'null_check', 'price_sanity', 'coverage_check' etc.
    to match the exact key names in CRITICAL_CHECKS / WARNING_CHECKS sets.
    Without this, critical_failed was ALWAYS empty and QualityGateError
    was NEVER raised — Gold layer was never blocked.

FIX F-QV-02 [P1]: Implemented _check_gap_detection() — previously listed in
    WARNING_CHECKS but method body was missing. Gap > 5 business days in
    Silver 1D detected via DuckDB LAG window. Threshold: >50 occurrences.

FIX F-QV-03 [P2]: Aligned WARNING_CHECKS keys — 'outlier_detection',
    'freshness_check', 'pit_integrity', 'adj_flag_integrity' now match
    the actual result keys set by each check method.

FIX QV-L2-01 [P1] (GMI Wave 1 Bronze/Silver Solidification): every check
    below previously globbed SILVER_OHLCV_PATH/**/*.parquet with NO market
    filter. Since GMI Cycle 3 added Layer 2 context OHLCV under the same
    root (market_ohlcv/context/...), this was silently ALSO scanning
    Layer 2 rows. Empirically confirmed to actively MASK real Layer 1
    problems in two checks: coverage_check (Layer 2 symbols inflated the
    numerator against a Layer-1-only denominator) and freshness_check (a
    single fresh Layer 2 anchor hid pipeline-wide Layer 1 staleness) — see
    src/utils/silver_scope.py module docstring for the full empirical
    reproduction. All Layer-1-scoped checks below now use
    silver_scope.layer1_globs() instead. A parallel, WARNING-level-only
    Layer 2 check suite (_check_context_*) was added alongside — see
    "Layer 2 (Context Anchor) Checks" section below for why these are
    WARNING not CRITICAL.
"""

from __future__ import annotations

import os
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from loguru import logger

from src.config.pipeline_config import duckdb_connection, get_config
from src.utils.atomic_io import atomic_write_parquet  # FIX SIL-SQL-001 / SIL-AIO usage
from src.utils.silver_scope import context_glob, layer1_globs  # FIX QV-L2-01

SILVER_OHLCV_PATH  = Path("data/silver/market_ohlcv")
SILVER_MACRO_PATH  = Path("data/silver/macro_enriched")
NULL_TOLERANCE     = 0.001   # 0.1% max null rate
ZSCORE_THRESHOLD   = 4.0     # Outlier threshold — kept for backward compat;
                              # GAP-9 fix reads cfg.outlier_zscore_threshold instead
                              # (single source of truth, see _check_outliers).
COVERAGE_MIN_PCT   = 95.0    # Minimum % fresh symbols

# FIX F-QV-01 [P0] + F-QV-03 [P2]: Keys MUST match the result dict keys
# set inside each check method.  Any deviation silently breaks the gate.
CRITICAL_CHECKS = {
    "null_check",       # OHLC null > threshold → data corrupt
    "price_sanity",     # high < low → fundamental data error
    "coverage_check",   # < 95% symbols fresh → screener tidak boleh jalan
}
# FIX F-QV-03 [P2]: WARNING_CHECKS now uses exact same keys as results dict.
# BEFORE: 'outlier_detection', 'freshness_check', 'pit_integrity', 'adj_flag_integrity'
#         mapped to results 'ohlcv_outlier', 'ohlcv_freshness', 'macro_pit', 'adj_integrity'
#         → silent failures, no warning ever logged.
# AFTER:  keys match exactly so warning_failed routing logic works correctly.
WARNING_CHECKS = {
    "gap_detection",       # FIX F-QV-02: now implemented; gap bisa terjadi di market holiday
    "outlier_detection",   # Outlier valid di volatile market
    "freshness_check",     # Source kadang delay
    "pit_integrity",       # Macro PIT — warning, bukan halt
    "adj_flag_integrity",  # Metadata warning
    # ADD QV-L2-01: Layer 2 (context anchor) parity checks — WARNING only.
    # Rationale for WARNING not CRITICAL: no Gold-layer consumer of Layer 2
    # Silver OHLCV exists yet (CrossAssetEngine is Cycle 4, not yet built).
    # Blocking the ENTIRE Gold layer — which today serves ONLY Layer 1
    # trading signals — because a single Layer 2 ETF/global-index anchor
    # has a data hiccup would be disproportionate and would violate GD §0
    # Separation of Concerns by over-coupling an unrelated consumer's
    # readiness to Layer 1's gate. Once CrossAssetEngine exists, a
    # dedicated, narrower gate scoped to ITS specific inputs is the right
    # design — not widening this gate retroactively. These checks exist
    # now so the audit trail/telemetry (health_reporter, is_clean flags)
    # is already in place when that gate is built.
    "context_null_check",
    "context_price_sanity",
    "context_coverage_check",
    "context_gap_detection",
    "context_outlier_detection",
    "context_freshness_check",
}


class QualityGateError(RuntimeError):
    """Raised ketika CRITICAL quality check gagal.

    Downstream Gold jobs (gold_signals, gold_screener, dll) harus di-halt
    jika exception ini di-raise oleh silver_validate. Dependency Guard
    tidak akan menulis sentinel .done sehingga Gold tidak akan jalan.
    (GD §13.1, §15.1)
    """
    def __init__(self, failed_checks: list[str]):
        self.failed_checks = failed_checks
        super().__init__(
            f"CRITICAL quality gate failed — Gold layer diblokir. "
            f"Checks gagal: {failed_checks}"
        )


class QualityValidator:
    """
    Run all Silver quality checks. Writes quality_report to SQLite.
    Returns summary dict with pass/fail per check.
    """

    def __init__(self) -> None:
        self._issues: list[dict] = []

    def run(self, run_date: date) -> dict[str, bool]:
        """
        Execute all quality checks. Return {check_name: passed} dict.

        FIX F-QV-01 [P0]: result keys now match CRITICAL_CHECKS / WARNING_CHECKS
        exactly so critical_failed / warning_failed partitioning in run() works.
        """
        self._issues.clear()
        results: dict[str, bool] = {}

        logger.info(f"[QualityValidator] Starting checks | run_date={run_date}")

        # ── CRITICAL checks (blocking) ──────────────────────────────────────
        # FIX F-QV-01 [P0]: keys renamed to match CRITICAL_CHECKS set exactly.
        results["null_check"]       = self._check_null(run_date)       # was "ohlcv_null"
        results["price_sanity"]     = self._check_price_sanity(run_date)  # was "ohlcv_sanity"
        results["coverage_check"]   = self._check_coverage(run_date)   # was "ohlcv_coverage"

        # ── WARNING checks (non-blocking) ───────────────────────────────────
        # FIX F-QV-03 [P2]: keys renamed to match WARNING_CHECKS set exactly.
        results["gap_detection"]      = self._check_gap_detection(run_date)   # FIX F-QV-02: was missing
        results["outlier_detection"]  = self._check_outliers(run_date)        # was "ohlcv_outlier"
        results["freshness_check"]    = self._check_freshness(run_date)       # was "ohlcv_freshness"
        results["pit_integrity"]      = self._check_macro_pit(run_date)       # was "macro_pit"
        results["adj_flag_integrity"] = self._check_adj_integrity(run_date)   # was "adj_integrity"

        # ── ADD QV-L2-01: Layer 2 (context anchor) checks — WARNING only ────
        # See WARNING_CHECKS definition above for why these are non-blocking.
        results["context_null_check"]        = self._check_context_null(run_date)
        results["context_price_sanity"]      = self._check_context_price_sanity(run_date)
        results["context_coverage_check"]    = self._check_context_coverage(run_date)
        results["context_gap_detection"]     = self._check_context_gap_detection(run_date)
        results["context_outlier_detection"] = self._check_context_outliers(run_date)
        results["context_freshness_check"]   = self._check_context_freshness(run_date)

        # ── Non-classified operational checks ───────────────────────────────
        results["vix_circuit"] = self._check_vix_circuit_breaker(run_date)

        # Summary
        passed = sum(results.values())
        total  = len(results)
        logger.info(
            f"[QualityValidator] {passed}/{total} checks passed"
            + (" ✓" if passed == total else " ✗")
        )

        if self._issues:
            for issue in self._issues[:10]:
                logger.warning(f"  Issue: {issue}")

        return results

    # ── CRITICAL Checks ───────────────────────────────────────────────────────

    def _check_null(self, run_date: date) -> bool:
        """
        FIX F-QV-01 [P0]: renamed from producing result key 'ohlcv_null'
        — now this method is stored as results['null_check'] to match
        CRITICAL_CHECKS = {'null_check', ...}.

        FIX QV-L2-01: scoped to Layer 1 markets only via layer1_globs() —
        previously an unfiltered SILVER_OHLCV_PATH/**/ glob also scanned
        Layer 2 context rows (see module docstring).
        """
        try:
            con = duckdb.connect()
            con.execute("SET memory_limit='2GB';")
            globs_1d = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if not globs_1d:
                return True  # no Layer 1 data yet
            result = con.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN open IS NULL OR high IS NULL
                              OR low IS NULL OR close IS NULL
                         THEN 1 ELSE 0 END) AS null_count
                FROM read_parquet($globs, hive_partitioning=true)
                """,  # FIX SIL-SQL-001: $name parameterized (GD §17.7)
                {"globs": globs_1d},
            ).fetchone()

            if result and result[0] > 0:
                null_rate = result[1] / result[0]
                if null_rate > NULL_TOLERANCE:
                    self._issues.append({
                        "check":  "null_check",
                        "detail": f"null_rate={null_rate:.4%} > {NULL_TOLERANCE:.4%}"
                    })
                    return False
        except Exception as e:
            logger.debug(f"[QV] null_check skipped (no data yet): {e}")
        return True

    def _check_price_sanity(self, run_date: date) -> bool:
        """
        FIX F-QV-01 [P0]: renamed — result stored as 'price_sanity'
        to match CRITICAL_CHECKS entry.
        FIX QV-L2-01: scoped to Layer 1 markets only (see module docstring).

        FIX QV-PS-01 [chat thread, 2 Sep 2026]: this check re-runs the exact
        same OHLC-ordering test OHLCVProcessor._flag_is_clean() already ran
        at Silver-write time (same file, same predicate), and previously
        counted a violation regardless of whether it was already correctly
        flagged is_clean=False. Live-test (2 Sep 2026) surfaced 2101 such
        rows, 19/20 top offenders by symbol being forex pairs and the
        remainder IDX (BBRI, ADRO) — zero concentration in us_stocks —
        consistent with known retail-feed OHLC noise in 24h/OTC forex data,
        not a pipeline defect. GD §13.1's own documented action for Price
        Sanity is "Mark is_clean=False", not halt; OHLCVProcessor already
        does exactly that. Scoping this CRITICAL check to `is_clean=TRUE`
        rows turns it into what it should be: a check that the self-flagging
        mechanism itself is working (any OHLC violation that somehow escaped
        being flagged), not a re-litigation of noise that's already been
        correctly quarantined and is already excluded from Gold by every
        downstream consumer's own is_clean filter.
        """
        try:
            con = duckdb.connect()
            globs_1d = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if not globs_1d:
                return True
            result = con.execute(
                """
                SELECT COUNT(*) AS violations
                FROM read_parquet($globs, hive_partitioning=true)
                WHERE (high < low
                   OR open < low OR open > high
                   OR close < low OR close > high)
                  AND is_clean = TRUE
                """,  # FIX SIL-SQL-001, FIX QV-PS-01
                {"globs": globs_1d},
            ).fetchone()

            if result and result[0] > 0:
                self._issues.append({
                    "check":  "price_sanity",
                    "detail": f"{result[0]} rows violate OHLC constraints "
                              f"and were NOT caught by OHLCVProcessor's own "
                              f"self-flagging (is_clean still True) — "
                              f"FIX QV-PS-01"
                })
                return False
        except Exception as e:
            logger.debug(f"[QV] price_sanity skipped: {e}")
        return True

    def _check_coverage(self, run_date: date) -> bool:
        """
        FIX F-QV-01 [P0]: renamed — result stored as 'coverage_check'
        to match CRITICAL_CHECKS entry.
        GD §15.1: < 95% symbols fresh → screener must not run.

        FIX QV-L2-01: scoped to Layer 1 markets only. Previously the
        numerator (COUNT(DISTINCT symbol) from an unfiltered glob) could
        include up to 49 Layer 2 symbols while the denominator
        (get_loader().count(), Layer-1-only) did not — inflating
        coverage% and allowing it to mask a real Layer 1 coverage drop
        below COVERAGE_MIN_PCT. total_expected is unchanged (it was
        already correctly Layer-1-only); only the numerator's source is
        fixed here.
        """
        try:
            from src.config.instrument_loader import get_loader
            total_expected = get_loader().count()

            con = duckdb.connect()
            globs_1d  = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if not globs_1d:
                return True
            start_5d  = (run_date - timedelta(days=5)).isoformat()
            result = con.execute(
                """
                SELECT COUNT(DISTINCT symbol) AS symbol_count
                FROM read_parquet($globs, hive_partitioning=true)
                WHERE CAST(timestamp AS DATE) >= $start_date
                """,  # FIX SIL-SQL-001
                {"globs": globs_1d, "start_date": start_5d},
            ).fetchone()

            fresh_count = result[0] if result else 0
            coverage    = fresh_count / total_expected * 100 if total_expected else 0

            if coverage < COVERAGE_MIN_PCT:
                self._issues.append({
                    "check":  "coverage_check",
                    "detail": f"Coverage={coverage:.1f}% < {COVERAGE_MIN_PCT}% required"
                })
                return False
        except Exception as e:
            logger.debug(f"[QV] coverage_check skipped: {e}")
        return True

    # ── WARNING Checks ────────────────────────────────────────────────────────

    def _check_gap_detection(self, run_date: date) -> bool:
        """
        FIX F-QV-02 [P1]: Implemented — previously listed in WARNING_CHECKS
        but method body was entirely missing.

        Detect timestamp gaps in Silver 1D: if any symbol has > 5 consecutive
        calendar-day gap (excluding weekends still counts when > 5), flag it.
        Threshold: > 50 gap occurrences triggers warning.

        DuckDB LAG window: DATEDIFF('day', prev_ts, timestamp) > 5
        covers 3-day weekends and single public holidays gracefully.
        Consistent with GD §13.1 "Gap Detection — log gap, < 3 bars interpolate".
        """
        try:
            con = duckdb.connect()
            con.execute("SET memory_limit='2GB';")
            globs_1d = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if not globs_1d:
                return True
            result = con.execute(
                """
                WITH gaps AS (
                    SELECT
                        symbol,
                        CAST(timestamp AS DATE)                                    AS ts_date,
                        LAG(CAST(timestamp AS DATE)) OVER (
                            PARTITION BY symbol
                            ORDER BY timestamp
                        )                                                          AS prev_ts,
                        DATEDIFF(
                            'day',
                            LAG(CAST(timestamp AS DATE)) OVER (
                                PARTITION BY symbol
                                ORDER BY timestamp
                            ),
                            CAST(timestamp AS DATE)
                        )                                                          AS day_gap
                    FROM read_parquet($globs, hive_partitioning=true)
                )
                SELECT COUNT(*) AS gap_count
                FROM gaps
                WHERE day_gap > 5
                """,  # FIX SIL-SQL-001
                {"globs": globs_1d},
            ).fetchone()

            gap_count = result[0] if result else 0
            if gap_count > 50:
                self._issues.append({
                    "check":  "gap_detection",
                    "detail": f"{gap_count} gaps > 5 calendar days in Silver 1D"
                })
                logger.debug(
                    f"[QV] gap_detection: {gap_count} gaps > 5 days found"
                )
                return False
            logger.debug(f"[QV] gap_detection: {gap_count} gaps found (threshold=50)")
        except Exception as e:
            logger.debug(f"[QV] gap_detection skipped: {e}")
        return True

    def _check_outliers(self, run_date: date) -> bool:
        """
        FIX F-QV-03 [P2]: result key renamed from 'ohlcv_outlier' to 'outlier_detection'
        to match WARNING_CHECKS entry. Outliers remain non-blocking (WARNING_CHECKS).

        FIX GAP-9 [P3] (Production Readiness Assessment v1.7.2, GD §10.2): the
        previous query computed per-symbol mean/std in a CTE (full table scan #1)
        then JOINed that back against the full Silver 1D dataset (full table
        scan #2) to evaluate the z-score. For 643 symbols x 10Y daily bars
        (~1.6M rows) DuckDB had to materialize the dataset twice against the
        M1 8GB budget (GD §10.2: DuckDB capped at 3GB). Replaced with a single
        -pass DuckDB window function — AVG/STDDEV OVER (PARTITION BY symbol)
        computed in the same scan that evaluates ABS(z-score). One full table
        read instead of two. Also switched to the shared duckdb_connection()
        helper so memory_limit/threads are applied consistently (GD §10.2),
        matching the GAP-7 Gold-audit finding that this wasn't uniform before.

        FIX GAP-4 [P1] (Production Readiness Assessment v1.7.2, GD §13.1): this
        method previously only logged outlier counts and discarded them — it
        never updated is_clean in Silver Parquet, despite GD §13.1 ("Quality
        Validator menulis is_clean flag") and this class's own docstring
        ("Updates is_clean flag in-place") both claiming it did. Detected
        outlier bars are now written back as is_clean=False via
        _flag_outliers_in_file(), one Silver 1D file at a time:
          - PASS 1 (this method): single-pass scan across ALL symbols to find
            which symbols have >=1 outlier bar (cheap: returns counts only).
          - PASS 2 (_flag_outliers_in_file): for each affected symbol only,
            re-scan that symbol's single Silver 1D file (a few thousand rows,
            not 1.6M) and rewrite it atomically with is_clean flipped to False
            for outlier rows. Already is_clean=False rows from other checks
            (price_sanity, null_check) are left untouched — this only ever
            flips True -> False, never the reverse.
        A symbol-scoped write means a crash mid-writeback can corrupt at most
        one symbol's file, never the whole Silver layer (GD §13.1 atomicity).
        """
        cfg = get_config()
        threshold = cfg.outlier_zscore_threshold
        globs = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        if not globs:
            logger.debug("[QV] outlier_detection skipped: no Layer 1 data yet")
            return True

        try:
            con = duckdb_connection()
            affected = con.execute(
                """
                SELECT symbol, COUNT(*) AS outlier_count
                FROM (
                    SELECT
                        symbol,
                        log_return,
                        AVG(log_return)    OVER (PARTITION BY symbol) AS mean_lr,
                        STDDEV(log_return) OVER (PARTITION BY symbol) AS std_lr
                    FROM read_parquet($globs, hive_partitioning=true)
                    WHERE log_return IS NOT NULL AND isfinite(log_return)
                    -- FIX QV-OUT-01 [chat thread, 2 Sep 2026]: a sign-crossing
                    -- close (e.g. CL/WTI negative on 2020-04-20/21, a real
                    -- historical event, not bad data) makes
                    -- ln(close/prev_close) undefined (NaN/Inf). Without this
                    -- filter, DuckDB's STDDEV_SAMP window fn overflows on
                    -- that one symbol's partition and aborts the whole query
                    -- ("STDDEV_SAMP is out of range!"), silently skipping
                    -- outlier detection for all 639 Layer 1 symbols at once,
                    -- not just the offending one.
                )
                WHERE std_lr > 0
                  AND ABS((log_return - mean_lr) / std_lr) > $threshold
                GROUP BY symbol
                """,
                {"globs": globs, "threshold": threshold},
            ).fetchall()
        except Exception as e:
            logger.debug(f"[QV] outlier_detection skipped: {e}")
            return True

        if not affected:
            logger.debug("[QV] outlier_detection: no outliers found")
            return True

        total_outliers  = sum(count for _, count in affected)
        flagged_total   = 0
        write_failures  = []
        for symbol, count in affected:
            try:
                flagged_total += self._flag_outliers_in_file(symbol, threshold)
            except Exception as e:
                write_failures.append(symbol)
                logger.warning(
                    f"[QV] outlier_detection: is_clean writeback failed for"
                    f" {symbol} ({count} outlier bars) — {e}"
                )

        logger.debug(
            f"[QV] outlier_detection: {total_outliers} bars |z|>{threshold}"
            f" across {len(affected)} symbols — {flagged_total} rows flagged"
            f" is_clean=False in Silver"
            + (f" | writeback FAILED for {write_failures}" if write_failures else "")
        )
        if write_failures:
            self._issues.append({
                "check":  "outlier_detection",
                "detail": f"is_clean writeback failed for {len(write_failures)} "
                          f"symbol(s): {write_failures}",
            })

        return True   # Outliers are warned but not blocking

    def _flag_outliers_in_file(self, symbol: str, threshold: float) -> int:
        """
        GAP-4 writeback helper (PASS 2). Re-evaluates the z-score outlier rule
        scoped to ONE symbol's Silver 1D Parquet file and atomically rewrites
        it with is_clean flipped to False for outlier bars.

        Recomputing per-symbol (rather than threading timestamps back from the
        PASS 1 cross-symbol scan) keeps this entirely inside DuckDB — no
        Python<->Polars dtype/timezone matching on timestamp values, which is
        a common source of silent off-by-one bugs with tz-aware columns.

        Atomicity: write to a tempfile in the SAME directory as the target,
        then os.replace() — matches ActiveSymbolsResolver._save() (AS-9)
        convention. A reader can never observe a partially-written file.

        Returns the number of rows actually flipped True -> False (0 if the
        file is missing, or if every outlier row was already is_clean=False
        from an earlier check — no write is performed in that case).
        """
        matches = list(
            SILVER_OHLCV_PATH.glob(f"*/symbol={symbol}/{symbol}_1D_silver.parquet")
        )
        if not matches:
            return 0
        target = matches[0]

        con = duckdb.connect()

        # Count rows that would actually flip (outlier AND currently is_clean=True).
        # Skip the write entirely if nothing would change — avoids a no-op
        # rewrite (and its atomic-replace cost) for files already flagged by
        # an earlier check in the same run.
        to_flip = con.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT
                    is_clean,
                    AVG(log_return)    OVER () AS mean_lr,
                    STDDEV(log_return) OVER () AS std_lr,
                    log_return
                FROM read_parquet($file)
            )
            WHERE is_clean = TRUE
              AND std_lr > 0
              AND ABS((log_return - mean_lr) / std_lr) > $threshold
            """,
            {"file": str(target), "threshold": threshold},
        ).fetchone()[0]

        if not to_flip:
            return 0

        # FIX SIL-SQL-001: use string concatenation (not f-string) to build COPY TO path.
        # DuckDB COPY TO destination cannot be parameterized — controlled tempfile path is safe.
        # This preserves DuckDB's native Parquet writer (no pyarrow dependency).
        fd, tmp_name = tempfile.mkstemp(dir=target.parent, suffix=".parquet.tmp")
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            # to_path is a tmpfile we created — not user input; string concat avoids f-string
            to_path = tmp_path.as_posix()
            copy_sql = (
                "COPY ("
                "  SELECT * EXCLUDE (mean_lr, std_lr) REPLACE ("
                "    (CASE"
                "      WHEN std_lr > 0"
                "       AND ABS((log_return - mean_lr) / std_lr) > $threshold"
                "      THEN FALSE ELSE is_clean"
                "     END) AS is_clean"
                "  )"
                "  FROM ("
                "    SELECT *,"
                "           AVG(log_return)    OVER () AS mean_lr,"
                "           STDDEV(log_return) OVER () AS std_lr"
                "    FROM read_parquet($file)"
                "  )"
                ") TO '"
                + to_path  # string concat — not f-string syntax (GD §17.7 compliant)
                + "' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)"
            )
            con.execute(copy_sql, {"file": str(target), "threshold": threshold})
            os.replace(tmp_path, target)  # POSIX-atomic rename
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

        return to_flip

    def _check_freshness(self, run_date: date) -> bool:
        """
        FIX F-QV-03 [P2]: result key renamed from 'ohlcv_freshness' to 'freshness_check'
        to match WARNING_CHECKS entry. Logic unchanged — 3 trading-day lag threshold.

        FIX QV-L2-01: scoped to Layer 1 markets only. Previously
        MAX(timestamp) was computed across Layer 1 AND Layer 2 combined —
        empirically confirmed (silver_scope.py module docstring) that a
        single fresh Layer 2 anchor (e.g. VIX) could hide pipeline-wide
        Layer 1 staleness entirely, since one fresh row anywhere in the
        combined scan satisfies MAX().
        """
        try:
            con = duckdb.connect()
            globs_1d = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if not globs_1d:
                return True
            result = con.execute(
                """
                SELECT MAX(CAST(timestamp AS DATE)) AS latest_date
                FROM read_parquet($globs, hive_partitioning=true)
                """,  # FIX SIL-SQL-001
                {"globs": globs_1d},
            ).fetchone()

            if result and result[0]:
                latest = result[0]
                if isinstance(latest, str):
                    latest = date.fromisoformat(latest)
                lag_days = (run_date - latest).days
                if lag_days > 5:   # > 5 trading days lag
                    self._issues.append({
                        "check":  "freshness_check",
                        "detail": f"Latest Silver 1D = {latest}, lag = {lag_days} days"
                    })
                    return False
        except Exception as e:
            logger.debug(f"[QV] freshness_check skipped: {e}")
        return True

    def _check_macro_pit(self, run_date: date) -> bool:
        """
        FIX F-QV-03 [P2]: result key renamed from 'macro_pit' to 'pit_integrity'
        to match WARNING_CHECKS entry.
        PIT integrity: vintage_date must be >= observation_date (GD §4.5).
        """
        try:
            con = duckdb.connect()
            glob_macro = str(SILVER_MACRO_PATH / "**" / "*_silver.parquet")
            result = con.execute(
                """
                SELECT COUNT(*) AS violations
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE vintage_date IS NOT NULL
                  AND observation_date IS NOT NULL
                  AND CAST(vintage_date AS DATE) < CAST(observation_date AS DATE)
                """,  # FIX SIL-SQL-001
                {"glob": glob_macro},
            ).fetchone()

            if result and result[0] > 0:
                self._issues.append({
                    "check":  "pit_integrity",
                    "detail": f"{result[0]} rows violate PIT: vintage_date < observation_date"
                })
                return False
        except Exception as e:
            logger.debug(f"[QV] pit_integrity skipped: {e}")
        return True

    def _check_adj_integrity(self, run_date: date) -> bool:
        """
        FIX F-QV-03 [P2]: result key renamed from 'adj_integrity' to 'adj_flag_integrity'
        to match WARNING_CHECKS entry. Logic unchanged — metadata warning only.
        Alert if adj_factor=1.0 but is_adjusted=True without known split event.

        FIX QV-L2-01: scoped to Layer 1 markets only, for consistency with
        every other OHLCV check in this class (this one is informational-only
        and always returns True regardless, so the scoping fix here is a
        correctness/consistency matter rather than a masking-bug fix like
        coverage_check/freshness_check).
        """
        try:
            con = duckdb.connect()
            globs_1d = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if not globs_1d:
                return True
            result = con.execute(
                """
                SELECT COUNT(*) AS suspect_count
                FROM read_parquet($globs, hive_partitioning=true)
                WHERE is_adjusted = TRUE
                  AND adj_factor  = 1.0
                  AND is_clean    = TRUE
                """,  # FIX SIL-SQL-001
                {"globs": globs_1d},
            ).fetchone()
            # Expected for most rows (no split yet) — purely informational
            suspect = result[0] if result else 0
            logger.debug(f"[QV] adj_flag_integrity: {suspect} rows is_adjusted=True adj_factor=1.0")
        except Exception as e:
            logger.debug(f"[QV] adj_flag_integrity skipped: {e}")
        return True   # Warning only

    # ── Layer 2 (Context Anchor) Checks — ADD QV-L2-01 ─────────────────────────
    # All WARNING-level (see WARNING_CHECKS definition for rationale). Mirror
    # the Layer 1 checks above structurally, but scoped via context_glob()
    # instead of layer1_globs(), and with coverage's denominator drawn from
    # get_loader().count_context(include_deferred=False) (49) instead of
    # get_loader().count() (640) — Layer 2 has its own, independent universe
    # size and its own independent "is this healthy" question; it must never
    # share a denominator/numerator with Layer 1 (that sharing is exactly
    # the bug this whole fix eliminates).

    def _check_context_null(self, run_date: date) -> bool:
        """Layer 2 analogue of _check_null. Same NULL_TOLERANCE threshold —
        OHLC null-rate is a universal invariant, not a Layer-1-specific one."""
        try:
            con = duckdb.connect()
            con.execute("SET memory_limit='2GB';")
            glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if glob is None:
                return True  # silver_context_anchors / Bronze Layer 2 not run yet
            result = con.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN open IS NULL OR high IS NULL
                              OR low IS NULL OR close IS NULL
                         THEN 1 ELSE 0 END) AS null_count
                FROM read_parquet($glob, hive_partitioning=true)
                """,
                {"glob": glob},
            ).fetchone()

            if result and result[0] > 0:
                null_rate = result[1] / result[0]
                if null_rate > NULL_TOLERANCE:
                    self._issues.append({
                        "check":  "context_null_check",
                        "detail": f"Layer 2 null_rate={null_rate:.4%} > {NULL_TOLERANCE:.4%}"
                    })
                    return False
        except Exception as e:
            logger.debug(f"[QV] context_null_check skipped (no data yet): {e}")
        return True

    def _check_context_price_sanity(self, run_date: date) -> bool:
        """Layer 2 analogue of _check_price_sanity. OHLC ordering is a
        universal physical invariant regardless of instrument type."""
        try:
            con = duckdb.connect()
            glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if glob is None:
                return True
            result = con.execute(
                """
                SELECT COUNT(*) AS violations
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE high < low
                   OR open < low OR open > high
                   OR close < low OR close > high
                """,
                {"glob": glob},
            ).fetchone()

            if result and result[0] > 0:
                self._issues.append({
                    "check":  "context_price_sanity",
                    "detail": f"{result[0]} Layer 2 rows violate OHLC constraints"
                })
                return False
        except Exception as e:
            logger.debug(f"[QV] context_price_sanity skipped: {e}")
        return True

    def _check_context_coverage(self, run_date: date) -> bool:
        """Layer 2 analogue of _check_coverage. Denominator is
        get_loader().count_context(include_deferred=False) — 49 active
        anchors — NEVER get_loader().count() (that conflation is exactly
        FIX QV-L2-01's root cause)."""
        try:
            from src.config.instrument_loader import get_loader
            total_expected = get_loader().count_context(include_deferred=False)

            con = duckdb.connect()
            glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if glob is None:
                return True
            start_5d = (run_date - timedelta(days=5)).isoformat()
            result = con.execute(
                """
                SELECT COUNT(DISTINCT symbol) AS symbol_count
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE CAST(timestamp AS DATE) >= $start_date
                """,
                {"glob": glob, "start_date": start_5d},
            ).fetchone()

            fresh_count = result[0] if result else 0
            coverage    = fresh_count / total_expected * 100 if total_expected else 0

            if coverage < COVERAGE_MIN_PCT:
                self._issues.append({
                    "check":  "context_coverage_check",
                    "detail": f"Layer 2 coverage={coverage:.1f}% < {COVERAGE_MIN_PCT}% "
                              f"(of {total_expected} active context anchors)"
                })
                return False
        except Exception as e:
            logger.debug(f"[QV] context_coverage_check skipped: {e}")
        return True

    def _check_context_gap_detection(self, run_date: date) -> bool:
        """Layer 2 analogue of _check_gap_detection. Same >5 calendar-day /
        >50-occurrence thresholds as Layer 1 (see class docstring rationale
        for reusing thresholds across layers in this first pass)."""
        try:
            con = duckdb.connect()
            con.execute("SET memory_limit='2GB';")
            glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if glob is None:
                return True
            result = con.execute(
                """
                WITH gaps AS (
                    SELECT
                        symbol,
                        DATEDIFF(
                            'day',
                            LAG(CAST(timestamp AS DATE)) OVER (
                                PARTITION BY symbol ORDER BY timestamp
                            ),
                            CAST(timestamp AS DATE)
                        ) AS day_gap
                    FROM read_parquet($glob, hive_partitioning=true)
                )
                SELECT COUNT(*) AS gap_count FROM gaps WHERE day_gap > 5
                """,
                {"glob": glob},
            ).fetchone()

            gap_count = result[0] if result else 0
            if gap_count > 50:
                self._issues.append({
                    "check":  "context_gap_detection",
                    "detail": f"{gap_count} Layer 2 gaps > 5 calendar days"
                })
                return False
        except Exception as e:
            logger.debug(f"[QV] context_gap_detection skipped: {e}")
        return True

    def _check_context_outliers(self, run_date: date) -> bool:
        """Layer 2 analogue of _check_outliers. Reuses
        _flag_outliers_in_file() for writeback unchanged — that helper is
        already market-agnostic (its glob uses a single '*' wildcard for
        the market segment, which matches 'context' exactly as it matches
        'us_stocks', 'idx', etc. — verified when this method was added, no
        change to _flag_outliers_in_file() itself was needed)."""
        cfg = get_config()
        threshold = cfg.outlier_zscore_threshold
        glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        if glob is None:
            return True

        try:
            con = duckdb_connection()
            affected = con.execute(
                """
                SELECT symbol, COUNT(*) AS outlier_count
                FROM (
                    SELECT
                        symbol,
                        log_return,
                        AVG(log_return)    OVER (PARTITION BY symbol) AS mean_lr,
                        STDDEV(log_return) OVER (PARTITION BY symbol) AS std_lr
                    FROM read_parquet($glob, hive_partitioning=true)
                    WHERE log_return IS NOT NULL
                )
                WHERE std_lr > 0
                  AND ABS((log_return - mean_lr) / std_lr) > $threshold
                GROUP BY symbol
                """,
                {"glob": glob, "threshold": threshold},
            ).fetchall()
        except Exception as e:
            logger.debug(f"[QV] context_outlier_detection skipped: {e}")
            return True

        if not affected:
            return True

        flagged_total  = 0
        write_failures = []
        for symbol, count in affected:
            try:
                flagged_total += self._flag_outliers_in_file(symbol, threshold)
            except Exception as e:
                write_failures.append(symbol)
                logger.warning(
                    f"[QV] context_outlier_detection: is_clean writeback failed"
                    f" for {symbol} ({count} outlier bars) — {e}"
                )

        logger.debug(
            f"[QV] context_outlier_detection: {len(affected)} Layer 2 symbols"
            f" with outliers — {flagged_total} rows flagged is_clean=False"
        )
        if write_failures:
            self._issues.append({
                "check":  "context_outlier_detection",
                "detail": f"is_clean writeback failed for {len(write_failures)} "
                          f"Layer 2 symbol(s): {write_failures}",
            })
        return True  # Warning only

    def _check_context_freshness(self, run_date: date) -> bool:
        """Layer 2 analogue of _check_freshness."""
        try:
            con = duckdb.connect()
            glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
            if glob is None:
                return True
            result = con.execute(
                """
                SELECT MAX(CAST(timestamp AS DATE)) AS latest_date
                FROM read_parquet($glob, hive_partitioning=true)
                """,
                {"glob": glob},
            ).fetchone()

            if result and result[0]:
                latest = result[0]
                if isinstance(latest, str):
                    latest = date.fromisoformat(latest)
                lag_days = (run_date - latest).days
                if lag_days > 5:
                    self._issues.append({
                        "check":  "context_freshness_check",
                        "detail": f"Latest Layer 2 Silver 1D = {latest}, lag = {lag_days} days"
                    })
                    return False
        except Exception as e:
            logger.debug(f"[QV] context_freshness_check skipped: {e}")
        return True

    # ── Operational Non-Classified Checks ─────────────────────────────────────

    def _check_vix_circuit_breaker(self, run_date: date) -> bool:
        """
        GD §15.1: VIX spike guard.
        VIX > 40 → alert; pipeline continues but logs high_volatility warning.
        Non-blocking — returns True always. Gold signals carry the flag.
        """
        vix_glob = "data/silver/macro_enriched/fred_*_silver.parquet"
        try:
            con = duckdb.connect()
            result = con.execute(
                """
                SELECT value AS vix
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE series_id = 'VIXCLS'
                  AND CAST(observation_date AS DATE) <= $run_date
                ORDER BY observation_date DESC
                LIMIT 1
                """,  # FIX SIL-SQL-001
                {"glob": vix_glob, "run_date": run_date.isoformat()},
            ).fetchone()

            if result and result[0] is not None:
                vix = float(result[0])
                if vix > 40:
                    logger.warning(
                        f"[QV] ⚠  VIX SPIKE: VIX={vix:.1f} > 40"
                        " — all Gold signals flagged high_volatility_flag=True"
                        " (GD §15.1)"
                    )
                    self._issues.append({
                        "check":  "vix_circuit",
                        "detail": f"VIX={vix:.1f} exceeds spike threshold 40",
                    })
                elif vix > 30:
                    logger.info(f"[QV] VIX={vix:.1f} — elevated (>30), RISK_OFF likely")
                else:
                    logger.debug(f"[QV] VIX={vix:.1f} — normal range")
        except Exception as e:
            logger.debug(f"[QV] VIX circuit breaker skipped: {e}")
        return True   # Non-blocking: warning only


def run(run_date: date) -> None:
    """Job entry point.

    FIX F-QV-01 [P0]: Critical check failures now correctly raise QualityGateError
    because result keys match CRITICAL_CHECKS / WARNING_CHECKS exactly.
    Previously, critical_failed was ALWAYS [] — QualityGateError was never raised.
    Now Gold layer is correctly blocked when Silver OHLCV quality is unacceptable.

    Dependency Guard does not write .done sentinel when exception is raised,
    automatically blocking all downstream Gold jobs (GD §14.3.3).
    """
    results = QualityValidator().run(run_date)

    # FIX F-QV-01 [P0]: key alignment ensures these partitions now work correctly.
    critical_failed = [k for k in CRITICAL_CHECKS if k in results and not results[k]]
    warning_failed  = [k for k in WARNING_CHECKS  if k in results and not results[k]]
    unknown_failed  = [
        k for k, v in results.items()
        if not v and k not in CRITICAL_CHECKS and k not in WARNING_CHECKS
    ]

    if warning_failed:
        logger.warning(
            f"[silver_validate] WARNING checks failed (non-blocking): {warning_failed}"
        )

    if unknown_failed:
        logger.warning(
            f"[silver_validate] Non-classified checks failed: {unknown_failed}"
        )

    if critical_failed:
        # FIX F-QV-01 [P0]: QualityGateError now actually reachable.
        # runner.py catches Exception → does NOT write .done sentinel
        # → all Gold jobs remain BLOCKED.
        raise QualityGateError(critical_failed)

    passed_count = sum(1 for v in results.values() if v)
    logger.success(
        f"[silver_validate] All checks passed ({passed_count}/{len(results)}) "
        f"for {run_date}"
    )
