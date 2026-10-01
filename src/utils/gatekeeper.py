"""
src/utils/gatekeeper.py — GMI-GATE-001 (ADR-050)

Thin cascading Bronze -> Silver -> Gold health gate. Deliberately built as
a layer OVER three things this codebase already has and already trusts —
DependencyGuard (job-level sentinel completion), QualityValidator's
persisted per-check log (data/health/quality_gate_log.parquet, FIX
GMI-GATE-001 in quality_validator.py), and PipelineLogger's job_registry
metadata (JOB_REGISTRY / LAYER_JOB_NAMES / _passes_schedule) — not a
parallel monitoring system that re-derives its own notion of "did the
pipeline run today" (ADR-050 Rejected Alternatives explicitly names this
as the duplicated-logic drift risk to avoid).

Output: data/health/gate_status.parquet
    run_date, scope, layer_health, gate_status, streak_days

    scope        — 'bronze' | 'silver' | 'gold'
    layer_health — TODAY's raw, unsmoothed signal for that scope: 'pass' |
                   'warn' | 'block'. See _bronze_raw_status() /
                   _silver_raw_status() / _gold_raw_status().
    gate_status  — layer_health after time-windowed hysteresis (see
                   _emit_status()). This is the column a downstream
                   consumer (health_report, PITDataLoader.get_gate_status(),
                   ADR-053's per-model Gatekeeper scope tag) should read —
                   layer_health is the diagnostic detail underneath it.
    streak_days  — how many consecutive days (including run_date, counting
                   backward) layer_health has held its CURRENT value.
                   Capped at LOOKBACK_DAYS as a lower bound (a streak that
                   hits the cap means "at least LOOKBACK_DAYS", not
                   exactly that many) — recomputed from the underlying
                   signals each run rather than trusted from a prior
                   gate_status.parquet write, so one bad historical write
                   can never propagate forward.

Cascading (Bronze -> Silver -> Gold): each layer's raw status is at least
as bad as the layer beneath it — a Bronze problem shows up as at least a
Silver warning even if every Silver-specific check passed, and propagates
the same way into Gold. This mirrors the layered i/j/k relationship in
this project's own gatekeeper design notes (hmm-gatekeeper-xgboost.md),
simplified here into one combined per-layer raw status rather than that
note's separate structure/data/analytic axes — see "Design simplifications"
below for why, and revisit if a real case emerges where the two would
genuinely have diverged.

Hysteresis (first-pass calibration — ADR-050 Consequences explicitly flags
this as unverified against live history, same caveat already carried by
global_index_regime.py's RISK_ON_THRESHOLD/RISK_OFF_THRESHOLD):
    - A 'pass' raw status must hold for RECOVERY_CONFIRM_DAYS consecutive
      days before gate_status says 'pass' — a single good day right after
      trouble does not immediately clear the gate.
    - A 'block' raw status must hold for BLOCK_CONFIRM_DAYS consecutive
      days before gate_status escalates all the way to 'block' — a single
      bad day surfaces as 'warn', not an immediate hard stop.
    - A 'warn' raw status (or a 'block' raw status that hasn't yet been
      sustained long enough to escalate) always surfaces as 'warn'
      immediately — problems are never silently held back, only the
      all-clear and the full-stop are damped.
These two window lengths (2 and 4 days) are read directly off
hmm-gatekeeper-xgboost.md's own gatekeeper notation (`t(0,1)` for the
pass condition, `t(0,1,2,3)` for the block condition) rather than
invented independently.

Design simplifications vs. hmm-gatekeeper-xgboost.md's fuller notation:
    - That note models each layer's health via a separate "health
      function" axis (structure/data/analytic) and a separate "layer
      state" axis (schema/raw-data integrity for Bronze; cleanser/
      semantic/lineage integrity for Silver; intelligence/analytic
      quality/lineage for Gold), combined only at the final gate. This
      module collapses both axes into ONE combined per-layer raw status,
      since in this codebase job-completion and check-outcome signals for
      a given layer are already tightly coupled by construction (a failed
      CRITICAL check already prevents QualityValidator's own caller from
      writing a sentinel) — keeping Gatekeeper itself "thin" per ADR-050's
      own rationale. Revisit this collapse, not the whole design, if a
      genuine case surfaces where the two axes would have disagreed.
    - Gold currently has no per-check quality validator of its own (no
      Gold-layer equivalent of quality_gate_log.parquet exists yet) — Gold's
      local signal is job-completion only. This is a known, named gap
      (matches the CI/CD guide's own open item: Gold layer has never had a
      formal audit), not something this ADR closes.
"""

from __future__ import annotations

import glob as glob_module
from datetime import date, timedelta
from pathlib import Path

import polars as pl
from loguru import logger

from src.scheduler.dependency_guard import DependencyGuard
from src.utils.atomic_io import atomic_write_parquet

GATE_STATUS_PATH      = Path("data/health/gate_status.parquet")
QUALITY_GATE_LOG_PATH = Path("data/health/quality_gate_log.parquet")
QUARANTINE_GLOB       = "data/quarantine/*.parquet"

SCOPES = ("bronze", "silver", "gold")

# Cascading/hysteresis ordering — index = severity rank.
_SEVERITY = {"pass": 0, "warn": 1, "block": 2}

# First-pass calibration (see module docstring) — read off
# hmm-gatekeeper-xgboost.md's t(0,1) / t(0,1,2,3) windows.
RECOVERY_CONFIRM_DAYS = 2
BLOCK_CONFIRM_DAYS    = 4

# A job outside DAILY_SEQUENCE (weekly/monthly cadence) is checked with
# this staleness tolerance rather than exact-date match — same convention
# and same 7-day figure already established for cross-cadence
# dependencies (FIX NEW-1, job_registry.py's own stale_tolerance entries:
# "mingguan = 7 hari").
WEEKLY_STALE_DAYS = 7

# How many days of raw-status history to (re)compute per run. Must be >=
# BLOCK_CONFIRM_DAYS; the extra headroom gives streak_days a little more
# resolution before it saturates at the cap.
LOOKBACK_DAYS = 14


def _worse(a: str, b: str) -> str:
    """Return whichever of two statuses is more severe (block > warn > pass)."""
    return a if _SEVERITY[a] >= _SEVERITY[b] else b


def _expected_jobs_for_layer(layer: str, run_date: date) -> list[str]:
    """
    Job names tagged `layer` that were actually scheduled to run on
    run_date, per each job's own _passes_schedule() guard.

    Lazy import: JOB_REGISTRY/LAYER_JOB_NAMES/_passes_schedule live in
    src.scheduler.job_registry, which registers this module's own run()
    as the 'gatekeeper' job — importing job_registry at module scope here
    would be circular. Matches this codebase's established pattern for
    the same class of circularity (e.g. health_reporter.py's lazy
    instrument_loader import inside _check_idx_coverage()).
    """
    from src.scheduler.job_registry import JOB_REGISTRY, LAYER_JOB_NAMES, _passes_schedule

    return [
        job_name
        for job_name in LAYER_JOB_NAMES.get(layer, [])
        if _passes_schedule(JOB_REGISTRY[job_name], run_date)
    ]


def _due_exactly_on_guard_days(job: dict) -> bool:
    """
    True if this job's ONLY schedule constraint is run_on_weekdays — i.e.
    the days _passes_schedule() lets it through are exactly the days it is
    due, and it should complete on each of them (bronze_macro_weekly /
    bronze_bis_rates / the three weekly CrossAssetEngine jobs: Sunday;
    bronze_eia: Wednesday). Multi-day-window guards (run_on_day_of_month,
    run_on_nth_weekday, run_on_months) admit several days per cycle for ONE
    run, so "completed today" is the wrong test for them — they fall back
    to the staleness window instead. (No such job is in any sequence today,
    so LAYER_JOB_NAMES never surfaces one; this keeps that case safe if one
    is ever added.)
    """
    guard_keys = {k for k in job if k.startswith("run_on_")}
    return guard_keys == {"run_on_weekdays"}


def _job_completed(
    guard: DependencyGuard,
    job_name: str,
    run_date: date,
    daily_sequence: set[str],
) -> bool:
    """
    Was `job_name` done for run_date? Three cadence classes, all derived
    from registry structure rather than a hardcoded job list:

      1. DAILY_SEQUENCE jobs — exact-date sentinel (Gatekeeper's own job
         runs at the very end of that day's sequence, so today's chance to
         write the sentinel has already passed).
      2. Non-daily jobs whose only guard is run_on_weekdays — ALSO exact-date,
         evaluated only on their due days (_expected_jobs_for_layer already
         restricts to those days). A window here would delay detection of a
         missed Sunday run until the NEXT Sunday: measured against the
         registry, a missed weekly run was first flagged 14 days after the
         last success (a full week late) under a 7-day window, versus on
         the due day itself with an exact match.
      3. Non-daily jobs with no run_on_weekdays-only guard (silver_macro,
         silver_global_rates carry none; multi-day-window guards) — the
         WEEKLY_STALE_DAYS staleness window, same convention DependencyGuard
         already applies to cross-cadence *dependencies* (FIX NEW-1). These
         have no single due day, and operators legitimately run them daily
         (silver_macro reads the daily FRED files bronze_fred_daily writes)
         or weekly — a window is correct for both. Measured: flagged 8 days
         after last success, i.e. the day after the window lapses.
    """
    from src.scheduler.job_registry import JOB_REGISTRY

    if job_name in daily_sequence or _due_exactly_on_guard_days(JOB_REGISTRY[job_name]):
        max_age = 0
    else:
        max_age = WEEKLY_STALE_DAYS
    return guard.is_done_within(job_name, run_date, max_age)


def _quarantine_today(run_date: date) -> bool:
    """Any Bronze schema-registry quarantine file (schema_validator.py)
    written on run_date? Matched by file mtime date, the same idiom
    health_reporter.py's IDX coverage check already uses for "did this
    happen today" against a timestamp that isn't itself a run_date column
    — quarantine filenames carry a wall-clock timestamp, not run_date."""
    try:
        for fname in glob_module.glob(QUARANTINE_GLOB):
            try:
                if date.fromtimestamp(Path(fname).stat().st_mtime) == run_date:
                    return True
            except (OSError, ValueError):
                continue
    except OSError:
        pass
    return False


def _bronze_raw_status(
    guard: DependencyGuard, run_date: date, daily_sequence: set[str]
) -> str:
    expected = _expected_jobs_for_layer("bronze", run_date)
    missing  = [j for j in expected if not _job_completed(guard, j, run_date, daily_sequence)]
    if missing:
        return "block"
    if _quarantine_today(run_date):
        return "warn"
    return "pass"


def _silver_raw_status(
    guard: DependencyGuard,
    run_date: date,
    daily_sequence: set[str],
    quality_log: pl.DataFrame,
    bronze_status: str,
) -> str:
    expected = _expected_jobs_for_layer("silver", run_date)
    missing  = [j for j in expected if not _job_completed(guard, j, run_date, daily_sequence)]

    day_str = run_date.isoformat()
    day_log = quality_log.filter(pl.col("run_date") == day_str) if quality_log.height else quality_log

    if missing:
        local = "block"
    elif day_log.height == 0:
        # silver_validate's own sentinel says done, but no persisted check
        # detail exists for this date (e.g. a prior run predates FIX
        # GMI-GATE-001, or the persistence write itself failed and was
        # swallowed) -- absence of evidence is not evidence of health.
        local = "warn"
    else:
        critical_fail = day_log.filter(
            (pl.col("tier") == "critical") & (~pl.col("passed"))
        ).height > 0
        warning_fail = day_log.filter(
            (pl.col("tier") == "warning") & (~pl.col("passed"))
        ).height > 0
        local = "block" if critical_fail else ("warn" if warning_fail else "pass")

    return _worse(bronze_status, local)


def _gold_raw_status(
    guard: DependencyGuard,
    run_date: date,
    daily_sequence: set[str],
    silver_status: str,
) -> str:
    expected = _expected_jobs_for_layer("gold", run_date)
    missing  = [j for j in expected if not _job_completed(guard, j, run_date, daily_sequence)]
    # No per-check Gold quality validator exists yet (see module docstring)
    # -- job completion is the only local Gold signal available today.
    local = "block" if missing else "pass"
    return _worse(silver_status, local)


def _raw_status_for_day(
    guard: DependencyGuard,
    run_date: date,
    daily_sequence: set[str],
    quality_log: pl.DataFrame,
) -> dict[str, str]:
    bronze = _bronze_raw_status(guard, run_date, daily_sequence)
    silver = _silver_raw_status(guard, run_date, daily_sequence, quality_log, bronze)
    gold   = _gold_raw_status(guard, run_date, daily_sequence, silver)
    return {"bronze": bronze, "silver": silver, "gold": gold}


def _emit_status(raw_history: list[str]) -> tuple[str, int]:
    """
    raw_history[0] = run_date's raw status, raw_history[1] = run_date-1,
    etc. (most recent first). Returns (gate_status, streak_days) per the
    hysteresis rule in the module docstring.
    """
    current = raw_history[0]
    streak = 1
    for prev in raw_history[1:]:
        if prev == current:
            streak += 1
        else:
            break

    if current == "pass":
        emitted = "pass" if streak >= RECOVERY_CONFIRM_DAYS else "warn"
    elif current == "block":
        emitted = "block" if streak >= BLOCK_CONFIRM_DAYS else "warn"
    else:
        emitted = "warn"

    return emitted, streak


def _read_quality_log() -> pl.DataFrame:
    empty_schema = {
        "run_date": pl.Utf8, "check_name": pl.Utf8, "tier": pl.Utf8,
        "passed": pl.Boolean, "detail": pl.Utf8, "logged_at": pl.Utf8,
    }
    if not QUALITY_GATE_LOG_PATH.exists():
        return pl.DataFrame(schema=empty_schema)
    try:
        return pl.read_parquet(QUALITY_GATE_LOG_PATH)
    except Exception as e:
        logger.warning(f"[Gatekeeper] quality_gate_log read failed (treating as empty): {e}")
        return pl.DataFrame(schema=empty_schema)


def compute(run_date: date) -> pl.DataFrame:
    """Compute this run_date's gate_status row for all three scopes,
    recomputing up to LOOKBACK_DAYS of raw history from the underlying
    signals each time (never trusts a previously-written gate_status.parquet
    row) so a bad historical write can never propagate forward."""
    from src.scheduler.job_registry import DAILY_SEQUENCE

    guard = DependencyGuard()
    daily_sequence = set(DAILY_SEQUENCE)
    quality_log = _read_quality_log()

    history: dict[str, list[str]] = {s: [] for s in SCOPES}
    for offset in range(LOOKBACK_DAYS):
        day = run_date - timedelta(days=offset)
        day_status = _raw_status_for_day(guard, day, daily_sequence, quality_log)
        for scope in SCOPES:
            history[scope].append(day_status[scope])

    rows = []
    for scope in SCOPES:
        emitted, streak = _emit_status(history[scope])
        rows.append({
            "run_date":     run_date.isoformat(),
            "scope":        scope,
            "layer_health": history[scope][0],
            "gate_status":  emitted,
            "streak_days":  streak,
        })
    return pl.DataFrame(rows)


def run(run_date: date) -> None:
    """Job entry point ('gatekeeper' in JOB_REGISTRY) — sits immediately
    before health_report in DAILY_SEQUENCE, so it reads that day's fully
    completed sentinels/checks before health_report surfaces gate status
    alongside its own summary."""
    result = compute(run_date)

    try:
        day_str = run_date.isoformat()
        if GATE_STATUS_PATH.exists():
            existing = pl.read_parquet(GATE_STATUS_PATH)
            existing = existing.filter(pl.col("run_date") != day_str)
            out = pl.concat([existing, result], how="diagonal_relaxed")
        else:
            out = result
        atomic_write_parquet(out, GATE_STATUS_PATH, compression="zstd", compression_level=3)
    except Exception as e:
        # A logging/persistence failure here must not fail the pipeline —
        # health_report (the very next job) degrades gracefully if gate
        # status is unavailable for today.
        logger.warning(f"[Gatekeeper] gate_status persistence failed (non-blocking): {e}")

    summary = ", ".join(
        f"{r['scope']}={r['gate_status']}({r['layer_health']}, streak={r['streak_days']})"
        for r in result.to_dicts()
    )
    logger.info(f"[Gatekeeper] {run_date}: {summary}")
