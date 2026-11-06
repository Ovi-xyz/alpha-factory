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

    scope        — 'bronze' | 'silver' | 'gold' | and, since ADR-053 D4, one DOMAIN scope per
                   intelligence domain ('hmm' | 'technical' | 'cross_asset' — the keys of
                   job_registry.GATE_DOMAINS): the health of that domain's own Gold jobs plus
                   their transitive upstream (Bronze/Silver) jobs, NOTHING else. A CrossAssetEngine
                   outage therefore never degrades the HMM domain's status, which is what lets a
                   specialist model tag its output with the status of its own chain only.
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

ADR-059 amendments (GHR-04 / GHR-05 / GHR-09, GMI-GHR-REC-001):
    - `skipped` raw status. A day covered by a deliberate-pause marker for a layer scope
      (src/utils/pause_markers.py, `runner.py --pause`) is `skipped`, not `block`; a DOMAIN scope is
      `skipped` when any layer in its chain is paused. `skipped` is TRANSPARENT in two places: when
      layers combine (`_worse`) and in the streak (`_emit_status`) — it neither extends nor breaks a
      streak, so the 2-day recovery and 4-day block windows count only evaluated days. A failing job on
      the first day after a long pause therefore emits `warn`, not an immediate `block`
      (GK5, "resume trap"). A scope that is `skipped` TODAY emits gate_status 'skipped'.
    - `pending` weekly jobs. On the EVALUATION day only (offset 0), a due weekly-only job (guarded by
      run_on_weekdays alone, not in DAILY_SEQUENCE) that has no sentinel is `pending`, not `missing`,
      while NO weekly-only job due that day has started (GK3: Sunday's gate ran 35 minutes before the
      weekly sentinels were written). As soon as one weekly job has a sentinel the sequence has
      started and any still-unfinished one is `missing`. Past days (offset >= 1) are final: a weekly
      job without a sentinel is `missing`, so a wrongly-`pending` evaluation self-corrects on the
      next day's recomputation. The recovery/block windows are NOT changed.
    - Append-only evidence: every `run()` also writes one part file
      data/health/gate_log/evaluated_at=<ts>.parquet holding, per (scope, day), the raw status, the
      quality verdict and quarantine files consulted, and per member job its state and sentinel
      mtime. History is recomputed from sentinels (mutable files), so without this log a sentinel
      cleanup rewrites the past; with it `gate_status` is derivable from the log alone
      (`derive_gate_status`). Part files are never rewritten.
    - Code/ADR note (GK6): ADR-050 says the Gatekeeper reads `pipeline_runs.db`; it never did, and
      must not while that store holds test fixtures (HR4). The ADR text is amended, not the code.
"""

from __future__ import annotations

import glob as glob_module
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
from loguru import logger

from src.scheduler.dependency_guard import DependencyGuard
from src.utils.atomic_io import atomic_write_parquet
from src.utils.data_paths import health_dir, quarantine_dir
from src.utils.pause_markers import is_paused, load_pauses, paused_layers_on

GATE_STATUS_PATH      = health_dir() / "gate_status.parquet"          # GHR-01: $ALPHA_FACTORY_DATA_DIR-aware
QUALITY_GATE_LOG_PATH = health_dir() / "quality_gate_log.parquet"
QUARANTINE_GLOB       = str(quarantine_dir() / "*.parquet")

SCOPES = ("bronze", "silver", "gold")

# ADR-059 GHR-05: append-only evidence log (one part file per evaluation).
GATE_LOG_DIR = health_dir() / "gate_log"

# ADR-059 GHR-09: a raw status for days covered by a pause marker. Not part of _SEVERITY on purpose:
# it is transparent (see _worse / _emit_status), never "worse" or "better" than anything.
SKIPPED = "skipped"

# Per-job states recorded in the evidence log.
JOB_DONE, JOB_MISSING, JOB_PENDING, JOB_PAUSED = "done", "missing", "pending", "paused"

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
    """Return whichever of two statuses is more severe (block > warn > pass). `skipped` (a paused
    layer, ADR-059 GHR-09) is transparent: it contributes nothing to a cascade."""
    if a == SKIPPED:
        return b
    if b == SKIPPED:
        return a
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


def _quarantine_day_and_kind(fname: str) -> tuple[date | None, str | None]:
    """(day, kind) a quarantine file records about ITSELF (ADR-059 GHR-07): `_quarantined_at` and
    `_quarantine_kind`, written by SchemaValidator.handle_mismatch. A file without them (written before
    GHR-07) or one that cannot be read falls back to its mtime date with kind None — the legacy rule.
    The recorded day is a naive LOCAL timestamp, the same clock `run_date` (date.today()) uses."""
    try:
        names = pl.scan_parquet(fname).collect_schema().names()
        if "_quarantined_at" in names:
            cols = ["_quarantined_at"] + (["_quarantine_kind"] if "_quarantine_kind" in names else [])
            row = pl.scan_parquet(fname).select(cols).head(1).collect()
            if row.height:
                stamp = row["_quarantined_at"][0]
                kind = row["_quarantine_kind"][0] if "_quarantine_kind" in cols else None
                return date.fromisoformat(str(stamp)[:10]), kind
    except Exception:                                   # noqa: BLE001 — unreadable/odd file: use mtime
        pass
    try:
        return date.fromtimestamp(Path(fname).stat().st_mtime), None
    except (OSError, ValueError):
        return None, None


def _quarantine_files(run_date: date) -> list[str]:
    """Bronze schema-mismatch quarantine files (schema_validator.py) quarantined on run_date.

    ADR-059 GHR-07: the day comes from the file's own `_quarantined_at` and only `schema_mismatch`
    files count; mtime is used only for legacy files that lack the marker. Previously mtime alone
    decided, so a copied/restored/touched file read as "quarantined today". Returned, not just counted,
    so the evidence log can name what a verdict rested on (ADR-059 GHR-05)."""
    found: list[str] = []
    try:
        for fname in sorted(glob_module.glob(QUARANTINE_GLOB)):
            day, kind = _quarantine_day_and_kind(fname)
            if day == run_date and kind in (None, "schema_mismatch"):
                found.append(fname)
    except OSError:
        pass
    return found


def _quarantine_today(run_date: date) -> bool:
    """Any Bronze schema-registry quarantine file written on run_date? (see _quarantine_files)"""
    return bool(_quarantine_files(run_date))


def _max_age_for(job_name: str, daily_sequence: set[str]) -> int:
    """The sentinel window _job_completed() applies to this job (0 = exact date)."""
    from src.scheduler.job_registry import JOB_REGISTRY

    if job_name in daily_sequence or _due_exactly_on_guard_days(JOB_REGISTRY[job_name]):
        return 0
    return WEEKLY_STALE_DAYS


def _sentinel_mtime(guard: DependencyGuard, job_name: str, run_date: date, max_age: int) -> str | None:
    """ISO mtime (UTC) of the newest sentinel inside the job's window, else None. Evidence only."""
    for back in range(max_age + 1):
        path = guard._sentinel_path(job_name, run_date - timedelta(days=back))
        try:
            if path.exists():
                return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
        except OSError:
            continue
    return None


def _is_weekly_only_guard_job(job_name: str, daily_sequence: set[str]) -> bool:
    from src.scheduler.job_registry import JOB_REGISTRY

    return job_name not in daily_sequence and _due_exactly_on_guard_days(JOB_REGISTRY[job_name])


def _weekly_sequence_started(guard: DependencyGuard, run_date: date, daily_sequence: set[str]) -> bool:
    """True once ANY weekly-only job that is due on run_date has its sentinel (ADR-059 GHR-04)."""
    from src.scheduler.job_registry import JOB_REGISTRY, WEEKLY_SEQUENCE, _passes_schedule

    due = [
        j for j in dict.fromkeys(WEEKLY_SEQUENCE)
        if _is_weekly_only_guard_job(j, daily_sequence) and _passes_schedule(JOB_REGISTRY[j], run_date)
    ]
    return any(guard.is_done(j, run_date) for j in due)


def _classify_jobs(
    guard: DependencyGuard,
    jobs: list[str],
    run_date: date,
    daily_sequence: set[str],
    *,
    allow_pending: bool = False,
    paused_layers: frozenset[str] = frozenset(),
) -> dict[str, str]:
    """{job: done | missing | pending | paused}. `pending` only with allow_pending (the evaluation
    day) and only for a due weekly-only job while the weekly sequence has not started (GHR-04)."""
    from src.scheduler.job_registry import JOB_REGISTRY

    started: bool | None = None
    states: dict[str, str] = {}
    for job_name in jobs:
        if JOB_REGISTRY[job_name]["layer"] in paused_layers:
            states[job_name] = JOB_PAUSED
        elif _job_completed(guard, job_name, run_date, daily_sequence):
            states[job_name] = JOB_DONE
        elif allow_pending and _is_weekly_only_guard_job(job_name, daily_sequence):
            if started is None:
                started = _weekly_sequence_started(guard, run_date, daily_sequence)
            states[job_name] = JOB_MISSING if started else JOB_PENDING
        else:
            states[job_name] = JOB_MISSING
    return states


def _record(
    evidence: list[dict] | None, *, scope: str, run_date: date, day_offset: int, raw: str,
    states: dict[str, str], guard: DependencyGuard, daily_sequence: set[str],
    quality_local: str | None = None, quarantine_files: list[str] | None = None,
) -> None:
    """Append this (scope, day)'s header row and one row per member job to the evidence list."""
    if evidence is None:
        return
    day = run_date.isoformat()
    evidence.append({
        "scope": scope, "day": day, "day_offset": day_offset, "raw_status": raw,
        "quality_local": quality_local,
        "quarantine_files": json.dumps(quarantine_files) if quarantine_files else None,
        "job": None, "job_state": None, "expected": None, "sentinel_mtime": None,
    })
    for job_name, state in states.items():
        evidence.append({
            "scope": scope, "day": day, "day_offset": day_offset, "raw_status": None,
            "quality_local": None, "quarantine_files": None,
            "job": job_name, "job_state": state, "expected": True,
            "sentinel_mtime": _sentinel_mtime(guard, job_name, run_date, _max_age_for(job_name, daily_sequence)),
        })


def _bronze_raw_status(
    guard: DependencyGuard, run_date: date, daily_sequence: set[str], *,
    allow_pending: bool = False, pauses: dict | None = None,
    evidence: list[dict] | None = None, day_offset: int = 0,
) -> str:
    pauses = load_pauses() if pauses is None else pauses
    expected = _expected_jobs_for_layer("bronze", run_date)
    if is_paused(pauses, "bronze", run_date):
        _record(evidence, scope="bronze", run_date=run_date, day_offset=day_offset, raw=SKIPPED,
                states={j: JOB_PAUSED for j in expected}, guard=guard, daily_sequence=daily_sequence)
        return SKIPPED
    states = _classify_jobs(guard, expected, run_date, daily_sequence, allow_pending=allow_pending)
    qfiles = _quarantine_files(run_date)
    missing = [j for j, st in states.items() if st == JOB_MISSING]
    raw = "block" if missing else ("warn" if qfiles else "pass")
    _record(evidence, scope="bronze", run_date=run_date, day_offset=day_offset, raw=raw, states=states,
            guard=guard, daily_sequence=daily_sequence, quarantine_files=qfiles)
    return raw


def _quality_local_status(quality_log: pl.DataFrame, run_date: date) -> str:
    """Silver's persisted-check verdict for run_date: block on a failed CRITICAL check, warn on a
    failed WARNING check, and warn when no check detail exists for the date at all (silver_validate's
    own sentinel says done, but absence of evidence is not evidence of health — e.g. a prior run
    predates FIX GMI-GATE-001, or the persistence write failed and was swallowed)."""
    day_str = run_date.isoformat()
    day_log = quality_log.filter(pl.col("run_date") == day_str) if quality_log.height else quality_log
    if day_log.height == 0:
        return "warn"
    critical_fail = day_log.filter((pl.col("tier") == "critical") & (~pl.col("passed"))).height > 0
    warning_fail = day_log.filter((pl.col("tier") == "warning") & (~pl.col("passed"))).height > 0
    return "block" if critical_fail else ("warn" if warning_fail else "pass")


def _silver_raw_status(
    guard: DependencyGuard,
    run_date: date,
    daily_sequence: set[str],
    quality_log: pl.DataFrame,
    bronze_status: str,
    *,
    allow_pending: bool = False, pauses: dict | None = None,
    evidence: list[dict] | None = None, day_offset: int = 0,
) -> str:
    pauses = load_pauses() if pauses is None else pauses
    expected = _expected_jobs_for_layer("silver", run_date)
    if is_paused(pauses, "silver", run_date):
        _record(evidence, scope="silver", run_date=run_date, day_offset=day_offset, raw=SKIPPED,
                states={j: JOB_PAUSED for j in expected}, guard=guard, daily_sequence=daily_sequence)
        return SKIPPED
    states = _classify_jobs(guard, expected, run_date, daily_sequence, allow_pending=allow_pending)
    missing = [j for j, st in states.items() if st == JOB_MISSING]

    quality_local = None
    if missing:
        local = "block"
    else:
        local = quality_local = _quality_local_status(quality_log, run_date)

    raw = _worse(bronze_status, local)
    _record(evidence, scope="silver", run_date=run_date, day_offset=day_offset, raw=raw, states=states,
            guard=guard, daily_sequence=daily_sequence, quality_local=quality_local)
    return raw


def _gold_raw_status(
    guard: DependencyGuard,
    run_date: date,
    daily_sequence: set[str],
    silver_status: str,
    *,
    allow_pending: bool = False, pauses: dict | None = None,
    evidence: list[dict] | None = None, day_offset: int = 0,
) -> str:
    pauses = load_pauses() if pauses is None else pauses
    expected = _expected_jobs_for_layer("gold", run_date)
    if is_paused(pauses, "gold", run_date):
        _record(evidence, scope="gold", run_date=run_date, day_offset=day_offset, raw=SKIPPED,
                states={j: JOB_PAUSED for j in expected}, guard=guard, daily_sequence=daily_sequence)
        return SKIPPED
    states = _classify_jobs(guard, expected, run_date, daily_sequence, allow_pending=allow_pending)
    missing = [j for j, st in states.items() if st == JOB_MISSING]
    # No per-check Gold quality validator exists yet (see module docstring)
    # -- job completion is the only local Gold signal available today.
    local = "block" if missing else "pass"
    raw = _worse(silver_status, local)
    _record(evidence, scope="gold", run_date=run_date, day_offset=day_offset, raw=raw, states=states,
            guard=guard, daily_sequence=daily_sequence)
    return raw


def _raw_status_for_day(
    guard: DependencyGuard,
    run_date: date,
    daily_sequence: set[str],
    quality_log: pl.DataFrame,
    *,
    allow_pending: bool = False, pauses: dict | None = None,
    evidence: list[dict] | None = None, day_offset: int = 0,
) -> dict[str, str]:
    pauses = load_pauses() if pauses is None else pauses
    kw = dict(allow_pending=allow_pending, pauses=pauses, evidence=evidence, day_offset=day_offset)
    bronze = _bronze_raw_status(guard, run_date, daily_sequence, **kw)
    silver = _silver_raw_status(guard, run_date, daily_sequence, quality_log, bronze, **kw)
    gold   = _gold_raw_status(guard, run_date, daily_sequence, silver, **kw)
    return {"bronze": bronze, "silver": silver, "gold": gold}


# ── Domain scopes (ADR-053 D4) ─────────────────────────────────────────────────────

def _upstream_closure(job_names, registry: dict) -> set[str]:
    """The jobs plus everything they transitively depend_on."""
    seen: set[str] = set()
    stack = list(job_names)
    while stack:
        j = stack.pop()
        if j in seen:
            continue
        seen.add(j)
        stack.extend(registry[j].get("depends_on", []))
    return seen


def domain_members(domain: str, run_date: date) -> list[str]:
    """Sequenced jobs of `domain`'s chain (its GATE_DOMAINS Gold jobs + their transitive upstream)
    that were scheduled to run on run_date."""
    from src.scheduler.job_registry import GATE_DOMAINS, JOB_REGISTRY, LAYER_JOB_NAMES, _passes_schedule

    if domain not in GATE_DOMAINS:
        raise KeyError(f"unknown gate domain {domain!r}; known: {sorted(GATE_DOMAINS)}")
    sequenced = {j for names in LAYER_JOB_NAMES.values() for j in names}
    closure = _upstream_closure(GATE_DOMAINS[domain], JOB_REGISTRY)
    return sorted(j for j in closure if j in sequenced and _passes_schedule(JOB_REGISTRY[j], run_date))


def _domain_raw_status(
    domain: str,
    guard: DependencyGuard,
    run_date: date,
    daily_sequence: set[str],
    quality_log: pl.DataFrame,
    *,
    allow_pending: bool = False, pauses: dict | None = None,
    evidence: list[dict] | None = None, day_offset: int = 0,
) -> str:
    """One day's raw status of a domain chain: block when any member job that was due did not
    complete; otherwise the worst of (a) Silver's persisted-check verdict, ONLY when silver_validate
    is part of the chain, and (b) a Bronze quarantine warning, ONLY when a Bronze job is.

    ADR-059: `skipped` when any layer the chain runs through is paused that day (GHR-09); due
    weekly-only members are `pending` rather than `missing` on the evaluation day (GHR-04)."""
    from src.scheduler.job_registry import GATE_DOMAINS, JOB_REGISTRY

    pauses = load_pauses() if pauses is None else pauses
    members = domain_members(domain, run_date)
    chain = _upstream_closure(GATE_DOMAINS[domain], JOB_REGISTRY)
    chain_layers = {JOB_REGISTRY[j]["layer"] for j in chain}
    paused = paused_layers_on(pauses, run_date)

    if chain_layers & paused:
        _record(evidence, scope=domain, run_date=run_date, day_offset=day_offset, raw=SKIPPED,
                states={j: JOB_PAUSED for j in members}, guard=guard, daily_sequence=daily_sequence)
        return SKIPPED

    states = _classify_jobs(guard, members, run_date, daily_sequence, allow_pending=allow_pending)
    if any(st == JOB_MISSING for st in states.values()):
        _record(evidence, scope=domain, run_date=run_date, day_offset=day_offset, raw="block",
                states=states, guard=guard, daily_sequence=daily_sequence)
        return "block"

    status = "pass"
    quality_local = None
    if "silver_validate" in chain:
        quality_local = _quality_local_status(quality_log, run_date)
        status = _worse(status, quality_local)
    qfiles: list[str] = []
    if any(JOB_REGISTRY[j]["layer"] == "bronze" for j in chain):
        qfiles = _quarantine_files(run_date)
        if qfiles:
            status = _worse(status, "warn")
    _record(evidence, scope=domain, run_date=run_date, day_offset=day_offset, raw=status, states=states,
            guard=guard, daily_sequence=daily_sequence, quality_local=quality_local, quarantine_files=qfiles)
    return status


def domain_status(domain: str, run_date: date, guard: DependencyGuard | None = None,
                  quality_log: pl.DataFrame | None = None, *,
                  pauses: dict | None = None, evidence: list[dict] | None = None) -> dict:
    """{'layer_health', 'gate_status', 'streak_days'} for one domain scope — the same hysteresis
    (_emit_status) as the layer scopes, recomputed from the sentinels over LOOKBACK_DAYS."""
    from src.scheduler.job_registry import DAILY_SEQUENCE

    guard = guard or DependencyGuard()
    quality_log = _read_quality_log() if quality_log is None else quality_log
    pauses = load_pauses() if pauses is None else pauses
    daily_sequence = set(DAILY_SEQUENCE)
    history = [
        _domain_raw_status(domain, guard, run_date - timedelta(days=o), daily_sequence, quality_log,
                           allow_pending=(o == 0), pauses=pauses, evidence=evidence, day_offset=o)
        for o in range(LOOKBACK_DAYS)
    ]
    emitted, streak = _emit_status(history)
    return {"layer_health": history[0], "gate_status": emitted, "streak_days": streak}


def domain_gate_status(domain: str, run_date: date) -> str:
    """What a specialist model stamps as its `quality` column. 'unknown' (never an exception) when
    the status cannot be computed — scoring must not die because the gate could not be read."""
    try:
        return domain_status(domain, run_date)["gate_status"]
    except Exception as e:                                  # noqa: BLE001 — see docstring
        logger.warning(f"[Gatekeeper] domain status for {domain!r} unavailable ({e}) — 'unknown'")
        return "unknown"


def _emit_status(raw_history: list[str]) -> tuple[str, int]:
    """
    raw_history[0] = run_date's raw status, raw_history[1] = run_date-1,
    etc. (most recent first). Returns (gate_status, streak_days) per the
    hysteresis rule in the module docstring.

    ADR-059 GHR-09: `skipped` days are TRANSPARENT. They neither extend nor break a streak, so the
    recovery (2-day) and block (4-day) windows count only evaluated days; a scope that is `skipped`
    today emits 'skipped' with the length of the leading skipped run as its streak.
    """
    current = raw_history[0]
    if current == SKIPPED:
        run = 1
        for prev in raw_history[1:]:
            if prev != SKIPPED:
                break
            run += 1
        return SKIPPED, run

    streak = 1
    for prev in raw_history[1:]:
        if prev == SKIPPED:
            continue
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


_EVIDENCE_SCHEMA = {
    "evaluated_at": pl.Utf8, "run_date": pl.Utf8, "evidence": pl.Utf8, "scope": pl.Utf8,
    "day": pl.Utf8, "day_offset": pl.Int32, "raw_status": pl.Utf8, "quality_local": pl.Utf8,
    "quarantine_files": pl.Utf8, "job": pl.Utf8, "job_state": pl.Utf8, "expected": pl.Boolean,
    "sentinel_mtime": pl.Utf8, "gate_status": pl.Utf8, "streak_days": pl.Int32,
}

_GATE_STATUS_SCHEMA = {
    "run_date": pl.Utf8, "scope": pl.Utf8, "layer_health": pl.Utf8,
    "gate_status": pl.Utf8, "streak_days": pl.Int64,
}


def compute_with_evidence(run_date: date) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(gate_status rows, evidence rows) for this run_date.

    Recomputes up to LOOKBACK_DAYS of raw history from the underlying signals each time (never
    trusts a previously-written gate_status.parquet row), so a bad historical write can never
    propagate forward. The evidence frame records what each verdict rested on (ADR-059 GHR-05)."""
    from src.scheduler.job_registry import DAILY_SEQUENCE, GATE_DOMAINS

    guard = DependencyGuard()
    daily_sequence = set(DAILY_SEQUENCE)
    quality_log = _read_quality_log()
    pauses = load_pauses()
    evidence: list[dict] = []

    history: dict[str, list[str]] = {s: [] for s in SCOPES}
    for offset in range(LOOKBACK_DAYS):
        day = run_date - timedelta(days=offset)
        day_status = _raw_status_for_day(
            guard, day, daily_sequence, quality_log,
            allow_pending=(offset == 0), pauses=pauses, evidence=evidence, day_offset=offset,
        )
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
    for domain in GATE_DOMAINS:                              # ADR-053 D4: one row per intelligence domain
        st = domain_status(domain, run_date, guard, quality_log, pauses=pauses, evidence=evidence)
        rows.append({
            "run_date":     run_date.isoformat(),
            "scope":        domain,
            "layer_health": st["layer_health"],
            "gate_status":  st["gate_status"],
            "streak_days":  st["streak_days"],
        })

    # The emitted verdict rides on each scope's offset-0 header row, so the log alone says what was
    # published (and `derive_gate_status` can be checked against it).
    emitted_by_scope = {r["scope"]: r for r in rows}
    for ev in evidence:
        if ev["job"] is None and ev["day_offset"] == 0:
            ev["gate_status"] = emitted_by_scope[ev["scope"]]["gate_status"]
            ev["streak_days"] = emitted_by_scope[ev["scope"]]["streak_days"]

    evaluated_at = datetime.now(timezone.utc).isoformat()
    ev_rows = [{
        "evaluated_at": evaluated_at, "run_date": run_date.isoformat(), "evidence": "evaluated",
        "gate_status": None, "streak_days": None, **e,
    } for e in evidence]
    return pl.DataFrame(rows), pl.DataFrame(ev_rows, schema=_EVIDENCE_SCHEMA)


def compute(run_date: date) -> pl.DataFrame:
    """Compute this run_date's gate_status rows for every scope (bronze, silver, gold + the ADR-053
    domains). See compute_with_evidence()."""
    return compute_with_evidence(run_date)[0]


def _write_gate_log(frame: pl.DataFrame) -> Path:
    """Append one evaluation as its own part file; an existing part is NEVER rewritten."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = GATE_LOG_DIR / f"evaluated_at={stamp}.parquet"
    n = 0
    while path.exists():
        n += 1
        path = GATE_LOG_DIR / f"evaluated_at={stamp}-{n}.parquet"
    atomic_write_parquet(frame, path, compression="zstd", compression_level=3)
    return path


def read_gate_log(log_dir: Path | None = None) -> pl.DataFrame:
    """Every part file of the evidence log as one frame (empty frame when there is none)."""
    base = Path(log_dir) if log_dir is not None else GATE_LOG_DIR
    parts = sorted(base.glob("*.parquet")) if base.exists() else []
    if not parts:
        return pl.DataFrame(schema=_EVIDENCE_SCHEMA)
    return pl.concat([pl.read_parquet(f) for f in parts], how="diagonal_relaxed")


def _seed_legacy_gate_log() -> bool:
    """One-time: when the log does not exist yet but a gate_status.parquet does, carry its rows in as
    evidence='legacy' so the history that predates the log is not orphaned. Idempotent."""
    if GATE_LOG_DIR.exists() and any(GATE_LOG_DIR.glob("*.parquet")):
        return False
    if not GATE_STATUS_PATH.exists():
        return False
    legacy = pl.read_parquet(GATE_STATUS_PATH)
    if legacy.height == 0:
        return False
    rows = [{
        "evaluated_at": "legacy", "run_date": r["run_date"], "evidence": "legacy", "scope": r["scope"],
        "day": r["run_date"], "day_offset": 0, "raw_status": r["layer_health"],
        "quality_local": None, "quarantine_files": None, "job": None, "job_state": None,
        "expected": None, "sentinel_mtime": None,
        "gate_status": r["gate_status"], "streak_days": r["streak_days"],
    } for r in legacy.to_dicts()]
    atomic_write_parquet(pl.DataFrame(rows, schema=_EVIDENCE_SCHEMA), GATE_LOG_DIR / "legacy_seed.parquet",
                         compression="zstd", compression_level=3)
    return True


def derive_gate_status(log: pl.DataFrame, run_date: date) -> pl.DataFrame:
    """Re-derive run_date's gate_status rows from the evidence log ALONE (ADR-059 GHR-05): the
    latest evaluation of that date, its per-day raw statuses through `_emit_status`. Falls back to
    the stored verdict for dates that only exist as legacy seed rows. Empty frame when unknown."""
    day_str = run_date.isoformat()
    heads = log.filter((pl.col("run_date") == day_str) & pl.col("job").is_null()) if log.height else log
    ev = heads.filter(pl.col("evidence") == "evaluated") if heads.height else heads
    rows = []
    if ev.height:
        latest = ev["evaluated_at"].max()
        ev = ev.filter(pl.col("evaluated_at") == latest)
        for scope in ev["scope"].unique(maintain_order=True).to_list():
            hist = ev.filter(pl.col("scope") == scope).sort("day_offset")["raw_status"].to_list()
            emitted, streak = _emit_status(hist)
            rows.append({"run_date": day_str, "scope": scope, "layer_health": hist[0],
                         "gate_status": emitted, "streak_days": streak})
    elif heads.height:
        for r in heads.filter(pl.col("evidence") == "legacy").to_dicts():
            rows.append({"run_date": day_str, "scope": r["scope"], "layer_health": r["raw_status"],
                         "gate_status": r["gate_status"], "streak_days": r["streak_days"]})
    return pl.DataFrame(rows, schema=_GATE_STATUS_SCHEMA)


def run(run_date: date) -> None:
    """Job entry point ('gatekeeper' in JOB_REGISTRY) — sits immediately
    before health_report in DAILY_SEQUENCE, so it reads that day's fully
    completed sentinels/checks before health_report surfaces gate status
    alongside its own summary."""
    result, evidence = compute_with_evidence(run_date)

    try:                                                      # ADR-059 GHR-05/09: keep pre-log history
        _seed_legacy_gate_log()
    except Exception as e:
        logger.warning(f"[Gatekeeper] legacy gate_log seed failed (non-blocking): {e}")

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

    try:                                                      # ADR-059 GHR-05: append-only evidence
        _write_gate_log(evidence)
    except Exception as e:
        logger.warning(f"[Gatekeeper] gate_log write failed (non-blocking): {e}")

    summary = ", ".join(
        f"{r['scope']}={r['gate_status']}({r['layer_health']}, streak={r['streak_days']})"
        for r in result.to_dicts()
    )
    logger.info(f"[Gatekeeper] {run_date}: {summary}")
