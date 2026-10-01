"""
scripts/replay_gatekeeper.py — re-run Gatekeeper against REAL sentinel history.

Why this exists (ADR-050 / RISK-35): Gatekeeper's hysteresis windows
(RECOVERY_CONFIRM_DAYS=2, BLOCK_CONFIRM_DAYS=4) are first-pass calibration.
The only honest way to tighten them is to replay the logic over the sentinel
history the pipeline has actually accumulated, and see (a) whether every
raw status is explained by a real gap and (b) how much the emitted status
flaps under alternative windows. This makes that repeatable as history grows.

Read-only: reads a sentinel directory (and optionally a quality_gate_log
parquet), prints a table, writes nothing. Safe to point at the live
data/.sentinels.

    python scripts/replay_gatekeeper.py --from 2026-09-08 --to 2026-09-28
    python scripts/replay_gatekeeper.py --from 2026-09-08 --to 2026-09-28 --sweep
    python scripts/replay_gatekeeper.py --sentinel-dir /tmp/copy/.sentinels ...

Caveat: replays are judged against TODAY'S job registry — a job added after
a replayed date (e.g. bronze_fred_daily, 7 Sep 2026) shows as missing for
earlier dates. Start the range after the newest job existed.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import src.utils.gatekeeper as gk
from src.scheduler.dependency_guard import DependencyGuard

SWEEP_WINDOWS = [(1, 1), (1, 3), (2, 3), (2, 4), (3, 4), (2, 5), (3, 5)]


def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def replay(
    start: date,
    end: date,
    sentinel_dir: Path = Path("data/.sentinels"),
    quality_log: Path | None = None,
) -> list[dict]:
    """One dict per (date, scope): layer_health / gate_status / streak_days.
    Patches gatekeeper's guard + quality-log path for the duration and
    always restores them."""
    orig_guard, orig_log = gk.DependencyGuard, gk.QUALITY_GATE_LOG_PATH
    gk.DependencyGuard = lambda: DependencyGuard(sentinel_dir=sentinel_dir)
    gk.QUALITY_GATE_LOG_PATH = quality_log or Path("/nonexistent/quality_gate_log.parquet")
    try:
        rows: list[dict] = []
        for day in _days(start, end):
            rows.extend(gk.compute(day).to_dicts())
        return rows
    finally:
        gk.DependencyGuard, gk.QUALITY_GATE_LOG_PATH = orig_guard, orig_log


def sweep(
    start: date,
    end: date,
    sentinel_dir: Path = Path("data/.sentinels"),
    quality_log: Path | None = None,
    windows: list[tuple[int, int]] = SWEEP_WINDOWS,
) -> list[dict]:
    """Emitted-status flips and block-days per scope for each (recovery,
    block) window pair. Restores the module's real windows afterwards."""
    orig = gk.RECOVERY_CONFIRM_DAYS, gk.BLOCK_CONFIRM_DAYS
    out = []
    try:
        for rec, blk in windows:
            gk.RECOVERY_CONFIRM_DAYS, gk.BLOCK_CONFIRM_DAYS = rec, blk
            rows = replay(start, end, sentinel_dir, quality_log)
            entry = {"recovery": rec, "block": blk}
            for scope in gk.SCOPES:
                seq = [r["gate_status"] for r in rows if r["scope"] == scope]
                entry[f"{scope}_flips"] = sum(1 for a, b in zip(seq, seq[1:]) if a != b)
                entry[f"{scope}_block_days"] = seq.count("block")
            out.append(entry)
    finally:
        gk.RECOVERY_CONFIRM_DAYS, gk.BLOCK_CONFIRM_DAYS = orig
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--from", dest="start", required=True, type=date.fromisoformat)
    p.add_argument("--to", dest="end", required=True, type=date.fromisoformat)
    p.add_argument("--sentinel-dir", type=Path, default=Path("data/.sentinels"))
    p.add_argument("--quality-log", type=Path, default=None)
    p.add_argument("--sweep", action="store_true", help="compare alternative hysteresis windows")
    a = p.parse_args()

    if a.sweep:
        print(f"{'(rec,blk)':10}" + "".join(f"{s+' flips':>14}{s+' blk-days':>16}" for s in gk.SCOPES))
        for e in sweep(a.start, a.end, a.sentinel_dir, a.quality_log):
            print(f"{str((e['recovery'], e['block'])):10}" + "".join(
                f"{e[s + '_flips']:>14}{e[s + '_block_days']:>16}" for s in gk.SCOPES))
        return

    rows = replay(a.start, a.end, a.sentinel_dir, a.quality_log)
    print(f"{'date':12}" + "".join(f"{s:>16}" for s in gk.SCOPES) + "   (raw>emitted, streak)")
    for day in _days(a.start, a.end):
        by = {r["scope"]: r for r in rows if r["run_date"] == day.isoformat()}
        print(f"{day}  " + "".join(
            f"{by[s]['layer_health'] + '>' + by[s]['gate_status'] + ',' + str(by[s]['streak_days']):>16}"
            for s in gk.SCOPES))


if __name__ == "__main__":
    main()
