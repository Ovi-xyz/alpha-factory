"""
tests/unit/test_replay_gatekeeper_adr050.py

scripts/replay_gatekeeper.py — read-only replay of Gatekeeper over real
sentinel history, used to re-check the first-pass hysteresis windows as
history accumulates (ADR-050 / RISK-35).

The property that matters most is that it is SAFE to point at the live
data/.sentinels: it must not write, and it must restore the module-level
state it temporarily patches (guard factory, quality-log path, windows) even
when something raises mid-replay.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from src.scheduler.job_registry import GATE_DOMAINS

import src.utils.gatekeeper as gk
from scripts.replay_gatekeeper import replay, sweep
from src.scheduler.dependency_guard import DependencyGuard
from src.scheduler.job_registry import JOB_REGISTRY, LAYER_JOB_NAMES, _passes_schedule

START, END = date(2026, 9, 14), date(2026, 9, 27)   # Mon .. Sun, 14 days


def _mark_healthy(sentinel_dir: Path, days: list[date], skip: set[date] = frozenset()) -> None:
    guard = DependencyGuard(sentinel_dir=sentinel_dir)
    for day in days:
        if day in skip:
            continue
        for layer in ("bronze", "silver", "gold"):
            for job in LAYER_JOB_NAMES[layer]:
                if _passes_schedule(JOB_REGISTRY[job], day):
                    guard.mark_done(job, day)


def _range() -> list[date]:
    return [START + timedelta(days=i) for i in range((END - START).days + 1)]


class TestReplay:

    def test_three_scope_rows_per_day(self, tmp_path):
        _mark_healthy(tmp_path / "s", _range())
        rows = replay(START, END, tmp_path / "s")
        assert len(rows) == (3 + len(GATE_DOMAINS)) * len(_range())     # layers + ADR-053 domain scopes
        assert {r["scope"] for r in rows} == {"bronze", "silver", "gold", *GATE_DOMAINS}

    def test_empty_history_is_all_block(self, tmp_path):
        rows = replay(START, END, tmp_path / "empty")
        assert {r["layer_health"] for r in rows} == {"block"}

    def test_bronze_healthy_history_is_pass(self, tmp_path):
        _mark_healthy(tmp_path / "s", _range())
        rows = replay(START, END, tmp_path / "s")
        last_bronze = [r for r in rows if r["scope"] == "bronze"][-1]
        assert last_bronze["layer_health"] == "pass" and last_bronze["gate_status"] == "pass"

    def test_read_only_writes_nothing_into_sentinel_dir(self, tmp_path):
        _mark_healthy(tmp_path / "s", _range())
        before = sorted(p.name for p in (tmp_path / "s").iterdir())
        replay(START, END, tmp_path / "s")
        assert sorted(p.name for p in (tmp_path / "s").iterdir()) == before

    def test_restores_gatekeeper_globals(self, tmp_path):
        g, q = gk.DependencyGuard, gk.QUALITY_GATE_LOG_PATH
        replay(START, START, tmp_path / "s")
        assert gk.DependencyGuard is g and gk.QUALITY_GATE_LOG_PATH == q

    def test_restores_globals_even_when_compute_raises(self, tmp_path, monkeypatch):
        g, q = gk.DependencyGuard, gk.QUALITY_GATE_LOG_PATH
        monkeypatch.setattr(gk, "compute", lambda d: (_ for _ in ()).throw(RuntimeError("x")))
        with pytest.raises(RuntimeError):
            replay(START, START, tmp_path / "s")
        assert gk.DependencyGuard is g and gk.QUALITY_GATE_LOG_PATH == q


class TestSweep:

    def test_one_entry_per_window_pair(self, tmp_path):
        _mark_healthy(tmp_path / "s", _range())
        out = sweep(START, END, tmp_path / "s", windows=[(1, 1), (2, 4)])
        assert [(e["recovery"], e["block"]) for e in out] == [(1, 1), (2, 4)]

    def test_restores_real_windows(self, tmp_path):
        rec, blk = gk.RECOVERY_CONFIRM_DAYS, gk.BLOCK_CONFIRM_DAYS
        sweep(START, END, tmp_path / "s", windows=[(1, 1), (3, 5)])
        assert (gk.RECOVERY_CONFIRM_DAYS, gk.BLOCK_CONFIRM_DAYS) == (rec, blk)

    def test_single_missed_day_hard_blocks_only_under_block_window_1(self, tmp_path):
        """The finding the sweep exists to show: with one skipped day in an
        otherwise healthy fortnight, a block window of 1 hard-blocks it and
        the production windows (2, 4) do not."""
        days = _range()
        _mark_healthy(tmp_path / "s", days, skip={date(2026, 9, 23)})
        out = {(e["recovery"], e["block"]): e
               for e in sweep(START, END, tmp_path / "s", windows=[(1, 1), (2, 4)])}
        assert out[(1, 1)]["bronze_block_days"] >= 1
        assert out[(2, 4)]["bronze_block_days"] == 0
