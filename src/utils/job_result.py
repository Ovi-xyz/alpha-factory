"""
src/utils/job_result.py — GHR-03 (ADR-059, GMI-GHR-REC-001)

What a job tells the runner about the work it did.

Problem (ADR-059 HR3): `rows_written` was 0 on 3,428 of 3,428 `pipeline_runs` rows, because
`run_job` never passed it, and `source` / `symbol` / `timeframe` were empty too. The Telegram success
line therefore always said "0 rows", and an empty run looked identical to a full one.

Contract:
    * A job function MAY return a ``JobResult``. ``run_job`` records its ``rows_written``.
    * A job that returns anything else (today: every job but the three that report counts) records
      ``rows_written = NULL`` — UNKNOWN, never 0. A 0 is a claim ("this job wrote nothing") and is
      only ever recorded when a job actually says so.
    * Only ``JobResult`` is honoured. A bare ``int`` return is NOT treated as a row count: several
      entry points return counters whose meaning is not "rows written", and mistaking one for the
      other would put a plausible-looking wrong number into the audit table.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class JobResult:
    rows_written: int | None = None
    rows_by_key: dict[str, int] | None = None
    warnings: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.rows_written is not None:
            if isinstance(self.rows_written, bool) or not isinstance(self.rows_written, int) \
                    or self.rows_written < 0:
                raise ValueError(f"rows_written must be a non-negative int or None, got {self.rows_written!r}")

    @staticmethod
    def rows_from(value: object) -> int | None:
        """The row count a job's return value declares, or None (unknown)."""
        return value.rows_written if isinstance(value, JobResult) else None

    @staticmethod
    def warnings_from(value: object) -> tuple[str, ...]:
        return value.warnings if isinstance(value, JobResult) else ()
