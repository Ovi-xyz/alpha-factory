# 2026-11-10 — Job alignment: `--job all` and `--job bronze|silver|gold` are one chain (ADR-060, GMI-JOB-ALIGN-001)

**Request.** Ovi: evaluate the misalignment between `python src/runner.py --job all|bronze|silver|gold`, then implement the update
based on the recommended direction. ADR number and component tag are proposals. Implemented against GitHub `main` (`a9f1570`); the
files read were byte-identical to the live repo (`job_registry.py`, `gatekeeper.py`, `health_reporter.py`, `macro_processor.py`,
`hmm_core.py` diffed). No version bump.

## Explore
Read `job_registry.py`, `runner.py`, `dependency_guard.py`, `gatekeeper.py`, `health_reporter.py`, `hmm_core.py`, `macro_processor.py`,
`regime_tag.py`, `pipeline_scheduler.py`. Imported the REAL registry (only `JobResult` stubbed) and computed, per weekday of 9-15 Nov 2026,
the job set of each command. Hypotheses checked and dropped: weekly cross-asset jobs tagging a stale regime in a Sunday layer run (no new bars
on Sunday; `regime_tag` has the 5-day guard); a fit-before-classify deadlock (steady state has no unscored gap; narrow trap only after a failed
classify); `silver_fred_daily` / `silver_macro` writing the same snapshot (idempotent by design).

## Findings (measured)
| | Mon-Sat | Sunday |
|---|---|---|
| `all` | 24 | 26 |
| `bronze`+`silver`+`gold` | 24 | 34 |
| difference | `silver_macro`, `silver_global_rates` only in layer runs; `gatekeeper`, `health_report` only in `all` | 10 jobs never run by `all` (2 Silver macro + 8 weekly Gold) |

1. `all`-only operation: `silver_macro` is never refreshed; `silver_validate` / `gold_raw_macro` / `gold_raw_market` refuse on day 8.
2. Gatekeeper expects the 8 weekly Gold jobs exact-date on Sunday; the head's Bronze sentinels make `pending` unavailable, so Gold is raw `block` every Sunday.
3. `health_report` (`depends_on` `gold_screener`, no `run_on_failure`) cannot run after a failure; its FAILED alert reads same-day rows.
4. `--job gold` on Sunday runs ~100 min of weekly work first, with no failure isolation.
5. Layer runs run no Gatekeeper, no `health_report`, no `register_views`.

## Decide
One list (`WEEKLY_SEQUENCE` = head, daily core, tail, util) that `all` and the layer runs both derive from. Weekly Gold becomes a TAIL. Head/tail
derived from registry structure. `silver_macro` / `silver_global_rates` declare their Sunday cadence (the one reversible choice). `health_report` mirrors
`gatekeeper`. One failure policy (`_run_chain`) with tail isolation. Layer runs get the post-steps. The specialists' gate read treats not-yet-run tail
members as `pending`.

## Implement (local commits, each independently revertible)
WP1 registry; WP2+3 runner and gatekeeper; WP4 tests (new module, 79 tests; existing pins updated); docs. The README run-command and job-list sections
were rewritten (the counts there were already stale).

## ADR-060 (decision record — ADR texts live outside the repo)
**Decision.** (1) `jobs_for(run_date)` is the only definition of a day's chain; layer runs are filters of it. (2) Weekly-only jobs split into HEAD (weekday
guard only, every parent in the head) and TAIL (the rest), derived by `_split_weekly`. (3) Tail runs after the daily chain, before util; a tail failure does not
skip other tail jobs; any earlier failure skips the tail. (4) `silver_macro`, `silver_global_rates`: `run_on_weekdays [6]`. (5) `health_report`: no upstream,
`run_on_failure`. (6) Layer runs: Gatekeeper after a failed run and after a completed `gold` run; views after a completed `gold` run; `--no-gate`. (7)
`domain_gate_status` treats due tail members as `pending` on the evaluation day.
**Rationale.** The commands differed because two lists were maintained (`DAILY_SEQUENCE` for `all`; "prefix + daily" for layers) and four jobs had no declared
cadence. Deriving both from one list removes the class, and a test (all minus util == union of layers, per weekday) keeps it removed.
**Consequences.** See KNOWN_RISKS RISK-47.
**Rejected alternatives.** Put `silver_macro` / `silver_global_rates` in `DAILY_SEQUENCE` (a pinned test says weekly cadence; `silver_fred_daily` already exists for the
daily slice; ~13 extra minutes a day). Keep them unguarded and add a second "chain cadence" key (a second schedule vocabulary, and layer runs would still
differ from `all`). Keep the weekly Gold jobs as a prefix and add them to `all` (fits before that day's classify; the gap guard can then refuse and abort the
chain; weekly work still in front of daily output). Run the Gatekeeper after every layer run (records the layers not yet run as missing/block for a day that is
simply unfinished). Silence the Sunday block by changing the confirmation windows (GHR guide: do not).

## Verify
- Pristine baseline from a separate clone before any edit, dependencies pinned from `poetry.lock`: 3498 collected; the previous record is 3496 passed / 2 failed
  (`test_check_poetry_env`, no Poetry in the sandbox).
- WHOLE SUITE, one process, changed tree: 3582 collected / 3580 passed / 2 failed (the same two environmental `test_check_poetry_env`
  failures as the record), 312 s. That is the baseline record's 3498 / 3496 / 2 plus 84 new tests.
- Per-file runs (the sandbox restarted twice and killed a monolithic run, so the suite was run one file per process with a `.done` marker per file):
  baseline 3489 passed / 9 failed; changed tree 3573 passed / 9 failed (3582 collected, +84). The same nine fail on both trees: the two environmental ones and
  seven in `test_health_reporter_ghr02_adr059.py`, which fail ONLY in isolation (`conftest` chdirs at import, so the first `get_loader()` call finds no
  `config/`; with `test_instrument_loader.py` run first, all 72 pass). Pre-existing order dependence, not caused by this change. The only per-file diffs are
  the new module (+79) and +3 / +2 in the two gatekeeper / run_on_failure files.
- Mutants on a throw-away copy only (`python -B`, restored from the working tree after each): 30 of 30 killed — registry (guards, split clauses, chain order, schedule
  filter, layer filter, `run_on_failure`, `depends_on`), runner (tail isolation both directions, layer lists with/without `--force`, post-step conditions,
  swallowed failure, KeyboardInterrupt, CLI flag, views, masking, overwritten first failure), gatekeeper (pending classification, day offset, tail-only scope,
  caller flag, forwarding). The unmutated control was green. One gap found while designing the mutants: the guard clause of the split rule is invisible in the real
  registry (every weekly job is guard-only), so it got its own synthetic-registry tests (R4 killed by them).
- Not verified: anything on live data; a real Sunday tail behind the real daily chain; `register_views` behaviour after a layer run; the durations in the registry.

## Process notes
The first two whole-suite background runs died with the sandbox (uptime reset) — a known hazard I had already logged on 7 Nov; the resumable per-file runner fixed it.
A per-file run is not equivalent to a whole-suite run for tests with hidden order dependence; compare per-file results only against a per-file baseline.

## Not done
`pipeline_scheduler.py`'s hand-listed jobs; the positional coupling of `gold_global_regime`; ADR-051 A2 / ADR-055 text edits (outside the repo); a measured
Sunday duration.
