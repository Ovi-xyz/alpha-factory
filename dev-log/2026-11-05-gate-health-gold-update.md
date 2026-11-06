# 2026-11-05 — Gatekeeper, health report and Gold row integrity (guide GHR-01..14)

**Request.** Implement `gate_health_gold_update_implementation_guide.md` (proposed ADR-059) against `main`.
No version bump; repo stays at v1.24.0 with the Unreleased entries.

## Explore
Read `gatekeeper.py`, `health_reporter.py`, `pipeline_logger.py`, `runner.py`, `job_registry.py`,
`dependency_guard.py`, `quality_validator.py`, `technical_signals.py`, `active_symbols.py`, the ingesters and the
tests that touch them. The guide marked many details [U] because those files were unread; reading them changed
three of its conclusions (below).

## Decide
- **GHR-01** sandbox working directory (symlink mirror minus `data/`) instead of an env override.
- **GHR-04** only dependency-free, guard-only weekly jobs go ahead of the daily chain; plus `pending`.
- **GHR-09** pause markers hold a list of periods so resume keeps history.
- **GHR-14** the log, not old files, is the source of truth for the next day's diff; first resolution of a day wins.
- Deferred, with reasons: GHR-10, 11, 12, 13, GHR-14 D7, the freshness util step.

## Findings that corrected the guide
1. The unmodified suite wrote `pipeline_runs.db`, `progress.db` and 18 quarantine stubs into the live tree.
2. The one-row `series_id` stubs are written by three tests, not by ingesters; every ingester guards `len(df) > 0`.
3. `--job all` runs `DAILY_SEQUENCE` only; the weekly Gold prefix cannot be placed first (hard same-day Silver deps).

## Built
`src/utils/data_paths.py`, `pause_markers.py`, `job_result.py`, `quality_baselines.py`; `src/gold/gold_manifest.py`;
`src/silver/membership_log.py`; `config/quality_baselines.yaml`; `scripts/maintenance/clean_pipeline_runs_fixtures.py`;
changes in `gatekeeper.py`, `health_reporter.py`, `pipeline_logger.py`, `runner.py`, `job_registry.py`,
`quality_validator.py`, `schema_validator.py`, `technical_signals.py`, `active_symbols.py`, `base_ingester.py`,
`market_ingester.py`, `ohlcv_processor.py`, and the path constants of seven operational modules.

## Verified
Full suite on a sandbox clone of `main` (7a28d2e): 3128 -> 3333 collected, 3331 passed, 2 environmental
failures identical on pristine (`test_check_poetry_env`). CI gates `check_glob_scope.py` and `validate_instruments.py`
pass; no new pyflakes findings vs pristine. Mutation checks on every package; survivors were investigated, not waved
through (see lessons).

## Lessons (also in the mistake log)
- A mutant of DESTRUCTIVE code was executed against the real working tree and deleted the working clone (src, tests,
  .git ...) with all uncommitted work; rebuilt from the conversation. Mutants now run on a throwaway copy; local commit
  + tarball after every package.
- A broad `ruff --fix` over touched files removed pre-existing unused imports unrelated to the change; reverted. Lint
  fixes are limited to imports orphaned by one's own edits and to files one created.
- An absolute env override for test isolation was wrong for a suite built on `chdir(tmp_path)`; the measured failure
  (15 tests) is what led to the sandbox directory.
- A design bug found only because a "delete an old input" scenario was written (membership log).

## Not done
GHR-10 (needs XAE-R06), GHR-11 (needs live Bronze FX), GHR-12 (as-of VIX semantics for period-start-stamped bars),
GHR-13 (needs replay calibration), GHR-14 D7 + builder adoption, freshness util step, `JobResult` for the remaining jobs.

## Operator
See the CHANGELOG entry: run the suite, archive-then-clean the polluted logs with the pipeline stopped, mark any
deliberate pause BEFORE resuming, calibrate `config/quality_baselines.yaml` after XAE-R05 on live Silver.
