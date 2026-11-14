# 2026-11-12 — VIX Silver after the rebuild: mixed Bronze zones, invisible read failures (FIX SIL-BRZ-SCHEMA-02, RISK-48)

**Request.** Ovi: examine the Silver and Gold run logs of 12 Nov 2026 (after the rebuild from Bronze), then create an update patch from the findings; the daily
`--job all` was still running, so nothing was written to the live tree. Built against GitHub `main` (`fae5154`); the files read were byte-identical to the live repo.

## Explore
Read: `ohlcv_processor.py`, `quality_validator.py`, `technical_signals.py`, `instruments_identity.yaml`, `instruments_taxonomy.yaml`, `job_result.py`, `runner.py` (run_job),
`progress_checkpoint.py`, the audit script and its tests, 13 of the 71 Bronze VIX 1D files. Reproduced the VIX failure with the real files before writing any fix.

## Findings (measured unless marked)
| # | Finding |
|---|---|
| 1 | VIX 1D/1W/1M have no Silver (3 of 174 context reads failed); cause: two tz-aware zones in one group, which the reconcile left alone on purpose |
| 2 | All 13 sampled Bronze VIX files are Chicago; `_tz_hint` says New York; the New-York-labelled file(s) are among the 58 unread ones |
| 3 | Failed per-symbol reads reach no gate: job `SUCCESS`, health report 0 failed, Silver raw `pass`; coverage 57/58 = 98.3% passes the 95% floor; `failed_report` has no consumer outside `pipeline_dashboard` |
| 4 | "0 rows flagged is_clean=False" is expected after a rewrite: `_flag_is_clean` already applies |z| > 4 at write time (9,123 bars across 594 symbols); XAE-R06 is not in the tree |
| 5 | HMM lineage after the rebuild: in-sample counts equal the pre-rebuild ones, 19/19/19 out-of-sample rows, fingerprints match, audit PASS (from the log) |
| 6 | `technical_xgboost_predict` and `hmm_xgboost_predict` report `SUCCESS` with "no fitted model — nothing written" (design); the first fits need a fresh label file |

## Decisions
- **Reader rule by local calendar date, not by instant**, because the meaning of the odd stamps is unknown and a 1D/1W/1M bar carries a trade date; guarded by a six-hour
  local-time limit so a different shape raises. Majority zone wins so Silver stamps stay what the uniform path writes. Rejected: `convert_time_zone` to the instrument zone
  (wrong if the label is a wall-clock relabel: NY midnight becomes 23:00 the previous day in Chicago); a per-instrument zone pin (changes stamps of every bar).
- **Visibility through the existing `JobResult.warnings` channel** (no new contract) plus the forecast-anchor clause in the existing WARNING-tier check (no new check, so the
  15-check pin and `quality_gate_log` shape are untouched). Not done: Gatekeeper or health-report consumption of failed checkpoints (a design call).

## Mistake recorded
My first read of the log said the 11 Nov Bronze VIX file was the odd one out and "the 11 Nov ingest changed its zone". Reading it showed it is Chicago like the other 12 sampled
files. A concat error plan names an input at the failure point, not the file whose dtype diverges. Logged in the project mistake log.

## Verification
Pristine baseline 3582 collected. After 3611 collected (+29), 3579 passed, 32 failed (the identical 32 fail on the pristine clone: packages missing from the sandbox venv, e.g. `fredapi`; the live baseline has 2). New classes: `TestBronzeCoarseZoneDrift` (16), `TestFailedReadVisibility` (4),
`TestContextForecastAnchorCoverage` (6), three audit-script tests. Against the old source 22 of the 29 new tests fail (13 of 16, 4 of 4, 2 of 6, 3 of 3); the other 7 pin behaviour that must not change. Mutants 11/11, 9/9, 3/4
(equivalent survivor: `len(set(aware)) < 1`). Every touched file passed `ast.parse`.

## Not done / follow-ups
- Locate the New-York-labelled Bronze file(s): run `python scripts/audit_bronze_schema_drift.py --market context`, or read the reader's WARNING after the first run.
- Which 6 "genuinely missing session days" the context gap check reports; which 2 symbols are stale in the screener (594/596).
- `global_regime` is a one-row store after the rebuild: `days_in_state` and `flicker_10d` are cold-start values for about 10 days.
- First specialist fits: label file (`label_triple_barrier.py --publish`), `gold_hmm_walk_forward`, then the two fit jobs.
- Gatekeeper / health report: failed-checkpoint consumption, the three domain scopes in the report line, baseline-aware severity (GHR-06).
