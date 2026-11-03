# 2026-11-03 — HMM regime engine update (HMM-R01/R02/R03/R06/R11), no version bump

## Request
Implement `hmm_update_implementation_guide.md` (PROPOSED ADR-058) against `main` e79ce67 (v1.24.0 + the
Unreleased cross-asset recalibration entry).

## Explore
Cloned `main`, pinned the environment to `poetry.lock` (118 packages, `--no-deps`), baseline from a PRISTINE second
clone: 3053 collected / 3038 passed + 15 deselected (`test_check_poetry_env.py`, needs the `poetry` binary). The guide
states that no job source was read, so every [D]/[U] statement about job behaviour was re-checked in source:
hmm_core, hmm_market_regime, hmm_macro_regime, raw_macro, raw_market, fred_pit, context_regime, walk_forward,
features_hmm, macro_processor, runner, job_registry and the existing HMM tests.

## Decide
Taken as recommended: R01 gap guard, R02 provenance columns + consumer rule, R03 before the first as-operated run
(D2), R06 harness, R11 audit. Not taken, with reason: R04 (the guide defers it; it overlaps GHR-09's pause marker),
R05/R07/R08/R10/R12 (need walk-forward / OOF stores, `validation.json`, live weekly fits — none available), R09
(conditional on R05). Open decisions D1, D3-D6 stay with the owner.

## Findings made while building (several correct the guide's [U] items)
1. **One classify call backfills the whole gap** (answers R01's open question): `append_new_rows` writes every panel row
   after the last stored date with the persisted model; `in_sample = date <= train_end`, so the gap is out-of-sample.
   The hazard is only the reverse order: `run_fit` fits on the whole panel FIRST and fills history after.
2. **`--force` never reaches a job.** `run_job` calls `fn(run_date)`. The guide's "refuse unless --force" cannot be
   built as written and would be wrong if it were: `--force` is how a fit is routinely run off its Sunday slot. The
   override is `HMM_ALLOW_REGIME_GAP=1`.
3. **`append_store` refused every schema difference** (ADR-051, and two existing tests pin it). R02's "additive,
   legacy rows NULL" needed a caller-declared exception (`additive=`). Undeclared columns still raise.
4. **A same-day second FRED run erased `is_revision`.** `_find_latest_silver` picked the run's own earlier snapshot as
   "the previous vintage". Harmless while `silver_macro` was weekly; with a daily slice plus the Sunday run it is
   routine. Fixed (strictly-earlier snapshots only).
5. **The `hmm` gate scope did not include `bronze_fred_daily` / `bronze_treasury`**: its upstream closure held only
   the weekly macro Bronze job, so a stalled daily FRED/Treasury fetch could not show in that scope. It does now.
6. `raw_market` filters `is_clean = TRUE` for the US breadth features (R11 answered: yes). Not changed.
7. The `PROCESSING_VERSION` bump cannot trigger a walk-forward/OOF rebuild: those key on their own constants.
8. `pit_data.get_context_regime` and the screener's `regime_in_sample` docs told consumers to "filter on in_sample" —
   misleading while the macro side has no out-of-sample row. Reworded.
9. `scripts/studies/` did not exist (the cross-asset guide's R12 also expects it); created.

## Built
`hmm_core` (`append_store(additive=)`, `unscored_gap`, `enforce_gap_guard`), both regime modules (`model_train_end`,
`<d>_age_days`, `gap_dates`, `*GapError`, guard in `run_fit`, version 1.1), `macro_processor` (`run_fred`, dated
`_find_latest_silver`), `job_registry` (`silver_fred_daily`, `gold_raw_*` deps, sequence), two doc fixes, two offline
utilities (`scripts/studies/hmm_replay.py`, `hmm_catchup_audit.py`), 75 new tests, 4 updated pins.

## Verification
Full suite on a snapshot of the final tree: 3128 collected, 3113 passed, 15 deselected (baseline 3053 / 3038 + 15;
+75 new tests). Mutation checks: 17 mutants on the new guards, all killed. Two survivors on the first pass were weak tests, not weak
code: the macro fixture gave every domain the same training end (`min` == `max`), and the audit had no boundary row
where `model_train_end == date`. One tooling mistake: a same-length mutant (`max` -> `min`) restored within the same
second left a stale `.pyc`, so the correct source ran as the mutant; mutants were re-run with `python -B`.
Not verified: anything on live data (see RISK-42 items 9-10).

## Operator runbook (first run after the pause; explicit job names, not `--job all`)
```
cp -r data/gold/hmm data/archive/hmm_pre_adr058_2026_11        # the first write widens both regime stores
python -m pytest tests/ -q                                      # expect the CHANGELOG count
python src/runner.py --job silver_ohlcv
python src/runner.py --job silver_fred_daily                    # new; needs today's bronze_fred_daily + bronze_treasury
python src/runner.py --job silver_validate
python src/runner.py --job gold_raw_macro
python src/runner.py --job gold_raw_market
python src/runner.py --job gold_hmm_macro_regime                # scores 19-30 Oct in one call, in_sample=False
python src/runner.py --job gold_hmm_market_regime               # expect WARNING "widened additively"
python src/runner.py --job gold_context_regime
python scripts/studies/hmm_catchup_audit.py --since 2026-10-16 --expect-context-rows 10
python src/runner.py --job gold_hmm_macro_fit --force           # refused if the gap is not closed — do not override
python src/runner.py --job gold_hmm_market_fit --force
python src/runner.py --status
```
Expected audit: A1 = 0 on both stores; `unverifiable_legacy_oos` market = 1 (the 2026-10-16 row, written before
`model_train_end` existed); A3 = 10. A refused fit means a classify job did not score everything: re-run it, do not
set `HMM_ALLOW_REGIME_GAP`. Rollback: restore `data/gold/hmm/` from the copy; registry and code revert by git.

## Mistakes made in this pass (also in the project memory log)
1. Launched the baseline run as a background job and polled an empty log without checking it was alive.
2. Edited the tree the baseline was running from, then failed to restart it from a pristine clone (a malformed tool
   call that ran nothing); the baseline was finally taken from a separate clone started before any edit.
3. The stale-`.pyc` mutation-harness artifact above.
