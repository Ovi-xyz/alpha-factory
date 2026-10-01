# 2026-09-29 — ADR-050 follow-up: resolving findings 1-5 before mirroring (v1.20.0)

## Trigger

"Resolve points 1-5 on findings first before moving toward the mirror
sequence." These were the five items flagged in the 2026-09-28 summary
(dev-log/2026-09-28-adr050-gatekeeper-pit-fred-foundations.md) — weekly-
cadence detection, detection lag, `--job all` never reaching `gatekeeper`
on a bad day, hysteresis calibration, and the CrossAssetEngine
history-loss limitation for ADR-053.

## Ground rule for this pass

Reason from live evidence, not from re-reading the ADR text a second time.
Pulled the live copies of the 4 files touched yesterday
(`pit_data.py`, `job_registry.py`, `quality_validator.py`,
`health_reporter.py`) via the Filesystem connector and diffed them against
the GitHub `main` clone — byte-identical, so the sandbox is still the right
baseline to keep working in. Then pulled the actual live sentinel directory
listing (`data/.sentinels`, 280 files) rather than reasoning about job
scheduling in the abstract.

## Point 1 — silver_macro / silver_global_rates guard

Read `macro_processor.py`'s own source instead of assuming. It reads
`data/bronze/macro/fred/**` — the same directory `bronze_fred_daily`
(daily cadence) writes into, not just `bronze_macro_weekly` (Sunday). Adding
a Sunday-only guard to `silver_macro` would silently starve ADR-051's
`raw_macro.py` of the daily FRED series it depends on. The live sentinel
history is consistent with this: `silver_macro` sentinels exist on
non-Sunday dates too, meaning it's run via `--job silver` daily in practice.

Decision: leave it unguarded. Gatekeeper's own staleness-window check
already handles this job correctly whether it runs daily or weekly — no
Gatekeeper change was needed for this specific job. What the investigation
DID surface — a job that's functionally daily but registered in
`WEEKLY_SEQUENCE`'s weekly-only prefix, invisible to `--job all` — is a
pre-existing `job_registry.py` orchestration question independent of this
ADR. Left open (RISK-35 update), not touched as a drive-by fix.

## Point 2 — detection lag (real bug, fixed)

Built a small reconstruction: recreated the 280 live sentinels as empty
files in a sandbox dir (grouped by job name from the directory listing),
then replayed `Gatekeeper._job_completed()` day-by-day. Measured, rather
than assumed, that a missed guarded weekly job (e.g. `bronze_macro_weekly`)
was first flagged at **day+14** under the original blanket 7-day window —
because the window still covered the PRIOR week's run on the due day
itself, so the miss only became visible once that covered run also aged
out.

Fix: `_due_exactly_on_guard_days()` classifies a job as "has exactly one due
day per cycle" if its only `run_on_*` key is `run_on_weekdays` — true for
`bronze_macro_weekly`, `bronze_bis_rates`, `bronze_eia`,
`gold_cross_asset_correlation`, `gold_lead_lag`, `gold_forecast`; false for
`silver_macro`/`silver_global_rates` (no guard) and any hypothetical
multi-day-window job (`run_on_day_of_month`, `run_on_nth_weekday`). Such
jobs get exact-date checking, evaluated only on the day they're actually
expected (`_expected_jobs_for_layer` already restricts to that day) — now
flagged at day+7, the day they're missed. Re-measured after the fix to
confirm.

## Point 3 — run_on_failure

Read `run_all()` and `run_job()` in full before changing anything. Added a
`run_on_failure` registry flag; after a first failure, `run_all()` skips
remaining jobs except flagged ones, then re-raises the ORIGINAL failure
(same exception/SystemExit) at the end — a later flagged job failing does
not mask it. Deliberately did not catch `BaseException` broadly — an
operator's `KeyboardInterrupt` must not trigger anything downstream. Only
`gatekeeper` carries the flag; pinned with a test so adding it elsewhere
stays a deliberate decision.

## Point 4 — hysteresis calibration

Wrote `scripts/replay_gatekeeper.py` (read-only, restores every module-level
value it patches even if `compute()` raises) so this replay is repeatable as
live history grows, rather than a one-off manual check. Ran it against the
280-sentinel reconstruction and a window sweep. Bronze holds `pass`
throughout; Silver correctly surfaces two real gaps. The sweep shows (2,4)
clearly calmer than (1,1) but statistically indistinguishable from
(3,4)/(2,5)/(3,5) on 21 days of data. Kept the values as shipped — there
isn't enough history yet to justify picking among the calmer options, and
the script exists precisely so that decision can be revisited later with
more data instead of guessed at now.

## Point 5 — CrossAssetEngine snapshot history

Read `gold/screener.py`'s actual join logic before designing anything: it
LEFT JOINs `lead_lag_tbl ON symbol = follower` and `forecast_tbl ON symbol`
against the whole snapshot file, no `computation_date` filter. Making the
snapshot files themselves retain history would have silently fanned out
every watchlist row once per retained week — ruled out immediately.

Built `src/gold/cross_asset/history.py` instead: additive, one dated file
per run written alongside the (unchanged) snapshot file, in each of the
three modules' `run()`. Rewrote the three `PITDataLoader` methods to read
the history glob and select exactly one snapshot (`MAX(computation_date)
<= trade_date`), never several weeks concatenated. Demonstrated the
regression a naive `computation_date <= trade_date` predicate would produce
once history exists (6 rows / 3 weeks vs. 2 rows / 1 week on a 3-week
fixture) before locking in the corrected query.

Explicitly still open: history only starts accruing from the first weekly
run after this ships (not a backfill — the modules need a resolved active-
symbol universe per date, which only exists from 5 Sep onward). The
`cross_asset_xgboost` (ADR-053) training-data gap this creates is
unresolved by this update.

## Bugs caught by tests, again

- First draft of the PIT rewrite used an f-string with an embedded
  parameterized fragment for the "latest snapshot" subquery —
  `test_fstring_sql_absence` failed immediately. Rewrote as two fully
  static query branches (symbol given / not given) per method, no string
  assembly at all.
- The new history-hook tests, and the pre-existing correlation/lead-lag/
  forecast module tests, both started leaking real files into
  `data/gold/cross_asset/history/` because those tests only redirected each
  module's OWN snapshot path, not the new history root. Fixed with an
  autouse fixture in `tests/conftest.py` (which also isolates Gatekeeper's
  `QUARANTINE_GLOB` from unrelated tests' quarantine files, caught the same
  way).

## Verification

- Confirmed the fix for each point by reverting just that change and
  re-running its tests: runner (3 failed), old blanket window (3 failed),
  missing history hooks (4 failed).
- Full suite: 1798 -> 1952 passed (+154 since the original 1798 baseline;
  +51 in this pass specifically), 0 regressions.
- `ast.parse` clean on all 20 touched `.py` files this ADR has touched
  in total. No f-string SQL.
- Compared file-leak behavior against a pristine HEAD checkout in a git
  worktree: every leaked file under `data/` after a full suite run
  (`correlation_clusters.parquet`, `hmm_regime_model.pkl`, `progress.db`,
  several `data/quarantine/*` files) is identical to what pristine HEAD
  already leaks — pre-existing, not introduced by this work, not touched.

## Not done

- Still not run against live Silver/sentinel data on the M1, still not
  mirrored to the live repo.
- ADR-051/053/054 untouched. The ADR-053 training-data gap (point 5) is
  documented, not fixed — out of this ADR's scope.
