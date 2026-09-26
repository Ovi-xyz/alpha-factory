# 2026-09-25 — GMI-JR-004: Gold-layer CrossAssetEngine schedule guard (RISK-34)

## Trigger

Ovi ran `python src/runner.py --job gold` on the live M1 and reported the
result plainly: "returned with no warning or error," then asked for a
check on it.

## Empirical-first check, before any code was touched

- GitHub mirror of the repo pulled into a sandbox (`codeload.github.com`
  tarball — `api.github.com` was rate-limited without auth). Confirmed
  **byte-identical** to the live `src/scheduler/job_registry.py` via
  `copy_file_user_to_claude` + `diff` before trusting the sandbox for
  anything, per this project's own sandbox-workflow discipline.
- Read `src/runner.py` and `src/scheduler/job_registry.py` in full.
  `--job gold` is GMI-JR-003's `run_layer("gold", ...)`, which resolves
  to `layer_sequence("gold")`:
  `["gold_cross_asset_correlation", "gold_lead_lag", "gold_forecast",
  "gold_global_regime", "gold_signals", "gold_mtf", "signal_aggregation",
  "gold_regime", "gold_sector", "gold_screener"]` — derived from
  `WEEKLY_SEQUENCE`'s ordering, which lists the three CrossAssetEngine
  jobs ahead of the daily chain.
- Checked all three CrossAssetEngine module `run()` functions
  (`correlation_module.py`, `lead_lag_module.py`, `forecast_module.py`)
  for any internal weekday guard. None exists — cadence is
  docstring/SOP-only, same shape as RISK-23's root cause.
- Checked `KNOWN_RISKS.md`/`CHANGELOG.md`/`dev-log/` for any prior
  mention of this specific gap. Not found — RISK-23 (31 Aug) fixed the
  identical pattern for `bronze_macro_weekly`/`bronze_bis_rates`, but
  predates GMI Wave 1 Cycle 4 (12-13 Sep, v1.18.0), which is what added
  these three Gold jobs to `WEEKLY_SEQUENCE` in the first place — the
  fix was never extended.
- Went to the **live machine** via the Filesystem MCP connector
  (`/Users/opi/alpha-factory`, read-only tools first, then confirmed
  write tools were also available this session) and pulled real evidence
  rather than reasoning from source alone:
  - `data/.sentinels/` listing: `gold_cross_asset_correlation` had fired
    on Wed(16), Thu(17), Mon(21), Tue(22), and now Fri(25) Sep 2026 —
    never once on an actual Sunday (20 Sep). `gold_forecast` had also
    fired on Sat(19)/Sun(20), i.e. genuinely no weekday pattern at all.
  - Timestamps on today's sentinels: `gold_cross_asset_correlation`
    08:58:38 → `gold_screener` 08:59:18 WIB — the entire 10-job gold
    layer completed in ~40 seconds.
  - Checked this wasn't a silent skip: `data/gold/cross_asset/*.parquet`
    and `data/gold/signals/tech_signals_*.parquet` (353 MB across 5
    files) were all freshly rewritten in that exact window, with
    plausible real sizes. Concluded the run was genuine, fast Polars/
    DuckDB compute on this dataset size, not a checkpoint-skip artifact
    — the `est_minutes` figures in `JOB_REGISTRY` are old, conservative
    placeholders, not current runtime.
  - This also appears to be the **first full live run of
    `signal_aggregation`** through the complete gold chain — every
    daily-chain gold job's sentinel (`gold_signals`, `gold_mtf`,
    `signal_aggregation`, `gold_regime`, `gold_sector`, `gold_screener`)
    only had a 2026-09-25 date, no earlier one — closing exactly the
    "Suggested next step" RISK-33 (opened 24 Sep) called for.

Reported back to Ovi: the run itself was genuine and correct; the one
real, unaddressed gap was the missing schedule guard on the three
CrossAssetEngine jobs. Ovi confirmed: "continue to fix the unaddressed
gap."

## Fix

`run_on_weekdays: [6]` added to `gold_cross_asset_correlation`,
`gold_lead_lag`, `gold_forecast` in `JOB_REGISTRY` — identical fix shape
to RISK-23. `gold_global_regime` deliberately left unguarded (it's
genuinely daily per Architecture v2.0 §6.5).

Checked whether a `stale_tolerance` ripple was needed anywhere (RISK-23's
own consequential fix): no `DAILY_SEQUENCE` job hard-depends on any of
the three — `gold_screener`'s dependency on CrossAssetEngine output is
deliberately soft/absent (RISK-31's own design decision) — so none was
needed. Added a regression test confirming this explicitly rather than
just asserting it in prose.

## Regression found and fixed in the same pass

`tests/integration/test_runner_weekly_cadence.py::TestLayerCommands::
test_bronze_then_silver_then_gold_completes_full_chain` asserted every
job in `LAYER_JOB_NAMES["gold"]` completes on a Wednesday `run_date` —
true before this fix (nothing gated them), false after (correctly so).
Updated the test's `weekly_only` skip-set to include the three newly-
gated jobs, mirroring exactly how `bronze_macro_weekly`/`bronze_bis_rates`
were already excluded in the same test for the same reason.

## Verification

- `tests/unit/test_check_poetry_env.py`'s 2 failures are pre-existing,
  environment-only (poetry binary absent from this sandbox) — present
  identically before and after this change.
- Pre-fix baseline confirmed clean: 1787 passed, 2 pre-existing failures,
  1789 collected — matches `tests/COUNT_BASELINE.txt` exactly before any
  edit.
- New test class `TestGoldCrossAssetScheduleGuard` (9 tests) confirmed to
  **fail** against the pre-fix source (swapped in the pristine live copy
  fetched via `copy_file_user_to_claude` before any edit, ran just the
  new class, 6/9 failed reproducing the live Friday behavior exactly),
  then confirmed to **pass** (9/9) after restoring the fix.
- Full suite post-fix: 1796 passed, 2 pre-existing failures, 1798
  collected. 0 regressions beyond the one caught and fixed above.
- `ast.parse` clean on all 3 modified files.
- No f-string SQL introduced.
- `tests/COUNT_BASELINE.txt` updated 1789 → 1798.
- `pyproject.toml` bumped 1.19.0 → 1.19.1 (PATCH — bug fix to existing
  scheduling behavior, no Interface Contract or schema change).

## Files changed

- `src/scheduler/job_registry.py` — the fix (3 dict entries).
- `tests/integration/test_job_registry_integrity.py` — new
  `TestGoldCrossAssetScheduleGuard` class (9 tests).
- `tests/integration/test_runner_weekly_cadence.py` — consequential test
  update (expanded `weekly_only` skip-set + docstring note).
- `tests/COUNT_BASELINE.txt`, `CHANGELOG.md`, `KNOWN_RISKS.md`,
  `pyproject.toml` — documentation/version bookkeeping.

## Not done / explicitly out of scope this pass

- Did not touch the `est_minutes` figures in `JOB_REGISTRY` even though
  live evidence suggests they're stale over-estimates relative to actual
  M1/Polars runtime on the current dataset size — not this session's
  scope, and re-benchmarking them properly needs more than one sample
  run.
- Did not re-run `--job gold` on the live machine after mirroring (no
  execution access on Ovi's M1 from this chat session, per this
  project's standing constraint — Filesystem MCP is read/write, not
  execute).
