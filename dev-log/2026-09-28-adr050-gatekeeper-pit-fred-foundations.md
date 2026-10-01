# 2026-09-28 — ADR-050: Gatekeeper, PITDataLoader extension, FRED `contributes_to` (v1.20.0)

## Trigger

"Continue with the implementation of adr-050-gatekeeper-pit-fred-foundations."
ADR-050 is PROPOSED, `Depends on: none`, and blocks ADR-051/053/054, so it
is the correct first build in the GMI intelligence series.

## Empirical-first pass (before any code)

- Cloned GitHub `main` (depth 1) into the sandbox. Live repo state was ahead
  of what project memory recorded (v1.19.1 / baseline 1798, not v1.18.0 /
  1713) — read `pyproject.toml` and `tests/COUNT_BASELINE.txt` rather than
  trusting recalled counts.
- `pip install -e .`, collected 1798 tests (matches baseline exactly), ran
  the full suite: 2 failures, both `test_check_poetry_env.py` (no `poetry`
  binary in the sandbox). Installed poetry → 1798 passed, 0 failed. Baseline
  clean before touching anything.
- Read in full: `quality_validator.py`, `pit_data.py`, `dependency_guard.py`,
  `pipeline_logger.py`, `health_reporter.py`, `job_registry.py`,
  `atomic_io.py`, `config/fred_series.yaml`, `instruments_taxonomy.yaml`'s
  `contributes_to` usage, and the producer modules for every new PIT read
  (`global_index_regime`, `correlation_module`, `lead_lag_module`,
  `forecast_module`, `signal_aggregation`) to get real column names.

## Findings that shaped the implementation

1. **`silver_macro` / `silver_global_rates` have no `run_on_weekdays`.** A
   naive "expected jobs = `LAYER_JOB_NAMES` filtered by `_passes_schedule`"
   would expect them every day and make Gatekeeper flag Silver as unhealthy
   6 days out of 7. Confirmed empirically (they appear in the expected list
   on both a Sunday and a Wednesday). Fix: jobs outside `DAILY_SEQUENCE`
   use `DependencyGuard.is_done_within(..., 7)` — the same convention as
   FIX NEW-1 — derived from sequence membership, not a hardcoded job list.
2. **`run_all()` aborts on the first failed job**, so the `--job all` path
   cannot reach `gatekeeper` on a failed day. Chose `depends_on: []` so it
   is manually runnable on a bad day, and the 14-day lookback recomputes
   missed days from sentinels. Recorded in RISK-35 rather than hidden.
3. **Three CrossAssetEngine outputs are overwritten weekly, no history**
   (`cross_asset_corr`, `lead_lag_matrix`, `cross_asset_forecast`), unlike
   `global_regime.parquet`. The new PIT methods are correct but only ever
   see one snapshot. This affects ADR-053's `cross_asset_xgboost` training —
   flagged in RISK-35, not resolved here (not ADR-050's scope).
4. **`fred_series.yaml` has no per-domain grouping object** (domain is an
   inline string per series), so "mirror `_meta.contributes_to`" was done
   as a new top-level `domains:` block keyed by the existing `domain:`
   vocabulary. Left empty on purpose — `instruments_taxonomy.yaml` already
   reversed untraceable weights once (FIX ADR-019).

## Design decisions (all explicit, first-pass)

- Gatekeeper raw status per layer: Bronze = expected-job completion
  (+ quarantine-today → warn); Silver = job completion + persisted check
  results, cascading Bronze; Gold = job completion, cascading Silver.
  Combined via worse-of (`block > warn > pass`).
- Hysteresis windows (2 / 4 days) read from `hmm-gatekeeper-xgboost.md`'s
  `t(0,1)` / `t(0,1,2,3)`; the note's two axes collapsed into one — noted in
  the module docstring and RISK-35.
- `get_gate_status` defaults to `scope="gold"` because of the cascade.
- `get_context_regime` returns `None` naturally (path doesn't exist yet) —
  no special-casing.
- Persistence in both `quality_validator` and `gatekeeper` swallows write
  failures (logged) so a logging side-effect can never fail the gate.

## Bug caught by the new tests

`_quarantine_today()` first used `Path(".").glob(QUARANTINE_GLOB)`, which
raises `NotImplementedError: Non-relative patterns are unsupported` when the
pattern is absolute (as in a tmp_path-isolated test). Replaced with
`glob.glob()`. 38/39 passed on the first run; the one failure was this.

## Verification

- 103 new tests across 5 files; `test_pit_data.py` is the first dedicated
  test file for that module.
- New tests confirmed to fail against the stashed pre-change source
  (persistence file fails at import; PIT 25/25 fail; wiring 7 failed + 7
  errors; FSR 6 of 9 fail — the 3 passing are intentional existing-invariant
  guards) and pass against the fix.
- `ast.parse` clean on all new/modified files; no f-string SQL
  (`test_fstring_sql_absence` still green).
- Full suite: 1798 → 1901 passed, 0 regressions.
- No stray `gate_status.parquet` / `quality_gate_log.parquet` in the real
  `data/health/` after test runs.

## Not done

- Not run against live Silver/sentinel data on the M1.
- Not mirrored to the live repo, not byte-verified (sandbox clone came from
  GitHub `main`, not the live copy).
- ADR-051/053/054 untouched.
