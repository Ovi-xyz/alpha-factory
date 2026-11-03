# 2026-11-02 — Cross-asset recalibration (XAE-R01/R02/R04/R05/R07/R08/R09A/R10), no version bump

## Request
Implement `cross_asset_recalibration_guide.md` (PROPOSED ADR-056/057) against `main` 2e2126a (v1.24.0 + the
Unreleased bridge-archive entry).

## Explore
Cloned `main`, pinned the environment to `poetry.lock` (duckdb 1.5.4, polars 1.42.1, numpy 2.4.6, pandas 3.0.3 —
an unpinned install gives duckdb 2.x and breaks `.pl()`), confirmed baseline 2881 collected / 2866 passed + 15
deselected (`test_check_poetry_env.py`, needs the `poetry` binary). Read the live source of regime_tag,
global_index_regime, lead_lag_module, forecast_module, correlation_module, quality_validator, gap_analysis and the
screener/PIT consumers. Guide statements that could be re-verified against source were (Q1: the context outlier
query lacks `isfinite` and its `except` returns True at DEBUG — confirmed; the screener 0.15 filter — it does not
exist in code, only in the ADR text).

## Decide (guide recommendations adopted; none was contradicted by source)
D2 oracle first, peers as fallback; D4 label forecast now, replace later; D5 defer the W4 trigger; D6 calibrated p
with q unchanged at 0.03; D7 not an ADR task. **D3 (Silver flag split) and D1 (information-set lags) not taken:**
R06 changes `is_clean` for Layer 1 consumers (technical_signals, active_symbols, screener) whose US universe the
guide itself calls unmeasured, and needs a live reprocess — not something to ship unmeasured.

## Findings made while building (not in the guide)
1. `screener.py` read `global_regime` with `date == run_date`; one row per bar date would have nulled the global
   columns on every weekend run. Fixed (as-of) and pinned.
2. DuckDB's `TRY()` does not guard a missing column (binder error) — the PIT tie-break uses a schema probe selecting
   between two static SQL literals.
3. `winsorise_columns`: `to_numpy()` turns a polars null into NaN, which `drop_nulls()` does not drop, so Granger got
   NaN and the whole pair vanished. Caught by a reporting test; fixed with `nan_to_null` and pinned.
4. Calibration math: an empirical null cannot clear BH (floor 1/(N+1)); a first tail extrapolation was both anchored on
   the noisiest point and clamped the slope in the anti-conservative direction. Fixed; slope cap mutation-checked.
5. A bug in my own gap-check wiring (`_last_gap_classification` is None when `_classified_gap_counts` is stubbed) was
   swallowed by the check's DEBUG `except` — the same failure class as QV-OUT-03. That `except` is now loud.
6. The guide's cost estimate for the placebo is optimistic ~5x (13 ms per Granger run measured, sandbox).
7. `null_corr_quantile`: the guide's p95 0.378 corresponds to n of about 46-50, not 44 (0.40); checked against the
   analytic Sidak bound.

## Tests
Added/extended: test_global_index_regime (+bar-date keying, debounce, region weighting, legacy rows),
test_cross_asset_regime_tag (+staleness, parity), test_screener (+as-of global regime, forecast has_signal,
actionable), test_pit_data, test_quality_validator (+QV-OUT-03), test_calendar_oracle (new), test_cross_asset_robust
(new, simulation-validated), test_lead_lag_module (+R08), test_forecast_module (+R09A), test_correlation_module
(+contract), tests/integration/test_cross_asset_recalibration_adr056.py (new structural pins). Two existing tests
were changed because they encoded the OLD semantics on purpose: the 9/14 RISK_ON count (now region-weighted) and the
undebounced transition test (now runs at DEBOUNCE_DAYS=1; debounce has its own class). A forecast-test line
asserting the vacuous `stable is True` for lag-0 now asserts NULL.

## Live (NOT done — operator)
1. Back up `data/gold/cross_asset/global_regime.parquet`, `poetry lock` if needed, install `exchange-calendars`.
2. `gold_global_regime --force`; confirm one row per bar date, `as_of_bar_date` populated, label debounced.
3. Run `silver_validate`; read `context_gap_detection` detail (closures by calendar / peers / session days with no
   bar) and the count of isolated gaps vs the old 72.
4. Weekly jobs (`gold_cross_asset_correlation`, `gold_lead_lag`, `gold_forecast`) — R02: `regime` tag must equal the
   engine's own label (or `UNKNOWN` if stale); time `gold_lead_lag` on the M1.
5. `gold_screener --force`: check `global_*` columns on a weekend run date, `forecast_has_signal`, `lead_lag_signal`.
6. Then, before trusting `actionable`: R12 (estimator study on US followers) and the pre-registered pairs report.
