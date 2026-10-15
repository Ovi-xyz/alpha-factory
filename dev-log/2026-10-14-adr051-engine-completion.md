# 2026-10-14 — ADR-051 completion: shared engine, market branch, context join (v1.21.0)

## Trigger

"Continue with the dual HMM regime engine implementation", with the previous
session's summary as context (Phases A–C built in a sandbox, mirror to live
unfinished, Phases D–E and the market weights open).

## What the ground actually looked like (explore, before any code)

- **The sandbox from the previous session no longer existed** (fresh container).
  Rebuilt from GitHub `main` (v1.20.0, baseline 1952) plus an overlay of the
  uncommitted HMM files copied from live (`src/gold/hmm/*`, `fred_series.yaml`,
  `job_registry.py`, 5 test files), plus the `hmm_regime.py` archive step.
- **Live was further along than the summary said** (source, yaml and registry for
  Phases A–C present) **but its HMM tests were stale**: 4 test files still asserted
  the pre-refinement contract, 98 failures + 1 collection error against the
  refined source. `hmm_macro_regime` had no tests on live at all. Live therefore
  could not have been run green on the M1 — consistent with the "don't run the
  tests until the mirror is done" warning.
- Decision: restore a green base for A–C **before** building D–E on it.

## Decisions

1. **Checkpoint mirror first.** The refreshed A–B tests (6 files) were mirrored
   and byte-verified before any new work, because the last session lost its
   sandbox mid-mirror.
2. **Shared engine (`hmm_core.py`) rather than a second copy in the market module.**
   Scaler / fit / forward filter / output columns / persistence helpers were
   *moved verbatim* out of `hmm_macro_regime.py` (generated from its own text,
   not retyped); `fit_model()` and `append_store()` extracted so the market branch
   does not duplicate ~80 lines. Gate: a golden master — the original macro
   module's fit on a synthetic 900-row panel (5 model JSONs + store) captured
   first, compared byte-for-byte after the move. `hmm_macro_regime` re-exports all
   moved names. Rejected: importing the macro module from the market module
   (couples the branches), and copying the code (the GAP-1 drift class).
3. **Market model settings in `hmm_market:`** — per-feature signed `weight`,
   `labels`, `fit`. `_validate_fit()` shared with macro. Weights first-pass:
   VIX / dispersion / RMS(median return) negative; share-up / median return
   positive; FX and yield changes 0.0 (sign flips with the macro regime). Model
   fingerprint covers routing, weights, labels, fit, breadth definition, PIT basis.
4. **Market store columns** include `n_features_observed` so rows scored with the
   breadth guard's nulls are visible; the store stays append-only (RISK-36 item 5).
5. **`context_regime`**: macro spine, market attached as-of (backward) within 5
   calendar days, explicit `market_age_days`, `in_sample` = OR of the sides used,
   thin by design, rebuilt each run, nothing written when either input is absent.
   Verified against the real `PITDataLoader.get_context_regime`.
6. **Not built:** the per-domain Gatekeeper scope (RISK-36 item 7), and the
   ADR-055 projection / `compare_regimes.py` / `hmm_legacy_mapping.yaml` (those
   were not on live, and ADR-055 D1–D3 are still unconfirmed).

## Findings during implementation

- Lint (`pyflakes`) caught my own regression from the extraction: `import math`
  removed from the macro module while still used.
- Writing the macro tests exposed a real robustness gap: a stored macro store
  missing `macro_label` died with a polars `ColumnNotFoundError` before the
  schema guard could explain it. Fixed with an explicit precheck.
- Mutation pass: 24 of 25 mutants killed on the first run; the survivor was the
  `n == short_history_min_obs` boundary (`<` vs `<=`). Boundary test added.
- Two of my own test expectations were wrong (a date-arithmetic slip; the carried
  row at the clamp boundary) — in both cases the code was right.
- The 3 failing tests (2× `test_check_poetry_env`, 1× `...Utf8Fallback`) were
  confirmed to fail identically on a pristine clone of `main` in this container.

## Numbers

1952 → 2420 collected; 2417 passed; 3 environmental failures. Mutation 25/25.
`COUNT_BASELINE.txt` 2149 → 2420 (live holds no test file that the sandbox lacks;
the mirror adds exactly five).

## First live run (M1) — nothing below has been executed yet

```
python runner.py --job gold_raw_macro
python runner.py --job gold_raw_market
python runner.py --job gold_hmm_macro_fit  --force
python runner.py --job gold_hmm_market_fit --force
python runner.py --job gold_hmm_macro_regime
python runner.py --job gold_hmm_market_regime
python runner.py --job gold_context_regime
python -m pytest tests/ -q          # expect 2420 collected, 3 environmental failures at most
```

Then ADR-055 E7: review the market weights and the macro label against known
macro dates before anything consumes `context_regime`.
