# archive/hmm_regime_orphan_2026_09/

ADR-051 (GMI-HMM2-001), Consequences — archived, not deleted.

## What's here

- `src/gold/hmm_regime.py` — the single flat 5-state `HMMRegimeDetector`
  (GD §8.2 Phase 2 sketch). Fully coded, **never wired into `job_registry.py`**.
- `tests/unit/test_hmm_regime.py` — its dedicated test file.

Paths mirror their original location under the repo root.

## Why it was retired

1. Orphaned: no job ever called it; nothing imported it.
2. Its `_load_features()` built one row per date by *intersecting* the dates on
   which VIX (daily), CPI (monthly) and GDP (quarterly) all have a value, with
   no forward fill. Measured on live Silver, the intersection of the five
   observation-date sets it needs is 77 dates — above its own `< 50` guard, so
   the guard passes and the model would have trained on 77 quarter-start points.
3. `classify()` scored one observation at a time, so the fitted transition
   matrix was never used.
4. A single flat HMM mixes the macro *environment* with market *behaviour*.

Successor: `src/gold/hmm/` (ADR-051) — cadence-native, forward-filled,
point-in-time-gated panels; separate macro and market branches; forward-filter
inference that actually uses the transition matrix.

`macro_regime.py` is NOT archived here: it is a live production module feeding
`regime_store.parquet` (Trading Engine interface). Its retirement is a separate
cutover decision (ADR-055, Option A: archive after burn-in).

Same convention as `archive/gold_correlation_retirement_2026_09/`.
