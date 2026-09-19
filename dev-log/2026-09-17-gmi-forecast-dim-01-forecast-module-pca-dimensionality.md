# 2026-09-17 — FIX GMI-FORECAST-DIM-01: ForecastModule PCA Dimensionality vs. Observation Count

**Version**: 1.18.1 → 1.18.2
**Trigger**: Ovi ran `gold_forecast` live on the Mac immediately after FIX
XAE-CAL-01 landed (log pasted as
`2026-09-17-cross-asset-engine-running-log.txt`, alongside
`gold_global_regime`, `gold_cross_asset_correlation`, `gold_lead_lag` from
the same session) and reported two things explicitly: `regime=UNKNOWN` on
three of the four jobs, and `gold_forecast` logging a VAR fit failure
message for every single equity before ending in `SUCCESS` with nothing
written. Asked to "evaluate the current cross-asset engine state." After
the investigation separated the two symptoms and identified the VAR
failure as a genuine, fixable root cause (with the regime=UNKNOWN finding
turning out to be expected behavior, not a bug), a resolution was
proposed with trade-off options; Ovi asked follow-up questions about the
weakest option (D — fixed `n_components`) before deciding, explicitly:
*"let's continue with this resolution you suggest"* — the option that
fixes the dimensionality at its source (subcategory aggregation +
decoupled PCA window) rather than capping `n_components` by fiat. Two
subsequent "continue the project work" instructions authorized
implementation through sandbox testing and the live mirror without
further recap.
**Scope**: 1 source file, 1 paired test file, `tests/COUNT_BASELINE.txt`,
`pyproject.toml`, `CHANGELOG.md`, this dev-log. No `KNOWN_RISKS.md` entry
— this is a resolved bug, not an accepted risk. No change to
`regime_store.parquet` lookup logic in any CrossAssetEngine module —
`regime=UNKNOWN` was diagnosed as expected behavior (see §1) and left
untouched.

---

## 0. Live symptom

```
gold_cross_asset_correlation: Universe: 196 Layer 1 + 58 Layer 2 = 254 merged (after de-dup)
                               253 symbols, 31,878 pairs, regime=UNKNOWN -> cross_asset_corr.parquet
gold_lead_lag:                10,976 pairs tested, 6 bh_significant (q=0.03) -> lead_lag_matrix.parquet
gold_forecast:                VAR fit failed for <every equity in the active_ohlcv universe>:
                               maxlags is too large for the number of observations and the
                               number of equations. The largest model cannot be estimated.
                               [gold_forecast] No equity produced a fittable VAR -- nothing to write
                               [logged SUCCESS]
```

The VAR failure message was identical for every symbol tested — from
`AAPL` and `EUR_USD` through IDX names like `BBCA` — regardless of asset
class, liquidity, or history depth. That uniformity was the first and
strongest empirical signal: a per-symbol data problem produces partial
failures with varying observation counts; a shared-input problem fails
everyone identically.

## 1. Two symptoms, two different verdicts

**`regime=UNKNOWN` — investigated and closed as *not a bug*.** Read
`correlation_module.py::_get_current_regime()` and
`forecast_module.py::_get_current_regime()` directly: both return
`"UNKNOWN"` exactly when `data/gold/macro/regime_store.parquet` doesn't
exist or has no row for `run_date` — documented, deliberate degrade-
gracefully behavior, not a fallback masking failure. Confirmed via
`get_file_info` that `data/gold/macro/` doesn't exist as a directory at
all, and `list_directory` on `data/gold/` showed only `cross_asset/` —
`gold_regime` (the macro regime job, code already exists in
`src/gold/macro_regime.py`, 15.51 KB) has never been run in this repo.
Checked `job_registry.py`: none of the four CrossAssetEngine jobs
declare `gold_regime` as a dependency, by design (regime is read
opportunistically, "cannot fail, block, or be blocked by" per the job
docstrings). Conclusion: Ovi has been testing Wave 1 Cycle 4 standalone,
ahead of the rest of the Gold layer chain
(`gold_signals`→`gold_mtf`→`gold_regime`→`gold_sector`→`gold_screener`).
Once the daily SOP runs through `gold_regime`, every CrossAssetEngine
module picks up a real regime label automatically — no code change
needed or made.

**The VAR failure — investigated and confirmed as a real, fixable
root cause.** Read `forecast_module.py` in full and
`instruments_taxonomy.yaml` in full (not summarized — every subcategory
counted by hand against the live YAML). Computed `forecast_context()`'s
current size directly from the taxonomy: dollar(1) + dollar_basket(7) +
equity_dm(9) + equity_em(5) + volatility(1) + commodity_energy(2) +
commodity_metals(5) + commodity_agri(1) + commodity_coal(1) +
etf_credit(1) + etf_commodity(1) + etf_international(5) +
etf_thematic(1) = **40 instruments**, plus the derived
`broad_dollar_return` column → **p ≈ 41 features** into PCA.
Cross-referenced `correlation_module.py`'s own live-calibration comment
(from FIX XAE-CAL-01, one day earlier) for real observation counts in
the shared 65-day window: AAPL 45, BBCA 44, EUR_USD 47, CL 47, SSEC 45,
JKSE 43, DXY 47, IDR 48 — all in the 43–48 range. Since
`_build_pca_scores()` builds its PCA input via a union-style pivot
(not intersection) across ~40 symbols each individually clearing the
43–48 range, the union very plausibly covers nearly every weekday in
the window → **n ≈ 47–48 rows**.

p≈41 against n≈47–48 (ratio ≈0.85) is exactly the regime where
`PCA(n_components=0.95)` on noisy daily financial returns has no
concentrated factor structure to find — with p this close to n, the
sample covariance matrix's eigenvalue spectrum spreads out (the
practical consequence of what random matrix theory predicts at
p/n→1), so reaching 95% cumulative variance requires retaining most of
the available components, not the 3–5 Architecture v2.0
§6.4.1–6.4.3 assumed. That assumption was calculated when Layer 2 was
25 series (p/n≈0.4, a genuinely healthy ratio) — later architecture
documents and GMI Decision Documents grew `forecast_context()` to 40
(dollar_basket alone added 7 minor-currency legs) without anyone
revisiting whether the PCA/VAR degrees-of-freedom assumption still
held. It didn't. The inflated `n_pcs` (almost certainly 25–40+, not
3–5) is shared by every per-equity VAR (`k = n_pcs + 1`, `maxlags=5`),
which is why the failure is universe-wide and identical in wording for
every symbol.

## 2. Decision

Presented four options (A: fixed `n_components`; B: adaptive
`maxlags`; C: reduce PCA input scope; D: A+B combined) with an honest
assessment that D was a reliability stopgap, not a resolution — it
would make the job run without knowing whether the surviving 5 fixed
components carried real signal or mostly noise at this p/n ratio, and
it doesn't touch the root tension (Layer 2 outgrew the observation
window). Ovi pushed back explicitly on that framing ("It cost too
much... if the objective is precise output, what is the resolution for
this problem?"), which reframed the question correctly: not the
cheapest fix, but the fix that resolves the actual p/n mismatch rather
than discarding information from it.

Proposed and Ovi approved: two independent, additive fixes that
reduce p at its source (not an arbitrary cutoff) and separately raise
n where there's genuine headroom to do so.

1. **`_aggregate_by_subcategory()`** — collapse the ~40 raw
   `forecast_context()` columns into one z-scored composite per
   `context_category` *before* PCA, using the taxonomy's own economic
   grouping (e.g. the 9 `context_equity_dm` indices become one
   composite instead of 9 collinear PCA columns) rather than an
   invented statistical threshold. Cuts p from ~41 to ~13–14.
2. **`PCA_LOOKBACK_DAYS = 180`** — decouple the Layer 2/Broad-Dollar
   PCA input window from `LOOKBACK_DAYS=65` (which stays 65,
   governing per-equity follower reads only). `ForecastModule`'s PCA
   step has no `CorrelationModule`-style regime-purity requirement
   forcing a short window (§6.2.1's rationale for staying short is
   specific to that module's regime-conditional correlation
   matrices), so widening just this window isn't a hidden trade-off
   against a documented requirement elsewhere.

Combined: p≈13–14 against n≈125 (≈9x ratio), genuinely healthy PCA
territory versus the prior ≈0.85x knife-edge.

## 3. Implementation

`forecast_module.py`:
- Module docstring gained a new section (`FIX GMI-FORECAST-DIM-01`)
  documenting the full empirical root-cause account and the two-fix
  rationale, following this codebase's established convention of
  keeping fix history readable from the file itself.
- `LOOKBACK_DAYS` comment narrowed to "per-equity follower reads
  only"; new `PCA_LOOKBACK_DAYS = 180` constant added with its own
  multi-line rationale comment.
- `_load_pivot()` gained an explicit `lookback_days: int =
  LOOKBACK_DAYS` parameter (backward-compatible default) so the
  Layer 2 PCA-input call site can pass `PCA_LOOKBACK_DAYS` while
  `_load_equity_returns()`'s call site is untouched and keeps the
  original window.
- `_build_pca_scores()` now keeps the `Instrument` objects from
  `forecast_context()` (not just their symbols), passes
  `lookback_days=PCA_LOOKBACK_DAYS` to the Layer 2 pivot call, and
  runs the pivot through `_aggregate_by_subcategory()` before it
  reaches PCA. `load_fx_returns()` also switched to
  `PCA_LOOKBACK_DAYS` — its own docstring's "Broad Dollar and PCA
  read from one consistent window" invariant is preserved, just at
  the new width.
- New static method `_aggregate_by_subcategory(pivot, instruments)`:
  builds a symbol→category map via
  `getattr(inst, "context_category", None) or inst.symbol` (the `or
  inst.symbol` fallback is what makes this a genuine no-op for every
  existing test double that only sets `.symbol` — each becomes its
  own singleton "category," reproducing pre-fix behavior exactly),
  then per-category z-scores each member column (nan-safe, ddof=0,
  `std<1e-12` substituted with `1.0` to avoid divide-by-zero on flat/
  pegged columns like HKD) and averages within the group via
  `np.nanmean` (so a single missing member on a given date doesn't
  NaN out the whole composite for that date).

No output schema change — `n_pcs` and `pca_variance_explained` were
already columns in the output; they now simply reflect the reduced,
more meaningful dimensionality.

## 4. Tests

Followed this project's "new regression test classes must fail
against buggy source and pass against fixed source" convention
literally: wrote 6 new tests, then `git stash`'d the fix and ran them
against the pre-fix source before touching anything else.

- `TestSubcategoryAggregation` (4 tests, unit-level on the static
  method in isolation): collapses correctly to one column per
  category; missing `context_category` falls back to symbol
  singleton (the explicit backward-compat guarantee); a flat/zero-
  variance column doesn't produce inf/NaN; a partially-null member
  on one date still yields a finite composite from the remaining
  member(s).
- `TestForecastDimensionalityRegression` (2 tests): the money test,
  `test_many_instruments_few_categories_var_still_fits`, builds 20
  synthetic Layer 2 symbols across 4 categories on a 50-observation
  window (deliberately matching the production failure's ~47–48 obs
  shape) and asserts the VAR now fits (`result is not None`) with
  `n_pcs <= 4` — proving the collapse to composites, not just "it
  didn't crash." The second sanity-checks `PCA_LOOKBACK_DAYS !=
  LOOKBACK_DAYS` and `PCA_LOOKBACK_DAYS > LOOKBACK_DAYS` so the two
  constants can never silently re-alias to each other.

Pre-fix run (`git stash` on `forecast_module.py` only):
```
6 failed, 13 passed  — AttributeError: _aggregate_by_subcategory /
PCA_LOOKBACK_DAYS don't exist on the pre-fix module; the money test
also independently reproduced the exact production VAR-fit-failure
message before hitting the AttributeError path in later assertions.
```
Post-fix (`git stash pop`): `19 passed` in `test_forecast_module.py`
(13 original + 6 new, zero existing assertions touched). Full suite:
`1719 collected, 1717 passed, 2 failed` — the 2 failures
(`test_check_poetry_env.py`) confirmed pre-existing and unrelated by
re-running the same file against an untouched `git stash` of the
whole change: identical 2 failures, 13 passed, caused by no `poetry`
binary on this sandbox's `PATH`, not by this fix.
`tests/COUNT_BASELINE.txt`: 1713 → 1719.

## 5. Sandbox → live mirror

Cloned `github.com/Ovi-xyz/alpha-factory` fresh into the container
(latest commit `8eedb94`, matching the running-log session), confirmed
`tests/COUNT_BASELINE.txt` (1713) and full-suite collection count
matched before touching anything. Implemented and tested entirely in
the clone first. Every one of the 5 changed files was mirrored to the
live Mac via targeted `edit_file` calls (or `write_file` for the two
wholly-new-content files) and byte-verified by immediately reading the
live file back via `copy_file_user_to_claude` + local `diff` against
the sandbox-tested version:

- `src/gold/cross_asset/forecast_module.py` — byte-identical
  (sha256 `7e6ce851...`).
- `tests/unit/test_forecast_module.py` — byte-identical.
- `CHANGELOG.md` — mirrored via a single targeted `edit_file` (the
  live file is ~342 KB of accumulated history; rather than transfer
  the whole file, the new v1.18.2 entry was prepended using the
  unique anchor `"# CHANGELOG — Data Platform\n\n## v1.18.1 — FIX
  XAE-CAL-01"`) — byte-identical after read-back.
- `pyproject.toml` — same targeted-edit approach (anchor: `version =
  "1.18.1"\n# FIX XAE-CAL-01`), following this file's own established
  convention of keeping a per-version fix-rationale comment block
  directly below the version line — byte-identical after read-back.
- `tests/COUNT_BASELINE.txt` — byte-identical.

## 6. Not done / explicitly out of scope this pass

- **Full `gold_domain_scores.parquet` (ADR-004)** — the Architecture
  Extension's own designed answer to "too many raw Layer 2 variables"
  (8 economically-interpretable domain scores instead of 61 raw
  series). Confirmed via `list_directory_with_sizes` that no
  `domain_scores.py` module exists anywhere under
  `src/gold/cross_asset/`. The subcategory-composite aggregation
  landed this pass is a lighter-weight version of the same idea
  (13–14 composites vs. 8 fully-computed domain scores) — genuinely
  correct as far as it goes, but a full domain-score module (its own
  schema, weighted `_meta.contributes_to` aggregation logic, its own
  tests) is a multi-day build, not this fix's scope.
- **`gold_regime` was not run.** `regime=UNKNOWN` is expected given
  the current state of this repo (see §1) and needs no code change —
  but it also means the empirical `regime` values recorded by
  `gold_cross_asset_correlation`/`gold_lead_lag`/`gold_forecast` in
  this session's log are all placeholder, not evidence of anything
  about regime-conditional behavior.
- **Actual empirical `pca_variance_explained` / `n_pcs` on live
  Silver data was not observed.** Every number in this fix's
  root-cause account (p≈41, n≈47–48, the expectation that `n_pcs`
  now lands near 3–6) is derived from taxonomy counts and the prior
  session's calibration comment — sound reasoning, but not yet a
  live `python runner.py --job gold_forecast --force` run against
  real Silver Parquet. That run, and reading back the actual
  `pca_variance_explained` column it produces, is the natural next
  empirical checkpoint before this fix is considered fully closed in
  practice rather than in sandbox.
