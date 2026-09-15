# 2026-09-13 — GMI Wave 1 Cycle 4: CrossAssetEngine (4 Modules) + gold_screener Integration

**Version**: 1.17.11 → 1.18.0
**Trigger**: Ovi's direct instructions across one continuous thread, taken
in sequence: (1) "continue with... gold, the implementation of cross-asset
engine cycle 4" — Gate 1's status was surfaced as a blocker first (Explore
phase, confirmed live: no `gold/cross_asset/` package existed at all,
`correlation_matrix.py`/`hmm_regime.py` were both explicitly pre-Cycle-4
placeholders by their own docstrings), two clarifying questions presented,
Ovi chose "GlobalIndexRegimeModule (simplest, daily)" as the starting
module; (2) "continue with the completion of correlation module, lead-lag
module, forecast module in sequence. do not handle version/CHANGELOG/
KNOWN_RISKS/dev-log before the completion" — explicit instruction to defer
all documentation until code was done; (3) mid-sequence, Ovi answered the
still-open Gate 1 question by pasting the real BIS weight extraction
output (`2026-09-12-bis-broad-dollar-weights.txt`) rather than picking one
of the three offered options — read as "extraction is done, wire it in";
(4) "continue with gold_screener: integrate CrossAssetEngine outputs";
(5) this documentation + sandbox→live mirror pass.
**Scope**: 6 new source files, 4 new test files, 3 modified files
(`job_registry.py`, `screener.py`, `test_screener.py`), `pyproject.toml`,
`CHANGELOG.md`, `KNOWN_RISKS.md`, `tests/COUNT_BASELINE.txt`.

---

## 0. Pre-work verification

- Cloned `github.com/Ovi-xyz/alpha-factory` into the sandbox — confirmed
  it matched the live repo exactly (`53fc62b`, v1.17.11, same
  `tests/COUNT_BASELINE.txt`=1641, same `src/gold/` listing) before
  touching anything.
- Installed dependencies via pip, then explicitly re-pinned
  `statsmodels==0.14.6` after finding the sandbox's default pip-latest
  (0.15.0) had silently REMOVED the `verbose` parameter from
  `grangercausalitytests()` — production's `poetry.lock` pins 0.14.6,
  which still has it, deprecated, and (this mattered) still prints full
  output even when `verbose=False` is passed explicitly. Building and
  testing against the wrong statsmodels version would have hidden a real
  production log-flooding bug (see §3 below).
- Baseline confirmed: `pytest tests/ -q` → 1641 passed, 0 failed — exact
  match to `tests/COUNT_BASELINE.txt`, before any code change.
- Read `instrument_loader.py`, `silver_scope.py`, `context_anchors.py`,
  `active_symbols.py`, `macro_regime.py`, `screener.py`,
  `job_registry.py`, and `instruments_taxonomy.yaml` directly (not from
  memory of the design docs, which have drifted materially since —
  e.g. "13 global equity indices" in Architecture v2.0 is 14 live, since
  ADR-003 added SPX to `context_equity_dm` after that doc was written)
  before writing any Cycle 4 code.

## 1. GlobalIndexRegimeModule (Architecture v2.0 §6.5)

First CrossAssetEngine module — chosen by Ovi from three presented
options as the simplest, daily-cadence starting point. Universe resolved
live from `instruments_taxonomy.yaml` via
`InstrumentLoader.by_context_category()`: `context_equity_dm`=9
(SPX, NYA, DJI, IXIC, FTSE, DAX, CAC, N225, AXJO — SPX included since
ADR-003), `context_equity_em`=5 (TWSE, KOSPI, HSI, SSEC, JKSE) = 14 total,
not the stale "13" the original doc still says. Asia-Pacific subset (7:
N225, AXJO + all 5 EM) unchanged since none of those 7 were touched by
the SPX/VIX/DXY reclassification.

Formulas: `global_risk_score` (% of 14 with close > EMA-50),
`dm_em_divergence` (DM score − EM score), `asia_pac_breadth` (% of 7
Asia-Pacific with close > EMA-20), `global_regime_label`
(RISK_ON/RISK_OFF/MIXED at 60/40 thresholds — this module's own
first-pass calibration, stated as such in the docstring since
Architecture v2.0 never gives exact cutoffs), `regime_transition`.

One deliberate departure from `macro_regime.py`'s own precedent:
`_get_prev_label()` explicitly excludes the run_date's own row when
looking up "previous" label (`date < run_date`, not `<=`).
`macro_regime.py`'s `_get_prev_regime()` does not exclude it, which means
a same-day rerun there would read its own already-written value back as
"previous" — a real, if minor, idempotency wrinkle in that module. Not
fixed there (out of scope), but not reproduced here either, since
excluding one date is essentially free.

Reused `add_ema()` from `gold/indicators/core_indicators.py` rather than
reimplementing EMA — same function `technical_signals.py` already uses.

Output: `data/gold/cross_asset/global_regime.parquet`, append-and-dedupe-
by-date pattern matching `macro_regime.py`'s own `run()`.

Job: `gold_global_regime`, `depends_on=['silver_ohlcv_context']`, placed
in `DAILY_SEQUENCE` right after `silver_context_anchors` — independent of
the `gold_signals → gold_mtf → gold_regime → gold_sector → gold_screener`
chain, not wired into `gold_screener` at this point in the sequence
(that came later, §7 below).

Manually smoke-tested with synthetic uptrend/downtrend fixtures before
writing formal tests (9 DM up / 5 EM down → `global_risk_score=64.3`,
`RISK_ON`, `asia_pac_breadth=28.6` — matches hand-calculation exactly);
idempotent-rerun and cross-day transition detection verified end-to-end
before formalizing into pytest. 20 tests
(`tests/unit/test_global_index_regime.py`): universe resolution against
the real live taxonomy (not mocked), breadth math, threshold boundaries,
no-data/no-lookahead handling, transition detection, idempotent reruns,
partial-coverage flagging, `is_clean` scoping (including the discovery
that excluding one dirty bar out of many does NOT drop a symbol from
coverage — the last clean bar is used instead, which is graceful
staleness rather than a bug, and the test was corrected to assert the
actual, correct behavior rather than the assumption the test was
originally written against).

Job registry integrity re-checked: `test_job_registry_integrity.py`'s
`len(DAILY_SEQUENCE) >= 13`-style floor assertions (not exact-count) were
confirmed unaffected before relying on that pattern for the rest of this
session's job additions. `WEEKLY_SEQUENCE = [...] + DAILY_SEQUENCE`
confirmed by direct read, so `LAYER_JOB_NAMES['gold']` picks up new
`DAILY_SEQUENCE` entries automatically — no manual list to keep in sync.

## 2. CorrelationModule (Architecture v2.0 §6.2)

Second module. Universe: `ActiveSymbolsResolver.load_ohlcv(run_date)`
(Layer 1) ∪ `ContextAnchorsResolver.load(run_date)` (Layer 2) — both
raise `FileNotFoundError` if not yet resolved for `run_date`; caught and
treated as "skip, log warning," matching every other module's convention
for missing upstream data.

Estimator: `sklearn.covariance.LedoitWolf`, not raw Pearson — mandatory
per §6.2.1 for a large matrix on a ~60-day window, and this single choice
also satisfies §6.2.1's separately-stated "Shanghai handling: robust
estimator" requirement (Ledoit-Wolf is explicitly named as one of the two
acceptable choices there). Clustering via
`scipy.cluster.hierarchy.linkage`+`fcluster` on `1 - |correlation|`
distance — a deliberate, spec-driven difference from the pre-Cycle-4
`correlation_matrix.py`'s `sklearn.AgglomerativeClustering`, since
Architecture v2.0 §6.2.1 names scipy specifically for THIS module.

Regime tagging: each weekly run tagged with the single CURRENT regime
label (most recent `regime_store.parquet` row with `date <= run_date`) —
documented explicitly as a narrower, honest reading of §6.2.1's "separate
matrices computed per active regime" language, which only becomes
meaningful once many weekly snapshots accumulate across different
regimes; a single fresh run has exactly one current regime to tag itself
with.

Schema clarification: Architecture v2.0 §6.2.2's schema lists a single
`cluster_id` column on a pair-level row, which is ambiguous (a pair can
span two clusters). Resolved as `cluster_id_a`/`cluster_id_b`, stated
explicitly in the module docstring as a clarification of a genuinely
ambiguous spec rather than a silent choice.

Explicitly does NOT retire or touch the pre-Cycle-4 `gold_correlation`
job — both coexist under different job names
(`gold_cross_asset_correlation` vs `gold_correlation`), different output
paths (`data/gold/cross_asset/cross_asset_corr.parquet` vs
`data/gold/correlation/correlation_clusters.parquet`). Architecture v2.0
§5.1 marks the old one "REPLACED," but actual retirement — deregistering
the job, redirecting `gold_screener`'s existing correlation-cluster read
— is a separate, larger decision flagged in RISK-31, not made here.

Verified with two synthetic scenarios: independent random-walk symbols
(near-zero correlation, sanity check on bounds/shape), and two
manufactured correlated groups (within-group correlation ~0.92,
cross-group ~-0.1 — directionally correct for the construction). Found
during testing: the `n_clusters = min(10, max(2, n//2))` heuristic
(inherited from the old module for consistency) forces MORE clusters
than natural at small `n` in a synthetic test (n=6 forced 3 clusters onto
2 real groups) — not a bug (production `n` is ~240+, where this heuristic
just caps at 10), but the clustering test was designed around it: n=4 (2
pairs) forces exactly `max(2, 4//2)=2` clusters, cleanly matching 2
synthetic groups, rather than fighting the heuristic with a larger n.

15 tests (`tests/unit/test_correlation_module.py`): universe merge/dedup,
graceful skip on missing resolvers, Ledoit-Wolf correlation math
(within/cross-group, bounds, self-pair exclusion), clustering shape and
correctness at the right `n`, regime tagging (including the
same-CROSS-JOIN-class hazard already fixed once in `screener.py` for
`regime_tbl` — checked here too, at a smaller scale, and confirmed
`LEFT JOIN`-equivalent handling via a plain "row not found → UNKNOWN"
Python check rather than SQL), insufficient-history handling,
`is_clean` filtering, output schema via `run()`.

Job: `gold_cross_asset_correlation`, `depends_on=['silver_active_symbols',
'silver_context_anchors']`, weekly (`WEEKLY_SEQUENCE`, right after the
old `gold_correlation`).

## 3. LeadLagModule (Architecture v2.0 §6.3, ADR-001)

Third module. Leaders: `InstrumentLoader.correlation_context()` minus any
instrument with `exclude_from_lead_lag_leader=True` (SSEC, currently the
only one — confirmed via direct grep of `instruments_taxonomy.yaml`).
Followers: `ActiveSymbolsResolver.load_ohlcv(run_date)`. Test:
`statsmodels.tsa.stattools.grangercausalitytests`, testing "leader
Granger-causes follower" at lag 1-5.

**Real statsmodels version bug found and handled, not hypothetical**:
built and initially tested against pip-latest statsmodels 0.15.0 (no
`verbose` parameter at all), then re-verified against 0.14.6 (the exact
version `poetry.lock` pins) once the discrepancy was noticed — and 0.14.6
`grangercausalitytests(..., verbose=False)` PRINTS the full per-lag test
summary to stdout regardless, confirmed by direct empirical test
(captured stdout length 0 only after wrapping the call in
`contextlib.redirect_stdout`, not from `verbose=False` alone), plus
raises a `FutureWarning` on every call. At ~11,000 (leader, follower)
pairs per weekly production run, this would have flooded logs
significantly had it shipped unnoticed — `_run_granger_quiet_per_lag()`
wraps every call in `redirect_stdout` + `warnings.simplefilter("ignore")`
and tries `verbose=False` first, falling back to omitting the parameter
entirely on `TypeError` for forward-compatibility with statsmodels 0.15+.

Multiple-testing correction: Benjamini-Hochberg FDR at q=0.03 (ADR-001,
superseding Architecture v2.0's own original q=0.10), applied GLOBALLY
across every (leader, follower, lag) raw p-value collected across the
ENTIRE run — not per-pair after an "optimal lag" is first selected. This
is a specific, stated reading of ADR-001's own "Total tests: 57.950 =
anchors × equities × lags" framing: the lag dimension is part of the
multiple-testing universe, not resolved before correction.
`optimal_lag` per pair is then selected AFTER correction, as the lag with
the smallest RAW p-value among the up-to-5 tested for that pair; the
p-value adjusted and R² reported are that same lag's post-correction
values — `statsmodels.stats.multitest.multipletests(..., method='fdr_bh')`
used directly rather than hand-rolling the BH procedure.

`max_cross_corr`: max |Pearson correlation| between leader (shifted back
k days) and follower, k=1..5 — a magnitude sanity-check independent of
Granger significance, exposed as a column (per ADR-001's "actionable
filter >= 0.15") but never used to drop rows — every tested pair is
written, letting a consumer combine `bh_significant` AND
`max_cross_corr` however it wants rather than losing rows silently.

Verified with a synthetic follower built to depend on a leader's value
lagged by exactly 2 days (`coef=0.8`): correctly detected at
`optimal_lag=2`, `r_squared=0.81`, `bh_significant=True`,
`max_cross_corr=0.90`; an independent (noise) pair correctly resolved
`bh_significant=False`. Confirmed the exception path for a genuinely
degenerate case too (constant-value follower — statsmodels itself raises
"columns include a constant value," caught and the pair silently
skipped, not propagated).

11 tests (`tests/unit/test_lead_lag_module.py`): leader exclusion,
graceful skip, lagged-dependency detection at the correct lag,
independent-pair non-significance, global BH correction's
never-shrinks-below-raw invariant checked directly, degenerate/
insufficient-data skipping, output schema (`optimal_lag ==
optimal_lag_days`, both names kept — Architecture v2.0's own table and
Architecture Extension's described columns disagree on which name to
use, so both are populated identically rather than guessing).

Job: `gold_lead_lag`, `depends_on=['silver_active_symbols',
'silver_context_anchors', 'gold_cross_asset_correlation']` (the last for
documented run-order only — this module does not read
`cross_asset_corr.parquet`'s contents), weekly.

## 4. Gate 1 closure — `broad_dollar.py`

Mid-sequence, in response to the still-open "how should Cycle 4 handle
`compute_broad_dollar()`" question from the previous session, Ovi pasted
the real BIS extraction output
(`2026-09-12-bis-broad-dollar-weights.txt`) rather than choosing one of
the three offered options — read as "the extraction is done for real,
wire it in," and treated as such.

13 currencies, `2020_2022` vintage (BIS's own most recent — no
`2023_2025` sheet exists yet, per the extraction script's own output).
Two decisions made that the design docs never specified, both documented
in the module docstring and KNOWN_RISKS.md RISK-16's update rather than
made silently:

- **Renormalized to sum 1.0.** The raw 13-currency weights sum to only
  ~68.6 (out of the US row's full ~64-economy basket) — this platform
  only ever has return data for these 13, so using raw values would
  systematically understate Broad Dollar's magnitude by ~31% even on a
  day USD moves uniformly against all 13, which would make the
  DXY-vs-Broad-Dollar divergence signal (§7.2) an artifact of scale
  rather than genuine DM/EM dispersion.
- **Sign convention derived from real quotation direction, verified
  against live config, not assumed.** Checked
  `instruments_taxonomy.yaml`'s Layer 1 forex block directly: AUD_USD,
  EUR_USD, GBP_USD are quoted with USD as the QUOTE currency (a pair rise
  = USD weakened) and are negated before weighting; USD_CAD, USD_CHF,
  USD_JPY, plus all 7 Layer 2 `dollar_basket` currencies (confirmed via
  their own `yfinance_symbol` values — `USDCNH=X`, `USDKRW=X`, etc., all
  USD-as-base) are used as-is. This corrects — not merely replaces — an
  internal sign inconsistency in Architecture v2.0 §7.2's own
  hand-approximated `BIS_WEIGHTS` sketch, which gave `USD_JPY`/
  `USD_CAD`/`USD_CHF` positive weights while giving `USD_CNH`/`USD_KRW`/
  `USD_SGD` NEGATIVE weights despite all six sharing the identical
  USD-is-base convention — an error in that placeholder, not a design
  being preserved.

Verified numerically: a synthetic "USD +1% uniformly against all 13"
scenario produces `broad_dollar_return = +0.01` exactly (weights sum to
1.0 in absolute-value terms by construction, so a uniform signal passes
through unchanged in scale) — a strong sanity check that both the
renormalization and the per-currency sign logic are correct together,
not just individually plausible.

`KNOWN_RISKS.md` RISK-16 updated with a new dated subsection ("Gate 1
CLOSED, 12 Sep 2026") rather than rewritten, matching this file's own
established accretive-update convention.

## 5. ForecastModule (Architecture v2.0 §6.4)

Fourth and final module — the one Gate 1 had blocked. PCA input:
`loader.forecast_context()` (Layer 2, OHLCV-based only) plus
`broad_dollar_return` (§4 above) as an extra derived column.

**Scope limitation, stated not hidden**: FRED rate series (SOFR, DGS*,
T10Y2Y/3M) and BIS central bank rates
(`silver_global_rates.parquet`) are NOT part of the PCA input. Both live
in a genuinely different Silver schema
(`series_id/observation_date/value`, forward-filled PIT series) than the
`symbol/timestamp/log_return` OHLCV schema every other input here uses —
folding them in cleanly is its own read/pivot/align path, not a one-line
addition. Confirmed via direct check that `InstrumentLoader`'s own
`by_context_group()` docstring lists exactly 6 groups (dollar,
dollar_basket, fx_normalization, equity, commodity, etf) — "rates" is
not one of them, meaning the rate series were already architecturally
separated from the Instrument-object-based Layer 2 system before this
session, not something this session chose to exclude for convenience.
Flagged in the module docstring and registered as part of RISK-31, not
silently omitted.

**Deliberate departure from Architecture v2.0 §6.4.1's own code sketch**:
that sketch builds ONE VAR across `[PCs, ALL active equities]`
simultaneously. With ~190 active equities and 3-5 PCs, that is a
~195-variable VAR fit on a ~60-observation window — statistically
unidentifiable (§6.4.3 itself acknowledges the degrees-of-freedom
problem without resolving it with a concrete limit). This implementation
fits one SEPARATE VAR per equity instead (`[PC1..PC_k, that equity's own
return]`, typically 4-6 variables total) — the standard way to use a
handful of shared macro factors as context for many individual
univariate forecasts.

Lag selection: BIC (`ic='bic'`), resolving OD-1 (§12, listed "OPEN" but
with an explicit textual lean toward BIC already in the doc).

**Real statsmodels edge case found and handled**: when BIC selects
`k_ar=0` (no lag structure detected at all), `VARResults.forecast()` and
`.is_stable()` BOTH raise — confirmed directly (`IndexError` on
`forecast()`, `ValueError: need at least one array to concatenate` on
`is_stable()`) rather than assumed. Handled explicitly: `k_ar=0` reports
the equity's own training-window mean return at every horizon (what a
VAR(0) intercept-only model conceptually represents) and `stable=True`
(trivially — nothing to be unstable).

**Two more real bugs found during this module's own testing** (in
addition to the statsmodels ones already covered):

- `.to_numpy()` on a narrow Polars column selection returned a
  READ-ONLY array in one test configuration, breaking the in-place NaN-
  mean-imputation both this module and `correlation_module.py` do before
  fitting. Fixed with an explicit `.copy()` in both files once found via
  one failing test.
- `_build_pca_scores()` originally checked `if not layer2_symbols: return
  None` BEFORE ever attempting to load FX/Broad-Dollar data — meaning
  the intended "Broad-Dollar-only, no Layer 2 context symbols" fallback
  path was dead code from the start, caught only because a test was
  specifically written to exercise that exact fallback and failed
  unexpectedly ("forecast_context() returned no eligible symbols," when
  the test had FX data available and never needed Layer 2 context at
  all). Also surfaced, while fixing this, that sklearn's `PCA` handles a
  single-feature input fine (`n_components=0.95` on 1 column just
  returns that column standardized, 100% variance) — an earlier `len(
  feature_cols) < 2` guard was overly conservative and was relaxed to
  `< 1`, with the affected test's expectation flipped to match (single-
  feature PCA is valid, not an error case).

Verified with a synthetic follower built to depend on a single dominant-
variance context factor lagged by 1 day: correctly detected `var_lag=1`
with a materially larger forecast magnitude than an independent
follower. (An earlier attempt using TWO equal-variance independent
context symbols produced `var_lag=0` for a genuinely-dependent follower
— not a bug, but a real PCA-rotation-ambiguity artifact: with two
independent, equal-variance inputs, PCA's principal axes are not
uniquely determined and can mix the two inputs, diluting a signal that
depended on only one of them. Understood and avoided in the test design,
not worked around in the module code, since it's an inherent property of
PCA, not a defect.)

12 tests (`tests/unit/test_forecast_module.py`): graceful skip, output
schema/horizon range, multi-symbol coverage, lagged-dependency detection
vs. independent baseline, the `k_ar=0` fallback exercised for real (not
just unit-tested in isolation), insufficient-history skip without
breaking the rest of a batch, both PCA-input fallback paths (Layer 2
only, Broad-Dollar only), output schema via `run()`.

Job: `gold_forecast`, `depends_on=['silver_active_symbols',
'silver_context_anchors', 'gold_lead_lag']` (last one for documented
run-order only), weekly. Regime-transition-triggered re-run (§6.4.3:
"if GlobalIndexRegimeModule detects `regime_transition=True`,
ForecastModule re-runs within the daily pipeline") is explicitly NOT
implemented — a scheduler/`runner.py`-level orchestration concern (a
`--force`-style trigger reading `global_regime.parquet`), noted in the
module docstring so it isn't mistaken for an oversight.

## 6. Job registry wiring — summary

| Job | Cadence | depends_on |
|---|---|---|
| `gold_global_regime` | Daily | `silver_ohlcv_context` |
| `gold_cross_asset_correlation` | Weekly | `silver_active_symbols`, `silver_context_anchors` |
| `gold_lead_lag` | Weekly | + `gold_cross_asset_correlation` (order only) |
| `gold_forecast` | Weekly | + `gold_lead_lag` (order only) |

None of the four touch `gold_screener`'s `depends_on` (see §7).
`test_job_registry_integrity.py`'s floor-based assertions (`>= 13` style,
not exact counts) confirmed unaffected by all four additions — re-run
after each one, not just once at the end.

## 7. gold_screener integration (Architecture v2.0 §5.3, §9.1 Phase 5)

Three new optional sources, following the EXACT existing
has-file/try-except/`_empty_*_df()` idiom already used for
`sector_tbl`/`active_tbl`/`regime_tbl`:

- `global_regime_tbl` → `global_risk_score`, `global_regime_label`,
  `dm_em_divergence`. Single-row-per-date, `LEFT JOIN ... ON TRUE` —
  the exact same broadcast pattern already fixed once for `regime_tbl`
  (GLD-SCR-001: a `CROSS JOIN` against a legitimately-empty subquery
  had silently zeroed the entire watchlist, not just that table's
  columns). Re-tested that exact hazard shape for the new table too
  (file exists but has no row for this run_date) rather than assuming
  the pattern transfers correctly by inspection alone.
- `lead_lag_tbl` → `lead_lag_signal` (bool), `lead_lag_top_leader`,
  `lead_lag_top_lag`. Pre-aggregated per follower symbol in Python
  before registering with DuckDB (raw store is one row per leader ×
  follower; screener wants one row per symbol) — sorted by
  `p_value_adjusted` ascending, `group_by("follower").agg(first(...))`
  picks the strongest relationship per symbol.
- `forecast_tbl` → `forecast_return_1d`, `forecast_stable`, filtered to
  `horizon_days=1` (nearest-term, most relevant to a watchlist of
  current candidates).

All three purely informational — none used in the `WHERE` clause or
`ORDER BY`, matching the Separation of Concerns treatment already given
to `days_to_earnings`/`sentiment_score` before Finnhub's retirement.
Explicitly NOT added to `gold_screener`'s `depends_on` — matches
`silver_active_symbols`'s own precedent (feeds `active_tbl`, also not a
hard dependency): confirmed by direct check that `gold_screener`'s
current `depends_on` is only `['gold_mtf', 'gold_regime', 'gold_sector']`
— three hard dependencies, with sector/active/regime beyond that treated
as soft, graceful-degrade sources. The three new Cycle 4 sources follow
the majority (soft) pattern.

11 new tests added to the existing `tests/unit/test_screener.py`
(`TestCrossAssetEngineIntegration`), reusing and extending the shared
`_patch_all_optional_sources()` fixture helper (which now also redirects
the three new path constants into the test's `tmp_path`). All 34
pre-existing screener tests re-run and confirmed unaffected before AND
after adding the new class.

## 8. Verification

- Full suite: **1710 passed, 0 failed** (1641 baseline + 69 new: 20 +
  15 + 11 + 12 + 11). Re-run after every module, not just once at the
  end, per this project's own "confirm baseline pass count PERSIS"
  workflow discipline.
- `ast.parse` clean across every new and modified `.py` file (Gate G-1
  equivalent, run manually).
- `grep -rn 'f"SELECT\|f'"'"'SELECT'` across `src/gold/cross_asset/` and
  `src/gold/screener.py` — clean, no f-string SQL (Gate G-2 equivalent).
- `tests/COUNT_BASELINE.txt` updated 1641 → 1710.
- `pyproject.toml` version bumped 1.17.11 → 1.18.0 (MINOR — new
  capability, no breaking interface change; matches the project's own
  versioning semantics table).

## 9. What was deliberately NOT done this session

(Also registered as KNOWN_RISKS.md RISK-31, so this list is not just a
dev-log footnote.)

- `gold_correlation`/`correlation_matrix.py` (pre-Cycle-4) not retired —
  coexists with the new `gold_cross_asset_correlation`.
- FRED/BIS macro rate series not part of ForecastModule's PCA input.
- ForecastModule's regime-transition-triggered re-run (§6.4.3) not
  implemented — scheduler-level concern.
- `signal_aggregation` module (§5.3) not built.
- None of the four Cycle 4 modules have ever run against real production
  Silver data — sandbox/synthetic-fixture testing only. This is the
  single biggest open item; see RISK-31's "Suggested next step."

## 10. Sandbox → live mirror

Mirrored via the Filesystem MCP connector to `/Users/opi/alpha-factory`.
Every new file written fresh; every modified file's live pre-mirror state
first confirmed to still match this session's sandbox starting point
(no drift since v1.17.11) before overwriting. Byte-for-byte `diff`
verification per file after write — see the chat turn itself for the
per-file confirmation log; not duplicated here since this dev-log file
is itself one of the mirrored artifacts.
