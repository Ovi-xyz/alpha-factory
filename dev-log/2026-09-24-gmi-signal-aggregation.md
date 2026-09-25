# 2026-09-24 — signal_aggregation (Architecture v2.0 §5.2.7): Composite Indicator Score + Sector Breadth

**Date:** 24 Sep 2026
**Version:** v1.18.8 → v1.19.0 (MINOR)
**Related:** CHANGELOG.md v1.19.0, KNOWN_RISKS.md RISK-33 (NEW, OPEN),
`job_registry.py`'s own `gold_global_regime` entry (carried the TODO
this session closes since v1.18.0), `active_symbols.py`'s module
docstring (already listed `signal_aggregation` as a consumer)

## 0. Pre-work verification

Instructed to "continue with signal_aggregation" (link to
github.com/Ovi-xyz/alpha-factory given for context, not as the working
copy). Explored the live repo via the Filesystem MCP connector first —
no `src/gold/signal_aggregation.py` existed; `src/gold/cross_asset/`
held only the four Cycle 4 modules. Found the actual scope pointer
empirically rather than guessing from the design docs alone: the
`gold_global_regime` job_registry entry's own comment (written during
Cycle 4, v1.18.0) explicitly named this exact module as deferred work,
and CHANGELOG.md v1.18.0's own "Yang SENGAJA belum dikerjakan" section
lists "modul `signal_aggregation` (§5.3)" verbatim.

Cloned the repo from GitHub into the sandbox (`git clone
https://github.com/Ovi-xyz/alpha-factory.git`) rather than working
file-by-file over the Filesystem MCP connector, since a full local test
run needs the whole tree. Confirmed byte-identical against the live
`job_registry.py` (`copy_file_user_to_claude` + `diff`) before trusting
the clone. HEAD was `0458896` / v1.18.8, matching live exactly — no
divergence between GitHub and the M1 checkout at session start.
`pip install -e .`, `tests/COUNT_BASELINE.txt` = 1746, full suite:
1744 passed / 2 failed (the standing `poetry`-not-on-PATH environment
failures, present since long before this session) — confirmed clean
baseline before touching anything.

## 1. Design decisions

Read `technical_signals.py`, `mtf_alignment.py`, `sector_rotation.py`,
`active_symbols.py`, `screener.py`, `atomic_io.py`, and
`progress_checkpoint.py` in full before writing anything — Architecture
v2.0 §5.3 only names composite_score's four inputs (RSI, MACD momentum,
ADX trend strength, relative_volume) and the five output columns; it
never gives a formula, a weighting scheme, or grade thresholds. Every
concrete number below is this session's own documented choice, not a
transcription from the design docs — written up front in
`signal_aggregation.py`'s own module docstring, not left implicit.

Validated the core math empirically in the sandbox REPL before writing
it into the module (`pl.mean_horizontal` null-skip behaviour, `.tanh()`/
`.clip()`/`.sign()` availability on polars 1.44.2 Expr, a `how="full",
coalesce=True` join's exact null/column behaviour) rather than assuming
polars API surface from memory.

Formula settled on:
- Per TF: `rsi_component = clip((rsi_14-50)/50, -1, 1)`,
  `macd_component = tanh(macd_hist/atr_14)` (null-guarded on
  `atr_14<=0`), `adx_component = min(adx/50,1) * sign(di_plus-di_minus)`,
  `volume_component = tanh(relative_volume-1.0)`. Averaged with
  `pl.mean_horizontal` (null-skip, not null-propagate) into
  `tf_composite_{TF}`.
- Across TFs: outer/full join (not inner) across all 5 `TIMEFRAMES`,
  then `mean_horizontal` again into `composite_score`, defaulting to
  0.0 only when literally zero TF composites are available anywhere
  (tracked via a new `tf_coverage_count` column so the default is
  auditable, not indistinguishable from a real neutral read).
- Grade buckets A/B/C/D at 0.60/0.35/0.15 — arbitrary, documented as
  such, tunable.
- Sector breadth reuses `sector_regime_weights.parquet`'s own `sector`
  column (don't recompute a second sector definition) with an
  `InstrumentLoader` fallback.
- `sector_momentum` scans this module's own prior daily output files
  for the one closest to a 7-calendar-day lookback.
- `breadth_divergence` is a soft join against `mtf_alignment` — null,
  not 0.0, when unavailable.

The one deliberate departure from the design doc's own code sketch:
Architecture v2.0 §6.6 lists `signal_aggregation` as a `gold_signals` +
`gold_mtf`-adjacent dependency of `gold_screener` (hard). This session
chose NOT to add either `signal_aggregation` or `gold_global_regime` to
`gold_screener`'s `depends_on` in `job_registry.py`, extending the
soft-dependency pattern `screener.py`'s own docstring already
established for the three CrossAssetEngine sources during Cycle 4 (see
RISK-31): these are Section 0.2/0.3 informational DATA fields, and a
hard dependency would let a stale/missing daily run block the entire
watchlist for no filtering benefit. Recorded in three places so it
reads as a decision, not an oversight: `signal_aggregation.py`'s module
docstring, the updated comment on `gold_global_regime`'s own
`job_registry.py` entry (which had carried the original TODO), and
CHANGELOG.md v1.19.0.

## 2. Module + tests

`src/gold/signal_aggregation.py` — `run()` entry point on the
established `ProgressCheckpoint("signal_aggregation", run_date)` /
single `"ALL"` checkpoint pattern (matching `mtf_alignment.py`, not
`technical_signals.py`'s per-TF checkpoints, since this is one
aggregation pass, not seven independent per-TF computations),
`atomic_write_parquet` for the output file. `TIMEFRAMES` imported from
`technical_signals.py` rather than re-declared a third time anywhere in
the codebase (mtf_alignment.py already has its own independent copy —
pre-existing, not this session's drift to fix).

Smoke-tested the full `_compute()` pipeline against hand-built synthetic
fixtures in the sandbox REPL before writing the formal pytest suite —
caught one fixture-authoring mistake (both symbols intended to
straddle their sector's EMA-50 both ended up on the same side) via the
resulting assertion failure, not a module bug.

`tests/unit/test_signal_aggregation.py` — 38 tests across per-component
formula bounds/null-safety, cross-TF outer-join correctness (including
a symbol present in only one TF, the ADR-046-Path-C-class drop bug this
codebase has hit before), the `tf_coverage_count`/no-nulls guarantee,
grade threshold boundaries, sector breadth percentage math, the
sector-momentum lookback window (exact match, out-of-window, and
closest-of-several cases), breadth_divergence's soft mtf dependency,
and `run()`'s checkpoint idempotency / failure / graceful-empty paths.
One real bug caught by the sector-breadth test itself during
authoring: `_compute_sector_breadth()` originally short-circuited to
empty whenever `sector_map` had zero rows, which silently discarded a
legitimate "no known sector mappings yet, bucket everyone as Unknown"
case — fixed by dropping that half of the guard once confirmed
empirically that `polars` left-joins against an empty right-hand
DataFrame correctly leave every row's new column null (checked directly
in the REPL, not assumed).

Coverage: `signal_aggregation.py` 84%, `screener.py` 96% after its own
changes — both above the CI 80% gate.

## 3. job_registry.py wiring

New `signal_aggregation` entry (`depends_on`:
`["gold_signals", "silver_active_symbols"]`), new `_signal_aggregation`
wrapper, inserted into `DAILY_SEQUENCE` right after `gold_mtf` (so the
soft `mtf_alignment` read is normally satisfied in the scheduled path,
without making it a hard ordering requirement). Updated the long-lived
comment on `gold_global_regime`'s own entry to record that the TODO it
had carried since v1.18.0 is now closed, and how (soft integration, not
the hard `depends_on.append()` the comment used to point to).

Ran `tests/integration/test_job_registry_integrity.py`,
`test_runner_weekly_cadence.py`, `test_full_system.py`,
`test_pipeline_dashboard.py`, `test_fred_daily_ingester.py`, and
`test_runner.py` immediately after this edit (170 tests) — all green,
no hardcoded `DAILY_SEQUENCE` length or job-set assertions broke.

## 4. gold_screener integration

New `signal_agg_tbl` soft source — `_empty_signal_agg_df()`,
try/except-guarded read of
`signal_aggregation_{run_date}.parquet`, `LEFT JOIN ... ON m.symbol =
sa.symbol` (per-symbol shape, same as `forecast_tbl`, not the
broadcast `LEFT JOIN ... ON TRUE` shape `regime_tbl`/`global_regime_tbl`
use). Five new watchlist columns: `composite_score`, `composite_grade`,
`sector_breadth_pct`, `sector_momentum`, `breadth_divergence`. Extended
`_patch_all_optional_sources()` in `test_screener.py` and added a new
`TestSignalAggregationIntegration` class (5 tests: absent, present,
symbol missing from signal_aggregation's own output, wrong-date file,
corrupt file) mirroring `TestCrossAssetEngineIntegration`'s existing
shape exactly rather than inventing a new test-authoring convention for
one more soft source.

## 5. views.py

Added `v_signal_aggregation`, same per-date glob shape as
`v_mtf_alignment`/`v_screener`. Confirmed no test hardcodes
`VIEW_DEFINITIONS`' length or key set before adding it.

## 6. Verification

Full suite after every wiring step, not just at the end: 44/44 on
`test_screener.py` after the screener edit, 28/28 on
`test_views.py`+`test_check_glob_scope.py` after the views edit, then
the complete suite: **1789 tests collected, 1787 passed, 2 failed**
(the same pre-existing `poetry` environment failures from the v1.18.8
baseline, confirmed identical before and after — 0 regressions).
`tests/COUNT_BASELINE.txt`: 1746 → 1789.

## 7. What was deliberately NOT done this session

- No live run against real Silver/Gold data on the M1 — everything here
  is sandbox/synthetic-fixture only. RISK-33 registers this explicitly,
  same pattern as RISK-31 did for Cycle 4.
- `composite_score`'s formula and weighting, and the grade thresholds,
  are uncalibrated against any real distribution — flagged in the
  module docstring and RISK-33, not silently asserted as correct.
- No dedicated freshness/staleness check for `signal_aggregation` or
  `gold_global_regime` output ages, mirroring the same gap RISK-31 left
  open for the three CrossAssetEngine soft sources.
- README.md was not touched — it already understated the pipeline
  (references CrossAssetEngine as "not yet built" and an outdated
  `DAILY_SEQUENCE` job count, predating this session's own start) and
  reconciling it is a separate, larger cleanup outside this session's
  scope; noted to Ovi rather than silently left inconsistent or
  silently "fixed" in passing.

## 8. Sandbox → live mirror

Every new/modified file mirrored to `/Users/opi/alpha-factory` via the
Filesystem MCP connector's `write_file`, then byte-verified
(`copy_file_user_to_claude` + `diff` against the sandbox copy) before
moving to the next file: `src/gold/signal_aggregation.py`,
`src/scheduler/job_registry.py`, `src/gold/screener.py`,
`src/gold/views.py`, `tests/unit/test_signal_aggregation.py`,
`tests/unit/test_screener.py`, `CHANGELOG.md`, `KNOWN_RISKS.md`,
`pyproject.toml`, `tests/COUNT_BASELINE.txt`, this file.
