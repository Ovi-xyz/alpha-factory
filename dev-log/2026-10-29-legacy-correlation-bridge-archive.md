# 2026-10-29 — Archive `legacy_correlation_bridge.py` (no version bump)

## Request
Archive `src/gold/cross_asset/legacy_correlation_bridge.py` as "no longer needed", without a version
bump.

## Explore (live source, `main` 0b78829)
The premise did not hold yet. The bridge writes `data/gold/correlation/correlation_clusters.parquet`,
still read by `screener.py::_deduplicate_by_cluster()` (GD §15.1 max-per-cluster guard, gated on
`GOLD_CORR_PATH.exists()`, failures logged at debug) and `views.py::v_correlation`; and
`correlation_module.py` imported it at module level. Archiving the module alone would have broken
`gold_cross_asset_correlation` at import and, with the import removed, left the screener on a
stale file with no error. Stopped and asked; Ovi chose option A (screener reads
`cross_asset_corr.parquet` directly; `v_correlation` removed).

## Decide
- Cluster source = UNION of both pair sides (upper triangle: last symbol only in `symbol_b`);
  `CorrelationModule._cluster()` gives one id per symbol per run -> same projection as the bridge.
- Cardinality guard (`unique(subset="symbol")`) so the left join cannot fan out.
- Dedup failure path debug -> warning (new source schema; a mismatch must be visible).
- `v_correlation` removed, not repointed (ADR-055 §5: no Trading Engine, nothing external depends
  on it). No version bump -> CHANGELOG `Unreleased` entry, RISK-31 dated update, this log.

## Implement / verify
- Sandbox clone of `main`, deps pinned to `poetry.lock` (a first run with unpinned duckdb 2.0.0 /
  polars 2.1.0 gave 183 environmental failures; pinning duckdb 1.5.4 / polars 1.42.1 brought the
  baseline to 2886 passed + 2 failed `test_check_poetry_env`; xgboost stayed 3.4.1 vs locked 3.2.0).
- After: 2879 passed + the same 2 failures; failure set identical to baseline. Collected 2888 ->
  2881 (−15 bridge tests, −1 parametrized per-file case in `test_regime_cutover_adr055`, +4 screener,
  +5 pins). `COUNT_BASELINE.txt` 2888 -> 2881.
- Mutants killed: symbol_a-only read, no cardinality guard, `v_correlation` re-added, `run()` recreating
  the legacy dir, legacy path string reintroduced in `src/`.

## Live
Mirror, byte-verify, then move `data/gold/correlation/correlation_clusters.parquet` to
`data/archive/legacy_correlation_clusters_2026_10/`. Run `gold_screener --force` once and confirm the
dedup still applies (watchlist has at most `MAX_PER_CLUSTER` symbols per cluster; no
`Cluster dedup skipped` warning).
