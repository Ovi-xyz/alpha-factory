# archive/legacy_correlation_bridge_2026_10/

Archived, not deleted (29 Oct 2026, no version bump, `pyproject.toml` stays 1.24.0). Rollback =
`git mv` the two files back (paths mirror their original location under the repo root), restore
the import and the `write_legacy_correlation_clusters(result)` call at the end of
`src/gold/cross_asset/correlation_module.py::run()`, repoint `screener.py::CROSS_ASSET_CORR_PATH`
usage at `data/gold/correlation/correlation_clusters.parquet`, and re-add the `v_correlation` view.

## What's here

- `src/gold/cross_asset/legacy_correlation_bridge.py` — `derive_legacy_correlation_clusters()` /
  `write_legacy_correlation_clusters()` (GMI-CORR-RETIRE-01, RISK-31, 20 Sep 2026). Projected
  `CorrelationModule`'s pairwise `cross_asset_corr.parquet` into the legacy per-symbol shape
  (`symbol, cluster_id, correlation_avg, n_cluster_members, computed_date`) and wrote it, best-effort,
  to `data/gold/correlation/correlation_clusters.parquet` at the end of every
  `gold_cross_asset_correlation` run.
- `tests/unit/test_legacy_correlation_bridge.py` — its 15 tests.

## Why it was retired

The bridge existed only so two consumers of the retired `gold_correlation` job kept working
unchanged: `screener.py::_deduplicate_by_cluster()` (the GD §15.1 max-2-per-cluster guard) and
`views.py::v_correlation`. Neither needs it any more:

1. `screener.py` now reads `cross_asset_corr.parquet` directly — the union of
   `(symbol_a, cluster_id_a)` and `(symbol_b, cluster_id_b)`, which is exactly the `cluster_id` the
   bridge carried (`CorrelationModule._cluster()` assigns one id per symbol per run). The guard
   itself is unchanged.
2. `v_correlation` is removed. There is no Trading Engine yet and nothing outside the repo
   depends on the view (ADR-055 §5); `cross_asset_corr.parquet` is the contract output.

`correlation_avg` and `n_cluster_members` had no consumer in `src/`.

## What replaced each consumer

| Consumer | After |
|---|---|
| `screener.py::_deduplicate_by_cluster()` | `CROSS_ASSET_CORR_PATH` (`data/gold/cross_asset/cross_asset_corr.parquet`), UNION of both pair sides, one-row-per-symbol cardinality guard |
| `views.py::v_correlation` | removed |
| `correlation_module.py::run()` | no bridge import or call |

## Operator step after the live mirror

```
mkdir -p data/archive/legacy_correlation_clusters_2026_10
mv data/gold/correlation/correlation_clusters.parquet data/archive/legacy_correlation_clusters_2026_10/
```

Recommended so a missed reader fails loudly instead of using a file that is no longer refreshed.

See `KNOWN_RISKS.md` RISK-31 (update 29 Oct 2026) and `archive/gold_correlation_retirement_2026_09/`
for the original retirement.
