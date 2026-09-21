# archive/gold_correlation_retirement_2026_09/

FIX GMI-CORR-RETIRE-01 (KNOWN_RISKS.md RISK-31, CHANGELOG.md v1.18.6,
20 September 2026).

## What's here

- `src/gold/correlation_matrix.py` — the pre-Cycle-4 `gold_correlation`
  job: Layer 1-only active-symbol universe, raw Pearson correlation via
  Polars `.corr()`, `sklearn.cluster.AgglomerativeClustering`.
- `tests/unit/test_correlation_matrix_glob_scope.py` — its one dedicated
  test file (4 tests, a narrow glob-scope regression check; the module
  otherwise had no comprehensive unit coverage).

Paths mirror their original location under the repo root so the layout
here is self-explanatory without cross-referencing anything.

## Why archived, not just deleted

The Filesystem MCP connector this project is developed through has no
delete capability (see `tools-and-paths.md`). This directory is the
archive-move convention used for retirements ever since — deliberately
**not** git-tracked (no `.git` history entry for this directory; check
`git status` and it won't show up), matching the Finnhub retirement
(ADR-043, August 2026) precedent rather than the earlier
`scripts/archive/` precedent (RISK-11), which WAS git-tracked, grew its
own "does the archive still exist" regression tests, and was ultimately
deleted outright by Ovi (6–7 August 2026) specifically because a
tracked archive directory turned out to be more maintenance burden than
value — the bytes already persist in git history via every commit that
ever touched these files; keeping a duplicate copy under version control
bought nothing.

Functionally this is equivalent to deletion: neither file is imported,
scheduled, or reachable from any live code path as of this retirement.
The difference is purely that the bytes also persist here, on the local
filesystem, for anyone who wants to glance at the old implementation
without checking out an old git revision.

## Why gold_correlation was retired

`gold_correlation`'s two real consumers (`screener.py`'s cluster-dedup
guard, `views.py`'s `v_correlation` — the documented Trading Engine
Interface Contract) now read a file derived from
`gold_cross_asset_correlation`'s pairwise Ledoit-Wolf output instead
(`src/gold/cross_asset/legacy_correlation_bridge.py`), at the exact same
path this old module used to write to. Full rationale, the empirical
finding that this job had never actually produced live output on this
repo, and the rejected alternatives: `KNOWN_RISKS.md` RISK-31 (20 Sep
2026 update) and `CHANGELOG.md` v1.18.6.

## If you need the old implementation back

`git log --all --follow -- src/gold/correlation_matrix.py` finds every
version of it in history. Restoring it to a live path and re-registering
it in `job_registry.py`/`WEEKLY_SEQUENCE` would be the reverse of this
fix — nothing here prevents that, it just isn't wired up any more.
