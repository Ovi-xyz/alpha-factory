# 2026-11-08 — Active symbols: one dated artifact, legacy `active_{date}.parquet` retired (GMI-AS-002)

**Request.** `active{date}.parquet` and `active_ohlcv{date}.parquet` in `data/silver/active_symbols/` hold redundant
information; if the first is a legacy, substitute it entirely with the second. Evaluate, then apply directly.
Implemented against GitHub `main` (shallow clone); the four source files changed were byte-identical to the live repo
before editing. No version bump.

## Explore
Read `active_symbols.py` end to end, then grepped `src/`, `scripts/`, `config/`, `tests/`, README for the legacy
spelling in every form (f-string, glob, literal). Listed the live directory: three pairs (16-18 Oct), identical sizes
pairwise, no stray `active_context_*` files. Baseline from a separate pristine clone BEFORE any edit, dependencies
pinned from `poetry.lock`: 3398 collected / 3396 passed / 2 environmental failures.

## Findings
1. The two files are the same frame written in a loop (`_save` and `_save_fallback`): redundant by construction.
2. Real consumers of the legacy name: screener, `v_active_symbols`, dashboard probe, resolver `load()`/`load_full()`.
3. **The redundancy was already a bug:** the view glob `active_*.parquet` also matches `active_ohlcv_*.parquet` and
   counted every row twice.
4. `context_anchors.py` documents that the legacy file was kept for "real consumers"; that is no longer true.
5. The filename was built in six places inside one module; that repetition is how a twin file drifts.

## Decide
- Retire the twin; do not delete anything automatically. A resolver that removes files is a different risk class.
- `load()` stays as an alias (cannot rule out external callers); `load_full()` is the only loader that returns the
  audit columns, so it stays and reads the canonical file.
- One filename builder (`_canonical_path`), one atomic writer (`_write_parquet_atomic`).
- Existing files: an offline, dry-run-by-default script, twin-identity verified, move not delete.
- Not done on purpose: removing `load()`, touching `membership_log`, any live-data step, `union_by_name` on the view.

## Built
See the CHANGELOG entry. New: `tests/unit/test_active_symbols_single_artifact_gmi_as_002.py` (14),
`tests/unit/test_archive_legacy_active_symbols_gmi_as_002.py` (16),
`scripts/maintenance/archive_legacy_active_symbols.py`. Changed: `active_symbols.py`, `screener.py`, `views.py`,
`pipeline_dashboard.py`, `context_anchors.py` (docstring), four existing test files.

## Verification
- First targeted run after the refactor failed one test: `TestAS9AtomicWrite` inspects `_save`'s source. Re-pointed at
  the shared writer and extended to pin `_save_fallback` (it had no pin). My error is logged in `mistakes.md`.
- First isolation test for the script scanned for its file name as text and tripped on my own docstring pointer;
  replaced by an import scan with a self-test.
- 19 mutants on a throw-away copy: 19 killed, control green. Full suite on an isolated snapshot: 3428 collected /
  3426 passed / the same 2 failures as the pristine baseline.

## Operator runbook (after the live mirror)
```
python -m pytest tests/ -q                                                     # expect 3428 collected
python scripts/maintenance/archive_legacy_active_symbols.py                    # dry run; expect 3 identical, 0 left
python scripts/maintenance/archive_legacy_active_symbols.py --apply
```
No pipeline job needs a re-run. The next `silver_active_symbols` run writes only `active_ohlcv_{date}.parquet`.
