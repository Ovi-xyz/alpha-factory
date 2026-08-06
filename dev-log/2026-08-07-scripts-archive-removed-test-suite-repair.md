# 2026-08-07 — `scripts/archive/` Removed Entirely, Test Suite Repaired

**Format note:** one file per release, per this project's own convention
(`dev-log/YYYY-MM-DD-topic.md`, never modified after creation).
`CHANGELOG.md` remains the exhaustive per-FIX technical record; this file
is the narrative companion for **v1.13.5 only**. Continues directly from
the v1.13.4 dev-log entry.

## Starting state

Commit `b013a6a` (6 Aug 2026) shipped the v1.13.4 work (Gate 1 discovery
confirmation, `poetry.lock` fix) and, in the same commit, Ovi separately
deleted `scripts/archive/` entirely — all 9 files, ~3,309 lines,
including `README.md`, both guarded migration scripts
(`migrate_instruments.py`, `build_instruments_v14.py`), the tvdatafeed
adapter/session pair, and the two `ARCHIVED_test_*.py` files. Reported
7 Aug as "G-6 — Coverage Gate failure."

## What this release did

### Diagnosis: not actually a coverage failure

Ran the exact CI-equivalent command
(`pytest tests/ --cov=src --cov-report=term-missing`) against the real
current state (fresh clone of GitHub main, `b013a6a` — already synced,
nothing to overlay this time). Result: **coverage 81.54%**, comfortably
above the 80% gate — `Required test coverage of 80.0% reached` printed
explicitly. The actual failure was **7 failed, 1425 passed**, all 7 in
one file: `tests/unit/test_archived_migration_scripts.py`. Whatever
wrapper reported this as "G-6" most likely does so because the same
combined `pytest --cov` invocation both runs the tests and measures
coverage — a test failure trips the whole command's exit code even
when the coverage number itself clears the bar. Worth being precise
about: this was a G-5 (test pass) problem wearing a G-6 label, not an
actual coverage regression.

### Root cause: a regression guard whose target no longer exists

`test_archived_migration_scripts.py` (RISK-11, 11 tests, added when
`migrate_instruments.py`/`build_instruments_v14.py` were archived —
see KNOWN_RISKS.md) was a **permanent regression guard verifying the
archive's continued existence**: README present, both scripts still
syntactically parseable, a bare `import scripts.archive.X` still
triggering the specific `SystemExit("ARCHIVED ...")` guard rather than
succeeding. With the directory gone, those checks fail exactly as
designed to fail when their precondition becomes false — `AssertionError`
on the missing README, `FileNotFoundError` on `ast.parse()` reading a
file that isn't there, and `ModuleNotFoundError: No module named
'scripts.archive'` instead of the expected `SystemExit` with an
"ARCHIVED" marker string in its output.

**Not a regression to fix by restoring anything.** The bug the whole
file guarded against — destructive import-time writes from either
script — is now structurally impossible, not just disabled: the files
don't exist anywhere, archived or not, so there is nothing left that
could be dangerously imported. Confirmed via full-tree grep that
nothing else in the repo (`src/`, other `scripts/`, other `tests/`)
holds a live import or file-path reference to anything under
`scripts/archive/` — the four other filename matches found
(`market_ingester.py`, `ohlcv_aggregator.py`,
`check_yfinance_tickers.py`, `scripts/validate_instruments.py`) are all
comments/docstrings, not imports, and the `Makefile`'s `migrate` target
never actually executes the archived path — it only `echo`s a pointer
to it and exits 1.

### Fix: retire the 10 obsolete tests, keep the 1 that's still real

Of the file's 11 tests, exactly one still tests something true
regardless of what happened to the archive: `make migrate`'s
muscle-memory safety net (still present in the `Makefile`, still
should fail loudly). Overwrote
`tests/unit/test_archived_migration_scripts.py` in place with just
that one test class (`TestMakefileMigrateTargetFailsLoudly` + its
`_run` helper), then renamed the file to
`tests/unit/test_makefile_safety_nets.py` to match what it actually
tests now. Validated the trimmed content in an isolated sandbox first
(ran it standalone — 1 passed) before touching the live repo.

## Files changed

- `tests/unit/test_archived_migration_scripts.py` → renamed to
  `tests/unit/test_makefile_safety_nets.py`, content reduced from 11
  tests to 1 (the 10 archive-existence checks retired, the Makefile
  safety-net check preserved).
- `tests/COUNT_BASELINE.txt` — 1432 → 1422 (net -10: -11 removed, +1
  kept).
- `pyproject.toml` — version 1.13.4 → 1.13.5 (PATCH: test-suite repair,
  no `src/` or interface change).
- `KNOWN_RISKS.md` — RISK-11: title updated to reflect the archive's
  final state (archived, then fully removed 7 Aug 2026); new "Update —
  7 Aug 2026" subsection added documenting this release in full,
  directly beneath the original 1300-passed verification note (left
  untouched as historical record).
- `CHANGELOG.md` — new v1.13.5 entry.

No `src/` files changed this release — this was pure test-suite
maintenance following up on a deliberate cleanup Ovi had already made.

## Verification

- `tests/unit/test_archived_migration_scripts.py` run in isolation
  first against the real (archive-deleted) state to confirm the exact
  failure mode empirically rather than from static analysis alone:
  **7 failed, 4 passed** — matched the hand-derived prediction exactly
  before any fix was applied.
- Full suite + coverage after the fix:
  **1422 passed, 0 failed.** Coverage: **81.43%** (`TOTAL` line
  4469/830, unchanged from before this release — expected, since
  nothing under `src/` was touched).
- Gates re-run clean: G-1 (156 files, 0 syntax errors — down from 164,
  matches exactly: 8 `.py` files removed from `scripts/archive/`),
  G-2 (0 f-string SQL), G-3 (699 symbols, unaffected), G-8 (0
  glob-scope violations).
- Applied to the live repo via `write_file` (overwrite in place) +
  `move_file` (rename) — the same content already validated in
  sandbox, not re-typed.
- One process note: the first `KNOWN_RISKS.md` edit_file call in this
  release was submitted without an explicit `dryRun` value and
  silently no-opped (default appears to be `true`) — caught by reading
  the file back and grepping for the new heading before assuming it
  had landed, rather than trusting the returned diff alone. Re-ran
  with `dryRun: false` explicit, then re-verified by read-back. Noting
  this because it's a real process gap worth carrying forward: the
  diff `edit_file` returns describes the *proposed* change, not proof
  of a completed write — always confirm via a separate read after any
  edit_file call, dry-run status is not visually distinguishable from
  a completed one in the tool's own output.

## What this does NOT resolve

Nothing new opened by this release. All items already open at the end
of v1.13.4 remain open and untouched: Gate 1's exact weight-value
extraction, `bronze_bis_rates`'s end-to-end live test,
CrossAssetEngine (GMI Wave 1 Cycle 4), RISK-15 (FRED Track 2), the
coverage tranche toward 95%, and the four proxy-correlation studies.

## Deliverable

Applied directly to `/Users/opi/alpha-factory` via the filesystem
connector — no zip.
