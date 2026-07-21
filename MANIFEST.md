# Changed files — ADR-026 (v1.11.0 → v1.11.1)

Contains only what changed. Not a full repo copy — see
GMI_Implementation_Checkpoint_v6.docx for the full narrative and
verification results (1214/1214 tests, all gates pass, verified via
independent fresh extraction).

## New files (3) — copy into the repo at these exact paths

- poetry.toml
- scripts/check_poetry_env.py
- tests/unit/test_check_poetry_env.py

## Modified files (5) — replace the existing file, or apply CHANGES.diff

- CHANGELOG.md
- Makefile
- README.md
- pyproject.toml
- tests/COUNT_BASELINE.txt

## To apply

Option A — copy files directly (simplest):
Copy all 8 files from this package into the repo root, preserving the
folder structure shown above (overwrites the 5 modified files, adds the
3 new ones).

Option B — apply the diff for the 5 modified files:
    cd alpha-factory/
    patch -p1 < CHANGES.diff
Then copy the 3 new files in separately (a diff can't create files that
don't exist in your working tree in a way `patch` handles as cleanly as
a plain copy — Option A is more reliable for the 3 new files).

## Verified against

Live main confirmed at commit 0048382 (v1.11.0) via `git ls-remote`
this thread. This package's "before" state (CHANGES.diff, `a/`) is a
fresh clone of that exact commit — not assumed, checked directly.

## Not included (deliberately)

`data/health/*.db`, `data/health/*.pkl` — runtime artifacts regenerated
by running the test suite in the sandbox this thread. Already covered
by `.gitignore` (added in Checkpoint v5); not a source change, not part
of this delta.
