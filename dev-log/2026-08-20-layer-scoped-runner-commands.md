# 2026-08-20 — Layer-Scoped Runner Commands: `--job bronze/silver/gold`

**Version**: 1.15.3 → 1.16.0
**Trigger**: Ovi's direct instruction, not a written GMI Decision Document —
"add these commands to runner that can run each layer separately during
live testing": `python src/runner.py --job bronze`, `--job silver`,
`--job gold`.
**Scope**: `src/scheduler/job_registry.py` (`layer_sequence()` +
`LAYER_JOB_NAMES`), `src/runner.py` (`run_layer()`, CLI routing/help
text); 3 test files extended (`tests/unit/test_runner.py`,
`tests/integration/test_job_registry_integrity.py`,
`tests/integration/test_runner_weekly_cadence.py`);
`tests/COUNT_BASELINE.txt`, `CHANGELOG.md`, `pyproject.toml`.

---

## 0. Pre-work verification

- HEAD parity: `.git/refs/heads/main` on live (`5a598f69d55681ea9948620c5337f05996387f95`)
  matched a fresh `git clone` of `github.com/Ovi-xyz/alpha-factory`
  exactly — sandbox was a clean mirror, no manual patching needed.
- Baseline confirmed empirically, not trusted from memory alone: full
  `pytest` run after installing the dependency set — **1613 collected
  (1612 passed, 1 skipped), 0 failed** — exact match to
  `tests/COUNT_BASELINE.txt` and the prior session's recorded figure.
- Read `src/runner.py` and `src/scheduler/job_registry.py` in full from
  the live repo via the Filesystem MCP connector before writing any code,
  to work from exact live bytes rather than sandbox/terminal output.

## 1. Design — Decide phase

Read both files end-to-end first: every `JOB_REGISTRY` entry already
carries a `layer` field (`bronze`/`silver`/`gold`/`util`); `DAILY_SEQUENCE`
and `WEEKLY_SEQUENCE` (the superset — weekly-only jobs + all of
`DAILY_SEQUENCE`) already encode a dependency-respecting order per job.
Three explicit exclusions from those sequences, confirmed by reading the
registry directly rather than assuming: `bronze_finnhub` (deliberate stub,
`raise NotImplementedError`, FIX R-F04), `silver_fundamental` (depends on
`bronze_finnhub`, orphaned per FIX NEW-2), and `bronze_bls_cpi` /
`bronze_bls_nfp` / `bronze_bea_gdp` (registered but never sequenced —
`bronze_macro_weekly` already covers BLS/BEA via its own FRED-mirror
call).

Key decision: derive the three layer job-lists **from `WEEKLY_SEQUENCE`**
at import time (`layer_sequence()` + `LAYER_JOB_NAMES`), not as a second,
hand-maintained list. A hand-copied list would silently drift the moment
a job is added, removed, or re-tagged — exactly the staleness class this
project's own preflight/coverage work (GMI v8/v9) exists to catch.
Consequence, verified rather than assumed: this makes the three
deliberate exclusions above fall out automatically, with zero extra
exclusion logic to maintain.

Second decision, made explicit rather than left implicit: cross-layer
dependency checks are **not** bypassed by the new commands. `--job
silver` run standalone still `sys.exit(1)`s if bronze hasn't run today,
same for `--job gold` after silver, unless `--force` is passed —
identical to how running any single job by name already behaves. This is
the intended staged-testing guard (Bronze → Silver → Gold, GD §17.2 Layer
Independence Guarantee), confirmed with a dedicated test rather than
assumed to "just work" from existing `run_job()` behavior.

Third: `health_report` keeps `layer='util'`, so `--job gold` deliberately
excludes it — the three new commands are scoped to exactly what was
asked for (one layer), not "layer + adjacent utility work."

## 2. Implementation

`src/scheduler/job_registry.py` — added `layer_sequence(layer)` (loops
`WEEKLY_SEQUENCE` once, dedups, filters by `JOB_REGISTRY[name]["layer"]`)
and a precomputed `LAYER_JOB_NAMES` dict (`{"bronze": [...], "silver":
[...], "gold": [...]}`), appended directly after the existing
`PIPELINE_SEQUENCE = DAILY_SEQUENCE` alias.

`src/runner.py` — imported `LAYER_JOB_NAMES`; added `run_layer(layer,
force, run_date)` (unknown layer → `pipeline_logger.error()` +
`sys.exit(1)`; otherwise banners and loops `run_job()` over the layer's
job list, same pattern as the existing `run_all()`); added an `elif
args.job in LAYER_JOB_NAMES:` branch in `main()` ahead of the `"all"`
branch; updated the `--job` help string and the `--help` epilog's
example list.

## 3. Testing

18 new tests, all built and run in the sandbox before touching live:

- `tests/unit/test_runner.py` (+7): `run_layer` importability; `--job
  bronze/silver/gold` argparse acceptance; new `TestRunLayer` class —
  unknown layer raises `SystemExit(1)`; bronze layer calls every
  `LAYER_JOB_NAMES["bronze"]` job in order (stubbed `fn`, `force=True` to
  isolate from the schedule guard, which `TestScheduleGuardInRunner`
  already covers directly elsewhere in the file); silver alone (no
  bronze today) raises `SystemExit`; `health_report` confirmed absent
  from the gold list.
- `tests/integration/test_job_registry_integrity.py` (+9, new
  `TestLayerJobNames` class): exactly three layer keys; every listed
  name exists in `JOB_REGISTRY`; every listed name's own `layer` field
  matches its list; no duplicates within a layer; the three deliberate
  exclusions (`bronze_finnhub`, `silver_fundamental`, the 3 manual
  BLS/BEA jobs) confirmed absent; `util`-layer jobs confirmed absent
  from all three lists; every bronze/silver/gold job that IS in
  `WEEKLY_SEQUENCE` is confirmed present in its matching list (catches
  `layer_sequence()` silently dropping a real job); `LAYER_JOB_NAMES`
  reconfirmed identical to a fresh `layer_sequence()` call; and one test
  specifically constructing a synthetic `WEEKLY_SEQUENCE` with a
  repeated job name via `monkeypatch` to exercise the dedup branch
  directly — the real `WEEKLY_SEQUENCE` has no duplicate today, so this
  branch was otherwise unreachable and would have shown as a coverage
  gap on the modified file.
- `tests/integration/test_runner_weekly_cadence.py` (+3, new
  `TestLayerCommands` class, reusing the file's existing
  `stubbed_registry`/`sandboxed_guard` fixtures): `--job bronze` alone
  completes standalone on a Wednesday (exercises `bronze_eia`'s real
  `run_on_weekdays=[2]` schedule guard, not `force=True`); `--job
  silver` alone on a fresh sentinel dir raises `SystemExit`; the full
  staged workflow — `--job bronze` then `--job silver` then `--job
  gold`, same `run_date`, no `--force` anywhere — completes end-to-end,
  confirming every job across all three layers reaches a `done`
  sentinel.

Full-suite regression after every batch: **1630 passed, 1 skipped, 0
failed** (1613 → 1631 collected, Δ +18). Aggregate coverage: 87.71%
(gate ≥80%, holds the post-v1.15.3 ~88% level).

## 4. Live-repo write discipline

All 8 files (2 source, 3 test, `tests/COUNT_BASELINE.txt`,
`CHANGELOG.md`, `pyproject.toml`) were built and fully verified in the
sandbox first (`ast.parse()` gate, targeted `pytest` runs, then two
full-suite regression passes), then mirrored to the live repo via the
Filesystem MCP connector using `edit_file` calls anchored on bytes read
directly from the live files (not retyped from terminal/sandbox output),
each followed by `Filesystem:get_file_info` byte-count verification and
a full read-back compared against the sandbox source of truth before
moving to the next file.

One self-caught discrepancy: `tests/COUNT_BASELINE.txt` was originally
edited in the sandbox with `echo -n` (no trailing newline), while the
live `edit_file` call preserved the original file's trailing newline —
live came back as 5 bytes (`"1631\n"`), sandbox as 4 (`"1631"`). Caught
by the byte-count comparison step itself; sandbox corrected to match
live (`printf "1631\n"`) rather than the reverse, since live's byte-count
edit had preserved the pre-existing file convention correctly and
sandbox's `echo -n` was the actual error.

## 5. Records updated

- `tests/COUNT_BASELINE.txt`: `1613` → `1631`.
- `CHANGELOG.md`: new `v1.16.0` entry.
- `pyproject.toml`: `1.15.3` → `1.16.0` — **MINOR**: new capability,
  fully backward-compatible, no interface-contract or schema change, no
  existing job's behavior touched.
- `KNOWN_RISKS.md`: not touched — no new open/accepted risk from this
  change; the cross-layer dependency-check behavior is intended design,
  documented in code comments and this log, not a risk.

## 6. What's still open (not this release)

Nothing new opened by this change. Coverage tranche Phase 3–5 (Silver
`quality_validator.py`, Gold modules, orchestration files) remains
exactly where v1.15.3 left it — untouched by this session.
