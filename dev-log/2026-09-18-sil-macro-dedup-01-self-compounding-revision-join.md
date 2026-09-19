# 2026-09-18 — FIX SIL-MACRO-DEDUP-01: silver_macro Self-Compounding Revision Join

**Version**: 1.18.2 → 1.18.3
**Trigger**: Ovi ran `python src/runner.py --job gold_forecast --force`
following up on FIX GMI-FORECAST-DIM-01 and reported "the command
returned with warning" — traced to `silver` job being OOM-killed
(`zsh: killed`) on 2026-09-18, and separately flagged
`fred_2026-09-17_silver.parquet` at **105,813,947 rows**. Two log files
provided (`2026-09-18-job-silver-running-log.txt`,
`2026-09-18-job-gold-forecast-running-log.txt`). Asked to "evaluate the
current cross-asset engine state" implicitly by reporting the symptom;
after root-cause investigation was presented, Ovi authorized explicitly:
*"continue with the code fix and delete already-corrupted files on
disk."*
**Scope**: 1 source file (`macro_processor.py`), 1 paired test file,
`tests/COUNT_BASELINE.txt`, `pyproject.toml`, `CHANGELOG.md`, this
dev-log, plus remediation of 38 corrupted production data files (see §5
— quarantined, not deleted; no delete tool was available).

---

## 0. Live symptom

```
silver_macro:    [04:54:44]  RUNNING  silver_macro
                 zsh: killed     caffeinate python src/runner.py --job silver

gold_forecast:   [06:39:50]  WARNING  active_ohlcv not resolved for 2026-09-18.
                             Run silver_active_symbols job first. -- skipping
                 [06:39:50]  SUCCESS  gold_forecast  (0.9s)
```

`gold_forecast --force`'s own behavior here was correct, not a bug —
`_resolve_followers()` is designed to skip gracefully when
`silver_active_symbols` hasn't produced a file for the run date, which
is exactly what happened once `silver` died upstream before reaching
that job. The actual problem was one layer up: why did `silver_macro`
OOM an 8GB M1 outright.

## 1. Root-cause investigation (empirical, read the actual data first)

Copied `fred_2026-09-17_silver.parquet` into the sandbox via
`copy_file_user_to_claude` and queried it directly with DuckDB — never
inferred from Ovi's reported row count alone.

- `SELECT COUNT(*)`: confirmed 105,813,947, matching exactly.
- Schema: `series_id`, `_series_id` (duplicate metadata column, harmless
  here), `observation_date`, `value`, `release_date`, `vintage_date`
  (only 1 distinct value — makes sense, stamped once per run),
  `_ingested_at` (446 distinct values), `is_revision`, `revision_seq`.
- `COUNT(DISTINCT series_id, observation_date)`: **180,797** unique
  keys — the 105.8M rows are almost entirely duplication, not distinct
  data.
- Per-key row counts, ranked: `MORTGAGE30US` / `2026-09-03` alone —
  **98,825,160** rows (93% of the entire file). `MORTGAGE30US` /
  `2026-08-27` — exactly **1,048,576 = 2²⁰**. Twelve *unrelated* series
  (`CPIAUCSL`, `CPILFESL`, `GDPC1`, `HOUST`, `INDPRO`, `PCOALAUUSDM`,
  `EXHOSLUSM495S`, `A191RL1Q225SBEA`, `PPIACO`, `PERMIT`, `PNICKUSDM`,
  `PIORECRUSDM`) sharing the identical **248,832** on their own
  respective dates.

That third finding was the tell: linear accumulation (e.g. "every
historical Bronze ingestion run got concatenated, never deduped")
would give each key a duplicate count proportional to however many
times its own source was ever ingested — not the exact same number
shared across a dozen otherwise-unrelated series. A shared, identical
multiplication factor across series onboarded together is the
signature of **exponential, day-by-day compounding**: series onboarded
on the same historical date have been through the same number of
compounding cycles since, independent of which series they are.

Ruled out Bronze as the source directly rather than assuming: listed
`data/bronze/macro/fred/monetary_policy/` — `MORTGAGE30US` has exactly
17 files, 2026-08-20 through today, each ~1.8KB (one small incremental
fetch per day, correct `IncFetchProtocol` behavior per Supplementary
Design G1). Bronze itself has never held anywhere near a million rows
for anything.

Read `macro_processor.py` in full. Found it in `_detect_revisions()`:

```python
prev = pl.scan_parquet(str(prev_path)).collect()...
joined = df.join(prev, on=["series_id", "observation_date"], how="left")
```

`prev_path` is yesterday's Silver output. A LEFT JOIN's row count per
key is `count(df matches) × count(prev matches)`. `prev` was itself
produced by this exact same unguarded join the day before (and *its*
`prev` the day before that) — so every day did not add to the
corruption, it **squared** it. Root duplication seed: `_process_domain()`'s
Bronze read (`SELECT * FROM read_parquet($glob, ...)`) has no dedup at
all, and Bronze's incremental-fetch overlap (7-day lookback re-fetch
for idempotency, Supplementary Design G1) legitimately writes the same
`(series_id, observation_date)` key into multiple Bronze files for the
most recent ~week of dates — a handful of genuine raw duplicates per
day, which the join then compounds exponentially over time.

Confirmed this wasn't FRED-specific by checking file sizes for the
other three domains sharing the same `_process_domain()` code path:
`bls_2026-09-17` (8.91MB) vs `bls_2026-09-16` (2.25MB) ≈ 4×;
`bea_2026-09-17` (3.11MB) vs `bea_2026-09-16` (804KB) ≈ 4×;
`eia_2026-09-17` (570KB) vs `eia_2026-09-16` (238KB) ≈ 2.4× (smaller
multiplier, consistent with FIX EIA-6 only landing 8/9 Sep — that
chain has had fewer days to compound).

## 2. Decision

Ovi's instruction was explicit and two-part: fix the code, and remove
the corrupted files. No alternative options were presented here (unlike
FIX GMI-FORECAST-DIM-01) — the root cause and correct remediation shape
were unambiguous once confirmed empirically, so the diagnosis message
went straight to "here's the fix, here's what needs deleting, confirm
before I touch production data" rather than a multi-option trade-off
discussion.

## 3. Implementation

`macro_processor.py`, three parts, all documented in a new module
docstring section:

1. **`_process_domain()`** dedupes `df` on `(series_id,
   observation_date)` immediately after the Bronze read: sorts by
   `_ingested_at` (when present) and keeps the last row per key —
   latest ingestion wins. Falls back to a plain last-row-wins dedup
   when `_ingested_at` isn't in the frame at all (kept the 7 existing
   unit tests' minimal fixtures, which don't set that column, working
   unchanged rather than forcing them to add it).
2. **`_detect_revisions()`** applies the identical dedup to `prev`
   before joining — defense in depth, not the primary fix. Once (1)
   holds, freshly-written Silver can never contain duplicate keys
   again; this only matters for a `prev_path` that somehow still
   points at pre-fix data.
3. New extracted static method **`_join_with_guard(df, prev, source)`**
   — does the join, then checks `len(joined) > len(df)`. Mathematically
   unreachable through the normal `_detect_revisions()` path once (1)
   and (2) both hold (a LEFT JOIN against a key-unique right side is
   row-count-preserving by construction) — but it's the thing that
   actually stops the compounding *mechanism* from ever recurring
   under some future change neither dedup anticipated, rather than
   trusting both to stay correct forever. Extracted as its own method
   specifically so this guard is unit-testable in isolation (see §4) —
   inlined, there would be no way to exercise its trip condition once
   the surrounding dedups are correct.

## 4. Tests

6 new tests in `test_macro_processor.py`, all confirmed to fail against
the pre-fix source (`git stash`) before passing against the fix —
including a genuine mid-implementation correction: the circuit-breaker
test was initially written to call `_detect_revisions()` end-to-end
with a pre-duplicated `prev` file on disk, expecting the breaker to
trip. It didn't — because `_detect_revisions()`'s own new prev-dedup
(part 2 above) absorbs the duplication *before* the join runs, so
`joined.height` never exceeds `df.height` through that path anymore.
That's correct behavior, not a test bug to route around: the guard's
trip condition is genuinely unreachable end-to-end once both dedups
hold, which is the whole point of having them. Rewrote the test class
to call `_join_with_guard()` directly instead, deliberately bypassing
`_detect_revisions()`'s own dedup to isolate the guard as its own
unit — this is *why* it was extracted as a separate method rather than
inlined.

- `TestBronzeDedupBeforeRevisionJoin` (3 tests): 3 overlapping Bronze
  rows for one key collapse to exactly 1 Silver row, keeping the
  latest `_ingested_at` value (not first-read, not merged); running
  `process_fred()` twice in a row with Bronze overlap present each
  time does not multiply row counts across the two runs (the actual
  production mechanism, reproduced at small scale); latest-wins
  ordering is correct even when the latest `_ingested_at` row isn't
  first in the file.
- `TestRevisionJoinCircuitBreaker` (3 tests, rewritten mid-session per
  above): a genuinely duplicate-keyed `prev` passed directly to
  `_join_with_guard()` trips it (`None`); a clean key-unique `prev`
  does not; the full `_detect_revisions()` path, given an on-disk
  `prev` file with the exact pre-fix duplicate shape, absorbs it via
  its own dedup and still correctly detects the genuine revision
  underneath (5.30 vs 5.25) rather than masking it.

Full suite: 1725 collected (1719 → +6), 1723 passed, 0 regressions (2
pre-existing `test_check_poetry_env.py` failures, environment-only,
re-confirmed unrelated).

## 5. Data remediation — quarantined, not deleted

Ovi's instruction said "delete." No delete tool exists on the
Filesystem MCP connector available in this session (`move_file`,
`create_directory`, `write_file`, `list_directory`,
`list_directory_with_sizes` only) — flagging this plainly rather than
working around it silently. Created
`data/silver/macro_enriched/_quarantine_sil_macro_dedup_01/` and moved
all 38 affected files into it via `move_file`:

- `bea_2026-09-{08..17}_silver.parquet` (10 files)
- `bls_2026-09-{08..17}_silver.parquet` (10 files)
- `eia_2026-09-{10..17}_silver.parquet` (8 files — no earlier EIA
  files existed)
- `fred_2026-09-{08..17}_silver.parquet` (10 files)

Deleted the full range for each source rather than trying to identify
a single "first corrupted" date — `bea`'s size curve, for instance, is
nearly flat for 09-08 through 09-11 (~6KB each) before visibly
compounding from 09-12 onward, but a small compounding factor can take
several generations to become visually obvious, so "looks flat" is not
the same as "verified clean." Given the total remediated size was
37.59MB (trivial), the safe choice was the full range, not a guess at
where visible growth starts.

Functionally equivalent to deletion for the pipeline's own purposes:
`_find_latest_silver()`'s glob (`SILVER_MACRO_PATH.glob(f"{source}_*_
silver.parquet")`) is non-recursive, so it will not descend into the
quarantine subdirectory — the next `silver_macro` run will find
`prev_path is None` for all four domains and take the clean
initial-release path. Quarantining rather than deleting is arguably the
more correct choice regardless of tooling: it preserves an audit trail
consistent with this codebase's own conventions (`KNOWN_RISKS.md`,
structural break registries, dev-logs) rather than irreversibly
destroying the evidence of a production incident. Confirmed post-move:
`data/silver/macro_enriched/` contains only `.DS_Store` and the
quarantine directory; the quarantine directory contains exactly the 38
files listed above.

## 6. Sandbox → live mirror

Continued in the same sandbox clone as FIX GMI-FORECAST-DIM-01
(uncommitted changes from that fix were still present locally;
`git status` confirmed before starting). Implemented and tested
entirely in the clone first, same as always. Mirrored to the live Mac:

- `src/silver/macro_processor.py` — `write_file`, byte-identical after
  `copy_file_user_to_claude` read-back.
- `tests/unit/test_macro_processor.py` — `write_file`, byte-identical.
- `CHANGELOG.md` — targeted `edit_file` (anchor: the existing
  `## v1.18.2` header), byte-identical.
- `pyproject.toml` — targeted `edit_file` (anchor: `version =
  "1.18.2"\n# FIX GMI-FORECAST-DIM-01`), byte-identical, re-verified
  as valid TOML on the live file specifically (not just the sandbox
  copy) via `tomllib.load()`.
- `tests/COUNT_BASELINE.txt` — `write_file`, byte-identical.

## 7. Not done / explicitly out of scope this pass

- **No investigation of whether other Silver domains (macro or
  otherwise) share a similar unguarded-join pattern.**
  `_detect_revisions()` was specific to `macro_processor.py`; whether
  `ohlcv_processor.py` or other Silver modules have an analogous
  "join against yesterday's own output" step was not checked. Worth a
  deliberate audit pass, not assumed safe by proximity to this fix.
- **`CURRENT_SILVER_VERSION` was not bumped.** The dedup fix doesn't
  change the schema or the semantic meaning of any correctly-computed
  row — it removes duplicates that should never have existed. A
  version bump exists in this codebase to trigger delta-reprocessing
  of *stale* rows (GD §13.3); there's nothing to reprocess here, only
  quarantine-and-regenerate, which was done directly.
- **The 38 quarantined files still sit on Ovi's disk** (37.59MB,
  trivial) rather than being permanently removed — his call whether to
  actually delete them himself, keep them as incident evidence, or
  something else.
