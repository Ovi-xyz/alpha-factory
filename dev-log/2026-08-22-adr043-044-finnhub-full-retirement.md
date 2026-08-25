# 2026-08-22 — Finnhub Full Retirement: Sentiment + Earnings/Quotes (ADR-043, ADR-044)

**Version**: 1.16.0 → 1.17.0
**Trigger**: Ovi's direct instruction — "Based on the GMI v10, continue with
implementation phase to finish outstanding issues in sequence" —
implementing `GMI_Decision_Document_v10.docx` Section 3's checklist in full.
**Scope**: `src/scheduler/job_registry.py`, `src/gold/screener.py` (core
ADR-043/044 changes); `src/bronze/finnhub_ingester.py`,
`src/bronze/finnhub_sentiment_ingester.py`,
`src/silver/fundamental_processor.py`, `src/silver/sentiment_processor.py`,
3 `config/schemas/finnhub_*.yaml` files, `scripts/preflight/check_finnhub_shape.py`
(all moved to `archive/finnhub_retirement_2026_08/`, mid-implementation
pivot from the originally-planned outright deletion — see §8); 7 test
files edited, 4 test files moved to the same archive;
`src/scheduler/pipeline_scheduler.py`, `src/utils/rate_limiter.py` (2
additional live consequences found via repo-wide grep sweep, not in v10's
literal checklist); `pyproject.toml`, `poetry.lock`, `.env.example`,
`KNOWN_RISKS.md`, `CHANGELOG.md`, `README.md`, `tests/COUNT_BASELINE.txt`.

---

## 0. Pre-work verification

- Cloned fresh from `github.com/Ovi-xyz/alpha-factory` (`main`,
  `6746c3c`, v1.16.0) into an isolated sandbox rather than trusting the
  live repo sight-unseen.
- Baseline confirmed empirically before any edit: installed `poetry`
  (missing from the bare sandbox — its absence caused 2 unrelated test
  failures on the first run, resolved by installing it rather than by
  touching test code) and re-ran the full suite: **1631 passed, 0
  failed** — exact match to `tests/COUNT_BASELINE.txt`.
- Live-repo parity check performed *before* any mirroring: the
  Filesystem MCP connector's actual allowed directory is
  `/Users/opi/alpha-factory` (confirmed via
  `Filesystem:list_allowed_directories` after an earlier session-turn
  mistakenly queried a *different* MCP server — `PDF Tools:
  get_allowed_directories`, which returned `Documents/Downloads/Desktop`
  and was wrongly taken as authoritative for the Filesystem server too).
  `pyproject.toml`'s header comment and `src/scheduler/job_registry.py`
  / `src/gold/screener.py` byte sizes were compared live-vs-sandbox
  before editing anything — exact match, confirming the sandbox clone
  was a clean mirror of the live repo's actual state.

## 1. Implementation — ADR-043 (job_registry.py)

Removed, not commented out or excluded-from-sequence: `_bronze_finnhub`,
`_bronze_finnhub_sentiment`, `_silver_fundamental`, `_silver_sentiment`
wrapper functions and their 4 `JOB_REGISTRY` entries. `DAILY_SEQUENCE`
16 → 14 (`bronze_finnhub_sentiment`, `silver_sentiment` removed); the
commented-out `silver_fundamental` line in `WEEKLY_SEQUENCE` (marked
"Opsi B, belum diimplementasikan") removed outright — Opsi B is now
closed per ADR-043, not deferred. `gold_screener.depends_on` no longer
references `silver_sentiment`, superseding FIX NEW-2's narrower
`silver_fundamental`-only guard from the prior audit cycle; the
in-registry comment block documenting that history was rewritten to
record ADR-043/044 as the current state rather than leaving stale
NEW-2/Opsi-B narrative in place. `LAYER_JOB_NAMES`'s own module comment
(listing deliberate sequence exclusions) updated to reflect that these
four jobs no longer exist to be excluded, rather than existing-but-
skipped — a distinction ADR-043's own Consequences section calls out
explicitly.

Verified via live import + assertions, not just `ast.parse()`: 23 jobs
remain (from 27), `gold_screener.depends_on ==
['gold_mtf', 'gold_regime', 'gold_sector']`, none of the four retired
job names appear in `JOB_REGISTRY`, `DAILY_SEQUENCE`, `WEEKLY_SEQUENCE`,
or any `LAYER_JOB_NAMES` layer list.

## 2. Implementation — ADR-044 (screener.py)

`_enrich_earnings()` and `_enrich_sentiment()` deleted in full, along
with both call sites in `build_watchlist()` — not left importing a
now-deleted module inside a broad `except Exception`, which ADR-044's
own Rationale identifies as the exact RISK-13 anti-pattern (a broad
except silently converting "module deleted on purpose" and "genuine
future bug" into the identical, indistinguishable no-op). Confirmed
before deleting: all 5 affected output columns
(`days_to_earnings`, `next_earnings_date`, `near_earnings_flag`,
`sentiment_score`, `buzz_score`) were already typed `NULL` placeholders
in `build_watchlist()`'s own main query — the enrichment functions only
ever *overwrote* those placeholders on success, never supplied a column
the main query didn't already declare. No Interface Contract change.

Dead module-level constants `SILVER_SENTIMENT` (already unused before
this fix — never referenced outside its own declaration) and
`SILVER_SENTIMENT_ROOT` (sole consumer was `_enrich_sentiment()`)
removed alongside.

## 3. Repo-wide grep sweep — 2 live consequences found beyond the v10 checklist

Per this project's own "grep sweep after any retirement/deletion is
mandatory" discipline: searched the full repo for `finnhub`,
`fundamental_processor`, `sentiment_processor`, `FundamentalProcessor`
across `src/`, `tests/`, `scripts/`, `config/`. Two genuine, functional
hits beyond what `GMI_Decision_Document_v10.docx` Section 3 enumerated:

1. **`src/scheduler/pipeline_scheduler.py`** (GD §14.5, the dormant
   APScheduler upgrade path — never activated in production, per GD's
   own description). `_make_job()` does a direct `JOB_REGISTRY[name]`
   lookup with no existence guard; the module's own `daily_schedule`
   list still scheduled `bronze_finnhub` (02:30) and `silver_sentiment`
   (03:30) — first activation would have raised `KeyError` immediately.
   Fixed: both removed from the list and from the docstring's cron
   table.
2. **`SourceLimiters.finnhub`** (`src/utils/rate_limiter.py`). Grep
   confirmed zero consumers anywhere in `src/` — unlike
   `.polygon`/`.alphavantage`/`.yfinance`, each of which has a live
   adapter reading it, `.finnhub`'s only conceivable consumers were the
   two Finnhub ingesters just deleted. Removed as a direct mechanical
   consequence of ADR-043, not a separate architectural decision —
   flagged as such in both the code comment and this log, since it
   wasn't literally named in `GMI_Decision_Document_v10.docx`'s
   checklist.

Both required corresponding test updates (`tests/unit/test_rate_limiter.py`;
`pipeline_scheduler.py` itself has no dedicated test file in this repo).

## 4. Test suite reconciliation

Beyond the 4 whole-file deletions and the `TestCheckFinnhubShape` class
removal (`test_preflight_scripts.py`) that v10's checklist named
explicitly, five more files needed changes the checklist didn't (and
couldn't, without the live code reads this session performed) enumerate
in advance:

- **`test_job_registry_integrity.py`**: 4 tests asserting the *old*
  contract ("`silver_fundamental` is registered but deliberately
  unsequenced") replaced with 3 tests asserting the *new* one ("these
  four jobs don't exist in `JOB_REGISTRY`/either sequence at all"). Two
  floor assertions (`len(DAILY_SEQUENCE) >= 15`) failed on first run
  after the `DAILY_SEQUENCE` 16→14 shrink — recalibrated to `>= 13` with
  an explicit comment distinguishing "deliberate, documented removal"
  from the regression a floor assertion is normally guarding against.
- **`test_runner_weekly_cadence.py`** (`GATE-N2`): previously exercised
  `run_job("bronze_finnhub", force=True)` and
  `run_job("silver_fundamental", ...)` directly — both now raise
  `SystemExit(1)` via `runner.py`'s own unknown-job-name guard rather
  than doing anything meaningful. Rewritten: `gold_screener` is now
  satisfied via its actual 3 current dependencies directly; a new test
  confirms all three retired job names fail the same clean way as any
  unrecognized job name.
- **`test_screener.py`**: `TestEnrichEarnings` (3 tests) and
  `TestEnrichSentiment` (4 tests) — direct unit tests of functions that
  no longer exist — removed. Added
  `test_watchlist_schema_stable_without_finnhub_enrichment`, asserting
  against the actual written Parquet output (column presence, exact
  Polars dtype per column, 100% null rate) rather than against
  functions — this is the regression guard ADR-044's own Consequences
  section calls for explicitly.
- **`test_full_system.py`**: one docstring updated
  (`test_l7_pipeline_sequence_comprehensive`) — the floor value (`>= 13`)
  was already correct, but its rationale referenced Opsi B as "not yet
  done" rather than "now closed."
- **`test_preexisting_violations_v1.py`** (found via the grep sweep in
  §3, not named in v10's checklist): two whole classes
  (`TestSILAIO004FundamentalProcessor`, `TestSILAIO004SentimentProcessor`
  — 10 tests) read `fundamental_processor.py`/`sentiment_processor.py`
  source text directly by path, with no existence guard —
  `FileNotFoundError` on first run post-deletion. Removed in full.
  Separately, `TestGlobalAuditClearance`'s `AUDIT_SCOPE_FILES` list (and
  one parametrize list) referenced the same two paths but *did* have an
  `exists()`-based skip guard — these didn't fail, they silently
  skipped (6 tests total: the source of an unexplained "6 skipped" seen
  in one intermediate run this session, resolved once traced to this
  exact mechanism). Removed as stale list entries rather than left to
  skip indefinitely.

## 5. Verification

Full-repo `ast.parse()` sweep (160 files across `src/`, `tests/`,
`scripts/`) — 0 syntax errors. Full `pytest` suite run twice
consecutively for stability: **1510 passed, 0 failed, 0 skipped** both
times (`--collect-only` independently confirms 1510 collected). Net
delta from baseline: 1631 → 1510 (Δ −121) — entirely attributable to the
whole-file deletions, class deletions, and the 6 previously-silently-
skipped `TestGlobalAuditClearance` cases now removed rather than left
skipping; no test that exercised still-live code was deleted or weakened
to force a pass.

## 6. Dependency and version bookkeeping

`finnhub-python` removed from `pyproject.toml` with a dated comment;
`poetry.lock` regenerated and diffed against the pre-edit lock — exactly
1 package removed (`finnhub-python`), 0 added, before applying live.
Sandbox environment's installed `finnhub` package uninstalled to match
(`import finnhub` now fails, as it should). Version bumped **1.16.0 →
1.17.0 (MINOR)** — reasoning recorded in-line in `pyproject.toml`'s own
dated comment block: this is a structural change to `JOB_REGISTRY` and
the Bronze/Silver dependency graph, comparable in scope to GMI-JR-003's
own MINOR bump for a no-schema-change capability *addition* — this is
the inverse (capability *removal*), same magnitude, still no Interface
Contract or Silver/Gold schema change, so PATCH felt too small and MAJOR
unwarranted.

`.env.example`: `FINNHUB_API_KEY` removed outright (not commented out)
— this file's own header states it is reconciled against real
`os.getenv()` call sites in `src/`, confirmed zero remaining via grep,
so the stricter "remove entirely" convention was followed rather than
`tvdatafeed`'s "left as dead, not urgent to scrub" precedent (ADR-029),
since that precedent applied to a file with no such stated
reconciliation policy.

## 7. Records updated

- `tests/COUNT_BASELINE.txt`: `1631` → `1510` (written with trailing
  newline, matching the existing file's own convention — verified by
  byte count, 5 bytes for `"1510\n"`, same shape as the prior `"1631\n"`).
- `CHANGELOG.md`: new `v1.17.0` entry, Indonesian prose, full detail
  per-file. Section header later corrected from `DELETE ADR-043` to
  `ARCHIVE ADR-043` once the archive pivot (§8) landed, with the bullet
  content rewritten to describe moving files rather than `git rm`.
- `pyproject.toml`: `1.16.0` → `1.17.0`.
- `KNOWN_RISKS.md`: RISK-4 title/status changed from "✅ FIXED (dormant,
  hardened)" to "✅ RESOLVED (retired)"; historical sections (schema
  design, the dtype-casting fragility fix, verification counts) kept
  intact as accurate historical record; new "Resolution (ADR-043 +
  ADR-044, 22 Aug 2026)" section added, structured to mirror RISK-1's
  own What-changed/What-this-does-NOT-resolve shape. Its own
  "What changed" bullet was later corrected from "Deleted outright" to
  describe the archive move once the pivot (§8) landed — the original
  wording was accurate at the time it was written, not a mistake to
  hide, but it needed updating once the actual mechanism changed.
- `README.md`: Data Sources table 12→11 (Finnhub row removed, pointer to
  RISK-4 added); "Pipeline Jobs (27 registered)" → "(23 registered)";
  `DAILY_SEQUENCE (16 jobs)` → "(14 jobs)" with the literal job listing
  updated; project-tree counts (`bronze/` 18→16, `silver/` 10→8,
  `config/schemas/` 12→10) and the `check_finnhub_shape.py` /
  `finnhub_ingester.py` / `finnhub_sentiment_ingester.py` tree entries
  removed; Layer Independence Guarantee table's Silver row no longer
  claims "one sanctioned supplement API call (Finnhub sentiment)";
  Environment Variables template's `FINNHUB_API_KEY` line removed.
  One historical roadmap-table cell ("Finnhub schema validation ✅
  Complete (v1.9.0)") deliberately left untouched — accurate record of
  what was true at that release, not a claim about the system today.
  One pre-existing, unrelated staleness noted but deliberately NOT
  fixed here (out of this document's scope): the Data Sources table's
  `tvdatafeed` row still reads "Session-based / IDX primary," which
  predates ADR-029 (`tvdatafeed` retired, `yfinance` `.JK` now sole IDX
  source, per `KNOWN_RISKS.md` RISK-1) — flagged for a future pass, not
  addressed as part of this Finnhub-scoped release.

## 8. Mid-implementation pivot — archive instead of delete

Everything in §0–§7 above describes the plan as originally executed:
`git rm` in the sandbox, mirrored to the live repository via the
Filesystem MCP connector's `edit_file`/`write_file` tools. That worked
for every file that was *modified* — it does not work for a file that
needs to disappear entirely, because the Filesystem MCP server exposed
to this session (14 tools: `read_file`, `read_text_file`,
`read_multiple_files`, `copy_file_user_to_claude`,
`list_allowed_directories`, `list_directory`,
`list_directory_with_sizes`, `create_directory`, `get_file_info`,
`edit_file`, `write_file`, `move_file`, `search_files`,
`directory_tree`) has no delete/remove capability at all — confirmed via
two separate `tool_search` queries before concluding this rather than
assuming it. All 12 whole-file removals (2 Bronze ingesters, 2 Silver
processors, 3 schema YAMLs, 1 preflight script, 4 unit test files) had
been correctly `git rm`'d in the sandbox and were sitting, unremoved, on
the live filesystem while every other file's edits landed successfully.

Ovi's direction, once this was surfaced: use the archive approach
instead of deletion. This is exactly the precedent RISK-1 (`tvdatafeed`,
ADR-029) already used — files moved to `scripts/archive/` rather than
deleted — except that precedent's specific location doesn't fit here
(`scripts/archive/` is scoped to scripts; these 12 files span
`src/bronze/`, `src/silver/`, `config/schemas/`, `scripts/preflight/`,
and `tests/unit/`). No existing top-level archive convention was found
in the live repository to copy directly — `scripts/archive/` itself
does not exist on disk in this repo despite README.md's project-tree
section describing it as if it does (a pre-existing documentation/reality
mismatch, unrelated to this release, not fixed here).

Created `archive/finnhub_retirement_2026_08/` at the repository root,
mirroring each file's original path exactly (`src/bronze/`, `src/silver/`,
`config/schemas/`, `scripts/preflight/`, `tests/unit/` subdirectories) so
provenance stays unambiguous. All 12 files moved via `Filesystem:move_file`
and verified byte-identical against their git blobs via `get_file_info`
size comparison, one at a time, with original modification timestamps
preserved (e.g. `finnhub_ingester.py` retained its `Jul 17 2026` mtime
post-move — direct evidence of a true move, not a rewrite). A dedicated
`README.md` was written for the archive directory: what's there, why
archived rather than deleted (this exact mechanical constraint, stated
plainly rather than dressed up as an architectural preference), what's
deliberately *not* there (files edited in place rather than removed
wholesale — `job_registry.py`, `screener.py`, `pipeline_scheduler.py`,
`rate_limiter.py` — with a pointer to where those changes are
documented instead), and explicit warnings against importing from the
archive, collecting its tests, or treating it as a casual restore path.

The sandbox was updated to match — the same `archive/finnhub_retirement_2026_08/`
tree recreated there via `git show HEAD:<path>` for each file (extracting
the pre-retirement blob directly rather than retyping content), so the
sandbox remains the consistent source of truth for any future session
that clones it fresh, rather than the live repository silently diverging
into a state the sandbox doesn't reflect.

`KNOWN_RISKS.md` RISK-4's "What changed" bullet and `CHANGELOG.md`'s
`DELETE ADR-043` section (renamed `ARCHIVE ADR-043`) were both corrected
to describe the archive move rather than outright deletion — both had
already been written and mirrored to the live repository under the
original "delete outright" plan before this pivot happened, making them
inaccurate the moment the plan changed. Corrected in the sandbox first,
then re-mirrored to live with the same edit-and-verify discipline as
every other file this session.

### A note on tool-transcription reliability, surfaced during this same stretch of work

Separately from the archive pivot, this session also surfaced a real,
repeatable limitation worth recording for future sessions: reliably
reproducing long runs (50+) of an identical Unicode character (the
box-drawing `─`/`═` characters used for comment-section dividers
throughout this codebase) inside an `edit_file` `oldText`/`newText`
parameter failed on the first attempt in three separate files
(`src/gold/screener.py`, `tests/unit/test_screener.py`,
`tests/unit/test_preexisting_violations_v1.py`), each time with a
*different* wrong dash count — not a one-off typo. The working fix in
every case was the same: extract the exact byte range from the live
file directly (via `copy_file_user_to_claude` + a Python script reading
raw bytes), and either (a) use that exact extracted string verbatim with
zero manual retyping, or (b) restructure the edit to avoid needing to
reproduce the dash run at all — matching only a short, dash-free unique
substring instead, which this session confirmed `edit_file` supports
(it performs substring matching, not whole-line-only matching, contrary
to what its own tool description's phrase "line sequences" might
suggest). Two files (`test_screener.py`, `test_preexisting_violations_v1.py`)
ended up with a deliberately simplified comment (ASCII `--` instead of
an em dash in one spot; one consolidated header line instead of a
multi-line explanation in another; one fewer decorative divider) rather
than pixel-perfect parity with the sandbox's prose, once it became clear
that chasing exact parity was consuming disproportionate effort for a
comment-only, zero-functional-impact difference. Both are noted inline
in this log and were a deliberate, bounded trade-off, not an oversight.

## 9. What's still open (not this release)

Everything `GMI_Decision_Document_v11.docx` named remains exactly where
it was — ADR-045 (Bronze OHLCV timeframe partition fix), ADR-046 (MTF
score coverage repair path — still awaiting Ovi's choice among Path
A/B/C), and ADR-047 (AU/AG ticker resolution) are unrelated to and
unaffected by this release. The README `tvdatafeed` staleness noted in
§7 above is new-found but out of scope for a Finnhub-titled release.
