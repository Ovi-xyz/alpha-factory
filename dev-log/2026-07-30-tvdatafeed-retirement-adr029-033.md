# 2026-07-30 — tvdatafeed Retirement (ADR-029) + CPO/RUBBER/TIN/NICKEL Proxy Adoption (ADR-030–033)

**Format note:** continuing the one-file-per-thread dev-log convention.
`CHANGELOG.md` (v1.13.0 entry) is the exhaustive per-FIX technical
record; this file is the narrative companion. `KNOWN_RISKS.md` RISK-1 →
RESOLVED, RISK-15 added.

## Starting state

Implemented `GMI_Decision_Document_v7.docx` §3's 14-item checklist in
full, applied directly to the live repo via the filesystem connector (no
zip package this thread — per standing instruction, changes were made
in place). Did not re-verify the exact prior git commit hash from a
fresh clone (no shell/git access to the actual repo in this session,
only direct file read/write) — all edits were made by reading each
target file's live content immediately before editing it, which is the
empirical-first substitute available in this environment.

## What this thread did

Worked the checklist in dependency order, reading every file before
editing it:

1. **`src/bronze/market_ingester.py`** — `idx_chain`:
   `ChainedAdapter([TvDatafeedAdapter(), YFinanceJKAdapter()])` →
   `ChainedAdapter([YFinanceJKAdapter()])`. `_primary_source_for()` idx
   case → `"yfinance"`. `TvDatafeedAdapter` import removed.
2. **`src/bronze/source_adapter.py`, `yfinance_adapter.py`** —
   docstrings/usage examples updated, no more tvdatafeed→yfinance
   2-adapter chain references.
3. **`src/utils/health_reporter.py::_check_idx_coverage()`** — reworked
   from tvdatafeed-vs-fallback to presence-vs-missing. This was load-
   bearing, not cosmetic: under the old schema, every IDX symbol would
   show as "fallback" the moment tvdatafeed stopped being in the chain
   at all, which would have permanently tripped
   `IDX_COVERAGE_ALERT_THRESHOLD` on every single healthy run. Query
   simplified (`SELECT DISTINCT _symbol`, no more `ROW_NUMBER()`
   per-source resolution).
4. **Archival** (plain moves, no `SystemExit` guard needed — neither
   `TvDatafeedAdapter` nor `TvDatafeedSessionManager` had a destructive
   write path, unlike the RISK-11 migration scripts):
   `src/bronze/tvdatafeed_adapter.py`, `tvdatafeed_session.py`,
   `scripts/preflight/check_tvdatafeed_symbols.py` → `scripts/archive/`.
   `tests/unit/test_tvdatafeed_adapter.py` (28 collected tests),
   `test_tvdatafeed_session.py` (35 collected tests) → also moved to
   `scripts/archive/`, renamed `ARCHIVED_test_*.py` (dropped the `test_`
   prefix so they can never be pytest-collected — confirmed
   `pyproject.toml`'s `testpaths = ["tests"]` already makes this
   unnecessary, kept anyway as cheap defense-in-depth).
   `test_preflight_scripts.py::TestCheckTvdatafeedSymbols` (5 tests)
   removed outright (a class within a file that's otherwise staying).
   `scripts/archive/README.md` extended with a new section distinguishing
   this "no import-time danger, plain move" category from the existing
   "destructive migration script" one.
5. **`pyproject.toml`** — `tvdatafeed` git dependency removed. Version
   `1.12.1` → `1.13.0` (MINOR, folding in a real staleness gap — see
   "Version bump reasoning" below). `[tool.coverage.report] fail_under`
   `70` → `80`, found stale while already in the file: `ci.yml` has
   enforced `--cov-fail-under=80` since the 28 Jul 2026 thread but this
   field was never touched to match.
6. **`config/instruments_identity.yaml` + `instruments_taxonomy.yaml`** —
   CPO, RUBBER, TIN, NICKEL: `context_available`/`include_in_forecast`
   `false` → `true`; `deferred_reason`/`planned_wave` removed;
   `yfinance_symbol`/`proxy_instrument` set to the 4 confirmed proxies
   (F34.SI/STA.BK/AFM.V/NIC.AX). CPO's `requires_fx_normalization`
   `true` → `false` (no longer a raw MYR-denominated commodity feed —
   see "Judgment calls" below).
7. **`KNOWN_RISKS.md`** — RISK-1 title/status → RESOLVED, new
   "Resolution (ADR-029, 30 Jul 2026)" section replacing the now-executed
   "Long-term migration path." RISK-15 (NEW, OPEN) added for an
   incidentally-discovered pre-existing gap (see below).
8. **`CHANGELOG.md`** — new `v1.13.0` entry at the top, consolidating
   this thread's work AND flagging (not fixing) the version-string
   staleness across several prior threads that never bumped
   `pyproject.toml`.
9. **Test count updates**: `test_context_anchors.py`,
   `test_instrument_loader.py`, `test_full_system.py` — every hardcoded
   `55`/`4 deferred`/`695` updated to `59`/`0 deferred`/`699`. All test
   renames were 1:1 (same count, different name/assertion) — see
   `test_deferred_count_is_4` → `test_deferred_count_is_0`,
   `test_deferred_instruments_have_required_fields` →
   `test_no_deferred_instruments_remain`,
   `test_forecast_context_excludes_deferred` →
   `test_forecast_context_now_includes_former_deferred`,
   `test_adr023_only_cpo_is_myr_dependent` →
   `test_adr023_history_superseded_by_adr030_033`,
   `test_resolve_excludes_deferred` →
   `test_resolve_no_instruments_currently_deferred`.
10. **`tests/COUNT_BASELINE.txt`** — `1487` → `1419` (Δ -68, all from
    archival in step 4; zero new tests added this thread).

## Judgment calls made (flagged for review, not silently decided)

- **`proxy_for`/`proxy_correlation_expected` deliberately NOT set** for
  any of the 4 new proxies. Read `scripts/validate_instruments.py`
  *before* deciding this — it hard-requires
  `proxy_correlation_expected` whenever `proxy_for` is present, and no
  empirical correlation analysis exists yet between any of these 4
  proxies and their target commodity (unlike VALE's documented ~0.81).
  Setting `proxy_for` without a real number would have meant either
  fabricating a correlation estimate or breaking Gate G-3 — both wrong.
  This is a real gap (the formal proxy linkage isn't declared), not
  invisible: flagged here and in the CHANGELOG.
- **`base_currency` removed** (not updated) for all 4 — CPO/RUBBER/TIN
  previously had MYR/USD/USD; NICKEL never had one. Followed the
  IRON_ORE/VALE and COAL_NEWC/WHC.AX precedent, where no currency flag
  is set despite those two also trading in non-USD currencies (USD for
  VALE, actually, but AUD for WHC.AX) — treating this as "equity proxy,
  not raw currency-denominated commodity feed" pattern rather than
  updating to the new proxy's own listing currency (SGD/THB/CAD/AUD).
  This is a defensible default, not the only one — flagged in case Ovi
  wants per-instrument currency metadata added later as its own,
  separate enhancement.
- **`config/fred_series.yaml` left untouched.** Discovered while reading
  it that ADR-005/006's own "Track 2" FRED monthly supplements
  (`PIORECRORECUSDM`, `PCOALAUUSDM`) were never actually added to the
  live file despite being decided in Architecture Extension v1.0 — a
  pre-existing gap, unrelated to this thread's scope, found incidentally.
  The 4 new candidate series this thread would imply
  (`PPOILUSDM`/`PRUBBUSDM`/`PTINUSDM`/`PNICKUSDM`) were **not** added
  either — whether `fred_ingester.py` even parses a `commodity` domain
  wasn't verified, and none of the 6 series (2 old + 4 new) have been
  confirmed against live FRED. Filed as `KNOWN_RISKS.md` RISK-15 (OPEN)
  rather than guessed at under time pressure.
- **Version bump: MINOR (1.13.0), not PATCH.** `pyproject.toml`'s
  version string had been stuck at `1.12.1` through ADR-027, GMI v6
  Decision E + G-6 fix, ADR-028, and the 28 Jul preflight-fixes thread —
  none of them bumped it, and the informal `v1.12.2`/`v1.12.3`/`v1.12.4`
  labels only ever existed as zip filenames, never as a committed
  string. Rather than guess which phantom patch digit belongs to which
  prior thread, one clean MINOR jump absorbs the whole catch-up
  honestly. `GMI_Decision_Document_v7.docx` §4 left this as "Ovi's call
  on scope" — this is the call made, stated plainly rather than buried.

## Verification

**What this session could actually do** (no shell/execute access to
Ovi's real Poetry/conda environment — filesystem read/write only):

- Copied every modified `.py` file to a sandbox and ran `ast.parse()` on
  all 10 — clean (Gate G-1 equivalent).
- Grepped the same 10 files for f-string SQL patterns — none introduced
  (Gate G-2 equivalent).
- Parsed both edited YAML files and `pyproject.toml` for syntax
  validity — clean; confirmed `tvdatafeed` absent from dependencies and
  `fail_under: 80` in the parsed TOML.
- **Actually instantiated the real `InstrumentLoader` class** (copied
  `instrument_loader.py` + `yaml_split_merge.py`, ran it against the
  real edited `instruments_identity.yaml`/`instruments_taxonomy.yaml`)
  rather than hand-computing expected counts — confirmed live:
  `count()` 640, `count_context()` 59, `count_context(include_deferred=True)`
  59, `deferred_count()` 0, `count_total()` 699,
  `by_context_group("commodity")` 11, all 4 symbols present in both
  `forecast_context()` and `correlation_context()` with the correct
  `yfinance_symbol`/`is_deferred=False`. Also directly re-ran the exact
  assertion bodies from the rewritten
  `test_no_deferred_instruments_remain`,
  `test_adr023_history_superseded_by_adr030_033`, and
  `test_correlation_context_includes_deferred_excluded_instruments`
  against this real loader instance — all passed.
- Confirmed no leftover references to the removed schema
  (`idx_tvdatafeed_count`/`idx_fallback_count`, `TvDatafeedAdapter`)
  anywhere outside explanatory comments, across all touched files.

**What Ovi then actually ran** (`poetry-logs_v1_13_0.txt`, project
knowledge — the real, authoritative verification this session couldn't
perform itself):

- `poetry lock` — 24.5s, clean resolve. Confirms `tvdatafeed` and its
  transitive deps are genuinely gone from the lockfile, not just from
  `pyproject.toml`'s text.
- `poetry run pytest tests/ -q` — **1 failed, 1418 passed** on the first
  real run. The one failure:
  `TestInstrumentLoaderLayer2::test_is_deferred_property` — asserted
  `tin.is_deferred is True`, a real miss in this session's own search
  (see "Post-hoc correction" below).
- `poetry run pytest tests/ --cov=src --cov-fail-under=80 -q` —
  **81.41%**, comfortably above the 80% gate.
- `python scripts/validate_instruments.py` — **PASSED**: "699 symbols
  (Layer 1=640, Layer 2=59), no errors."
- `python scripts/check_glob_scope.py` (Gate G-8) — **PASSED**: "0
  glob-scope violations in src/."

## Post-hoc correction — `test_is_deferred_property` missed in the original search

The real pytest run above caught something this session's own review
didn't: `test_is_deferred_property` (`TestInstrumentLoaderLayer2`) picks
TIN as its concrete example of a deferred instrument
(`assert tin.is_deferred is True`) to test the `is_deferred` property
mechanism itself — no hardcoded count anywhere in it, so the grep-style
search this session used for the 55/59/4/695/699 pattern never surfaced
it. This is a real gap in the original search method, not a false
alarm.

**Fix, verified against the real `InstrumentLoader` before being written
(not guessed at a second time)**: read `instrument_loader.py`'s
`_load_layer2`/`_build_context_instrument` logic directly to understand
exactly what a synthetic identity/taxonomy pair needs to look like, then
actually ran a synthetic `FAKE_DEFERRED`/`FAKE_ACTIVE` pair through the
real `InstrumentLoader` + `merge_split_trees` in the sandbox and
confirmed `is_deferred` resolved to `True`/`False` correctly *before*
writing anything to the repo. Only then:

- `test_is_deferred_property` → `test_is_deferred_property_false_for_active_instruments`
  (same class, `TestInstrumentLoaderLayer2`) — assertion flipped to
  `False` for both TIN and COPPER, since the `is_deferred==True` branch
  is now dead against real config (zero deferred Layer 2 instruments).
- New test added — `test_is_deferred_property_true_for_deferred_instrument`
  (`TestInstrumentLoaderCoverageGaps`, the class that already exists
  specifically for "branches the real config doesn't currently
  exercise") — synthetic pair keeps both branches of `Instrument.is_deferred`
  covered.

This is a genuine +1 test (not the "zero new tests" this session
originally claimed for the whole thread) — `tests/COUNT_BASELINE.txt`
updated 1419 → **1420**. `CHANGELOG.md`'s v1.13.0 entry amended in place
(not superseded by a new version) with a "Correction" section and
updated intro/verification numbers, since nothing has been tagged or
pushed yet — this is still the same in-progress delivery.

**Not yet done**: the fix above hasn't been run through a real `poetry
run pytest` a second time. Expected 1420 passed / 0 failed based on
arithmetic (1418 + the fixed test + the 1 new test), not empirically
confirmed. Recommend one more real run before treating this as closed.

## What's still open

- **This thread's own new gap**: `KNOWN_RISKS.md` RISK-15 — FRED Track 2
  supplement series (2 pre-existing + 4 new) not in `fred_series.yaml`,
  not verified against live FRED, `fred_ingester.py`'s domain support
  unconfirmed. Needs its own scoped thread.
- **Formal `proxy_for`/`proxy_correlation_expected` linkage** for the 4
  new proxies — needs an actual correlation study against historical
  commodity price data before it can be honestly declared.
- **RUBBER (STA.BK) row-count gap** (3/5 on initial preflight) and
  **TIN (AFM.V) unverified "CIRO trade resumption" headline** — both
  carried over from `GMI_Decision_Document_v7.docx` as post-adoption
  watch items, not blockers.
- Everything already open before this thread and untouched by it: GMI
  Wave 1 Cycle 4 (CrossAssetEngine, not started), Decision C-style
  coverage tranche toward 95% (still not started), BIS API 501/404
  (separate, unresolved), Gate 1 (ADR-017/018 exact Broad Dollar
  weights, still blocked on BIS EER data).

## Deliverable

No zip this thread — all changes applied directly to
`/Users/opi/alpha-factory` via the filesystem connector, per standing
instruction. Nothing has been executed against the real Poetry
environment; see "Verification" above for the exact command sequence to
run before treating this as confirmed.
