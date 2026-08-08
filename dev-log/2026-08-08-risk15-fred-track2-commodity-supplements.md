# 2026-08-08 — RISK-15: FRED Track 2 Commodity Supplements Resolved

**Version:** 1.13.5 -> 1.14.0 (MINOR)
**Trigger:** Ovi: "Let's resolve RISK-15 -- FRED Track 2 commodity supplements" (explicit Decide-phase signal, following an Explore-phase status report covering three open items -- Gate 1 BIS weight extraction, proxy correlation studies, and RISK-15 -- with RISK-15 recommended first on the grounds that the proxy correlation studies are downstream of it).
**Baseline:** v1.13.5, 1422 passed / 0 failed, coverage 81.43%, Gates G-1 through G-8 clean (confirmed via fresh dev-log read at the start of the Explore phase, not assumed from memory).

## What RISK-15 was

`config/fred_series.yaml` had no `commodity` domain at all. Architecture
Extension v1.0 ADR-005/006 had decided two FRED Track 2 series
(`PIORECRORECUSDM` for Iron Ore, `PCOALAUUSDM` for Coal Australia) as
monthly supplements to the daily equity proxies (VALE, WHC.AX) -- neither
was ever actually added to the file. The 30 Jul 2026 ADR-030-033 thread
(CPO/RUBBER/TIN/NICKEL proxy adoption) found this gap incidentally,
identified 4 more candidate series of the same shape (`PPOILUSDM`,
`PRUBBUSDM`, `PTINUSDM`, `PNICKUSDM`), and deliberately flagged the whole
thing as RISK-15 rather than fixing it in that pass -- out of scope for a
tvdatafeed-retirement thread, and none of the 6 series had been verified
against live FRED.

## What was done, in order

1. **Explored, not assumed.** Read `scripts/preflight/check_bis_eer_weights.py`
   (Gate 1 code), `KNOWN_RISKS.md` (RISK-15's own suggested next steps),
   `src/bronze/fred_ingester.py`, `config/schemas/fred_macro.yaml`, and
   `src/silver/macro_processor.py` directly before writing anything, to
   answer RISK-15's own three open questions empirically:
   - Does `fred_ingester.py` need domain-parsing logic for `commodity`?
     **No** -- `domain` is only used to build the Bronze output path.
   - Does the SchemaValidator need a commodity-specific schema? **No** --
     `fred_macro.yaml` is 4 generic columns, no per-domain branching.
   - Does `macro_processor.py` need updating? **No** -- it globs
     `data/bronze/macro/fred/**/*.parquet` as one pass regardless of
     per-series domain.
   This closes as a config-only addition -- confirmed, not assumed.

2. **Web-verified all 6 candidate series against live FRED** (search +
   fetch against `fred.stlouisfed.org`'s own series pages -- this
   sandbox's bash tool has no route to `api.stlouisfed.org`, same as
   every other preflight script in this project). Found a real bug in
   the process: Iron Ore's actual series ID is **`PIORECRUSDM`**, not
   **`PIORECRORECUSDM`** -- the ID both Architecture Extension v1.0
   ADR-005 and this project's own KNOWN_RISKS.md RISK-15 entry had been
   citing, unverified, since 25 Jun 2026. `PIORECRORECUSDM` does not
   exist on FRED (a duplicated "ORE": `PIORE-CR-ORE-C-USDM` vs the real
   `PIORE-CR-USDM`). The other 5 (`PCOALAUUSDM`, `PPOILUSDM`,
   `PRUBBUSDM`, `PTINUSDM`, `PNICKUSDM`) were confirmed correct as
   documented.

3. **`config/fred_series.yaml`** -- new `commodity` domain section, all 6
   series (corrected `PIORECRUSDM`), `regime_input: false` on all six (no
   macro-regime consumer -- Track 2 is a future `ForecastModule` input,
   GMI Wave 1 Cycle 4 / CrossAssetEngine not yet built). Stale header
   comment ("60 series") corrected to 67 in the same pass -- already off
   by one (VIXCLS never recounted when added) before this change.
   Sandbox-validated first: merged the addition against a real copy of
   the live file and re-parsed with `pyyaml` (67 total series, 0
   duplicate IDs) before touching the live file.

4. **`src/bronze/fred_ingester.py`** -- `RELEASE_LAG_DAYS` entries added
   for all 6 new series, 25 days each. Grounded, not guessed: each
   series' `fred.stlouisfed.org` page showed "Updated: Mar 24, 2026" for
   Feb-2026 period data, consistently, across all 6 -- a ~24-day
   real-world lag. 25d matches the existing PPI-series precedent
   (`PPIFIS`/`PPIFGS`/`PPIACO`) exactly and rounds up slightly, staying
   on the conservative side of this dict's own stated design intent.

5. **`scripts/preflight/check_fred_commodity_series.py`** -- new script
   (none of the 4 existing scripts in that directory covered FRED --
   confirmed via directory listing before writing, not assumed).
   Mirrors `check_bis_cbpol_d.py`'s structure exactly: independent
   `EXPECTED_COMMODITY_SERIES` dict (duplicated, not imported, matching
   `check_bis_cbpol_d.py`'s own `EXPECTED_REF_AREAS` independence
   rationale), `--series` filter flag, PASS/FAIL per item, exit 0/1.
   10 new tests (`TestCheckFredCommoditySeries`,
   `tests/unit/test_preflight_scripts.py`) -- built and run standalone
   in an isolated sandbox first (10/10 passed), then merged against a
   full reconstruction of the real `test_preflight_scripts.py` (real
   header + real existing 4 classes' worth of content + this append) and
   collected with `pytest --collect-only`: **42 total tests collected, 0
   collection errors** -- the specific failure mode this project's CI/CD
   Ops Guide (Gate G-4) exists to catch (NEW-4's lesson: pass/fail alone
   doesn't catch collection breakage). The 32 non-mine "failures" in that
   merge check were `ModuleNotFoundError` for the 3 dependency preflight
   scripts I deliberately didn't replicate in the scratch check (not a
   real regression -- those scripts are untouched and sit right next to
   their tests in the actual live repo).

6. **`KNOWN_RISKS.md`** -- RISK-15 section rewritten in full: status ->
   RESOLVED, "What was found while closing it" section documenting the
   `PIORECRUSDM` catch, original "Why not fixed then" content preserved
   verbatim for audit trail rather than deleted. Also found and fixed an
   adjacent gap while in this file: the document-level "Last updated: vX"
   rolling footer was still showing **v1.13.4** as its most recent entry
   -- v1.13.5's own dev-log (`scripts/archive/` removal, 1432->1422
   tests) had updated RISK-11's own section but never added its own line
   to this footer. Added a "Prior entry: v1.13.5" summary (grounded by
   re-reading that dev-log fresh, not from memory of reading it earlier
   this same thread) before inserting the new v1.14.0 entry at the top.

7. **`pyproject.toml`** -- version bumped 1.13.5 -> **1.14.0**. MINOR, not
   PATCH -- a judgment call, stated explicitly rather than made silently:
   this activates real new data flow into Bronze the next time
   `bronze_macro_weekly` runs (6 new series will actually be fetched),
   which reads as "new indicator" under this project's own CI/CD Ops
   Guide versioning semantics table, even though no live Gold-layer
   consumer exists yet for Track 2 data.

8. **`tests/COUNT_BASELINE.txt`** -- 1422 -> 1432 (+10, matching the new
   test class exactly).

## Process note: caught and fixed my own date error

Several early comments in this session's first few file writes (config/
fred_series.yaml, fred_ingester.py, check_fred_commodity_series.py) used
"7 Aug 2026" -- anchored off the most recently-read dev-log's date
(2026-08-07) rather than checking the actual current date. Caught via an
explicit correction, verified against the system clock (2026-08-08,
Saturday). Fixed across all 3 affected files via targeted `edit_file`
calls before any further work built on top of the wrong date, each
closed-loop re-verified (fresh `copy_file_user_to_claude`, not a stale
sandbox cache) to confirm the fix actually landed and nothing else
regressed in the process.

## Verification discipline followed throughout

Every file write in this session followed the same loop: build and
validate in the sandbox first (yaml/`ast.parse`, and for the two new test
files, an actual offline `pytest` run) -> `edit_file` with `dryRun: true`
and read the returned diff before assuming it's correct -> `dryRun: false`
to apply -> `copy_file_user_to_claude` fresh (not reused from an earlier
point in the session) -> diff against the sandbox-validated version to
confirm byte-identical. One early `str_replace` call was misdirected at
this sandbox's own filesystem instead of the Filesystem MCP connector
(caught immediately via the tool's own "File not found" error, since
`/Users/opi/alpha-factory` doesn't exist on this machine) -- corrected by
switching to `Filesystem:edit_file`, the tool that actually reaches
Ovi's machine.

## What this does NOT resolve

- **Not yet run for real**: `check_fred_commodity_series.py` against live
  FRED with a real `FRED_API_KEY`, and a full `poetry run pytest` on
  real hardware. Everything above is offline-verified (syntax, parsing,
  mocked-network test logic) -- the actual live-API confirmation and the
  real test-suite run both need to happen on the M1, same
  authoring/execution split every other preflight script in this
  project already carries.
- **No live consumer for Track 2 data yet** -- `ForecastModule` /
  CrossAssetEngine (GMI Wave 1 Cycle 4) is still unstarted. These 6
  series will start accumulating in Bronze on the next
  `bronze_macro_weekly` run, with nothing downstream reading them yet.
  This was a known, accepted characteristic of the original ADR-005/006
  design (Track 2 was always meant to precede its consumer), not a new
  gap introduced here.
- **Gate 1 (BIS weight extraction) and the proxy correlation studies**
  (F34.SI/STA.BK/AFM.V/NIC.AX vs. CPO/RUBBER/TIN/NICKEL) remain open,
  per the originally proposed sequencing -- RISK-15 was scoped as
  step 1 of 3, specifically because the correlation studies need a
  commodity price benchmark to correlate the proxies against, and these
  6 series are exactly that benchmark, now available for the first time.

## Next immediate action

Run `python scripts/preflight/check_fred_commodity_series.py` (requires
`FRED_API_KEY`, already in `.env`) on the M1 to confirm all 6 series live
end-to-end, then a full `poetry run pytest` to confirm 1432/1432 for
real. After that: Gate 1 (write the US-row extraction function -- file
located and layout fully characterized already, just the targeted
extraction pass remains) or the proxy correlation studies (now unblocked
-- FRED benchmark series exist), per whichever Ovi prioritizes next.
