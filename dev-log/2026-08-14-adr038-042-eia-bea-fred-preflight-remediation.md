# 2026-08-14 — ADR-038–042: Data Source Preflight Remediation (EIA APIv2, BEA Table/Line Corrections, FRED Registry Hygiene)

**Version**: 1.15.1 → 1.15.2
**Trigger**: Ovi directed implementation of ADR-038 through ADR-042, decided (not implemented) in
`GMI_Decision_Document_v9.docx` (14 Aug 2026), "continue with implementation phase to resolve the
issues inside alpha-factory sequentially."
**Scope**: `config/fred_series.yaml`, `config/schemas/eia_oil.yaml`, `src/bronze/eia_ingester.py`,
`src/bronze/bea_ingester.py`, `src/bronze/fred_ingester.py`, `src/bronze/bls_ingester.py`,
`scripts/preflight/check_fred_series.py`, `scripts/preflight/check_eia_series.py`,
`scripts/preflight/check_bea_datasets.py`, 3 test files (1 new, 2 updated),
`tests/COUNT_BASELINE.txt`, `KNOWN_RISKS.md`, `CHANGELOG.md`, `pyproject.toml`.

---

## 0. Exploration before any code was written

Empirical-first, per house convention. Read the live repo (both via Filesystem MCP against
`/Users/opi/alpha-factory` and a fresh `git clone` of `github.com/Ovi-xyz/alpha-factory` for
isolated sandbox validation) before trusting `GMI_Decision_Document_v9.docx` alone:

- `pyproject.toml` (1.15.1), `git log -1` (commit `481e93c`) — matches the decision document's
  own stated repo-state header exactly.
- **Discovered a gap the decision document didn't flag**: the sandbox clone from GitHub is missing
  all 8 preflight scripts the decision document is built on (`check_fred_series.py`,
  `check_eia_series.py`, `check_bea_datasets.py`, `check_treasury_yield_curve.py`,
  `check_bls_series.py`, `check_imf_weo.py`, `check_polygon_shape.py`,
  `check_alphavantage_fx.py`) — they exist only in Ovi's local working directory, never
  git-committed. Confirmed via `tests/unit/test_preflight_scripts.py`'s own current imports
  (none of the 8 appear). Read the 3 scripts this release touches
  (`check_fred_series.py`/`check_eia_series.py`/`check_bea_datasets.py`) directly from the live
  filesystem via Filesystem MCP, then wrote their exact content into the sandbox clone before
  editing, so sandbox validation covers the real files, not a reconstruction.
- `src/bronze/eia_ingester.py`, `bea_ingester.py`, `fred_ingester.py`, `treasury_ingester.py` — all
  tracked in git and confirmed byte-identical between the live filesystem and the sandbox clone
  (`wc -c` cross-check on `eia_ingester.py`: 10220 bytes both sides) before editing either copy.
- Grep sweep (ADR-041 checklist item 11) across `src/`, `scripts/`, `config/`, `tests/` for the 5
  series being pruned. Found two real references beyond inert config: `bls_ingester.py`'s
  `fred_mirror_map["PPI"] = ["PPIFIS", "PPIFGS"]` (a genuine live reference — traced its usage
  through `_run_via_fred_mirror()` to confirm removing `PPIFGS` from the registry would make it a
  silently-harmless dead filter entry, not a break, but still worth cleaning), and
  `fred_ingester.py`'s `RELEASE_LAG_DAYS` dict (5 keys that become inert once their series leaves
  the registry — same vestigial-config class as this project's own `index: []` precedent, ADR-035).

## 1. External research to ground ADR-038/039/040 before touching code

Per house convention, verified each fix's factual basis independently rather than trusting the
decision document's own citations alone:

- **EIA APIv2 response shape** (ADR-038): web-searched EIA's own API documentation and published
  response examples to confirm the `/v2/seriesid/{id}` envelope shape
  (`{"response": {"data": [...]}}`, rows as dicts with `period`/`value` keys) before writing any
  parsing code — this sandbox has no network route to `api.eia.gov` to confirm directly.
- **BEA Table 1.1.x line structure** (ADR-040): web-searched an actual BEA table listing (Table
  1.1.3, which shares the identical standardized line layout with Table 1.1.5/T10105) and confirmed
  Line 15 = "Net exports of goods and services" — matching the decision document's own "likely
  15–16 range" guess, giving confidence to implement `LineNumber == "15"` while still flagging it
  as inferred, not empirically confirmed against a live T10105 response for this pipeline's own
  parameters (exactly per the decision document's own "pending live confirmation" framing).

## 2. Implementation, sandbox-first

All edits made first against the fresh clone, never directly against the live repo. Sandbox
dependencies installed fresh (`polars[all]`, `duckdb`, `pandas-ta-classic`, `fredapi`, `httpx`,
`python-dotenv`, `poetry`, etc.) and a clean baseline established before any edit: **1467 passed, 0
failed** (this sandbox's own baseline differs by +1 from the recorded production baseline of 1466 —
traced to `poetry` not being pre-installed here; installing it changed one test's outcome from a
hard failure to a pass. Confirmed unrelated to this session's diff.).

**ADR-041 + ADR-042** (`config/fred_series.yaml`, done together since both touch the same file):
5 dead/redundant series removed (`GOLDAMGBD228NLBM`, `NAPM`, `NMFCI`, `PPIFGS`,
`CSCICP03USM665S`), each with an inline `# REMOVED ADR-041:` comment citing the specific live
finding. 6 Treasury tenors added under `monetary_policy` (`DGS1MO`/`DGS3MO`/`DGS6MO`/`DGS1`/
`DGS7`/`DGS20`), matching the existing `DGS2`/`DGS5`/`DGS10`/`DGS30` entry shape exactly, per the
decision document's own ready-to-apply YAML draft. 67 → 68 total (62 non-commodity + 6
commodity). Grep-sweep cleanup applied in the same pass: `fred_ingester.py`'s `RELEASE_LAG_DAYS`
lost its 5 now-dead keys (with an inline note explaining the 6 new tenors are *deliberately not*
added here, since ADR-042's own consequences text explicitly says "no code change needed in
`fred_ingester.py`"); `bls_ingester.py`'s `fred_mirror_map["PPI"]` lost `PPIFGS`, kept `PPIFIS`.
`check_fred_series.py`'s docstring count corrected (61 → 62 non-commodity, recomputed for the true
post-both-ADRs state rather than copying the decision document's own intermediate "56" figure,
which describes ADR-041 alone before ADR-042's 6 additions are applied).

**ADR-038** (`eia_ingester.py`, `check_eia_series.py`, `eia_oil.yaml`): `_fetch_series()` migrated
from the dead v1 endpoint to `EIA_BASE_URL.format(series_id=...)` — the v2 constant was already
declared but unused (dead code from a prior session), so this activated existing intent rather than
introducing a new URL. Response parsing rewritten for v2's `response.data` list-of-dicts shape.
Smoke-tested manually with a mocked v2 response before touching tests: confirmed URL, params, and
parsed DataFrame all correct. `eia_oil.yaml`'s `expected_columns` required no field changes — the
internal Bronze record shape (`observation_date`/`value`/`series_id`/`release_date`) was
deliberately kept identical across the v1→v2 parsing change, added a comment documenting this
reverification (checklist item 5) explicitly. `check_eia_series.py` mirrored identically.

**ADR-039 + ADR-040** (`bea_ingester.py`, `check_bea_datasets.py`, done together since both touch
`_fetch_nipa()`'s matching logic): new `LINE_NUMBER_FILTER` dict added alongside the existing
`LINE_FILTER`, with `pce_deflator` and `trade_balance` switched to `LineNumber`-first matching
(`LINE_FILTER`'s strings retained as human-readable labels only for those two); `real_gdp`
deliberately left untouched, still `LineDescription`-matched, since it already passes live.
`trade_balance`'s `BEA_SERIES` entry: `table_name` `T40100` → `T10105`. Smoke-tested both changes
directly against `_fetch_nipa()`: confirmed `pce_deflator` matches a row via `LineNumber` even when
its `LineDescription` wording is deliberately different from the old filter string (proving
robustness to the exact wording-drift that broke it live), and confirmed `real_gdp` still correctly
rejects a same-`LineNumber` row with the wrong description (proving its unchanged behavior wasn't
accidentally loosened). `check_bea_datasets.py` mirrored identically, including the `LineNumber`-
first-else-`LineDescription` fallback logic.

## 3. Test suite

**`test_eia_ingester.py`**: required a near-total rewrite — every existing test was built around the
v1 shape (`params["series_id"]`, `[period, value]` row pairs). Rewrote `_eia_response()` and every
test touching the fetch/parse path; two tests renamed to reflect the new contract
(`test_no_key_still_attempts_v2_request`, `test_no_response_envelope_no_write`) rather than left
silently broken, per this project's own "update the test, document why" convention (CI/CD Ops
Guide's own NEW-5 precedent). 16/16 passing.

**`test_bea_ingester_gld001.py`**: `test_pce_deflator_filters_correct_row` and
`test_trade_balance_filters_correct_row` rewritten to prove the actual point of ADR-039/040 — the
matching row now carries a deliberately different `LineDescription` than the old filter string,
demonstrating the fix survives exactly the wording drift that broke it live. 2 new tests added
(`test_line_number_filter_adr039_040`, `test_trade_balance_table_switched_to_t10105`). 32/32
passing.

**New file `test_fred_series_registry_adr041_042.py`** (24 tests): static registry content checks
(68 total, no duplicates, 5 pruned series absent, 6 tenors present with correct fields, none of the
pruned series were ever in `regime_inputs`) plus, more load-bearing, an end-to-end reproduction of
the exact silent-drop mechanism the ADR describes — using the **real** `config/fred_series.yaml`
(not a mocked registry) with a mocked `fredapi.Fred`, confirming `series_filter=["DGS20"]` now
actually reaches `get_series()` and writes a Bronze file, where before this fix it would have
silently retained 0 series with no error. Also covers the grep-sweep cleanups
(`TestGrepSweepCleanup`: confirms the 5 dead `RELEASE_LAG_DAYS` keys are gone, the 4 pre-existing
Treasury entries are untouched, and `PPIFGS` no longer appears in `bls_ingester.py`'s FRED-mirror
map while `PPIFIS` still does).

One test bug caught and fixed during this pass: the silent-drop-mechanism test initially used a
Saturday date for a daily-cadence series, which the pipeline's own weekday guard correctly skips —
unrelated to the fix under test, corrected to a Monday.

Full sandbox suite: `python -m pytest tests/ -q` → **1493 passed, 0 failed** (sandbox baseline
1467; +26 net new tests, all accounted for: 24 in the new file + 2 in the BEA file; the EIA file's
two renames are net-zero). Coverage: **81.46%** aggregate (gate: 80%), essentially unchanged from
the pre-session baseline — confirmed the two files with the deepest logic changes
(`bea_ingester.py`'s `_fetch_nipa()`, `eia_ingester.py`'s `_fetch_series()`) have their new branches
exercised directly, and that `bea_ingester.py`'s lower isolated file-level coverage (56–59%) is a
pre-existing gap in `run()`/`_run_via_fred_mirror()` (methods this session never touched), not
something ADR-039/040 introduced.

## 4. A transcription bug caught during the live write, not before

While preparing the `CHANGELOG.md` edit for the live repo, a byte-count mismatch (239366 live vs.
239288 sandbox) surfaced after the first write. Traced it precisely: the **sandbox** copy had a bug
from an earlier `str_replace` in this same session — the connecting header line
("## v1.15.1 — Taxonomy Hygiene...") had been dropped between the new v1.15.2 entry and the
existing v1.15.1 entry, leaving an orphaned double-blank-line. The **live** file, written more
carefully with the header explicitly included, was actually correct. Fixed by syncing the sandbox
to match the live file (not the other way around) and re-ran the full test suite to confirm the
correction didn't affect anything (`CHANGELOG.md` content isn't exercised by pytest — 1493 passed,
unchanged). Recorded here per Ovi's standing instruction to log every update, including ones that
correct the session's own working copy.

## 5. Records updated

- `tests/COUNT_BASELINE.txt`: 1466 → 1492 (production baseline; +26 matching the sandbox delta,
  not the sandbox's own 1467→1493 count, since the sandbox's extra pre-existing `+1` is an
  environment artifact specific to this run, not a real baseline difference).
- `KNOWN_RISKS.md`: three new entries — RISK-17 (EIA APIv1 death → APIv2 migration), RISK-18 (BEA
  `pce_deflator`/`trade_balance` LineNumber fix + table swap), RISK-19 (FRED registry hygiene:
  prune + Treasury tenor registration + grep-sweep). Footer chain updated: new
  `*Last updated: v1.15.2*` summary, prior `v1.15.0` entry preserved unchanged beneath it as
  `Prior entry:`.
- `CHANGELOG.md`: new `v1.15.2` entry, one subsection per ADR (038–042), plus an explicit "found
  but deliberately not touched" subsection (ADR-040's exact `LineNumber` still pending live
  confirmation; ADR-038's v2 response shape not empirically confirmed from this sandbox;
  `bea_ingester.py`'s pre-existing `run()`/`_run_via_fred_mirror()` coverage gap, unrelated to this
  session).
- `pyproject.toml`: `1.15.1` → `1.15.2`. **PATCH, not MINOR** — all five ADRs are bug fixes to
  existing ingesters (EIA/BEA were silently broken) and config pruning/addition (FRED registry
  hygiene), not a new job/market/indicator exposed to any downstream consumer (same class as
  v1.13.4/v1.13.5's PATCH precedent, not v1.14.0/v1.15.0's MINOR precedent for genuinely new
  capability).

## 6. Mirrored to live repo

All of the above was built and fully verified in the sandbox clone first (`ast.parse()` on every
modified `.py` file, `yaml.safe_load()` on the modified config, full `pytest` + coverage,
`validate_instruments.py` exit 0 with 699 symbols unaffected — this release touches macro series
config only, not the instrument universe). Every one of the 16 touched files was then written to
the live repo via the Filesystem MCP connector and **byte-count verified** immediately after each
write (`get_file_info` size vs. sandbox `wc -c`), including the two large markdown files
(`CHANGELOG.md`, `KNOWN_RISKS.md`) via a full `edit_file` dry-run preview before applying for real,
followed by a complete `read_text_file` read-back and diff against the intended content — not just
a size check — given their size and the transcription risk that surfaced in §4.

## 7. What's still open (not this release)

- ADR-040's exact `LineNumber` for T10105 — inferred from an external reference and a same-layout
  sibling table, not yet empirically confirmed against a live response for this pipeline's own
  request parameters. Decided-in-direction, not decided-in-detail, per the decision document's own
  framing.
- ADR-038's v2 response shape — confirmed via EIA's own documentation and published examples, not
  against a live response for these 4 specific series (no network route to `api.eia.gov` from any
  sandbox on this project to date).
- The grep-sweep's own item 11 is fully closed this release (both dead references found and
  cleaned) — no follow-up needed there.
- The 5 other preflight scripts authored the same thread as the 3 touched here
  (`check_treasury_yield_curve.py`, `check_bls_series.py`, `check_imf_weo.py`,
  `check_polygon_shape.py`, `check_alphavantage_fx.py`) were confirmed clean in prior sessions per
  the decision document and are untouched by this release.
- `gold/cross_asset/broad_dollar.py` / CrossAssetEngine (GMI Wave 1 Cycle 4) — not started, unrelated
  to this batch.
- Gate 1 (BIS Broad Dollar weight) persistence mechanism — still open, unrelated to this batch.
