# 2026-08-11 — ADR-034–037: Taxonomy Hygiene & Proxy Correlation Discipline

**Version**: 1.15.0 → 1.15.1
**Trigger**: Ovi directed implementation of ADR-034 through ADR-037, decided (not implemented) in
`GMI_Decision_Document_v8.docx` (10 Aug 2026).
**Scope**: `config/instruments_identity.yaml`, `config/instruments_taxonomy.yaml`,
`config/schemas/instruments/identity.schema.yaml`, `config/schemas/instruments/taxonomy.schema.yaml`,
`scripts/validate_instruments.py`, `src/config/instrument_loader.py`, `src/silver/context_anchors.py`,
6 test files, `tests/COUNT_BASELINE.txt`, `KNOWN_RISKS.md`, `CHANGELOG.md`, `pyproject.toml`.

---

## 0. Exploration before any code was written

Empirical-first, per house convention. Read the live repo (both via Filesystem MCP against
`/Users/opi/alpha-factory` and a fresh `git clone` of `github.com/Ovi-xyz/alpha-factory` for
isolated sandbox validation) before trusting `GMI_Decision_Document_v8.docx` alone:

- `pyproject.toml` (1.15.0), `tests/COUNT_BASELINE.txt` (1460), `git log -1` (commit `d24892b`) —
  matches the decision document's own stated baseline exactly.
- `config/instruments_identity.yaml` / `instruments_taxonomy.yaml` — confirmed live pre-state:
  TIN/RUBBER/CPO/NICKEL all `context_available: true`, none with `proxy_for` set; `index: []`
  present in both files; `USD_IDR` in Layer 1 forex; `MYR` the sole `fx_normalization` member;
  `dollar_basket` at 6 currencies. Counted manually: Layer 1 = 640, Layer 2 = 59, total = 699 —
  matches the decision document's stated repo-state header.
- `scripts/validate_instruments.py` (full read) — `EXPECTED_TOTAL = 699`,
  `EXPECTED_SUBCATEGORIES` (22, frozenset), `REQUIRED_FIELDS` (includes `"index"`),
  `layer1_markets` tuple in `_validate_layer1()` (includes `"index"`). Domain-score weight
  validation confirmed **subcategory-level only** — deferring an individual instrument inside a
  subcategory does not require renormalizing that subcategory's `contributes_to` weights (directly
  relevant to ADR-034's own rationale about this being a known schema gap, not something this
  release closes).

## 1. A gap `GMI_Decision_Document_v8.docx` itself didn't check

`config/schemas/instruments/identity.schema.yaml` and `taxonomy.schema.yaml` both declare
`"index"` as a **required** top-level property. ADR-035's own §1.3 "Verified Baseline" reads
`instrument_loader.py`, `silver_scope.py`, `context_anchors.py`, and `validate_instruments.py` —
never the jsonschema files. Removing the `index: []` key from the real YAML files without a
matching schema fix would have made `validate_split()` fail immediately with a jsonschema
required-property error, breaking the checklist's own item 8 ("re-run — confirm exit 0").

Resolved as a necessary consequential fix within ADR-035's own stated scope (a market category
with zero members / zero required-property references is exactly the debt class ADR-035 targets):
removed `"index"` from both schemas' `required` list and `properties` block, with an inline
comment explaining why. Flagged explicitly to Ovi before proceeding, not silently patched.

## 2. Implementation, sandbox-first

All edits made first against the fresh clone (`/home/claude/alpha-factory` in the sandbox), never
directly against the live repo, per house convention ("sandbox clone → install deps → run validate
+ pytest → confirm all pass → mirror to live via Filesystem MCP `str_replace`").

**ADR-034** (`instruments_taxonomy.yaml`): TIN, RUBBER → `context_available: false` +
`deferred_reason` citing the measured correlation + `planned_wave: 2`. The wave number is a
structural placeholder only — `GMI_Decision_Document_v8.docx` §4 explicitly leaves the real
re-evaluation trigger open ("no fixed date/wave decided"); reusing `2` (their state before
ADR-031/032) satisfies the schema's mandatory-field requirement without asserting a false FX-
normalization trigger. This exact ambiguity — schema hard-requires a value the decision document
declined to set — is called out inline in the YAML comment and here, rather than silently invented.
CPO, NICKEL → `proxy_for` (`CPO_FCPO_BMDI`, `NICKEL_LME_NI`) + `proxy_correlation_expected`
(0.405, 0.586) + caveat notes. `agri`/`metals` subcategory `_meta.note`/`reliability_notes` updated
to match (subcategory-level weights confirmed unchanged, per the schema-gap finding above).

**ADR-035**: `index: []` removed from both config files; `"index"` removed from
`REQUIRED_FIELDS` and `layer1_markets` in `validate_instruments.py`; both schema files fixed
(§1 above). Deeper dead-code sweep (`InstrumentLoader._build_index()`, `is_index` property,
`YFINANCE_INDEX_MAP`'s SPX/DJI/VIX/RUT entries, `symbol_utils.py`'s index-market branch) left
untouched — explicitly out of scope per ADR-035's own text, same restraint applied here.

**ADR-036**: `USD_IDR` moved out of Layer 1 forex into `context.dollar_basket` as `IDR`
(`raw_symbol` dropped, `reclassified_from: layer_1_forex` added — same audit-trail pattern as
DXY/SPX/VIX, ADR-003). Layer 1 forex 19→18, `dollar_basket` 6→7.

**ADR-037**: `MYR` → `THB` in `context.fx_normalization` (net swap, count unchanged).
`_meta.note` added documenting why AUD/CAD/SGD are deliberately not duplicated there (already
reachable via Layer 1 forex / `dollar_basket`).

`validate_instruments.py` (sandbox, post-edit): `VALIDATION PASSED — 699 symbols (Layer 1=639,
Layer 2=60), no errors.`

## 3. Test suite

Ran `tests/unit/test_instrument_loader.py` + `test_validate_instruments.py` first (90 passing
baseline in sandbox, confirmed before any edit) — 16 failures after the config edits, all
analytically predicted in advance (count assertions: forex 19→18, Layer 2 59→60, active 59→58,
`deferred_count()` 0→2, `dollar_basket` 6→7, `MYR`→`THB`). Fixed all 16, following the existing
file's own convention (docstring cites the superseding ADR, `# REPLACES <old_test_name>` pattern
rather than silent deletion). Added explicit ADR-035 index-removal coverage
(`test_index_key_absent_from_real_files`, `test_index_not_required_by_schema`, etc.) since the
checklist called for it but no existing test covered it.

Broader repo sweep (`grep` for `TIN|RUBBER|CPO|NICKEL`, `640`, `EXPECTED_TOTAL`, `dollar_basket`,
`context_fx_normalization` across all of `tests/`) surfaced 4 more affected files not mentioned in
the decision document's own checklist: `tests/integration/test_full_system.py` (Layer 1/Layer 2
count assertions), `tests/unit/test_context_anchors.py` (4 assertions — `ContextAnchorsResolver`
reads straight off `InstrumentLoader.all_context()`), `tests/unit/test_package_exports.py`
(`test_get_loader_returns_640`), `tests/integration/test_pipeline_config_integration.py`
(same). All fixed. `src/silver/context_anchors.py`'s own module docstring also had a stale
"TIN, CPO, RUBBER" deferred-set claim — fixed alongside, same category as `instrument_loader.py`'s
docstrings (checklist item 7), just not explicitly named there.

Full sandbox suite: `python -m pytest tests/ -q` → **1466 collected, 1465 passed, 1 skipped, 0
failed** (baseline 1460; +6 net new tests). Confirmed the +6 delta traces exactly to new tests
added (2 in `test_instrument_loader.py`, 4 in `test_validate_instruments.py`) with no accidental
duplicates or silent deletions. Two initial failures (`test_check_poetry_env.py`) and two more
(`test_pandas_indicators.py`) were pure sandbox-environment gaps (`poetry` binary,
`pandas_ta_classic` package not installed) — confirmed unrelated to this diff, resolved by
installing them rather than skipped over.

## 4. Records updated

- `tests/COUNT_BASELINE.txt`: 1460 → 1466.
- `KNOWN_RISKS.md` RISK-1: added a "10 Aug 2026 — run for real, decision made (ADR-034)" section
  with the measured correlations and the resulting `context_available` split.
- `CHANGELOG.md`: new `v1.15.1` entry, one subsection per ADR, plus an explicit "found but
  deliberately not touched" subsection (`symbol_utils.py`'s `KNOWN_EDGE_CASES["MYR"]`,
  `check_yfinance_tickers.py`'s `MYR` references, Gate 1 persistence mechanism) — these are stale
  or open in ways adjacent to this work but outside all 4 ADRs' decided scope.
- `pyproject.toml`: `1.15.0` → `1.15.1`. **PATCH, not MINOR** — judgment call (the decision
  document itself left this open, §3 item 12): all four ADRs are taxonomy/data-quality corrections
  and a reclassification (same shape as ADR-003's precedent), not a new job/market/indicator.

## 5. Mirrored to live repo

All of the above was built and fully verified in the sandbox clone first. Identical edits then
applied to the live repo (`/Users/opi/alpha-factory`) via the Filesystem MCP connector, each
followed by a read-back to confirm the write landed byte-for-byte as intended. Live repo was not
touched until every sandbox check above was green.

## 6. What's still open (not this release)

- Gate 1 (BIS Broad Dollar weight) persistence mechanism — `GMI_Decision_Document_v8.docx` §4:
  "genuinely open, not decided here." Untouched.
- `gold/cross_asset/broad_dollar.py` / CrossAssetEngine (GMI Wave 1 Cycle 4) — not started.
- The deeper `InstrumentLoader`/`symbol_utils.py` dead-code sweep ADR-035 itself declines to do.
- A future Silver-layer FX-normalization consumer for THB/CPO's proxies — ADR-037 prepares the
  anchor, does not build the consumer.
