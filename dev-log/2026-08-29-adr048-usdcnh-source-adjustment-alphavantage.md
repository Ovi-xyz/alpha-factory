# 2026-08-29 — USD/CNH Source Adjustment: yfinance → AlphaVantage FX_DAILY (ADR-048)

**Version**: 1.17.1 → 1.17.2
**Trigger**: Ovi's direct instruction — "Based on CNH issue, continue with
implementation phase to finish outstanding issues in sequence" (followed by
"Continue the project work") — implementing
`alpha_factory_usdcnh_source_adjustment_v1_0.docx` (ADR-048), the addendum's
own status at session start: "DECIDED — Implementation PENDING."
**Scope**: `config/instruments_identity.yaml`, `config/instruments_taxonomy.yaml`,
`config/schemas/instruments/identity.schema.yaml`,
`config/schemas/alphavantage_fx.yaml` (new), `src/bronze/market_ingester.py`,
`src/bronze/alphavantage_adapter.py`; 3 test files
(`tests/unit/test_market_ingester.py`, `tests/unit/test_alphavantage_adapter.py`,
`tests/unit/test_schema_validator.py`) + 1 test extended in place
(`tests/unit/test_instrument_loader.py`); `pyproject.toml`, `CHANGELOG.md`,
`KNOWN_RISKS.md`, `tests/COUNT_BASELINE.txt`.

---

## 0. Pre-work verification — closing the addendum's own blocking item

The addendum itself flagged a BLOCKING open item before any implementation
could start: `instruments.yaml` (singular) no longer exists in the live
repo (confirmed by Ovi, 29 Aug 2026, per the addendum's own Section 6), and
every governing design document still assumes it as the single source of
truth — stale relative to live code. Closed this by reading the live repo
directly via the Filesystem MCP connector (`/Users/opi/alpha-factory`)
before writing anything:

- `directory_tree` confirmed the real mechanism: `config/instruments_identity.yaml`
  (sourcing fields) + `config/instruments_taxonomy.yaml` (routing/taxonomy
  fields), joined positionally at load time by `src/config/yaml_split_merge.py`
  (Decision B Step 2, GMI Decision Document v5 §2.1 — a split this session
  had not previously read in this much detail).
- Read `src/config/instrument_loader.py` in full: confirmed
  `_build_context_instrument()` already folds any YAML key outside
  `_CONTEXT_CONSUMED_KEYS` into `Instrument.meta` as a catch-all — this
  turned out to be the single most important fact for scoping the whole
  implementation small (see Section 2 below).
- Read `config/instruments_identity.yaml` in full and located CNH's real
  entry: `context.dollar_basket.instruments[0]`, `yfinance_symbol:
  USDCNH=X` — exactly the broken ticker the addendum's investigation
  trail (Section 2) describes.
- Read `src/bronze/market_ingester.py` in full: confirmed
  `_run_context_symbol()` computes `api_symbol = inst.yfinance_symbol`
  unconditionally for every Layer 2 context instrument, and `_fetch()`'s
  market dispatch falls into a yfinance-only `else` branch for
  `market == 'context'` — exactly reproducing the reported bug (CNH has no
  fallback route at all today).
- Read `config/schemas/instruments/identity.schema.yaml`: found
  `additionalProperties: false` on `$defs.instrument` — this meant adding
  new identity-side fields without a matching schema update would make
  `validate_split()` reject the real file. Caught before writing any YAML,
  not discovered as a test failure afterward.
- Cloned a fresh `github.com/Ovi-xyz/alpha-factory` sandbox, installed
  `poetry` (missing from the bare sandbox — same one-time step as the
  22 Aug session) + project dependencies via pip, confirmed baseline:
  **1521 passed, 0 failed** — exact match to `tests/COUNT_BASELINE.txt`
  and the live repo's `pyproject.toml` version comment.

## 1. Design decision: keep `yfinance_symbol`, add an explicit override

Two options considered for CNH's now-invalid `yfinance_symbol: USDCNH=X`:
blank it (matching the `''` "deferred, no source yet" sentinel convention
noted in `instrument_loader.py`'s own docstring), or retain it and add a
separate, explicit override field the fetch code must check first.

Chose **retain + override**, for two concrete reasons found empirically,
not by preference alone:

1. `test_instrument_loader.py::test_cnh_uses_offshore_ticker_not_onshore`
   asserts `cnh.yfinance_symbol == "USDCNH=X"` against the **real** config
   files (`self.loader = get_loader()`, no fixture). That assertion is
   still factually true — it's the real yfinance ticker string, just
   confirmed non-functional. Blanking it would break a still-correct test
   for zero functional gain.
2. `validate_instruments.py`'s Layer 2 validation
   (`_validate_layer2()`) never format-checks `yfinance_symbol` the way it
   does for Layer 1 forex/idx/commodity (`.endswith("=X")` etc.) — so
   nothing downstream currently treats CNH's `yfinance_symbol` as
   load-bearing besides the exact one code path this fix changes.

Added `data_source: alphavantage_fx`, `from_symbol: USD`,
`to_symbol: CNH` to CNH's identity entry instead. `market_ingester.py`
checks `inst.meta.get('data_source')` **before** ever reading
`inst.yfinance_symbol` for a context instrument, so the stale field is
provably never reached by any live code path — verified by a dedicated
regression test (Section 3).

`from_symbol`/`to_symbol` as **explicit** fields (not derived from
`inst.symbol` via string slicing) were necessary, not just tidier: Layer 2
`dollar_basket` entries store the bare currency code (`inst.symbol ==
'CNH'`, 3 characters) — unlike a Layer 1 forex pair, there is no pair
string to parse out of it at all. `AlphaVantageForexAdapter._parse_pair()`
already handles a combined `"USD_CNH"` string correctly (verified via its
existing test suite, unmodified), so the fix builds that string at the
call site from the two explicit fields and passes it through unmodified —
zero changes to `_parse_pair()` or the adapter's public `fetch()` signature.

## 2. Zero changes to `instrument_loader.py`

Confirmed via a direct live read *before* deciding an implementation
approach (not after, as a discovery): `_build_context_instrument()`'s
existing catch-all —

```python
extra_meta = {
    k: v for k, v in item.items() if k not in cls._CONTEXT_CONSUMED_KEYS
}
```

— already forwards any identity/taxonomy key outside a small explicit
allow-list into `Instrument.meta`. `data_source`, `from_symbol`,
`to_symbol` are not in that allow-list, so they land in `meta` with no
dataclass change at all. Verified empirically in the sandbox before
writing any market_ingester.py code:

```
$ PYTHONPATH=. python3 -c "
from src.config.instrument_loader import get_loader
cnh = get_loader().get_context('CNH')
print(cnh.meta)"
{'data_source': 'alphavantage_fx', 'from_symbol': 'USD', 'to_symbol': 'CNH', 'notes': '...'}
```

This is the single fact that kept the whole fix's blast radius to two
source files (`market_ingester.py`, `alphavantage_adapter.py`) plus
config/schema — no dataclass field, no `InstrumentLoader` API surface
change, no ripple into any other module that constructs an `Instrument`.

## 3. `market_ingester.py` — three call sites, one override, checked for consistency

Same override check (`inst.meta.get('data_source') == 'alphavantage_fx'`)
applied at three points that must all agree, or the read and write sides
of the Bronze partition would disagree on the `source={...}` path
segment — the exact class of bug ADR-045 closed for the `timeframe={...}`
segment, one layer up:

- **`_fetch()`**: routes to `ChainedAdapter([AlphaVantageForexAdapter()])`
  ahead of (not folded into) the existing per-market dispatch — chosen so
  a future second override on a different market would not need a new
  `elif` per market, just the same one check.
- **`_primary_source_for()`**: returns `'alphavantage'` instead of the
  context-market default `'yfinance'`. This is what feeds
  `resolve_start_date(source=primary_src, ...)` — the Bronze read-side
  scan. Missing this one would have looked correct in isolation (the
  fetch itself would still work) while silently breaking incremental
  fetch: the read-side scan would look under `source=yfinance` for data
  actually written under `source=alphavantage`, find nothing, and
  re-trigger a full cold-start fallback fetch on every single run.
- **`_run_context_symbol()`**: builds `api_symbol` from
  `f"{meta['from_symbol']}_{meta['to_symbol']}"` instead of
  `inst.yfinance_symbol`.

A dedicated test (`test_read_write_source_segment_consistent`) asserts
`resolve_start_date()` and `write()` are called with the same `source`
value end-to-end through `_run_context_symbol()`, not just that each
individual method returns the right thing in isolation.

## 4. `config/schemas/alphavantage_fx.yaml` — new Bronze schema, no volume

AlphaVantage FX endpoints structurally lack a volume column
(`AlphaVantageForexAdapter` already sets it to `None` unconditionally,
FIX AV-3, pre-existing). Per the addendum's own Section 4 analysis: CNH is
Layer 2-only, consumed exclusively by CrossAssetEngine, whose four modules
all operate on close-derived returns — `ActiveSymbolsResolver` (the
pipeline's sole volume consumer) never applies to Layer 2 anchors. Schema
declares `open`/`high`/`low`/`close` only, matching
`yfinance_ohlcv.yaml`/`polygon_ohlcv.yaml`'s existing structure. Wired
into `_load_schema_validators()`'s `validator_map`.

## 5. `alphavantage_adapter.py` — `outputsize` compact/full

Named explicitly in the addendum's own Consequences section as a required
follow-up, not an optional nice-to-have: `outputsize` was hardcoded to
`'full'` regardless of the requested date range. Changed to
`"full" if (end - start).days > 100 else "compact"` — a cold-start
backfill (`IncFetchProtocol`'s multi-year `fallback_years` path) still
gets `'full'`; a routine incremental run
(`last_date - DEFAULT_LOOKBACK_DAYS=7`, G1, unchanged) gets `'compact'`
(AV's latest ~100 points), so CNH's 3 daily calls (1D/1W/1M) stop
re-downloading the entire ~11.8-year history once Bronze has caught up
once. Boundary tested explicitly at exactly 100 and 101 days
(`date(2026,1,1)` → `date(2026,4,11)` = 100 days = compact;
→ `date(2026,4,12)` = 101 days = full — verified via direct Python
`date` arithmetic before writing the assertions, not assumed).

This is a general adapter-level change (affects any AV forex call, not
just CNH), reviewed for regression risk against the existing DXY-fallback
path: `TestFetchHttpFlow`'s existing tests don't assert a specific
`outputsize` value anywhere, so none needed updating.

## 6. KNOWN_RISKS.md RISK-22 — accepted exception, not a resolved bug

Every existing entry in `KNOWN_RISKS.md` (RISK-1 through RISK-21) is a
**resolved bug**. This is the first entry that is a **deliberate,
accepted design exception** instead — CNH becomes AlphaVantage's second
permanent (not occasional-fallback) consumer of the shared 25-req/day
budget, a scoped exception to Supplementary Design G4's "AV removed from
default chain" policy. Written per the addendum's own explicit
instruction to document this so a future third permanent consumer is a
deliberate decision, not a silent repeat of the same reasoning that would
eventually re-saturate the shared budget. Status marked `✅ ACCEPTED`
rather than `✅ RESOLVED` to keep that distinction visible in the file's
own vocabulary.

## 7. Verification sequence (sandbox, before mirroring)

1. Baseline: 1521 passed, 0 failed (poetry installed first — same
   one-time environment gap as 22 Aug).
2. All edits applied in the sandbox clone only.
3. `ast.parse()` on every modified `.py` file — clean.
4. `PYTHONPATH=. python3 scripts/validate_instruments.py` — **PASSED,
   699 symbols** (confirms the schema.yaml update was sufficient and
   correct — this would have failed loudly on `additionalProperties`
   otherwise).
5. Direct REPL check: `get_loader().get_context('CNH').meta` contains all
   three new fields — confirms the catch-all mechanism before trusting it
   in the fix.
6. Full affected-suite run: **173 passed** (153 pre-existing + 20 new),
   before touching anything else.
7. Full repo suite: **1533 passed, 0 failed** (1521 + 12 net new —
   6 market_ingester routing tests, 4 adapter outputsize boundary tests,
   2 schema tests; the CNH identity test was extended in place, not
   counted as a new test).
8. `grep -rn 'f"SELECT\|f\x27SELECT' src/` — clean (no new f-string SQL;
   this change touches no SQL at all).
9. Coverage: 88.06% aggregate (gate 80%, unchanged from baseline);
   `market_ingester.py` and `alphavantage_adapter.py` individually at
   **100%** coverage after the new tests.

## 8. Mirrored to live repo

All 15 changed/new files copied to `/Users/opi/alpha-factory` via the
Filesystem MCP connector, each followed by a `get_file_info` byte-count
check against the sandbox `wc -c` output — per this project's own
mandatory post-write verification discipline.
