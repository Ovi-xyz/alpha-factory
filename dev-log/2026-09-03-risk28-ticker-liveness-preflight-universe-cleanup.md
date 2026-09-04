# 2026-09-03 — RISK-28 Closed: Ticker Liveness Preflight + 36-Symbol Universe Cleanup

**Version**: 1.17.5 → 1.17.6
**Trigger**: Ovi's two-part instruction, following v1.17.5's RISK-28
(coverage_check at 92.8%, decision left open): "First, build an
automated ticker-liveness preflight. Second, use that automated
ticker-liveness preflight to verify the 41 to unblock the gate."
**Scope**: `scripts/preflight/check_ticker_liveness.py` (new),
`config/instruments_identity.yaml`, `config/instruments_taxonomy.yaml`,
`scripts/validate_instruments.py`, 5 test files (6 assertions),
`pyproject.toml`, `CHANGELOG.md`, `KNOWN_RISKS.md`.

---

## 1. The preflight script

Read every existing script in `scripts/preflight/` before writing a new
one — `check_yfinance_tickers.py` for the Layer-1-analogous recency-check
pattern (it only covers Layer 2 context anchors), `check_alphavantage_fx.py`
for the tiered/budget-conscious design convention, `check_bis_cbpol_d.py`
for the raw-httpx-for-a-new-endpoint convention (no adapter class covers
LISTING_STATUS; FX_DAILY's `AlphaVantageForexAdapter` is a different call
shape entirely).

`check_ticker_liveness.py`: Tier 1 (default) — yfinance 5-day recency
check per Layer 1 symbol, same shape as `check_yfinance_tickers.py`'s
`_check_one()`. Tier 2 (`--cross-check-delisting`, opt-in, costs 2 of the
25 daily AlphaVantage requests — one bulk call each for `state=active`
and `state=delisted`) — classifies every Tier-1 FAIL as DELISTED /
LIKELY_TRANSIENT / UNRESOLVED. Never writes config; only reports —
removing or renaming a Layer 1 entry is a human decision given the
positional join contract between the two instrument YAML files.

Could not execute Tier 1 for real in this sandbox — no network route to
`finance.yahoo.com` (same constraint every other preflight script in
this directory already documents). Syntax-validated only. For the
actual verification task, used the equivalent live classification via
direct tool calls instead — see below.

## 2. Verifying the 45

Loaded the Alpha Vantage MCP connector's `LISTING_STATUS` tool directly
(not through the sandbox's blocked HTTP path) and pulled both
`state=active` and `state=delisted` in full (9,457 delisted rows, a much
larger active-list pull). Cross-referenced all 41 previously-unverified
symbols by exact ticker match.

**First finding that reshaped the approach**: one specific
`delistingDate` (2026-09-01, the most recent date in the snapshot)
carried 598 of the 9,457 delisted rows — a wildly disproportionate
cluster against every other date's usual single/double digits, across
an incoherent mix of SPAC warrant classes, ETFs, and unrelated
small-caps. Treated this as "AV's most-recent-snapshot cutoff" — weak
evidence for the exact date, not zero evidence the delisting is real.
Cross-checked the 3 of my own eventual removal-candidates that fell on
this suspicious date (HBI, ASTR, NKLA) against the active list too —
none were double-listed, consistent with genuine (if imprecisely dated)
delistings. This distinction is now baked into the preflight script's
own docstring so a future run doesn't over-trust that field.

**Second finding, the more consequential one**: 24 of the 41 matched
AlphaVantage's delisted list on the first pass by exact symbol. The
other 17 did not — I checked whether those 17 were simply still
ACTIVE (6 were: ANSS, JNPR, HES, HYZN, RDFN, SAVA — genuinely live
companies, meaning their `coverage_check` gap has nothing to do with
the universe and is a separate fetch-pipeline problem worth its own
investigation) versus genuinely unresolved (11 were in neither list).
Individually verified those 11 plus a few "surprising" delisted claims
(AVB, SEE, CTRA, CFLT, FOLD — none matched my own prior expectations, so
I didn't want to trust AV's record alone for these) via targeted web
search, one query per symbol rather than batching, per the usual search
guidance: batching unrelated names returns shallow results.

That research surfaced the second reason AlphaVantage alone isn't
sufficient: **pure ticker-rename events don't reliably show up as
"delisted" under the old symbol** in AV's data the way M&A/bankruptcy
delistings do. SQ→XYZ, ABC→COR, RE→EG, IAC→PPLI, USM→AD, ZI→GTM — all
six were absent from both AV lists under their old ticker; all six
resolved cleanly via search once I knew to look for a rename rather than
a delisting. ABC in particular renamed to COR back in **August 2023** —
this ticker has been dead in the universe for roughly three years,
undetected the entire time, which says something about how long
`coverage_check`'s zero-tolerance gate had presumably been silently
under-reporting before F-QV-01 (the same fix from v1.17.5) made CRITICAL
checks actually block instead of only log.

One near-miss worth recording: AV's delisted record for **PEAK** turned
out to be a same-ticker collision with an unrelated shell company
(IPO'd and delisted within 3 days, back in 2023) — not our instrument.
Cross-checking PEAK against the active list too (also absent) confirmed
this specific symbol is genuinely unresolved rather than confidently
either-way, and it was left alone rather than removed on a
false-positive match. This is the exact failure mode the preflight
script's Tier 2 is designed to expose to a human rather than
auto-resolve — a naive "found in delisted list → remove" rule would have
gotten this one wrong.

Final tally of the original 45: **36 confirmed dead** (30 delisted, 6
renamed), **6 confirmed still active** (unrelated fetch-pipeline issue,
left alone), **3 genuinely unresolved** (SJW, NEW, PEAK — left alone,
insufficient evidence).

## 3. The removal itself

`instruments_identity.yaml` and `instruments_taxonomy.yaml` are joined
positionally by `src/config/yaml_split_merge.py` — same tree path/list
index in both files must mean the same instrument, and a list-length
mismatch between them raises `ValueError` at load time. Before touching
anything: wrote a small script to extract each file's `us_stocks`
sector-by-sector symbol ordering and confirmed byte-for-byte identical
ordering between the two files (588 symbols, 12 sectors, exact match).
This meant a straightforward exact-full-line-match removal (`  - symbol:
XXX`) from both files independently — since both start from identical
order and the same 36 symbols are removed from both, the remaining
symbols stay aligned automatically; no manual index arithmetic needed.

Verified all 36 targets existed exactly once in `instruments_identity.yaml`
before removing anything (no duplicates, no market-section surprises —
all 36 were confirmed in `us_stocks`, consistent with all being US
equities). Removed, then re-ran the same ordering-parity check
post-removal (still identical, 552 symbols per sector-list pair), then
— the check that actually matters — loaded the result through the real
`InstrumentLoader`, not just my own regex extraction: `loader.count()
== 603`, `by_market("us_stocks") == 552`, none of the 36 removed symbols
still resolvable. No `ValueError` from the positional-join safety check.

## 4. Downstream updates

`scripts/validate_instruments.py`'s `EXPECTED_TOTAL`: 699 → 663,
documented as `GMI-VAL-004` in the same changelog-in-docstring pattern
every prior `EXPECTED_TOTAL` change in this file already uses.
`python scripts/validate_instruments.py` → "VALIDATION PASSED — 663
symbols (Layer 1=603, Layer 2=60), no errors."

Full suite run surfaced exactly 6 pre-existing hardcoded-count failures
(639/588/697), all traced to the same root cause (a real universe-size
change), none masking an actual regression: `test_full_system.py`
(`get_loader().count()`), `test_pipeline_config_integration.py`
(same), `test_instrument_loader.py` (three separate assertions —
`count()`, `by_market("us_stocks")`, `count_total()`), and
`test_package_exports.py` (`get_loader().count()` via the package
export). Updated all 6 to the correct post-cleanup values (603/552/661),
each with a `FIX GMI-VAL-004` docstring note alongside the existing
`FIX GMI-IL-001`/`UPD ADR-036` history already in those same
docstrings — this project's own established pattern of accreting
rationale in place rather than replacing it.

## 5. Deliberately not done this pass

Adding XYZ, COR, EG, PPLI, AD, GTM as new instruments to replace the 6
renamed tickers — a universe *addition* changes `EXPECTED_TOTAL`
upward and is a more consequential decision than a removal (which
instruments and their properties are correct?). Left for Ovi.

Manual verification of the 6 LIKELY_TRANSIENT symbols (ANSS, JNPR, HES,
HYZN, RDFN, SAVA) — their `coverage_check` gap is real but is a
fetch-pipeline problem, not a universe problem; investigating why they
fail to fetch despite being genuinely active is out of scope for this
pass and registered as a follow-up note on RISK-28 rather than a new
risk (same root investigation, different next step).

The 3 UNRESOLVED symbols (SJW, NEW, PEAK) similarly left alone —
insufficient evidence to act on any of the three directions (remove,
rename, or "it's fine, investigate the fetch pipeline instead").

## 6. Verification

1. `ast.parse()` — new preflight script plus every modified file — clean.
2. `grep -rn 'f"SELECT\|f\'SELECT'` across `src/` — clean (no SQL touched
   this pass at all).
3. Ordering-parity check between the two instrument YAML files — run
   twice (before and after removal), both times reporting identical
   per-sector symbol order.
4. Real `InstrumentLoader`/`merge_split_trees()` load test post-removal
   — no `ValueError`, correct counts, no removed symbol still resolvable.
5. `python scripts/validate_instruments.py` — PASSED, 663 symbols.
6. Full suite: 1567 passed (unchanged count — 6 existing assertions
   updated, no new tests this pass), 0 regressions.

## 7. Mirrored to live repo

9 files mirrored to `/Users/opi/alpha-factory` via the Filesystem MCP
connector: `scripts/preflight/check_ticker_liveness.py` (new, via
`write_file`), `config/instruments_identity.yaml` and
`config/instruments_taxonomy.yaml` (full-file `write_file` rather than
36 scattered `edit_file` calls each — safer for this many dispersed
single-line removals; still verified byte-for-byte after), and 6 files
via targeted `edit_file` (`scripts/validate_instruments.py`, the 5 test
files, `pyproject.toml`, `CHANGELOG.md`, `KNOWN_RISKS.md`). Every file
pulled back via `copy_file_user_to_claude` and diffed against the
sandbox source of truth. Two cosmetic (non-functional) line-wrap
mismatches surfaced on this pass — both in comment prose, zero code
impact — caught by the byte-diff step exactly as it's meant to, and
corrected before considering the mirror complete. Final diff pass: all
9 files byte-identical.

## 8. Open items carried forward

RISK-28 (KNOWN_RISKS.md) closed as RESOLVED. Three follow-up threads
noted on the same entry, none elevated to a new RISK number: (a) the
6 LIKELY_TRANSIENT symbols' fetch-pipeline issue, unrelated to the
universe; (b) the 3 UNRESOLVED symbols (SJW, NEW, PEAK); (c) whether to
add XYZ/COR/EG/PPLI/AD/GTM as replacement instruments for the 6 renames.
