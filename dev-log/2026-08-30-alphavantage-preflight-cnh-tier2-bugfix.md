# 2026-08-30 — AlphaVantage Preflight: CNH-aware + Tier 2 Bug Fix

**Version**: 1.17.2 → 1.17.3
**Trigger**: Ovi — "We need to fix alphavantage preflight module as well.
The preflight modules always run first before doing live test." Followed
by a clarification once I started drafting mocked unit tests for it:
"Preflight modules are out of src. Hence, those modules don't need test
coverage. Preflight role ensures that everything is pass before running
live test."
**Scope**: `scripts/preflight/check_alphavantage_fx.py` only.
`pyproject.toml`, `CHANGELOG.md`, `KNOWN_RISKS.md` (RISK-22 mitigation
item added, not a new risk).

---

## 0. Why this needed fixing, not just extending

Read the live script before touching it (same discipline as every prior
session). Two things stood out:

1. **A real, pre-existing gap, not just a missing feature**: the script
   had a DXY-specific Tier 1 static check and a generic single-pair Tier
   2 live check, but nothing CNH-specific. Given ADR-048 (yesterday's
   session) made CNH AlphaVantage's sole, no-fallback source, this
   preflight script — whose entire job is "confirm everything passes
   before the live pipeline touches the real API" — had no way to
   confirm CNH's specific routing before trusting it live.

2. **A genuine bug, found while reading, not while extending**: Tier 2's
   `_check_live_fetch()` was hand-building its own `requests.get(...)`
   params dict — a duplicate of `AlphaVantageForexAdapter.fetch()`'s
   logic, not a call to it. This duplicate still hardcoded
   `outputsize: "full"` unconditionally. Yesterday's ADR-048 session
   added `outputsize` compact/full sizing to the *real* adapter — but
   this preflight script's copy never got that memo, because it was
   never actually calling the real adapter to begin with. A preflight
   script whose live check silently stopped reflecting the real
   production code path is worse than no preflight check at all — a
   green PASS here would have meant nothing about whether the real
   adapter's new logic actually works.

Fixed both by routing every live-network tier through
`AlphaVantageForexAdapter.fetch()` directly, and adding CNH-specific
coverage to both the zero-cost static tier and a new opt-in live tier.

## 1. No test file added — explicit instruction, not an oversight

Started drafting a `TestCheckAlphavantageFx` class for
`tests/unit/test_preflight_scripts.py` (following that file's own
existing pattern for the other 5 preflight scripts it covers) before Ovi
stopped me: `scripts/preflight/` is outside `src/`, and per Ovi's
explicit direction, these modules don't need test coverage — "Preflight
role ensures that everything is pass before running live test" is itself
the test, run for real on network-enabled hardware.

This actually contradicts what `test_preflight_scripts.py`'s own
docstring currently claims ("Covers the network-INDEPENDENT logic in
scripts/preflight/*.py") and what it already does for 5 other scripts in
that directory — worth flagging to Ovi as a documentation/convention
inconsistency to resolve at some point (is the existing coverage for the
other 5 scripts to be kept as-is, or should the docstring's framing
change project-wide?), but out of scope for this session: not touching
existing passing tests without an explicit instruction to do so.

In place of a committed test file, verified the actual logic
interactively in the sandbox (not persisted, not part of the repo):

- Tier 1 (both checks) run for real with zero mocking — genuinely
  zero-network logic, no reason to fake it.
- Tier 2 mocked at `requests.get`: confirmed `outputsize='compact'` is
  requested for a 30-day window.
- Tier 3 mocked at `requests.get`: confirmed `outputsize='full'` is
  requested for a >100-day window; confirmed PASS at 3,079 correctly
  unique-dated rows (matching the addendum's own baseline) and FAIL at 1
  row (the exact regression shape yfinance's USDCNH=X had) and on a
  simulated `ConnectionError`.
- `main(--check-cnh)` end-to-end with a mocked `_check_cnh_live_depth`
  returning success: exit code 0, as expected.

## 2. Design: three tiers, same budget discipline as before

- **Tier 1** (always runs, zero network, zero budget): DXY skip-sentinel
  (existing, unchanged) + new CNH pair-parse
  (`_parse_pair('USD_CNH') == ('USD', 'CNH')`) — the two checks assert
  *opposite* outcomes (skip vs. resolve) for a reason: DXY genuinely has
  no AV endpoint (FIX AV-2), CNH genuinely does (ADR-048). Testing both
  in the same tier makes that asymmetry explicit rather than leaving it
  implicit.
- **Tier 2** (`--live-fetch`, opt-in, 1/25 budget): unchanged behavior
  (generic single-pair check, default EUR/USD, short window), but now
  implemented by actually calling `AlphaVantageForexAdapter.fetch()`
  instead of a hand-rolled duplicate.
- **Tier 3** (`--check-cnh`, opt-in, 1/25 budget, NEW): CNH-specific,
  wide window (>100 days, 13 years requested) to deliberately cross the
  adapter's `outputsize='full'` threshold, checking row count against a
  conservative floor (1000 — well under the addendum's own 3,079-row
  baseline, generous headroom against ordinary variation while still
  catching a near-total-failure regression of the exact shape this whole
  ADR exists to fix).

Kept Tier 2 and Tier 3 as independent opt-in flags (not one combined
"--live" flag) — they check different things (generic shape vs.
CNH-specific historical depth) and cost budget independently; combining
them would remove the ability to run just one.

## 3. KNOWN_RISKS.md — extended RISK-22, did not create a new risk

This is tooling that improves *observability* of an existing accepted
risk (RISK-22, AlphaVantage's second permanent budget consumer), not a
new risk in itself. Added as mitigation item 4 rather than a new
`RISK-23` entry — the underlying risk (shared 25/day budget, CNH has no
fallback) is unchanged; what changed is now there's a repeatable way to
check CNH's AlphaVantage-side health before it silently degrades in
production.

## 4. Verification

1. `ast.parse()` on the modified file — clean.
2. Full suite re-run: 1533 passed, 0 failed — unchanged from before this
   fix (no tests added or removed, per Ovi's direction).
3. `grep -rn 'f"SELECT...'` across `src/` and `scripts/` — clean (no SQL
   touched by this change at all).
4. `validate_instruments.py` — unaffected, re-run as a sanity check only
   (699 symbols, PASSED).
5. Interactive verification per Section 1 above.

## 5. Mirrored to live repo

`scripts/preflight/check_alphavantage_fx.py`, `pyproject.toml`,
`CHANGELOG.md`, `KNOWN_RISKS.md` copied to `/Users/opi/alpha-factory` via
the Filesystem MCP connector, each followed by a `get_file_info`
byte-count check plus a full `diff` against the sandbox source of truth
(byte-count alone is insufficient — a same-length word substitution
would pass a byte-count check but fail a real diff, as happened with a
one-word typo during the previous ADR-048 session).
