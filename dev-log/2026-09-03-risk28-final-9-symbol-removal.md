# 2026-09-03 (continued) — RISK-28 Fully Closed: Remaining 9 Symbols Removed

**Version**: 1.17.6 → 1.17.7
**Trigger**: Ovi's direct follow-up instruction after the v1.17.6
summary: "Resolve the untouched tickers with removal approach."
**Scope**: `config/instruments_identity.yaml`,
`config/instruments_taxonomy.yaml`, `scripts/validate_instruments.py`,
same 5 test files as v1.17.6 (6 assertions, updated again),
`pyproject.toml`, `CHANGELOG.md`, `KNOWN_RISKS.md`.

---

## What changed from v1.17.6

v1.17.6 deliberately left two buckets untouched: 6 symbols (ANSS, JNPR,
HES, HYZN, RDFN, SAVA) confirmed genuinely ACTIVE via AlphaVantage
LISTING_STATUS — their `coverage_check` gap traced to an undiagnosed
fetch-pipeline issue, not the universe — and 3 symbols (SJW, NEW, PEAK)
left UNRESOLVED after both AlphaVantage and web search failed to
classify them either way. Ovi's instruction here is unambiguous: remove
both buckets rather than investigate further.

## One thing worth being explicit about in the record

The 6 confirmed-active symbols are a genuinely different case from the
36 already removed in v1.17.6 — those 36 are actually gone (delisted or
renamed), this removal is a **stopgap over an unsolved pipeline bug**.
Made sure that distinction is preserved in KNOWN_RISKS.md, CHANGELOG.md,
and the version comment in pyproject.toml, not just executed silently —
a future reader asking "why isn't ANSS in the universe" should find
"stopgap, real bug still unsolved," not an incorrect inference that the
company was delisted. This isn't second-guessing Ovi's instruction (it's
executed exactly as given); it's making sure the *reason* survives
alongside the *action*, consistent with this project's own established
practice of writing down rationale, not just outcomes.

## Execution

Identical procedure to v1.17.6's removal, same safety checks:

1. Confirmed all 9 targets present exactly once in
   `instruments_identity.yaml`'s `us_stocks` section, no duplicates
   (ANSS/Technology, JNPR/Technology, HES/Energy, HYZN/High Growth &
   Popular, RDFN/High Growth & Popular, SAVA/High Growth & Popular,
   SJW/Utilities, NEW/Utilities, PEAK/Real Estate).
2. Removed the matching `  - symbol: XXX` line from both
   `instruments_identity.yaml` and `instruments_taxonomy.yaml`.
3. Re-verified per-sector symbol order still identical between both
   files post-removal (543 symbols per sector-list pair).
4. Load-tested through the real `InstrumentLoader`: `count() == 594`,
   `by_market("us_stocks") == 543`, none of the 9 still resolvable, no
   `ValueError` from the positional-join safety check.
5. `scripts/validate_instruments.py` `EXPECTED_TOTAL`: 663 → 654
   (`GMI-VAL-005`). `python scripts/validate_instruments.py` →
   "VALIDATION PASSED — 654 symbols (Layer 1=594, Layer 2=60), no
   errors."
6. Full suite surfaced the same 6 tests as before, now failing on the
   next set of stale counts (603/552/661 → 594/543/652) — updated all
   6 with a second `FIX GMI-VAL-005` note alongside the existing
   `GMI-VAL-004` history in each docstring.
7. Full suite: 1567 passed (unchanged — no new tests, existing
   assertions updated again), 0 regressions.

## Deliberately still not done

No replacement instruments added for any of the 45 original gap
symbols, across either v1.17.6 or this pass — remains Ovi's call, noted
again in this pass's KNOWN_RISKS.md follow-up section rather than
silently dropped from the record now that RISK-28 itself is closed.

## Mirrored to live repo

9 files mirrored via the Filesystem MCP connector: 2 YAML config files
via `write_file` (same rationale as v1.17.6 — full-file write safer than
9 scattered `edit_file` calls each for dispersed single-line removals),
`scripts/validate_instruments.py` + 5 test files + `pyproject.toml` +
`CHANGELOG.md` + `KNOWN_RISKS.md` via targeted `edit_file`. Every file
pulled back via `copy_file_user_to_claude` and diffed byte-for-byte
against sandbox — all 9 identical, no mismatches this pass.

RISK-28 is now fully closed. All 45 original `coverage_check` gap
symbols accounted for: 36 removed as genuinely dead (v1.17.6), 9 removed
per Ovi's explicit stopgap/insufficient-evidence call (this pass), 0
remaining unaddressed.
