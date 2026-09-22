# ADR-049 continued: NZD gap in the Broad Dollar basket extraction script

**Date:** 22 Sep 2026 (corrected from an initial mislabel of 21 Sep — see dev-log/2026-09-22-adr049-nzd-gate1-closed.md)
**Version:** v1.18.6 → v1.18.7 (PATCH)
**Related:** ADR-049 (chat thread, 5 Sep 2026), KNOWN_RISKS.md RISK-16
("Gate 1 CLOSED, 12 Sep 2026" subsection), GMI Wave 1 Cycle 4
(dev-log/2026-09-13-gmi-wave1-cycle4-cross-asset-engine.md)

## Context

ADR-049 (5 Sep 2026) decided to extend the Broad Dollar basket's
Layer-1-reuse currency set from 6 to 7 pairs — EUR_USD, USD_JPY,
GBP_USD, USD_CAD, USD_CHF, AUD_USD, **NZD_USD** — reusing NZD_USD
directly from its existing always-on Layer 1 forex pair rather than
duplicating it into `context.dollar_basket` as an 8th instrument. That
decision is recorded as a comment in `config/instruments_taxonomy.yaml`
above `context.dollar`.

Separately and one week later (12 Sep 2026), Ovi ran
`scripts/preflight/check_bis_eer_weights.py --extract-weights` for real
on the M1 during the same session that built GMI Wave 1 Cycle 4
(CrossAssetEngine) — the empirical Gate 1 closure documented in
KNOWN_RISKS.md RISK-16. That run, and the `BROAD_DOLLAR_REF_AREAS` dict
it read from, covered only the **original 13 currencies**
(EUR/JPY/GBP/CAD/CHF/AUD/IDR/CNH/KRW/SGD/HKD/TWD/NOK). ADR-049's NZD
decision was never actually wired into that script.

## The report this thread started from

Ovi re-ran `--extract-weights` and reported: "NZD_USD was unextracted."

## Root cause (confirmed empirically against live source, not assumed)

Read `scripts/preflight/check_bis_eer_weights.py` directly (via the
Filesystem MCP connector, against the live repo — not from log text).
`BROAD_DOLLAR_REF_AREAS` never contained an `"NZD"` key at all. This
means:

- `extract_us_weights_from_sheet()` never had a reason to look for an
  `NZ` column in the BIS worksheet, since it only searches for columns
  matching the dict's own values.
- This is a **different, simpler** failure mode than the function's own
  `None` / "MISSING" result, which the function already handles and
  `_extract_weights()` already reports explicitly (a target currency
  whose column exists in the sheet but wasn't matched, or whose
  US-row cell came back empty). NZD was never a target key to report on
  in the first place, so it could not have surfaced there either.
- In short: this was a **script gap** (ADR-049 decided, never
  implemented in this specific file), not a BIS data-availability
  problem, and not a bug in the extraction/parsing logic itself.

Confirmed no other file duplicates this dict incorrectly: `grep`-swept
the whole repo for `BROAD_DOLLAR_REF_AREAS` and any other hardcoded "13
currencies" assumption before making any change (see "Other 13→14
touch-points" below).

## Fix

1. **`scripts/preflight/check_bis_eer_weights.py`**
   - Added `"NZD": "NZ"` to `BROAD_DOLLAR_REF_AREAS`, positioned
     immediately after `"AUD"` (its closest conceptual sibling — same
     Layer-1-reuse / same USD-is-quote sign convention), with an
     explanatory comment citing ADR-049 and this exact gap.
   - `BIS_EER_ENDPOINT`'s key is already built from
     `BROAD_DOLLAR_REF_AREAS.values()` (FIX BIS-1's own drift-proofing
     from the HKD/TWD/NOK episode) — `NZ` is picked up there
     automatically, no separate key edit needed.
   - Module docstring: added a new dated ("UPD ADR-049...") paragraph
     narrating this gap and fix, in the same accretive style as the
     file's existing history — did not rewrite any earlier dated
     paragraph.
   - `extract_us_weights_from_sheet()`'s docstring: "13-currency" →
     "14-currency (13 + NZD, ADR-049)" — a forward-looking accuracy
     statement about the dict's current size, not a historical claim,
     so updating it (rather than leaving it stale) was the right call.

2. **Two additional hardcoded-"13" spots found and fixed in the same
   file while verifying no other drift risk remained** (not reported by
   Ovi, found via direct inspection before editing):
   - `_extract_weights()`'s own printed summary line —
     `print(f"\nSum of these 13 target-currency weights: ...")` — was a
     **literal** in the actual runtime output, not just a comment. Left
     unfixed, the real `--extract-weights` output Ovi is about to
     generate would have said "13" while genuinely summing 14 values.
     Changed to `f"...{len(BROAD_DOLLAR_REF_AREAS)}..."`.
   - The `--extract-weights` argparse `help=` string had the same
     literal "13" — changed to the same dynamic f-string.
   - Both are the exact drift class FIX BIS-1 already closed for
     `BIS_EER_ENDPOINT`'s key; now closed here too.

3. **`gold/cross_asset/broad_dollar.py`** — docstring-only "PENDING"
   paragraph added, cross-referencing KNOWN_RISKS.md RISK-16. **No
   numeric or behavioral change** — `_RAW_BIS_WEIGHTS_PCT`,
   `_CURRENCY_SYMBOL_MAP`, and the derived `BIS_WEIGHTS` still cover
   only the original 13 currencies. Explicitly did **not** guess or
   interpolate a 14th value — this project's own "read live output,
   don't theorize" discipline (this same file's own docstring, written
   during Cycle 4) applies identically to Gate 1's NZD leg.

4. **`config/instruments_taxonomy.yaml`** — appended an accretive `UPD`
   note under the existing ADR-049 comment block, reconciling it with
   two facts the original note (5 Sep 2026) couldn't have known: (a)
   `compute_broad_dollar()` was in fact built during Cycle 4 (13 Sep
   2026), contradicting that note's own "remains unbuilt" line; (b) the
   12 Sep 2026 Gate 1 extraction predates this ADR-049 note by a week
   and covered only 13 currencies, so `broad_dollar.py` still lacks
   NZD.

## What this does NOT resolve

Gate 1's actual NZD weight *value*. Ovi will run `--extract-weights`
for real on the M1 (the only machine with a route to bis.org — this
sandbox and every prior sandbox on this project have never had one) and
report the output back into this thread, exactly the same
authoring/execution split this whole file's history already follows for
the original 13 currencies. Once reported:

- `broad_dollar.py`'s `_RAW_BIS_WEIGHTS_PCT` gains a 14th key/value.
- `_CURRENCY_SYMBOL_MAP["NZD"] = ("NZD_USD", True)` (`True` because
  NZD_USD, like AUD_USD/EUR_USD/GBP_USD, quotes USD as the *quote*
  currency — a pair RISE means USD weakened, so it must be negated
  before weighting to share the same sign convention as the other 11
  USD-is-base legs).
- `BIS_WEIGHTS`'s renormalization is fully automatic (it re-derives
  `_RAW_WEIGHT_SUM` from whatever keys `_RAW_BIS_WEIGHTS_PCT` holds at
  the time) — no other line in that module will need to change.

## Verification

Full workflow performed in an isolated sandbox first (GitHub clone of
`Ovi-xyz/alpha-factory`, confirmed byte-identical to the live repo
before any edit — same commit, same `tests/COUNT_BASELINE.txt` value,
same 1733/1733 collected/passed-or-known-failed baseline), per this
project's sandbox-first, byte-verified workflow:

- `ast.parse()` clean on every modified `.py` file.
- `grep` swept `scripts/preflight/check_bis_eer_weights.py` and
  `src/gold/cross_asset/` for f-string SQL — none present (not
  applicable to these files, but checked per this project's own G-2
  gate discipline).
- `tests/unit/test_preflight_scripts.py` alone: 66 → 67 passed.
- Full suite: 1733 → **1734** collected. 1732 passed both before and
  after; the same 2 pre-existing failures both before and after
  (`test_check_poetry_env.py::TestArgparseSurface`, environment-only —
  the `poetry` binary is absent from this sandbox, unrelated to this
  fix). **0 regressions.**
- Every touched file mirrored to the live repo via the Filesystem MCP
  connector and byte-diffed against the tested sandbox copy
  immediately after each write (`copy_file_user_to_claude` +
  `diff`) — clean before moving to the next file, per this project's
  mirror-verification pattern. Files touched: `scripts/preflight/
  check_bis_eer_weights.py`, `src/gold/cross_asset/broad_dollar.py`,
  `tests/unit/test_preflight_scripts.py`, `config/
  instruments_taxonomy.yaml` (comment-only, YAML re-parsed clean and
  diffed to confirm this was the *only* change), `KNOWN_RISKS.md`,
  `CHANGELOG.md`, `pyproject.toml` (version + comment; re-parsed as
  TOML, confirmed valid; editable install re-run clean), `tests/
  COUNT_BASELINE.txt`.

## Next step (Ovi, on the M1)

```
python scripts/preflight/check_bis_eer_weights.py --extract-weights
```

Paste the output back into this thread — specifically NZD's row —
so `broad_dollar.py`'s `BIS_WEIGHTS` can be closed out the same way the
original 13 were on 12 Sep 2026.
