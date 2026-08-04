# 2026-08-02 — BIS Fix Confirmed Live + HKD/TWD/NOK Dollar Basket Completion

**Format note:** one file per release, per this project's own convention
(`dev-log/YYYY-MM-DD-topic.md`, never modified after creation).
`CHANGELOG.md` remains the exhaustive per-FIX technical record; this file
is the narrative companion for **v1.13.2 only**. Continues directly from
the v1.13.1 dev-log entry (`2026-08-01-bis-endpoint-root-cause-fix.md`).

## Starting state

v1.13.1 landed: `WS_CBPOL_D` → `WS_CBPOL`, `WS_EER_M` → `WS_EER`, plus the
EER `--discover` structure-prefix fix. Code-fixed and test-verified
(1426 passed), but explicitly flagged as **not yet confirmed against the
live BIS API** — no sandbox on this project has ever had a network route
to `stats.bis.org`.

## What this release did

### Live confirmation

Ovi ran all 4 current preflight modules on the M1 against the v1.13.1
fix and shared the resulting logs. This closed the one gap the fix
couldn't close from a sandbox: **`check_bis_cbpol_d.py` returned all 12
REF_AREA codes PASS with `daily-resolution=True`** (real observation
counts, current dates through 2026-07-29); **`check_bis_eer_weights.py
--discover` succeeded** (568,951 real bytes from the corrected
`structure/dataflow/BIS/WS_EER/1.0` endpoint, previously a 501); **`check_
bis_eer_weights.py` returned all 10 REF_AREA codes PASS** (182,410 bytes
each). This also resolved the Monthly-vs-Daily concern raised in
v1.13.1's dev-log more favorably than expected: the 4 sampled central
banks (GB/CH/NO/JP) that looked Monthly-only from `data.bis.org`'s
portal turned out to have Daily data available too once queried with
FREQ wildcarded — ADR-010's original daily-resolution rationale is now
empirically confirmed for the full 12-CB set, not contradicted.
`KNOWN_RISKS.md` RISK-16 updated from "FIXED (code), pending live
confirmation" to "RESOLVED (confirmed live)."

### HKD/TWD/NOK dollar basket completion

Separately, Ovi pointed directly at a gap the 28 Jul thread had
explicitly flagged and left alone ("Ovi's instruction was specifically
MXN->IDR"): `check_bis_eer_weights.py`'s `BROAD_DOLLAR_REF_AREAS` was
still missing HKD/TWD/NOK, 10 of the 13 currencies the *current* Broad
Dollar basket design actually calls for. Added all three. While making
this change, found a second, more structural issue worth fixing at the
same time: the endpoint's key was a hand-duplicated literal string,
separate from the dict — adding entries to the dict alone, without
changing the key construction, would have left the three new currencies
permanently unfetched while `_check_one()` kept confidently reporting
"not present," indistinguishable from a genuine API failure. Refactored
the key to build itself from `BROAD_DOLLAR_REF_AREAS.values()`, closing
that whole bug class structurally rather than just adding three more
entries to a value that could drift out of sync again next time.

### A placement mistake, caught and fixed in the same pass

Applying the new regression test to the real repo, a short `oldText`
match (`def test_endpoint_uses_correct_dataflow_id(self):`) turned out
not to be unique — that exact method name exists in both
`TestCheckBisCbpolD` and `TestCheckBisEerWeights` (one asserting
`WS_CBPOL`, the other `WS_EER`) — and the edit landed in the wrong
class. The sandbox copy, edited moments earlier with a longer,
genuinely-unique match, never had this problem — the discrepancy between
the two is what surfaced it. Fixed immediately with a properly-scoped
edit before this was called done, and recorded here rather than quietly
corrected without a trace: a passing test suite does not, by itself,
prove correct placement — this specific test was self-contained enough
(it does its own `import check_bis_eer_weights` inside the method body)
that it would have kept passing indefinitely in the wrong class without
anyone noticing.

## Files changed

- `scripts/preflight/check_bis_eer_weights.py` — `BROAD_DOLLAR_REF_AREAS`
  gains HKD/TWD/NOK (13 total); `BIS_EER_ENDPOINT_MONTHLY` refactored to
  build its key from the dict's `.values()`; module docstring and the
  28-Jul "flagged, not fixed" note updated to RESOLVED; stray "four
  scripts" count corrected to "three" (tvdatafeed archival left 4 total,
  not 5); two stale `WS_EER_M` references in `_discover()`'s docstring
  and CLI help text fixed.
- `tests/unit/test_preflight_scripts.py` — new
  `test_hkd_twd_nok_completes_dollar_basket` regression guard (placed
  correctly in `TestCheckBisEerWeights` after the placement mistake was
  caught and fixed).
- `KNOWN_RISKS.md` — RISK-16 updated: status → RESOLVED (confirmed
  live), new "Live confirmation" and "HKD/TWD/NOK added" subsections,
  footer updated.
- `CHANGELOG.md` — new v1.13.2 entry.
- `pyproject.toml` — version 1.13.1 → 1.13.2 (PATCH: bug fix + scope
  completion, no interface-contract or schema change).
- `tests/COUNT_BASELINE.txt` — 1426 → 1427.

## Verification

- Full suite, sandbox clone: 1426 → 1427 passed, 0 failed (1 new test).
- Coverage unchanged at 81.43%, still above the 80% gate.
- Gates G-1/G-2/G-3/G-8 all re-run clean.
- Sandbox and real-repo copies diffed directly against each other (not
  just re-read independently) after the placement-mistake fix, to
  positively confirm the correction landed in the right class this time
  — a dry-run `edit_file` match count of exactly 1 was used as the
  confirmation signal, not just "tests pass."

## What this does NOT resolve

**Not yet re-run live:** the 13-currency EER expansion (the live
confirmation above covers only the 10-currency set that was actually
tested against the API); Gate 1 (exact Broad Dollar weight
*components* — the 568,951-byte discover payload hasn't been manually
inspected for a weight-bearing dimension); TYPE (Real vs Nominal) for
the EER query, still deliberately wildcarded, not decided; and the
production `bronze_bis_rates` ingester's own CSV-parsing path
end-to-end (only the preflight scripts' lighter parsing has been
confirmed live so far — a different code path).

## What's still open at the end of this release

- **GMI Wave 1 Cycle 4 — CrossAssetEngine.** Still not started.
- **RISK-15** — FRED Track 2 commodity supplements, still open.
- **Coverage tranche toward 95%** — still not started.
- **Proxy correlation study** for F34.SI/STA.BK/AFM.V/NIC.AX — still open.
- **13-currency EER live re-test, Gate 1, TYPE decision** — the three
  items directly above. **Addressed in the v1.13.3 release — see that
  dev-log entry.**

## Deliverable

Applied directly to `/Users/opi/alpha-factory` via the filesystem
connector — no zip.
