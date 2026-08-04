# 2026-08-01 — BIS CBPOL/EER Endpoint Root-Cause Fix (FIX BIS-1)

**Format note:** one file per release, per this project's own convention
(`dev-log/YYYY-MM-DD-topic.md`, never modified after creation).
`CHANGELOG.md` remains the exhaustive per-FIX technical record; this file
is the narrative companion for **v1.13.1 only**.

## Starting state (re-verified empirically)

- Live main and the local filesystem confirmed in sync: `pyproject.toml`
  version `1.13.0` matched on both `raw.githubusercontent.com` and the
  local repo, same commit content (`89efdb9`). This meant a GitHub clone
  could be used as a real, in-sync test environment for this thread —
  not something every prior thread on this project could assume.
- v1.13.0 itself (`GMI_Decision_Document_v7.docx`'s tvdatafeed retirement
  + CPO/RUBBER/TIN/NICKEL proxy adoption) was independently re-verified
  against the live filesystem before this thread started, in response to
  Ovi's "let's start the implementation phase" opener — this closed a gap
  between memory (still describing v1.12.2–v1.12.4) and reality.

## What this thread did

Ovi picked "resolving BIS issues" from a set of offered priorities. This
was **not** one of the options offered — a deliberate choice to work on
the item flagged as lowest-priority/not-directly-implementable ("needs
live network access I don't have"). It turned out to be directly
resolvable anyway, just not by iterating on the existing code — by
actually finding out what BIS's real API looks like.

### The pattern that had already failed twice

Two prior threads (GMI v6, then the 28 Jul 2026 preflight-fixes thread)
each treated the BIS 404s as a URL *path structure* problem (v1 → v2) and
fixed only that. The 28 Jul thread went further and claimed the dataflow
IDs themselves (`WS_CBPOL_D`, `WS_EER_M`) were independently confirmed
correct, citing "a BIS SDMX Python client's dataflow listing" — a source
specific enough to sound authoritative, vague enough that it was never
actually named or checked. The 29 Jul preflight log (real, run on Ovi's
M1) shows both endpoints still 404ing/501ing after that "fix" landed.
Rather than attempt a third guess at the URL shape, this thread went and
found BIS's actual, current, real API — via `web_search`/`web_fetch`, not
by iterating on assumptions already twice disproven.

### Root cause found

The dataflow IDs are `WS_CBPOL` and `WS_EER` — not `WS_CBPOL_D`/
`WS_EER_M`. The "_D"/"_M" suffixes were daily/monthly cadence labels
mistaken for part of the dataflow identifier somewhere back in the
original Data Source & Rates Adjustment v1.0, and every subsequent
thread inherited the mistake without re-deriving it from BIS's own
current material. Frequency is a **key dimension**
(`FREQ.REF_AREA` for CBPOL, `FREQ.TYPE.BASKET.REF_AREA` for EER), not
part of the flow name. Confirmed via three independent sources, not one:

1. `data.bis.org`'s own indexed URLs — 8 CBPOL country pages and 7 EER
   country pages, all served under `BIS,WS_CBPOL,1.0`/`BIS,WS_EER,1.0`,
   none under a `_D`/`_M` variant.
2. A live, working third-party code example (a blog with comments dated
   Aug 2024, still posting through Jul 2026) using the exact
   `/api/v2/data/dataflow/BIS/<FLOW>/1.0/<key>?format=csv` shape for a
   sibling dataflow, `WS_CBTA`.
3. A real SDMX 2025 conference paper with worked examples for a third
   sibling dataflow (`WS_XRU`), confirming both the data-query shape and
   — separately — the `structure/dataflow/...?references=all` shape for
   discovery queries, which explained the EER `--discover` endpoint's 501
   (it was missing the `structure/` segment entirely, a different and
   more specific bug than the data-query 404s).

None of this could be tested live from any sandbox on this project
(`stats.bis.org` has never been in a sandbox network allowlist here) —
this is external-web research, not API access, and is flagged as such
everywhere it matters.

### An unplanned finding along the way

Of the 8 sampled CBPOL countries, 4 are in this platform's own 12-CB
list (GB/BOE, CH/SNB, NO/NORGES, JP/BOJ) — and all 4 came back as
**Monthly**, not Daily, in the samples. This sits in tension with
ADR-010's original rationale for using BIS over FRED for ECB
specifically ("BIS provides daily where FRED only has monthly"). ECB/XM
itself wasn't among the sampled countries, so this doesn't directly
contradict ADR-010 — but it's a real, unplanned data point worth
reviewing once the corrected endpoint is confirmed live, not something
to quietly resolve unilaterally in this thread. Reflected in the key
construction (FREQ deliberately wildcarded rather than hardcoded to `D`)
and flagged explicitly in `KNOWN_RISKS.md` RISK-16 and the `_daily_
resolution()` check's own comments, which were left functionally
unchanged rather than relaxed — "some CBs come back non-daily" is now a
question for ADR-010 review, not a preflight bug to paper over.
**Resolved in a later release — see the v1.13.2 dev-log entry.**

### Verification approach — a step beyond the project's usual pattern

Every prior BIS-related fix on this project was verified by static
review plus the test suite, with live confirmation deferred to Ovi's
next run on real hardware — the honest limit of what a network-
sandboxed thread can do. This thread went one step further: since
`raw.githubusercontent.com` confirmed GitHub `main` was in sync with the
local filesystem, the repo was cloned into a sandbox
(`git clone https://github.com/Ovi-xyz/alpha-factory.git`), a full
`poetry install --with dev` was run (113 packages, clean — notably
*easier* than it would have been before v1.13.0, since the `tvdatafeed`
git dependency that used to be the main install-risk is gone), and the
fix was implemented and tested there FIRST — full suite, coverage, and
all 4 static gates (G-1/G-2/G-3/G-8) — before being replayed, edit for
edit, onto the real repo via the filesystem connector. This doesn't
close the "not live-tested against stats.bis.org" gap (nothing can, from
here) but it does mean every claim about the test suite and gates in
this entry is independently reproduced, not asserted from static
reading.

## Files changed

- `config/bis_cb_rates.yaml` — endpoint corrected, version 1.0 → 1.1.
- `src/bronze/bis_rates_ingester.py` — `_BIS_ENDPOINT` corrected
  (production ingester hardcodes its own copy, doesn't read the YAML, so
  needed an independent fix — same pattern the 28 Jul thread already
  established for *why* this file needs its own copy of the fix).
- `scripts/preflight/check_bis_cbpol_d.py` — `BIS_ENDPOINT` corrected,
  `_daily_resolution()` pass/fail semantics deliberately left unchanged.
- `scripts/preflight/check_bis_eer_weights.py` — both
  `BIS_EER_ENDPOINT_MONTHLY` and `BIS_EER_DATAFLOW_STRUCTURE_URL`
  corrected.
- `tests/unit/test_bis_rates_ingester.py` — new `TestBisEndpoint` class
  (2 tests).
- `tests/unit/test_preflight_scripts.py` — 4 new regression-guard tests
  added, 1 pre-existing test (`test_endpoint_uses_v2_path_structure`,
  which asserted the now-superseded `WS_EER_M` value) rewritten with a
  docstring explaining why, not silently deleted.
- `KNOWN_RISKS.md` — new RISK-16 entry, footer updated.
- `CHANGELOG.md` — new v1.13.1 entry (Indonesian prose + English
  technical identifiers, matching the file's established convention —
  confirmed by re-reading the live v1.13.0 entry before writing, not
  assumed from memory of the project's stated convention, which several
  other files in this repo have drifted away from).
- `pyproject.toml` — version 1.13.0 → 1.13.1 (PATCH: bug fix, no
  interface-contract or Silver/Gold schema change).
- `tests/COUNT_BASELINE.txt` — 1420 → 1426.

## Verification

- Full suite, sandbox clone: **1420 passed (baseline, confirmed exact
  match to Ovi's own `2026-08-01—poetry-logs.txt` run) → 1426 passed, 0
  failed** after the fix and 6 new tests.
- Ran the two directly-affected test files in isolation both before and
  after the fix — confirmed exactly one pre-existing failure
  (`test_endpoint_uses_v2_path_structure`, the expected one) and zero
  unexpected breakage anywhere else in either file.
- Coverage: 81.41% → 81.43%, against the 80% gate.
- Gate G-1 (164 files, `ast.parse` clean), G-2 (0 f-string SQL), G-3
  (`validate_instruments.py` → 699 symbols, Layer 1=640, Layer 2=59, exit
  0 — unaffected by this fix, confirmed rather than assumed), G-8 (0
  glob-scope violations) — all re-run clean.
- Every file edit re-read back after being applied to the real repo via
  the filesystem connector, confirming the diff matched the
  sandbox-verified version exactly.

## What this does NOT resolve

This is a code fix, verified against the test suite and 4 static gates —
**not** verified against the live BIS API, which no sandbox on this
project has ever had a network route to. The next real preflight run
(`check_bis_cbpol_d.py`, `check_bis_eer_weights.py`, on the M1) is what
actually closes this loop, same as every other BIS/tvdatafeed fix in
this project's history. If it turns out any part of the key construction
is still wrong (e.g. BIS's real dimension order or count differs from
what 8–7 sampled portal URLs implied), that will show up as a real,
specific error rather than the previous generic 404 — which is itself
diagnostic progress even in the failure case.

Also unresolved, deliberately not bundled into this fix: whether the
Monthly-vs-Daily finding affects ADR-010 for any specific CB; Gate 1
(ADR-017/018 exact Broad Dollar basket weight *components* — the
corrected EER endpoint gets index *values*, not necessarily the
methodology-appendix weight percentages GMI v6 already flagged as
possibly not machine-readable at all); TYPE (Real vs Nominal) for the
EER query, left wildcarded rather than decided; and RISK-15 (the
pre-existing FRED Track 2 commodity-supplement gap), which remains
exactly as open as before this thread — unrelated dataflow, unrelated
source, deliberately not touched.

## What's still open at the end of this release

- **GMI Wave 1 Cycle 4 — CrossAssetEngine.** Still the next major
  milestone, still not started.
- **RISK-15** — FRED Track 2 commodity supplements, still open.
- **Coverage tranche toward 95%** (`GMI_Decision_Document_v6.docx` §4's
  11-file list) — still not started.
- **Proxy correlation study** for F34.SI/STA.BK/AFM.V/NIC.AX — still
  open.
- **Live confirmation of this thread's fix** — the immediate next step
  once Ovi has a network-enabled environment available. **Addressed in
  the v1.13.2 release — see that dev-log entry.**

## Deliverable

Applied directly to `/Users/opi/alpha-factory` via the filesystem
connector — no zip. All 9 files listed above are live on the local
filesystem as of v1.13.1.
