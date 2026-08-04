# 2026-08-03 — Gate 1 Weights File Located, TYPE Decision, 13-Currency Live Re-Confirmation

**Format note:** one file per release, per this project's own convention
(`dev-log/YYYY-MM-DD-topic.md`, never modified after creation).
`CHANGELOG.md` remains the exhaustive per-FIX technical record; this file
is the narrative companion for **v1.13.3 only**. Continues directly from
the v1.13.2 dev-log entry.

## Starting state

v1.13.2 landed with three items explicitly still open: (1) the
13-currency EER expansion not yet live-tested (only the prior 10-currency
version had been), (2) Gate 1 (ADR-017/018 exact Broad Dollar weight
components) unresolved, (3) TYPE (Real vs Nominal) for the EER query
left deliberately wildcarded, undecided.

## What this release did

Ovi asked directly to continue resolving all three. Two distinct kinds of
work: one was closing a loop with evidence Ovi had already generated
(the 13-currency re-test), the other two required genuine new research.

### 13-currency live re-confirmation

Ovi re-ran the preflight scripts against the v1.13.2 code. Result:
`check_bis_eer_weights.py` — **all 13 REF_AREA codes PASS**, 237,188
bytes returned per check (up from 182,410 bytes at 10 currencies — the
larger response reflects the wider key). `check_bis_cbpol_d.py` —
identical 12/12 `daily-resolution=True` result, confirming stability
across repeated runs. This closes the "not yet re-run against 13
currencies" gap from v1.13.2 outright.

### Gate 1 — the actual weights file, found

GMI v6 had framed Gate 1 as possibly unresolvable via any API — BIS's
methodology might publish weights only as "a documentation artifact, not
necessarily a queryable SDMX series." Web research this release found
the real answer: `data.bis.org/topics/EER` (a server-rendered page,
unlike the JS-SPA pages encountered everywhere else on this project)
links directly, under its own "Methodology" section, to a downloadable
weights table — `https://www.bis.org/statistics/eer/weightsb.xlsx`
(Broad, 64 economies; Narrow only covers 26/27 core economies and would
exclude IDR/HKD/TWD, so Broad is the correct one, matching the platform's
existing BASKET=B choice). `web_fetch` confirmed this is genuinely
reachable and a real `.xlsx` (mime type
`application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`, not
a redirect or error page) — Gate 1's weight components ARE
machine-readable, just not through the SDMX API this script otherwise
uses. Also learned, same page: BIS weights are **time-varying on a
3-year basis** (vintages 1993-95 through 2017-19 per BIS's own FAQ; the
2017-19 vintage has been in continuous use since, until the next 3-year
update publishes) — there's no single permanent "exact weight," but
there is a specific, nameable current vintage.

Could not parse the actual xlsx content through available tools —
`web_fetch` returns it as opaque binary, and `bis.org` isn't in any
sandbox's network allowlist on this project (checked: not reachable from
`bash_tool` either). So this is genuine progress (file located, confirmed
real and reachable) but **not full closure** — the exact per-currency
weight values still require someone with network access to actually open
the file. Wrote `check_bis_eer_weights.py::_discover_weights()` to make
that the next trivial step rather than a from-scratch investigation:
downloads the file, reports real sheet names/dimensions, a structural
sample, and a scan for our own currency/REF_AREA codes — deliberately
NOT assuming a row/column layout, since guessing wrong here would just be
a new version of the exact problem this whole BIS thread has been fixing
(WS_CBPOL_D, WS_EER_M, the missing `structure/` segment — three
confident-but-wrong guesses already found and corrected). Same
two-phase discover-then-extract pattern already established for the API
structure itself (`--discover`).

`openpyxl` was already resolving transitively (via `polars[all]`'s
optional `read_excel` backend) but had zero explicit imports anywhere —
promoted to an explicit direct dependency in `pyproject.toml`, matching
the exact precedent `jsonschema` set in Decision B Step 3 ("declare what
you import").

### TYPE decision — Nominal

Previously left wildcarded pending a decision; decided this release.
**Nominal**, for two independent, converging reasons: (1) DXY itself —
the index this platform's Broad Dollar Index is explicitly designed as a
companion/extension of (Architecture v2.0 §7.2) — is a nominal
currency-value index, not inflation-adjusted; pairing a Real EER against
a Nominal DXY under one "Dollar strength" umbrella would conflate two
different concepts. (2) The same `data.bis.org/topics/EER` page that
revealed the weights file also states Daily-frequency EER data exists
**only for Nominal indices, never Real** ("the latter available only as
nominal indices") — and this platform's Layer 2 anchors are specified at
Daily cadence (Architecture v2.0 §7.2: "Cadence: Daily (same as forex)"),
so Nominal is the only choice that can actually deliver that cadence at
all, independent of the DXY-consistency argument.

Given TYPE is now fixed, FREQ was changed from fixed-`M` to wildcarded —
mirroring the exact reasoning already applied to `check_bis_cbpol_d.py`'s
key: request whatever frequency BIS actually has per country rather than
assume, so genuinely-available daily data comes through without risking
a false failure on any currency that turns out to be monthly-only.
Renamed `BIS_EER_ENDPOINT_MONTHLY` → `BIS_EER_ENDPOINT` accordingly (no
longer accurately describable as monthly-only). **This new key shape has
not yet been live-tested** — the 13-currency confirmation above was
against the prior `M..B.` structure, before this decision was
implemented; that re-test is the natural next preflight run.

## Files changed

- `scripts/preflight/check_bis_eer_weights.py` — module docstring
  rewritten (CANNOT-resolve section replaced with the real Gate 1
  finding; TYPE decision documented with full rationale; usage/exit-code
  section updated); `BIS_EER_ENDPOINT_MONTHLY` renamed to
  `BIS_EER_ENDPOINT`, key changed from `M..B.` to `.N.B.`; new
  `BIS_EER_WEIGHTS_BROAD_URL` constant; new `_discover_weights()`
  function; new `--discover-weights` CLI flag; stale end-of-run NOTE
  text updated to point at the new flag.
- `pyproject.toml` — `openpyxl` added as an explicit direct dependency;
  version 1.13.2 → 1.13.3 (PATCH: bug/gap-closure work, no
  interface-contract or schema change).
- `tests/unit/test_preflight_scripts.py` — 2 existing tests updated for
  the rename/key-shape change; 1 new test for the weights-file URL; 4
  new tests for `_discover_weights()` (download failure, unparseable
  content, a synthetic-workbook scan proving the parsing logic itself is
  correct, and CLI wiring).
- `tests/COUNT_BASELINE.txt` — 1427 → 1432.
- `KNOWN_RISKS.md` — RISK-16 updated with the 13-currency
  re-confirmation, the TYPE decision, and a new Gate 1 subsection framed
  honestly as "substantially advanced, not yet closed."
- `CHANGELOG.md` — new v1.13.3 entry.

## Verification

- Full suite, sandbox clone: 1427 → 1432 passed, 0 failed (5 new tests).
- Coverage unchanged at 81.43%, still above the 80% gate.
- Gates G-1 (164 files clean), G-2 (0 f-string SQL), G-3 (699 symbols,
  unaffected), G-8 (0 glob-scope violations) all re-run clean.
- Given the scope of the diff (roughly 330 changed lines in the main
  script, 155 in its test file), applied to the real repo via whole-file
  `write_file` rather than targeted `edit_file` calls — after the
  ambiguous-match mistake found and fixed in the v1.13.2 release, a
  full-file overwrite of an already fully-verified sandbox version was
  judged the lower-risk path for a diff this size, not a shortcut around
  verification. Spot-read the real files back afterward (head and tail)
  to confirm no truncation or encoding corruption.
- `_discover_weights()`'s scanning logic is tested against a *synthetic*
  in-memory workbook constructed in the test itself, not the real BIS
  file — this proves the code correctly finds known codes in a workbook
  with a known layout, but says nothing about whether it will find
  anything useful in BIS's actual, unknown layout. That's explicit in
  both the test's own docstring and this entry.

## What this does NOT resolve

Gate 1's exact per-currency weight *values* — `_discover_weights()` is
authored and unit-tested but has not been run against the real file from
any sandbox on this project (no route to `bis.org`). The new
`.N.B.`-keyed `BIS_EER_ENDPOINT` has not been live-tested — the most
recent live confirmation (13 currencies, above) was against the prior
key shape. The production `bronze_bis_rates` ingester's own CSV-parsing
path has still not been run end-to-end against a real BIS response.

## What's still open at the end of this release

- **GMI Wave 1 Cycle 4 — CrossAssetEngine.** Still not started.
- **RISK-15** — FRED Track 2 commodity supplements, still open.
- **Coverage tranche toward 95%** — still not started.
- **Proxy correlation study** for F34.SI/STA.BK/AFM.V/NIC.AX — still open.
- **`--discover-weights` run on real hardware** — the immediate next step
  for Gate 1; will reveal the actual xlsx layout and enable a targeted
  extraction pass in a follow-up release.
- **Live re-test of the new `.N.B.` key shape** — needed before the TYPE
  decision can be called fully verified, not just decided and coded.

## Process note

This release's dev-log entry, and the two before it
(`2026-08-01-bis-endpoint-root-cause-fix.md`,
`2026-08-02-bis-live-confirmation-dollar-basket-completion.md`), replace
a single merged file (`2026-08-02-bis-endpoint-root-cause-fix.md`,
originally created for v1.13.1 and then incorrectly appended to twice
more as v1.13.2 and v1.13.3 work landed in the same continuous
conversation) that violated this project's own "one dev-log file per
release, never modified after creation" convention. The merged file has
been moved to `dev-log/archive/` rather than deleted, preserving the
audit trail while correcting the going-forward structure. Flagged here
explicitly rather than silently fixed.

## Deliverable

Applied directly to `/Users/opi/alpha-factory` via the filesystem
connector — no zip.
