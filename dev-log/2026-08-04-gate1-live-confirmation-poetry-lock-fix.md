# 2026-08-04 — Gate 1 Discovery Phase Live-Confirmed, `.N.B.` Key Shape Re-Verified, poetry.lock Desync Fixed

**Format note:** one file per release, per this project's own convention
(`dev-log/YYYY-MM-DD-topic.md`, never modified after creation).
`CHANGELOG.md` remains the exhaustive per-FIX technical record; this file
is the narrative companion for **v1.13.4 only**. Continues directly from
the v1.13.3 dev-log entry.

## Starting state

v1.13.3 closed the 13-currency live re-test and located Gate 1's actual
weights file, but left three items explicitly open (RISK-16, "What this
does NOT resolve"): (1) Gate 1's exact per-currency weight *values* —
`--discover-weights` had never been run against the real file, only a
synthetic in-test workbook; (2) the TYPE=Nominal decision's new `.N.B.`
key shape had never been live-tested — the most recent live confirmation
was still against the prior `M..B.` structure; (3) the production
`bronze_bis_rates` ingester's own CSV-parsing path had never been run
end-to-end against a real BIS response.

Separately, unrelated to BIS: `poetry.lock`'s content-hash had gone stale
sometime after the 31 Jul manual `poetry lock` run (which had correctly
dropped tvdatafeed) — the 3 Aug `openpyxl` promotion to an explicit
dependency changed `pyproject.toml` again without a follow-up lock
regeneration, and nobody had run `poetry install` since to notice.

## What this release did

Two independent threads, evaluated and closed in the same session: Ovi
supplied fresh M1 preflight logs (2026-08-04) as project-knowledge
evidence, and separately flagged "a new issue of pyproject.toml" with no
error text attached.

### Gate 1 — `--discover-weights` run for real, structure now known

Ovi ran `check_bis_eer_weights.py --discover-weights` on the M1 for the
first time (no sandbox on this project has ever had a route to
`bis.org`). Result: `weightsb.xlsx` downloaded clean (492,941 bytes),
10 sheets — `1993_1995` through `2020_2022`, each a rolling 3-year
vintage, ~72 rows x 67 cols. Confirms the FAQ's stated 3-year revision
cadence directly (last published vintage: 2020-22; no newer one exists
yet). Every sheet is a symmetric "who weights whom" matrix — row label
= country, column header = currency being weighted, cell = percent
weight — and the scan found all 13 `BROAD_DOLLAR_REF_AREAS` codes
present as **both** row and column entries in **every** one of the 10
sheets (26 total code-position matches per sheet: 13 as column headers
at row 6, 13 as row labels at column 2 — identical positions across all
10 sheets, confirming a stable schema across vintages).

This is real progress, not just "file exists": the scan proves the
layout is exactly what `_discover_weights()` was designed to detect
without assuming, and gives the (row, col) coordinates needed for a
targeted extraction. It does **not** yet give the actual weight
*values* to wire into `BIS_WEIGHTS` — that still needs a follow-up pass
that (a) locates the US row specifically (not itself one of the 13
target codes, so not surfaced by this scan), and (b) reads off that
row's values at the 13 target columns. Gate 1 stays open, moved from
"file located, layout unknown" to "file located, layout fully
characterized, ready for extraction."

### TYPE=Nominal / `.N.B.` key shape — now live-confirmed

Ovi also re-ran plain `check_bis_eer_weights.py` (no flags). All 13
currencies PASS — but at **3,813,875 bytes per currency**, roughly 16x
the 237,188 bytes recorded for the same 13-currency check under the old
`M..B.` key (KNOWN_RISKS.md, 3 Aug). That jump is exactly what
wildcarding FREQ (instead of fixing it to `M`) should produce: the
query now pulls whatever frequency BIS actually has per country — for
these 13, that's daily — instead of being artificially restricted to
monthly. This closes the "not yet live-re-confirmed against this exact
key shape" gap from v1.13.3 outright; it isn't just a re-test of the old
behaviour, the byte-count delta is itself evidence the new key shape is
doing something structurally different, in the expected direction.

`--discover` (structure endpoint) and `check_bis_cbpol_d.py` were also
re-run: 568,951 bytes and 12/12 `daily-resolution=True` respectively —
the structure-endpoint byte count is identical to the 3 Aug figure
already in KNOWN_RISKS.md, and the CBPOL_D obs-count range (6,775 KR
minimum, 24,850 JP maximum) and latest-date range (through 2026-07-29)
both match what's already recorded there exactly. Consistent with BIS's
own stated T+1-T+3 update latency, not a re-run artifact — no new
observations had propagated in the intervening day for any of the 12
central banks. Read as a stability reconfirmation, not new information.

### poetry.lock content-hash desync — found, fixed, fully verified

Ovi's "new issue of pyproject.toml" had no attached error, so this
started from the file itself rather than a stack trace. The 3 Aug
`openpyxl` dependency comment (see v1.13.3) already flagged, in its own
text, that the pyproject.toml edit alone would not regenerate
`poetry.lock` and that no session up to that point had shell access to
verify or fix it. Confirmed via `poetry.lock`'s file-modified timestamp
(31 Jul, one day *after* the 30 Jul tvdatafeed removal — a manual
`poetry lock` run had already dropped tvdatafeed correctly) predating
the 3 Aug `openpyxl` edit. Reproduced the actual failure in an isolated
sandbox (poetry 2.4.1, PyPI network access) against the exact live
files:

```
Error: pyproject.toml changed significantly since poetry.lock was last
generated. Run `poetry lock` to fix the lock file.
```

`tvdatafeed` was already correctly absent from the lock (the 31 Jul run
handled that); the desync was purely the un-regenerated content-hash
after the later `openpyxl` edit. Regenerated with `poetry lock`; diffed
old vs. new lock at the package level: **113/113 packages identical
name and version** — only the `content-hash` metadata line changed. No
pandas/numpy/etc. drift, nothing silently upgraded. Applied to the live
repo as a single-line `edit_file` change (dry-run diffed, applied, read
back, re-diffed against the sandbox-verified copy — byte-identical).
The now-inaccurate "not something this session can execute" note on the
tvdatafeed-removal comment was also corrected in place to record the
actual fix, method, and date.

## Files changed

- `poetry.lock` — content-hash regenerated to match current
  `pyproject.toml`. Zero package/version changes (113/113 unchanged).
- `pyproject.toml` — version 1.13.3 -> 1.13.4 (PATCH: bug fix, no
  interface/schema change); corrected the stale provenance note on the
  tvdatafeed-removal comment.
- `KNOWN_RISKS.md` — RISK-16: TYPE/`.N.B.` key shape moved from "not
  yet live-re-confirmed" to confirmed; Gate 1 subsection updated to
  reflect the now-known file layout; "What this does NOT resolve"
  trimmed to the two items that are actually still open.
- `CHANGELOG.md` — new v1.13.4 entry.

No source code, test, or config/schema files changed this release —
this was evidence review plus one build-tooling bug fix, not feature
work.

## Verification

- Fresh clone of GitHub main (`fce8be9`) spot-checked against known-true
  local state before trusting it: version 1.13.3, tvdatafeed correctly
  archived (not in `src/bronze/`, present under `scripts/archive/`),
  HKD/TWD/NOK present in both instrument config files, `openpyxl`
  declared — all matched. Only `poetry.lock`'s hash was stale there
  too, exactly as expected (the fix hadn't been pushed yet).
- Overlaid the verified `pyproject.toml`/`poetry.lock` onto that clone
  and ran for real, not dry-run: `poetry install --with dev` -> all 113
  packages, `alpha-factory 1.13.4` installed editable, clean.
- `poetry run pytest tests/ -q` -> **1432 passed, 0 failed, 0 error** —
  exact match to `tests/COUNT_BASELINE.txt` (unchanged this release, no
  tests added or removed).
- Coverage: **81.43%**, unchanged, still above the 80% gate.
- Gates G-1 (164 files, 0 syntax errors), G-2 (0 f-string SQL), G-3
  (699 symbols — Layer 1=640, Layer 2=59 — unaffected), G-8 (0
  glob-scope violations) all independently re-run clean against the real
  install.
- `poetry.lock` fix applied to the real repo via a single `edit_file`
  call (dry-run diffed first), then read back and re-diffed against the
  sandbox-verified copy to confirm byte-identical — same discipline as
  the `_discover_weights()` real-file caveat above: never assume a fix
  landed correctly, always read it back.

## What this does NOT resolve

Gate 1's exact per-currency weight *values* — the file's layout is now
fully known, but no code has been written yet to read the US row and
extract the 13 target weights into `BIS_WEIGHTS`. The production
`bronze_bis_rates` ingester's own CSV-parsing path (`_parse_csv()`) —
still not run end-to-end against a real BIS response; only the
preflight scripts' lighter-weight parsing has ever been confirmed live,
and that remains a different code path.

## What's still open at the end of this release

- **Gate 1 extraction pass** — locate the US row in `weightsb.xlsx`'s
  2020-22 sheet, read off the 13 target-currency weight values, wire
  into `BIS_WEIGHTS` (or a config-driven equivalent) replacing the
  current hand-approximated 10-pair weights in Architecture v2.0 §7.2.
  Now unblocked — the layout is known — but not started.
- **`bronze_bis_rates` end-to-end live test** — still open, untouched
  this release.
- **GMI Wave 1 Cycle 4 — CrossAssetEngine.** Still not started.
- **RISK-15** — FRED Track 2 commodity supplements, still open.
- **Coverage tranche toward 95%** — still not started.
- **Proxy correlation study** for F34.SI/STA.BK/AFM.V/NIC.AX — still
  open.

## Process note

`Alpha_Factory_Development_Log.md` (the single-file, pre-dev-log-folder
convention document, last touched 2026-07-24) was referenced by name
earlier in this same thread as if it were still the living document —
it isn't, and has been superseded by this `dev-log/` folder structure
since. Corrected here rather than silently dropped: no update was made
to that file, and none should be — it is now purely historical.

## Deliverable

Applied directly to `/Users/opi/alpha-factory` via the filesystem
connector — no zip.
