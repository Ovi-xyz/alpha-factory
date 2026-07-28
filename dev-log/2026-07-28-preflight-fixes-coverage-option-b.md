# 2026-07-28 — Preflight Findings Fixed, Coverage Gate Option B, MXN→IDR

**Format note:** continuing the one-file-per-thread dev-log convention
started 2026-07-27. `CHANGELOG.md` remains the exhaustive per-FIX
technical record; this file is the narrative companion.

## Starting state (re-verified empirically)

- Live main: commit `9d0fe51` — confirmed this is ADR-028 (poetry
  bootstrap check) landed on top of the prior thread's preflight-module
  package. Working copy synced via `git reset --hard` + `git pull
  --ff-only` after an earlier local edit collided with the same file
  Ovi had already applied.
- `alpha-factory_preflight_logs___28_July_2026.txt` (project knowledge):
  a real run of all 5 preflight scripts (3 original + 2 authored last
  thread) against live yfinance/tvdatafeed/Finnhub/BIS from the M1
  hardware — the first time any of ADR-025's or last thread's scripts had
  actually executed against live data.

## What this thread did

Four requests, worked in the order that let each inform the next:

### 1. CI Coverage Gate — Option B

`GMI_Decision_Document_v6.docx` §4 presented Option A (jump to 95% now,
CI red until a large tranche lands) vs. Option B (honest intermediate
value now, ratchet up as tranches land). Ovi chose B. Set
`--cov-fail-under=80` — about 2 points under the real, re-measured
81.98% (not the stale 81.97% from two threads ago), enough headroom for
normal fluctuation without being decorative. 95% remains the target;
getting there is still the separate, larger tranche work from Decision C
onward, not done in this thread.

### 2. Preflight log findings — investigated and fixed, not just read

Every FAIL in the log was chased to a real root cause rather than
patched at the symptom:

- **`poetry install` scope, not this run's concern** — the log's
  `TV_USERNAME / TV_PASSWORD not set` and `FINNHUB_API_KEY not set`
  outputs looked at first like the same class of problem ADR-028 (last
  thread) already fixed, but they're a different bug: **nothing in this
  repo has ever called `load_dotenv()`**, despite `python-dotenv` being a
  declared dependency since Grand Design v1.2 §12.3 (confirmed by grep —
  zero hits, repo-wide, before this fix). A filled `.env` file only ever
  reached `os.getenv()` if the shell had separately exported it. Fixed at
  the two levels that actually matter: `src/runner.py` (the real
  production entry point — every job dispatched through it now gets
  `.env` loaded once, covering all 10 bronze ingesters transitively) and
  each of the 5 preflight scripts directly (they bypass `runner.py`
  entirely, so each needed its own call). This is a bigger fix than the
  preflight scripts alone — it's the first time `.env` has ever actually
  been loaded anywhere in this codebase.

- **`NICKEL` (`NI=F`) — confirmed 404, deferred rather than patched with
  a guess.** Web search found no working yfinance ticker for LME Nickel
  (unlike `COPPER`/`HG=F`, which is COMEX-listed and reliable). Rather
  than invent a replacement ticker I can't verify, `NICKEL` is now
  deferred (`context_available: false`) following the exact pattern
  already established for `TIN` — same class of gap (needs tvdatafeed LME
  routing, unverified), not yet added to `check_tvdatafeed_symbols.py`'s
  routing table since that specific symbol/exchange pairing needs the
  same empirical verification the other four still need.

- **`SSEC` (`^SSEC`) — confirmed 404, fixed with a verified replacement.**
  Web search confirmed Yahoo Finance's actual symbol is `000001.SS`
  (Shanghai suffix convention — same shape as `.JK` for Jakarta already
  used elsewhere in this file). `^SSEC` is a ticker other vendors use, not
  yfinance's.

- **BIS API — both preflight scripts AND the real production ingester
  were broken.** `check_bis_cbpol_d.py`'s 404 turned an *inconclusive*
  signal from last thread (a v1-vs-v2 URL structure note, deliberately
  not acted on then) into a *confirmed* one: BIS's own current docs
  (`data.bis.org/help/legal`) point exclusively at `api-doc/v2/`, a real
  working v2 example was found for a different dataflow, and the v1
  endpoint now empirically 404s. Fixed the URL structure in
  `check_bis_cbpol_d.py`, `check_bis_eer_weights.py` (also removing
  `WS_EER_D`, whose *dataflow-structure* query 404'd too — a much
  stronger signal than a data-query 404 that it doesn't exist at all, not
  just a key-syntax problem — leaving `WS_EER_M` as the sole target), and
  — the one that actually matters in production —
  **`src/bronze/bis_rates_ingester.py`**, which hardcodes its own copy of
  the endpoint and does not read `config/bis_cb_rates.yaml`'s `endpoint:`
  field at all. The real `bronze_bis_rates` job has been hitting this
  same 404 in production, not just the diagnostic script.

### 3. MXN → IDR

`check_bis_eer_weights.py`'s `BROAD_DOLLAR_REF_AREAS` was carried over
unmodified from Architecture v2.0 §7.2's original `BIS_WEIGHTS` dict —
including `MXN`, a generic EM-currency placeholder from before this
platform's Indonesia-specific work (ADR-013–018) existed. Confirmed via
repo-wide grep: this script was `MXN`'s only occurrence anywhere.
Replaced with `IDR` (`REF_AREA=ID`) per Ovi's instruction — already a
Layer 1 forex pair (`USD_IDR`), already referenced in
`instruments_taxonomy.yaml`'s own comments as part of the *current*
Broad Dollar basket design, and Bank Indonesia is already BIS-covered via
`context_rates_em_cb`.

**Found, flagged, not silently expanded:** while fixing this, the same
comments in `instruments_taxonomy.yaml` show the *actual* current basket
design is 13 currencies (the original 6 + `IDR` + the 6-currency
`context_dollar_basket` group: `CNH`/`KRW`/`SGD`/`HKD`/`TWD`/`NOK`), not
the 9 this script now checks. Left as a documented gap in the script's
own docstring rather than guessing the remaining 3 REF_AREA codes
unasked — Ovi's instruction was specifically MXN→IDR.

### Side effects of the NICKEL deferral — expected, not masked

Deferring `NICKEL` moved Layer 2's active/deferred split from 56/3 to
55/4, and the merged-universe ceiling from 696 to 695. This broke 10
tests across `test_instrument_loader.py`, `test_context_anchors.py`, and
`test_full_system.py` that hardcoded the old numbers — all updated to
the new, correct values (the same treatment the original TIN/CPO/RUBBER
deferrals got). None of these were bugs being masked; all were the
expected, mechanical consequence of one real instrument's status
changing, caught immediately by the existing test suite doing exactly
its job.

## Verification

- Full suite: **1484 passed, 0 failed** (1474 baseline + 10 new tests:
  5 covering the 2 preflight scripts that had zero coverage since their
  own authoring thread, 1 locking in MXN removed/IDR added, 1 locking in
  the v2 endpoint, plus the CLI/logic tests each new script needed).
- Coverage: **81.98%** against the new 80% gate (was 81.97%/70% gate).
- Gates re-run manually: G-1 (11 changed `.py` files, `ast.parse` clean),
  G-2 (0 f-string SQL), G-3 (`validate_instruments.py` → still 699,
  exit 0 — NICKEL's deferral doesn't change the total, only its active/
  deferred split), G-8 (0 glob-scope violations). Modified `ci.yml`
  re-parsed as valid YAML.
- Both YAML config files (`instruments_identity.yaml`,
  `instruments_taxonomy.yaml`) parse clean; `validate_instruments.py`'s
  own required-field check (`deferred_reason`/`planned_wave`) correctly
  caught a first-draft omission on `NICKEL` before this was called done.

## What's still open (unchanged unless noted)

- **Decision C-style coverage tranche toward 95%** — the 11-file list
  from `GMI_Decision_Document_v6.docx` §4 (`quality_validator.py`,
  `market_ingester.py`, `job_registry.py`, and the rest) — still not
  started. Option B is now live; the tranche work to actually earn the
  next ratchet up is separate, sizable work, same reasoning as before.
- **NICKEL's tvdatafeed routing** — needs the same ticker/exchange
  verification as `TIN`/`RUBBER`/`CPO`, not yet added to
  `check_tvdatafeed_symbols.py`'s scope.
- **The 3-currency gap in `check_bis_eer_weights.py`** (`HKD`/`TWD`/
  `NOK`) — flagged in the script's own docstring, not fixed; out of
  scope for the specific MXN→IDR ask.
- **BIS v2 URL structure itself** — stronger evidence than last thread,
  but still not a live-confirmed 200 response from this sandbox (no
  network route to `stats.bis.org` here either). Worth a 30-second check
  the next time any BIS preflight script actually runs.
- **Gate 1 (exact Broad Dollar weight components)** — still open;
  `check_bis_eer_weights.py` confirms index reachability, not weights,
  per its own documented scope.
- GMI Wave 1 Cycle 4 (CrossAssetEngine) — not started, untouched.
  Trading Engine — permanently out of scope per Grand Design §0.

## Deliverable

`alpha-factory-v1_12_4-changed-files.zip` — `MANIFEST.md` + `CHANGES.diff`
+ all new/modified files, on top of `9d0fe51`. Not yet applied to live
main — no push access in any session to date.
