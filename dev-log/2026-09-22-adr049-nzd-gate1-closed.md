# ADR-049 Gate 1 CLOSED: NZD's real Broad Dollar weight extracted and wired

**Date:** 22 Sep 2026
**Version:** v1.18.7 → v1.18.8 (PATCH)
**Related:** ADR-049 (chat thread, 5 Sep 2026), KNOWN_RISKS.md RISK-16
("Gate 1 CLOSED" subsections — 12 Sep 2026 original, 22 Sep 2026 this
entry), dev-log/2026-09-22-adr049-nzd-broad-dollar-gap.md (the script fix
this closure builds on, same session)

## Context

The prior dev-log entry (same session) fixed `scripts/preflight/
check_bis_eer_weights.py` so `--extract-weights` would actually look for
NZD's column — it had never been a target currency in
`BROAD_DOLLAR_REF_AREAS` at all. That entry could not close Gate 1 for
NZD, only unblock it: the actual weight value requires running the
script for real against `bis.org`, which no sandbox on this project has
network access to — only Ovi's M1 does.

## What Ovi ran

```
python scripts/preflight/check_bis_eer_weights.py --extract-weights
```

Real output, `2020_2022` vintage (BIS's 3-year-cycle table — still the
most recent; no `2023_2025` sheet exists yet):

```
Currency   REF_AREA  Weight in US Broad EER basket (%)
  AUD      AU        0.326985
  CAD      CA        8.054349
  CHF      CH        2.885704
  CNH      CN        22.579565
  EUR      XM        16.048545
  GBP      GB        2.939921
  HKD      HK        0.026992
  IDR      ID        0.929609
  JPY      JP        5.897981
  KRW      KR        4.183282
  NOK      NO        0.168854
  NZD      NZ        0.069725
  SGD      SG        1.341145
  TWD      TW        3.216667

Sum of these 14 target-currency weights: 68.669322
All 14 target-currency weights extracted successfully.
```

## Verification against the prior 12 Sep 2026 run — empirical, not assumed

Compared this run's 13 pre-existing currencies against the original 12
Sep 2026 extraction (`gold/cross_asset/broad_dollar.py`'s pre-existing
`_RAW_BIS_WEIGHTS_PCT`) digit for digit: **identical for all 13**. This
is expected (same static `2020_2022` vintage sheet, not re-computed by
BIS between the two runs) but was checked directly rather than assumed.

The printed line itself — "Sum of these **14** target-currency weights"
— is also a direct empirical confirmation that the prior patch's fix
(replacing two hardcoded `"13"` literals in this script's own output
with `len(BROAD_DOLLAR_REF_AREAS)`) behaves correctly against a real
run, not just against the isolated sandbox test that exercised it with
a synthetic 2-currency dict.

Sum cross-check: `68.599597` (prior 13-currency sum) `+ 0.069725`
(NZD) `= 68.669322` — matches the script's own printed total exactly.

## Fix — `gold/cross_asset/broad_dollar.py`

1. `_RAW_BIS_WEIGHTS_PCT["NZD"] = 0.069725` — inserted alphabetically
   (between `NOK` and `SGD`), matching the dict's existing ordering.
2. `_CURRENCY_SYMBOL_MAP["NZD"] = ("NZD_USD", True)` — `True` because
   NZD_USD, like AUD_USD/EUR_USD/GBP_USD, quotes USD as the *quote*
   currency (a pair RISE means USD weakened), so it must be negated
   before weighting to share this module's established sign convention:
   positive contribution == USD strengthened against that currency.
   Confirmed against `instruments_taxonomy.yaml`'s own Layer 1 forex
   block this session, not assumed from the currency name alone.
3. `BIS_WEIGHTS`'s dict-comprehension renormalization required **no
   code change** — it already re-derives `_RAW_WEIGHT_SUM` from whatever
   keys `_RAW_BIS_WEIGHTS_PCT` holds at import time, exactly as the
   module's own prior "PENDING" docstring paragraph anticipated.
4. `GATE_1_EXTRACTION_DATE`: `"2026-09-12"` → `"2026-09-22"` (now the
   complete 14-currency source). `BIS_WEIGHTS_VINTAGE` unchanged
   (`"2020_2022"` — same sheet both times, only the target-currency set
   changed).
5. Module docstring: added a new "RESOLVED (chat thread, 22 Sep 2026)"
   paragraph with the full verbatim extraction output, placed alongside
   (not replacing) the prior "PENDING" paragraph — accretive, matching
   this project's documentation convention. Also updated two
   forward-looking accuracy statements that would otherwise go stale:
   the `DECISION` paragraph's "13 weights" / "~51 economies" → "14
   weights" / "~50 economies"; the `SIGN CONVENTION` paragraph's "AUD,
   EUR, and GBP" / "other 10" → "AUD, EUR, GBP, and NZD" / "other 10"
   (unchanged count, NZD joins the negated group); `load_fx_returns()`'s
   docstring "13 BIS_WEIGHTS symbols — 6 Layer 1 forex majors" → "14 ...
   7 Layer 1 forex majors".

## New test file — `tests/unit/test_broad_dollar.py`

This module had **no dedicated unit test file** before this session —
only indirect coverage via `test_forecast_module.py` /
`test_correlation_module.py`, which exercise its outputs but never
assert its own invariants directly. Added 12 tests across 4 classes:

- `TestRawBisWeights` — 14-currency count, NZD's exact raw weight
  (`0.069725`, matched against the live extraction, not a rounded or
  re-derived value), all 13 pre-existing values unchanged.
- `TestCurrencySymbolMap` — NZD → `("NZD_USD", True)`; the post-ADR-049
  negated set is exactly `{AUD, EUR, GBP, NZD}`; every raw-weight key has
  a symbol-map entry (guards the dict comprehension against a bare
  `KeyError` at import time).
- `TestBisWeightsRenormalization` — 14 symbols in `BIS_WEIGHTS`;
  `sum(|values|) == 1.0`; `NZD_USD`'s signed weight is negative; the
  full expected 14-symbol key set.
- `TestGate1Metadata` — `GATE_1_EXTRACTION_DATE == "2026-09-22"`;
  `BIS_WEIGHTS_VINTAGE` unchanged.

Follows this codebase's established import convention
(`import src.gold.cross_asset.broad_dollar as bd`, matching
`test_correlation_module.py`) rather than a local `sys.path` hack — the
first draft used the latter and was corrected before mirroring.

## A note on floating-point summation order (checked, not a bug)

`sum(bd._RAW_BIS_WEIGHTS_PCT.values())` in this sandbox evaluates to
`68.669324`, not the `68.669322` the extraction script itself printed —
a `2e-6` gap. Verified via `math.fsum()` and exact `decimal.Decimal`
arithmetic that `68.669324` is the mathematically correct sum of the 14
values as given; the `2e-6` discrepancy is pure floating-point
summation-order noise (`sum()` over floats is not perfectly
associative), not a data-entry error. Confirmed this is not new: the
same class and magnitude of gap already existed, undocumented, in the
pre-existing 13-currency comment (`# 68.599597` vs. the
Decimal-exact `68.599599`) before this session touched anything. Kept
the code comment matching the extraction tool's own printed figure
(`# 68.669322`), consistent with how the original 12 Sep 2026 entry
already handled this exact non-issue. `_RAW_WEIGHT_SUM` itself is always
computed at runtime (`sum(_RAW_BIS_WEIGHTS_PCT.values())`), so this only
affects a documentary comment, never the actual renormalization the code
performs.

## Verification

- `ast.parse()` clean on `broad_dollar.py` and `test_broad_dollar.py`.
- Numeric cross-checks run directly against the real extracted value
  before writing any test: 14 currencies, NZD's weight matches exactly,
  `BIS_WEIGHTS` keys correct, `sum(|BIS_WEIGHTS.values()|) == 1.0` to
  within `1e-9`, `NZD_USD`'s signed weight is negative.
- `tests/unit/test_broad_dollar.py` alone: 12/12 passed.
- `test_forecast_module.py` + `test_correlation_module.py` (the modules
  that consume `broad_dollar.py`'s output): 37/37 passed, unaffected.
- Full suite: 1734 → **1746** collected (+12, the new test file). 1744
  passed both before and after; the same 2 pre-existing failures both
  before and after (`test_check_poetry_env.py::TestArgparseSurface`,
  environment-only — `poetry` absent from this sandbox). **0
  regressions.**
- Every touched file mirrored to the live repo via the Filesystem MCP
  connector and byte-diffed against the tested sandbox copy immediately
  after each write — including, this time, immediately after the
  KNOWN_RISKS.md edit specifically (see "Process correction" below).

## Files touched

`src/gold/cross_asset/broad_dollar.py` (wiring + docstring),
`tests/unit/test_broad_dollar.py` (new), `tests/COUNT_BASELINE.txt`
(1734 → 1746), `config/instruments_taxonomy.yaml` (ADR-049 comment,
accretive UPD note), `KNOWN_RISKS.md` (RISK-16, new "Gate 1 CLOSED (22
Sep 2026)" subsection), `CHANGELOG.md` (v1.18.8 entry), `pyproject.toml`
(version + comment). Plus date-label corrections (see below) across
`scripts/preflight/check_bis_eer_weights.py`,
`tests/unit/test_preflight_scripts.py`, and this dev-log file's own
filename/header.

## Date-label correction (own error, found and fixed this session)

The immediately-prior patch (script fix) dated several comments/
docstrings/the dev-log filename "21 Sep 2026." The actual date — both
by the system's own current date and by the timestamp embedded in the
extraction output's own filename Ovi supplied — is 22 Sep 2026, and
there is no evidence this conversation spans two calendar days. Found
and corrected across `check_bis_eer_weights.py`,
`test_preflight_scripts.py`, `broad_dollar.py`,
`instruments_taxonomy.yaml`, `KNOWN_RISKS.md`, and `pyproject.toml` —
this dev-log file was renamed from `2026-09-21-...` to `2026-09-22-...`
and its own header corrected, rather than left to quietly disagree with
every other file's now-correct date. Comment/docstring text only in
every case — zero functional change from the correction itself,
verified by re-running the full suite after each fix.

## Process correction (own error, caught before it compounded)

The KNOWN_RISKS.md edit for the prior patch (script fix) was applied via
the Filesystem MCP connector and appeared to succeed (a clean diff was
returned), but had not actually landed — not caught until a
consolidated byte-verification sweep at the very end of that patch,
rather than immediately after the write, as every other file in that
patch was checked. Root cause: relied on the tool's returned diff as
confirmation without an independent `copy_file_user_to_claude` + `diff`
re-fetch immediately afterward, the one file in that patch where this
step was skipped. Re-applied explicitly and re-verified immediately;
confirmed correct before moving on. Every write in this closure patch
was byte-verified immediately, with no exceptions.

## Next steps

None outstanding for Gate 1 / the Broad Dollar basket itself — the
14-currency set is complete and wired. `compute_broad_dollar()`'s output
is available for `ForecastModule`'s PCA pre-processing chain and any
future DXY-vs-Broad-Dollar divergence signal (Architecture v2.0 §7.2),
per the pre-existing design.
