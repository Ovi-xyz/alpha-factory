# 2026-09-02 — silver_validate CRITICAL Gate Failure: price_sanity, coverage_check, outlier_detection

**Version**: 1.17.4 → 1.17.5
**Trigger**: Ovi reported `--job silver` failing with `QualityGateError`
from a live run on 2026-09-02 (log `[05:19:29] RUNNING
silver_validate.txt` attached, in-conversation, not a file upload).
CRITICAL checks failed: `price_sanity` (2101 rows), `coverage_check`
(92.8% < 95%). WARNING check crashed rather than merely warning:
`outlier_detection` — `Out of Range Error: STDDEV_SAMP is out of range!`.
**Scope**: `src/silver/quality_validator.py`, `src/silver/ohlcv_processor.py`,
2 test files, `pyproject.toml`, `CHANGELOG.md`, `KNOWN_RISKS.md`,
`tests/COUNT_BASELINE.txt`. `config/instruments.yaml` /
`validate_instruments.py` deliberately **not** touched — see RISK-28.

---

## 0. Approach — Explore before Decide, Decide before Implement

Read the live repo directly before forming any hypothesis:
`quality_validator.py`, `ohlcv_processor.py`, `market_ingester.py`,
`instrument_loader.py`, `silver_scope.py`, plus `CHANGELOG.md`,
`KNOWN_RISKS.md`, `pyproject.toml`, and the `dev-log/` directory listing
to establish the exact live version (1.17.4) and confirm the three prior
live-test bugs (RISK-23/24/25) were already resolved, not just diagnosed
(memory had them as "not yet confirmed shipped" — stale; the live
CHANGELOG/pyproject showed they'd actually shipped 31 Aug 2026).

Rather than guessing root cause from code alone, asked Ovi to run a
short diagnostic DuckDB script directly against live Silver data before
any fix was proposed as final — three queries: `price_sanity` violation
concentration by symbol, the exact list of `coverage_check`-missing
symbols, and the specific rows with non-positive OHLC (candidate for the
`STDDEV_SAMP` crash). The three results reshaped the diagnosis
materially — see below.

## 1. FIX QV-PS-01 — `price_sanity` re-detected already-quarantined rows

### Root cause

`_check_price_sanity()` and `OHLCVProcessor._flag_is_clean()` run the
identical OHLC-ordering predicate — the latter at Silver-write time
(correctly flagging `is_clean=False`), the former again at
Silver-validate time, counting every violation regardless of flag state
and blocking Gold on it unconditionally. The diagnostic query's
concentration-by-symbol result was decisive: 19 of the top 20 offenders
by row count were forex pairs (USD_JPY 162, EUR_JPY 138, NZD_USD 127,
GBP_JPY 117, GBP_AUD 116, EUR_NZD 114, AUD_USD 96, EUR_AUD 94, GBP_NZD
93, USD_CAD 77, GBP_CAD 68, EUR_USD 67, USD_CHF 64, GBP_USD 60, EUR_CAD
54, EUR_CHF 52, EUR_GBP 45), the remainder IDX (BBRI 48, ADRO 34) — zero
in us_stocks. This is the signature of known retail-feed OHLC noise
characteristics in 24h/OTC forex data (no single canonical daily close,
multiple market-maker sourcing), not a code defect.

### Fix

`_check_price_sanity()`'s SQL gained `AND is_clean = TRUE`. The check
now verifies the self-flagging invariant itself (did any violation
escape being flagged) rather than re-counting noise `OHLCVProcessor`
already correctly quarantined and every downstream Gold consumer
already excludes via its own `is_clean` filter. GD §13.1's own
documented action for Price Sanity is "Mark is_clean=False", not
halt — `OHLCVProcessor` already does exactly that; the validator's job
is to confirm it worked, not repeat it with harsher consequences.

## 2. FIX QV-OUT-01 + FIX OP-LR-01 — CL/WTI's real 2020 negative print

### Root cause

The diagnostic query's third result was the actual finding of the
session: `CL 2020-04-20` (`close=-37.63`) and `CL 2020-04-21`
(`open=-14.00`) — WTI crude's genuine, historically famous negative
print during the COVID storage-capacity crunch. Both rows are
internally well-formed (`high >= low`, `open`/`close` within
`[low,high]`) — neither trips `price_sanity`. But
`log_return = ln(close/prev_close)` is undefined across a sign
crossing, producing NaN/Inf, with two independent, compounding
consequences from the same source:

1. `_check_outliers()` runs ONE DuckDB window-function query
   (`AVG`/`STDDEV(log_return) OVER (PARTITION BY symbol)`) across ALL
   Layer 1 symbols in a single scan. CL's poisoned partition overflowed
   `STDDEV_SAMP` and aborted the *entire* query — silently skipping
   outlier detection for all 639 symbols that run, not just CL. This is
   why the WARNING check crashed with an unhandled-looking error instead
   of just logging a per-symbol issue.
2. `OHLCVProcessor._flag_is_clean()` (and the structurally identical
   `_flag_is_clean_4h()`) computes `mean_`/`std_` from the symbol's FULL
   `log_return` column. The NaN/Inf values made `std_` itself NaN, which
   makes `if std_ and std_ > 0:` silently evaluate `False` in Python
   (`NaN and (NaN > 0)` → `NaN and False` → `False`) — no exception, no
   log line. Outlier detection has been effectively disabled for CL's
   entire Silver 1D history since April 2020, invisibly, until this
   session.

Neither `null_check` nor `price_sanity` reject non-positive prices
(both only check ordering); there has never been an "OHLC > 0" check
anywhere in the pipeline, GD §13.1 included — a reasonable gap for a
universe that's overwhelmingly equities/forex/IDX, until the one
commodity future capable of a real negative print actually printed one.

### Fix

`_check_outliers()`'s CTE gained `AND isfinite(log_return)` — a
poisoned partition no longer aborts the batch query for every other
symbol. `_flag_is_clean()`/`_flag_is_clean_4h()` now compute
`mean_`/`std_` from `is_finite()`-filtered `log_return` values only —
the poisoned rows still end up correctly flagged `is_clean=False` on
their own (NaN/Inf z-score comparisons evaluate `False`, not null, in
Polars) without dragging the rest of the symbol's history down.
Deliberately *not* framed as "reject negative prices" — the event is
real, important market history; only the return-based statistics layer
needed hardening against a sign crossing it was never designed to
handle.

## 3. RISK-28 — `coverage_check` at 92.8%: NOT fixed, decision needed

### What changed mid-session

The initial hypothesis (floated before the diagnostic query results
came back) was that this was fallout from the ADR-045 Bronze
timeframe-partition migration (22 Aug) plus RISK-24's null-quarantine
fix (31 Aug) forcing a still-in-progress mass cold-start backfill. The
diagnostic query's second result — the actual list of 45 missing
symbols — falsified that theory. Individually verified via web search
(4 of 45): **MRO** (Marathon Oil) delisted 22 Nov 2024 —
ConocoPhillips all-stock acquisition. **PXD** (Pioneer Natural
Resources) delisted 3 May 2024 — ExxonMobil all-stock acquisition.
**EA** (Electronic Arts) delisted 4 Aug 2026 — PIF/Silver
Lake/Affinity $55B take-private, closed only 3 weeks before this run.
**SQ** (Block Inc.) — not delisted, ticker changed to **XYZ** effective
21 Jan 2025, company still trades under the new symbol. These are
permanent, will-never-fetch-again gaps, not a temporary backfill lag.

### Why not fixed this pass

The remaining 41 symbols (ANSS, JNPR, HBI, ABC, EXAS, HOLX, CMA, DFS,
RE, IAC, SAVE, SPR, CTRA, HES, K, SPTN, USM, AVB, EQR, PEAK, NEW, SJW,
SEE, WRK, X, ALLK, ALTR, ASTR, CFLT, COOP, FOLD, HYZN, NKLA, RDFN, RIDE,
SAVA, SUMO, VERV, VLDR, ZI) pattern-match the same 2023–2025 M&A wave
but are not individually confirmed. Editing `instruments.yaml` /
`instruments_identity.yaml` to remove or rename symbols is a consequential
data decision — remove vs. rename differs per symbol, and doing it on
an unverified 41/45 basis risks silently dropping something that's
actually still fetchable for an unrelated, transient reason. Registered
as **KNOWN_RISKS.md RISK-28**, left open. Decision needed from Ovi:
manually verify the remaining 41 (one-time, time-intensive) vs. build an
automated ticker-liveness preflight check (structural fix — flags any
Layer 1 symbol with zero fetched rows over a rolling window as a
delisting candidate, catches future delistings/renames too, consistent
with this project's own hardcode/silent-failure-avoidance principles)
vs. some combination of both.

## 4. Test additions

9 new tests. `tests/unit/test_quality_validator.py`:
`TestPriceSanityIsCleanScoping` (+3 — already-flagged violation no
longer blocks; unflagged violation still correctly blocks, proving the
check was narrowed not neutered; a mixed file counts only the unflagged
row) and `TestOutlierSurvivesNonFiniteLogReturn` (+3 — Infinity
partition survives, NaN partition survives,
`test_other_symbols_outliers_still_detected_alongside_poisoned_one` as
the core regression proof that TSLA's genuine outlier is no longer
collateral damage from CL's partition aborting the whole query).
`tests/unit/test_ohlcv_processor.py`: `TestFlagIsCleanOutlierIsolation`
(+3 — Infinity case, NaN case tested separately, and a no-nonfinite-values
regression guard confirming ordinary data behaves identically to
before the fix).

## 5. Verification

1. Sandbox cloned fresh from `github.com/Ovi-xyz/alpha-factory`,
   confirmed HEAD at v1.17.4, baseline 1558/1558 passed before any
   change was made.
2. `ast.parse()` across both modified source files and both modified
   test files — clean.
3. `grep -rn 'f"SELECT\|f\'SELECT'` across `src/` — clean, no f-string
   SQL introduced (both SQL edits are static parameterized strings with
   inline `--` comments).
4. Full suite: 1558 → 1567 passed, 0 failed, 0 regressions.
5. Aggregate coverage (full-suite invocation matching the actual CI
   gate, `pytest tests/ --cov=src`): 88.22%, gate ≥80%.
6. `scripts/validate_instruments.py` scope unaffected — 699 symbols, not
   touched (RISK-28 deliberately not executed without Ovi's decision).
7. `tests/COUNT_BASELINE.txt` updated 1558 → 1567 (plain integer +
   trailing newline, 5 bytes).

## 6. Mirrored to live repo

All 8 changed files (`src/silver/quality_validator.py`,
`src/silver/ohlcv_processor.py`, `tests/unit/test_quality_validator.py`,
`tests/unit/test_ohlcv_processor.py`, `pyproject.toml`, `CHANGELOG.md`,
`KNOWN_RISKS.md`, `tests/COUNT_BASELINE.txt`) mirrored to
`/Users/opi/alpha-factory` via the Filesystem MCP connector using
`edit_file` with the exact same anchor/replacement pairs applied in the
sandbox (dry-run diff previewed for the first edit to confirm anchor
match before applying for real). Every file pulled back via
`copy_file_user_to_claude` and `diff`'d byte-for-byte against the
sandbox source of truth — all 8 confirmed identical on the first pass,
no mismatches to resolve this time.

## 7. Open item carried forward

RISK-28 (KNOWN_RISKS.md) — Layer 1 universe staleness. Not actioned
this pass. Next step is Ovi's call on verification approach before
`instruments.yaml`/`instruments_identity.yaml` is touched.
