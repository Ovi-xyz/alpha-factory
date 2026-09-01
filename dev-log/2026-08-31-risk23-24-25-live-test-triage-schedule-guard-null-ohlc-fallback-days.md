# 2026-08-31 — Live-Test Triage: Schedule Guard, Trailing Null OHLC, FALLBACK_DAYS

**Version**: 1.17.3 → 1.17.4
**Trigger**: Ovi reported three findings from live-test 2026-08-31: (1)
"bronze_treasury returns macro idempotent skip during live test 20260831";
(2) "many tickers particularly from us_stocks and context timeframe=1D/
source=yfinance that returned in quarantine which caused by <Schema
error: Column 'open': not nullable but has 1 nulls>"; (3) "idx/
timeframe=1H/source=yfinance_jk that returned only 1 ticker." Two log
files attached (`2026-08-31-AAPL-1D.txt`, `2026-08-31-idx-1H.txt`).
**Scope**: `src/scheduler/job_registry.py`, `src/bronze/yfinance_adapter.py`,
`src/bronze/inc_fetch.py`, `src/bronze/market_ingester.py`, 6 test files,
`pyproject.toml`, `CHANGELOG.md`, `KNOWN_RISKS.md`, `tests/COUNT_BASELINE.txt`.

---

## 0. Approach — Explore before Decide, Decide before Implement

All three findings were diagnosed against the live repo (via the
Filesystem MCP connector) before any code was written — `job_registry.py`,
`runner.py`, `yfinance_adapter.py`, `inc_fetch.py`, `market_ingester.py`
read directly, cross-referenced against the two attached log files line
by line. Root causes for all three were presented to Ovi and confirmed
(including a clarifying exchange on Issue 2 — whether a 1-hour schedule
shift would have avoided it; traced the actual NYSE-session-in-WIB-time
math to show it wouldn't, by roughly 17.5 hours, not 1 — and on Issue 3,
where Ovi proposed 720 days directly) before any sandbox implementation
began.

## 1. RISK-23 — bronze_macro_weekly/bronze_bis_rates had no schedule guard

### Root cause

`bronze_macro_weekly` and `bronze_bis_rates` carried a comment ("called
explicitly on weekly SOP") but no `run_on_weekdays` constraint at all —
an assumption that predates GMI-JR-003's `--job bronze` layer-scoped
runner. `layer_sequence("bronze")` derives from `WEEKLY_SEQUENCE`, which
lists `bronze_macro_weekly` before `bronze_treasury`. `runner.run_job()`
does apply `_passes_schedule()` per job even in layer mode — confirmed by
reading `runner.py` directly — but with no constraint defined,
`bronze_macro_weekly` always passes. Its `FREDIngester().run(run_date)`
call (no `series_filter`) writes the full ~60-series registry, which
already includes all 13 `TREASURY_FRED_SERIES` tenors. `bronze_treasury`
runs moments later in the same `--job bronze` invocation and finds every
one of its target series already written this same day — FIX BI-1's
idempotency check (a deliberate, correct mechanism) skips all 13,
producing exactly the reported symptom, every single time this command
runs, any day of the week.

### Fix

`run_on_weekdays: [6]` (Sunday) added to both jobs, matching `bronze_eia`'s
existing `[2]` (Wednesday) precedent.

### Consequential fix found while testing, not anticipated up front

Adding the guard broke two pre-existing integration tests
(`test_bronze_layer_completes_standalone`,
`test_bronze_then_silver_then_gold_completes_full_chain` in
`test_runner_weekly_cadence.py`) that had encoded the old assumption
(everything in a layer completes on any day). Deeper issue underneath the
test failure: `silver_macro`/`silver_global_rates` depend on
`bronze_macro_weekly`/`bronze_bis_rates` with an exact-date dependency
check — before this fix, that check always trivially passed because the
weekly job always had a same-day sentinel by construction. Once the
schedule guard is real, `--job silver` on a non-Sunday would newly
`sys.exit(1)`. This is the exact `stale_tolerance` pattern FIX NEW-1
already established one hop downstream (`silver_validate`/`gold_regime`
depending on the weekly `silver_macro`) — it had just never been extended
one hop further up, because there was nothing to make the gap visible
until now. Added `stale_tolerance: {"bronze_macro_weekly": 7}` and
`stale_tolerance: {"bronze_bis_rates": 7}` respectively. Updated the two
tests to seed a preceding-Sunday sentinel first (mirroring the real SOP
and the existing `TestJobAllAcrossWeek` precedent in the same file)
rather than expecting same-day completion of a genuinely weekly job.

## 2. RISK-24 — trailing null-OHLC placeholder row quarantined the whole batch

### Root cause

`market_ingester.py` passes `end=run_date` to `Ticker.history()`. The
live-test ran at ~03:08 WIB — roughly 17.5 hours before NYSE's own
session for `run_date` even opens in UTC terms (worked this out
explicitly when Ovi asked whether a 1-hour schedule shift to 04:08 WIB
would have avoided it — it would not have, by a wide margin). Requesting
a range whose `end` sits that far ahead of any real session data returns
the legitimate history plus one trailing placeholder row with null OHLC.
`config/schemas/yfinance_ohlcv.yaml`'s `nullable: false` on those columns
makes `SchemaValidator` quarantine the entire DataFrame over that one
artifact row — not a symbol-specific bug, structural to any 1D fetch run
in that window.

### Fix

`_drop_trailing_null_ohlc()` added to `yfinance_adapter.py`, called from
`_normalize_df()` before the DataFrame reaches `SchemaValidator`. Only
strips rows from the tail where open/high/low/close are **all** null —
walks backward from the last row, stops at the first row that isn't
fully null. A null row anywhere else in the series (mid-series, or only
some OHLC fields null) is untouched and still fails validation, which is
correct: that's a real data-quality signal, not a boundary artifact.
Chose this over relaxing the schema's `nullable: false` — `Ovi confirmed
Option A` in the prior turn — because it doesn't weaken the registry's
ability to catch genuine schema drift, and because `IncFetchProtocol`'s
existing 7-day lookback overlap means nothing is actually lost by not
chasing same-day bars; tomorrow's run picks up the completed bar cleanly.

## 3. RISK-25 — FALLBACK_YEARS["1H"]=2 sat exactly on yfinance's 730-day wall

### Root cause

`int(365 * 2)` = 730, landing exactly on Yahoo's own stated ceiling ("must
be within the last 730 days") for 1H intraday history. Live-test log:
28 of 29 IDX symbols failed cold-start with precisely this error; only
the first symbol processed (AADI) got through, almost certainly on
sub-second request-timing luck rather than because 730 is actually safe.
This directly contradicts this repo's own v1.17.1 changelog entry, which
claimed the same constant was "verified, not assumed" correct during
ADR-046 Path C's implementation — whatever check was run there evidently
didn't reproduce the real multi-symbol, live-boundary conditions this
log shows failing. Flagged this contradiction to Ovi explicitly rather
than quietly overwriting the old claim.

### Fix

Ovi's own proposed value (720, not something I chose independently).
Implemented as a new explicit `FALLBACK_DAYS: dict[str, int] = {"1H": 720}`
constant in `inc_fetch.py`, rather than trying to find a `fallback_years`
float that round-trips to exactly 720 through `int(365 * years)` — raised
this to Ovi first: floating-point truncation could silently give 719
instead of 720, and the existing `365 * years` formula is independently
leap-year-sensitive across different `run_date`s regardless of the
specific value chosen. `resolve_start_date()` gained an optional
`fallback_days` parameter that takes precedence over `fallback_years`
entirely when provided (not merged, not cross-checked — a clean override).
`FALLBACK_YEARS["1H"] = 2` left in place as a human-readable label, now
annotated as superseded for actual resolution. Both `market_ingester.py`
call sites (`_run_symbol`, `_run_context_symbol`) updated to pass
`fallback_days=FALLBACK_DAYS.get(tf)` — the fix in `inc_fetch.py` only
takes effect because both call sites forward it; wrote a dedicated wiring
test for this (`TestRunSymbolFallbackDaysWiring`) rather than assuming it.

## 4. Test additions

25 new tests: `test_schedule_guard.py` (+1, direct `_passes_schedule()`
check for the new Sunday-only constraint), `test_job_registry_integrity.py`
(+6, `TestWeeklyMacroScheduleGuard` — locks in both `run_on_weekdays`
values and both `stale_tolerance` entries against the live registry, not
just a synthetic dict), `test_yfinance_adapter.py` (+9,
`TestDropTrailingNullOhlc` — single/multiple trailing nulls, mid-series
null left alone, partial-null row left alone, all-null empties out,
end-to-end `_normalize_df()` for both the partial- and total-placeholder
cases), `test_inc_fetch.py` (+6, `TestFallbackDaysOverride` — value,
margin, precedence over `fallback_years`, no-regression on the existing
path, run_date-independence, existing-data path unaffected),
`test_market_ingester.py` (+3, `TestRunSymbolFallbackDaysWiring` — both
call sites actually forward the right value).

## 5. Verification

1. `ast.parse()` across all 143 `.py` files in `src/`+`tests/` — clean.
2. `grep -rn 'f"SELECT...'` across `src/` — clean, no f-string SQL touched.
3. `scripts/validate_instruments.py` — unaffected sanity check, 699
   symbols, PASSED.
4. Full suite: 1533 → 1558 passed, 0 failed, 0 regressions (baseline
   re-confirmed clean at 1533/1533 before any change was made, including
   installing `poetry` into the sandbox first to clear 2 environment-only
   pre-existing failures unrelated to this session's scope).
5. `tests/COUNT_BASELINE.txt` updated 1533 → 1558 (plain integer +
   trailing newline).

## 6. Mirrored to live repo

All 14 changed/touched files (`src/scheduler/job_registry.py`,
`src/bronze/yfinance_adapter.py`, `src/bronze/inc_fetch.py`,
`src/bronze/market_ingester.py`, 6 test files, `pyproject.toml`,
`CHANGELOG.md`, `KNOWN_RISKS.md`, `tests/COUNT_BASELINE.txt`) mirrored to
`/Users/opi/alpha-factory` via the Filesystem MCP connector using
`edit_file` with the exact same anchor/replacement pairs applied in the
sandbox — not a full-file rewrite — so the live diff mirrors the
sandbox's own git diff precisely. Every file pulled back via
`copy_file_user_to_claude` and `diff`'d against the sandbox source of
truth (not byte-count alone). One real mismatch found this way: an
accidental double blank line in `CHANGELOG.md` from the sandbox-side
Python insertion script (the `/tmp/changelog_entry.md` heredoc already
ended in a trailing newline; the insertion script added a second `\n` on
top of it). Fixed in the sandbox to match the live copy's already-correct
single blank line, re-diffed clean, re-ran the full suite once more to
confirm the whitespace-only fix didn't disturb anything (1558 passed).
All 14 files confirmed byte-for-byte identical after the fix.
