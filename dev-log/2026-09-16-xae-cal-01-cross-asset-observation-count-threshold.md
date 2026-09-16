# 2026-09-16 — FIX XAE-CAL-01: CrossAssetEngine History Threshold, Observation-Count Native

**Version**: 1.18.0 → 1.18.1
**Trigger**: Ovi ran all four CrossAssetEngine jobs live on the Mac
(`gold_global_regime`, `gold_cross_asset_correlation`, `gold_lead_lag`,
`gold_forecast`, log pasted as `2026-09-16-cross-asset-engine-running-log.txt`)
and reported: two jobs (`gold_cross_asset_correlation`, `gold_lead_lag`)
returned WARNING and wrote nothing; the other two wrote real output under
`data/gold/cross_asset/`. No question was asked outright — this was read as
an implicit request to investigate, per this project's standing "empirical-
first" convention (never diagnose from log text alone). After the
investigation surfaced a confirmed root cause and three repair-path options
were presented, Ovi decided explicitly: *"continue with switch from
calendar-day window to trading-day-count. for cross-asset engine; do not
use trading day literally, use observation count instead. cross-asset
engine = observation-count native, calendar-aware."* Two subsequent
"continue the project work" instructions authorized implementation through
sandbox testing and the live mirror without further recap.
**Scope**: 3 source files, 3 paired test files, `tests/COUNT_BASELINE.txt`,
`pyproject.toml`, `CHANGELOG.md`, this dev-log. No `KNOWN_RISKS.md` entry —
this is a resolved bug, not an accepted risk.

---

## 0. Live symptom

```
gold_cross_asset_correlation: Universe: 196 Layer 1 + 58 Layer 2 = 254 merged (after de-dup)
                               Only 1 symbols have enough history — need at least 2
                               Nothing written (no data)
gold_lead_lag:                No leader/follower with sufficient history (valid_leaders=0, valid_followers=0)
                               Nothing written (no data)
gold_forecast:                1 symbols x 5 horizons (0 unstable-VAR rows) -> cross_asset_forecast.parquet
                               [logged SUCCESS]
```

Confirmed via Filesystem MCP (`list_directory_with_sizes` on
`data/gold/cross_asset/`) that only `global_regime.parquet` and
`cross_asset_forecast.parquet` existed — matching the log exactly.
`gold_forecast`'s "success" was the more dangerous of the three outcomes:
no warning at all, despite covering ~1 of ~196 active equities.

## 1. Root-cause investigation (empirical, not documentation-first)

Read the three weekly CrossAssetEngine modules directly from the live repo
via `mcp__Filesystem__read_text_file` (`correlation_module.py`,
`lead_lag_module.py`, `forecast_module.py`). All three independently
define:

```python
LOOKBACK_DAYS      = 65
MIN_HISTORY_RATIO  = 0.8
...
min_days = int(LOOKBACK_DAYS * MIN_HISTORY_RATIO)   # = 52
```

and filter candidate symbols on `n_days >= min_days`, comparing a row
count against a window bounded by `run_date - timedelta(days=65)` —
i.e. 65 CALENDAR days, not trading days.

Computed the theoretical ceiling directly: the window
`2026-07-13 -> 2026-09-16` (66 days inclusive) contains exactly **48
weekdays**. A threshold of 52 is unreachable by ANY 5-day-week market
instrument, however clean its data — this is not a probabilistic or
data-quality-dependent failure, it's a structural certainty, the same
shape as `GMI_Decision_Document_v11.docx` §1.5's `MIN_MTF_SCORE` finding.

Verified empirically against real production data — copied live Silver 1D
parquet via `mcp__Filesystem__copy_file_user_to_claude`, queried with
DuckDB in the sandbox (`WHERE date >= run_date - 65 days AND date <=
run_date`, counting rows with `log_return IS NOT NULL AND is_clean =
TRUE`), across one representative instrument per market type in the
universe:

| Symbol | Market | Valid obs (65-day window) |
|---|---|---|
| AAPL | us_stocks | 45 |
| BBCA | idx | 44 |
| EUR_USD | forex (Layer 1) | 47 |
| CL | commodity | 47 |
| SSEC | context (chronic-gap flagged) | 45 |
| JKSE | context (chronic-gap flagged) | 43 |
| DXY | context | 47 |
| IDR | context | 48 |

All 8 fall between 43-48 — none reach 52. Range confirms the 48-weekday
ceiling derivation.

**Falsification check** (matching this project's "verifikasi empiris"
convention — actual implementation output as expected value, not a
hand-computed guess): reproduced the exact production symptom directly.
Ran `CorrelationModule().compute()` against the live (pre-fix)
`correlation_module.py` with clean, synthetic, weekday-only fixture data
(46 valid observations, zero gaps, zero data-quality issues) and got:

```
[gold_cross_asset_correlation] Universe: 2 Layer 1 + 0 Layer 2 = 2 merged (after de-dup)
[gold_cross_asset_correlation] Only 0 symbols have enough history — need at least 2
result: None
```

Word-for-word the same warning class observed in production, from
perfect data — conclusive proof the threshold itself is the defect, not
anything about the live Silver data's actual quality.

## 2. Also confirmed: existing unit tests could never have caught this

`grep`'d all three test files' fixture generators —
`DATES = [BASE_DATE + timedelta(days=i) for i in range(N_DAYS)]` in every
one, i.e. every fixture writes a row for EVERY consecutive calendar day
(weekends included), with `N_DAYS` in the 70-100 range. Under
`MIN_HISTORY_RATIO=0.8` this always cleared 52 trivially (70-100 >> 52),
so none of the 38 pre-existing tests across the three files ever exercised
a realistic 5-day-week calendar. This is the same "unrealistic fixture"
anti-pattern the CI/CD Ops Guide (`alpha_factory_cicd_ops_guide_v1_7_4.docx`)
already documents from a prior audit finding (NEW-3).

## 3. Decision

Presented three repair paths (A: lower the ratio; B: switch to a
trading-day-count window; C: adaptive threshold relative to max observed
count). Ovi's decision, verbatim: *"switch from calendar-day window to
trading-day-count. for cross-asset engine; do not use trading day
literally, use observation count instead. cross-asset engine =
observation-count native, calendar-aware."*

Read as: the outer window bound (`LOOKBACK_DAYS`, still calendar days —
needed so the pivot/join has a common date index across every symbol, an
inherent requirement for cross-symbol correlation/Granger testing) stays
as-is. The completeness GATE inside that window changes from a
calendar-density ratio to a literal, absolute observation count — no
per-market trading-calendar logic (NYSE calendar, IDX holiday table,
FX week definition) is modeled; the actual Silver row count already
reflects each market's real calendar, so counting observations directly
is "calendar-aware" without needing to model calendars explicitly.

## 4. Implementation

`MIN_HISTORY_RATIO = 0.8` removed from all three modules, replaced with
`MIN_OBSERVATIONS = 40` — calibrated from the 8-instrument empirical
distribution above (43-48 valid obs): 40 clears every sample with margin
for a worse week (e.g. an Eid al-Fitr closure landing inside the window —
this project's own known chronic IDX gap source, per
`silver_validate` KNOWN_RISKS entries) while still requiring ~83% of the
48-weekday ceiling. `LOOKBACK_DAYS = 65` unchanged in all three files.

Each `_pivot_returns`/`_load_pivot` filter changed from:
```python
min_days = int(LOOKBACK_DAYS * MIN_HISTORY_RATIO)
sym_counts = returns.group_by("symbol").agg(pl.len().alias("n_days")).filter(pl.col("n_days") >= min_days)
```
to:
```python
sym_counts = returns.group_by("symbol").agg(pl.len().alias("n_obs")).filter(pl.col("n_obs") >= MIN_OBSERVATIONS)
```

Threshold logic remains independently duplicated across the three
modules (not factored into a shared helper) — consistent with
Architecture v2.0 §6.1's explicit design principle that CrossAssetEngine
modules "share no internal state" and communicate only through Parquet.
Flagged here rather than silently centralized without asking; each
module's constant carries a full comment pointing to
`correlation_module.py`'s calibration rationale so the three don't drift
independently in the future.

## 5. Tests

Added `TestCalendarAwareness` to all three test files — the first
fixtures in this project to use a genuine Mon-Fri-only calendar
(`_weekday_dates_ending()`, skips Saturday/Sunday) rather than every
other fixture's artificial 7-day-a-week consecutive dates. Each asserts
both fixture sanity (`n < 52` and `n >= MIN_OBSERVATIONS`, confirming the
test actually exercises the old failure mode and clears the new
threshold) and the real behavior (symbols/pairs are detected, not
dropped).

Verified two-directionally, not just "new tests pass":
- `git stash` on the 3 source files only (tests kept) → all 3 new tests
  FAIL, with `AttributeError: module '...' has no attribute
  'MIN_OBSERVATIONS'` (the old code never defined it) — confirms the
  tests genuinely exercise the new API, not a tautology.
- `git stash pop` (fix restored) → all 41 tests in the three files pass.

`tests/COUNT_BASELINE.txt`: 1710 → 1713. Full suite: `pytest tests/ -q` →
**1713 passed, 0 failed, 0 error**.

## 6. Sandbox → live mirror

Fresh clone of `github.com/Ovi-xyz/alpha-factory` into
`/home/claude/alpha-factory` — confirmed matched the live repo before any
edit (same `v1.18.0`, same `1710`-line `COUNT_BASELINE.txt`, same
constants read live from the Mac earlier in the session via
`mcp__Filesystem__read_text_file`). All 6 code files edited and tested in
sandbox first. `ast.parse()` clean on all 6. Each of the 7 changed files
(3 source, 3 test, `COUNT_BASELINE.txt`) mirrored to the live repo via
`mcp__Filesystem__write_file`, then read back via
`copy_file_user_to_claude` and `diff`'d byte-for-byte against the
sandbox-tested version — all 7 confirmed byte-identical.

`pyproject.toml`'s version bump was first attempted via
`mcp__Filesystem__edit_file` (a small, single-anchor change — the normal
choice for this file's size, per this project's own `edit_file` vs.
`write_file` convention). That edit's own returned diff looked correct,
but a subsequent byte-verification read-back showed it had duplicated an
unrelated 11-line block elsewhere in the file — a real tool-behavior
surprise, not a content mistake on this session's part, and caught only
because byte-verification is unconditional here rather than trusting the
diff the edit call itself reported. Corrected immediately via
`write_file` (full-file replacement) with the sandbox-verified content,
then re-verified byte-identical. `CHANGELOG.md` (5,746 lines) was edited
via `edit_file` with `dryRun=true` checked first and inspected before
applying for real, given the file's size ruled out a full `write_file`
transfer and the `pyproject.toml` incident had just demonstrated
`edit_file`'s own diff output isn't sufficient confirmation on its own —
the real edit's diff matched the dry run exactly, and byte-verification
after applying confirmed no duplication this time. This dev-log file
itself is new (no existing content to merge with), so `write_file` posed
no such risk.

Lesson for future sessions editing large pre-existing files on the live
repo via this Filesystem MCP connector: prefer `write_file` for full
replacement when the file is small enough to transfer safely; when it
isn't, use `edit_file` with `dryRun=true` first and always
byte-verify the actual result afterward regardless of what the edit
call's own diff reports — this session's `pyproject.toml` incident is
the concrete case for why the byte-verification step in this project's
existing mirror workflow is unconditional, not a formality.

## 7. Not done / explicitly out of scope this pass

- Threshold logic not centralized into a shared helper (see §4) —
  flagged as a future consideration if the three modules' constants ever
  need to diverge deliberately, but not acted on unprompted.
- `GMI_Decision_Document_v11.docx`'s ADR-045 (Bronze OHLCV timeframe
  partition), ADR-046 (MTF score coverage repair path), and ADR-047
  (AU/AG ticker fix) remain fully undecided-on-implementation /
  unimplemented, per that document's own status ("DECIDED — nothing
  implemented" as of 22 Aug 2026). Untouched by this session; unrelated
  code paths (Bronze ingestion / `gold_mtf` / `symbol_utils.py`, not
  `gold/cross_asset/`).
- The exact identity of the single symbol that passed the OLD threshold
  in the live production run (giving `gold_forecast` its "1 symbol x 5
  horizons" output) was not tracked down — not material to the root
  cause or the fix, and none of the spot-checked candidates (IDR at 48,
  DXY at 47) actually cleared 52 either.
