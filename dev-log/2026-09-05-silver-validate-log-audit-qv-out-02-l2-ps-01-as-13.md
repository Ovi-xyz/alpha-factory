# 2026-09-05 — Silver Validate Log Audit: QV-OUT-02, QV-L2-PS-01, QV-MSG-01, AS-13

**Version**: 1.17.7 → 1.17.8
**Trigger**: Ovi asked for a debug pass over the `silver_validate` /
`silver_active_symbols` / `silver_context_anchors` log from the
2026-09-05 04:54 run, followed by explicit authorization ("Apply poin
1-3") after live-data confirmation of point 2, plus a separate,
unrelated instruction to bump the IDR active-symbols threshold and
extend the Broad Dollar basket's Layer-1-reuse currency set.
**Scope**: `src/silver/quality_validator.py`, `src/silver/active_symbols.py`,
`config/instruments_taxonomy.yaml` (comment-only), `tests/unit/test_quality_validator.py`,
`tests/unit/test_active_symbols.py`, `pyproject.toml`, `tests/COUNT_BASELINE.txt`,
`CHANGELOG.md`.

---

## 1. Reading the log against live code, not against assumptions

Four WARNING-level anomalies in the log, each traced to an actual code
read rather than inferred from log text:

- `outlier_detection`: writeback failed for CL with `Out of Range Error:
  STDDEV_SAMP is out of range!`, yet the check still reported "passed."
- `context_price_sanity`: 959 Layer 2 rows flagged as OHLC violations.
- `[silver_validate] All checks passed (12/15)` — self-contradicting on
  its face.
- `active_symbols`: "58 unknown-market symbols excluded (not in
  instruments.yaml)" — a file that no longer exists (split into
  `instruments_identity.yaml` / `instruments_taxonomy.yaml`, ADR-027).

## 2. FIX QV-OUT-02 — the writeback path never got QV-OUT-01's guard

`_check_outliers()` PASS 1 (FIX QV-OUT-01, 2 Sep 2026) filters
`isfinite(log_return)` before computing `mean`/`std` across all Layer 1
symbols in one pass. `_flag_outliers_in_file()` — PASS 2, the per-symbol
writeback helper from GAP-4, which has existed since v1.7.5, three
months before QV-OUT-01 — recomputes `mean_lr`/`std_lr` independently,
with no such guard. CL's real 2020-04-20/21 negative-price crossing
still lives in its log_return column; QV-OUT-01 stopped it from
poisoning the cross-symbol PASS 1 query, but PASS 2 only ever runs
*because* PASS 1 flagged a symbol as affected — and CL has genuine
z-score outliers elsewhere across ten years of history independent of
the 2020 event, so PASS 2 still gets called for it, and still crashes.

Fix used `FILTER (WHERE isfinite(log_return)) OVER (...)` rather than a
`WHERE` clause on both the `to_flip` COUNT query and the `copy_sql` COPY
query — verified empirically in a standalone DuckDB session first
(`SELECT x, AVG(x) FILTER (WHERE isfinite(x)) OVER (), STDDEV(x)
FILTER (...) OVER () FROM t`) that FILTER excludes a value from the
aggregate without dropping its row from the result set. A `WHERE`
clause would have silently deleted the non-finite row from Silver on
rewrite — a worse bug than the one being fixed.

**Side effect: two pre-existing QV-OUT-01 tests were vacuous.**
`TestOutlierSurvivesNonFiniteLogReturn` writes CL to a directory named
`commodity_trading/`. `layer1_markets()` (derived live from
`InstrumentLoader`, not hardcoded) actually returns `commodity` — no
underscore, no `_trading` suffix — confirmed by running it directly.
`layer1_globs()` filters to markets whose directory exists among that
exact set, so `commodity_trading/` was never included; `_check_outliers()`
short-circuited via "no Layer 1 data yet" for both CL-only tests in that
class, and their `assert result is True` passed regardless — that
method's return value is hardcoded `True` no matter what happens
internally, so the assertion couldn't tell "ran and succeeded" apart
from "silently skipped." Found this while writing a new test with the
correct directory name and comparing behavior against the old one.
Fixed all three occurrences of `"commodity_trading"` in that class to
`"commodity"` — confirmed via `git diff` that this was purely a
pre-existing naming bug, not something this session introduced.

New tests: `TestOutlierWritebackSurvivesNonFiniteLogReturn` (3 cases —
combined outlier+non-finite scenario via `_check_outliers()`, direct
`_flag_outliers_in_file()` call to pin the exact pre-fix crash site, and
a NaN variant).

## 3. FIX QV-L2-PS-01 — confirmed live before touching anything

Ovi's own instruction here was explicit: extend the diagnostic query
against live data before deciding. Copied all 58 `context/symbol=*/
*_1D_silver.parquet` files from the live repo into the sandbox via
Filesystem MCP's `copy_file_user_to_claude` (one call per file — no
bulk/glob copy available), then ran the exact predicate
`_check_context_price_sanity()` uses, cross-tabbed by `is_clean`:

```
Total OHLC violations (current check, unscoped): 959
Breakdown by is_clean: [(False, 959)]
```

All 959 already `is_clean=False`. Symbol breakdown: 900 of 959 (94%)
concentrated in exactly the 7 `context_dollar_basket` legs (KRW 271,
IDR 184, TWD 144, HKD 86, SGD 80, THB 72, NOK 63) — every one of which
`instruments_taxonomy.yaml` already annotates as "ticker convention
unconfirmed live." Same shape as QV-PS-01's 2 Sep 2026 finding for
Layer 1 forex (2101 rows, 19/20 top offenders were forex pairs), just
never propagated to this Layer 2 sibling check when that fix landed.
Re-ran the predicate with `AND is_clean = TRUE` appended before writing
any code — 0 rows. Zero escaped-violation risk, confirmed empirically,
not assumed.

Fix mirrors QV-PS-01 exactly: `AND is_clean = TRUE` added to the query.
New tests: `TestContextPriceSanityIsCleanScoping` (2 cases — already-
flagged violation no longer fails the check; a genuinely escaped
violation, `is_clean` still `True`, still fails it).

## 4. FIX QV-MSG-01 — wording only

`run()`'s final `logger.success()` always read `"All checks passed
(N/M)"`, even when `N < M` (WARNING checks failed, gate still passes
since no `QualityGateError` was raised). Split into two branches: `"All
{total} checks passed"` when true, `"CRITICAL gate passed — N/M checks
green (see WARNING lines above for the rest)"` otherwise. No behavior
change, no new test — this only affects a log string.

## 5. FIX AS-13 — the metric that permanently reports 58

`_audit_unknown_markets()` counts every Silver symbol absent from
`mkt_map`, which is Layer 1-only by this module's own design
(`GMI-CTX-001` moved Layer 2 to `context_anchors.py`). The log's "58
unknown-market symbols excluded (not in instruments.yaml)" is an exact
match, every run, for `silver_context_anchors`'s own resolved count —
confirmed by listing `data/silver/market_ohlcv/context/` (58
subdirectories) and reading `context_anchors.py` directly
(`get_loader().all_context(include_deferred=False)`). This metric has
been permanently reporting the entire Layer 2 universe as "unknown"
since Layer 2 anchors started landing in the same `market_ohlcv` tree —
worthless as a signal, since a genuine new orphan bumping 58→59 is easy
to miss on a skim, while 0→1 would not be. The filename reference was
also stale: `instruments.yaml` was split into `instruments_identity.yaml`
/ `instruments_taxonomy.yaml` under ADR-027 and no longer exists.

Fix cross-references `get_loader().all_context(include_deferred=True)`,
wrapped in its own nested `try/except` — a Layer 2 lookup failure must
degrade to "no Layer 2 symbols known" (equivalent to pre-fix behavior
for that subset only), not abort the orphan audit entirely. Layer 2
overlap is now logged at DEBUG (informational); the WARNING and the
returned count reflect only symbols in neither registry. Confirmed
against the existing mocked test (`test_unknown_market_count_in_output`,
which mocks `get_loader` without configuring `all_context` — the
resulting `MagicMock` isn't iterable, caught by the inner `except`,
falls back to an empty context set, and the test still passes for the
right underlying reason since its fixture has no unmatched symbols at
all either way). New tests: `TestAS10UnknownMarket` gained two cases —
a Layer 2 symbol (`DXY`) present in Silver must not be counted; a
genuine orphan alongside it must still be counted, and only it.

## 6. Config-only, separate from the four bug fixes above

- `THRESHOLDS["idx"]["dollar_volume_20d"]`: 5,000,000,000 → 50,000,000,000
  IDR, per Ovi's direct instruction, applied and mirrored earlier in
  this same session before the debug investigation above.
- ADR-049 (`instruments_taxonomy.yaml`, comment-only, no structural or
  code change): the Layer-1-reuse currency set for the still-unbuilt
  `compute_broad_dollar()` — 6 pairs sourced directly from Layer 1
  forex rather than duplicated into `context_dollar_basket` (EUR_USD,
  USD_JPY, GBP_USD, USD_CAD, USD_CHF, AUD_USD) — is extended to 7 with
  NZD_USD, per Ovi's instruction. NZD_USD already exists as an always-on
  Layer 1 forex pair; it will be reused the same way AUD_USD/USD_CAD
  already are, not duplicated as an 8th `dollar_basket` instrument, once
  CrossAssetEngine (Cycle 4) is built — still gated on Gate 1 (BIS EER
  weight sourcing). Appended below the existing ADR-036 amendment rather
  than rewriting the original 2025 decision text, matching this file's
  established convention for dated amendments.

## 7. Verification

`ast.parse` clean on every modified `.py` file. Full suite: 1574 passed
/ 0 failed / 0 error (1567 baseline + 7 new tests — 3 QV-OUT-02, 2
QV-L2-PS-01, 2 AS-13). `python scripts/validate_instruments.py` →
"VALIDATION PASSED — 654 symbols (Layer 1=594, Layer 2=60), no errors."
(unchanged, as expected — ADR-049 touched only a comment block).
`COUNT_BASELINE.txt`: 1567 → 1574. Version: 1.17.7 → 1.17.8 (PATCH —
bug fixes and config tuning, no new capability exposed downstream).
Mirrored file-by-file via Filesystem MCP `edit_file`/`write_file`,
each verified with `copy_file_user_to_claude` + `diff` against the
tested sandbox source of truth before being considered done.
