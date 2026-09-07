# 2026-09-07 — VIXCLS Starvation: Scheduling Deadlock, write_macro() UTC Bug, Success-Counter Miscounting

**Version**: 1.17.8 → 1.17.9
**Trigger**: Ovi's manual finding during inspection —
`data/bronze/macro/fred/volatility` had only 2 files, VIXCLS missing for
20260905. Requested a precise check on whether `fred_ingester.py` is bug
clean and in prime condition, and separately whether the
`bronze_treasury` run log (2026-09-06 04:51:48) is in prime condition.
**Scope**: `src/bronze/base_ingester.py`, `src/bronze/fred_ingester.py`,
`src/bronze/eia_ingester.py`, `src/bronze/bls_ingester.py`,
`src/bronze/imf_ingester.py`, `src/bronze/bea_ingester.py`,
`src/scheduler/job_registry.py` (all modified); `src/bronze/fred_daily_ingester.py`
(new); `tests/unit/test_base_ingester.py` (modified),
`tests/unit/test_fred_daily_ingester.py` (new); `pyproject.toml`,
`tests/COUNT_BASELINE.txt`, `CHANGELOG.md`, `KNOWN_RISKS.md` (RISK-29, new).

---

## 1. Reading live code and live files before forming any hypothesis

Two direct questions, both answered by reading actual code and actual
Bronze files rather than the design docs (Grand Design / Supplementary
Design describe `fred_ingester.py`'s intended cadence behavior; they
don't reflect what `job_registry.py` actually does today, and the two
had already diverged once before — RISK-23, 31 Aug 2026).

Read in order: `src/bronze/fred_ingester.py`, `config/fred_series.yaml`
(confirmed VIXCLS: `domain: volatility`, `cadence: daily`,
`regime_input: true`), `src/scheduler/job_registry.py`
(`bronze_macro_weekly`'s `run_on_weekdays: [6]`, added 31 Aug 2026 for
an unrelated reason — see that entry's own comment), `src/bronze/treasury_ingester.py`
(confirmed its `TREASURY_FRED_SERIES` filter doesn't include VIXCLS or
8 other daily-cadence series), then `src/bronze/base_ingester.py`
(`write_macro()`).

Then verified against the filesystem directly — `list_directory` on
`data/bronze/macro/fred/volatility/` and `.../credit/`, `get_file_info`
on individual files — rather than trusting filenames alone. This is
what surfaced the second bug: a file named `VIXCLS_20260830_*.parquet`
whose real filesystem creation timestamp was `Mon Aug 31 2026 03:06:32
GMT+0700`, one full calendar day later than its own name.

## 2. FIX GMI-FRED-DAILY-01 — the scheduling deadlock

`bronze_macro_weekly` is the only job that calls
`FREDIngester().run(run_date)` with no `series_filter` — the full
registry. Since RISK-23 (31 Aug 2026), that job carries
`run_on_weekdays: [6]` (Sunday only). Inside `fred_ingester.py::run()`,
any series with `cadence: daily` is skipped unless
`run_date.weekday()` is Mon–Fri:

```python
if cadence == "daily" and run_date.weekday() not in range(5):
    logger.debug(f"[FRED] Skipping daily series {series_id} — not weekday")
    continue
```

Sunday (`weekday() == 6`) can never be in `range(5)`. There is no day on
which "the only caller runs" (Sunday) and "the series is allowed to
fetch" (Mon–Fri) are both true. This isn't a race condition or a flaky
edge case — it's a deterministic, permanent deadlock for every
`cadence: daily` series not also covered by `treasury_ingester.py`'s
own 13-tenor daily fetch.

Cross-referenced `config/fred_series.yaml`'s full series list against
`TREASURY_FRED_SERIES` to get the exact affected set: `VIXCLS`,
`DEXUSEU`, `BAMLH0A0HYM2`, `BAMLC0A0CM`, `DCOILWTICO`, `DEXJPUS`, `DFF`,
`T5YIE`, `T10YIE` — 9 series. Three carry `regime_input: true` in the
registry: `VIXCLS` (`vix_proxy`), `DEXUSEU` (`dxy_proxy`),
`BAMLH0A0HYM2` (`credit_spread`) — 3 of `gold_regime`'s 7 macro regime
inputs.

Confirmed empirically rather than just algebraically: `list_directory`
on `data/bronze/macro/fred/volatility/` and `.../credit/` showed every
one of the 9 daily-non-treasury series frozen at exactly 2 files each
(`*_20260820_*`, `*_20260830_*`), while `cadence: weekly` series living
in the *same folders* — `M2SL`, `NFCI`, `STLFSI4`, `WALCL` — had a third
file dated `20260905` and kept updating normally. That split (all daily
frozen, all weekly current, same directories, same job) is the
deadlock's fingerprint — ruling out network flakiness or an API-key
problem, which would have hit both groups indiscriminately.

Also cross-checked `KNOWN_RISKS.md` before writing anything: RISK-23
covers the *cause* (the schedule guard itself, added for an unrelated
reason — the `bronze_treasury` idempotent-skip collision) but nothing
in the document names this specific side effect. New finding, not a
duplicate.

**Repair path decision.** Presented Ovi three options (mirroring the
project's own ADR-046 three-path precedent for a comparable
architectural fork): (A) split the full FRED sweep into a dedicated
daily-cadence job, (B) drop the internal weekday gate for the
once-weekly full-registry call specifically, (C) reclassify these 9
series' `cadence` to `weekly` in the registry and accept the capability
reduction. Ovi chose (A).

**Implementation.** New module `src/bronze/fred_daily_ingester.py` —
`FRED_DAILY_SERIES` (the 9-item list, each entry commented with its
`regime_input` role where applicable) and a `run(run_date)` that
delegates to `FREDIngester().run(run_date, series_filter=FRED_DAILY_SERIES)`,
wrapped in the same outer `try/except` pattern
`treasury_ingester.py::TreasuryIngester.run()` already uses (a delegate
failure must not abort the rest of `DAILY_SEQUENCE`). New
`job_registry.py` entry `bronze_fred_daily`: `depends_on: []` (GD
§17.3.1 — Bronze ingesters independent), no `run_on_weekdays` — deliberate,
since `fred_ingester.py`'s own weekday check is the correct and
sufficient gate for a caller that is itself invoked daily, exactly as it
already is for `bronze_treasury`. Added to `DAILY_SEQUENCE` right after
`bronze_treasury`.

Deliberately did **not** touch `bronze_macro_weekly` to exclude these 9
series from its own full-registry Sunday call. They'll keep being
evaluated and skipped there every Sunday — a debug log line each, no API
call, no write, no cost worth the alternative: a second, independently
maintained "all series except these 9" exclusion list would reproduce
the exact dual-source-of-truth shape `GMI_Decision_Document_v11.docx`
ADR-047 already rejected for the AU/AG ticker table, for the identical
underlying reason (two hand-maintained lists that can silently drift
apart is how bugs like this start).

## 3. FIX GMI-BI-DATE-01 — write_macro()'s date came from the wrong clock

`write_macro()`'s idempotency check and its output filename's date
component were both derived from `datetime.utcnow()`. `run_date` wasn't
even a parameter of the method — every other reproducibility-sensitive
path in this codebase (`IncFetchProtocol.resolve_start_date()`, G1)
uses `run_date` specifically to avoid this class of bug; `write_macro()`
had never been brought in line with that precedent.

Ovi's SOP runs the daily/weekly pipeline in the 04:00–05:00 WIB window
(UTC+7) — meaning UTC is still on the *previous* calendar day for the
entire operating window. This is not a rare boundary condition; it's
the routine case, every single day the SOP is followed.

Verified directly against two live files via `get_file_info` rather
than trusting the theory alone:

| File | Name implies (UTC) | Actual local creation (WIB) |
|---|---|---|
| `VIXCLS_20260830_200632.parquet` | Sun 30 Aug | **Mon 31 Aug 03:06:32** |
| `VIXCLS_20260820_211150.parquet` | Thu 20 Aug | **Fri 21 Aug 04:11:50** |

Both off by exactly one calendar day, in the same direction, both
inside the 00:00–07:00 WIB window where UTC lags local by a full day.
This is also the direct, confirmed explanation for the log line that
triggered the second half of Ovi's original question: `bronze_treasury`'s
2026-09-06 04:51:48 run logged `"MORTGAGE30US already written for
20260905"` while its own `run_date` was `2026-09-06` — the idempotency
check computed `datetime.utcnow()` at that wall-clock moment, which was
still `2026-09-05` in UTC.

Same bug class `GMI_Decision_Document_v11.docx` ADR-045 already found
(but explicitly left unfixed, "flagged for follow-up") in `write()` —
the OHLCV path. `write_macro()` is a separate method with its own
independent copy of the identical mistake; it was never in ADR-045's
stated scope and had no entry anywhere in `KNOWN_RISKS.md`.

**Fix.** `write_macro()` gained a required `run_date: date` parameter
(no default — matching this codebase's established pattern of forcing a
loud `TypeError` at every un-updated call site rather than silently
preserving the bug for callers that don't pass it, same choice G1 made
for `resolve_start_date()`). `date_prefix` and the filename's date
segment now come from `run_date`. `datetime.utcnow()` is retained only
for the `_ingested_at` audit column and the filename's `HHMMSS`
uniqueness suffix — neither carries reproducibility meaning.

Updated all 5 real call sites: `fred_ingester.py`, `bea_ingester.py`,
`bls_ingester.py`, `imf_ingester.py`, `eia_ingester.py` — each already
had `run_date` in scope inside its own `run(self, run_date, ...)`
method, so this was a mechanical addition of `run_date=run_date` at
each call, not a design decision. `bis_rates_ingester.py` was checked
and confirmed to use `self.write()` (the OHLCV path), not
`write_macro()` — unaffected, out of scope.

Deliberately did **not** touch `write()` itself, even though it has the
identical `datetime.utcnow()` pattern per ADR-045's own prior finding.
That fix belongs to the OHLCV path, wasn't part of what Ovi
diagnosed/approved this session, and touching it would have meant
re-verifying a completely different set of call sites and tests under
time pressure that wasn't asked for. Left as a known, already-documented
adjacent issue (ADR-045's own text already covers it).

## 4. FIX GMI-FRED-COUNT-01 — idempotent skips counted as "OK"

```python
self.write_macro(df=df, source="fred", domain=domain, series_id=series_id)
success += 1
```

`success` incremented unconditionally right after the `write_macro()`
*call*, never inspecting its return value. An idempotent skip returns
`None`; it was counted identically to a real write. This is exactly
what made the `bronze_treasury` log read as a clean success —
`"[FRED] Complete: 1 OK, 0 failed"` — on a run that wrote zero new
Bronze rows (the "1 OK" was `MORTGAGE30US` hitting bug #3's
wrong-reference-date idempotent skip).

**Fix.** `success` now only increments when `write_macro()` returns a
non-`None` path. A new `skipped` counter tracks idempotent skips
separately. Closing log line: `"N OK, M failed, K skipped (idempotent)"`
— can no longer read as a clean run while silently doing nothing.

Confirmed via `grep` that `bea_ingester.py`, `bls_ingester.py`,
`imf_ingester.py`, `eia_ingester.py` all share the same unconditional
`success += 1` pattern after their own `write_macro()` calls. Left
untouched — this was diagnosed and approved for `fred_ingester.py`
specifically; extending it to the other four ingesters is a reasonable
follow-up but wasn't part of this session's scope. Noted here (and in
the RISK-29 entry) so it isn't lost.

## 5. Verification

Before touching anything: cloned `alpha-factory` fresh into the sandbox
via `git clone` (confirmed in sync with the live repo — same
`pyproject.toml` version, same latest commits), installed via `poetry
install`, ran the full suite once to confirm the 1574-test baseline was
genuinely green before any edit.

Found every call site of `write_macro()` across `src/` and `tests/`
before changing its signature (`grep -rn "write_macro"`), to make sure
the required-parameter change wouldn't silently break an un-updated
caller. Checked every test file that mocks or monkeypatches
`write_macro()` (`test_bea_ingester.py`, `test_fred_ingester.py`,
`test_imf_ingester.py`, `test_eia_ingester.py`) for hardcoded kwarg-set
assertions that the new `run_date` kwarg might break — none existed,
all use `**kwargs`-style capture or full-replacement mocks. Checked
every `JOB_REGISTRY`/`DAILY_SEQUENCE`/`WEEKLY_SEQUENCE`/`LAYER_JOB_NAMES`
assertion in the integration suite for hardcoded exact-length or
exact-list checks that a new job might break — all use `>=` floors or
values derived from the registry itself at test time, not hand-copied
literals.

3 new regression tests (`TestWriteMacroDateOwnership`,
`tests/unit/test_base_ingester.py`) reproduce the exact WIB/UTC
day-boundary shape found live, using a mocked, fixed `datetime.utcnow()`
subclass rather than depending on wall-clock timing at test-run time:
filename date must come from `run_date` even when `utcnow()` reports the
day before; idempotency for the same `run_date` must survive a UTC
day-rollover between two calls; two genuinely different `run_date`s must
not collide.

16 new tests (`tests/unit/test_fred_daily_ingester.py`) cover
delegation with the correct `series_filter`, the filter's exact 9-series
contents (naming the 3 regime inputs explicitly as a drift guard), zero
overlap with `TREASURY_FRED_SERIES`, delegate-exception isolation, and
`job_registry.py` wiring — registered, no `depends_on`, no
`run_on_weekdays`, present in `DAILY_SEQUENCE` and
`LAYER_JOB_NAMES["bronze"]`, absent from `WEEKLY_SEQUENCE`'s
weekly-only prefix (i.e. not double-scheduled).

`ast.parse` clean on every modified and new `.py` file. Full suite:
**1592 passed / 0 failed / 0 error** (1574 baseline + 18 new — 3
`TestWriteMacroDateOwnership` + 15 `test_fred_daily_ingester.py`),
collected count re-verified directly via `pytest -q` rather than
computed by hand. `COUNT_BASELINE.txt`: 1574 → 1592. Version: 1.17.8 →
1.17.9 (PATCH — `bronze_fred_daily` is a
recovery mechanism for capability the registry already declared
intended, `regime_input: true` and `cadence: daily` both predate this
fix; not a new capability by this project's own versioning convention).

Mirrored file-by-file via Filesystem MCP `edit_file`/`write_file`, each
verified with `copy_file_user_to_claude` + `diff` against this tested
sandbox source of truth before being considered done.
