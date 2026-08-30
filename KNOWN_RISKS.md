# Known Risks

This document tracks risks that are accepted/understood by design rather
than bugs to be fixed. Each entry states the risk, its blast radius, the
mitigation currently in place, and what an operator should do if it
materializes.

---

## RISK-1: tvdatafeed is an unofficial, reverse-engineered API — RESOLVED (retired)

**Status:** ✅ **RESOLVED — tvdatafeed retired entirely as a platform
dependency.** Fixed via `GMI_Decision_Document_v7.docx` Decision I
(ADR-029, 30 Jul 2026). Originally tracked under Production Readiness
Assessment v1.7.2, **GAP-10** (P3 LOW) as an accepted, partially-mitigated
risk — see "Resolution" below for why mitigation escalated to full
retirement rather than staying at detection-and-alerting.

**GD Reference:** GD §9.1 (IDX30 Primary Source), §3.3.2 (Market OHLCV Sources).

### What the risk is

[`tvdatafeed`](https://github.com/StreamAlpha/tvdatafeed) is a third-party
Python library that talks to TradingView's **private WebSocket API** —
the same one TradingView's own web/desktop charting client uses
internally. It is not a published, supported, or rate-limit-documented
API. Using it:

- Likely violates TradingView's Terms of Service (reverse-engineered
  access to a private endpoint, not a public developer API).
- Can **break without warning** if TradingView changes their WebSocket
  protocol, auth flow, or rate limiting — there is no deprecation notice,
  changelog, or SLA, because this was never a published integration.
- Depends on a TradingView account (`TV_USERNAME` / `TV_PASSWORD` in
  `.env`) that could be rate-limited, flagged, or suspended at
  TradingView's discretion.

### Blast radius (historical — tvdatafeed no longer in the source chain)

`tvdatafeed` was the **primary source for IDX30** (30 of 640 Layer 1
instruments, GD §9.1) — Indonesian large-cap equities. No other free-tier
source in the GD §3.4 Data Source Configuration Matrix covers IDX with
comparable depth:

- **Fallback (pre-ADR-029):** `YFinanceJKAdapter` (`.JK` suffix) via
  `ChainedAdapter([TvDatafeedAdapter(), YFinanceJKAdapter()])`
  (`src/bronze/market_ingester.py`). Coverage was **lower** — some IDX30
  constituents were thinly covered or entirely absent on yfinance's
  Indonesian listings. As of ADR-029, `YFinanceJKAdapter` is IDX30's SOLE
  source (`ChainedAdapter([YFinanceJKAdapter()])`), not a fallback — the
  same coverage gap that existed as a *fallback* limitation now applies
  unconditionally, since there is no longer a primary to fall back from.
- If both `tvdatafeed` AND the yfinance `.JK` fallback fail for a symbol
  on a given day, that symbol simply has no Bronze OHLCV for that day —
  it silently drops out of Silver/Gold for that date (existing
  `ChainedAdapter` behavior, GD §3.5), not a crash, but a coverage gap.
- A full IDX30 outage degrades: `silver_ohlcv` (idx market subset),
  `gold_signals` (idx timeframes), `gold_mtf`, `gold_screener` (IDX
  candidates drop out of ranking), and `gold_correlation` (one fewer
  asset class in the correlation matrix).
- Does **not** affect US stocks, forex, commodities, or macro data —
  those are sourced independently per GD §3.4.

### Mitigation in place

1. **Session resilience** (IDD §6, pre-existing): `TvDatafeedSessionManager`
   (`src/bronze/tvdatafeed_session.py`) auto-reconnects, retries with
   exponential backoff, and runs a lightweight health check (`BBCA` 1D
   bar) before trusting a session. `force_reconnect()` is triggered on
   empty results, since `tvdatafeed` often fails silently (empty
   `DataFrame`, not an exception).
2. **Automatic fallback** (GD §3.5, pre-existing): `ChainedAdapter`
   transparently falls through to `YFinanceJKAdapter` per-symbol if
   `tvdatafeed` fails, no manual intervention required.
3. **Runtime coverage alert (FIX GAP-10, superseded by ADR-029):**
   `src/utils/health_reporter.py::_check_idx_coverage()` ran as part of
   the daily `health_report` job. It read the `_source` / `_symbol`
   metadata every Bronze write carried and compared, for each of the 30
   IDX symbols, whether today's data came from `tvdatafeed`, fell back to
   `yfinance_jk`, or was missing entirely. This mechanism is now retired
   along with `tvdatafeed` itself — see "Resolution" below.
   `_check_idx_coverage()` was reworked from tvdatafeed-vs-fallback to a
   simpler presence-vs-missing check (there is only one source now, so a
   source-of-origin distinction is meaningless); the `IDX_PARTIAL_FAILURE`
   alert marker, `IDX_COVERAGE_ALERT_THRESHOLD = 5`, and Telegram-priority
   behavior all carry over unchanged in spirit, just measuring "missing"
   instead of "fallback + missing."

### What this does NOT do

This is a **detection and alerting** mitigation, not a structural fix —
it does not reduce the underlying probability that TradingView breaks
`tvdatafeed` access. If that happens, the operator will know the same
day (via the health report / Telegram alert) instead of discovering a
silent multi-week IDX data gap later, but IDX coverage will still be
degraded until either TradingView access is restored or a migration
(below) happens.

### Operator playbook if `IDX_PARTIAL_FAILURE` fires

1. Check `data/health/pipeline_runs.db` / the terminal health report for
   the exact `idx_fallback_count` / `idx_missing_count` split.
2. If `TvDatafeedSessionManager.is_available` is `False`, check whether
   `TV_USERNAME` / `TV_PASSWORD` are still valid and whether TradingView
   has flagged/locked the account.
3. If the account is fine but `tvdatafeed` itself is broken (protocol
   change), check the upstream repo
   (`github.com/StreamAlpha/tvdatafeed`) for a fix/fork before assuming
   a multi-week outage.
4. Pipeline continues running on the `yfinance_jk` fallback in the
   meantime — no manual action is required to keep the pipeline moving,
   only to restore full IDX30 coverage.

### Resolution (ADR-029, 30 Jul 2026)

tvdatafeed's sign-in started failing in practice, not just in theory —
confirmed via `alpha-factory_preflight_logs___29_July_2026.txt`
(`check_tvdatafeed_symbols.py`, full run): sign-in itself failed ("error
while signin"), the client fell back to "nologin method, data you access
may be limited," and while the session health check (a lightweight IDX
`BBCA` daily bar) reported "Connection established and healthy," every
subsequent fetch for a non-IDX exchange (BMDI, SGX, LME, ICE — the
exchanges the deferred CPO/RUBBER/TIN/COAL_NEWC context instruments
needed) timed out. This reads as a structural nologin-mode access-tier
gap, not a transient blip: the account can reach some baseline
TradingView data (enough for the IDX health check) but not the specific
exchanges this platform needed beyond IDX.

**Decision:** retire `tvdatafeed` entirely rather than keep it as a
lower-priority IDX fallback. `YFinanceJKAdapter` was already the tested
ChainedAdapter fallback for IDX30 and is now its sole source — this is
priority reordering and dependency removal, not new integration risk.
CPO/RUBBER/TIN never got live tvdatafeed wiring in the first place (still
`context_available: false` at the time of this decision), so nothing in
production actually depended on removing it for them — only the
config-intent (`tvfeed_symbol`/`tvfeed_exchange` fields, the
`check_tvdatafeed_symbols.py` `ROUTING_TABLE`) did. All four
(CPO/RUBBER/TIN/NICKEL) were re-sourced as yfinance equity proxies
instead — F34.SI (Wilmar Intl.), STA.BK (Sri Trang Agro), AFM.V (Alphamin
Resources), NIC.AX (Nickel Industries) — confirmed live via
`check_yfinance_tickers.py --candidates`
(`alpha-factory verify-preflight logs — 30 July 2026.txt`).

**What changed:** `TvDatafeedAdapter`/`TvDatafeedSessionManager` archived
to `scripts/archive/` (no import-time side effects, plain move — unlike
RISK-11's guarded scripts). `market_ingester.py`'s `idx_chain` and
`_primary_source_for()` updated to yfinance-only.
`health_reporter.py::_check_idx_coverage()` reworked to presence-vs-
missing (see mitigation #3 above). `pyproject.toml`'s `tvdatafeed` git
dependency removed. `TV_USERNAME`/`TV_PASSWORD` left in `.env` as dead
(not urgent to scrub). This section ("Operator playbook if
`IDX_PARTIAL_FAILURE` fires" above) is now moot for the tvdatafeed-
specific steps (checking `TvDatafeedSessionManager.is_available`, the
upstream `tvdatafeed` repo) — left in place as historical record rather
than deleted, since the general "check the health report, then
investigate the fallback source" shape still applies if `yfinance_jk`
itself ever degrades for IDX.

**What this does NOT resolve:** RUBBER's proxy (STA.BK) returned only
3/5 rows on its initial preflight check (likely a Thai exchange holiday,
not independently confirmed) — flagged for a longer-window re-check
before this feeds a real Bronze run. TIN's proxy (AFM.V) has an
unverified "CIRO trade resumption" headline (~Jan 2026) surfaced during
candidate research, not investigated. Neither blocks this resolution —
both are monitored risks on the *new* sources, unrelated to the
tvdatafeed retirement itself, and are lower severity than a fully broken
primary source. Also unresolved: none of the 4 proxies has an empirical
`proxy_correlation_expected` value (unlike VALE's ~0.81, ADR-005) —
`instruments_taxonomy.yaml`'s own NICKEL/TIN/CPO/RUBBER entries carry
this exact caveat inline, deliberately leaving `proxy_for`/
`proxy_correlation_expected` unset rather than guessing. **Update (9 Aug
2026):** a new preflight script,
`scripts/preflight/check_proxy_correlation.py` (17 new tests), computes
month-over-month return correlation between each proxy's yfinance
history and its FRED Track 2 benchmark (RISK-15) — authored and
unit-tested against synthetic data, not yet run against real
yfinance/FRED. Wiring a real result into `instruments_taxonomy.yaml` is
a follow-up decision once that run happens.

**Update (10 Aug 2026) — run for real, decision made (ADR-034):**
`check_proxy_correlation.py` was run live against real yfinance/FRED
data for the first time. Results, against each proxy's RISK-15 FRED
Track 2 benchmark: NICKEL (NIC.AX) +0.586/36mo, CPO (F34.SI) +0.405/120mo,
RUBBER (STA.BK) +0.229/120mo, TIN (AFM.V) +0.139/120mo — all well below
this platform's own VALE (~0.81, ADR-005) / WHC.AX (~0.78, ADR-006)
proxy-quality precedent. `GMI_Decision_Document_v8.docx` ADR-034
(differentiated by measured strength, not treated uniformly): **TIN and
RUBBER re-deferred** (`context_available: false`, `deferred_reason` set,
`proxy_for`/`proxy_correlation_expected` left unset — same state they
held before ADR-031/032) — too weak to trust as unbiased context
anchors. **CPO and NICKEL retained active**, now with `proxy_for`
(`CPO_FCPO_BMDI`, `NICKEL_LME_NI`) and `proxy_correlation_expected`
(0.405, 0.586) set to the measured values, plus an explicit "moderate,
not strong" caveat in each entry's `notes`. NICKEL's 36-month sample
(vs. 120 for the other three) is flagged separately — cause (NIC.AX's
own price-history depth on yfinance vs. something else) not
investigated. Implemented via the Filesystem MCP connector on
11 Aug 2026, sandbox-validated first (`validate_instruments.py` exit 0,
699 symbols; full `pytest` suite: 1466 collected, 0 failed) before being
mirrored to the live repo. `deferred_count()`: 0 → 2.

---

## RISK-2: DuckDB rejects glob patterns with multiple `**` wildcards — RESOLVED (audited)

**Status:** ✅ **AUDITED — no further instances found.** Originally logged
as "unaudited elsewhere" during GMI Wave 1 Cycle 3; closed during the
Bronze+Silver formal audit that immediately followed Cycle 3 (per the
project's own precedent — see R-4 audit trigger below).

**GD Reference:** GD §17.7 (DuckDB SQL conventions). Discovered during GMI
Wave 1 Cycle 3 (Task 9.1); resolved during the Cycle 3→4 transition audit.

### What the risk was

`_get_latest_vix()` contained a glob string with **two** `**` wildcards in
one path. DuckDB's `read_parquet()` rejects this outright
(`IO Error: Cannot use multiple '**' in one path`), confirmed to predate
GMI Wave 1 entirely — masked since the function was first written by a
blanket `except Exception: pass`. Full detail preserved in CHANGELOG.md
v1.8.0 (GMI-GLD-001).

### Audit performed to close this

1. Comprehensive grep across the **entire** `src/` tree (not the narrower
   Bronze/Silver-only check done during Cycle 3) for every occurrence of
   `**` in a string literal — 60+ hits reviewed individually.
2. Every genuine double-`**`-in-one-string case identified and its read
   mechanism checked directly: the only two beyond the already-fixed
   `_get_latest_vix()` are in `ohlcv_processor.py`'s Bronze→Silver PASS 1
   loop (Layer 1's pre-existing loop, and Cycle 3's new `run_context()`,
   which deliberately mirrors it) — both use `pl.scan_parquet()`
   (**Polars**), not DuckDB. Confirmed empirically that Polars tolerates
   the identical double-`**` string DuckDB rejects — this is a
   library-specific limitation, not a universal glob-syntax error.
3. `quality_validator.py` (the largest DuckDB-based consumer of Silver
   OHLCV globs, 8 call sites) checked specifically: every glob is a
   **single** `**` — safe per the same empirical DuckDB probe. Its own
   36 tests were re-run directly and confirmed passing, corroborating
   these patterns genuinely execute against real fixture data, not just
   "present in code that happens to never run."
4. All other `read_parquet($glob...)` call sites across
   `pit_data.py`, `correlation_matrix.py`, `screener.py`, `eia_ingester.py`,
   `fred_ingester.py`, `imf_ingester.py`, `finnhub_ingester.py`,
   `inc_fetch.py`, `fundamental_processor.py`, `macro_processor.py`,
   `global_rates_processor.py`, `sentiment_processor.py`,
   `delta_reprocessor.py`, `health_reporter.py`, `active_symbols.py`,
   `pipeline_dashboard.py` — all use a single `**` per pattern.

### Conclusion

The double-`**`/DuckDB defect class was an **isolated incident**, not a
systemic pattern — one occurrence, now fixed, with test coverage
(`test_technical_signals_vix_path.py`) that would catch a regression.

### Residual limitation (honest, not a blocker)

A grep-based audit, even a careful one, cannot prove the *absence* of a
dynamically-constructed glob that assembles a double-`**` pattern at
runtime through string concatenation across multiple variables in a way
no static search would surface. None was found, and the codebase's own
convention (build the glob as one `str(Path(...) / "**" / ... )`
expression, always visible at the call site) makes this an unlikely
failure mode — but "no static search found one" is evidence, not proof.
If a similarly silent, `except: pass`-masked failure surfaces again in
the future, this defect class should be the first thing checked.

---

## RISK-3: Pre-existing f-string SQL in `sector_rotation.py` / `views.py` — RESOLVED (fixed)

**Status:** ✅ **FIXED.** Both violations resolved; a permanent,
AST-based, whole-`src/`-tree regression guard added
(`TestNoFStringSQLAnywhereInSrc` in `test_fstring_sql_absence.py`) so this
class of gap — "audited piecemeal, file by file, with each audit's scope
narrower than the codebase" — cannot recur silently.

**GD Reference:** GD §17.7 (f-string SQL anti-pattern, hard constraint).
CI/CD Ops Guide v1.7.4, Gate G-2.

### What the risk was, and why the prior audit (GLD-003) missed it

```
src/gold/sector_rotation.py:193:  f"SELECT regime FROM read_parquet('{regime_path}')"
src/gold/views.py:182,196:        f"SELECT COUNT(*) FROM {view_name} LIMIT 1"
```

Two compounding causes, both confirmed by reading the prior audit's own
test file: (1) `test_fstring_sql_absence.py`'s docstrings **explicitly**
scoped GLD-003 to specific files, admitting `quality_validator.py`,
`macro_processor.py`, and most Bronze ingesters were never checked, "to be
addressed in a formal Silver audit" that never happened; (2) the
scanner function itself (`_scan_fstring_sql_violations`) only matched
**triple-quote** f-strings (`f"""`/`f'''`) — both violations use
single-quote f-strings (`f"..."`), which would not have been caught even
if the files had been in scope.

### Fix applied

- `sector_rotation.py:193` — straightforward value interpolation, fixed
  to `$name` parameterized binding (`FIX GMI-AUD-001`).
- `views.py:182,196` — `view_name` is a SQL **identifier** (a view name),
  not a value. No SQL engine can bind an identifier via `$name`/`?`
  parameter substitution — parameterization is defined for values only.
  The correct fix is a validated, quoted-identifier helper
  (`_quoted_identifier()`, `FIX GMI-AUD-002`): regex-validates the name
  against `^[A-Za-z_][A-Za-z0-9_]*$`, wraps it in double-quotes, raises
  `ValueError` on anything else, and is applied via plain string
  concatenation (not an f-string). `view_name` in this codebase always
  originates from iterating the module's own hardcoded `VIEW_DEFINITIONS`
  dict — never external input — so the guard is defense-in-depth against
  future refactoring, not a response to a live injection path.
- **Broader, structural fix**: replaced the old triple-quote-only,
  targeted-files-only scanner with an AST-based scanner
  (`_scan_fstring_sql_violations_ast`) that inspects the literal text of
  every `ast.JoinedStr` node — precise (checks only the f-string's own
  content, not a character-window that can sweep in unrelated nearby
  code) and now runs across the **entire** `src/` tree, permanently,
  covering every file this class of finding previously slipped past.
  Validated against 7 individually-confirmed false-positive candidates
  (log messages sitting near genuine parameterized SQL) before being
  trusted — it correctly ignores all seven.

### Verification

19 new tests added (`test_sector_rotation.py::TestGetActiveRegimeParameterizedQuery`,
new `test_views.py`, `test_fstring_sql_absence.py::TestNoFStringSQLAnywhereInSrc`).
Full suite: 1055 passed, 0 failed (up from 1036 pre-audit). CI Gate G-2
(`grep -rn 'f"SELECT...'`) re-run manually: clean.

---

## RISK-4: `finnhub_ingester.py` writes to Bronze with zero schema validation — RESOLVED (retired)

**Status:** ✅ **RESOLVED — Finnhub retired entirely as a platform data
source (both halves: sentiment and earnings/quotes).** Fixed via
`GMI_Decision_Document_v10.docx` (ADR-043, ADR-044, 22 Aug 2026). The
hardening work below (schema validation, dtype-casting fragility fixes)
is now moot rather than in reserve — see "Resolution" below.

**GD Reference:** GD §3.1, §3.7 (unchanged from original entry).

### What was blocking this, and how it was resolved (historical — pre-retirement)

The original entry correctly identified the real blocker: designing a
schema from the ingester's own field list alone (`symbol`,
`earnings_date`, `eps_estimate`, ... ) without verifying against Finnhub's
actual API contract "risks encoding assumptions as fact." Rather than
guess, Finnhub's documented `/quote` and `/calendar/earnings` response
shapes were looked up directly (field names, types, and — critically —
which fields are nullable) and used as the schema source of truth:
`config/schemas/finnhub_quote.yaml`, `config/schemas/finnhub_earnings_calendar.yaml`.

### A second fragility found and fixed in the same pass (historical)

Designing the schema against real documentation surfaced a problem a
naive schema-plus-validator wiring would NOT have caught: this ingester
fetches a 90-day **forward** earnings window, so `eps_actual` is
genuinely `None` for essentially every row in real operation — the
*normal* case, not an anomaly. Letting Polars infer column dtypes from
raw API dict values means an all-`None` column infers Polars' `Null`
dtype, not `Float64` — which would fail an exact-match schema check on
the single most common case. A related fragility: `revenueEstimate` can
arrive as a whole-number JSON integer (no decimal point) or a float
depending on the value, so an all-integer batch would infer `Int64`
against a `Float64` schema declaration. Both write paths explicitly
`.cast(pl.Float64, strict=False)` / `.cast(pl.Int64, strict=False)` every
column immediately after DataFrame construction, before validation —
this made the schema contract stable regardless of what values happen
to be present in a given fetch, rather than merely "usually correct."
All of this — the schema files, the casting fix, `finnhub_ingester.py`
itself — was deleted intact as part of ADR-043; none of it was found to
be wrong, it simply no longer has anything to validate.

### What was intentionally NOT touched (historical, now superseded)

`_bronze_finnhub`'s `NotImplementedError` block (job_registry.py) was
originally left unchanged — this fix hardened `FinnhubIngester` for when
that block might eventually be lifted ("Finnhub Integration" roadmap
item). ADR-043 closes that roadmap item as retired rather than lifting
it — see Resolution below.

**UPD v1.10.0:** the `high_52w`/`low_52w` column-naming misnomer mentioned
below (fixed at the time as "out of scope, no current consumer") was
revisited in v1.10.0 per `GMI_Decision_Document_v2.docx` §5 and renamed to
`day_high`/`day_low`. Its stated rationale ("zero current consumers, zero
migration cost") was verified **false** during that rename:
`src/silver/fundamental_processor.py::process_quotes()` is a real, live
consumer that reads these exact columns from Bronze and writes them
through to Silver unchanged — both the Bronze producer and the Silver
consumer were updated together. See CHANGELOG.md v1.10.0 for full detail.
(`fundamental_processor.py` itself was deleted under ADR-043 — this
history is kept for the record, not because the code still exists.)

### Blast radius — historical, moot post-retirement

Unchanged from the original entry until ADR-043: `_bronze_finnhub`'s
unconditional `NotImplementedError` meant this code path was never
reachable through the normal pipeline. Post-retirement, there is no
code path at all — `bronze_finnhub` does not exist in `JOB_REGISTRY`.

### Verification (historical, pre-retirement)

40 new tests added across `tests/unit/test_finnhub_ingester.py` (16 —
first test file this module ever had) and `tests/unit/test_pandas_indicators.py`
(10, unrelated discovery — see new entry below) plus others from the same
session. Full suite: 1131 passed, 0 failed. (`test_finnhub_ingester.py`
itself was deleted under ADR-043, along with the module it tested.)

### Resolution (ADR-043 + ADR-044, 22 Aug 2026)

A live `--job bronze` run (21 Aug 2026) surfaced `bronze_finnhub_sentiment`
returning `FinnhubAPIException(status_code: 403)` for every symbol in the
640-instrument universe — a plan-tier gate (confirmed against Finnhub's
own documented tier-gating response and an identical, independently-
reported occurrence on Finnhub's public issue tracker), not an invalid
key (which returns 401) or a code defect. The job still "succeeded" —
zero real records is only a WARNING-level log, the sentinel is still
written, silently indistinguishable from success in a status check that
only looks at sentinel presence.

Exploring "treat Finnhub like `tvdatafeed`" (RISK-1 above) surfaced a real
disanalogy: RISK-1's retirement rested on a *structural* failure (sign-in
itself broken) with a *proven substitute already in place*
(`YFinanceJKAdapter`). Neither holds for Finnhub sentiment — its 403 is a
purchasable-plan gap, and nothing stands in for it. The earnings/quotes
half (this entry's original subject) is the closer match to RISK-1: it
never ran in production (permanent `NotImplementedError` stub, FIX
R-F04) and nothing in the live pipeline consumed its output
(`gold_screener` was already decoupled from it, FIX NEW-2).

**Decision:** Ovi's explicit instruction — full retirement of both halves
together, rather than a split verdict (sentiment kept dormant/accepted-
risk, only earnings/quotes retired). The disanalogy above was presented
as the case for a split; recorded here as the honest basis for the full-
retirement call, not as an unresolved objection. Treating both halves
identically avoids a subtler cost than the disanalogy alone would
suggest: a partial retirement leaves one Finnhub integration fully gone
and one merely paused, a harder distinction for a future reader to hold
onto correctly than "Finnhub is not currently a data source in this
platform, full stop."

**What changed:** Removed from the active codebase — `src/bronze/finnhub_ingester.py`,
`src/bronze/finnhub_sentiment_ingester.py`,
`src/silver/fundamental_processor.py`, `src/silver/sentiment_processor.py`,
all 3 `config/schemas/finnhub_*.yaml` files, `scripts/preflight/check_finnhub_shape.py`,
and all 4 dedicated unit test files. ADR-043's own Consequences section
originally specified outright deletion (`git rm`) — that changed
mid-implementation for a purely mechanical reason: the session mirroring
these changes to the live repository had read/write/move access but no
file *deletion* capability, and Ovi's direction was to use an archive
approach instead of deletion given that constraint. All 12 files were
moved byte-identical (verified via size comparison against their git
blobs; original mtimes preserved) into
`archive/finnhub_retirement_2026_08/`, mirroring their original paths,
with its own README explaining the archival and warning against
importing from it, collecting its tests, or treating it as a drop-in
restore path. Functionally this is equivalent to deletion — none of
these files are imported, scheduled, or reachable from any live code
path — the difference is that the bytes persist in the archive
directory rather than only in git history. `job_registry.py`: 4 `JOB_REGISTRY`
entries + 4 wrapper functions removed; `bronze_finnhub_sentiment` and
`silver_sentiment` removed from `DAILY_SEQUENCE` (16 → 14 entries); the
commented-out `silver_fundamental` line removed from `WEEKLY_SEQUENCE`;
`gold_screener.depends_on` no longer references `silver_sentiment`
(supersedes FIX NEW-2's narrower `silver_fundamental`-only guard).
`screener.py` (ADR-044): `_enrich_earnings()`/`_enrich_sentiment()` and
their call sites removed — rather than left importing a now-deleted
module inside a broad `except Exception` (the exact RISK-13 anti-pattern
this codebase's own history argues against). Watchlist output schema
unchanged: `days_to_earnings`, `next_earnings_date`, `near_earnings_flag`,
`sentiment_score`, `buzz_score` remain, permanently `NULL` via the main
query's own placeholders, not via either enrichment step — no Interface
Contract change (GD §0.4, §17.6). `SourceLimiters.finnhub`
(`rate_limiter.py`) removed as a direct mechanical consequence — zero
remaining consumers once the two ingesters above were deleted, unlike
`.polygon`/`.alphavantage`/`.yfinance`, each of which has a live adapter.
`pipeline_scheduler.py` (GD §14.5, dormant APScheduler path): `bronze_finnhub`/
`silver_sentiment` removed from both the documented cron table and the
actual `daily_schedule` list — both would have `KeyError`'d against
`JOB_REGISTRY` on first activation. `pyproject.toml`: `finnhub-python`
removed; `poetry.lock` regenerated (diff: exactly 1 package removed, 0
added). `FINNHUB_API_KEY` removed from `.env.example` (this file's own
stated convention is reconciliation against real `os.getenv()` call
sites, not aspirational listing — zero remain).

**What this does NOT resolve / still open:** Sentiment's 403 is a
purchasable-plan gap, not a structural one — reversible any time Finnhub's
plan is upgraded, unlike RISK-1's tvdatafeed sign-in failure. Retiring it
now (rather than carrying it as a dormant/accepted-risk entry) trades that
reversibility for a simpler, smaller, more honestly-described system
today — a real trade-off, accepted knowingly per Ovi's explicit
instruction, not defaulted into. Re-integrating either half in future
means writing new ingesters/processors from scratch, not un-commenting
anything — nothing was archived, everything was deleted.

## RISK-5 (NEW): `add_adx()` crashed on every real invocation — RESOLVED (fixed)

**Status:** ✅ **FIXED.** Discovered empirically while adding the first-
ever test coverage for `technical_signals.py` / `pandas_indicators.py` —
neither had any prior test file. This was a live, P0, production-breaking
defect confirmed present in the pristine repo before this session's
changes (byte-identical diff check against the untouched extraction).

**GD Reference:** GD §5.2.2 (Technical Signal Store schema — `adx`,
`di_plus`, `di_minus` columns).

### What the risk was

`src/gold/indicators/pandas_indicators.py::add_adx()` renamed pandas-ta's
output columns via `if lc.startswith("adx"): col_map[c] = "adx"`. The
installed pandas-ta version (0.4.71b0) returns **four** columns from
`ta.adx()`, not three: `['ADX_14', 'ADXR_14_2', 'DMP_14', 'DMN_14']`.
`ADXR` (Average Directional Index **Rating**, a smoothed variant this
pipeline never asked for) also starts with the substring `"adx"`, so
`col_map` mapped **both** `ADX_14` and `ADXR_14_2` to the single target
name `"adx"` — producing a pandas DataFrame with a duplicate column name.
`pl.from_pandas()` correctly rejects this
(`ValueError: Pandas dataframe contains non-unique indices and/or column
names`), meaning **every** real call to `add_adx()` — i.e. every real
invocation of `gold_signals`, and everything downstream of it
(`gold_mtf`, `gold_screener`) — raised. Confirmed via direct isolated
reproduction against the real installed library, not a hypothetical.

### Why this was invisible

`pandas_indicators.py` (`add_adx`, `add_bbands`) had **zero** test
coverage anywhere in the repo — confirmed via a full grep of `tests/`
finding no reference to either function under any name, in any file, at
any point in this project's history. `technical_signals.py`'s own
`_process_timeframe()`/`run()` had similarly never been tested beyond one
narrow path fix (`test_technical_signals_vix_path.py`, itself added only
for the `_get_latest_vix()` bug). No integration or smoke test exercised
the real Gold indicator pipeline against real `pandas-ta` output.

### Fix

Changed the match from `lc.startswith("adx")` to `lc.startswith("adx_")`
— matches `ADX_14` (immediately followed by the length-suffix
underscore) while correctly excluding `ADXR_14_2` (immediately followed
by `r`, not `_`). `add_bbands()` was audited against the same real
`pandas-ta` output as part of this investigation and confirmed to have
**no** analogous collision (`BBB_`/`BBP_` columns match none of the
upper/mid/lower target patterns) — no change needed there.

### Verification

New `tests/unit/test_pandas_indicators.py` (10 tests, first ever for
this file) includes a direct regression guard
(`test_adxr_is_not_silently_used_as_adx`) that would catch this exact
collision recurring even if a future pandas-ta version adds yet another
ADX-prefixed column — it computes ADX/ADXR independently via pandas-ta
and asserts the wrapper's `adx` column matches ADX, not ADXR, rather than
only checking "no crash." Full suite: 1131 passed, 0 failed.

---

## RISK-6 (NEW): Layer 1 Silver quality/signal checks silently scanned Layer 2 rows — RESOLVED (fixed)

**Status:** ✅ **FIXED** (originally, v1.9.0 — `quality_validator.py`,
`technical_signals.py`) **— then FOUND TO BE INCOMPLETE and further FIXED
(v1.10.0 — 6 additional instances)** via CI Gate G-8
(`GMI_Decision_Document_v2.docx` ADR-022), which was added specifically
because this defect class had already been found by manual code-reading
once and the project had no static guard against a recurrence. It found
one immediately: `pit_data.py`, `correlation_matrix.py`, `screener.py`,
`views.py`, `delta_reprocessor.py`, `pipeline_dashboard.py` all had the
identical unfiltered `market_ohlcv/**/...` glob pattern, undiscovered by
the v1.9.0 audit because that audit's own scope was "the two consumers
found via the code-reading path that thread happened to follow" (its own
stated limitation, Checkpoint v3 §11.3) — not an exhaustive one.

**GD Reference:** GD §13.1 (Data Quality Checks), §15.2 (System Health
Thresholds — coverage/freshness gates); Architecture v2.0 §5.2 (gold_signals
scope); Architecture Extension v1.0 ADR-003 (VIX/DXY/SPX reclassified out
of Layer 1 specifically because indicator computation on them is not
meaningful).

### What the risk was

`quality_validator.py`'s checks and `technical_signals.py`'s Silver read
both globbed `data/silver/market_ohlcv/**/*.parquet` with **no market
filter**. Since GMI Cycle 3 added Layer 2 context OHLCV under the same
root (`market_ohlcv/context/...`), every one of these globs was silently
also scanning Layer 2 rows. Confirmed to cause three separate, concrete
problems (see `src/utils/silver_scope.py` module docstring for the full
empirical reproduction, now also captured as permanent regression tests):

1. `quality_validator.py::_check_coverage` — Layer 2 symbols inflated
   `COUNT(DISTINCT symbol)` against a Layer-1-only denominator
   (`get_loader().count()`), letting coverage% mask a real Layer 1 drop
   below the 95% gate.
2. `quality_validator.py::_check_freshness` — a single fresh Layer 2
   anchor (e.g. VIX) hid pipeline-wide Layer 1 staleness, since
   `MAX(timestamp)` spanned both layers combined.
3. `technical_signals.py::_process_timeframe` — RSI/MACD/ADX/BBands were
   being computed for VIX, DXY, 13 global indices, 25 ETFs, and 8
   commodity context anchors as if they were tradeable candidates,
   directly contradicting ADR-003's own stated rationale for
   reclassifying VIX/DXY out of Layer 1 in the first place.

### Fix

New shared utility `src/utils/silver_scope.py` (`layer1_globs()`,
`context_glob()`) derives the Layer 1 market list from `InstrumentLoader`
rather than a hardcoded/unfiltered glob, and is now used by both
`quality_validator.py` (5 existing checks re-scoped) and
`technical_signals.py` (Silver read re-scoped). A full parallel Layer 2
check suite (`_check_context_*`, 6 checks) was added to
`quality_validator.py` at **WARNING** level — deliberately not CRITICAL,
since no Gold-layer consumer of Layer 2 Silver OHLCV exists yet
(CrossAssetEngine is Cycle 4, not yet built); blocking the entire Gold
layer over a Layer 2 anchor's data hiccup today would over-couple an
unrelated future consumer's readiness to Layer 1's gate. `technical_signals.py`
also gained the Architecture v2.0 §5.2 `active_ohlcv` filter
(`ActiveSymbolsResolver.load_ohlcv()`) that had never been implemented,
with graceful fallback to the full Layer-1-scoped universe when
unavailable.

### Verification

Both masking bugs reproduced as permanent regression tests (not just
fixed and trusted) — `test_quality_validator.py::TestQVL2MaskingBugsFixed`
and `test_technical_signals.py::TestProcessTimeframeLayer1Scoping`. New
`test_silver_scope.py` (11 tests) covers the shared utility directly,
including a guard against ever reintroducing a double-`**` glob (the
RISK-2 defect class). Full suite: 1131 passed, 0 failed.

### v1.10.0 — Six additional instances found and fixed (Gate G-8)

Severity varied materially across the six:

- **`screener.py::_check_data_freshness`** — **P1, genuine correctness
  bug**, the same masking-bug shape as the original `_check_coverage`
  finding: a freshness GATE whose entire purpose is blocking the
  screener on stale Layer 1 data could be silently satisfied by fresh
  Layer 2 anchors.
- **`views.py`** — **highest blast radius**: these three DuckDB views are
  the documented Interface Contract (GD §0.4) for the Trading Engine, an
  external consumer this pipeline cannot audit or coordinate with. A
  Trading Engine querying "give me OHLCV" had no way to know VIX/DXY/an
  ETF could appear as if tradeable — precisely what ADR-003 reclassified
  them out of Layer 1 to prevent. The first fix attempt for this file was
  itself wrong (baked a fixed 4-market SQL list at Python import time,
  which broke immediately against any environment without data in all 4
  markets simultaneously — DuckDB's `read_parquet()` list argument raises
  for the whole query if even one entry matches zero files) — corrected
  to resolve the glob list fresh at connection-creation time.
- **`pit_data.py`, `correlation_matrix.py`, `delta_reprocessor.py`** —
  lower severity: each already applied an explicit per-symbol or
  `active_symbols`-filtered `WHERE` clause downstream, so Layer 2 rows
  were being scanned unnecessarily but not silently miscounted in an
  aggregate. Still fixed for consistency and to close Gate G-8 cleanly.
- **`pipeline_dashboard.py`** — lowest severity, a display-only
  diagnostic; split into separate Layer 1 / Layer 2 rows as a genuine
  accuracy improvement for the operator, not just gate compliance.

Full detail, including the corrected views.py design and all new tests,
is in CHANGELOG.md v1.10.0. Full suite after all six fixes plus the new
Gate G-8 scanner and its own tests: 1188 passed, 0 failed.

---

## RISK-7 (NEW): pandas-ta's entire 0.3.x line removed from PyPI — RESOLVED (migrated to pandas-ta-classic)

**Status:** ✅ **FIXED** via migration to `pandas-ta-classic`
(`GMI_Decision_Document_v2.docx` ADR-020, v1.10.0). Recorded here as a
permanent historical note per that ADR's own requirement — this is
exactly the kind of "silent upstream break with no changelog or SLA"
this document exists to track (see RISK-1's framing for the same
category of risk applied to a different dependency).

### What happened

At some point before July 2026, PyPI removed the *entire* `pandas-ta`
0.3.x release line — including `0.3.14`, the version this project's
`pyproject.toml`/`environment.yml` had declared as the floor since the
original Grand Design. Only two releases remained on the index,
`0.4.67b0` and `0.4.71b0`, both explicitly prerelease-tagged and both
`requires_python >=3.12`. This broke CI's dependency-install step
entirely — not a test failure, a resolution failure, meaning nothing
downstream could even be verified.

This was a **double barrier**, not a single Python-version mismatch:
confirmed empirically that even on Python 3.12, the exact declared
constraint (`pandas-ta>=0.3.14`) still failed to resolve, because `pip`
excludes prerelease versions from an explicit `>=` floor specifier by
default. Only a bare unconstrained install, `--pre`, or a prerelease-
tagged floor (e.g. `>=0.4.67b0`) would have resolved it.

### Why this matters beyond "CI was red"

Neither `KNOWN_RISKS.md` (this file, before this entry) nor
`CHANGELOG.md` mentioned this anywhere prior to its discovery — it was
found only when a live CI failure log was supplied directly and
diagnosed from first principles (direct PyPI JSON API queries against
`https://pypi.org/pypi/pandas-ta/json`), not from any existing
documentation. A `pyproject.toml`/`environment.yml` dependency pin is
only as durable as its upstream's continued existence on PyPI — this is
the first time that assumption broke for this project, and is unlikely
to be the last for *some* dependency eventually.

### Fix

Migrated to `pandas-ta-classic`, a community-maintained continuation of
the exact same abandoned 0.3.x lineage (`0.3.14b1` → ... → `0.6.52`),
with genuine stable releases and a Python floor (`>=3.9`/`>=3.10`)
compatible with this project's stated `>=3.11` floor — which was **held
at 3.11**, not bumped to 3.12, specifically to avoid quietly falsifying
the project's own "3.11+" claim everywhere it's written (a distinct,
smaller version of the same "stated vs. actual" gap this migration was
fixing in the first place). Verified directly (not assumed): the
installed `pandas-ta-classic` 0.6.52's `ta.adx()` emits
`['ADX_14', 'DMP_14', 'DMN_14']` — no `ADXR` column at all, meaning the
RISK-5 collision below cannot occur in this fork; `ta.bbands()` emits the
same column ordering the existing wrapper already assumes.

### What to watch for

`pyproject.toml`/`environment.yml` and `poetry.lock` (new in v1.10.0)
now all reference `pandas-ta-classic`, not `pandas-ta`. Do not reintroduce
`pandas-ta` as a dependency without re-verifying its PyPI state first —
this exact failure mode (declared floor version silently vanishes from
the index) has now happened once for this project and should not be
assumed impossible for any other pinned dependency either.

---

## RISK-8 (NEW): `atomic_write_parquet` never imported in `fundamental_processor.py` — RESOLVED (fixed, FIX FP-AIO-001)

**Status:** ✅ **FIXED.** Discovered, like RISK-5 before it, as a side
effect of writing the first-ever real-data (non-empty) test for a
function that had previously only ever been tested against the
graceful-no-data path.

**GD Reference:** Supplementary Design G2 (atomic write pattern
requirement); GD §3.1 (Bronze immutability — the pattern this guards).

### What the risk was

`src/silver/fundamental_processor.py` calls `atomic_write_parquet()` at
two sites (`process_earnings()`, `process_quotes()`) — the standard
tempfile+`os.replace()` atomic-write pattern used throughout Silver/Gold
— but **never imports it**. Every other module using this function
(`technical_signals.py`, `mtf_alignment.py`, `correlation_matrix.py`,
`screener.py`, `sector_rotation.py`, `macro_regime.py`,
`schema_validator.py`, `forex_cache.py`, `base_ingester.py`,
`backtest/engine.py`) correctly imports it from `src.utils.atomic_io`;
this file simply omitted the import. Every real (non-empty) invocation of
either method raised `NameError` — confirmed via direct reproduction
before writing the fix.

### Why this was invisible

Both methods already had test coverage — `test_process_earnings_graceful_no_bronze`
and `test_process_quotes_graceful_no_bronze` — but both exercise only the
"no Bronze data found" path, which returns early *before* reaching the
`atomic_write_parquet()` call. Neither method had ever been tested with
real, non-empty data, in this project's history. Identical root-cause
shape to RISK-5 (`add_adx()`): a real invocation path with test coverage
that never actually reaches the code doing the real work.

### Fix

`from src.utils.atomic_io import atomic_write_parquet` added to the
module's import block.

### Verification

Two new tests — `test_process_quotes_reads_day_high_day_low` and
`test_process_earnings_writes_real_data` — are the first-ever tests for
either method against real, non-empty Bronze fixture data, closing the
exact test gap that let this hide. Full suite: 1188 passed, 0 failed.

---

`active_symbols.py` (2 call sites), `context_anchors.py` (1 call site —
MOVED GMI-CTX-001 from active_symbols.py's former `resolve_context()`,
which was one of the original 3; the extraction redistributed this note's
count, it did not create or remove a call site), and
`global_rates_processor.py` (1 call site) implement the atomic
tempfile+`os.replace` write pattern **by hand**, duplicating (correctly —
verified atomic) rather than calling the shared `atomic_write_parquet()`
utility in `atomic_io.py`. Still 4 sites total. Not a correctness bug —
each site was individually checked and is genuinely POSIX-atomic — but a
future change to the shared utility (e.g. a bug fix or added logging)
would need manual replication across these 4 sites to stay in sync. Worth
consolidating during a future Silver-layer pass; not urgent enough to
touch now.

---

## RISK-9 (NEW): Repo had no `.gitignore` — runtime artifacts and a binary pickle were tracked in git — RESOLVED (fixed)

**Status:** ✅ **FIXED.** Discovered during a routine pre-implementation
audit of the live repo (fresh clone from `github.com/Ovi-xyz/alpha-factory`),
not while working on any specific ADR — general security hygiene check.

**GD Reference:** None directly — this is repo hygiene, not a pipeline
architecture concern. Closest precedent: GD §3.1 immutability (Bronze
append-only) is about pipeline data lineage, not source-control hygiene,
but the same "don't let mutable runtime state masquerade as a versioned
artifact" instinct applies.

### What the risk was

The repository had **no `.gitignore` at any point in its 3-commit
history** (confirmed via `git log --all --name-only --diff-filter=A`).
Two categories of file were tracked as a result:

1. Five `.DS_Store` files (macOS Finder metadata) — cosmetic, no security
   implication, just noise.
2. Three genuine runtime artifacts under `data/health/`:
   `hmm_regime_model.pkl` (a pickled `sklearn` `StandardScaler` +
   `GaussianHMM`), `pipeline_runs.db` and `progress.db` (SQLite databases
   written by `PipelineLogger`/`ProgressCheckpoint` on every pipeline
   run).

An empirical secret scan (`git log -p --all -- .env`, plus a regex sweep
for `api_key=`/`secret=`/`password=`/`token=` patterns across every
tracked file in every commit) found **zero credentials or secrets** —
this is not a leaked-secret incident. The concrete risk is narrower but
still real: a binary pickle in version control is a standing
deserialization risk (`pickle.load()`/`joblib.load()` can execute
arbitrary code if the file is ever swapped or tampered with — it doesn't
get the same line-by-line code review every `.py` change does), and it
was already observably drifting: loading the committed
`hmm_regime_model.pkl` under this session's `scikit-learn` version
produced `InconsistentVersionWarning: Trying to unpickle estimator
StandardScaler from version 1.8.0 when using version 1.9.0`. The two
SQLite files are pure runtime state — every local `pytest` run mutates
them, so the "correct" committed content is not even a well-defined
concept.

### Fix

- `.gitignore` added (repo's first) — covers secrets (`.env` + variants,
  `*.pem`/`*.key`), Python/Poetry caches, `data/` (all pipeline runtime
  output — GD §7 estimates ~73-117 GB total, far too large for version
  control regardless of the tracking question, and it's regenerable by
  design), and OS/editor metadata.
- The 5 `.DS_Store` files and 3 `data/health/*` artifacts were removed
  from tracking via `git rm --cached` (files remain on disk locally —
  this is untrack-going-forward, not deletion).
- **Deliberately NOT a history rewrite** (`git filter-repo` / `BFG` /
  force-push): rewriting history is the correct remedy when a genuine
  secret was committed, since removal-only leaves it recoverable from any
  clone's reflog/object store. Here, the empirical scan confirmed no
  secret was ever present, so a rewrite would only add risk (force-push
  disruption, invalidated collaborator clones) for zero actual benefit.

### Verification

`git check-ignore -q <path>` exit-code-verified for both directions post-fix:
new files under `data/` are now correctly ignored (exit 0), while
`poetry.lock`, `.env.example`, and every tracked config/source file
remain un-ignored (exit 1). Full suite re-run from a state with the
runtime artifacts deleted entirely (simulating a fresh clone): 1188
passed, 0 failed — confirms nothing in the pipeline actually depends on
these files pre-existing (each module creates its own directory tree via
`mkdir(parents=True, exist_ok=True)` on first write).

---

## RISK-10 (NEW): Architecture v2.1 Addendum's own commodity taxonomy tables disagree with each other — RESOLVED (fixed)

**Status:** ✅ **FIXED.** Found empirically while implementing Decision B
Step 1 (GMI_Decision_Document_v3.docx) — specifically, by a new test
(`test_no_orphaned_commodity_subcategory`) that failed with a `KeyError`
against the very weight matrix being built from the same source
document.

**GD Reference:** Architecture v2.1 Addendum §7.1 (commodity_subcategory
enum definition) and §8.2 (REGIME_SECTOR_WEIGHTS key-name table) — both
in the same document, describing the same 5-way taxonomy, disagreeing
with each other.

### What the risk was

Addendum §7.1 defines `commodity_subcategory`'s valid enum values as:
`energy`, `precious_metals`, `base_metals`, `agricultural`, `bulks`.
Addendum §8.2's own REGIME_SECTOR_WEIGHTS key-rename table lists the
five replacement keys as: `commodity_energy`, `commodity_precious`,
`commodity_base_metals`, `commodity_agricultural`, `commodity_bulks`.
Four of five keys are the mechanical `f"commodity_{subcategory}"` formula
applied literally; the fifth (`commodity_precious`) is not —
`precious_metals` mechanically produces `commodity_precious_metals`, not
`commodity_precious`. This is an internal inconsistency within a single
document, not a discrepancy between two different documents in the
authority hierarchy.

Left unfixed, this would have caused a silent, not a loud, failure:
`sector_rotation.run()`'s lookup is `weights.get(key, 1.0)` — a missing
key falls back to a neutral 1.0 weight rather than raising. AU and AG
(the two `precious_metals` Layer 1 instruments) would have silently
received neutral weighting in every regime, including RISK_OFF where the
correct weight (1.4, precious-metals-as-safe-haven overweight) is one of
the more consequential values in the entire matrix.

### Fix

Resolved in favor of the mechanical formula: `commodity_precious` renamed
to `commodity_precious_metals` throughout `REGIME_SECTOR_WEIGHTS` (all 5
regimes). This was chosen over the alternative (change §7.1's enum value
instead) because 4 of 5 keys already follow the formula exactly — making
the formula the internally-consistent choice — and because the enum value
(`commodity_subcategory` in `instruments.yaml`) is the more
consumer-facing contract of the two, better left stable.

### Verification

New permanent regression guard —
`test_subcategory_to_weight_key_map_matches_sector_rotation_keys`
(`test_validate_instruments.py`) — independently cross-checks
`validate_instruments.py`'s own `COMMODITY_SUBCATEGORY_TO_WEIGHT_KEY` map
against `sector_rotation.py`'s live `REGIME_SECTOR_WEIGHTS` keys across
all 5 regimes, specifically designed to catch this exact class of
cross-module naming drift again if it recurs. Full suite: 1204 passed, 0
failed.

---

## RISK-11 (NEW): Two migration scripts executed a destructive `config/instruments.yaml` write at *import time*, with zero guard — RESOLVED (archived, then fully removed 7 Aug 2026)

**Status:** ✅ **FIXED.** Discovered while assessing
`scripts/migrate_instruments.py` and `scripts/build_instruments_v14.py` for
archival per `GMI_Decision_Document_v3.docx` Priority 3 / Checkpoint v6 §8
item 3 — not from re-reading either script's docstring, which never
mentioned the actual severity.

**GD Reference:** GD §17.7 (anti-patterns) in spirit — not a Bronze/Silver/
Gold layer violation, but the same "silent corruption instead of a loud
failure" failure mode the anti-pattern list exists to prevent.

### What the risk was

Neither script had an `if __name__ == "__main__":` guard. Both executed
their write to `config/instruments.yaml` as **top-level module code** —
meaning a bare `import scripts.migrate_instruments` (e.g. from a stray
script, a misconfigured test auto-collector, or an IDE's "organize
imports") would silently trigger the full destructive rewrite, with no
`python scripts/...` invocation ever needed.

- `migrate_instruments.py` read the *original* Grand Design v1.2 flat
  structure (`src/config/instruments_raw.py` — 643 instruments, 4 markets,
  no Layer 2) and would overwrite the current v1.5 hierarchical
  `instruments.yaml` (699 instruments, `context.*`, domain-score
  `_meta.contributes_to` routing) with that 4-version-old shape.
- `build_instruments_v14.py` was worse: `SRC` and `DST` were **the same
  path** (`config/instruments.yaml`). It was a one-time v1.2→v1.4 in-place
  transform; instruments.yaml is now at v1.5. Re-running it would read the
  *current* file and silently discard everything added since v1.4 —
  `commodity_role`/`commodity_subcategory`, the 5 disaggregated
  `REGIME_SECTOR_WEIGHTS` keys' upstream config, and 79+ hand-written
  ADR-rationale comments (Checkpoint v5 §5.1) — with no backup step and no
  external input to diff against.

Blast radius if triggered: total loss of the hand-maintained instrument
universe config, silently, with the only symptom being `validate_instruments.py`
(Gate G-3) failing on the *next* CI run — by which point `git blame` on
`instruments.yaml` would point at whatever unrelated commit happened to
run first, not the actual cause.

### Fix

- Both scripts moved to `scripts/archive/` (`git mv`, history preserved).
- A hard, unconditional `raise SystemExit(...)` inserted as the first
  executable statement of each file — before `sys.path.insert`, before any
  import — so the guard fires on **both** direct execution and plain
  `import`, closing the actual hole (not just the `python script.py` path).
- `src/config/instruments_raw.py` (the pure-data file `migrate_instruments.py`
  read from — zero functions/classes, 700 lines, and after this fix its
  *only* remaining reference in the entire codebase) relocated alongside
  it to `scripts/archive/instruments_raw.py`. This also removes it from
  `[tool.coverage.run] source = ["src"]` scope, so it stops permanently
  diluting the `src/` coverage metric for code that will never run again —
  no `omit` config entry needed.
- `Makefile`'s `migrate` target kept (not deleted) but now fails loudly
  with a pointer to `scripts/archive/README.md`, so `make migrate` from
  muscle memory gets a clear explanation instead of either "No rule to
  make target" or silent data loss.
- `README.md` project-structure tree and `scripts/validate_instruments.py`'s
  header comment updated to stop pointing at the now-archived scripts.
- `scripts/archive/README.md` (new) documents the full root cause and
  explicitly states what a genuine future rebuild would require — a fresh
  migration against the *current* schema with a diff-and-sign-off step,
  not resurrecting either archived script.

### Verification

`tests/unit/test_archived_migration_scripts.py` (new, 11 tests) — a
permanent regression guard run in an isolated subprocess (not the test
runner's own process, since the original bug was specifically an
import-time side effect a same-process `import` wouldn't faithfully
reproduce): confirms the guard fires on direct execution AND bare import
for both scripts, confirms `config/instruments.yaml`'s content and mtime
are byte-identical before/after both invocation paths, and confirms
`make migrate` exits non-zero with an explanation. Full suite: 1300
passed, 0 failed.

### Update — 7 Aug 2026: archive removed entirely, regression guard retired

Ovi deleted `scripts/archive/` outright (all 9 files, ~3,309 lines,
including this fix's own `README.md` and both guarded scripts) as
further cleanup, roughly a week after the archival above and the
separate tvdatafeed retirement (RISK-1) it was bundled alongside in
the same commit. Broke 7 of the 11 tests in
`tests/unit/test_archived_migration_scripts.py` — all of them checks
that the archive *existed* (README present, scripts still
syntactically parseable, `import scripts.archive.X` failing with the
specific "ARCHIVED" guard message rather than a plain
`ModuleNotFoundError`), which is no longer true by construction.

**Not a regression.** The bug this test file guarded against —
destructive import-time writes from `migrate_instruments.py` /
`build_instruments_v14.py` — is now structurally impossible, not just
disabled: the files don't exist anywhere in the repo, archived or
otherwise, so there's nothing left that could be dangerously imported.
Coverage itself was unaffected (`scripts/archive/` was never in
`[tool.coverage.run] source = ["src"]` scope, per the Fix section
above) — the 7 failures were pure test breakage from testing a
precondition Ovi had just deliberately removed, not a drop below the
80% gate.

Retired the 10 tests whose entire premise was the archive's existence.
Preserved the one still-genuinely-relevant check —
`TestMakefileMigrateTargetFailsLoudly` (the `make migrate` target is
still kept in the Makefile as a muscle-memory safety net and still
needs to fail loudly) — by moving it to a new, more accurately named
file: `tests/unit/test_makefile_safety_nets.py`. Full suite re-verified:
**1422 passed, 0 failed** (1432 -> 1422, net -10 matches 11 removed + 1
preserved). Coverage 81.43%, unchanged. `tests/COUNT_BASELINE.txt`
updated to 1422. Full detail:
`dev-log/2026-08-07-scripts-archive-removed-test-suite-repair.md`.

---

## RISK-12 (NEW): `gold_screener`'s regime join silently zeroed the entire watchlist whenever regime data was momentarily unavailable — RESOLVED (fixed)

**Status:** ✅ **FIXED.** Discovered empirically while writing the first
real-function coverage for `build_watchlist()` (previously
`tests/unit/test_screener_gld005.py` covered only `_check_data_freshness()`
— `build_watchlist()` itself, the actual multi-source join, had zero
tests).

**GD Reference:** GD §5.2.4 (Screener & Watchlist), §0.4 Interface
Contract (`watchlist_{date}.parquet` is a promised daily Gold output).

### What the risk was

`build_watchlist()`'s main query broadcast the single active-regime row
onto every MTF candidate via `CROSS JOIN (SELECT * FROM regime_tbl LIMIT
1) r`. When `regime_store.parquet` doesn't exist yet, or exists but has
no row for the exact `run_date` (a `--force` run ahead of `gold_regime`,
or a backfill date regime detection never covered), `regime_tbl` is a
legitimately **zero-row** placeholder — and a `CROSS JOIN` (Cartesian
product) against an empty relation is empty, by definition, regardless of
how many rows are on the other side. This silently discarded the entire
watchlist even when MTF/sector/active data were all perfectly healthy.
Reproduced with a standalone DuckDB query before touching the source: 0
rows out with `CROSS JOIN`, 2/2 preserved (with `r.*` correctly `NULL`)
after switching to `LEFT JOIN (SELECT * FROM regime_tbl LIMIT 1) r ON
TRUE` — the same graceful-degrade contract `sector_tbl`/`active_tbl`
already get via `LEFT JOIN` + `COALESCE` a few lines above it in the same
query.

### Why this was invisible

In the normal dependency-guarded daily sequence, `gold_regime` always
precedes `gold_screener` and (in the common case) writes a row for
today before the screener runs, so the bug wouldn't trigger under
routine operation — only under `--force`, backfill, or a regime-detection
gap for that specific date. No existing test called `build_watchlist()`
at all, so this went unnoticed regardless.

### Fix

`CROSS JOIN (SELECT * FROM regime_tbl LIMIT 1) r` → `LEFT JOIN (SELECT *
FROM regime_tbl LIMIT 1) r ON TRUE`. Regime columns (`regime`,
`regime_composite`, `regime_confidence`, `regime_transition`,
`transition_alert`) are now `NULL` — not row-eliminating — when regime
data is unavailable, consistent with the project's "data field, not a
decision" philosophy (GD §0.3): the screener reports what it knows,
Trading Engine interprets absence.

### Verification

`tests/unit/test_screener.py::TestBuildWatchlistRegimeJoinRegression` (6
tests) is the permanent regression guard: missing file, file-exists-but-
no-matching-row, corrupt file, and the correct-broadcast happy path are
all covered. Full suite: 1385 passed, 0 failed, `screener.py` coverage
31% → 100%.

---

## RISK-13 (NEW): correlation-cluster deduplication in `gold_screener` has never actually executed — RESOLVED (fixed)

**Status:** ✅ **FIXED.** Discovered empirically alongside RISK-12, while
building a real `correlation_clusters.parquet` fixture for
`_deduplicate_by_cluster()` — also previously untested.

**GD Reference:** GD §15.1 (Correlation Concentration Guard — "Max 2
posisi per correlation cluster (enforced di screener SQL)").

### What the risk was

`_deduplicate_by_cluster()` computed `pl.int_ranges(pl.len()).over
("cluster_id")` intending a per-row "rank within cluster" — but
`int_ranges` (plural) is polars' **list-producing** primitive: it
broadcasts a single `List[Int64]` value (e.g. `[0, 1, 2]`) identically to
every row in a group, it does not number rows individually. The
subsequent `.filter(pl.col("cluster_rank") < MAX_PER_CLUSTER)` then
raised `SchemaError: could not evaluate '<' comparison ... List(Int64)`
on every call where correlation data actually existed — caught by this
same function's own `except Exception: logger.debug(...)`, which simply
returned the **unmodified** input DataFrame. Net effect: the
Correlation Concentration Guard has silently never fired for any real
correlation input since it was written; screener output could legally
contain more than `MAX_PER_CLUSTER` (2) highly-correlated symbols with no
error, warning, or visible signal that dedup wasn't happening.

### Why this was invisible

The broad `except Exception` around the whole function body is
appropriate in principle (a missing/corrupt correlation file shouldn't
break the screener) — but it also swallowed a genuine logic bug at
`logger.debug` level, below the visibility threshold anyone would notice
in normal operation. No test exercised this function with real, non-empty
correlation data before now.

### Fix

`pl.int_ranges(pl.len())` → `pl.int_range(pl.len())` (singular). Verified
empirically before the source change: the singular form produces a
proper per-row `Int64` position-within-group (`0, 1, 2, ...`), and the
existing DataFrame row order — which is already `ORDER BY
ABS(mtf_score) DESC, ...` from the caller — means rank 0 within a cluster
is always the highest-priority candidate, so the correct member(s) are
kept.

### Verification

`tests/unit/test_screener.py::TestClusterDeduplication` (3 tests) is the
permanent regression guard, including a direct call proving the previous
`SchemaError` no longer occurs and that the lowest-priority member of an
over-represented cluster is the one dropped. Full suite: 1385 passed, 0
failed.

---

## RISK-14 (NEW): EIA incremental-fetch cache scanned a path that never matched real written data — RESOLVED (fixed)

**Status:** ✅ **FIXED.** Discovered empirically while writing real-function
coverage for `bronze/eia_ingester.py` (Decision C tranche item #6,
previously zero coverage).

**GD Reference:** Supplementary Design v1.1 G1 (`IncFetchProtocol` —
the general incremental-fetch pattern this cache is meant to mirror for
EIA specifically, per its own `FIX EIA-2`/`FIX EIA-4` comments).

### What the risk was

`_build_last_known_cache()` scanned a hardcoded literal
`"data/bronze/commodity/eia/**/*.parquet"` — independent of
`self.BASE_PATH` entirely, and pointed at `commodity/eia/`. The
ingester's own `write_macro(source="eia", domain="crude_oil")` call
actually writes to `BASE_PATH/macro/eia/crude_oil/` (per
`BronzeIngester.write_macro()`'s `path = self.BASE_PATH / "macro" /
source / domain`). The scan pattern therefore never matched any file
this ingester ever wrote. `FIX EIA-4` (an earlier, in-code-documented fix)
correctly repaired how the cache is *read* — it was being queried with
`spec['name']` instead of the cache's actual `spec['id']` keys — but the
cache was never populated in the first place, so that fix alone couldn't
have restored the intended behavior. Net effect: `EIAIngester.run()`
silently used the full 5-year lookback window on every single invocation,
never the intended 14-day incremental buffer — not a crash, not a
visible error, just permanently degraded to the slow path.

### Why this was invisible

The scan is wrapped in a broad `except Exception: pass`, and an empty
result set from a non-matching glob doesn't raise — it just yields `{}`,
identical in behavior to "no prior EIA data exists yet." Both look the
same from the outside (a full lookback fetch), so nothing about normal
operation would surface the difference. No test called
`_build_last_known_cache()` with a real, correctly-placed bronze fixture
before now.

### Fix

`pattern = "data/bronze/commodity/eia/**/*.parquet"` → `pattern =
str(self.BASE_PATH / "macro" / "eia" / "crude_oil" / "**" / "*.parquet")`
— now BASE_PATH-relative (testable, deployment-correct) and pointed at
the domain `write_macro()` actually uses.

### Verification

`tests/unit/test_eia_ingester.py::TestBuildLastKnownCache` and
`TestIncrementalFetchWindow` are the permanent regression guards — both
failed against the pre-fix code (empty cache, cache lookup `KeyError`,
incremental window falling back to the 5-year default) and pass against
the fix. Full suite: 16/16 in this file, no regressions elsewhere.

---

## RISK-15 (RESOLVED): ADR-005/006's FRED Track 2 monthly supplements (`PIORECRORECUSDM`, `PCOALAUUSDM`) were never actually added to `config/fred_series.yaml`

**Status:** ✅ **RESOLVED (8 Aug 2026).** Closed in a dedicated thread,
answering all 3 questions this entry's own "Suggested next step" left
open, and surfacing one previously-unknown bug along the way (see
"What was found while closing it").

**GD Reference:** Architecture Extension v1.0 ADR-005 (Iron Ore — VALE
proxy + FRED `PIORECRORECUSDM` monthly supplement), ADR-006 (Newcastle
Coal — WHC.AX proxy + FRED `PCOALAUUSDM` monthly supplement). Both
describe a "Track 2" two-track design: a daily equity proxy (implemented,
live) plus a monthly official FRED series as a lower-frequency supplement
for `ForecastModule`.

### What the risk was

`config/fred_series.yaml` (60-series IDD §5 registry: monetary_policy,
inflation, growth, labor, credit, housing, volatility domains) had **no
`commodity` domain at all**. Neither `PIORECRORECUSDM` nor `PCOALAUUSDM`
— both explicitly specified in ADR-005/006's own "Consequences" sections
— existed anywhere in it, despite Track 1 (the equity proxies,
VALE/WHC.AX) being fully live.

### What was found while closing it

Web-verifying all 6 candidate series against `fred.stlouisfed.org`
directly (this thread, since no sandbox on this project has a route to
`api.stlouisfed.org` either) surfaced a **second, previously-unknown
bug**: Iron Ore's real FRED series ID is `PIORECRUSDM` — **not**
`PIORECRORECUSDM`, the ID this entry itself (and Architecture Extension
v1.0 ADR-005) had been citing. `PIORECRORECUSDM` does not exist on FRED
— a duplicated "ORE" typo (`PIORE-CR-ORE-C-USDM` vs the real
`PIORE-CR-USDM`) carried, unverified, across two design documents and
this risk entry itself, until now. The other 5 series (`PCOALAUUSDM`,
`PPOILUSDM`, `PRUBBUSDM`, `PTINUSDM`, `PNICKUSDM`) were all confirmed
correct as documented.

The 3 open questions from this entry's original "Why not fixed then"
(below, preserved for audit trail) were answered by direct read of the
live source this thread, not assumption: `src/bronze/fred_ingester.py`
is fully domain-agnostic — `domain` is only used to build the Bronze
output path — so **no ingester code change was needed**;
`config/schemas/fred_macro.yaml` (the SchemaValidator gate) is equally
generic (4 columns, no per-domain branching); `src/silver/
macro_processor.py` globs `data/bronze/macro/fred/**/*.parquet` as a
single pass regardless of per-series domain, so it required no change
either. This closes as a config-only addition.

### Why not fixed then (preserved for audit trail)

ADR-030–033 (30 Jul 2026 thread) introduced 4 new candidate FRED series
of the same Track 2 shape — `PPOILUSDM` (palm oil), `PRUBBUSDM` (rubber),
`PTINUSDM` (tin), `PNICKUSDM` (nickel), matching the same World Bank Pink
Sheets naming convention as the two above. These were deliberately
**NOT** added to `fred_series.yaml` in that pass either: (1) fixing the
pre-existing IRON_ORE/COAL_NEWC gap was out of scope for a
tvdatafeed-retirement thread; (2) whether `fred_ingester.py` even has
domain-parsing logic for a `commodity` domain was not verified that
thread; (3) none of the 6 series had been empirically confirmed against
the live FRED API from any sandbox to date.

### Fix

`config/fred_series.yaml` — new `commodity` domain section, all 6 series
(`PIORECRUSDM` corrected, `PCOALAUUSDM`, `PPOILUSDM`, `PRUBBUSDM`,
`PTINUSDM`, `PNICKUSDM`), `regime_input: false` on all six (no
macro-regime consumer; Track 2 is a future `ForecastModule` input — GMI
Wave 1 Cycle 4 / CrossAssetEngine not yet built). Stale header comment
("60 series") corrected to 67 in the same pass — already off by one
(VIXCLS) before this change.

`src/bronze/fred_ingester.py` — `RELEASE_LAG_DAYS` entries added for all
6 new series (25 days each), matching the existing PPI-series precedent
(`PPIFIS`/`PPIFGS`/`PPIACO`, also 25) and rounding up from the ~24-day
publication lag directly observed on each series' `fred.stlouisfed.org`
page this thread (Feb-2026 period data consistently showed "Updated: Mar
24, 2026" across all 6).

`scripts/preflight/check_fred_commodity_series.py` — new preflight
script (none of the 4 existing scripts in that directory covered FRED),
mirroring `check_bis_cbpol_d.py`'s pattern: authored and offline-tested
this thread (same network constraint as every other preflight script in
this project — execution against the live API deferred to
network-enabled hardware). Duplicates `EXPECTED_COMMODITY_SERIES`
independently of `fred_series.yaml` rather than importing it, matching
`check_bis_cbpol_d.py`'s own `EXPECTED_REF_AREAS` independence rationale.

### Verified empirically

All 6 series IDs, including the corrected `PIORECRUSDM`, confirmed live
and monthly via direct web check of their `fred.stlouisfed.org` pages.
`config/fred_series.yaml` re-parses cleanly (67 total series, 0
duplicate IDs). `src/bronze/fred_ingester.py` and
`scripts/preflight/check_fred_commodity_series.py` both pass
`ast.parse()`. 10 new tests (`TestCheckFredCommoditySeries`,
`tests/unit/test_preflight_scripts.py`) pass in isolation in a sandbox
and collect cleanly alongside the existing 4 preflight-script test
classes (42 total collected, 0 collection errors — the exact failure
mode this project tracks separately from pass/fail, per the CI/CD Ops
Guide's own G-4 rationale). Every file write closed-loop verified:
re-fetched from the live repo after each edit and diffed against the
sandbox-validated version before moving on.

**Live-confirmed (9 Aug 2026):** Ovi ran `check_fred_commodity_series.py`
on the M1 with a real `FRED_API_KEY` — all 6 series PASS, all with
`latest=2026-06-01` and 12/12 usable observations in the last 12
requested (`PIORECRUSDM`, `PCOALAUUSDM`, `PPOILUSDM`, `PRUBBUSDM`,
`PTINUSDM`, `PNICKUSDM`). This closes the live-FRED half of this entry's
own "Not yet run" note.

**Still not run:** a full `poetry run pytest` on real hardware confirming
1432/1432 for real — not part of the 9 Aug 2026 preflight run, and not
otherwise confirmed to date. Nothing above depends on it to be correctly
specified and offline-verified; this is the same authoring/execution
split every other preflight script in this project already carries.

**Test baseline:** `tests/COUNT_BASELINE.txt` 1422 → 1432 (+10).

---

## RISK-16 (NEW): BIS CBPOL/EER endpoints used the wrong dataflow ID, not just the wrong URL structure — RESOLVED (confirmed live)

**Status:** ✅ **RESOLVED — confirmed live on the M1** (all 4 preflight
modules run, logs reviewed this thread). Fixed via FIX BIS-1 (1 Aug
2026), superseding the 28 July "v1->v2 path structure" fix, which was
necessary but not sufficient — confirmed by the 29 July preflight log
still showing 404/501 after that fix landed. The corrected endpoints
were run for real immediately after the code fix and returned genuine
data, closing the "pending live confirmation" gap this entry originally
carried.

**GD Reference:** Data Source & Rates Adjustment v1.0 §3.2 (BIS API
specification), ADR-010/011/012 (CB rate coverage), ADR-017/018 (Broad
Dollar basket weights, blocked on this).

### What the risk was

Three threads (GMI v6, the 28 Jul preflight-fixes thread, and the 29 Jul
live preflight run) all treated the BIS 404/501s as a URL *path
structure* problem (v1 → v2) and fixed only that. The 28 Jul thread went
further and explicitly claimed the dataflow IDs themselves (`WS_CBPOL_D`,
`WS_EER_M`) were independently confirmed correct via "a BIS SDMX Python
client's dataflow listing" — a claim that was never actually verified
against live BIS and turned out to be false, as the 29 Jul live run (with
the v2 path fix already applied) still 404'd on both.

Root cause, found via web research (BIS has no route from any sandbox on
this project, so this was confirmed via `data.bis.org`'s own publicly
indexed pages and independent third-party working code examples, not a
live API call — the live API call came *after* the fix, see "Live
confirmation" below): the dataflow IDs are `WS_CBPOL` and `WS_EER` — not
`WS_CBPOL_D` / `WS_EER_M`. The "_D"/"_M" suffixes were daily/monthly
cadence labels mistaken for part of the dataflow identifier, most likely
traceable to a v1-era academic example (fgeerolf.com) that already used a
guessed flow name before this project's v1→v2 migration. Frequency is a
KEY dimension (`FREQ.REF_AREA` for CBPOL, `FREQ.TYPE.BASKET.REF_AREA` for
EER), not part of the flow name. A second, independent error in the EER
`--discover` endpoint: it was missing the `structure/` path segment
entirely (`/api/v2/dataflow/...` instead of `/api/v2/structure/
dataflow/...`), which is consistent with the 501 it was actually
returning (a malformed/unrecognized v2 path) rather than the clean 404 a
bad key alone would produce.

### Evidence trail (pre-fix research)

- `data.bis.org` central-bank-policy-rate pages for 8 countries
  (AR/BR/GB/CH/DK/NO/JP/CL), all served under
  `topics/CBPOL/BIS,WS_CBPOL,1.0/{FREQ}.{REF_AREA}` — no `_D` anywhere.
- `data.bis.org` effective-exchange-rate pages for 7 countries
  (US/AE/CN/KR/XM/JP), all served under
  `topics/EER/BIS,WS_EER,1.0/{FREQ}.{TYPE}.{BASKET}.{REF_AREA}` — no `_M`
  anywhere; both Real and Nominal baskets observed.
- A live, working third-party code example (jamelsaadaoui.com/EconMacro
  blog, comments dated Aug 2024, site posting through Jul 2026) for the
  sibling dataflow `WS_CBTA`, using the identical
  `/api/v2/data/dataflow/BIS/<FLOW>/1.0/<key>?format=csv` shape.
- A real SDMX 2025 conference paper (sdmx2025.org) with worked examples
  for a third sibling dataflow (`WS_XRU`), independently confirming BOTH
  the data-query shape (`/api/v2/data/dataflow/...`) AND the
  structure/discovery shape (`/api/v2/structure/dataflow/...?references=all`).

### Live confirmation (Ovi, M1, this thread — 4 preflight modules run, re-run 3 Aug against the 13-currency expansion)

All four current preflight scripts were run for real against the
corrected endpoints, immediately closing the gap this entry originally
flagged as open — and re-run again after the HKD/TWD/NOK expansion:

- **`check_bis_cbpol_d.py`** — **all 12 REF_AREA codes PASS with
  `daily-resolution=True`**, real observation counts (6,775–24,850 per
  country) and current dates (latest = 2026-07-01 through 2026-07-29
  depending on country). Re-run 3 Aug: identical result, confirming
  stability. This **fully closes** the "unplanned finding" flagged in the
  original fix more favorably than the pre-fix web research suggested:
  the 4 central banks sampled from `data.bis.org`'s portal (GB/CH/NO/JP)
  appeared to be Monthly-only from that sampling, but the real API query
  — with FREQ wildcarded, per the fix's own design — returns Daily data
  for all 12, ECB/XM included, on both runs. **Answering the specific
  question this entry originally deferred: no, the Monthly-vs-Daily
  finding does not affect ADR-010 for any of the 12 CBs** — ADR-010's
  original "BIS is daily where FRED is monthly" rationale is empirically
  confirmed correct for the full set, not just asserted.
- **`check_bis_eer_weights.py --discover`** — succeeded both runs,
  fetching 568,951 real bytes of dataflow structure from the corrected
  `structure/dataflow/BIS/WS_EER/1.0` endpoint (was a 501 pre-fix).
- **`check_bis_eer_weights.py`** — first run (10 currencies, pre-HKD/TWD/
  NOK): all 10 PASS, 182,410 bytes per check. **Re-run 3 Aug against the
  full 13-currency expansion: all 13 PASS**, 237,188 bytes per check (the
  larger response reflecting the wider currency key) — this closes the
  "not yet re-run live against the 13-currency version" gap the previous
  update to this entry left open.

This confirms the endpoint/key construction is genuinely correct, not
just plausible — the CBPOL script in particular had to correctly *parse*
real response data to report per-country observation counts and dates,
which a merely-reachable-but-malformed response could not have produced.

### HKD/TWD/NOK added (Ovi, 1 Aug thread, following up) — now live-confirmed

Ovi pointed out `BROAD_DOLLAR_REF_AREAS` in `check_bis_eer_weights.py`
was still missing 3 currencies — a gap the 28 Jul thread had explicitly
flagged rather than guessed at ("Ovi's instruction was specifically
MXN->IDR"). Added: HKD→HK, TWD→TW, NOK→NO, completing all 13
currencies of the *current* Broad Dollar basket design
(`instruments_taxonomy.yaml`'s `dollar` + `dollar_basket` groups). While
fixing this, found and corrected a second, structural issue: the
endpoint's key was a hand-duplicated literal string separate from the
`BROAD_DOLLAR_REF_AREAS` dict — adding entries to the dict alone would
have left them permanently unfetched while `_check_one()` kept
confidently reporting "not present," indistinguishable from a genuine API
failure. The endpoint key is now built FROM `BROAD_DOLLAR_REF_AREAS
.values()` (`"+".join(...)`), making this whole bug class structurally
impossible going forward. **Confirmed live 3 Aug** (see above) — all 13
currencies PASS against the real API.

### TYPE decision (Ovi, 3 Aug 2026 thread) — Nominal, not Real

Previously left deliberately wildcarded pending a decision. Now decided:
**Nominal**. Two independent reasons converged: (1) DXY itself — the
index this platform's Broad Dollar Index is explicitly designed as a
companion/extension of (Architecture v2.0 §7.2) — is a nominal
currency-value index, not inflation-adjusted; comparing Real EER against
a Nominal DXY under one "Dollar strength" umbrella would conflate two
different concepts. (2) BIS's own EER overview page
(`data.bis.org/topics/EER`) states Daily-frequency EER data exists ONLY
for Nominal indices, never Real ("the latter available only as nominal
indices") — since this platform's Layer 2 anchors are specified at Daily
cadence (Architecture v2.0 §7.2), Nominal is the only choice that can
actually deliver that. Endpoint key's TYPE segment fixed to `N`; FREQ
(previously fixed to `M`) is now wildcarded instead, mirroring the same
reasoning already applied to `check_bis_cbpol_d.py` — request whatever
frequency BIS actually has rather than assume, so genuinely-available
daily data comes through without risking a false failure on currencies
that may only have monthly EER. Constant renamed
`BIS_EER_ENDPOINT_MONTHLY` → `BIS_EER_ENDPOINT` accordingly (no longer
accurately "monthly-only"). **Live-re-confirmed 4 Aug 2026** — Ovi
re-ran `check_bis_eer_weights.py` against the current (`.N.B.`,
wildcarded FREQ) code: all 13 currencies PASS at 3,813,875 bytes per
currency, ~16x the 237,188 bytes recorded above under the old `M..B.`
key. That jump is exactly what wildcarding FREQ should produce (daily
instead of monthly-only data coming through) and is itself evidence
the new key shape is live and working as designed, not just reachable.
Full detail:
`dev-log/2026-08-04-gate1-live-confirmation-poetry-lock-fix.md`.

### Gate 1 (ADR-017/018 exact Broad Dollar weight components) — substantially advanced, not yet closed

GMI v6 had framed this as possibly unresolvable via any API — "weights
may be a documentation artifact, not necessarily a queryable SDMX
series." This thread found the actual source: BIS's own
`data.bis.org/topics/EER` page (server-rendered, unlike the SPA pages
encountered elsewhere on this project) links directly, under its own
"Methodology" section, to a downloadable weights table:
`https://www.bis.org/statistics/eer/weightsb.xlsx` (Broad, 64 economies
— confirmed the right one, since Narrow only covers 26/27 core economies
and would exclude IDR/HKD/TWD). Confirmed reachable and genuinely an
`.xlsx` (mime type `application/vnd.openxmlformats-officedocument.
spreadsheetml.sheet`, not a redirect or error page) via `web_fetch` this
thread. Also confirmed: weights are **time-varying on a 3-year basis**
(vintages 1993-95 through 2017-19 per BIS's own FAQ; the 2017-19 vintage
has been in continuous use for "the latest period" since, until BIS
publishes the next update) — there is no single permanent "exact
weight," but there is a specific, nameable current vintage.

`check_bis_eer_weights.py` gained a new `--discover-weights` mode:
downloads the file and reports its real sheet names, dimensions, a
structural sample, and a scan for our own currency/REF_AREA codes —
deliberately NOT assuming a row/column layout (openpyxl added as an
explicit direct dependency, promoted from transitive the same way
jsonschema was in Decision B Step 3).
**Run against the real file 4 Aug 2026** — Ovi ran `--discover-weights`
on the M1 (the first sandbox on this project with a route to
`bis.org`): `weightsb.xlsx` downloaded clean (492,941 bytes), 10
sheets (`1993_1995` through `2020_2022`, confirming the stated 3-year
vintage cadence directly — no vintage newer than 2020-22 exists yet).
Every sheet is a symmetric "who weights whom" matrix (row = country,
column = currency being weighted, cell = percent weight); the scan
found all 13 `BROAD_DOLLAR_REF_AREAS` codes present as both row and
column entries, at identical positions, in every one of the 10 sheets.
This is genuine progress (the file is located, confirmed real, AND its
internal layout is now fully characterized against the real data, not
just a synthetic test workbook) but Gate 1 is **still not closed** —
the scan gives coordinates, not values, and doesn't itself locate the
US row (not one of the 13 target codes) whose weights-on-partners are
what the Broad Dollar Index actually needs. Full detail:
`dev-log/2026-08-04-gate1-live-confirmation-poetry-lock-fix.md`.

**Extraction pass authored (9 Aug 2026):**
`extract_us_weights_from_sheet()` (pure, no I/O) plus a new
`--extract-weights` CLI mode added to `check_bis_eer_weights.py` —
locates the US row (column-2 match; not one of the 13 target codes, so
never surfaced by `--discover-weights`'s own scan), re-derives the
header row's column positions on every call rather than trusting the 4
Aug run's "identical across all 10 sheets" finding as a hardcoded
layout, defaults to the most recent vintage (`max(sheetnames)` →
`2020_2022`) with a `--sheet` override, and reports each of the 13
target currencies' weight in the US's own Broad EER basket. 11 new tests
(`TestCheckBisEerWeights::test_extract_*`), all against a synthetic
workbook shaped like the real confirmed layout — same caveat as
`--discover-weights`'s own tests: proves the parsing logic is correct,
not that it matches `bis.org`'s specific real file, which still has
never been reached from any sandbox on this project. **Gate 1 remains
open** — this moves the targeted extraction pass from "unblocked but not
started" to "written and unit-tested," not to "values extracted."
Running `--extract-weights` for real on the M1 is the next step.

### What this does NOT resolve

Gate 1's exact per-currency weight *values* — the file's layout is now
fully known (see above), but the values themselves have not been
extracted into `BIS_WEIGHTS`. The production `bronze_bis_rates`
ingester's own CSV-parsing path (`_parse_csv()`) has not been run
end-to-end against a real BIS response — only the preflight scripts'
lighter-weight parsing has been confirmed live; the two are different
code paths.

### Verification

7 regression-guard tests locking in the corrected dataflow IDs/key
structure and the HKD/TWD/NOK completion across all three touch points
(production ingester, both preflight scripts) —
`tests/unit/test_bis_rates_ingester.py::TestBisEndpoint`,
`tests/unit/test_preflight_scripts.py::TestCheckBisCbpolD::test_endpoint_uses_correct_dataflow_id`
/ `test_endpoint_key_wildcards_freq_and_includes_all_ref_areas`,
`TestCheckBisEerWeights::test_endpoint_uses_correct_dataflow_id` /
`test_structure_endpoint_uses_structure_prefix` /
`test_key_wildcards_freq_and_fixes_broad_basket` /
`test_hkd_twd_nok_completes_dollar_basket`. One pre-existing test
(`test_endpoint_uses_v2_path_structure`) asserted the now-superseded
`WS_EER_M` value and was rewritten, not deleted, with a docstring
explaining why. Full suite: 1427 passed, 0 failed (up from 1420 pre-fix)
— zero regressions in the 24 pre-existing ingester tests or 22
pre-existing preflight-script tests, confirmed by running both files
before AND after the fix. Coverage: 81.43% (was 81.41%). Gates
G-1/G-2/G-3/G-8 all re-run clean. Code changes reproduced twice: once in
an isolated sandbox clone (Python 3.12, fresh `poetry install --with
dev`), once applied to the real repo via the filesystem connector. The
endpoint correctness itself was then independently confirmed a third way
— against the actual live BIS API, on real hardware.

---

## RISK-17 (NEW): EIA APIv1 fully discontinued Nov 2022 — every `bronze_eia` run had silently written zero rows for ~3.75 years — RESOLVED (migrated to APIv2)

**Status:** ✅ **FIXED (14 Aug 2026).** Discovered on `check_eia_series.py`'s
first-ever live run — part of the 8-script preflight batch authored this
thread (FRED/EIA/BLS/BEA/US Treasury/IMF/Polygon/AlphaVantage).

**GD Reference:** GD §3.3.3 (EIA weekly crude oil cadence),
Supplementary Design v1.1 G1 (`IncFetchProtocol` pattern `_fetch_series`
mirrors for EIA specifically).

### What the risk was

`eia_ingester.py::_fetch_series()` called `https://api.eia.gov/series/`
(EIA's legacy v1 endpoint), per `FIX EIA-1`'s original rationale ("v1 is
the stable, correct format for PET.* series"). EIA's own documentation
confirms APIv1 was fully discontinued November 2022 — the endpoint had
not existed for roughly 3.75 years. Every Wednesday `bronze_eia` run
since deployment returned HTTP 404 on all 4 series, both batched and
isolated, and silently wrote zero rows.

### Why this was invisible

`_fetch_series()` catches non-200 responses with a `logger.warning` and
returns `None` — `run()` then simply logs `0 OK, 0 failed` (not an
exception, not a crash). No `EIA_API_KEY not set` note printed alongside
the 404s, so this was never mistaken for an auth gap either. Nothing in
the existing test suite exercised the real v1 URL against a live
response — all mocks assumed the old shape was still valid.

### Fix

Migrated to APIv2's `/v2/seriesid/{id}` backward-compatibility route,
which accepts the same legacy v1-style series IDs (`PET.WCRSTUS1.W`
etc.) directly — the minimal-surface-area fix, not the larger v2
category/facet route redesign `FIX EIA-1` was originally avoiding (that
concern — v2's category paths vary per dataset — still holds for the
full route/facet approach; it just doesn't apply to the `/seriesid/`
backward-compat path). Response parsing updated from v1's
`data.series[0].data = [[period, value], ...]` to v2's
`response.data = [{"period": ..., "value": ..., ...}, ...]`.
`config/schemas/eia_oil.yaml`'s `expected_columns` required no changes —
`_fetch_series()` normalizes both shapes into the identical internal
Bronze record before the schema validator is ever consulted.

### What this does NOT resolve

Not empirically confirmed against a live EIA APIv2 response for these 4
specific series — this sandbox has no network route to `api.eia.gov`.
Pending live confirmation on network-enabled hardware (same discipline
as RISK-16's Gate 1 before its live run).

### Verification

`tests/unit/test_eia_ingester.py` rewritten for the v2 shape (URL now
carries `series_id` in the path, not `params["series_id"]`; response
rows are dicts, not `[period, value]` pairs) — 16/16 passing, 0
regressions in the rest of the suite. `check_eia_series.py` mirrors the
identical URL + parsing change to keep testing the real production path.

---

## RISK-18 (NEW): BEA `pce_deflator` LineDescription match failed 0/310 rows live; `trade_balance` was reading the wrong table entirely — RESOLVED (LineNumber-based matching + table swap)

**Status:** ✅ **FIXED (14 Aug 2026).** Discovered on
`check_bea_datasets.py`'s first-ever live run.

**GD Reference:** CI/CD Ops Guide v1.7.4 NEW-7 / this project's own
`FIX GLD-001` (BEA NIPA unit-mixing — LineDescription filter).

### What the risk was

Two independent problems in `bea_ingester.py::_fetch_nipa()`:

1. **`pce_deflator` (T20304):** `FIX GLD-001`'s `LINE_FILTER` required an
   exact string match against `LineDescription == "Personal consumption
   expenditures"`. Live, T20304 returned 310 rows and **zero** matched —
   BEA's actual wording differs from the string this project had assumed
   (table choice itself is correct, confirmed via BEA's own NIPA table
   register). `_fetch_nipa()` would have returned an empty DataFrame for
   this series on every real run.
2. **`trade_balance` (T40100):** Table `T40100` is BEA's International
   Transactions / current-account (balance-of-payments) table — confirmed
   via its own returned rows ("Balance on current account, NIPAs",
   "Current payments to the rest of the world", ...) — a different,
   broader concept than a GDP-component "net exports of goods and
   services" line (current account also nets in primary/secondary income
   flows GDP accounting excludes). This reads as the wrong table chosen
   at original design time, not wording drift.

### Why this was invisible

Both failure modes are silent by design (`FIX GLD-001`'s own filter
returns an empty DataFrame, not an error, when nothing matches) — exactly
the class of bug this project's "silent drops are a risk pattern"
principle exists to catch. No live preflight check existed for either
table until this thread.

### Fix

`pce_deflator`: switched to `LineNumber == "1"` — BEA's NIPA table
convention places a table's headline/aggregate row at a fixed
`LineNumber`, structural to the table format rather than a label BEA
reworks casually; robust to exactly the wording drift that broke the old
match. `trade_balance`: table switched `T40100` → `T10105` (Table 1.1.5,
Gross Domestic Product — the standard GDP-components table), matched via
`LineNumber == "15"`, inferred from Table 1.1.x's standardized line
structure (confirmed via an actual BEA Table 1.1.3 listing sharing the
identical line layout — Line 15 = "Net exports of goods and services").
`real_gdp` (T10106) is **unchanged** — its `LineDescription` match
already passes live.

### What this does NOT resolve

`trade_balance`'s exact `LineNumber` for T10105 is inferred from an
external reference and a same-layout sibling table, **not yet
empirically confirmed against a live T10105 response for this
pipeline's own request parameters** — this sandbox has no network route
to `apps.bea.gov`. Treat as decided-in-direction, not decided-in-detail,
until that live run happens.

### Verification

`tests/unit/test_bea_ingester_gld001.py`'s `pce_deflator`/`trade_balance`
tests rewritten to prove the fix's actual point — the row that matches
now carries a **deliberately different** `LineDescription` wording than
the old filter string, demonstrating the match survives exactly the
drift that broke it live. `real_gdp`'s tests are untouched and still
pass. 2 new tests lock in the `LINE_NUMBER_FILTER` dict and the
`T10105` table swap. 32/32 passing in this file, 0 regressions elsewhere.

---

## RISK-19 (NEW): 5 dead/redundant FRED series silently costing nothing but registry cleanliness; 6 declared Treasury tenors silently dropped by the registry-gate mechanism — RESOLVED (pruned + registered)

**Status:** ✅ **FIXED (14 Aug 2026).** Discovered on
`check_fred_series.py`'s first-ever live run — the first live check ever
run against the 61 non-commodity series in `config/fred_series.yaml`
(RISK-15's `check_fred_commodity_series.py` only ever covered the 6
`domain: commodity` Track 2 series).

**GD Reference:** GD §3.3.3 ("full 1M–30Y yield curve"), Supplementary
Design v1.1 G1 (`series_filter` registry-gate mechanism).

### What the risk was

**Dead series (5):** `GOLDAMGBD228NLBM` (deleted FRED 2022-01-31, ICE
Benchmark Administration data removal), `NAPM` and `NMFCI` (both deleted
FRED 2016-06-24 at ISM's own request — no free replacement exists, ISM
data is paid-license only), `PPIFGS` (discontinued by BLS ~Feb 2016,
frozen at 2015-12-01 live — redundant with `PPIFIS`, already live and
fresh), `CSCICP03USM665S` (frozen since 2024-01 — its Canada sibling
series in the same OECD family already carries FRED's explicit
`(DISCONTINUED)` tag; redundant with `UMCSENT`, confirmed fresh). None
were in `regime_inputs` — macro regime detection was never affected.

**Registry gap (6 Treasury tenors):** `treasury_ingester.py`'s
`TREASURY_FRED_SERIES` has always declared all 13 tenors (1M through
30Y), but `FREDIngester.run()`'s `series_filter` can only retain series
already present in the loaded `config/fred_series.yaml` registry —
`DGS1MO`, `DGS3MO`, `DGS6MO`, `DGS1`, `DGS7`, and `DGS20` were never
registered, so they were silently dropped on every run despite being
named in the filter. Bronze had only ever ingested 4 of the 13 declared
tenors (2Y/5Y/10Y/30Y) — the exact "silent drops" failure class this
project's own principles flag as a recurring risk pattern.

### Why this was invisible

`_fetch_series()` catching a dead series' API exception logs at DEBUG
level only, incrementing neither the success nor the failed counter in
`run()`'s own INFO-level summary — a dead series and a series that
simply never existed in the registry look identical from the outside
(neither appears in the run log at all). No prior live check had ever
enumerated the full non-commodity registry against real FRED responses.

### Fix

5 dead/redundant series removed from `config/fred_series.yaml` (67 → 62).
6 missing Treasury tenors registered under `monetary_policy`, matching
the existing `DGS2`/`DGS5`/`DGS10`/`DGS30` entry pattern (62 → 68 total).
Grep-sweep (checking for other `src/` references to the 5 pruned series)
found and cleaned two genuine dead references: `fred_ingester.py`'s
`RELEASE_LAG_DAYS` dict (5 now-inert keys removed — same vestigial-config
class as this project's own `index: []` precedent) and
`bls_ingester.py`'s `fred_mirror_map["PPI"]` (dropped `PPIFGS`, kept
`PPIFIS`). The 6 new tenors deliberately do **not** get a
`RELEASE_LAG_DAYS` entry — they fall back to the 7-day default rather
than their siblings' 1-day lag, a decided scope boundary (no `src/`
behavior change beyond the registry gate itself), not an oversight.

### Verification

New dedicated file `tests/unit/test_fred_series_registry_adr041_042.py`
(24 tests) — static registry content (counts, presence/absence, no
duplicate IDs), and, more load-bearing, an end-to-end reproduction of
the exact silent-drop mechanism using the **real** production registry
file (not a mocked one): `series_filter=["DGS20"]` against
`FREDIngester()` now actually reaches `fredapi.Fred.get_series()` and
writes a Bronze file, where before this fix it would have silently
retained 0 series and written nothing, with no error. 1466 → 1492
passed (+26: 24 new + 2 new BEA additions above), 0 regressions
elsewhere. Coverage: 81.46% aggregate (gate: 80%).

---

## RISK-20 (NEW): MTF score was structurally unreachable — watchlist mathematically incapable of returning a single row — RESOLVED (ADR-046 Path C)

**Status:** ✅ **FIXED (22 Aug 2026).** Discovered via live-code trace in
`GMI_Decision_Document_v11.docx` §1.3–1.5 (no live data run needed — the
formula's own thresholds prove the ceiling unreachable), resolved this
session per Ovi's explicit choice of Path C.

**GD Reference:** GD §6.3 (MTF score −7..+7, grades A/B/C/D),
Supplementary Design v1.1 (`bronze_ohlcv_daily` fetch scope).

### What the risk was

`mtf_alignment.py` summed `TIMEFRAMES = ["5m","15m","1H","4H","1D","1W","1M"]`
into `mtf_score`, filling any timeframe with no matching
`tech_signals_{TF}.parquet` to a literal `0` rather than excluding it from
the sum. `market_ingester.py`'s `INTRADAY_TIMEFRAMES = ["5m","15m","1H"]`
was defined but wired into no job — only `DEFAULT_TIMEFRAMES=[1D,1W,1M]`
was ever fetched. Because `ohlcv_aggregator.py` synthesizes 4H from Silver
1H only, and 1H was never fetched either, 4H was empty too. Four of the
seven `TIMEFRAMES` entries — 5m, 15m, 1H, 4H — contributed 0 to every
symbol's score, every day, leaving only 1D/1W/1M able to contribute a
real ±1 each: maximum achievable `|mtf_score|` was 3. `screener.py`
required `ABS(mtf_score) >= 5 AND signal_quality IN ('A','B')` — under
the grade table (A: ≥6, B: ==5, C: ==4, D: ≤3), a ceiling of 3 falls
entirely inside grade D. The screener's `mtf` CTE returned zero rows
**unconditionally** — not a probabilistic or market-condition-dependent
outcome, a structural certainty traced end to end against the live
formula and live threshold constants.

### Why this was invisible

Every individual piece degraded gracefully and logged only a WARNING
("treating as neutral") — no exception ever propagated, no job ever
failed, `run_job()`'s sentinel was written every day as if the pipeline
had succeeded. `build_watchlist()` returning an empty DataFrame and
`run()` logging "No candidates found" is indistinguishable, from a
status check that only looks at sentinel presence, from a day with
genuinely no qualifying symbols. This is the same failure shape as
RISK-17 (EIA silent zero-row writes) and RISK-19 (silent registry drops)
— four independently-reasonable degradation decisions compounding into a
structurally guaranteed empty output, with no single line of code that
looks wrong in isolation.

### Fix (Path C — Ovi's explicit choice among three options presented)

1H wired up alone (not 5m/15m) via a new `LAYER1_TIMEFRAMES =
DEFAULT_TIMEFRAMES + ["1H"]` constant, applied only to
`job_registry.py::_bronze_ohlcv()` (Layer 2 context deliberately
untouched — no consumer needs intraday context data this cycle). This
required ADR-045 (Bronze OHLCV timeframe partition, this same session) to
land first, or 1H would starve via the identical same-day idempotency
collision ADR-045 exists to close for 1D/1W/1M. Real contributors raised
from 3 to 5 (1H, 4H, 1D, 1W, 1M) — 1H both scores directly and unblocks
4H's existing synthesis. `TIMEFRAMES` in `technical_signals.py` and
`mtf_alignment.py` trimmed to exactly these 5 (5m/15m removed entirely,
not left as permanent 0-padding). Score range and grades recalibrated per
Ovi's explicit instruction: −5..+5, **A ≥ 4, B == 3, C ≤ 2** (grade D
removed — C is the new catch-all). `screener.py`'s `MIN_MTF_SCORE`: 5 → 3,
preserving the original design relationship to the grade B boundary.
Downstream consumers of the old scheme (`backtest/engine.py`'s
`min_mtf_score` default and its `quality == "D"` exit/sentinel logic,
`pipeline_config.py`'s `min_mtf_score_screener` default) updated to match.

### What this does NOT resolve

5m/15m remain permanently unfetched by design (Path C, not Path A) —
grade A (`|score|>=4`) is reachable at 4-of-5 contributors agreeing, not
5-of-5 unanimous, which is a real (accepted) change in what "strong
alignment" means versus the original 7-timeframe design intent.

### Verification

`test_mtf_alignment.py`'s `TestMTFScoreLogic` and
`TestComputeMtfAlignmentScoring` rewritten end to end for the new range —
every threshold asserted is one this project's own formula actually
produces post-fix, not a value carried over from the old scheme.
`test_screener.py`, `test_backtest_engine.py`, `test_pipeline_config.py`,
`test_full_system.py` updated to match. 1510 → 1521 passed (session
total, combined with RISK-21 and ADR-045/047 below), 0 regressions.
Coverage 88.04% (gate 80%).

---

## RISK-21 (NEW): Silver's Bronze-read glob was timeframe-blind — every declared timeframe would have silently read the identical rows — RESOLVED (fixed)

**Status:** ✅ **FIXED (22 Aug 2026).** Found empirically while verifying
ADR-045 (Bronze OHLCV timeframe partition) would actually fix its
reported symptom — not named in ADR-045's own text, which was written
against the Bronze write/scan side only.

**GD Reference:** GD §17.2 (Layer Independence — Silver reads Bronze via
a defined interface), Supplementary Design v1.1 G1
(`IncFetchProtocol`/timeframe scoping pattern this bug's fix mirrors).

### What the risk was

`ohlcv_processor.py`'s Bronze-read glob (both `run()` PASS 1 and
`run_context()`) constructed
`BRONZE_OHLCV_PATH / market / "**" / f"symbol={symbol}" / "**" / "*.parquet"`
— no timeframe segment anywhere in the pattern. Reproduced empirically
against a real production Bronze file (copied from the live repo via the
Filesystem MCP connector, not synthesized): a real `AAPL` Bronze parquet
(2,512 daily-cadence rows, 2016-08-23 to 2026-08-20 — the only Bronze
OHLCV data that had ever successfully persisted, per RISK-20/ADR-045's
same-day idempotency finding) matched this glob **identically** for
`tf='5m','15m','1H','1D','1W','1M'` — every one of PASS 1's six declared
timeframes. Silver had never actually been run against this repo's Bronze
data (`data/silver/` did not exist), so no real output had been
corrupted yet — but the very first `silver_ohlcv` run would have written
the same 2,512 rows to `AAPL_5m_silver.parquet`,
`AAPL_15m_silver.parquet`, `AAPL_1H_silver.parquet`, etc., all mislabeled
with a `timeframe` column that did not match their actual cadence.

Left unfixed, ADR-045's own Bronze-side partition fix would have made
this **worse, not moot**: once Bronze genuinely separates 1D/1W/1M/1H
into distinct partitions, this glob's recursive `**` would union all of
them back together into one blended multi-cadence series per Silver TF
file, instead of scoping to the one Bronze partition that actually
matches the timeframe being processed.

This finding also revised RISK-20's own severity retroactively: even the
pre-fix "3 working timeframes" (1D/1W/1M) were never 3 independent votes
— had Silver ever actually run, all three would have read the identical
Bronze blob (since only 1D ever successfully wrote), so `mtf_score`'s
only non-zero values would ever have been −3/0/+3, never a value
reflecting genuine weekly/monthly divergence from daily.

### Why this was invisible

Silver had simply never been run against real Bronze data in this repo —
a gap in *operational history*, not a gap in test coverage (the existing
unit tests all wrote a single shared Bronze fixture per test and asserted
every TF picked it up, which is passing-but-wrong: that assertion is
exactly what a timeframe-blind glob would make true, and the tests never
distinguished "correctly scoped" from "accidentally uniform"). No error,
no exception, no warning — a glob that matches everything looks
identical to a glob that matches the right thing, right up until two
genuinely different timeframes' worth of Bronze data exist to tell them
apart.

### Fix

`timeframe={tf}` segment added to both glob-construction sites in
`ohlcv_processor.py`, matching Bronze's new ADR-045 partition structure
exactly (`market/timeframe={tf}/source={src}/symbol={sym}/...`).

### Verification

Empirical reproduction performed *before* writing the fix, not after
(matching this project's own empirical-first discipline): the real
`AAPL` Bronze file was copied into an isolated sandbox and the exact
pre-fix glob pattern run against it for all 6 `_RUN_BRONZE_TFS` entries,
confirming identical matches across every one. New regression test
`test_run_does_not_blend_timeframes_across_bronze_partitions`: two
differently-sized Bronze fixtures written at `timeframe=1D` (40 rows) and
`timeframe=1W` (5 rows) for the same symbol must produce two
differently-sized Silver outputs — before the fix this would have been
one glob matching both, after the fix each timeframe reads only its own
partition. Existing fixture helpers (`_write_bronze_fixture`,
`_write_bronze_context_fixture`) reworked to require an explicit
`timeframe` parameter, closing the "single shared fixture satisfies every
TF" gap the old tests never caught. 1510 → 1521 passed (session total,
combined with RISK-20 and ADR-045/047 above), 0 regressions. Coverage
88.04% (gate 80%).

---

## RISK-22 (NEW): AlphaVantage's 25-req/day budget gains a second, permanent, non-DXY consumer — ACCEPTED (scoped exception, not a reopened policy)

**Status:** ✅ **ACCEPTED — documented exception (29 Aug 2026).** Introduced
by `USD/CNH Source Adjustment Addendum v1.0` (ADR-048) — recorded here per
that addendum's own explicit instruction ("Dicatat di KNOWN_RISKS.md
sebagai accepted exception ... agar tidak diam-diam menjadi preseden" —
documented as an accepted exception so it does not silently become a
precedent).

**GD Reference:** GD §11.2 (`DailyBudgetLimiter`), Supplementary Design
v1.1 G4 ("AV dihapus dari chain default" — AlphaVantage removed from the
default Forex `ChainedAdapter`, DXY-only fallback carve-out preserved).

### What the risk is

Supplementary Design v1.1 G4 established a narrow, single carve-out:
AlphaVantage stays in the codebase and in the Forex `ChainedAdapter`
purely as a last-resort fallback for DXY, consuming at most a handful of
the shared 25-req/day `DailyBudgetLimiter` budget on days yfinance + the
24h `ForexDayCache` both fail. ADR-048 adds a second, structurally
different consumer: the Layer 2 USD/CNH context anchor now uses
AlphaVantage FX_DAILY as its **sole, permanent primary source** — not a
fallback. This is a **reliable, daily** draw on the shared budget (3
calls/day: 1D/1W/1M via `run_context()`'s `DEFAULT_TIMEFRAMES`), not an
occasional one.

### Blast radius

- 3/25 calls/day reserved for CNH under normal operation, leaving 22/25
  for DXY's occasional fallback use and any future AV consumer —
  comfortable headroom today, but the budget is a *shared, unpartitioned*
  pool (`SourceLimiters.alphavantage`, `src/utils/rate_limiter.py`). A
  third permanent consumer added later without re-reading this entry
  could silently starve CNH, DXY-fallback, or both on a day both draw
  simultaneously.
- `outputsize` is sized to the requested date-range span (`>100 days` →
  `full`, else `compact` — `alphavantage_adapter.py`, FIX ADR-048)
  specifically to keep CNH's routine daily draw to 3 lightweight calls
  rather than 3 full-history downloads; this bounds the *load* per call,
  not the *count* against the shared budget.
- If the AlphaVantage budget is ever exhausted before CNH's 3 daily calls
  run (e.g. a future consumer added without re-reading this entry, or
  repeated DXY-fallback firing on a bad yfinance day), CNH's Bronze write
  for that timeframe simply does not happen that day (`_fetch()` returns
  `None`, logged at `debug` level, `_run_context_symbol()` returns early)
  — a silent coverage gap for that one Layer 2 anchor, not a crash. This
  degrades any CrossAssetEngine module reading CNH for that date (all
  four currently operate on close-derived returns per the addendum's §4)
  but does not affect any other instrument.

### Mitigation in place

1. This entry itself — the addendum's explicit instruction was to
   document this as a *scoped, accepted* exception precisely so a future
   third permanent AV consumer is a deliberate decision weighed against
   this context, not a silent repeat of the same "just add one more"
   reasoning that would eventually re-saturate the shared budget it was
   designed to protect.
2. `outputsize` compact/full sizing (FIX ADR-048) keeps CNH's per-call
   cost low regardless of call *count*.
3. `DailyBudgetLimiter.record_call()` already warns at ≤3 remaining
   (`src/utils/rate_limiter.py`) — a shared, pre-existing signal, not new
   to this risk, but the relevant one to watch now that a second
   permanent consumer exists.

### Operator playbook if the AlphaVantage budget is found exhausted

1. Check `data/health/pipeline_runs.db` / the daily health report for
   which job(s) hit the `[AV] Daily budget exhausted` log line
   (`alphavantage_adapter.py`).
2. Confirm CNH's 3 daily calls (1D/1W/1M) landed before assuming a
   capacity problem — a burst of DXY-fallback calls on an unrelated bad
   yfinance day is the more likely one-off cause, not structural
   under-provisioning.
3. If genuinely and repeatedly tight, the fix is a deliberate design
   decision (raise the AlphaVantage tier, move CNH to a source-specific
   limiter, or reduce DXY's fallback eagerness) — not a silent
   workaround. Update this entry if the resolution changes the
   shared-budget model.

---

*Last updated: v1.17.2 — `USD/CNH Source Adjustment Addendum v1.0`
(ADR-048, 29 Aug 2026): the Layer 2 USD/CNH context anchor
(`context.dollar_basket`) is re-sourced from yfinance (`USDCNH=X`,
confirmed broken live — returns 1 row) to AlphaVantage
`FX_DAILY(USD,CNH)` exclusively — not primary, not fallback for
yfinance. Two rejected substitute candidates preceded this decision:
`CNH=F` (thin-OI quarterly-roll futures, no live Yahoo data) and `CNH=X`
(same sparsity as `USDCNH=X`); `CNY=X` (onshore) was clean but
economically distinct — a managed PBOC rate that decouples from offshore
CNH by hundreds of pips exactly during the regime-stress episodes
`score_em_risk` exists to detect — so was rejected as a substitute
despite being live-clean. AlphaVantage `FX_DAILY(USD,CNH)` verified
empirically: 3,079 clean daily rows, 2014-11-07 → 2026-08-27 (~11.8
years), covering both historical CNH stress episodes (2015 devaluation,
Aug 2019 "break 7"). `instruments_identity.yaml`'s CNH entry gains a
`data_source: alphavantage_fx` / `from_symbol: USD` / `to_symbol: CNH`
override (`identity.schema.yaml` updated to allow the 3 new
instrument-level fields); `yfinance_symbol` retained for audit/
historical reference only. `market_ingester.py` gains one override
check (`inst.meta.get('data_source')`) applied consistently across
`_fetch()`, `_primary_source_for()`, and `_run_context_symbol()` — a
mismatch between any two would reproduce ADR-045's read/write
partition-label bug one layer up (source segment instead of timeframe
segment). New `config/schemas/alphavantage_fx.yaml` (open/high/low/close
only, no volume — a structural characteristic of the endpoint, not a gap,
since CNH's sole consumer CrossAssetEngine operates on close-derived
returns exclusively). `alphavantage_adapter.py`'s `outputsize` now sized
to the requested date-range span (`>100 days` → `full`, else `compact`)
instead of always `full` — named explicitly in the addendum's own
Consequences section, so routine daily CNH runs stop re-downloading the
entire ~11.8-year history on every call once Bronze has caught up once.
Second, permanent, non-DXY consumer of the shared 25/day AlphaVantage
budget — documented as RISK-22 (accepted, scoped exception to
Supplementary Design G4's policy, not a reopening of it). PATCH bump:
source-reassignment bug fix for one existing Layer 2 instrument, no
Interface Contract or universe change (`EXPECTED_TOTAL` stays 699). 1521
→ 1533 passed (+12: 6 routing tests, 4 adapter outputsize boundary tests,
2 schema tests), 0 failed, 0 regressions, coverage 88.06% (gate 80%),
both modified files at 100% coverage. August 2026.
Prior entry: v1.17.1 — `GMI_Decision_Document_v11.docx` (22 Aug 2026):
all three items (ADR-045, ADR-046, ADR-047) reconciled into a single
version bump per Ovi's explicit instruction, since none had been mirrored
to the live repo before that instruction arrived. ADR-045: Bronze OHLCV
write/scan path gains a `timeframe={tf}` partition — 1D's same-day
idempotency write was silently starving 1W/1M (and would have starved 1H)
on every run, not just cold start (RISK-20). Consequential: identical fix
applied to `_run_context_symbol()` (Layer 2), and a far more severe,
genuinely new finding — Silver's own Bronze-read glob was timeframe-blind
and would have blended every declared timeframe's Bronze data together on
the very first `silver_ohlcv` run (RISK-21) — found and fixed in the same
pass, approved by Ovi mid-session given the universe-wide blast radius.
ADR-046 Path C (Ovi's explicit choice, with exact recalibrated grade
thresholds supplied directly): MTF score was mathematically incapable of
ever populating the watchlist (RISK-20) — 1H wired into Bronze alone (not
5m/15m), real contributors raised 3→5, score range recalibrated to −5..+5
with grade D removed (A≥4, B==3, C≤2 catch-all), `MIN_MTF_SCORE` 5→3, all
downstream consumers of the old scheme updated to match. ADR-047: AU/AG
commodity tickers routed through `inst.yfinance_symbol` directly
(`GC=F`/`SI=F`) instead of `to_api_symbol()`'s generic suffix rule, which
produced invalid `AU=F`/`AG=F`; `CL=F` unaffected (already correct by
coincidence). PATCH bump: bug fixes to existing broken components across
all three ADRs, no Interface Contract or Silver/Gold schema change. 1510
→ 1521 passed (+11), 0 failed, 0 regressions, coverage 88.04% (gate 80%).
Pre-existing non-partitioned Bronze OHLCV data left as-is by design
(confirmed only 1D data had ever actually persisted) — quarantine-vs-
leave decision remains open, Ovi's call, per ADR-045's own Consequences.
August 2026.
Prior entry: v1.15.2 — `GMI_Decision_Document_v9.docx` (14 Aug 2026):
first live run of all 8 preflight scripts authored the same thread
surfaced 5 findings across EIA/BEA/FRED, all fixed here. ADR-038: EIA
APIv1 confirmed fully dead (discontinued Nov 2022) — migrated to APIv2's
`/v2/seriesid/` backward-compat route (RISK-17). ADR-039: BEA
`pce_deflator`'s `LineDescription` match failing 0/310 rows live — fixed
via `LineNumber`-based matching, robust to wording drift (RISK-18).
ADR-040: BEA `trade_balance` was reading the wrong table (`T40100`,
current-account) — switched to `T10105` (GDP components), `LineNumber`
inferred from Table 1.1.x's standard layout, pending live confirmation
(RISK-18). ADR-041: 5 dead/redundant FRED series pruned
(`GOLDAMGBD228NLBM`/`NAPM`/`NMFCI`/`PPIFGS`/`CSCICP03USM665S`), plus a
grep-sweep cleanup of two now-dead references (`RELEASE_LAG_DAYS`,
`bls_ingester.py`'s FRED-mirror map). ADR-042: 6 Treasury tenors
(`DGS1MO`/`DGS3MO`/`DGS6MO`/`DGS1`/`DGS7`/`DGS20`) registered, closing
the registry-gate silent-drop gap `series_filter` had against them since
`treasury_ingester.py` was written (RISK-19). PATCH bump: bug fixes to
existing ingesters + config pruning/addition, no new capability exposed
downstream (matching this project's own PATCH precedent — cf. v1.13.4/
v1.13.5). 1466 → 1492 passed (+26), coverage 81.43% → 81.46%. Not yet
empirically confirmed against live EIA/BEA APIs from this environment —
3 items (ADR-038's v2 response shape, ADR-040's exact `T10105` LineNumber)
explicitly flagged pending live confirmation, same discipline as this
file's own RISK-16/Gate 1 precedent. August 2026.
Prior entry: v1.15.0 — Three-part session, none touching `src/`:
(1) RISK-15's live-FRED half confirmed — `check_fred_commodity_series.py`
run for real on the M1, all 6 Track 2 series PASS. (2) Gate 1 (RISK-16)
extraction pass authored: `extract_us_weights_from_sheet()` +
`--extract-weights` CLI mode on `check_bis_eer_weights.py`, 11 new tests
against a synthetic workbook — not yet run against the real file. (3)
Proxy correlation studies authored: new
`scripts/preflight/check_proxy_correlation.py` (CPO/RUBBER/TIN/NICKEL
vs. their RISK-15 FRED benchmarks, monthly-return Pearson correlation),
17 new tests — not yet run against real yfinance/FRED. All 71 tests in
`test_preflight_scripts.py` (43 pre-existing + 28 new) collected and
passed together in an isolated sandbox before either script touched the
real repo. MINOR bump: two new preflight capabilities, no `src/` change
(matching this project's own "new job/feature" MINOR precedent, not a
PATCH-level fix). 1432 → 1460 passed (+28), coverage unchanged. August
2026.
Prior entry: v1.14.0 — RISK-15 (FRED Track 2 commodity supplements)
resolved: `config/fred_series.yaml` gained a new `commodity` domain (6
series — 2 pre-existing per ADR-005/006, 4 new per ADR-030–033),
`src/bronze/fred_ingester.py` gained matching `RELEASE_LAG_DAYS`
entries, and a new preflight script
(`scripts/preflight/check_fred_commodity_series.py`) was authored to
verify all 6 against live FRED. Found and fixed a second bug while
verifying: Iron Ore's real FRED series ID is `PIORECRUSDM`, not the
`PIORECRORECUSDM` both Architecture Extension v1.0 ADR-005 and this
risk entry itself had been citing unverified. MINOR bump (not PATCH):
judgment call — this activates real new data flow into Bronze on the
next `bronze_macro_weekly` run, which reads as "new indicator" under
this project's own versioning semantics (CI/CD Ops Guide) even though
no live Gold-layer consumer exists yet (`ForecastModule`/CrossAssetEngine
not built). 1422 → 1432 passed (+10), coverage unchanged. August 2026.
Prior entry: v1.13.5 — `scripts/archive/` removed entirely (Ovi, 6 Aug
2026 commit) broke `test_archived_migration_scripts.py`'s 10
archive-existence regression guards (RISK-11) — diagnosed as a G-5
(test pass) failure wearing a G-6 (coverage) label, not an actual
coverage regression (81.54% measured, above the 80% gate; the combined
`pytest --cov` invocation's exit code was tripped by the test failures
alongside it). Confirmed via full-tree grep that nothing else in the
repo held a live import or path reference to `scripts/archive/` — the
bug those 10 tests guarded against (destructive import-time writes) is
now structurally impossible, not just disabled. File trimmed to its 1
still-meaningful test (`make migrate`'s safety net) and renamed
`test_archived_migration_scripts.py` → `test_makefile_safety_nets.py`.
PATCH bump: test-suite maintenance only, no `src/` change. 1432 → 1422
passed (net –10: –11 removed, +1 kept), coverage 81.43% unchanged.
August 2026.
Prior entry: v1.13.4 — Gate 1 discovery phase live-confirmed on the
M1: `weightsb.xlsx` downloaded for real (492,941 bytes, 10 sheets,
1993-95 through 2020-22), all 13 target currency codes located in
every sheet — layout now fully known, values not yet extracted (Gate
1 stays open). TYPE=Nominal / `.N.B.` key shape live-confirmed
(3,813,875 bytes per currency, ~16x the prior monthly-restricted
figure). Unrelated: `poetry.lock` content-hash desync found and fixed
(stale since the 3 Aug openpyxl edit; 113/113 packages unchanged, only
the hash line differed). 1432 passed / 0 failed / 0 error, coverage
81.43% unchanged. August 2026.
Prior entry: v1.13.3 — Gate 1 (ADR-017/018) substantially advanced: BIS's
actual Broad EER weights file located (data.bis.org/topics/EER's own
Methodology section → bis.org/statistics/eer/weightsb.xlsx, confirmed
real and reachable) with a new `--discover-weights` inspection mode
(openpyxl). TYPE decided: Nominal (matches DXY; BIS confirms Daily EER
exists only for Nominal). 13-currency EER expansion confirmed LIVE (3 Aug
preflight re-run, 237,188 bytes, all 13 PASS) — closes the Monthly-
vs-Daily/ADR-010 question definitively (all 12 CBs confirmed daily via
live query). August 2026.
Prior entry: v1.13.2 — HKD/TWD/NOK added to `check_bis_eer_weights.py`'s
Broad Dollar basket (13/13 currencies complete); endpoint key refactored
to derive from `BROAD_DOLLAR_REF_AREAS` rather than a hand-duplicated
literal, closing that drift risk structurally. FIX BIS-1's core fix
(WS_CBPOL/WS_EER dataflow correction) confirmed LIVE on the M1 — RISK-16
→ RESOLVED. August 2026.
Prior entry: v1.13.1 — FIX BIS-1 (1 Aug 2026): BIS CBPOL/EER endpoints
corrected — the real root cause was the dataflow IDs themselves
(WS_CBPOL_D → WS_CBPOL, WS_EER_M → WS_EER), not just the v1→v2 URL path
structure the 28 Jul thread fixed. RISK-16 (NEW) — fixed at the code
level, pending live confirmation. July/August 2026.
Prior entry: v1.13.0 — ADR-029–033 (`GMI_Decision_Document_v7.docx`):
tvdatafeed retired entirely (RISK-1 → RESOLVED); CPO/RUBBER/TIN/NICKEL
un-deferred via yfinance equity proxies (F34.SI/STA.BK/AFM.V/NIC.AX);
RISK-15 (NEW, OPEN) — pre-existing FRED Track 2 supplement gap found
incidentally, flagged not fixed. July 2026.
Prior entry: v1.12.1 (in progress) — Decision C coverage tranche items
#1–#3 (`mtf_alignment.py`, `screener.py`, `eia_ingester.py`), three real
bugs found and fixed via first real-function test coverage (RISK-12,
RISK-13, RISK-14), July 2026.
Prior entry: v1.11.2 — Post-ADR-026 hardening (coverage gap closure,
dead-script archival, hardcode fixes), July 2026.
Prior entry: GMI Decision Document v3 implementation (Decision A +
Decision B Step 1), July 2026.
Earlier: GMI Decision Documents v1 & v2 implementation, July 2026.
Earlier still: Bronze+Silver formal audit following GMI Wave 1 Cycle 3, July 2026.
Earliest: Production Readiness Assessment v1.7.2 remediation, June 2026.*
