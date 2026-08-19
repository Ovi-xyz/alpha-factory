# 2026-08-17 — Coverage Tranche Phase 1–2: 25 Modules to 100% Line Coverage

**Version**: 1.15.2 → 1.15.3
**Trigger**: Ovi's direct instruction, not a written GMI Decision Document —
"let's continue with the coverage tranche toward 95%" after confirming
EIA's `PET.WGIRIUS2.W` timeout on 17 Aug 2026 was a transient blip (retry
passed). Direct continuation of the v1.12.1 "Decision C" coverage tranche
precedent (July 2026) — same methodology, same exclusion policy for
`correlation_matrix.py`/`hmm_regime.py` (explicitly reconfirmed by Ovi
this thread rather than assumed from the old precedent).
**Scope**: 25 source modules brought to 100% line coverage across two
phases; 17 test files modified, 2 new test files created
(`test_base_ingester.py`, `test_bea_ingester.py`); `tests/COUNT_BASELINE.txt`,
`CHANGELOG.md`, `pyproject.toml`. No `src/` files touched — this release
is test-only.

---

## 0. Pre-tranche verification (before any test was written)

Per house convention, verified the live repo empirically before starting,
since the prior session ended mid-write on `src/bronze/eia_ingester.py`:

- `Filesystem:get_file_info` on `eia_ingester.py` (12,306 bytes) then a full
  `read_text_file` — confirmed the ADR-038 v2 migration was fully and
  correctly written, not truncated. The "mid-write" concern in memory was
  stale; the file was complete.
- `.git/refs/heads/main` (local: `8239130...`) compared against a fresh
  `git clone` of `github.com/Ovi-xyz/alpha-factory` — **identical commit**.
  This meant the sandbox clone was an exact mirror of live with zero manual
  sync needed, unlike prior sessions where local-uncommitted work had to be
  patched into the sandbox by hand.
- `pip install` (not poetry — faster for a pure test-coverage task) of the
  full dependency set, then `poetry` itself installed (a `check_poetry_env`
  preflight test expects the binary on PATH — sandbox environment gap, not
  a real defect).
- Baseline: **1493 passed, 0 failed** — matches memory and
  `tests/COUNT_BASELINE.txt`'s intended value exactly (the file itself was
  found stale at `1492` — off by one from the true baseline; corrected as
  part of this tranche's own baseline update, not treated as a separate
  fix).
- Full coverage baseline: **81.46% (4476 stmts, 830 missed)** — exact match
  to memory's recorded figure.

## 1. Scope decision — Decide phase before Implement

Presented a phased plan (5 phases, ranked by miss-count/effort) before
writing any test. Flagged `correlation_matrix.py` (66 missed, confirmed via
grep to be genuinely wired into `job_registry.py`'s `gold_correlation` job
— not dead code despite Architecture v2.0 describing it as "REPLACED" by a
CrossAssetEngine that hasn't been built yet) and `hmm_regime.py` (67
missed, confirmed via grep to be referenced **nowhere** in `src/` — an
orphaned Phase-2 module per GD §8.2) as items needing an explicit decision
before inclusion.

Ovi's response: exclude **both** from this tranche (not just the orphaned
one), then proceed with Phase 1. This is a stricter exclusion than the
v1.12.1 precedent's stated rationale ("REPLACED by design") would strictly
require for `correlation_matrix.py` given its confirmed live-wiring — Ovi's
call, recorded as a plain instruction, not re-litigated.

## 2. Phase 1 — 17 files, 61 lines closed (81.46% → 82.82%)

Small/isolated files first: `symbol_utils.py`, `eia_ingester.py`,
`bls_ingester.py`, `imf_ingester.py`, `schema_validator.py`,
`base_ingester.py` (new file — zero prior tests despite being the base
class every Bronze ingester subclasses), `source_adapter.py`,
`dependency_guard.py`, `context_anchors.py`, `sentiment_processor.py`,
`global_rates_processor.py`, `mtf_alignment.py`, `ohlcv_aggregator.py`,
`views.py`, `atomic_io.py`, `progress_checkpoint.py`,
`pipeline_dashboard.py`.

Notable non-trivial gaps closed:
- **`mtf_alignment.py`'s `atr_1h_df is None` branch**: the real DuckDB
  query always explicitly `SELECT`s `atr_14` by name, so a genuinely
  column-less result is structurally unreachable through normal file
  writes — any malformed source file just raises inside the query's own
  try/except and never reaches `tf_dfs` at all. Covered by mocking
  `duckdb.connect()` directly to return a controlled column-less frame,
  matching the branch's own defensive intent rather than fighting the
  real query's schema guarantee.
- **`pipeline_dashboard.py`'s `if job_name not in statuses: continue`**:
  similarly only reachable via `PIPELINE_SEQUENCE`/`JOB_REGISTRY` drift
  (a job name in the sequence list but removed from the registry) —
  covered by patching both to a deliberately inconsistent state.
- **False-positive coverage artifact**: `pipeline_dashboard.py` briefly
  showed line 292 (`main()` inside `if __name__ == "__main__":`) as
  missing when run with `--cov-config=/dev/null`. Traced to
  `pyproject.toml`'s own `exclude_lines` (which already excludes this
  exact pattern) being bypassed by the `/dev/null` override used for
  isolated per-file coverage checks. Re-ran under the project's real
  config — confirmed 100%, not a real gap. No src change; a reminder that
  `/dev/null`-config spot-checks can produce artifacts the full-suite run
  (which uses the real config) won't show.

**Byte-mismatch incident (self-inflicted, fully resolved)**: while writing
`test_sentiment_processor.py` to the live repo, a hand-typed decorative
`─` (U+2500, 3 bytes UTF-8) separator comment caused a 24-byte mismatch
between sandbox and live after an `edit_file` call. Diagnosed via an
actual `diff`/`cmp` against a reconstructed tail rather than re-guessing
the dash count, root-caused to miscounting a multi-byte character by hand,
and resolved by simplifying the comment (dropped the arbitrary trailing
dashes) identically in both sandbox and live — verified byte-identical
after. Checked all other Phase 1/2 additions for the same risk pattern
before continuing; none found elsewhere.

## 3. Phase 2 — 8 files, 248 lines closed (82.82% → 88.36%)

`alphavantage_adapter.py`, `polygon_adapter.py`, `yfinance_adapter.py`,
`market_ingester.py`, `bea_ingester.py` (new test file), `fred_ingester.py`,
`finnhub_ingester.py`, `bis_rates_ingester.py`.

### 3.1 Three Bronze adapters — the entire HTTP body had zero coverage

`alphavantage_adapter.py` (90-173), `polygon_adapter.py` (108-200), and
`yfinance_adapter.py`'s `YFinanceAdapter` class were essentially untested
end-to-end — only static helpers (`_parse_pair()`) and pre-request guard
clauses (missing API key, exhausted budget) had tests. Built full
`requests.get`/`yfinance.Ticker` mocking suites covering: success paths,
non-200/429 responses, malformed/missing field handling (confirming `None`
not `0.0` for missing OHLC values — an existing `FIX POL-2`/`FIX AV-3`
contract), pagination (`polygon_adapter.py`'s `next_url` following, capped
at `MAX_PAGES`), and network exceptions.

`polygon_adapter.py` and `yfinance_adapter.py` both wrap **real blocking
rate limiters** (`SourceLimiters.polygon` at 5 req/min free tier, ~15s
between calls). Every new test patches `.wait()`/`time.sleep()` to a
no-op — without this the suite would have taken minutes per adapter.

### 3.2 `market_ingester.py` — Layer 1 had zero coverage from the start

The single largest finding this tranche. `run()`, `_run_symbol()`, and
`_fetch()` — the entire Layer 1 (640-instrument trading-candidate) Bronze
ingestion path — had **no test coverage whatsoever**. Only the Layer 2
context-anchor methods (added later, GMI Wave 1 Cycle 3) had a test suite.
This means the production path responsible for the platform's actual
tradeable OHLCV data had run in production with zero direct test coverage
since inception.

No bug was found in the production code itself — `run()`/`_run_symbol()`/
`_fetch()` all behaved correctly under test. This is purely a historical
test-suite gap, not a defect. 26 new tests (`TestRunEntryPoint`,
`TestRunSymbol`, `TestFetchChainConstruction`,
`TestPrimarySourceForRemainingBranches`) close it, mirroring the existing
Layer 2 test patterns (`monkeypatch.chdir(tmp_path)` + fake loader/fetch
mocking) for consistency.

One correction mid-implementation: the four `TestFetchChainConstruction`
tests initially mocked adapter `.fetch()` to return a bare string
`"SENTINEL"`. `ChainedAdapter.fetch()` calls `.with_columns()` on whatever
the adapter returns to stamp the `_source` column — a string doesn't have
that method, so all four tests failed even though coverage had already
hit 100% (the exception path itself is inside `ChainedAdapter`, already
covered elsewhere). Fixed by returning a real minimal Polars DataFrame
(`_sample_yf_df()`, already defined in the file) and asserting on the
resulting `_source` column value instead of object identity.

### 3.3 Test-isolation bug found and fixed: `SourceLimiters.alphavantage`

`DailyBudgetLimiter` is instantiated once at module import as a
`SourceLimiters` class attribute — a genuine process-lifetime singleton.
`test_budget_exhausted_returns_none` (pre-existing) deliberately drains
the budget and never resets it. No `conftest.py` fixture resets it either.
This means `test_unsupported_tf_returns_none` (also pre-existing, directly
above it in the same file) could pass for the **wrong reason** — hitting
the budget-exhausted early return instead of the unsupported-timeframe
branch it claims to test — depending entirely on pytest's execution order
within the session.

Confirmed empirically: running the new `TestFetchHttpFlow` class's tests
in isolation initially left lines 96-97 (the `function is None` branch)
showing as missed at 97% coverage, even with 21 new passing tests — direct
evidence the old test wasn't reliably exercising that path. Fixed with:
(1) an `autouse` fixture resetting `SourceLimiters.alphavantage._reset_date`
before and after every test in the new class; (2) a new test,
`test_unsupported_tf_returns_none_isolated`, which asserts
`mock_get.assert_not_called()` — proving the code path never reaches the
HTTP layer at all, which the original test's assertion (`result is None`)
alone couldn't distinguish from a budget-exhausted short-circuit.

No `src/bronze/alphavantage_adapter.py` change — this is purely a test
infrastructure fix. Not logged as a new `KNOWN_RISKS.md` entry: the actual
production code was never wrong, and the issue is fully closed within this
same session (not deferred), which doesn't meet the bar `KNOWN_RISKS.md`
entries are reserved for (open/accepted risks).

## 4. Live-repo write discipline

All 25 test files (23 modified via targeted `edit_file` anchored on exact
tail content, 2 written fresh via `write_file`) were built and fully
verified in the sandbox first (`pytest` per-file with targeted coverage,
then full-suite regression after every batch), then mirrored to the live
repo via the Filesystem MCP connector with `get_file_info` byte-count
verification immediately after every write — no write was accepted without
an exact byte match against the sandbox source of truth. The one mismatch
that did occur (§2, `test_sentiment_processor.py`) was caught by this
discipline rather than assumed correct, diagnosed via `diff`, and fully
resolved before moving on.

`test_market_ingester.py`'s Phase 1 Layer 1 addition (246 lines) contained
the same decorative-dash pattern that caused the §2 incident. Caught
**preemptively** this time by grepping the addition for `─` before
writing to live, and simplified the one offending comment line in both
sandbox and live before the `edit_file` call — zero mismatch on the first
attempt, unlike the earlier reactive fix.

## 5. Records updated

- `tests/COUNT_BASELINE.txt`: `1492` (stale) → `1613` (true current count,
  correcting the pre-existing one-off drift from the true `1493` baseline
  in the same update as the tranche's own `+120`).
- `CHANGELOG.md`: new `v1.15.3` entry — Phase 1/Phase 2 summary tables,
  the `market_ingester.py` Layer 1 gap finding, and the
  `SourceLimiters.alphavantage` test-isolation fix, framed as findings
  notes rather than `FIX` subsections since neither represents a
  production-code defect (matching the v1.12.1 precedent's own
  distinction between real bugs found via coverage work — which DID get
  `FIX` subsections and `KNOWN_RISKS.md` entries — and this tranche's
  findings, which don't).
- `KNOWN_RISKS.md`: **not touched**. No open/accepted risk resulted from
  this tranche.
- `pyproject.toml`: `1.15.2` → `1.15.3`. **PATCH** — test-only, zero
  interface/schema/runtime-behavior change, same class as v1.12.1's own
  PATCH precedent for the original coverage tranche.

## 6. What's still open (not this release)

- **Phase 3–5, not started**: Silver (`quality_validator.py` — 110 missed
  lines, the single largest remaining block), Gold (`macro_regime.py`,
  `technical_signals.py`, `pandas_indicators.py` —
  `correlation_matrix.py`/`hmm_regime.py` remain excluded per Ovi's
  standing instruction), and orchestration
  (`job_registry.py`, `runner.py`, `pipeline_logger.py`,
  `health_reporter.py`, `delta_reprocessor.py`).
- Remaining gap to the 95% target: **297 lines** (currently 521 missed;
  need ≤224 to reach 95% of 4476 statements).
- The two stale "PENDING LIVE CONFIRMATION" comments flagged at the start
  of this thread (`bea_ingester.py`'s `LINE_NUMBER_FILTER` comment for
  `trade_balance`'s `LineNumber '15'`, now empirically confirmed live per
  the 17 Aug preflight log; `eia_ingester.py`'s "sandbox has no network
  route" docstring note) — flagged, not fixed, deferred to whichever
  session next touches those two files.
