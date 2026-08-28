# 2026-08-22 — Bronze Timeframe Partition, MTF Score Coverage (Path C), AU/AG Ticker (ADR-045, ADR-046, ADR-047)

**Version**: 1.17.0 → 1.17.1
**Trigger**: Ovi's direct instruction — "Based on the GMI v11, continue with
implementation phase to finish outstanding issues in sequence" —
implementing `GMI_Decision_Document_v11.docx` Section 2/3 in full, plus a
mid-session pivot to ADR-046 Path C once Ovi chose it explicitly and
supplied the exact recalibrated grade thresholds. Reconciled into a single
version bump per Ovi's explicit instruction ("Reconcile all fixes inside
this sandbox into a version bump"), since nothing had been mirrored to the
live repo before that instruction arrived.
**Scope**: `src/bronze/market_ingester.py` (ADR-045, ADR-046 Path C,
ADR-047 — three separate edits in one file); `src/silver/ohlcv_processor.py`
(consequential Silver-side companion fix to ADR-045, approved by Ovi
mid-session); `src/gold/mtf_alignment.py`, `src/gold/technical_signals.py`,
`src/gold/screener.py`, `src/scheduler/job_registry.py`,
`src/backtest/engine.py`, `src/config/pipeline_config.py` (ADR-046 Path C
recalibration); 6 test files updated
(`tests/unit/test_market_ingester.py`, `tests/unit/test_ohlcv_processor.py`,
`tests/unit/test_mtf_alignment.py`, `tests/unit/test_screener.py`,
`tests/unit/test_backtest_engine.py`, `tests/unit/test_pipeline_config.py`,
`tests/integration/test_full_system.py`); `pyproject.toml`, `CHANGELOG.md`,
`tests/COUNT_BASELINE.txt`.

---

## 0. Pre-work verification

- Read the live repo directly via the Filesystem MCP connector
  (`/Users/opi/alpha-factory`) before writing anything — confirmed
  `market_ingester.py`, `ohlcv_processor.py`, `symbol_utils.py`,
  `pyproject.toml` byte sizes matched a fresh `github.com/Ovi-xyz/
  alpha-factory` clone at `main` (`3ab3a7d`, v1.17.0) exactly, so the
  sandbox clone was a faithful starting point despite the live repo
  having uncommitted GMI-BRZ-001/GMI-SIL-001/GLD-ACTIVE-001/GLD-L2-01/
  GMI-GLD-001/GLD-MTF-COV-01 work at first glance — all of it turned out
  to already be on `main`.
- Baseline confirmed empirically: installed `poetry` (missing from the
  bare sandbox, caused 2 unrelated test failures on the first run,
  resolved by installing it rather than touching test code) and re-ran
  the full suite: **1510 passed, 0 failed** — exact match to
  `tests/COUNT_BASELINE.txt`.
- Read `instrument_loader.py` and `config/instruments_identity.yaml`
  directly to confirm AU/AG's real `yfinance_symbol` values (`GC=F`,
  `SI=F`) before touching any code — matches ADR-047's own finding
  exactly.

## 1. ADR-045 — Bronze OHLCV write/scan path gains a timeframe partition

`market_ingester.py::_run_symbol()` passed a symbol+market+source-scoped
`bronze_path`/`asset_class` (no timeframe dimension) to both
`IncFetchProtocol.resolve_start_date()` and `BronzeIngester.write()`.
`DEFAULT_TIMEFRAMES=[1D,1W,1M]` processes 1D first; its freshly-written
file made the read-side scan report a near-today `last_date` for 1W/1M
(trivial ~7-day fetch instead of a multi-year cold-start backfill), and
the write-side same-day idempotency check (FIX GD-F08) then found 1D's
file and skipped 1W/1M's write entirely — confirmed via a direct read of
`base_ingester.py`, matching the exact `"[Bronze] Idempotent skip"` log
line from the 21 Aug 2026 live-test bug report GMI v11 §1.2 cites.

Fix: timeframe folded into both paths at the call site only
(`asset_class=f"market/ohlcv/{market}/timeframe={tf}"`) — the shared
`BronzeIngester`/`IncFetchProtocol` base-class signatures are unchanged,
since every non-OHLCV Bronze domain (FRED/BLS/BEA/Treasury/IMF/BIS) uses
them too and has no timeframe concept. Applied identically to
`_run_context_symbol()` (Layer 2 / GMI-BRZ-001) — not named in ADR-045's
own Consequences (written against `_run_symbol()` only), but structurally
identical bronze_path/asset_class construction meant the identical
starvation bug applied there too. Flagged to Ovi as a consequential
finding; approved before mirroring.

## 2. Consequential finding — Silver's Bronze-read glob was timeframe-blind

While verifying ADR-045 would actually fix the reported symptom, read
`ohlcv_processor.py`'s Bronze-read glob (`run()` PASS 1 and
`run_context()`) and found it has never filtered by timeframe at all —
`market/**/symbol={symbol}/**/*.parquet`, no `tf` segment anywhere.

Reproduced empirically rather than reasoned about in the abstract, per
this project's own convention: copied the live repo's actual
`AAPL_raw_20260820_211330.parquet` (via
`Filesystem:copy_file_user_to_claude`) into the bash sandbox, rebuilt the
exact directory structure it lives at
(`data/bronze/market/ohlcv/us_stocks/source=yfinance/symbol=AAPL/
year=2026/month=08/`), and ran the pre-fix glob pattern against it for
every `tf` in `_RUN_BRONZE_TFS`:

```
tf=5m   -> MATCHED, 2512 rows, 2016-08-23 to 2026-08-20
tf=15m  -> MATCHED, 2512 rows, 2016-08-23 to 2026-08-20
tf=1H   -> MATCHED, 2512 rows, 2016-08-23 to 2026-08-20
tf=1D   -> MATCHED, 2512 rows, 2016-08-23 to 2026-08-20
tf=1W   -> MATCHED, 2512 rows, 2016-08-23 to 2026-08-20
tf=1M   -> MATCHED, 2512 rows, 2016-08-23 to 2026-08-20
```

Identical match for every timeframe — confirming that on the very first
`silver_ohlcv` run (Silver has never actually been run in this repo; no
`data/silver/` directory existed before this session — confirmed via
`Filesystem:list_directory` on `data/`), every declared `_RUN_BRONZE_TFS`
entry would have silently written the same 2,512 daily-cadence rows under
a different timeframe label.

This also meant ADR-045's own Bronze-side fix would have made things
*worse*, not moot, if shipped alone: once Bronze genuinely separates
1D/1W/1M (and 1H, once ADR-046 Path C landed), this glob's recursive `**`
would union all of them back together into one blended multi-cadence
series per Silver TF file, instead of scoping to the one Bronze partition
that actually matches.

Flagged this explicitly to Ovi mid-session ("Given the magnitude here —
this touches core Silver read logic universe-wide") rather than folding
it in silently, given the blast radius (every symbol, every timeframe,
retroactively). Ovi's response: "Continue with core Silver read logic is
allowed." Fixed in the same pass: `timeframe={tf}` segment added to both
glob sites in `ohlcv_processor.py`, matching Bronze's new partition
structure. New regression test:
`test_run_does_not_blend_timeframes_across_bronze_partitions` — two
differently-sized Bronze fixtures at 1D (40 rows) vs 1W (5 rows) must
produce two differently-sized Silver outputs, not an identical union.

This finding directly fed ADR-046's own calculus, reported to Ovi
alongside the path options: even the old "3 working timeframes"
(1D/1W/1M) were never 3 independent votes pre-fix — Silver's glob bug
meant all three would have read the identical Bronze blob, so
`mtf_score`'s only non-zero values would ever have been -3/0/+3.

## 3. ADR-046 — MTF score coverage, Path C (Ovi's explicit choice)

GMI v11 presented three repair paths without recommending one. Ovi chose
Path C directly ("continue with path C") and supplied the exact
recalibrated grade thresholds in the same instruction: "sum of trend
direction per timeframe (-5 to +5) ... grade A (|score|>=4), grade B
(|score|==3), grade C (|score|<=2), grade D removed."

**Wiring 1H into Bronze (Layer 1 only):** added
`LAYER1_TIMEFRAMES = DEFAULT_TIMEFRAMES + ["1H"]` to `market_ingester.py`,
applied only at `job_registry.py::_bronze_ohlcv()`'s instantiation site —
`_bronze_ohlcv_context()` (Layer 2) is a *separate* `MarketOHLCVIngester()`
instantiation in the same file, left on plain `DEFAULT_TIMEFRAMES`
deliberately, per `run_context()`'s own docstring stating no Layer 2
consumer needs intraday context data this cycle. Verified before wiring,
not assumed: `FALLBACK_YEARS["1H"] = 2` (inc_fetch.py) was already the
correct ~2-year yfinance ceiling per ADR-046's own Consequences, and
`yfinance_adapter.py`'s `_INTERVAL_MAP` already had `"1H": "1h"` — no
changes needed to either.

**Score range and grade recalibration** (both `technical_signals.py` and
`mtf_alignment.py` must agree — the former only computes
`tech_signals_{TF}.parquet` for TFs in its own `TIMEFRAMES` list, the
latter only reads what exists):
- `TIMEFRAMES` trimmed from the 7-entry
  `["5m","15m","1H","4H","1D","1W","1M"]` to the 5 real contributors
  `["1H","4H","1D","1W","1M"]` — 5m/15m removed entirely rather than left
  as permanent always-0 padding, which is the exact "wired but silently
  zeroed" shape GMI v11 investigated in the first place.
- Grade thresholds: A `|score|>=4`, B `|score|==3`, C `|score|<=2`
  (catch-all `otherwise` branch — D removed from the `pl.when/then` chain
  entirely, not just renamed).
- `get_mtf_summary()`'s returned dict no longer has a `grade_D` key.
- `screener.py`'s `MIN_MTF_SCORE`: 5 → 3, preserving the original design
  relationship (the old 5 matched grade B's exact boundary; 3 matches the
  new grade B's exact boundary the same way).

**Downstream consumers of the old scheme, found via a full-repo grep for
`signal_quality`/`mtf_score`/`A/B/C/D`/`-7 to +7`, not left to surface
later as silent staleness:**
- `src/backtest/engine.py`: `BacktestConfig.min_mtf_score` default 5 → 3;
  the quality-degradation exit condition (`quality == "D"`) and the
  `mtf.get("signal_quality", "D")` default sentinel both updated to `"C"`
  — grade D no longer exists, C is the new "weak" catch-all that D used
  to be.
- `src/config/pipeline_config.py`: `min_mtf_score_screener` default 5 → 3
  for consistency, though confirmed via grep this field currently has no
  source-code reader — `screener.py`'s own `MIN_MTF_SCORE` module
  constant is what's actually wired into the query.
- `src/scheduler/job_registry.py`: stale job description string
  `"MTF alignment — score -7..+7, signal quality A/B/C/D"` corrected;
  `bronze_ohlcv_daily`'s description and `est_minutes` (35 → 42) updated
  for the added 1H fetch (~639 symbols × 0.6s throttle ≈ 6.4 extra
  minutes).

## 4. ADR-047 — AU/AG commodity ticker routes through `inst.yfinance_symbol`

`_run_symbol()`'s commodity branch now reads `inst.yfinance_symbol`
directly for `market=='commodity'`, mirroring `_run_context_symbol()`'s
already-established Layer 2 pattern — rather than
`to_api_symbol(inst.raw_symbol, inst.market, primary_src)`, whose
commodity branch has no override table and falls through to a generic
`sym + "=F"` suffix rule, producing `AU=F`/`AG=F` (invalid; confirmed
against `config/instruments_identity.yaml`'s real `GC=F`/`SI=F` values via
`grep`). `CL=F` was already correct by coincidence and is unaffected.
`to_api_symbol()` itself is unchanged — call-site fix only, per ADR-047's
own Rejected alternative (a commodity override table inside
`to_api_symbol()` would reintroduce the dual-source-of-truth shape that
plausibly caused the bug in the first place).

## 5. Test suite changes

All changes verified against a real, run test suite at every step — no
threshold or fixture value was written without first confirming the
actual code path it exercises.

- `test_market_ingester.py`: fixed one stale `asset_class` assertion;
  added `TestRunSymbolADR045TimeframePartition`,
  `TestRunContextSymbolADR045TimeframePartition`,
  `TestRunSymbolADR047CommodityTicker` (11 new tests total covering both
  ADRs, including regression guards that 1D/1W/1M resolve to 3 distinct
  bronze_paths and that non-commodity markets are unaffected by ADR-047).
- `test_ohlcv_processor.py`: `_write_bronze_fixture`/
  `_write_bronze_context_fixture` helpers now require an explicit
  `timeframe` parameter and write to the new partitioned path — this is
  the corrected version of what used to be a single shared fixture
  (implicitly encoding the pre-fix bug: one Bronze blob satisfying all 6
  declared timeframes). Added
  `test_run_does_not_blend_timeframes_across_bronze_partitions`
  (regression guard for the consequential finding in §2).
- `test_mtf_alignment.py`: `TestMTFScoreLogic` and
  `TestComputeMtfAlignmentScoring` substantially reworked for the new
  5-timeframe, -5..+5, A/B/C-only scheme — every hand-duplicated `_grade()`
  helper test and every real-`_compute_mtf_alignment()` fixture test
  rewritten with scores/timeframes that are actually reachable under Path
  C (e.g. `test_all_five_tf_bullish_score_5_grade_a` replaces
  `test_all_seven_tf_bullish_score_7_grade_a`; the malformed-file test now
  corrupts `tech_signals_4H.parquet` rather than `tech_signals_15m.parquet`,
  since "15m" is no longer in `TIMEFRAMES` and would never be attempted).
  `TestGetMtfSummaryFullPath` updated to drop `grade_D` from the expected
  dict.
- `test_screener.py`: 4 fixture rows updated — 2 that literally used the
  now-nonexistent grade `"D"` (changed to `"C"`), 2 whose comments/values
  assumed the old `MIN_MTF_SCORE=5` threshold.
- `test_backtest_engine.py`, `test_pipeline_config.py`: default-value
  assertions updated (5 → 3).
- `test_full_system.py`: `test_l6_mtf_grade_complete_coverage` rewritten
  for the `range(-5, 6)` domain and A/B/C-only grade set.

**Result**: `pytest tests/ -q` → **1521 passed, 0 failed** (1510 baseline
→ +11: 6 new in `test_market_ingester.py`, 1 regression test in
`test_ohlcv_processor.py`, +1 net across `test_mtf_alignment.py`'s
grade-scheme rework after accounting for renames). Coverage: **88.04%**
(gate 80%); all touched source files individually at 100% except
`technical_signals.py` (85%, pre-existing uncovered branches unrelated to
this session) and `ohlcv_processor.py` (90%, likewise pre-existing).
Syntax validation (`ast.parse` on every `.py` file), f-string-SQL
anti-pattern grep, and `scripts/validate_instruments.py` (699 symbols,
Layer 1=639, Layer 2=60) all clean.

## 6. Version reconciliation

Ovi's instruction: "Reconcile all fixes inside this sandbox into a version
bump." Since nothing from this session had been mirrored to the live repo
before that instruction, all three ADRs plus the consequential Silver fix
were folded into a single `pyproject.toml` v1.17.1 changelog comment
block (rewritten from an earlier, narrower ADR-045/047-only draft written
before Ovi chose ADR-046's path) and a single `CHANGELOG.md` entry, rather
than fragmenting into 1.17.1 (ADR-045/047) + 1.17.2 (ADR-046). PATCH bump
throughout: all three ADRs are bug fixes to existing broken components,
with no Interface Contract (GD §0.4/§17.6) or Silver/Gold output schema
change — the watchlist's column schema is unchanged; only which rows
clear the filter changes.

One editing bug caught and fixed during this reconciliation: the first
`str_replace` against `pyproject.toml`'s version-comment header matched
only through the word "Finnhub" in the pre-existing v1.17.0 comment's
opening sentence, leaving that sentence's continuation ("retired in
full...") orphaned directly below the new v1.17.1 block with no subject.
Caught by re-reading the file after the edit rather than assuming success
from the tool's "successfully replaced" confirmation; fixed by restoring
the missing lead-in line.

## 7. Not decided / explicitly out of scope this pass

- Pre-existing Bronze OHLCV data written under the pre-ADR-045
  non-partitioned path is left as-is. Confirmed via direct Filesystem MCP
  inspection that only 1D data has ever actually persisted in the live
  repo (`source=yfinance/symbol=AAPL/year=2026/month=08/`, 2,512 rows,
  2016-08-23 to 2026-08-20) — the new `timeframe={tf}` partition starts
  genuinely empty for every symbol on the next run, no explicit migration
  needed. Quarantine-vs-leave-as-is for that pre-fix data remains Ovi's
  call, per ADR-045's own Consequences.
- `BronzeIngester.write()`'s idempotency check still keys off
  `datetime.utcnow()` rather than the `run_date` parameter — flagged in
  ADR-045's own Consequences as a separate, narrower issue (only surfaces
  during `--date` backfill), not addressed this pass.
- 5m/15m remain permanently unfetched by design (Path C, not Path A) —
  grade A under the new scheme (`|score|>=4`) is reachable at 4-of-5
  contributors agreeing, not requiring unanimous 5-of-5.
