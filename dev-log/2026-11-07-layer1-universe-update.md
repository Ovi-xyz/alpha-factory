# 2026-11-07 — Layer 1 universe update: `index_futures`, PARA out, active ceiling 200 (GMI-UNI-001)

**Request.** `us_stocks` removes PARA; new Layer 1 category `index_futures` = YM=F, ES=F, NQ=F, NKD=F;
`ALWAYS_IN_MARKETS = (forex, commodity, index_futures)`; active symbols stay <= 200. Implement against `main`
(commit 4981c7f). No version bump; repo stays at v1.24.0 with the Unreleased entries.

## Explore
Read the loader, both YAMLs and schemas, `validate_instruments.py`, `symbol_utils.py`, the Bronze market ingester,
the Silver processor/aggregator/scope helpers, `active_symbols.py`, the intelligence feature builders and the tests
that pin counts or market lists. Baseline taken from a separate pristine clone BEFORE any edit, with dependencies
pinned from `poetry.lock`: 3333 collected / 3331 passed / 2 environmental failures.

## Findings that shaped the design
1. **`ES` collides with Eversource** (us_stocks, Utilities). Same class as CL / RISK-30. -> registry symbol `ES_F`.
2. **The ceiling held only by coincidence.** `_SCREENED_LIMIT` 175 + 18 forex + 3 commodity + 4 futures = 200,
   with zero headroom, and the always-in market names were duplicated as a literal inside the SQL.
3. **`index` already existed** as a legacy, empty, schema-removed market (ADR-035) and was still the third
   always-in market in the resolver. `index_futures` is a different category; the legacy one was left alone.
4. **Bronze derived non-commodity tickers from the symbol**; for the futures that would have been correct only by
   luck of the suffix table. -> direct routing through `yfinance_symbol`.
5. **The intelligence feature builders keep their own `MARKETS`.** A missing market is a silent null `market_code`.
6. The validator checked duplicates only inside one market, so a cross-market collision could not fail at config
   time.

## Decide
- Symbols `ES_F`/`NKD_F`/`NQ_F`/`YM_F`; raw/yahoo `ES=F` etc.; timezone America/New_York.
- Ceiling as a guarantee: constant + effective limit + post-condition, not a larger or smaller `_SCREENED_LIMIT`.
- Cold-start fallback stays exempt (a one-off pre-backfill state; changing it would alter AS-2).
- `MARKETS` appended, never re-sorted. Validator gets a global uniqueness check.
- Not done on purpose: exclusion of the futures from the CrossAssetEngine, triple-barrier recalibration, any
  live-data step, a PSKY (PARA successor) addition.

## Built
See the CHANGELOG entry. New: `tests/unit/test_layer1_universe_gmi_uni_001.py` (48 tests),
`scripts/preflight/check_index_futures.py`. Changed: registry YAMLs + schemas, loader, validator, symbol_utils,
market_ingester, ohlcv_processor, ohlcv_aggregator, silver_scope (docstring), active_symbols, features_gold,
features_hmm, and 8 existing test files whose pins named the old universe.

## Verification
- Targeted runs after each change; full suite on an isolated snapshot copy. The first full-suite run found a pin
  the targeted runs had missed (`test_silver_scope`, the old market list): I had grepped the old counts but not
  the old market-name list.
- 26 mutants on a throw-away copy (never the working tree): 26 killed, 0 survived.
- A sandbox restart killed one full-suite run mid-way (uptime reset); it was rerun, not interpreted.
- Local commits and a tarball backup outside the repo were taken before mutation testing.

## Operator steps (not done here)
1. Mirror to the live repo, byte-verified file by file.
2. Run `python scripts/preflight/check_index_futures.py` on the M1 (needs Yahoo).
3. Archive PARA's Bronze/Silver files; otherwise AS-10 warns `unknown_market=1` every run.
4. First Bronze run backfills the four new symbols; then Silver, then `python src/runner.py --status`.

## Addendum — live preflight, same day
Ovi ran `check_index_futures.py` on live Yahoo: ES/NQ/YM passed, NKD_F failed (0.397 vs a guessed 0.50). Diagnosed with a
measurement-only script (level ratio, lag table, two-lag R^2, NKD vs ES, jump audit): a timing artefact — CME bar D
contains Tokyo D plus the Europe/US session, so the right shape is lag-aware, not same-date. My own prediction for the
next-day correlation (0.60) overshot the measurement (0.417); the range I gave (0.3-0.6) held. Mistake logged: a
pass/fail threshold shipped without a measured basis, for an instrument whose timing I already knew was awkward.
Fix: gates only on measured/structural facts, information elsewhere, common-calendar returns (the old script also
had that flaw). +17 tests; 13 mutants, one survived (zero-row frame / None), scenario added, all killed.
The 2026-11-05 NKD jump was cross-checked against press reports of the Nikkei's 5-6 Nov selloff.
