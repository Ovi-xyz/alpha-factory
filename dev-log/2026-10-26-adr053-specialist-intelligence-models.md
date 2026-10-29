# 2026-10-26 — ADR-053 specialist intelligence models (v1.24.0)

Phase order: adr-050 → 051 → 055 → 052 → **053** → 054. Explore → Decide → Implement; Decide was a hard
stop (the ADR was not implementable as written) and the five decisions below were accepted as recommended.

## Explore (live data, read-only copies of the stores)

| # | Finding | Evidence |
|---|---|---|
| F1 | Stored Gold history cannot fit anything | `mtf_alignment` 3 files, `signal_aggregation` 3, `global_regime` 3 rows, CrossAssetEngine history 2 weekly snapshots |
| F2 | HMM stores are in-sample | macro 7,862/7,862 rows `in_sample`, market 2,541/2,542 |
| F3 | `get_signals` cannot reach `tech_signals` history; VIX columns are run-day constants | `signal_date` = 2026-10-18 on all 141,645 weekly rows; `vix_at_signal` 19.99 / `high_volatility_flag` False on all of them |
| F4 | Weekly/monthly bars are stamped at period start | every weekly stamp a Monday (708 Tuesday after holidays) |
| F5 | ADR-052 pin vs a weekly fit job that needs labels | isolation test forbids any mention of the labeler in gold/scheduler |
| F6 | Gatekeeper has no per-domain scope | `SCOPES = (bronze, silver, gold)`; RISK-36 #7 |

## Decisions (D1–D5, accepted 26 Oct)

D1 target = ADR-052 `label_binary`, own-domain features only · D2 one as-of builder for fit and predict (HMM
walk-forward history; completed bars) · D3 labels handed over as Parquet (`--publish`), fit never imports the
labeler · D4 `hmm` / `technical` / `cross_asset` Gatekeeper scopes over the INPUT chain · D5 Wave 1 =
core + hmm_xgboost + gold_xgboost; cross_asset_xgboost after a backfill-cost spike.

## Implement — notable moments

- **The tests found two real bugs before any data did.** (1) Brier score with a fractional (aggregated) label is
  `p² − 2p·ȳ + ȳ`, not `(p − ȳ)²` — the exactness test (weighted vs replicated rows) failed; fixed. (2) Two fits of
  identical data differed: a `group_by`-built frame has no stable row order and XGBoost's row subsampling is
  positional; every frame is now totally ordered, with a shuffle regression test.
- **Honest regime history is much weaker than the stored one.** Walk-forward macro (4,120 rows, 64 folds, 291 s)
  agrees with the stored label on 64% of days; all 261 days of 2013 are `recession` (stored: `recovery`). Market
  (2,022 rows, 33 folds, 44 s) agrees on 86%. Recorded in RISK-40 #2; not a defect.
- **Cross-module collisions were caught by existing pins, not by review:** the ADR-052 isolation test flagged the
  labeler's file name in a docstring and a job comment; the ADR-051 independence test flagged the new HMM-named jobs
  (HMM_JOBS extended); gatekeeper/replay tests assumed 3 scopes (now 3 + domains).
- **Mutants killed:** purge that purges nothing → `LeakageError`; `timestamp`-stamp availability instead of the
  completed-week rule → feature differs; HMM import / technical-store literal / domain literal in core → three
  independence tests fail.
- **Real-file validation (1W, 1M copies):** all 141,645 weekly bars become available the following Monday; a
  Wednesday decision sees the previous week. The 100 MB `tech_signals_1D` could not be copied through the MCP
  connector (timeout) — the 1D branch (availability = bar date + 1) is covered by synthetic tests only.

## Verification

Sandbox clone of GitHub `main` at v1.23.0 (baseline collected 2,711). After: 2,888 collected, 2,872 passed,
15 deselected (poetry env), 1 environmental failure identical on pristine `main`
(`test_ohlcv_processor::...Utf8Fallback`, polars version).

## Operator runbook (after the live mirror)

```
brew install libomp && poetry lock                      # xgboost is new
python -m pytest tests/ -q                              # expect 2888 collected
python scripts/label_triple_barrier.py --grid-all --years 15 --step 1 --publish
python src/runner.py --job gold_hmm_walk_forward --force      # ~6 min
python src/runner.py --job gold_hmm_xgboost_fit --force
python src/runner.py --job gold_gold_xgboost_fit --force      # first run: whole OOF history, slow
cat data/gold/intelligence/hmm_xgboost/validation.json data/gold/intelligence/gold_xgboost/validation.json
python src/runner.py --job gold_hmm_xgboost_predict --force
python src/runner.py --job gold_gold_xgboost_predict --force
python src/runner.py --job gatekeeper --force                  # now 6 scopes per day
```

## Open

RISK-40 (1–12). ADR-054 must read `oof_predictions.parquet` (insert-only, causally calibrated `p`), not the
in-sample production stores. GMI-PIT-002: `PITDataLoader.get_signals` is misleading for history.
