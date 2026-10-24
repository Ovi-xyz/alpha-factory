# 2026-10-20 — ADR-055 v2: total cutover to the dual-HMM regime engine (v1.22.0)

## Trigger

Ovi: the cutover starts now (E1 — 60 out-of-sample days — is not required); `macro_regime.py`
is superseded because its regime detection is inaccurate; replace it *completely*; the Gold layer
is technical / cross-asset / dual-HMM engines, then specialist intelligence -> decision_xgboost ->
`gold_screener`; phase order adr-050 -> 051 -> 055 -> 052 -> 053 -> 054; give ADR-055 a version bump.
Second pass, same day: `gold_sector` is regime-free without `gold_regime`, so archive it too; the
technical engine is (gold_signals, gold_mtf, signal_aggregation); the Trading Engine does not exist
yet (Gold is unstable and unfinished); v1.22.0 to be reworked before the MCP mirror.

## What explore found (live repo; GitHub `main` clone byte-identical on CHANGELOG, baseline 2504)

- ADR-055 v1's "burn-in instrument (built)" — `scripts/compare_regimes.py`,
  `config/hmm_legacy_mapping.yaml` — is in neither the live repo nor GitHub. Not rebuilt.
- ADR-055 v1's design (projection onto `regime_store.parquet`, 5-state vocabulary, 100-weight
  sector table, shadow store, swap `gold_regime`'s fn) contradicts the structure Ovi gave. v1's D1
  and D3 are superseded.
- Consumers of the legacy regime, verified in source: `mtf_alignment` (two columns),
  `sector_rotation` (weights by regime), `screener` (five columns + sector filter), three
  CrossAssetEngine modules (a string tag), `PITDataLoader.get_regime`, `views.py`,
  `pipeline_dashboard`, the legacy scheduler stub, Makefile, README.
- **Measured hazard:** DuckDB `read_parquet(glob)` over `mtf_alignment_*.parquet` files with
  different column sets raises a schema mismatch; `PITDataLoader.get_mtf_score` swallows it and
  returns `None` for every symbol. Same class as SIL-BRZ-SCHEMA-01. Fixed with
  `union_by_name=true` (+ tests) wherever dated MTF / watchlist files are globbed.

## Decisions

1. **Hard replacement, no projection.** `gold_regime`, `regime_store.parquet`, the 5-state
   vocabulary go. `macro_regime.py` + tests archived (`git mv`) with a rollback README.
2. **`gold_sector` archived (Ovi).** First draft kept it as a regime-free `sector_map.py`; Ovi's
   call: regime-free it has nothing left to do. Archived with `sector_rotation.py`, its tests and
   the weight table + schema. Its one remaining product, the sector *label*, is registry data:
   `signal_aggregation` reads it straight from the InstrumentLoader (`inst.sector or inst.market`,
   the rule the archived job used) for its breadth columns. A first draft shared that read through
   `src/utils/sector_labels.py` with the screener; Ovi then dropped the screener's `sector` column
   (21 Oct), leaving one consumer, so the shared module and its test were removed again. The dead
   `COMMODITY_SUBCATEGORY_TO_WEIGHT_KEY` map in `validate_instruments.py` went with the table.
3. **Technical engine = gold_signals, gold_mtf, signal_aggregation**, regime-free by construction:
   MTF drops `regime_compatible` / `active_regime`; `get_mtf_score` strips them from historical
   files too, so the retired regime cannot reach `gold_xgboost`.
4. **Screener = synthesis tip; HMM context is a soft source.** As-of join, 5-day tolerance, NULL
   when missing/stale/broken (stale and schema failures logged), `context_age_days` and
   `regime_in_sample` carried. `depends_on` is just `gold_mtf`; no edge to an HMM job, so "no
   non-HMM job depends on an HMM job" holds without an exception. `DAILY_SEQUENCE`: technical
   engine, then HMM engine, then screener — an HMM failure cannot stop the technical engine.
5. **No Trading Engine yet.** `gold_screener` is the designated future interface; its schema is
   provisional; nobody to notify; contract formalised when Gold is finished (ADR-054). Wording in
   code, registry, changelog and risks changed from "the interface" to "designated future interface".
6. **Transition signal produced where the regime is.** `context_transition` /
   `context_transition_alert` in `context_regime` (`PROCESSING_VERSION` 1.1), compared with the
   last *known* label.
7. **CrossAssetEngine tags with its own `global_regime_label`** — one shared reader
   (`regime_tag.py`) replaces three identical copies; no HMM read.
8. **Version:** 1.22.0, which also releases the two 1.21.0 "Unreleased" fixes.

## Verification

Sandbox baseline 2488 passed + 15 deselected (poetry env) + 1 polars-version failure = 2504,
matching Ovi's run. After: 2595 collected, 2579 passed, same 15 + 1. Structural pins in
`test_regime_cutover_adr055.py` (retired jobs/modules/literals absent from src, archive complete,
technical engine has no HMM upstream or import, watchlist carries no `sector`, screener reads exactly
one HMM store, daily order, fit jobs weekly-only). Mutants on the new tests: all killed. A slip
caught on the way: a test-file slice that deleted a class header (`TestDomainScoreWeightSumValidation`)
— found because 4 tests failed; every modified test file was then compared to HEAD by class/test
name, and all remaining removals are the intentional retirements.

## Operator runbook (M1)

```
python -m pytest tests/ -q                                   # expect 2595 collected
python src/runner.py --job gold_context_regime --force       # rebuild with the v1.1 columns
python src/runner.py --job gold_mtf --force                  # new-schema MTF file for today
python src/runner.py --job gold_screener --force
python src/runner.py --status
mkdir -p data/archive/regime_store_legacy_2026_10            # optional but recommended
mv data/gold/macro/regime_store.parquet data/gold/sector/sector_regime_weights.parquet \
   data/archive/regime_store_legacy_2026_10/
```

## Open after this release

ADR-052 (labels), ADR-053 (specialists; `hmm_xgboost` needs a PIT accessor for per-domain
posteriors), ADR-054 (decision model; regime x sector; formal Trading Engine contract), the E7 manual
review of regime transitions against known macro dates, a per-domain Gatekeeper scope (RISK-36 item 7).
