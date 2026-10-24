# archive/macro_regime_cutover_2026_10/

ADR-055 v2 (GMI-HMM2-CUT-001) — archived, not deleted. Rollback = `git mv` the files back
(paths mirror their original location under the repo root) and restore the registry entries
listed below.

## What's here

- `src/gold/macro_regime.py` — `MacroRegimeDetector`, the rule-based 5-state regime classifier
  (RISK_ON / RISK_OFF / STAGFLATION / REFLATION / DISINFLATION) and producer of
  `data/gold/macro/regime_store.parquet`. Job: `gold_regime`.
- `src/gold/sector_rotation.py` — `REGIME_SECTOR_WEIGHTS` and the regime -> sector-weight
  selection; producer of `data/gold/sector/sector_regime_weights.parquet`. Job: `gold_sector`.
- `config/regime_sector_weights.yaml` + `config/schemas/instruments/regime_sector_weights.schema.yaml`
  — the hand-set table of 5 regimes x 20 sector keys = 100 weights and its schema.
- `tests/unit/test_macro_regime.py`, `tests/unit/test_macro_regime_gld002.py`,
  `tests/unit/test_sector_rotation.py` — their tests (GLD-002, the DXY-score finding, is retired
  with the module, not lost).

## Why they were retired

1. `macro_regime.py`'s regime detection results were judged inaccurate (a single-axis priority
   tree that mixes market behaviour — RISK_ON/OFF — with the macro environment —
   STAGFLATION/REFLATION/DISINFLATION; a missing input silently became a neutral default).
2. The platform's regime is the dual-HMM engine (ADR-051, `src/gold/hmm/`): macro environment and
   market behaviour as two separate states, joined in `context_regime.parquet`.
3. `gold_sector` only ever did one thing: pick sector weights by the legacy regime. Without
   `gold_regime` it is regime-free, and a regime-free sector job has nothing left to compute (its
   one remaining product, the sector label, is registry data). It was not re-keyed on the HMM
   regime: the technical engine may not read the HMM stores (domain independence), and ~240 new
   weights would have no evidence behind them.
4. The Gold layer is four engines feeding `gold_screener` (technical = gold_signals, gold_mtf,
   signal_aggregation; cross-asset; dual-HMM; specialist/decision intelligence). None of them has
   a place for a rule-based regime or a regime-keyed sector table.

## What replaced each consumer

| Consumer of the retired code | After the cutover |
|---|---|
| `mtf_alignment` (`regime_compatible`, `active_regime`) | removed — the technical engine is regime-free |
| `sector_rotation` (regime -> sector weights) | nothing — job and weights retired |
| sector label (`signal_aggregation` breadth grouping) | read straight from the InstrumentLoader inside `signal_aggregation` (`inst.sector or inst.market`) — no Gold store |
| screener `sector` column | dropped (Ovi, 21 Oct 2026) |
| `screener` (regime, regime_composite, regime_confidence, regime_transition, transition_alert, sector_weight_adj, sector) | joins `context_regime` as-of (macro_label, market_label, context_label, context_transition, ...); no sector weight |
| CrossAssetEngine correlation / lead-lag / forecast (regime tag) | their own `global_regime_label` (`cross_asset/regime_tag.py`) |
| `PITDataLoader.get_regime`, `views.v_macro_regime`, `views.v_sector_weights`, dashboard | `get_context_regime`, `v_context_regime`, (no sector view), context_regime path |

## Registry entries removed (for rollback)

- `JOB_REGISTRY["gold_regime"]` (fn `src.gold.macro_regime.run`, depends_on `silver_macro` +
  `silver_validate` with `stale_tolerance` for `silver_macro`, est 5 min).
- `JOB_REGISTRY["gold_sector"]` (fn `src.gold.sector_rotation.run`, depends_on `gold_regime`, est 2 min).
- Their `DAILY_SEQUENCE` slots (`gold_regime` between `gold_mtf` and `gold_sector`; `gold_sector`
  before `signal_aggregation`) and `gold_screener`'s `depends_on` on both.
- `scripts/validate_instruments.py`'s `COMMODITY_SUBCATEGORY_TO_WEIGHT_KEY` (a guard against a
  commodity subcategory with no key in the weight table; the enum check is the whole guard now).
- `src/gold/views.py`: `v_macro_regime`, `v_sector_weights`.
