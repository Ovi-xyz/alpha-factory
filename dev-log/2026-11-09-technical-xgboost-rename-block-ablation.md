# 2026-11-09 — `technical_xgboost`: rename, optional feature blocks, ablation harness, intraday probe (ADR-053 Amendment 2)

**Request.** Ovi: `gold_xgboost` never sees `tech_signals_{1H,4H}`; split it into `intraday_xgboost` (1H/4H) and rename the
1D/1W/1M model `interday_xgboost` if it cannot read `mtf_alignment` / `signal_aggregation`. Evaluate, then build a patch
from the suggested path. Implemented against GitHub `main` (shallow clone, `06f8166`); the files touched were
byte-identical to the live repo before editing (`core.py`, `job_registry.py`, `technical_signals.py` diffed). No version bump.

## Explore
Read `gold_xgboost.py`, `features_gold.py`, `core.py`, `config/intelligence.yaml`, the three ADR-053 test modules, the
gate-domain and domain-independence pins and the job registry; grepped every spelling of the old names across `src/`,
`scripts/`, `config/`, `tests/` and README (names, job keys, tuples, a test parametrize id). Listed `data/gold`: no
`intelligence/` directory, so the specialist has never run. Read the schema of the live `tech_signals_1M.parquet`.
Measured the real Silver 1H/4H files of AAPL, BBCA and EUR_USD. Baseline from a separate pristine clone BEFORE any
edit, dependencies pinned from `poetry.lock`: 3428 collected / 3426 passed / 2 environmental failures.

## Findings
1. Never run live, so the rename moves no lineage.
2. The registry names stuttered (`gold_gold_xgboost_*`) and the model is the only one off the `<domain>_xgboost` pattern.
3. `validate_config` accepts any key under `models:`: a drifting name silently loses the technical `min_child_weight` override.
4. `initial_train_months` = 36 gives zero folds for a ~2-year history; 12 months gives 4-5 folds (10.5-12.5 months).
5. The builder reads no `open/high/low`; the live 1M file has them (and `tr`, `atr_14`, `volume`).
6. Intraday conventions differ per market and 4H is anchored differently from 1H; the Gold copy is in one timezone.

## Decide
- Rename now to `technical_xgboost` (domain-keyed), not `interday_xgboost` (timeframe-keyed).
- Do not split. Intraday joins as an optional block, admitted only by a same-window ablation through the production engine.
- Build the cheap, deep proxy (`candle`) and keep it OFF. Do not write an intraday builder before the probe has measured the files.

## Implement (four local commits, each independently revertible)
P1 rename (all-or-nothing script, every anchor count asserted) + five rename guards; P2 feature blocks (`base` hash pinned
to the pre-change value) + 11 block tests; P3 ablation script + 31 unit tests + 23 AST isolation pins; P4 probe script.

## Verify
Full suite on the patched tree, then the patch applied to a fresh clone and the suite run again (see CHANGELOG for counts).
Mutation checks ran on a throw-away copy only. Survivors were analysed before adding tests: two missing scenarios
(the verdict rule in the mirror direction; a near-zero ATR), one unasserted sign, one unpinned interval level, and one
equivalent mutant (a window filter redundant with the row intersection), which was deleted instead of tested.

## Not done
Anything on live data; the live `tech_signals_1D` schema; the probe on live Gold; an intraday builder; ADR text edits
(the ADRs live outside the repo; Amendment 2 text delivered separately).
