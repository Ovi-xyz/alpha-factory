# 2026-11-20 - Silver OHLCV: the 18 Nov quarantine and rebuild guard were too broad (SIL-SEAM-SCOPE-01, SIL-OHLC-POSITIVE-01, SIL-AUDIT-OHLCV-03)

Status: code and tests mirrored to the live repo and byte-verified; no live Silver or Gold data rewritten; rollout not run.
Version: `CURRENT_SILVER_VERSION` 1.3 -> 1.4, `pyproject.toml` unchanged (1.24.0). Baseline 3831 -> 3877.

## Trigger
Ovi, 19 Nov: `silver_audit_2026-11-19.json` (the audit re-run after the 18 Nov patch) shows Silver OHLCV is not ready to be promoted to Gold and the problem remains unresolved. Gate: `technical_xgboost` BLOCK (CL 1W/1M `ohlc`; MNTS 1W `anchor` + `leak`), `cross_asset_xgboost` ATTENTION.
Decisions, 20 Nov: D1 apply the changes; if the blast radius of the Silver OHLCV code is large, rewrite Silver OHLCV from Bronze after the fix. D2 `technical_xgboost` stays skipped (fit `skipped`, `LabelsMissing`) until the FX window rule is settled.

## What was done, in order
1. Read the JSON (4,258 findings): 39 FAIL, 274 WARN. The headline FAIL count rose against 15 Nov only because the seam, basis and leak checks are new; on the checks present in both, anchor FAIL 16 -> 4, current_period FAIL 13 -> 2, gaps WARN 22 -> 16, window WARN 76 -> 51.
2. The gate blocks on two symbols, but 27 coarse series were quarantined by the processor (`n clean = 0`) and counted as handled. Read `ohlcv_processor.py` and `audit_silver_ohlcv.py` to see how: `_quarantine_reversing_seams` ran on 1D, 1W and 1M.
3. Tested the rule on real Silver files (CVNA, MARA, CLSK, MNST): CVNA 1D has no seam and its 1M equals the 1D month-end closes; MARA 1W reconciles 532/532 with 1D but 0/760 clean; MNST 1D is the true positive (7/7 jumps reverse within days). At 1D the rule fires on 1 of 654 series.
4. Dead end: volume continuity as a split-vs-move discriminator. On the MNST control the glitch shows no inverse volume step, so it was dropped before any code.
5. Read the rebuild guard: `find_split_seams(...)` non-empty skips the whole rebuild. 38 symbols have a 1D seam; NG and MNTS are the two non-FX symbols with remaining anchor / fragment defects and both are among them. On MARA and CLSK the vendor 1W/1M agree with 1D across every seam date and the Aug/Sep 2026 1M bars are frozen (2 of 122, up to 14%), invisible to the 0.95 window threshold.
6. `_flag_is_clean` has no price > 0 test; that is why CL 1W/1M (April 2020, low <= 0) stayed `is_clean = TRUE`.
7. Implemented in a sandbox clone of GitHub `main` (a67007b) plus the live 18 Nov files; wrote tests; ran 38 mutants; ran the full suite before and after; ran the fixed code on real files; mirrored five files to live and byte-verified each.

## Findings while building
- A coarse series that equals a quarantined 1D carries its wrong basis (MNST 1W) and must be quarantined; one that differs from it is the right side (MNST 1M). "Scope the quarantine to 1D" alone would have let MNST 1W through, so inheritance by reproduction (>= 95% of periods equal the 1D close) was added. The first draft of the proposal did not have this and was corrected before any code.
- The audit's `Options.checks` default was `()`. A real-file audit run through the Python API judged nothing and the gate printed PASS; caught because zero findings on a tree that should hold persistent-seam WARNs was implausible. `Options` now defaults to every check and rejects an empty selection.
- My first real-file simulation fed already-processed Silver files to the rebuild (stale `is_clean`, extra `rebuilt_from_1d` column) and produced a misleading "0 -> 0 clean". Redone through `process_symbol` on a Bronze-shaped frame, as `run()` does.
- Operator trap: `--force` bypasses dependency checks only; the per-`run_date` ProgressCheckpoint is not cleared and CLI `--reset` clears sentinels, so a same-`run_date` Silver rerun skips every finished symbol. The 18 Nov rollout line said only `--force`.

## Results (sandbox)
- Full suite: 259 failures before and after, identical set (0 only-before, 0 only-after; environment), 3,618 passed (= 3,572 + 46).
- Mutants: 38 (20 processor, 18 audit), all killed; three survivors at first, closed with tests.
- Real files through `process_symbol` + rebuild: MARA 1W 0 -> 750 clean of 760; MARA 1M 0 -> 174 of 175 (Sep close 12.92 -> 11.33); CLSK 1M 0 -> 120 of 121; CVNA 1M 0 -> 115 of 116; NG 1M 3 replaced + 17 filled (last bar 3.198 = last daily close); MNTS 1W 345 replaced (Wednesday stamps 346 -> 1); CL 1W/1M price <= 0 bars unclean; MNST 1D stays quarantined (0 of 2,564).
- Full audit over those seven symbols (16 files): the only FAIL is MNST 1D; both gates ATTENTION, 0 FAIL.

## Files changed (live)
`src/silver/ohlcv_processor.py`, `scripts/audit_silver_ohlcv.py`, `tests/unit/test_silver_coarse_rebuild.py`, `tests/unit/test_audit_silver_ohlcv.py`, `tests/unit/test_silver_seam_scope.py` (new), `tests/COUNT_BASELINE.txt`, `CHANGELOG.md`, `KNOWN_RISKS.md`, this file.

## Open
- D3: persistent-seam WARNs (real moves) count as ATTENTION, so PASS is unreachable on live data. Recommendation: informational.
- FX 1W/1M (GHR-11); the ADR-052 labeler ignores `is_clean`; XAE-R06; MNST history re-fetch.
- About 30 further symbols are now rebuilt but were not opened.
- Rollout not run: back up `data/silver/market_ohlcv`, suite, `silver_ohlcv --force` (new `run_date` or cleared checkpoints), `silver_ohlcv_context --force`, `silver_validate`, audit `--format json` (`meta`, `gate`, `quarantined_count` expected 1), Gold, `--status`.
