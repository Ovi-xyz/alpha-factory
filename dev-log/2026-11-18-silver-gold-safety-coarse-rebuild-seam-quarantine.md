# 2026-11-18 — Silver OHLCV safe for Gold: 1W/1M rebuilt from 1D, reversing-seam quarantine, quarantine-aware gate

**Objective (Ovi, 17 Nov 2026).** Silver OHLCV, code and datasets, safe and clean before Gold, so `technical_xgboost` and `cross_asset_xgboost` are not polluted. **18 Nov: decisions 1, 2, 3 approved as recommended.**

## Trail
1. 17 Nov: read the 15 Nov JSON audit; tested option A ("1D is the truth") against every mismatch class with live files. ALL / CSX: 1W/1M history is the stale vintage; MNST: 1D is stale (not split-adjusted, and Bronze already holds it). A as written would have overwritten the only correct MNST series. Logged as a mistake.
2. Found the consumers from source: `technical_xgboost` reads 1D/1W/1M of five Layer 1 markets; the cross-asset engine reads 1D only; all readers filter `is_clean = TRUE`. Found the ABBV one-session lookahead through the `features_technical.py` availability rule.
3. 17 Nov: shipped the measuring half (audit v2: `basis`, `seam`, `leak`, `last_bar`, explained gaps, gate).
4. 18 Nov: shipped the repair half in the processor: guarded 1W/1M rebuild from 1D (non-FX), data-driven whole-series quarantine of reversing split seams, `rebuilt_from_1d`, version 1.3. The audit now imports the processor's seam / basis rules and its gate treats a quarantined series as not blocking.
5. Tests found three real design points: a window shift on a trending series mimics a constant ratio (basis check now ignores series that reconcile on their own window); the eager `read_parquet` pin (SIL-RPQ-001) forbids the obvious daily read (a lazy scan is used); two old fixtures put incoherent 1W bars next to 1D bars (their assertions were updated, intent kept).
6. Real-data check before finishing: healthy AAPL 1W untouched (0 of 794), BBCA 1W 6 bars moved by 0.32%, ES_F 1M 17 months filled, AAPL 1M Aug/Sep stale bars fixed (hidden by the audit's 0.95 threshold), MNST 1M left alone, MNST 1D 0 of 2,510 rows clean.

## Not done, and why
- The ADR-052 labeler ignores `is_clean` on purpose, so MNST labels are still built from glitched bars: a follow-up in `scripts/label_triple_barrier.py` (skip series with no `is_clean` row). This is the remaining pollution path into `technical_xgboost`.
- FX waits for the weekend-stub flag (GHR-11). The `is_clean` split (XAE-R06) stays the next package, a prerequisite for both models.
- MNST history re-fetch from the vendor: the Bronze ingester was not read.
- MNST 1W, MNTS, ODFL, eight dividend symbols and the full live Silver tree were not opened.

## Rollout (proposed)
Back up `data/silver/market_ohlcv` -> run the suite (3831 collected) -> `silver_ohlcv --force`, `silver_ohlcv_context --force`, `silver_validate` -> audit with `--format json`, read `gate` -> Gold -> `--status`. Rollback: restore the backup, revert the patch.
