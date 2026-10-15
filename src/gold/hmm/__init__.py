"""
src/gold/hmm — ADR-051 (GMI-HMM2-001): Dual-HMM Regime Engine.

Five independent modules, each with its own store and job, handing off only
through Parquet (same independence rule as src/gold/cross_asset/):

    raw_macro.py          FRED panel, PIT-gated, forward-filled   (macro branch)
    raw_market.py         Silver-only market-behaviour panel      (market branch)
    hmm_macro_regime.py   per-domain GaussianHMM + macro_label
    hmm_market_regime.py  GaussianHMM -> market_label
    context_regime.py     pure join of the two regime stores

Domain independence (ADR-051 Amendment 2 §A): nothing in this package may
read a Gold store produced by the CrossAssetEngine or the technical chain,
and neither of those may depend on anything here.

Shared helpers: config.py (registry routing + validation), fred_pit.py
(point-in-time availability + as-of panel construction).
"""
