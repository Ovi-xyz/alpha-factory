"""
gold/cross_asset/ — CrossAssetEngine (Architecture v2.0 §6, GMI Wave 1 Cycle 4)

All four modules named in Architecture v2.0 §6.1 are implemented here:
    global_index_regime.py  — GlobalIndexRegimeModule (daily)
    correlation_module.py   — CorrelationModule (weekly, Ledoit-Wolf)
    lead_lag_module.py      — LeadLagModule (weekly, Granger + BH-FDR q=0.03)
    forecast_module.py      — ForecastModule (weekly, PCA + per-equity VAR)
    broad_dollar.py          — Gate 1 closure; Broad Dollar Index derived
                                feature consumed by forecast_module.py

None of the four are wired into gold_screener's ranking/filtering logic —
their outputs are surfaced there as purely informational columns (see
src/gold/screener.py's own "ADD GMI Wave 1 Cycle 4" docstring section).
See CHANGELOG.md v1.18.0 and KNOWN_RISKS.md RISK-31 for full status,
including what was deliberately left out of this pass (FRED/BIS rate
series in ForecastModule's PCA input, retirement of the pre-Cycle-4
gold_correlation job, live validation against real Silver data).
"""
