"""
build_instruments_v14.py — GMI Wave 1 Pre-Implementation
Generate instruments.yaml v1.4 dari YAML v1.2 existing.

Perubahan:
  1. DXY dipindahkan dari forex (Layer 1) ke context.dollar (Layer 2)
  2. SPX dipindahkan dari index ke context.equity.dm (Layer 2)
  3. VIX dipindahkan dari index ke context.volatility (Layer 2)
  4. index section dikosongkan (Layer 1 index tidak ada lagi)
  5. +13 global equity indices di context.equity.dm/em
  6. +25 ETF di context.etf.*
  7. +11 commodity context di context.commodity.*
  8. EXPECTED_TOTAL: 643 → 692

Jalankan SATU KALI:
  python scripts/build_instruments_v14.py
"""

from __future__ import annotations
import sys, os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import yaml

SRC = Path(__file__).parent.parent / "config" / "instruments.yaml"
DST = Path(__file__).parent.parent / "config" / "instruments.yaml"

# ── Baca YAML existing ────────────────────────────────────────────────────────
data = yaml.safe_load(SRC.read_text())

# ── 1. Hapus DXY dari forex ───────────────────────────────────────────────────
forex_groups = data.get("forex", {})
for group_name, pairs in list(forex_groups.items()):
    data["forex"][group_name] = [p for p in pairs if p.get("symbol") != "DXY"]

# ── 2. Kosongkan index section (SPX + VIX pindah ke context) ─────────────────
data["index"] = []          # Tetap ada agar InstrumentLoader lama tidak error

# ── 3. Tambahkan context section (Layer 2) ────────────────────────────────────
data["context"] = {

    # ── Dollar & Rates Group ──────────────────────────────────────────────────
    "dollar": {
        "_meta": {
            "subcategory_id": "context_dollar",
            "source": "yfinance",
            "description": "US Dollar Index — primary macro anchor",
            "contributes_to": [
                {"score": "score_dollar_strength", "weight": 0.50,
                 "aggregation": "z_score_level"},
            ],
        },
        "instruments": [
            {"symbol": "DXY", "yfinance_symbol": "DX-Y.NYB",
             "layer": 2, "context_category": "context_dollar",
             "context_group": "dollar",
             "context_available": True, "include_in_forecast": True,
             "reclassified_from": "layer_1_forex",
             "timezone": "UTC"},
        ],
    },

    "rates": {
        "fed": {
            "_meta": {
                "subcategory_id": "context_rates_fed",
                "source": "fred",
                "description": "FED policy rates — FRED API exclusive",
                "contributes_to": [
                    {"score": "score_dollar_strength", "weight": 0.30,
                     "aggregation": "z_score_level"},
                    {"score": "score_yield_curve", "weight": 0.20},
                ],
                "series": ["SOFR", "FEDFUNDS", "IORB", "EFFR"],
                "series_count": 4,
            },
        },

        "curve": {
            "_meta": {
                "subcategory_id": "context_rates_curve",
                "source": "fred",
                "description": "US Treasury yield curve — key tenors",
                "contributes_to": [
                    {"score": "score_yield_curve", "weight": 0.50},
                    {"score": "score_dollar_strength", "weight": 0.30},
                ],
                "series": ["DGS2", "DGS5", "DGS10", "DGS30"],
                "series_count": 4,
            },
        },

        "spread": {
            "_meta": {
                "subcategory_id": "context_rates_spread",
                "source": "fred",
                "description": "Yield curve spread — recession signal",
                "contributes_to": [
                    {"score": "score_yield_curve", "weight": 0.50},
                    {"score": "score_credit_stress", "weight": 0.50},
                ],
                "series": ["T10Y2Y", "T10Y3M"],
                "series_count": 2,
            },
        },

        "dm_cb": {
            "_meta": {
                "subcategory_id": "context_rates_dm_cb",
                "source": "bis_cbpol_d",
                "description": "G10 ex-FED central bank policy rates — BIS CBPOL_D",
                "contributes_to": [
                    {"score": "score_dollar_strength", "weight": 0.20,
                     "aggregation": "rate_differential_fed_minus_dm_weighted"},
                    {"score": "score_yield_curve", "weight": 0.10},
                    {"score": "score_global_growth", "weight": 0.10,
                     "aggregation": "z_score_level_inverted_mean"},
                ],
                "central_banks": ["ECB", "BOE", "BOJ", "BOC", "RBA",
                                  "RBNZ", "SNB", "NORGES", "RIKSBANK"],
                "ref_area_codes": {"ECB": "XM", "BOE": "GB", "BOJ": "JP",
                                   "BOC": "CA", "RBA": "AU", "RBNZ": "NZ",
                                   "SNB": "CH", "NORGES": "NO", "RIKSBANK": "SE"},
                "reliability_notes": (
                    "BOJ: YCC 2016-2024 causes rate_bps to be flat -10bps — registered. "
                    "SNB: Negative rate -75bps (2015-2022) — StandardScaler handles range."
                ),
            },
        },

        "em_cb": {
            "_meta": {
                "subcategory_id": "context_rates_em_cb",
                "source": "bis_cbpol_d",
                "description": "EM central bank rates — carry and capital flow signals",
                "contributes_to": [
                    {"score": "score_em_risk", "weight": 0.25,
                     "aggregation": "rate_differential_fed_minus_em_bi_weighted_2x"},
                    {"score": "score_global_growth", "weight": 0.10},
                    {"score": "score_inflation_pressure", "weight": 0.05},
                ],
                "central_banks": ["PBOC", "BOK", "BI"],
                "ref_area_codes": {"PBOC": "CN", "BOK": "KR", "BI": "ID"},
                "reliability_notes": (
                    "PBOC: 7-day repo rate only (MLF/LPR not in BIS free tier). Accepted. "
                    "BI: Structural break 2016-08-19 (BI Rate → BI7DRR). Registered. "
                    "BOK: MSCI EM classification maintained for taxonomy consistency."
                ),
            },
        },
    },

    # ── Global Equity Group ───────────────────────────────────────────────────
    "equity": {
        "dm": {
            "_meta": {
                "subcategory_id": "context_equity_dm",
                "description": "DM equity indices — global risk appetite",
                "contributes_to": [
                    {"score": "score_global_growth", "weight": 0.40,
                     "aggregation": "z_score_momentum_20d"},
                    {"score": "score_risk_appetite", "weight": 0.25},
                ],
            },
            "instruments": [
                {"symbol": "SPX",  "yfinance_symbol": "^GSPC",  "layer": 2,
                 "context_category": "context_equity_dm", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "reclassified_from": "layer_1_index", "timezone": "America/New_York"},
                {"symbol": "NYA",  "yfinance_symbol": "^NYA",   "layer": 2,
                 "context_category": "context_equity_dm", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York"},
                {"symbol": "DJI",  "yfinance_symbol": "^DJI",   "layer": 2,
                 "context_category": "context_equity_dm", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York"},
                {"symbol": "IXIC", "yfinance_symbol": "^IXIC",  "layer": 2,
                 "context_category": "context_equity_dm", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York"},
                {"symbol": "FTSE", "yfinance_symbol": "^FTSE",  "layer": 2,
                 "context_category": "context_equity_dm", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "Europe/London"},
                {"symbol": "DAX",  "yfinance_symbol": "^GDAXI", "layer": 2,
                 "context_category": "context_equity_dm", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "Europe/Berlin"},
                {"symbol": "CAC",  "yfinance_symbol": "^FCHI",  "layer": 2,
                 "context_category": "context_equity_dm", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "Europe/Paris"},
                {"symbol": "N225", "yfinance_symbol": "^N225",  "layer": 2,
                 "context_category": "context_equity_dm", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "Asia/Tokyo"},
                {"symbol": "AXJO", "yfinance_symbol": "^AXJO",  "layer": 2,
                 "context_category": "context_equity_dm", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "Australia/Sydney"},
            ],
        },

        "em": {
            "_meta": {
                "subcategory_id": "context_equity_em",
                "description": "EM equity indices — capital flow signals",
                "contributes_to": [
                    {"score": "score_global_growth", "weight": 0.15,
                     "aggregation": "z_score_momentum_20d"},
                    {"score": "score_em_risk", "weight": 0.40},
                ],
            },
            "instruments": [
                {"symbol": "TWSE",  "yfinance_symbol": "^TWII", "layer": 2,
                 "context_category": "context_equity_em", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "Asia/Taipei"},
                {"symbol": "KOSPI", "yfinance_symbol": "^KS11", "layer": 2,
                 "context_category": "context_equity_em", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "Asia/Seoul"},
                {"symbol": "HSI",   "yfinance_symbol": "^HSI",  "layer": 2,
                 "context_category": "context_equity_em", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "Asia/Hong_Kong"},
                # SSEC — reliability_flag required (circuit breakers, structural breaks)
                {"symbol": "SSEC",  "yfinance_symbol": "^SSEC", "layer": 2,
                 "context_category": "context_equity_em", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "reliability_flag": True, "exclude_from_lead_lag_leader": True,
                 "timezone": "Asia/Shanghai",
                 "notes": "Circuit breakers, structural breaks — use robust estimators"},
                {"symbol": "JKSE",  "yfinance_symbol": "^JKSE", "layer": 2,
                 "context_category": "context_equity_em", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "Asia/Jakarta"},
            ],
        },

        "volatility": {
            "_meta": {
                "subcategory_id": "context_volatility",
                "description": "VIX — threshold-based regime indicator",
                "contributes_to": [
                    {"score": "score_risk_appetite", "weight": 0.40,
                     "aggregation": "z_score_inverted"},
                ],
            },
            "instruments": [
                {"symbol": "VIX", "yfinance_symbol": "^VIX", "layer": 2,
                 "context_category": "context_volatility", "context_group": "equity",
                 "context_available": True, "include_in_forecast": True,
                 "reclassified_from": "layer_1_index", "timezone": "America/New_York"},
            ],
        },
    },

    # ── Commodity Group ───────────────────────────────────────────────────────
    "commodity": {
        "energy": {
            "_meta": {
                "subcategory_id": "context_commodity_energy",
                "contributes_to": [
                    {"score": "score_commodity_cycle",    "weight": 0.40,
                     "aggregation": "z_score_momentum_60d"},
                    {"score": "score_inflation_pressure", "weight": 0.38},
                ],
            },
            "instruments": [
                {"symbol": "BRENT", "yfinance_symbol": "BZ=F", "layer": 2,
                 "context_category": "context_commodity_energy", "context_group": "commodity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "UTC"},
                {"symbol": "NG",    "yfinance_symbol": "NG=F", "layer": 2,
                 "context_category": "context_commodity_energy", "context_group": "commodity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "UTC"},
            ],
        },

        "metals": {
            "_meta": {
                "subcategory_id": "context_commodity_metals",
                "contributes_to": [
                    {"score": "score_global_growth",   "weight": 0.25,
                     "aggregation": "z_score_momentum_20d"},
                    {"score": "score_commodity_cycle", "weight": 0.40,
                     "aggregation": "z_score_momentum_60d"},
                ],
                "reliability_notes": "Nickel structural break 2022-03-07 registered",
            },
            "instruments": [
                {"symbol": "COPPER",    "yfinance_symbol": "HG=F",  "layer": 2,
                 "context_category": "context_commodity_metals", "context_group": "commodity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "UTC",
                 "notes": "Industrial bellwether, leading PMI"},
                # NICKEL — structural break 2022-03-07 (LME trading suspension)
                {"symbol": "NICKEL",    "yfinance_symbol": "NI=F",  "layer": 2,
                 "context_category": "context_commodity_metals", "context_group": "commodity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "UTC",
                 "structural_break": {"date": "2022-03-07", "severity": "HIGH",
                                      "description": "LME trading suspension — price +100% in hours"},
                 "notes": "EV/battery cycle indicator"},
                {"symbol": "ALUMINIUM", "yfinance_symbol": "ALI=F", "layer": 2,
                 "context_category": "context_commodity_metals", "context_group": "commodity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "UTC"},
                # ZINC — ticker verification required before production
                {"symbol": "ZINC",      "yfinance_symbol": "ZN=F",  "layer": 2,
                 "context_category": "context_commodity_metals", "context_group": "commodity",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "UTC",
                 "notes": "Verify ZN=F ticker empirically before production deploy"},
                # IRON_ORE — proxy via VALE (NYSE) + FRED PIORECRORECUSDM (monthly)
                {"symbol": "IRON_ORE",  "yfinance_symbol": "VALE",  "layer": 2,
                 "context_category": "context_commodity_metals", "context_group": "commodity",
                 "context_available": True, "include_in_forecast": True,
                 "proxy_for": "IRON_ORE_SGX_FE62",
                 "proxy_instrument": "VALE",
                 "proxy_correlation_expected": 0.81,
                 "timezone": "America/New_York",
                 "notes": "VALE proxy — ~67pct revenue from iron ore. Corr ~0.81 vs SGX benchmark"},
                # TIN — deferred Wave 2 (MYR normalization required)
                {"symbol": "TIN",       "yfinance_symbol": None,    "layer": 2,
                 "context_category": "context_commodity_metals", "context_group": "commodity",
                 "context_available": False, "include_in_forecast": False,
                 "deferred_reason": "MYR→USD normalization pipeline not yet implemented",
                 "planned_wave": 2,
                 "requires_fx_normalization": True, "base_currency": "MYR"},
            ],
        },

        "agri": {
            "_meta": {
                "subcategory_id": "context_commodity_agri",
                "contributes_to": [
                    {"score": "score_inflation_pressure", "weight": 0.35,
                     "aggregation": "z_score_level"},
                ],
                "note": "Weights normalized when context_available=False (Wave 2 deferred)",
            },
            "instruments": [
                # CPO — deferred Wave 2 (MYR normalization required)
                {"symbol": "CPO",    "yfinance_symbol": None, "layer": 2,
                 "context_category": "context_commodity_agri", "context_group": "commodity",
                 "context_available": False, "include_in_forecast": False,
                 "deferred_reason": "MYR→USD normalization pipeline not yet implemented",
                 "planned_wave": 2,
                 "requires_fx_normalization": True, "base_currency": "MYR"},
                # RUBBER — deferred Wave 2
                {"symbol": "RUBBER", "yfinance_symbol": None, "layer": 2,
                 "context_category": "context_commodity_agri", "context_group": "commodity",
                 "context_available": False, "include_in_forecast": False,
                 "deferred_reason": "MYR→USD normalization pipeline not yet implemented",
                 "planned_wave": 2,
                 "requires_fx_normalization": True, "base_currency": "MYR"},
            ],
        },

        "coal": {
            "_meta": {
                "subcategory_id": "context_commodity_coal",
                "contributes_to": [
                    {"score": "score_commodity_cycle",    "weight": 0.20,
                     "aggregation": "z_score_momentum_60d"},
                    {"score": "score_inflation_pressure", "weight": 0.22},
                ],
            },
            "instruments": [
                # COAL_NEWC — proxy via WHC.AX (Whitehaven Coal, ASX)
                {"symbol": "COAL_NEWC", "yfinance_symbol": "WHC.AX", "layer": 2,
                 "context_category": "context_commodity_coal", "context_group": "commodity",
                 "context_available": True, "include_in_forecast": True,
                 "proxy_for": "NEWC_THERMAL_COAL",
                 "proxy_instrument": "WHC.AX",
                 "proxy_correlation_expected": 0.78,
                 "timezone": "Australia/Sydney",
                 "structural_break": {
                     "dates": [
                         {"date": "2020-10-01", "end": "2022-01-15",
                          "severity": "MEDIUM",
                          "description": "China import ban — NEWC and WHC.AX diverged"},
                     ],
                 },
                 "notes": "WHC.AX = Whitehaven Coal, Pacific basin alignment"},
            ],
        },
    },

    # ── ETF Group ─────────────────────────────────────────────────────────────
    "etf": {
        "broad_market": {
            "_meta": {
                "subcategory_id": "context_etf_broad",
                "description": "Broad market benchmarks — NOT in ForecastModule (multicollinearity)",
                "contributes_to": [
                    {"score": "score_risk_appetite", "weight": 0.25},
                ],
            },
            "instruments": [
                {"symbol": "SPY", "yfinance_symbol": "SPY", "layer": 2,
                 "context_category": "context_etf_broad", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "IVV", "yfinance_symbol": "IVV", "layer": 2,
                 "context_category": "context_etf_broad", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "QQQ", "yfinance_symbol": "QQQ", "layer": 2,
                 "context_category": "context_etf_broad", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "DIA", "yfinance_symbol": "DIA", "layer": 2,
                 "context_category": "context_etf_broad", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "IWM", "yfinance_symbol": "IWM", "layer": 2,
                 "context_category": "context_etf_broad", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
            ],
        },

        "sector": {
            "_meta": {
                "subcategory_id": "context_etf_sector",
                "description": "Sector ETFs — NOT in ForecastModule (subset of Layer 1)",
            },
            "instruments": [
                {"symbol": "XLK",  "yfinance_symbol": "XLK",  "layer": 2,
                 "context_category": "context_etf_sector", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "XLF",  "yfinance_symbol": "XLF",  "layer": 2,
                 "context_category": "context_etf_sector", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "XLV",  "yfinance_symbol": "XLV",  "layer": 2,
                 "context_category": "context_etf_sector", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "XLY",  "yfinance_symbol": "XLY",  "layer": 2,
                 "context_category": "context_etf_sector", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "XLP",  "yfinance_symbol": "XLP",  "layer": 2,
                 "context_category": "context_etf_sector", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "XLI",  "yfinance_symbol": "XLI",  "layer": 2,
                 "context_category": "context_etf_sector", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "XLB",  "yfinance_symbol": "XLB",  "layer": 2,
                 "context_category": "context_etf_sector", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "XLU",  "yfinance_symbol": "XLU",  "layer": 2,
                 "context_category": "context_etf_sector", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "XLRE", "yfinance_symbol": "XLRE", "layer": 2,
                 "context_category": "context_etf_sector", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "XLC",  "yfinance_symbol": "XLC",  "layer": 2,
                 "context_category": "context_etf_sector", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
            ],
        },

        "factor": {
            "_meta": {
                "subcategory_id": "context_etf_factor",
                "description": "Factor ETFs — NOT in ForecastModule (dividend subset of Layer 1)",
            },
            "instruments": [
                {"symbol": "SCHD", "yfinance_symbol": "SCHD", "layer": 2,
                 "context_category": "context_etf_factor", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
                {"symbol": "NOBL", "yfinance_symbol": "NOBL", "layer": 2,
                 "context_category": "context_etf_factor", "context_group": "etf",
                 "context_available": True, "include_in_forecast": False,
                 "timezone": "America/New_York"},
            ],
        },

        "credit": {
            "_meta": {
                "subcategory_id": "context_etf_credit",
                "contributes_to": [
                    {"score": "score_credit_stress", "weight": 0.50,
                     "aggregation": "z_score_inverted"},
                ],
            },
            "instruments": [
                {"symbol": "HYG", "yfinance_symbol": "HYG", "layer": 2,
                 "context_category": "context_etf_credit", "context_group": "etf",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York",
                 "notes": "Daily credit risk proxy — genuinely orthogonal to equity"},
            ],
        },

        "commodity_etf": {
            "_meta": {
                "subcategory_id": "context_etf_commodity",
                "contributes_to": [
                    {"score": "score_inflation_pressure", "weight": 0.05},
                ],
            },
            "instruments": [
                {"symbol": "DBA", "yfinance_symbol": "DBA", "layer": 2,
                 "context_category": "context_etf_commodity", "context_group": "etf",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York",
                 "notes": "Agriculture macro — external supply shock indicator"},
            ],
        },

        "international": {
            "_meta": {
                "subcategory_id": "context_etf_international",
                "contributes_to": [
                    {"score": "score_em_risk",      "weight": 0.35,
                     "aggregation": "z_score_momentum_20d"},
                    {"score": "score_global_growth", "weight": 0.05},
                ],
            },
            "instruments": [
                {"symbol": "EEM",  "yfinance_symbol": "EEM",  "layer": 2,
                 "context_category": "context_etf_international", "context_group": "etf",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York"},
                {"symbol": "EFA",  "yfinance_symbol": "EFA",  "layer": 2,
                 "context_category": "context_etf_international", "context_group": "etf",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York"},
                {"symbol": "EWJ",  "yfinance_symbol": "EWJ",  "layer": 2,
                 "context_category": "context_etf_international", "context_group": "etf",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York"},
                {"symbol": "INDA", "yfinance_symbol": "INDA", "layer": 2,
                 "context_category": "context_etf_international", "context_group": "etf",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York"},
                # EIDO — bridge signal for IDX30 (leads by 1 day)
                {"symbol": "EIDO", "yfinance_symbol": "EIDO", "layer": 2,
                 "context_category": "context_etf_international", "context_group": "etf",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York",
                 "notes": "iShares MSCI Indonesia — bridge EIDO→IDX30. Leads by 1 day."},
            ],
        },

        "thematic": {
            "_meta": {
                "subcategory_id": "context_etf_thematic",
                "contributes_to": [
                    {"score": "score_risk_appetite", "weight": 0.35,
                     "aggregation": "z_score_momentum"},
                ],
            },
            "instruments": [
                {"symbol": "ARKK", "yfinance_symbol": "ARKK", "layer": 2,
                 "context_category": "context_etf_thematic", "context_group": "etf",
                 "context_available": True, "include_in_forecast": True,
                 "timezone": "America/New_York",
                 "notes": "Risk appetite proxy — leading indicator speculative behavior"},
            ],
        },
    },
}

# ── Bump version dan tanggal ──────────────────────────────────────────────────
data["version"] = "1.4"
data["last_updated"] = "2026-06-30"

# ── Tulis ke file ──────────────────────────────────────────────────────────────
yaml_text = yaml.dump(
    data,
    allow_unicode=True,
    sort_keys=False,
    default_flow_style=False,
    indent=2,
    width=120,
)
DST.write_text(yaml_text)
print(f"instruments.yaml v1.4 ditulis ke {DST}")

# ── Verifikasi count ──────────────────────────────────────────────────────────
import json

def count_context_ohlcv(ctx: dict) -> int:
    """Count OHLCV instruments in context section (has yfinance_symbol)."""
    total = 0
    for group_key, group_val in ctx.items():
        if not isinstance(group_val, dict):
            continue
        for subkey, subval in group_val.items():
            if subkey == "_meta":
                continue
            if isinstance(subval, dict):
                # Nested: context.equity.dm, context.rates.fed, etc.
                if "instruments" in subval:
                    total += len(subval["instruments"])
                elif "_meta" in subval and "instruments" not in subval:
                    # Rates subcategory — macro series only, not OHLCV
                    pass
                else:
                    # Recurse one level
                    for k2, v2 in subval.items():
                        if k2 == "_meta":
                            continue
                        if isinstance(v2, dict) and "instruments" in v2:
                            total += len(v2["instruments"])
            elif isinstance(subval, list) and subkey == "instruments":
                total += len(subval)
    return total

ctx = data["context"]
l2_ohlcv = count_context_ohlcv(ctx)

# Layer 1
l1_us = sum(len(v) for v in data["us_stocks"].values())
l1_idx = sum(len(v) for v in data["idx_stocks"].values())
l1_comm = sum(len(v) for v in data["commodity"].values())
l1_forex = sum(len(v) for v in data["forex"].values())
l1_index = len(data.get("index", []))
l1_total = l1_us + l1_idx + l1_comm + l1_forex + l1_index

print(f"\nLayer 1:")
print(f"  us_stocks : {l1_us}")
print(f"  idx_stocks: {l1_idx}")
print(f"  commodity : {l1_comm}")
print(f"  forex     : {l1_forex}  (DXY removed)")
print(f"  index     : {l1_index}  (SPX/VIX moved to context)")
print(f"  SUBTOTAL  : {l1_total}")
print(f"\nLayer 2 context (OHLCV): {l2_ohlcv}")
print(f"\nGRAND TOTAL: {l1_total + l2_ohlcv}  (target: 692)")
