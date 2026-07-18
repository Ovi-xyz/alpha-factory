"""
sector_rotation.py — IDD §4 + GD §5.2.6
Sector Rotation Store: terapkan REGIME_SECTOR_WEIGHTS berdasarkan aktif regime.

Output: data/gold/sector/sector_regime_weights.parquet

Schema:
    symbol, market, sector, regime, sector_weight_adj, date

Separation of Concerns (GD §0):
    Pipeline menghasilkan sector_weight_adj sebagai DATA.
    Trading Engine yang memutuskan apakah override atau follow weights.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl
from loguru import logger

from src.utils.atomic_io import atomic_write_parquet

from src.config.instrument_loader import get_loader

# ── Regime Sector Weight Matrix ───────────────────────────────────────────────
# IDD §4: Definisi lengkap — sebelumnya hanya "tetap berlaku" di GD §5.2.6
#
# weight_adj multiplier: 0.0 = exclude, 0.5 = underweight, 1.0 = neutral,
#                        1.3 = moderate OW, 1.5 = strong OW, 2.0 = max OW

# ADD Decision B Step 1 (GMI_Decision_Document_v3.docx): commodity_* key
# names below are ALL the mechanical f"commodity_{subcategory}" formula
# (Architecture v2.1 Addendum §8.4's own pseudocode) applied to the 5
# commodity_subcategory enum values from Addendum §7.1 — EXCEPT one:
# Addendum §8.2's own key-name table says "commodity_precious" (no
# "_metals"), which does not match §7.1's own enum value
# "precious_metals" — an internal inconsistency within that single
# document, caught empirically by
# test_no_orphaned_commodity_subcategory (it would have silently degraded
# AU/AG to a neutral 1.0 weight in every regime via weights.get(key, 1.0)'s
# fallback, instead of erroring). Resolved in favor of the mechanical
# formula — the other 4 keys (energy/base_metals/agricultural/bulks) are
# ALL literal f"commodity_{subcategory}" matches already, so
# "commodity_precious_metals" is the internally-consistent choice, not
# "commodity_precious". §7.1's enum value itself (commodity_subcategory=
# 'precious_metals' in instruments.yaml) is left untouched — it is the
# more consumer-facing contract of the two.
REGIME_SECTOR_WEIGHTS: dict[str, dict[str, float]] = {

    "RISK_ON": {
        "Technology":             1.5,
        "Consumer Discretionary": 1.3,
        "Communication Services": 1.3,
        "Financials":             1.2,
        "Industrials":            1.1,
        "Health Care":            0.9,
        "Energy":                 1.0,
        "Materials":              1.0,
        "Consumer Staples":       0.7,
        "Real Estate":            0.8,
        "Utilities":              0.6,
        "idx":                    1.1,    # IDX30 — foreign fund inflow
        "forex":                  1.0,
        # ADD Decision B Step 1 (GMI_Decision_Document_v3.docx, Architecture
        # v2.1 Addendum §8): flat 'commodity' key replaced by 5 disaggregated
        # subcategory keys. Base metals OW (China growth/PMI), precious UW
        # (safe haven not needed in risk-on).
        "commodity_precious_metals":     0.7,
        "commodity_energy":       1.0,
        "commodity_base_metals":  1.4,
        "commodity_agricultural": 1.1,
        "commodity_bulks":        1.3,
        "index":                  1.0,
        "High Growth & Popular":  1.4,
    },

    "RISK_OFF": {
        "Technology":             0.6,
        "Consumer Discretionary": 0.5,
        "Communication Services": 0.7,
        "Financials":             0.7,
        "Industrials":            0.6,
        "Health Care":            1.5,
        "Energy":                 0.9,
        "Materials":              0.7,
        "Consumer Staples":       1.5,
        "Real Estate":            0.8,
        "Utilities":              1.5,
        "idx":                    0.5,    # foreign fund outflow, IDR weakens
        "forex":                  1.0,    # DXY bias long in risk-off
        # ADD Decision B Step 1: precious metals OW (AU/AG safe haven),
        # bulks severely UW (China construction halts).
        "commodity_precious_metals":     1.4,
        "commodity_energy":       0.9,
        "commodity_base_metals":  0.6,
        "commodity_agricultural": 0.9,
        "commodity_bulks":        0.5,
        "index":                  1.0,
        "High Growth & Popular":  0.5,
    },

    "STAGFLATION": {
        "Technology":             0.5,
        "Consumer Discretionary": 0.5,
        "Communication Services": 0.6,
        "Financials":             0.7,
        "Industrials":            1.0,
        "Health Care":            1.2,
        "Energy":                 1.5,    # CL primary beneficiary
        "Materials":              1.4,
        "Consumer Staples":       1.3,
        "Real Estate":            0.5,    # Rate pressure hurts REITs
        "Utilities":              1.1,
        "idx":                    0.6,
        "forex":                  1.0,    # commodity-linked FX (AUD, CAD)
        # ADD Decision B Step 1: energy and precious metals primary
        # beneficiaries; base metals UW (demand collapse despite inflation).
        "commodity_precious_metals":     1.4,
        "commodity_energy":       1.5,
        "commodity_base_metals":  0.8,
        "commodity_agricultural": 1.3,
        "commodity_bulks":        0.7,
        "index":                  0.8,
        "High Growth & Popular":  0.5,
    },

    "REFLATION": {
        "Technology":             0.8,
        "Consumer Discretionary": 1.0,
        "Communication Services": 0.9,
        "Financials":             1.5,    # yield curve steepening = banks benefit
        "Industrials":            1.4,
        "Health Care":            0.9,
        "Energy":                 1.3,
        "Materials":              1.5,
        "Consumer Staples":       0.8,
        "Real Estate":            0.9,
        "Utilities":              0.7,
        "idx":                    1.2,
        "forex":                  1.0,
        # ADD Decision B Step 1: base metals and bulks OW (infrastructure
        # cycle, China stimulus); precious UW (rising yields = opportunity
        # cost of gold).
        "commodity_precious_metals":     0.9,
        "commodity_energy":       1.3,
        "commodity_base_metals":  1.4,
        "commodity_agricultural": 1.1,
        "commodity_bulks":        1.2,
        "index":                  1.0,
        "High Growth & Popular":  0.9,
    },

    "DISINFLATION": {
        "Technology":             1.5,    # growth + duration benefit
        "Consumer Discretionary": 1.1,
        "Communication Services": 1.2,
        "Financials":             0.8,
        "Industrials":            0.9,
        "Health Care":            1.1,
        "Energy":                 0.7,
        "Materials":              0.7,
        "Consumer Staples":       1.0,
        "Real Estate":            1.4,    # duration + lower rates
        "Utilities":              1.3,
        "idx":                    0.9,
        "forex":                  1.0,
        # ADD Decision B Step 1: energy and bulks UW (demand slows, prices
        # fall); precious moderately OW (falling real yields).
        "commodity_precious_metals":     1.2,
        "commodity_energy":       0.6,
        "commodity_base_metals":  0.7,
        "commodity_agricultural": 0.8,
        "commodity_bulks":        0.6,
        "index":                  1.0,
        "High Growth & Popular":  1.3,    # long-duration growth benefits
    },
}

# Neutral fallback jika regime tidak dikenali
NEUTRAL_WEIGHTS: dict[str, float] = {k: 1.0 for k in REGIME_SECTOR_WEIGHTS["RISK_ON"]}

GOLD_SECTOR_PATH = Path("data/gold/sector")


def run(run_date: date) -> None:
    """
    Compute sector rotation weights berdasarkan aktif regime.
    Requires: gold_regime job sudah selesai (sentinel .done tersedia).
    """
    # 1. Baca aktif regime
    regime = _get_active_regime(run_date)
    weights = REGIME_SECTOR_WEIGHTS.get(regime, NEUTRAL_WEIGHTS)
    logger.info(f"[sector_rotation] run_date={run_date} | Regime={regime}")

    # 2. Join ke instrument universe
    loader = get_loader()
    rows = []
    for inst in loader.all_symbols():
        # ADD Decision B Step 1 (Architecture v2.1 Addendum §8.4): commodity
        # instruments route via commodity_subcategory (5 disaggregated keys),
        # not the flat 'commodity' market key — matches the disaggregated
        # REGIME_SECTOR_WEIGHTS above. us_stocks still use sector; everything
        # else (idx/forex/index) still falls back to market as before.
        if inst.is_commodity:
            key = f"commodity_{inst.commodity_subcategory}"
        elif inst.is_us_stock and inst.sector:
            key = inst.sector
        else:
            key = inst.market
        weight_adj = weights.get(key, 1.0)
        rows.append({
            "symbol":           inst.symbol,
            "market":           inst.market,
            "sector":           inst.sector or inst.market,
            "regime":           regime,
            "sector_weight_adj": weight_adj,
            "date":             str(run_date),
        })

    df = pl.DataFrame(rows)

    # 3. Write output — FIX GLD-004: atomic write pattern
    GOLD_SECTOR_PATH.mkdir(parents=True, exist_ok=True)
    out_path = GOLD_SECTOR_PATH / "sector_regime_weights.parquet"
    # FIX GLD-004: atomic_write_parquet via tempfile + os.replace
    atomic_write_parquet(
        df,
        out_path,
        compression="zstd",
        compression_level=3,
    )

    logger.info(
        f"[sector_rotation] {len(df)} symbols weighted"
        f" | regime={regime} → {out_path}"
    )


def _get_active_regime(run_date: date) -> str:
    """Read aktif regime dari regime_store.parquet."""
    regime_path = "data/gold/macro/regime_store.parquet"
    try:
        con = duckdb.connect()
        # FIX GMI-AUD-001: $name parameterized query — f-string SQL dilarang
        # (GD §17.7 hard constraint, CI Gate G-2). Ditemukan saat audit
        # KNOWN_RISKS.md RISK-3 — TIDAK tersentuh oleh audit GLD-003
        # sebelumnya (scope GLD-003 tidak mencakup file ini; lihat docstring
        # test_fstring_sql_absence.py). str(run_date) dipakai (bukan raw
        # date object) untuk mempertahankan semantik perbandingan STRING yang
        # persis sama dengan f-string asli — bukan re-interpretasi tipe.
        result = con.execute(
            """
            SELECT regime FROM read_parquet($path)
            WHERE date = $run_date
            LIMIT 1
            """,
            {"path": regime_path, "run_date": str(run_date)},
        ).fetchone()
        if result:
            return result[0]
    except Exception as e:
        logger.warning(
            f"[sector_rotation] Regime store not available: {e}"
            " — defaulting to RISK_ON"
        )
    return "RISK_ON"   # Default fallback
