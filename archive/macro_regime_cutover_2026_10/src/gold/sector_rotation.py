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
import yaml
from loguru import logger

from src.utils.atomic_io import atomic_write_parquet

from src.config.instrument_loader import get_loader

# ── Regime Sector Weight Matrix ───────────────────────────────────────────────
# IDD §4: Definisi lengkap — sebelumnya hanya "tetap berlaku" di GD §5.2.6
#
# weight_adj multiplier: 0.0 = exclude, 0.5 = underweight, 1.0 = neutral,
#                        1.3 = moderate OW, 1.5 = strong OW, 2.0 = max OW

# ADD GMI Decision Document v5 §2.1 (Decision B Step 2, 2026-07-22):
# externalized from a Python dict literal into config/regime_sector_weights.yaml.
# Values are UNCHANGED (extracted from the live dict via ast.literal_eval(),
# not re-transcribed by hand) — including the commodity_precious_metals
# naming fix from Decision B Step 1 (RISK-10, v1.11.0): the mechanical
# f"commodity_{subcategory}" formula is the correct key, not the
# Architecture v2.1 Addendum §8.2 table's "commodity_precious" typo. Full
# history preserved in config/regime_sector_weights.yaml's own header, not
# duplicated here.
REGIME_SECTOR_WEIGHTS_PATH = Path("config/regime_sector_weights.yaml")
REGIME_SECTOR_WEIGHTS: dict[str, dict[str, float]] = yaml.safe_load(
    REGIME_SECTOR_WEIGHTS_PATH.read_text()
)

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
