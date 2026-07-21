"""
migrate_instruments.py — G7 Supplementary Design v1.1  [ARCHIVED — DO NOT RUN]
Konversi src/config/instruments_raw.py -> config/instruments.yaml
Jalankan SATU KALI sebelum coding dimulai.

ARCHIVED (v1.11.2, GMI_Decision_Document_v3 Priority 3 / Checkpoint v6 §8.3):
config/instruments.yaml has evolved far past what this script produces —
it reads only the original 643-instrument flat structure from
src/config/instruments_raw.py (Grand Design v1.2 era: 4 markets, no Layer 2,
no context.*, no domain scores) and would OVERWRITE the current 699-instrument
hierarchical instruments.yaml (Layer 1 + Layer 2, 22 subcategories,
commodity_role/commodity_subcategory, _meta.contributes_to blocks) with that
stale structure. Kept for historical reference only. See scripts/archive/README.md.

Usage: python scripts/migrate_instruments.py   [DISABLED — see guard below]
"""

import sys
import os

# ADD (v1.11.2) — hard guard, unconditional. This script's body runs its
# destructive config/instruments.yaml write at MODULE IMPORT TIME (no
# `if __name__ == "__main__":` gate ever existed here) — even `import`ing
# this module from a stray script or misconfigured test-collector would
# silently corrupt the live, hand-maintained instruments.yaml. Halting here,
# before sys.path is even touched, closes that hole completely. This is a
# deliberate one-line removal if a genuine historical rebuild is ever
# needed — not a bypass flag, so it can't be silently disabled.
raise SystemExit(
    "migrate_instruments.py is ARCHIVED and will not run.\n"
    "It targets the pre-v1.4 instruments.yaml schema (superseded July 2026)\n"
    "and would corrupt the current config/instruments.yaml if executed.\n"
    "See scripts/archive/README.md for context and safe alternatives."
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pathlib import Path
import yaml
# NOTE: this import is unreachable — the SystemExit guard above fires first.
# Path updated for internal consistency: instruments_raw.py moved alongside
# this file in v1.11.2 (was src/config/instruments_raw.py).
from instruments_raw import (
    US_STOCKS_BY_SECTOR,
    IDX_STOCKS,
    COMMODITY,
    FOREX,
)

# ── Override & Mapping Tables ─────────────────────────────────────────────────
SYMBOL_OVERRIDES: dict[str, str] = {
    "MOBILEYE": "MBLY",
    "BRK.B":    "BRK-B",
    "BRK.A":    "BRK-A",
}

YFINANCE_INDEX_MAP: dict[str, str] = {
    "SPX": "^GSPC",
    "NDX": "^NDX",
    "DJI": "^DJI",
    "VIX": "^VIX",
    "RUT": "^RUT",
    "DXY": "DX-Y.NYB",
}

COMMODITY_YF_MAP: dict[str, str] = {
    "AU": "GC=F",
    "AG": "SI=F",
    "CL": "CL=F",
    "NG": "NG=F",
    "HG": "HG=F",
}


# ── Helper Functions ──────────────────────────────────────────────────────────
def normalize(s: str) -> str:
    """Normalize symbol: safe for Hive path, DuckDB, filenames."""
    return SYMBOL_OVERRIDES.get(s, s).replace(".", "-").replace("/", "_").upper()


def make_stock_entry(s: str) -> dict:
    """Build us_stocks / idx_stocks entry."""
    n = normalize(s)
    entry: dict = {"symbol": n}
    if n != s:
        entry["raw_symbol"] = s
    return entry


# ── Build Output Structure ────────────────────────────────────────────────────
out: dict = {
    "version":      "1.2",
    "last_updated": "2026-05-19",
    "us_stocks":    {},
    "idx_stocks":   {},
    "commodity":    {},
    "forex":        {},
    "index":        [],
}

# US Stocks + Index sub-sector
for sector, symbols in US_STOCKS_BY_SECTOR.items():
    if sector == "Index":
        # SPX & VIX kept inside us_stocks as well as index section
        out["index"] = [
            {
                "symbol":          s,
                "yfinance_symbol": YFINANCE_INDEX_MAP.get(s, f"^{s}"),
            }
            for s in symbols
        ]
        continue
    out["us_stocks"][sector] = [make_stock_entry(s) for s in symbols]

# IDX Stocks
for group_name, symbols in IDX_STOCKS.items():
    out["idx_stocks"][group_name] = [
        {
            "symbol":          s,
            "yfinance_symbol": f"{s}.JK",
        }
        for s in symbols
    ]

# Commodity
for grp, symbols in COMMODITY.items():
    out["commodity"][grp] = [
        {
            "symbol":          s,
            "yfinance_symbol": COMMODITY_YF_MAP.get(s, f"{s}=F"),
        }
        for s in symbols
    ]

# Forex  (G7 FIX: FOREX sekarang diproses)
for grp, pairs in FOREX.items():
    out["forex"][grp] = [
        {
            "symbol":          p.replace("/", "_"),
            "raw_symbol":      p,
            "yfinance_symbol": (
                "DX-Y.NYB"
                if p == "DXY"
                else p.replace("/", "") + "=X"
            ),
        }
        for p in pairs
    ]

# G7 Cross-gap: sort symbols alphabetically to keep git diffs clean
for market in ["us_stocks", "idx_stocks", "commodity", "forex"]:
    for sector in out[market]:
        out[market][sector] = sorted(
            out[market][sector], key=lambda x: x["symbol"]
        )
out["index"] = sorted(out["index"], key=lambda x: x["symbol"])

# ── Write YAML ────────────────────────────────────────────────────────────────
config_dir = Path(__file__).parent.parent / "config"
config_dir.mkdir(parents=True, exist_ok=True)
output_path = config_dir / "instruments.yaml"

output_path.write_text(
    yaml.dump(
        out,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )
)

print(f"[migrate_instruments] Migration complete → {output_path}")
print("[migrate_instruments] Next step: python scripts/validate_instruments.py")
