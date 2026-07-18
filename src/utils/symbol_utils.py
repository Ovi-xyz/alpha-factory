"""
symbol_utils.py — G3 Supplementary Design v1.1
Symbol normalization & API symbol mapping.

KONTRAK PENTING:
  - normalize_symbol(): untuk Hive path, filename, DuckDB — JANGAN untuk API
  - to_api_symbol():    untuk API call — INPUT HARUS raw_symbol dari Instrument.raw_symbol
"""

# ── Override Tables ───────────────────────────────────────────────────────────

SYMBOL_OVERRIDES: dict[str, str] = {
    "MOBILEYE": "MBLY",
    "BRK.B":    "BRK-B",
    "BRK.A":    "BRK-A",
}

# yfinance suffix per market
# Index TIDAK menggunakan suffix — lihat YFINANCE_INDEX_MAP
YFINANCE_SUFFIX: dict[str, str] = {
    "us_stocks": "",
    "idx":       ".JK",
    "forex":     "=X",
    "commodity": "=F",
    "index":     "",
}

# FIX v1.1: Index membutuhkan ^ prefix, bukan suffix
# DXY menggunakan format khusus di yfinance
YFINANCE_INDEX_MAP: dict[str, str] = {
    "SPX": "^GSPC",
    "NDX": "^NDX",
    "DJI": "^DJI",
    "VIX": "^VIX",
    "RUT": "^RUT",
    "DXY": "DX-Y.NYB",
}

POLYGON_OVERRIDES: dict[str, str] = {
    "BRK-B": "BRK.B",    # Polygon pakai titik
    "BRK-A": "BRK.A",
}

# Known edge cases documentation (untuk reference dan debugging)
KNOWN_EDGE_CASES: dict[str, dict] = {
    "MOBILEYE": {
        "normalized": "MBLY",
        "note": "Full name di instruments, ticker NASDAQ: MBLY",
    },
    "BRK.B": {
        "normalized": "BRK-B",
        "note": "Titik diganti dash untuk path safety",
    },
    "BRK.A": {
        "normalized": "BRK-A",
        "note": "Pola sama dengan BRK.B",
    },
    "DXY": {
        "yf_symbol": "DX-Y.NYB",
        "note": "DXY tidak ada di yfinance suffix — pakai INDEX_MAP",
    },
    "CL": {
        "note": (
            "CL adalah Colgate-Palmolive (us_stocks) DAN WTI Crude Oil "
            "(commodity). Bedakan dengan market parameter."
        ),
    },
    # ADD ADR-013/024 (GMI Decision Documents v1/v2): Layer 2 context
    # currencies resolve yfinance_symbol directly from instruments.yaml
    # (bypass to_api_symbol() entirely — see instrument_loader.py
    # _build_context_instrument / Checkpoint v2 Decision D5). Documented
    # here for symbol-mapping auditability only, mirroring the DXY entry
    # above; NOT consulted at runtime for Layer 2 resolution.
    "CNH": {
        "yf_symbol": "USDCNH=X",
        "note": (
            "ADR-013: offshore renminbi, not onshore CNY (avoids PBOC "
            "policy-rate/managed-FX double-counting). USD<CCY>=X convention, "
            "consistent with USD_CAD/USD_CHF/USD_JPY. Confirmed live."
        ),
    },
    "KRW": {"yf_symbol": "USDKRW=X", "note": "Ticker convention unconfirmed live — Gate 2, non-blocking."},
    "SGD": {"yf_symbol": "USDSGD=X", "note": "ADR-016: FX-policy-band grounds (S$NEER). Ticker unconfirmed live — Gate 2."},
    "HKD": {"yf_symbol": "USDHKD=X", "note": "ADR-015: pegged currency, reliability_flag=true, near-zero basket weight. Ticker unconfirmed live — Gate 2."},
    "TWD": {"yf_symbol": "USDTWD=X", "note": "Ticker convention unconfirmed live — Gate 2, non-blocking."},
    "NOK": {"yf_symbol": "USDNOK=X", "note": "Ticker convention unconfirmed live — Gate 2, non-blocking."},
    "MYR": {
        "yf_symbol": "MYR=X",
        "note": (
            "ADR-024: differs from the USD<CCY>=X convention above by "
            "explicit choice — MYR=X is Yahoo Finance's canonical form "
            "(same <currency>=X pattern as JPY=X/HKD=X/SGD=X/IDR=X). "
            "Confirmed live. Sole consumer: Silver-layer CPO normalization."
        ),
    },
}


# ── Core Functions ────────────────────────────────────────────────────────────

def normalize_symbol(raw: str, market: str) -> str:
    """
    Return simbol aman untuk Hive path, file name, dan DuckDB.
    Input: raw symbol dari instruments (e.g. 'BRK.B', 'EUR/USD').
    Output: normalized (e.g. 'BRK-B', 'EUR_USD').

    JANGAN gunakan untuk API call — pakai to_api_symbol().
    """
    sym = SYMBOL_OVERRIDES.get(raw, raw)
    return sym.replace(".", "-").replace("/", "_").upper()


def to_api_symbol(raw: str, market: str, source: str) -> str:
    """
    Return simbol untuk API call ke source tertentu.

    INPUT HARUS raw_symbol dari Instrument.raw_symbol — BUKAN normalized symbol.
    FIX v1.1: Kontrak input diperjelas — raw diterima, normalization tidak
              dilakukan sebelum ini karena raw sudah dalam format yang benar.

    Args:
        raw:    Raw symbol dari instruments (e.g. 'BRK.B', 'EUR/USD', 'SPX')
        market: Market string ('us_stocks'|'idx'|'forex'|'commodity'|'index')
        source: Data source ('yfinance'|'polygon'|'tvdatafeed'|'alphavantage')

    Returns:
        Symbol string siap dipakai untuk API call ke source.

    Examples:
        to_api_symbol('SPX',     'index',    'yfinance') == '^GSPC'
        to_api_symbol('EUR/USD', 'forex',    'yfinance') == 'EURUSD=X'
        to_api_symbol('BBCA',    'idx',      'yfinance') == 'BBCA.JK'
        to_api_symbol('BRK.B',   'us_stocks','polygon')  == 'BRK.B'
        to_api_symbol('AU',      'commodity','yfinance')  == 'GC=F'
    """
    # Terapkan override dulu (e.g. MOBILEYE -> MBLY)
    sym = SYMBOL_OVERRIDES.get(raw, raw)

    if source == "yfinance":
        # FIX v1.1: index ditangani khusus via YFINANCE_INDEX_MAP
        if market == "index":
            return YFINANCE_INDEX_MAP.get(sym, f"^{sym}")

        if market == "forex":
            # EUR/USD -> EURUSD=X (raw input punya slash)
            # DXY adalah special case di YFINANCE_INDEX_MAP
            if sym == "DXY":
                return YFINANCE_INDEX_MAP["DXY"]
            clean = sym.replace("/", "")
            suffix = YFINANCE_SUFFIX.get(market, "")
            return clean + suffix

        suffix = YFINANCE_SUFFIX.get(market, "")
        return sym + suffix

    if source == "polygon":
        return POLYGON_OVERRIDES.get(sym, sym.replace("-", "."))

    if source == "tvdatafeed":
        # tvdatafeed uses raw ticker for IDX, no transformation needed
        if market == "idx":
            return sym
        return sym

    if source == "alphavantage":
        # AlphaVantage FX: FROM_TO format (EURUSD) — no slash, no suffix
        if market == "forex":
            return sym.replace("/", "")
        return sym

    return sym


def symbol_to_polygon_forex(raw: str) -> str:
    """
    Convert forex pair ke Polygon.io format.
    EUR/USD -> C:EURUSD
    """
    clean = raw.replace("/", "")
    return f"C:{clean}"
