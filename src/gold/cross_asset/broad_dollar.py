"""
broad_dollar.py — Architecture v2.0 §7.2 (Broad Dollar Index — derived
feature). GMI Wave 1 Cycle 4 — Gate 1 closure.

Gate 1 (BIS Broad Dollar EER weight extraction, ADR-017/018) blocked this
file's real implementation since ~28 Jun 2026 (KNOWN_RISKS.md RISK-16):
scripts/preflight/check_bis_eer_weights.py was written and unit-tested in
sandbox but had never been run against the real
https://www.bis.org/statistics/eer/weightsb.xlsx file, since no sandbox
in this project has network access to bis.org — only Ovi's M1 does. Ovi
ran it for real on 12 Sep 2026 (this session); output kept verbatim below
as the empirical source of truth for the BIS_WEIGHTS constant, per this
project's own "read live output, don't theorize" discipline.

    $ python scripts/preflight/check_bis_eer_weights.py --extract-weights
    Sheet: 2020_2022  (vintage; BIS revises this table on a 3-year cycle
    — no 2023-2025 sheet exists yet, confirmed both 4 Aug 2026 and again
    12 Sep 2026, so this remains the current vintage)

      Currency  REF_AREA  Weight in US Broad EER basket (%)
        AUD       AU        0.326985
        CAD       CA        8.054349
        CHF       CH        2.885704
        CNH       CN       22.579565
        EUR       XM       16.048545
        GBP       GB        2.939921
        HKD       HK        0.026992
        IDR       ID        0.929609
        JPY       JP        5.897981
        KRW       KR        4.183282
        NOK       NO        0.168854
        SGD       SG        1.341145
        TWD       TW        3.216667

      Sum of these 13 target-currency weights: 68.599597 (out of the US
      row's full ~64-economy basket — expected to be well under 100, not
      a parsing error).

DECISION (this file, not previously made anywhere): renormalize these 14
weights to sum to 1.0 rather than using the raw ~68.7-of-100 values
directly. This pipeline only ever has return data for these 14
currencies — the other ~50 economies in BIS's true Broad EER basket
(Mexican peso, Brazilian real, Indian rupee, etc.) have no series
anywhere in this platform to weight. Using the raw values would
systematically understate Broad Dollar's magnitude by ~31% even on a day
USD moves uniformly against every one of these 14 — renormalizing
preserves each currency's weight RELATIVE to the others while producing
a return series at a comparable scale to DXY (itself a 100%-weighted
6-currency basket), which is what makes the DXY-vs-Broad-Dollar
DIVERGENCE signal (§7.2 "Information Value") meaningful: comparing a
full-scale index against a mechanically-scaled-down one would make the
divergence an artifact of scale, not genuine DM/EM dispersion.

SIGN CONVENTION: BIS's weight is on the CURRENCY, not a specific pair
direction. AUD, EUR, GBP, and NZD are conventionally quoted with USD as
the QUOTE currency (AUD_USD, EUR_USD, GBP_USD, NZD_USD — a pair RISE
means USD WEAKENED) and are negated before weighting so every term
shares one convention: positive contribution == USD strengthened
against that currency. The other 10 (CAD, CHF, JPY — Layer 1
USD_CAD/USD_CHF/USD_JPY; CNH, KRW, SGD, HKD, TWD, NOK, IDR — Layer 2
dollar_basket, all quoted USD-as-base per their yfinance_symbol values
USDCNH=X etc.) are used as-is. Confirmed against the live
instruments_taxonomy.yaml Layer 1 forex block and Layer 2 dollar_basket
group this session, not assumed.

This corrects, rather than merely replaces, Architecture v2.0 §7.2's own
hand-approximated BIS_WEIGHTS sketch, which had an internal sign
inconsistency: USD_JPY/USD_CAD/USD_CHF (also USD-as-base pairs, same
convention as its own USD_CNH/USD_KRW/USD_SGD entries) were given
POSITIVE weights while USD_CNH/USD_KRW/USD_SGD were given NEGATIVE
weights despite sharing the identical quoting convention. Not a
deliberate design being preserved — that sketch was explicitly a
placeholder pending this file.

PENDING (ADR-049, chat thread 5 Sep 2026; gap found and the extraction
script fixed 22 Sep 2026 — see KNOWN_RISKS.md RISK-16): ADR-049 extends
the Layer-1-reuse currency set from 6 to 7, adding NZD_USD the same way
AUD_USD is already reused here. scripts/preflight/check_bis_eer_weights.py
never carried NZD as a target currency, which is why Ovi's
--extract-weights run reported it unextracted rather than missing — that
gap is now fixed in that script (NZD -> REF_AREA "NZ"), but the actual
weight VALUE has not been extracted yet. This file's
_RAW_BIS_WEIGHTS_PCT/_CURRENCY_SYMBOL_MAP/BIS_WEIGHTS still cover only
the original 13 currencies — do not add a 14th entry here from a guessed
or interpolated number. Once Ovi re-runs --extract-weights for real on
the M1 and reports NZD's weight in the US Broad EER basket, add it to
_RAW_BIS_WEIGHTS_PCT (NZD_USD is a USD-is-quote pair like
AUD_USD/EUR_USD/GBP_USD, so _CURRENCY_SYMBOL_MAP["NZD"] = ("NZD_USD",
True)) — BIS_WEIGHTS's renormalization is automatic (it re-derives
_RAW_WEIGHT_SUM from whatever keys _RAW_BIS_WEIGHTS_PCT holds), so no
other line in this module needs to change.

RESOLVED (chat thread, 22 Sep 2026): Ovi re-ran --extract-weights for
real on the M1 with the fixed script. Output kept verbatim below,
superseding the 13-currency table above as the canonical source for
_RAW_BIS_WEIGHTS_PCT — the 13 pre-existing values are unchanged digit
for digit against the 12 Sep run (same static 2020_2022 BIS vintage
sheet, confirmed by direct comparison, not assumed), so nothing about
those 13 needed re-verification; only NZD is new.

    $ python scripts/preflight/check_bis_eer_weights.py --extract-weights
    Sheet: 2020_2022  (vintage; BIS revises this table on a 3-year cycle)

      Currency  REF_AREA  Weight in US Broad EER basket (%)
        AUD       AU        0.326985
        CAD       CA        8.054349
        CHF       CH        2.885704
        CNH       CN       22.579565
        EUR       XM       16.048545
        GBP       GB        2.939921
        HKD       HK        0.026992
        IDR       ID        0.929609
        JPY       JP        5.897981
        KRW       KR        4.183282
        NOK       NO        0.168854
        NZD       NZ        0.069725
        SGD       SG        1.341145
        TWD       TW        3.216667

      Sum of these 14 target-currency weights: 68.669322 (out of the US
      row's full ~64-economy basket — expected to be well under 100, not
      a parsing error). This total also empirically confirms the fix
      itself: the printed line reads "14" here rather than a stale
      literal "13", and the sum equals the prior 68.599597 plus exactly
      NZD's own 0.069725 — the two hardcoded-"13" spots fixed earlier
      this same session in that script's own output are behaving
      correctly in a real run, not just in the isolated sandbox test.

GATE_1_EXTRACTION_DATE below is updated to this 22 Sep 2026 run, since
it is now the complete 14-currency source; BIS_WEIGHTS_VINTAGE is
unchanged (same "2020_2022" sheet both times).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from loguru import logger

from src.utils.silver_scope import context_glob, layer1_globs

SILVER_OHLCV_PATH = Path("data/silver/market_ohlcv")

BIS_WEIGHTS_VINTAGE    = "2020_2022"
GATE_1_EXTRACTION_DATE = "2026-09-22"  # UPD ADR-049 (22 Sep 2026): was "2026-09-12" -- updated to the run that added NZD; the other 13 values are unchanged across both runs

# ── Gate 1 empirical extraction, 22 Sep 2026 (2020_2022 vintage) ───────────
# UPD ADR-049 (22 Sep 2026): NZD added -- see the module docstring's
# "RESOLVED" paragraph above. The other 13 values are digit-for-digit
# identical to the original 12 Sep 2026 extraction.
_RAW_BIS_WEIGHTS_PCT: dict[str, float] = {
    "AUD": 0.326985, "CAD": 8.054349, "CHF": 2.885704, "CNH": 22.579565,
    "EUR": 16.048545, "GBP": 2.939921, "HKD": 0.026992, "IDR": 0.929609,
    "JPY": 5.897981, "KRW": 4.183282, "NOK": 0.168854, "NZD": 0.069725,
    "SGD": 1.341145, "TWD": 3.216667,
}
_RAW_WEIGHT_SUM = sum(_RAW_BIS_WEIGHTS_PCT.values())  # 68.669322

# Currency -> (Silver symbol, negate_before_weighting).
# negate=True for the 4 USD-is-quote pairs (AUD_USD, EUR_USD, GBP_USD, NZD_USD).
_CURRENCY_SYMBOL_MAP: dict[str, tuple[str, bool]] = {
    "AUD": ("AUD_USD", True),
    "CAD": ("USD_CAD", False),
    "CHF": ("USD_CHF", False),
    "CNH": ("CNH",     False),
    "EUR": ("EUR_USD", True),
    "GBP": ("GBP_USD", True),
    "HKD": ("HKD",     False),
    "IDR": ("IDR",     False),
    "JPY": ("USD_JPY", False),
    "KRW": ("KRW",     False),
    "NOK": ("NOK",     False),
    "NZD": ("NZD_USD", True),  # NEW ADR-049 (22 Sep 2026)
    "SGD": ("SGD",     False),
    "TWD": ("TWD",     False),
}

# Renormalized, SIGNED weights keyed by SILVER SYMBOL — the form
# compute_broad_dollar() actually consumes. Sums to 1.0 in magnitude
# terms (sum of |values| == 1.0); signed sum is < 1.0 because 3 terms
# are negative.
BIS_WEIGHTS: dict[str, float] = {
    _CURRENCY_SYMBOL_MAP[ccy][0]: (
        (-1.0 if _CURRENCY_SYMBOL_MAP[ccy][1] else 1.0) * pct / _RAW_WEIGHT_SUM
    )
    for ccy, pct in _RAW_BIS_WEIGHTS_PCT.items()
}


def compute_broad_dollar(returns: pl.DataFrame) -> pl.DataFrame:
    """
    Construct the daily Broad Dollar return series from `returns` — a
    wide DataFrame with a 'date' column and one column per BIS_WEIGHTS
    symbol (load_fx_returns() below produces this shape). A missing
    column contributes 0 (with a coverage warning) rather than raising —
    matching every other module's "partial coverage, not a crash"
    convention in this codebase. Returns a 2-column DataFrame: date,
    broad_dollar_return.
    """
    present = [c for c in BIS_WEIGHTS if c in returns.columns]
    missing = [c for c in BIS_WEIGHTS if c not in returns.columns]
    if missing:
        logger.warning(
            f"[broad_dollar] {len(missing)}/{len(BIS_WEIGHTS)} BIS_WEIGHTS symbols "
            f"missing from Silver data: {missing} — contributing 0. This widens the "
            "scale gap between Broad Dollar and DXY until the missing symbol's "
            "Silver data exists; not renormalized further to compensate."
        )
    if not present:
        logger.warning("[broad_dollar] No BIS_WEIGHTS symbols present at all — cannot compute")
        return pl.DataFrame({"date": [], "broad_dollar_return": []})

    expr = sum((pl.col(sym) * BIS_WEIGHTS[sym]).fill_null(0.0) for sym in present)
    return returns.select([pl.col("date"), expr.alias("broad_dollar_return")])


def load_fx_returns(run_date: date, lookback_days: int) -> Optional[pl.DataFrame]:
    """
    Load the wide-format (date x symbol) log_return pivot for exactly the
    14 BIS_WEIGHTS symbols — 7 Layer 1 forex majors + 7 Layer 2
    dollar_basket currencies — over [run_date - lookback_days, run_date].
    Shared by ForecastModule so Broad Dollar and PCA read from one
    consistent window rather than two independently-parameterized reads.
    """
    symbols = sorted(BIS_WEIGHTS.keys())
    globs = layer1_globs(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
    ctx_glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
    if ctx_glob is not None:
        globs = globs + [ctx_glob]
    if not globs:
        return None

    start = run_date - timedelta(days=lookback_days)
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'; SET threads=4;")
    sym_df = pl.DataFrame({"symbol": symbols})
    con.register("fx_symbols_tbl", sym_df.to_arrow())
    try:
        returns = con.execute(
            """
            SELECT symbol, CAST(timestamp AS DATE) AS date, log_return
            FROM read_parquet($globs, hive_partitioning=true)
            WHERE CAST(timestamp AS DATE) >= $start
              AND CAST(timestamp AS DATE) <= $run_date
              AND symbol IN (SELECT symbol FROM fx_symbols_tbl)
              AND log_return IS NOT NULL
              AND is_clean = TRUE
            ORDER BY date, symbol
            """,
            {"globs": globs, "start": start, "run_date": run_date},
        ).pl()
    except Exception as e:
        logger.error(f"[broad_dollar] Silver FX read failed: {e}")
        return None
    if returns.is_empty():
        return None
    return (
        returns
        .pivot(values="log_return", index="date", on="symbol", aggregate_function="first")
        .sort("date")
    )
