"""
pandas_indicators.py — IDD §3.3 (pandas-ta Thin Wrapper)
BBands + ADX via pandas-ta-classic — Polars↔Pandas konversi di-isolasi di sini.

UPD ADR-020 (GMI_Decision_Document_v2.docx, 2026-07-11): migrated from
pandas-ta to pandas-ta-classic. PyPI removed pandas-ta's entire 0.3.x line
(the project's stated floor, 0.3.14, included) — only two Python-3.12-only
prerelease builds remain. pandas-ta-classic is a maintained continuation of
the same abandoned 0.3.x lineage, with genuine stable releases compatible
with this project's Python >=3.11 floor (unchanged). Import name differs
from the package name: `import pandas_ta_classic as ta`.

Hanya dua indicator yang membutuhkan pandas-ta-classic:
    BBands: Bollinger Bands (20, 2.0)
    ADX:    Average Directional Index + DI+/DI-

Konversi overhead diminimized:
    - Groupby per symbol (sekali konversi per symbol, bukan per bar)
    - Tidak ada Pandas state di luar fungsi ini
"""

from __future__ import annotations

import polars as pl
from loguru import logger


def add_bbands(
    df: pl.DataFrame,
    period: int = 20,
    std: float = 2.0,
) -> pl.DataFrame:
    """
    Bollinger Bands via pandas-ta.
    Outputs: bbands_upper, bbands_mid, bbands_lower
    """
    try:
        import pandas as pd
        # FIX ADR-020 (GMI_Decision_Document_v2.docx): pandas-ta -> pandas-ta-classic.
        # PyPI removed pandas-ta's entire 0.3.x line (incl. the project's stated
        # floor 0.3.14); only two Python-3.12-only prerelease builds remain.
        # pandas-ta-classic is a maintained continuation of the abandoned 0.3.x
        # lineage with genuine stable releases, requires_python >=3.9/3.10 —
        # compatible with this project's >=3.11 floor (unchanged by this fix).
        import pandas_ta_classic as ta  # type: ignore
    except ImportError:
        logger.warning(
            "[BBands] pandas-ta-classic not installed — returning null columns"
        )
        return df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("bbands_upper"),
            pl.lit(None).cast(pl.Float64).alias("bbands_mid"),
            pl.lit(None).cast(pl.Float64).alias("bbands_lower"),
        ])

    results: list[pl.DataFrame] = []

    for sym_val in df["symbol"].unique().to_list():
        grp = df.filter(pl.col("symbol") == sym_val)
        pd_df = grp.to_pandas().sort_values("timestamp")

        bb = ta.bbands(pd_df["close"], length=period, std=std)

        if bb is None or bb.empty:
            logger.warning(f"[BBands] Failed for {sym_val} — filling null")
            grp = grp.with_columns([
                pl.lit(None).cast(pl.Float64).alias("bbands_upper"),
                pl.lit(None).cast(pl.Float64).alias("bbands_mid"),
                pl.lit(None).cast(pl.Float64).alias("bbands_lower"),
            ])
        else:
            # pandas-ta BBands output column names: BBL, BBM, BBU, BBB, BBP
            col_map = {}
            for c in bb.columns:
                lc = c.lower()
                if "bbu" in lc or "upper" in lc:
                    col_map[c] = "bbands_upper"
                elif "bbm" in lc or "mid" in lc:
                    col_map[c] = "bbands_mid"
                elif "bbl" in lc or "lower" in lc:
                    col_map[c] = "bbands_lower"

            bb = bb.rename(columns=col_map)
            keep = [c for c in ["bbands_upper", "bbands_mid", "bbands_lower"]
                    if c in bb.columns]

            pd_df = pd.concat([pd_df, bb[keep]], axis=1)

            existing_cols = grp.columns
            new_cols = [c for c in keep if c not in existing_cols]
            grp = pl.from_pandas(pd_df[existing_cols + new_cols])

        results.append(grp)

    return pl.concat(results) if results else df


def add_adx(
    df: pl.DataFrame,
    period: int = 14,
) -> pl.DataFrame:
    """
    Average Directional Index + DI+/DI- via pandas-ta-classic.
    Outputs: adx, di_plus, di_minus

    FIX GLD-ADX-001 [P0] (discovered empirically while adding test coverage
    for technical_signals.py — GMI Wave 1 Bronze/Silver Solidification):
    the pandas-ta version installed at the time (0.4.71b0) returned FOUR
    columns from ta.adx(), not three: ['ADX_14', 'ADXR_14_2', 'DMP_14',
    'DMN_14']. ADXR = Average Directional Index Rating, a smoothed variant
    this pipeline does not use. The prior column-matching rule
    (lc.startswith("adx")) matched BOTH 'adx_14' AND 'adxr_14_2' — 'adxr'
    starts with 'adx' as a plain substring — renaming BOTH to the single
    target name 'adx' and producing a DataFrame with a duplicate column
    name. pl.from_pandas() correctly rejects this
    ("Pandas dataframe contains non-unique indices and/or column names"),
    meaning _every_ real call to add_adx() with that pandas-ta version
    raised, and gold_signals (and everything downstream of it — gold_mtf,
    gold_screener) was non-functional against real data. This was
    completely invisible because pandas_indicators.py (add_adx, add_bbands)
    had ZERO test coverage anywhere in the repo before this fix — confirmed
    via a full grep across tests/ finding no reference to either function.
    Fix: match "adx_" (with the trailing underscore pandas-ta always emits
    before the length suffix) instead of "adx" — this still matches
    'ADX_14' while correctly excluding 'ADXR_14_2' (empirically verified:
    'adxr_14_2'.startswith('adx_') is False, since the character
    immediately after 'adx' is 'r', not '_'). add_bbands() was audited
    against the same real pandas-ta output as part of this investigation
    and confirmed NOT to have an analogous collision (BBB_/BBP_ columns
    match none of the upper/mid/lower target patterns) — no change needed
    there.

    UPD ADR-020 (GMI_Decision_Document_v2.docx, 2026-07-11): the project
    migrated from pandas-ta to pandas-ta-classic (PyPI removed pandas-ta's
    entire 0.3.x line — see add_bbands()'s import comment). pandas-ta-classic
    0.6.52's ta.adx() was verified empirically to emit only THREE columns —
    ['ADX_14', 'DMP_14', 'DMN_14'] — with no ADXR column at all, so the
    collision this fix guards against cannot currently occur in this fork.
    The startswith("adx_") rule is kept as-is rather than reverted to
    startswith("adx"): it is strictly more precise, costs nothing, and
    remains a live guard should a future pandas-ta-classic release
    reintroduce an ADX-prefixed sibling column. See
    test_wrapper_adx_matches_raw_adx_column and
    test_pandas_ta_classic_does_not_emit_adxr_column in
    tests/unit/test_pandas_indicators.py.
    """
    try:
        import pandas as pd
        import pandas_ta_classic as ta  # type: ignore
    except ImportError:
        logger.warning(
            "[ADX] pandas-ta-classic not installed — returning null columns"
        )
        return df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("adx"),
            pl.lit(None).cast(pl.Float64).alias("di_plus"),
            pl.lit(None).cast(pl.Float64).alias("di_minus"),
        ])

    results: list[pl.DataFrame] = []

    for sym_val in df["symbol"].unique().to_list():
        grp = df.filter(pl.col("symbol") == sym_val)
        pd_df = grp.to_pandas().sort_values("timestamp")

        adx_df = ta.adx(
            pd_df["high"], pd_df["low"], pd_df["close"], length=period
        )

        if adx_df is None or adx_df.empty:
            logger.warning(f"[ADX] Failed for {sym_val} — filling null")
            grp = grp.with_columns([
                pl.lit(None).cast(pl.Float64).alias("adx"),
                pl.lit(None).cast(pl.Float64).alias("di_plus"),
                pl.lit(None).cast(pl.Float64).alias("di_minus"),
            ])
        else:
            # pandas-ta ADX output: ADX_{n}, ADXR_{n}_{m} (unused), DMP_{n}, DMN_{n}
            col_map = {}
            for c in adx_df.columns:
                lc = c.lower()
                if lc.startswith("adx_"):        # FIX GLD-ADX-001: was "adx" (collided with "adxr_...")
                    col_map[c] = "adx"
                elif "dmp" in lc or "di+" in lc or "diplus" in lc:
                    col_map[c] = "di_plus"
                elif "dmn" in lc or "di-" in lc or "diminus" in lc:
                    col_map[c] = "di_minus"
                # else: leave unmapped (e.g. ADXR) — dropped via `keep` below

            adx_df = adx_df.rename(columns=col_map)
            keep = [c for c in ["adx", "di_plus", "di_minus"]
                    if c in adx_df.columns]

            pd_df = pd.concat([pd_df, adx_df[keep]], axis=1)

            existing_cols = grp.columns
            new_cols = [c for c in keep if c not in existing_cols]
            grp = pl.from_pandas(pd_df[existing_cols + new_cols])

        results.append(grp)

    return pl.concat(results) if results else df
