"""
macro_regime.py — GD §5.2.1 (Gold Macro Regime Store)
Detect macro regime dari Silver enriched macro data.

Regimes: RISK_ON | RISK_OFF | STAGFLATION | REFLATION | DISINFLATION

v1.2 additions:
    - regime_transition: Boolean — True jika regime berubah hari ini
    - transition_alert:  String — deskripsi perubahan (e.g. "RISK_ON -> RISK_OFF")

Rule-based threshold (Phase 1). HMM upgrade planned v1.3 (GD §8.2).

Separation of Concerns (GD §0.3):
    Pipeline menghasilkan regime + regime_transition sebagai DATA.
    Trading Engine yang memutuskan tindakan terhadap posisi aktif.

Output: data/gold/macro/regime_store.parquet
Schema: date, regime, vix_score, yield_curve_score, cpi_score, gdp_score,
        dxy_score, composite_score, confidence, prev_regime,
        regime_persistence_days, regime_transition, transition_alert
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from loguru import logger

from src.utils.atomic_io import atomic_write_parquet

SILVER_MACRO_PATH = Path("data/silver/macro_enriched")
GOLD_MACRO_PATH   = Path("data/gold/macro")
REGIME_STORE_PATH = GOLD_MACRO_PATH / "regime_store.parquet"

# ── Regime Thresholds (GD §8.1) ───────────────────────────────────────────────
REGIME_RULES = {
    "RISK_ON": {
        "vix_max":         20.0,
        "yield_spread_min": 0.0,    # 10Y-2Y > 0 (non-inverted)
        "cpi_max":          4.0,
        "description":     "VIX < 20, SPX > 200MA, non-inverted yield curve",
    },
    "RISK_OFF": {
        "vix_min":         30.0,
        "description":     "VIX > 30, DXY rising",
    },
    "STAGFLATION": {
        "cpi_min":          5.0,
        "gdp_max":          1.0,
        "description":      "CPI > 5%, GDP < 1%",
    },
    "REFLATION": {
        "yield_spread_min": 0.5,
        "cpi_max":          4.0,
        "description":      "Yields rising, CPI < 4%",
    },
    "DISINFLATION": {
        "cpi_max":          2.5,
        "description":      "PCE < 2.5%, falling inflation",
    },
}


class MacroRegimeDetector:
    """
    Rule-based macro regime detector.
    Reads from Silver macro enriched data.
    Upgrade path: HMM (GD §8.2) target v1.3.
    """

    def detect(self, run_date: date) -> dict:
        """
        Detect regime for run_date.
        Returns regime record dict compatible with regime_store schema.
        """
        indicators = self._load_indicators(run_date)
        regime, scores, confidence = self._classify(indicators)
        prev_regime, persistence   = self._get_prev_regime(run_date)

        record = {
            "date":                   str(run_date),
            "regime":                 regime,
            "vix_score":              scores.get("vix", 0.0),
            "yield_curve_score":      scores.get("yield_curve", 0.0),
            "cpi_score":              scores.get("cpi", 0.0),
            "gdp_score":              scores.get("gdp", 0.0),
            "dxy_score":              scores.get("dxy", 0.0),
            "composite_score":        scores.get("composite", 0.0),
            "confidence":             confidence,
            "prev_regime":            prev_regime or regime,
            "regime_persistence_days": persistence,
            # v1.2: regime transition detection
            "regime_transition":      regime != (prev_regime or regime),
            "transition_alert": (
                f"{prev_regime} -> {regime}"
                if regime != (prev_regime or regime)
                else None
            ),
        }
        return record

    def _load_indicators(self, run_date: date) -> dict:
        """
        Load key macro indicators from Silver macro enriched.
        Returns dict {indicator_name: latest_value}.

        FIX GAP-1 [P0] (Production Readiness Assessment v1.7.2, GD §5.2.1,
        §8.1): F-MP-01 (v1.7.2) fixed macro_processor.run() to call
        process_bls() / process_bea(), so BLS and BEA Silver output now
        exists as bls_*_silver.parquet / bea_*_silver.parquet. But this
        method's glob only ever matched 'fred_*_silver.parquet' — BLS/BEA
        Silver output was written and then never read by Gold. A half-fix:
        the bug moved from "data never reaches Silver" to "data reaches
        Silver but Gold never looks at it", which is functionally identical
        from MacroRegime's point of view (CPI/GDP from BLS/BEA still never
        influence STAGFLATION vs REFLATION classification).

        Investigating *why* the bug had zero visible effect even when FRED
        data WAS present revealed a second, deeper issue the assessment's
        glob-only fix wouldn't have caught: series_id values are NOT
        consistent across domains.
            FRED   CPI  -> series_id 'CPIAUCSL'          (fred_ingester.py)
            BLS    CPI  -> series_id 'CUUR0000SA0'        (bls_ingester.py,
                            native fetch path; BLS-key-absent fallback
                            re-delegates to FREDIngester instead, which
                            writes under the FRED domain, not BLS)
            BEA    GDP  -> series_id 'real_gdp'            (bea_ingester.py)
        A wildcard glob across all three domains alone would NOT surface
        BLS/BEA data, because series_map below was still only requesting
        the FRED-style id ('CPIAUCSL', 'A191RL1Q225SBEA') -- a query that
        widens its file search but keeps the same WHERE series_id = ...
        literal finds nothing new. Fixed by giving each indicator a
        priority-ordered list of (domain, series_id) candidates instead.

        CPI: FRED 'CPIAUCSL' tried first (longest history, canonical
        regime_input per fred_series.yaml), falling back to BLS native
        'CUUR0000SA0' if the FRED file is unavailable. Both are CPI-U index
        levels (same unit, ~300-ish), so the fallback is safe to mix in.

        GDP is intentionally NOT given a BEA-native fallback ('real_gdp').
        BEAIngester._fetch_nipa() pulls every LineNumber row of NIPA Table
        1.1.6 with no row filter, so bea_*_silver.parquet can contain
        multiple differently-united values (level vs. %-change vs.
        contribution) per (series_id, observation_date) -- aliasing it here
        without resolving which LineNumber is the %-change row risks
        silently mixing units into a single regime indicator. That
        resolution belongs to the Gold layer formal audit (GAP-7), not a
        guess made while fixing GAP-1.
        """
        indicators = {
            "vix":         20.0,   # Neutral defaults
            "yield_spread": 0.5,
            "cpi":          3.0,
            "gdp":          2.0,
            "dxy":          100.0,
        }

        # FIX GAP-1: priority-ordered (domain, series_id) candidates per
        # indicator. First candidate that resolves to a non-null value wins;
        # remaining candidates in the list are untried (no need to fall
        # further once a value is found).
        candidates: dict[str, list[tuple[str, str]]] = {
            "vix":          [("fred", "VIXCLS")],
            "yield_spread": [("fred", "T10Y2Y")],
            "cpi":          [("fred", "CPIAUCSL"), ("bls", "CUUR0000SA0")],
            "gdp":          [("fred", "A191RL1Q225SBEA")],
            "dxy":          [("fred", "DEXUSEU")],   # inverse: lower EURUSD = stronger DXY
        }

        # FIX GAP-1: glob widened from FRED-only to FRED+BLS+BEA domains —
        # each indicator's candidate list controls which domain is actually
        # queried, this dict just maps domain -> its Silver glob pattern.
        domain_globs = {
            domain: str(SILVER_MACRO_PATH / f"{domain}_*_silver.parquet")
            for domain in ("fred", "bls", "bea")
        }

        try:
            con = duckdb.connect()
            con.execute("SET memory_limit='1GB';")

            for indicator, alias_list in candidates.items():
                for domain, series_id in alias_list:
                    try:
                        # FIX (DuckDB parameterization): replaced f-string
                        # interpolation of series_id/run_date with $name
                        # bound parameters — same pattern as active_symbols.py
                        # (AS-1) and the rest of this rewritten method.
                        result = con.execute(
                            """
                            SELECT value
                            FROM read_parquet($glob, hive_partitioning=true)
                            WHERE series_id = $series_id
                              AND CAST(observation_date AS DATE) <= $run_date
                            ORDER BY observation_date DESC
                            LIMIT 1
                            """,
                            {
                                "glob":      domain_globs[domain],
                                "series_id": series_id,
                                "run_date":  run_date,
                            },
                        ).fetchone()
                    except Exception:
                        result = None

                    if result and result[0] is not None:
                        val = float(result[0])
                        # DXY proxy: DEXUSEU is USD/EUR rate → invert for DXY direction
                        if indicator == "dxy":
                            indicators["dxy"] = 1.0 / val * 100 if val > 0 else 100.0
                        else:
                            indicators[indicator] = val
                        logger.debug(
                            f"[MacroRegime] {indicator} <- {domain}:{series_id} = {val}"
                        )
                        break   # first matching alias wins
                else:
                    logger.debug(
                        f"[MacroRegime] {indicator}: no source matched "
                        f"{[f'{d}:{s}' for d, s in alias_list]} — using neutral default"
                    )

        except Exception as e:
            logger.warning(
                f"[MacroRegime] Could not load indicators ({e})"
                " — using neutral defaults"
            )

        return indicators

    def _classify(self, ind: dict) -> tuple[str, dict, float]:
        """
        Rule-based regime classification.
        Returns (regime_name, score_dict, confidence_0_to_1).
        """
        vix      = ind.get("vix", 20.0)
        yield_sp = ind.get("yield_spread", 0.5)
        cpi      = ind.get("cpi", 3.0)
        gdp      = ind.get("gdp", 2.0)
        # FIX GLD-002: ekstrak dxy dari indicators dict — sebelumnya diabaikan.
        # _load_indicators() memuat DEXUSEU dan mengkonversi ke DXY proxy:
        #   dxy = 1 / EURUSD * 100  (inverted: kuat USD = EURUSD rendah = DXY tinggi)
        # Neutral value: ~100 (historis DXY berkisar 85–120).
        dxy = ind.get("dxy", 100.0)

        # Score each indicator: higher = more risk-on friendly
        # FIX GLD-002: dxy_score dihitung aktual — tidak lagi hardcoded 0.5.
        #   DXY > 105 = strong dollar = risk-off = score rendah (mendekati 0)
        #   DXY < 95  = weak dollar  = risk-on  = score tinggi (mendekati 1)
        #   Formula: (110 - dxy) / 20 — DXY=90→1.0, DXY=100→0.5, DXY=110→0.0
        scores = {
            "vix":         max(0, min(1, (40 - vix) / 20)),          # 0@VIX40, 1@VIX20
            "yield_curve": max(0, min(1, (yield_sp + 1) / 2)),       # -1→0, +1→1
            "cpi":         max(0, min(1, (5 - cpi) / 5)),            # 5%→0, 0%→1
            "gdp":         max(0, min(1, gdp / 4)),                  # 0%→0, 4%→1
            "dxy":         max(0, min(1, (110 - dxy) / 20)),         # FIX GLD-002
        }
        scores["composite"] = sum(scores.values()) / len(scores)
        composite = scores["composite"]

        # Rule-based classification — priority order matters
        if vix > 30:
            regime = "RISK_OFF"
            confidence = min(1.0, (vix - 30) / 20)
        elif cpi > 5 and gdp < 1:
            regime = "STAGFLATION"
            confidence = min(1.0, (cpi - 5) / 3 * 0.5 + max(0, 1 - gdp) * 0.5)
        elif cpi < 2.5:
            # DISINFLATION: low CPI is the dominant signal regardless of VIX
            regime = "DISINFLATION"
            confidence = min(1.0, (2.5 - cpi) / 2.5)
        elif vix < 20 and yield_sp > 0 and cpi < 4:
            # RISK_ON: low VIX + positive yield spread + moderate inflation
            regime = "RISK_ON"
            confidence = min(1.0, (20 - vix) / 10 * 0.5 + min(yield_sp, 1) * 0.5)
        elif yield_sp > 0.5 and cpi < 4:
            regime = "REFLATION"
            confidence = min(1.0, yield_sp / 2 * 0.6 + (4 - cpi) / 4 * 0.4)
        else:
            # Ambiguous zone (GD §15.1: composite_score -2 to +2)
            regime     = "RISK_ON" if composite > 0.5 else "RISK_OFF"
            confidence = abs(composite - 0.5) * 2   # Low confidence near boundary

        return regime, scores, round(confidence, 3)

    def _get_prev_regime(self, run_date: date) -> tuple[Optional[str], int]:
        """Read previous regime from existing regime_store."""
        if not REGIME_STORE_PATH.exists():
            return None, 0

        try:
            df = pl.read_parquet(REGIME_STORE_PATH).sort("date", descending=True)
            if df.is_empty():
                return None, 0

            latest = df.row(0, named=True)
            prev_regime = latest.get("regime")
            persistence = latest.get("regime_persistence_days", 0) or 0

            # Check if it's actually the previous day's record
            latest_date = date.fromisoformat(str(latest["date"]))
            if (run_date - latest_date).days <= 7:   # Within a week
                return prev_regime, persistence + 1
            return prev_regime, 1   # Reset persistence if gap too large

        except Exception as e:
            logger.debug(f"[MacroRegime] Could not read prev regime: {e}")
            return None, 0


def compute_regime_transition(df: pl.DataFrame) -> pl.DataFrame:
    """
    Utility: compute regime_transition column from sorted regime_store.
    GD §5.2.1 v1.2 addition.
    """
    return df.with_columns([
        (pl.col("regime") != pl.col("prev_regime")).alias("regime_transition"),
        pl.when(pl.col("regime") != pl.col("prev_regime"))
          .then(
              pl.concat_str([
                  pl.col("prev_regime"), pl.lit(" -> "), pl.col("regime")
              ])
          )
          .otherwise(pl.lit(None))
          .alias("transition_alert"),
    ])


def run(run_date: date) -> None:
    """Job entry point for gold_regime."""
    detector = MacroRegimeDetector()
    record   = detector.detect(run_date)

    # Append to regime_store
    GOLD_MACRO_PATH.mkdir(parents=True, exist_ok=True)

    new_row = pl.DataFrame([record])

    if REGIME_STORE_PATH.exists():
        existing = pl.read_parquet(REGIME_STORE_PATH)
        # Remove today's row if exists (idempotent)
        existing = existing.filter(pl.col("date") != str(run_date))
        df = pl.concat([existing, new_row], how="diagonal_relaxed")
    else:
        df = new_row

    # FIX GLD-004: atomic write pattern (tempfile + os.replace)
    # mencegah partial/corrupt file jika OOM atau crash mid-write.
    atomic_write_parquet(
        df,
        REGIME_STORE_PATH,
        compression="zstd",
        compression_level=3,
    )

    logger.info(
        f"[gold_regime] {run_date} | regime={record['regime']}"
        f" | confidence={record['confidence']:.2f}"
        + (f" | TRANSITION: {record['transition_alert']}"
           if record["regime_transition"] else "")
    )
