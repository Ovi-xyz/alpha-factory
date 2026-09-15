"""
global_index_regime.py — Architecture v2.0 §6.5 (CrossAssetEngine —
GlobalIndexRegimeModule). GMI Wave 1 Cycle 4 — first CrossAssetEngine
module implemented (chosen as the simplest/daily module to start with;
see instruments_taxonomy.yaml ADR-049 comment for why the other three
modules — CorrelationModule, LeadLagModule, ForecastModule — are not
part of this pass).

Daily module (unlike CorrelationModule/LeadLagModule/ForecastModule,
which are weekly per Architecture v2.0 §6.1) that reads the Layer 2
global equity indices (context_equity_dm + context_equity_em,
instruments_taxonomy.yaml) and derives a supplementary regime signal,
independent of macro_regime.py's VIX/rates-based classification
(GD §8.1) and independent of the rest of CrossAssetEngine, none of
which exists yet.

Universe (live-confirmed against instruments_taxonomy.yaml, 12 Sep
2026 — NOT the stale "13 global equity indices" figure in Architecture
v2.0 §3.2, which predates ADR-003's SPX reclassification into
context_equity_dm):
    DM     (9): SPX, NYA, DJI, IXIC, FTSE, DAX, CAC, N225, AXJO
    EM     (5): TWSE, KOSPI, HSI, SSEC, JKSE
    Global (14) = DM ∪ EM
    Asia-Pacific (7) = N225, AXJO (DM) + all 5 EM — unchanged from the
        original 13-index design; none of the 7 were touched by ADR-003.

Formulas (Architecture v2.0 §6.5 table):
    global_risk_score   = % of 14 indices with close > EMA(50)  [0, 100]
    dm_em_divergence    = DM score − EM score                   [-100, 100]
    asia_pac_breadth    = % of 7 Asia-Pacific indices with close > EMA(20) [0, 100]
    global_regime_label = RISK_ON / RISK_OFF / MIXED
    regime_transition   = label changed vs the previous stored row

Classification thresholds — NOT specified anywhere in Architecture v2.0,
so stated here explicitly rather than silently invented: global_risk_score
>= 60 -> RISK_ON, <= 40 -> RISK_OFF, else MIXED. A 20-point neutral band
on each side of 50/50 breadth so noise near the midpoint doesn't flip the
label daily. First-pass calibration — revisit once enough live history
exists to check the label's actual duty-cycle (no live data exists yet
to calibrate against empirically).

SSEC carries reliability_flag=true (circuit-breaker outliers —
Architecture v2.0 §3.5) but is NOT excluded here: exclude_from_lead_lag_leader
is scoped to LeadLagModule's "leader" designation specifically (Granger-
causality false positives from circuit-breaker discontinuities), not to
a simple close-vs-EMA breadth threshold, which is far more robust to
isolated outlier bars than a correlation/causality estimator is. Included
with no special-casing; revisit if live output shows SSEC alone swinging
the EM score materially.

PIT note: unlike technical_signals.py's _process_timeframe() (which reads
the full Silver history unbounded, relying on Bronze/Silver being
append-only-to-date), this module explicitly bounds its read to
timestamp <= run_date. Cheap to add and correct for --date backfill
invocations; the difference is invisible on a normal same-day run.

Output: data/gold/cross_asset/global_regime.parquet
Schema: date, global_risk_score, dm_score, em_score, dm_em_divergence,
        asia_pac_breadth, global_regime_label, prev_global_regime_label,
        regime_transition, n_dm_covered, n_em_covered, n_asia_pac_covered,
        partial_coverage_flag, processing_version

Job registry: 'gold_global_regime', depends_on=['silver_ohlcv_context']
— the Layer 2 OHLCV price job, NOT 'silver_context_anchors' (pure config
metadata, no price data — see src/silver/context_anchors.py docstring).
Not yet wired into gold_screener's depends_on (Architecture v2.0 §6.5's
own code snippet does this) — deliberately deferred to the
signal_aggregation / screener-integration pass (Architecture v2.0 §5.3,
§9.1 Phase 5), out of scope for this module.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from loguru import logger

from src.config.instrument_loader import get_loader
from src.gold.indicators.core_indicators import add_ema
from src.utils.atomic_io import atomic_write_parquet
from src.utils.silver_scope import context_glob

SILVER_OHLCV_PATH        = Path("data/silver/market_ohlcv")
GOLD_CROSS_ASSET_PATH    = Path("data/gold/cross_asset")
GLOBAL_REGIME_STORE_PATH = GOLD_CROSS_ASSET_PATH / "global_regime.parquet"

PROCESSING_VERSION: str = "1.0"

# Asia-Pacific subset — unchanged since Architecture v2.0 §3.2 (none of
# these 7 were touched by ADR-003's SPX/VIX/DXY reclassification).
_ASIA_PACIFIC_SYMBOLS: frozenset[str] = frozenset(
    {"N225", "AXJO", "TWSE", "KOSPI", "HSI", "SSEC", "JKSE"}
)

# Breadth classification thresholds — see module docstring for rationale.
RISK_ON_THRESHOLD:  float = 60.0
RISK_OFF_THRESHOLD: float = 40.0


class GlobalIndexRegimeModule:
    """Architecture v2.0 §6.5 — daily supplementary regime signal from the
    Layer 2 global equity indices (context_equity_dm + context_equity_em)."""

    def compute(self, run_date: date) -> Optional[dict]:
        """
        Compute one day's global_regime record. Returns None (not a
        placeholder row) if no Layer 2 equity-index Silver data exists
        yet for run_date — caller (run()) must skip the write in that
        case, matching technical_signals.py's own "no data yet, don't
        write a garbage row" convention.
        """
        dm_symbols, em_symbols = self._resolve_universe()
        all_symbols = dm_symbols | em_symbols

        df = self._load_context_ohlcv(sorted(all_symbols), run_date)
        if df.is_empty():
            logger.warning(
                f"[gold_global_regime] No Layer 2 equity-index Silver data "
                f"on/before {run_date} — skipping"
            )
            return None

        latest = self._latest_ema_snapshot(df)
        if latest.is_empty():
            logger.warning(
                f"[gold_global_regime] Silver data found but no rows "
                f"survived EMA computation for {run_date} — skipping"
            )
            return None

        dm_score,    n_dm    = self._pct_above(latest, dm_symbols, "ema_50")
        em_score,    n_em    = self._pct_above(latest, em_symbols, "ema_50")
        global_score, _      = self._pct_above(latest, all_symbols, "ema_50")
        asia_score,  n_asia  = self._pct_above(
            latest, _ASIA_PACIFIC_SYMBOLS & all_symbols, "ema_20"
        )

        label      = self._classify(global_score)
        prev_label = self._get_prev_label(run_date)

        return {
            "date":                     str(run_date),
            "global_risk_score":        round(global_score, 3),
            "dm_score":                 round(dm_score, 3),
            "em_score":                 round(em_score, 3),
            "dm_em_divergence":         round(dm_score - em_score, 3),
            "asia_pac_breadth":         round(asia_score, 3),
            "global_regime_label":      label,
            "prev_global_regime_label": prev_label,
            "regime_transition":        prev_label is not None and label != prev_label,
            "n_dm_covered":             n_dm,
            "n_em_covered":             n_em,
            "n_asia_pac_covered":       n_asia,
            "partial_coverage_flag":    n_dm < len(dm_symbols) or n_em < len(em_symbols),
            "processing_version":       PROCESSING_VERSION,
        }

    # ── Universe resolution ───────────────────────────────────────────────

    def _resolve_universe(self) -> tuple[frozenset[str], frozenset[str]]:
        """Return (DM symbols, EM symbols) from instruments_taxonomy.yaml
        via InstrumentLoader — never hardcoded, stays correct automatically
        if a global index is ever added/removed from either subcategory."""
        loader = get_loader()
        dm = frozenset(
            i.symbol for i in loader.by_context_category("context_equity_dm")
            if i.context_available
        )
        em = frozenset(
            i.symbol for i in loader.by_context_category("context_equity_em")
            if i.context_available
        )
        return dm, em

    # ── Silver read ───────────────────────────────────────────────────────

    def _load_context_ohlcv(self, symbols: list[str], run_date: date) -> pl.DataFrame:
        glob = context_glob(SILVER_OHLCV_PATH, "*_1D_silver.parquet")
        if glob is None:
            logger.warning(
                "[gold_global_regime] Layer 2 context/ directory does not "
                "exist yet (silver_ohlcv_context not yet run)"
            )
            return pl.DataFrame()

        con = duckdb.connect()
        con.execute("SET memory_limit='2GB'; SET threads=4;")
        try:
            df = con.execute(
                """
                SELECT symbol, timestamp, close
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE is_clean = TRUE
                  AND symbol = ANY($symbols)
                  AND CAST(timestamp AS DATE) <= $run_date
                ORDER BY symbol, timestamp
                """,
                {"glob": glob, "symbols": symbols, "run_date": run_date},
            ).pl()
        except Exception as e:
            logger.warning(f"[gold_global_regime] Silver read failed: {e}")
            return pl.DataFrame()
        return df

    def _latest_ema_snapshot(self, df: pl.DataFrame) -> pl.DataFrame:
        """EMA(20)/EMA(50) per symbol over the full history read, then keep
        only each symbol's most recent row (the one we classify on)."""
        enriched = df.sort(["symbol", "timestamp"]).pipe(
            add_ema, periods=[20, 50], col="close"
        )
        return (
            enriched
            .group_by("symbol", maintain_order=True)
            .last()
            .drop_nulls(subset=["ema_20", "ema_50"])
        )

    # ── Breadth aggregation ───────────────────────────────────────────────

    @staticmethod
    def _pct_above(
        latest: pl.DataFrame, symbols: frozenset[str], ema_col: str
    ) -> tuple[float, int]:
        """Return (% of symbols with close > ema_col, count of symbols
        actually found in `latest`). 0.0/0 if none of `symbols` are present
        — caller's partial_coverage_flag surfaces this, it is never a
        silent 100%/0% default."""
        if not symbols:
            return 0.0, 0
        filtered = latest.filter(pl.col("symbol").is_in(list(symbols)))
        total = filtered.height
        if total == 0:
            return 0.0, 0
        above = filtered.filter(pl.col("close") > pl.col(ema_col)).height
        return above / total * 100.0, total

    @staticmethod
    def _classify(global_risk_score: float) -> str:
        if global_risk_score >= RISK_ON_THRESHOLD:
            return "RISK_ON"
        if global_risk_score <= RISK_OFF_THRESHOLD:
            return "RISK_OFF"
        return "MIXED"

    def _get_prev_label(self, run_date: date) -> Optional[str]:
        """Most recent stored label strictly BEFORE run_date. Deliberately
        excludes run_date's own row (unlike macro_regime.py's
        _get_prev_regime, which does not filter out today's own date) —
        on a same-day rerun, macro_regime.py's approach would read today's
        own already-written label back as "prev", which is a real (if
        minor) idempotency wrinkle in that module. Not this module's bug
        to fix, but not worth reproducing here when excluding one date is
        this cheap."""
        if not GLOBAL_REGIME_STORE_PATH.exists():
            return None
        try:
            df = pl.read_parquet(GLOBAL_REGIME_STORE_PATH)
            prior = df.filter(pl.col("date") < str(run_date)).sort("date", descending=True)
            if prior.is_empty():
                return None
            return prior.row(0, named=True)["global_regime_label"]
        except Exception as e:
            logger.debug(f"[gold_global_regime] Could not read prev label: {e}")
            return None


def run(run_date: date) -> None:
    """Job entry point for gold_global_regime (DAILY_SEQUENCE)."""
    module = GlobalIndexRegimeModule()
    record = module.compute(run_date)
    if record is None:
        logger.warning(f"[gold_global_regime] {run_date}: nothing written (no data)")
        return

    GOLD_CROSS_ASSET_PATH.mkdir(parents=True, exist_ok=True)
    new_row = pl.DataFrame([record])

    if GLOBAL_REGIME_STORE_PATH.exists():
        existing = pl.read_parquet(GLOBAL_REGIME_STORE_PATH)
        # Idempotent re-run: drop today's own row before appending, same
        # convention as macro_regime.py's run().
        existing = existing.filter(pl.col("date") != str(run_date))
        out = pl.concat([existing, new_row], how="diagonal_relaxed")
    else:
        out = new_row

    atomic_write_parquet(
        out,
        GLOBAL_REGIME_STORE_PATH,
        compression="zstd",
        compression_level=3,
    )

    logger.info(
        f"[gold_global_regime] {run_date} | label={record['global_regime_label']}"
        f" | global={record['global_risk_score']:.1f}"
        f" | dm_em_divergence={record['dm_em_divergence']:+.1f}"
        f" | asia_pac={record['asia_pac_breadth']:.1f}"
        + (
            f" | TRANSITION: {record['prev_global_regime_label']} -> "
            f"{record['global_regime_label']}"
            if record["regime_transition"] else ""
        )
    )
