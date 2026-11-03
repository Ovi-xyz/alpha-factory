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
independent of the (retired) macro_regime.py's VIX/rates-based classification and of the dual-HMM regime engine
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

XAE-R01 / XAE-R07 (cross_asset_recalibration_guide, processing_version 1.1) — what changed and why:

  ONE ROW PER BAR DATE (R01, guide G1). The store used to append one row per RUN date, so a Saturday and
  a Sunday run after a Friday bar wrote two identical rows and the Silver catch-up of one bar looked like a
  new regime event. Rows are now keyed on `as_of_bar_date` — the trade date of the newest bar any covered
  index contributed (the max across the latest bars). A re-run that sees the same bar refreshes that row;
  `date` stays the FIRST run date that saw the bar (when the information became available — the PIT-safe
  choice; readers do an as-of `date <= trade_date` read). `regime_transition` and the debounce count bar
  dates, never run dates.

  STABLE LABEL (R07, guide G2–G4). The raw label (60/40 cutoffs on the breadth score, no hysteresis)
  changed ~45 times a year with a median run of 2 days; 6 of 14 indices sat within ±0.65% of their EMA50.
  `global_regime_label_raw` keeps that label unchanged. `global_regime_label` is now its DEBOUNCE: it
  switches only after DEBOUNCE_DAYS (3) consecutive bar dates of a new raw label (guide measurement:
  ~11 transitions/yr). State lives in the store, not in memory (GD §17.7): the debounced series is a causal
  forward replay of the stored raw labels, so a normal forward run never changes a past row. It is NOT
  immutable under a BACKFILL: running an older --date inserts a bar in the middle, and a revised Silver bar can
  change a past raw label; either re-derives every later row's debounced label, transition flag, days_in_state
  and flicker_10d (the store is rewritten whole, as it always was). Rows already consumed downstream
  (watchlists) are not revised.
  New continuous columns: `breadth_dist_pct` (mean distance of close to EMA50 in %, each index clipped to
  ±DIST_CLIP_PCT so one crash bar cannot dominate), `days_in_state` (bar dates the debounced label has held),
  `flicker_10d` (raw-label changes among the last FLICKER_WINDOW bar dates).

  REGION-WEIGHTED SCORE (R07, guide G5). SPX, NYA, DJI and IXIC are one factor (pairwise correlation ≥ 0.78,
  all four agree on 81% of days) but were 4 of 14 votes — 28.6 points of a 20-point neutral band, enough to
  flip the label alone. `global_risk_score` now gives the US block ONE vote (US_VOTE_BLOCK; each covered
  member weighs 1/n_covered_members), so the block moves the score by at most 1/11 = 9.1 points. The old
  equal-weight count is kept as `global_risk_score_equal` (continuity and replay audit). dm_score, em_score
  and asia_pac_breadth are unchanged. The block membership and the one-vote rule are untuned starting values
  [U]; an empty US_VOTE_BLOCK restores equal weighting.

  WHAT THE LABEL IS (guide G6): a VOLATILITY conditioner, not a direction signal — SPX-proxy 5-day forward
  P(up) is 0.608 / 0.605 / 0.616 for RISK_ON / MIXED / RISK_OFF while the forward SD is 1.64 / 2.23 / 3.82%.
  Do not feed it to a model as a monotone risk-off feature. Its incremental value over trailing volatility is
  untested.

  NOT DONE HERE: the EMA input still reads `is_clean = TRUE` bars (R07 says it follows R06 — the Silver
  quality-flag split, not yet landed). The 10-year replay acceptance (≤ 12 transitions/yr; reproduce stored
  rows within ±1 index) needs live Silver and is an operator check.

Output: data/gold/cross_asset/global_regime.parquet
Schema: date, global_risk_score, dm_score, em_score, dm_em_divergence,
        asia_pac_breadth, global_regime_label, prev_global_regime_label,
        regime_transition, n_dm_covered, n_em_covered, n_asia_pac_covered,
        partial_coverage_flag, processing_version,
        as_of_bar_date, global_regime_label_raw, global_risk_score_equal,
        breadth_dist_pct, days_in_state, flicker_10d      (the last six: XAE-R01/R07, additive)

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
from src.utils.trade_date import with_trade_date  # FIX TS-DATE-01

SILVER_OHLCV_PATH        = Path("data/silver/market_ohlcv")
GOLD_CROSS_ASSET_PATH    = Path("data/gold/cross_asset")
GLOBAL_REGIME_STORE_PATH = GOLD_CROSS_ASSET_PATH / "global_regime.parquet"

PROCESSING_VERSION: str = "1.1"   # XAE-R01/R07: bar-date keying, debounce, region-weighted score

# Asia-Pacific subset — unchanged since Architecture v2.0 §3.2 (none of
# these 7 were touched by ADR-003's SPX/VIX/DXY reclassification).
_ASIA_PACIFIC_SYMBOLS: frozenset[str] = frozenset(
    {"N225", "AXJO", "TWSE", "KOSPI", "HSI", "SSEC", "JKSE"}
)

# Breadth classification thresholds — see module docstring for rationale.
RISK_ON_THRESHOLD:  float = 60.0
RISK_OFF_THRESHOLD: float = 40.0

# XAE-R07 — stability parameters. DEBOUNCE_DAYS = 3 is the guide's measured choice (G4: ~45 -> ~11
# transitions/yr on a 10-year replication); the others are untuned starting values [U].
DEBOUNCE_DAYS:   int   = 3
FLICKER_WINDOW:  int   = 10
DIST_CLIP_PCT:   float = 5.0
# One factor, four votes (guide G5): these cast ONE vote together. Empty = equal weighting.
US_VOTE_BLOCK: frozenset[str] = frozenset({"SPX", "NYA", "DJI", "IXIC"})

STORE_COLUMNS: tuple[str, ...] = (
    "date", "global_risk_score", "dm_score", "em_score", "dm_em_divergence",
    "asia_pac_breadth", "global_regime_label", "prev_global_regime_label",
    "regime_transition", "n_dm_covered", "n_em_covered", "n_asia_pac_covered",
    "partial_coverage_flag", "processing_version",
    "as_of_bar_date", "global_regime_label_raw", "global_risk_score_equal",
    "breadth_dist_pct", "days_in_state", "flicker_10d",
)


# ── Pure state helpers (XAE-R01 / XAE-R07) ───────────────────────────────────
# Free functions on plain lists / frames so the debounce and the bar-date keying can be tested without
# any Silver fixture. No I/O, no module state.

def debounce_labels(labels: list[str], n: int) -> list[str]:
    """Causal debounce: the output label switches to a new raw label only after `n` consecutive inputs
    carry that same new label; a return to the current label cancels any pending switch. The first
    input is accepted as-is. n <= 1 returns the input unchanged."""
    if n <= 1 or not labels:
        return list(labels)
    out: list[str] = []
    cur, cand, cnt = labels[0], None, 0
    for v in labels:
        if v == cur:
            cand, cnt = None, 0
        else:
            cnt = cnt + 1 if v == cand else 1
            cand = v
            if cnt >= n:
                cur, cand, cnt = v, None, 0
        out.append(cur)
    return out


def _run_lengths(labels: list[str]) -> list[int]:
    out: list[int] = []
    for i, v in enumerate(labels):
        out.append(out[-1] + 1 if i > 0 and labels[i - 1] == v else 1)
    return out


def _flicker(raw: list[str], window: int) -> list[int]:
    """Raw-label changes among the last `window` entries (pairs fully inside the window)."""
    out: list[int] = []
    for i in range(len(raw)):
        lo = max(1, i - window + 2)
        out.append(sum(1 for j in range(lo, i + 1) if raw[j] != raw[j - 1]))
    return out


def _order_key():
    """Bar-date sort key; legacy rows without as_of_bar_date fall back to their run date."""
    return pl.coalesce([
        pl.col("as_of_bar_date"),
        pl.col("date").cast(pl.Utf8).str.to_date(strict=False),
    ])


def normalize_legacy_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Rows written before processing_version 1.1 carry no as_of_bar_date and were appended per RUN date,
    so a weekend re-run left an identical copy of the previous row (guide G1). Drop a legacy row whose
    metrics equal the previous legacy row's — same bar, not a new observation. Rows with a bar date are
    never touched."""
    if df.is_empty() or "as_of_bar_date" not in df.columns:
        return df
    legacy = df.filter(pl.col("as_of_bar_date").is_null()).sort("date")
    keep_legacy = legacy
    if legacy.height > 1:
        cols = [c for c in ("global_risk_score", "dm_score", "em_score", "asia_pac_breadth",
                            "global_regime_label") if c in legacy.columns]
        same = pl.all_horizontal([pl.col(c) == pl.col(c).shift(1) for c in cols]).fill_null(False)
        keep_legacy = legacy.filter(~same)
    return pl.concat([df.filter(pl.col("as_of_bar_date").is_not_null()), keep_legacy],
                     how="diagonal_relaxed")


def upsert_bar_row(store: pl.DataFrame, new_row: pl.DataFrame) -> pl.DataFrame:
    """Merge one computed row into the store keyed on as_of_bar_date. A row for the same bar is replaced
    (so a re-run reflects the latest Silver) but keeps the EARLIEST run date that has seen the bar."""
    if store.is_empty():
        return new_row
    bar = new_row["as_of_bar_date"][0]
    same = store.filter(pl.col("as_of_bar_date") == bar)
    if not same.is_empty():
        first_seen = min(str(d) for d in same["date"].to_list() + new_row["date"].to_list())
        new_row = new_row.with_columns(pl.lit(first_seen).alias("date"))
    kept = store.filter(pl.col("as_of_bar_date").is_null() | (pl.col("as_of_bar_date") != bar))
    return pl.concat([kept, new_row], how="diagonal_relaxed")


def apply_state(series: pl.DataFrame, debounce_days: int | None = None) -> pl.DataFrame:
    """Recompute the stateful columns over the whole bar-date-ordered series, causally.
    Needs `global_regime_label_raw` (legacy rows: filled from their label)."""
    n = DEBOUNCE_DAYS if debounce_days is None else debounce_days
    series = series.with_columns(
        pl.col("global_regime_label_raw").fill_null(pl.col("global_regime_label"))
        if "global_regime_label_raw" in series.columns
        else pl.col("global_regime_label").alias("global_regime_label_raw")
    ).with_columns(_order_key().alias("_key")).sort(["_key", "date"])

    raw = series["global_regime_label_raw"].to_list()
    deb = debounce_labels(raw, n)
    prev = [None] + deb[:-1]
    return series.with_columns([
        pl.Series("global_regime_label", deb, dtype=pl.Utf8),
        pl.Series("prev_global_regime_label", prev, dtype=pl.Utf8),
        pl.Series("regime_transition", [p is not None and c != p for c, p in zip(deb, prev)],
                  dtype=pl.Boolean),
        pl.Series("days_in_state", _run_lengths(deb), dtype=pl.Int32),
        pl.Series("flicker_10d", _flicker(raw, FLICKER_WINDOW), dtype=pl.Int32),
    ]).drop("_key")


class GlobalIndexRegimeModule:
    """Architecture v2.0 §6.5 — daily supplementary regime signal from the
    Layer 2 global equity indices (context_equity_dm + context_equity_em)."""

    def compute(self, run_date: date) -> Optional[dict]:
        """One bar date's global_regime record (None when there is no data) — the stateful columns
        (debounced label, prev label, transition, days_in_state, flicker) already applied against the
        stored history. See compute_series() for the whole store."""
        out = self.compute_series(run_date)
        return None if out is None else out[1]

    def compute_series(self, run_date: date) -> Optional[tuple[pl.DataFrame, dict]]:
        """(full updated series, this run's record). The series is the stored history with this run's
        bar upserted and every stateful column recomputed causally; nothing is written. None when no
        Layer 2 equity-index Silver data exists on/before run_date — the caller must not write a
        placeholder row (technical_signals.py convention)."""
        raw = self._compute_raw(run_date)
        if raw is None:
            return None
        new_row = pl.DataFrame([raw])
        store = normalize_legacy_rows(self._read_store())
        merged = upsert_bar_row(store, new_row)
        series = apply_state(merged)
        bar = raw["as_of_bar_date"]
        record = series.filter(pl.col("as_of_bar_date") == bar).row(0, named=True)
        return series, record

    def _compute_raw(self, run_date: date) -> Optional[dict]:
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
        equal_score, _       = self._pct_above(latest, all_symbols, "ema_50")
        weighted_score       = self._weighted_pct_above(latest, all_symbols, "ema_50")
        asia_score,  n_asia  = self._pct_above(
            latest, _ASIA_PACIFIC_SYMBOLS & all_symbols, "ema_20"
        )
        covered = latest.filter(pl.col("symbol").is_in(list(all_symbols)))

        return {
            "date":                     str(run_date),
            "global_risk_score":        round(weighted_score, 3),
            "dm_score":                 round(dm_score, 3),
            "em_score":                 round(em_score, 3),
            "dm_em_divergence":         round(dm_score - em_score, 3),
            "asia_pac_breadth":         round(asia_score, 3),
            "global_regime_label":      self._classify(weighted_score),   # debounced by apply_state()
            "prev_global_regime_label": None,
            "regime_transition":        False,
            "n_dm_covered":             n_dm,
            "n_em_covered":             n_em,
            "n_asia_pac_covered":       n_asia,
            "partial_coverage_flag":    n_dm < len(dm_symbols) or n_em < len(em_symbols),
            "processing_version":       PROCESSING_VERSION,
            "as_of_bar_date":           covered["trade_date"].max(),
            "global_regime_label_raw":  self._classify(weighted_score),
            "global_risk_score_equal":  round(equal_score, 3),
            "breadth_dist_pct":         self._mean_clipped_distance(covered),
            "days_in_state":            1,
            "flicker_10d":              0,
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
                with_trade_date("""
                SELECT symbol, timestamp, close, __TRADE_DATE__ AS trade_date
                FROM read_parquet($glob, hive_partitioning=true)
                WHERE is_clean = TRUE
                  AND symbol = ANY($symbols)
                  AND __TRADE_DATE__ <= $run_date
                ORDER BY symbol, timestamp
                """),
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
    def _weighted_pct_above(
        latest: pl.DataFrame, symbols: frozenset[str], ema_col: str
    ) -> float:
        """XAE-R07: % of VOTES above ema_col, where US_VOTE_BLOCK casts one vote in total (each covered
        member weighs 1/n covered members) and every other index casts one. 0.0 when nothing is covered."""
        filtered = latest.filter(pl.col("symbol").is_in(list(symbols)))
        if filtered.is_empty():
            return 0.0
        block = US_VOTE_BLOCK & frozenset(filtered["symbol"].to_list())
        w = 1.0 / len(block) if block else 1.0
        weights = [w if s in block else 1.0 for s in filtered["symbol"].to_list()]
        above = [1.0 if c > e else 0.0 for c, e in zip(filtered["close"].to_list(),
                                                       filtered[ema_col].to_list())]
        return sum(a * x for a, x in zip(above, weights)) / sum(weights) * 100.0

    @staticmethod
    def _mean_clipped_distance(covered: pl.DataFrame) -> float:
        """XAE-R07: mean of (close / EMA50 - 1) in %, each index clipped to ±DIST_CLIP_PCT [U]."""
        if covered.is_empty():
            return 0.0
        d = ((pl.col("close") / pl.col("ema_50") - 1.0) * 100.0).clip(-DIST_CLIP_PCT, DIST_CLIP_PCT)
        return round(float(covered.select(d.mean()).item()), 4)

    @staticmethod
    def _classify(global_risk_score: float) -> str:
        if global_risk_score >= RISK_ON_THRESHOLD:
            return "RISK_ON"
        if global_risk_score <= RISK_OFF_THRESHOLD:
            return "RISK_OFF"
        return "MIXED"

    # ── Store read (read-only; run() owns the write) ──────────────────────

    @staticmethod
    def _read_store() -> pl.DataFrame:
        if not GLOBAL_REGIME_STORE_PATH.exists():
            return pl.DataFrame()
        try:
            df = pl.read_parquet(GLOBAL_REGIME_STORE_PATH)
        except Exception as e:
            logger.warning(f"[gold_global_regime] Could not read store ({e}) — treating as empty")
            return pl.DataFrame()
        df = df.with_columns(pl.col("date").cast(pl.Utf8))
        if "as_of_bar_date" not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Date).alias("as_of_bar_date"))
        return df


def run(run_date: date) -> None:
    """Job entry point for gold_global_regime (DAILY_SEQUENCE)."""
    module = GlobalIndexRegimeModule()
    out = module.compute_series(run_date)
    if out is None:
        logger.warning(f"[gold_global_regime] {run_date}: nothing written (no data)")
        return
    series, record = out

    GOLD_CROSS_ASSET_PATH.mkdir(parents=True, exist_ok=True)
    cols = [c for c in STORE_COLUMNS if c in series.columns]
    extra = [c for c in series.columns if c not in STORE_COLUMNS]
    atomic_write_parquet(
        series.select(cols + extra),
        GLOBAL_REGIME_STORE_PATH,
        compression="zstd",
        compression_level=3,
    )

    logger.info(
        f"[gold_global_regime] {run_date} | bar={record['as_of_bar_date']}"
        f" | label={record['global_regime_label']} (raw={record['global_regime_label_raw']})"
        f" | global={record['global_risk_score']:.1f}"
        f" | dm_em_divergence={record['dm_em_divergence']:+.1f}"
        f" | asia_pac={record['asia_pac_breadth']:.1f}"
        f" | days_in_state={record['days_in_state']} | flicker_10d={record['flicker_10d']}"
        + (
            f" | TRANSITION: {record['prev_global_regime_label']} -> "
            f"{record['global_regime_label']}"
            if record["regime_transition"] else ""
        )
    )
