"""
triple_barrier.py — ADR-052 (GMI-TB-001): Triple-Barrier Labeling for decision_xgboost (ADR-054)

╔══════════════════════════════════════════════════════════════════════════════╗
║  WARNING — LOOKAHEAD BY DESIGN.  LABEL CONSTRUCTION ONLY.                    ║
║                                                                              ║
║  This module reads bars AFTER the decision date. That is the entire point of ║
║  a forward-looking training label, and the exact opposite of what            ║
║  PITDataLoader guarantees (it only ever returns bars strictly BEFORE         ║
║  trade_date). It must NEVER be imported from a live inference path, a        ║
║  pipeline job, a Gold module, or PITDataLoader itself. It is invoked only    ║
║  from offline labeling scripts (scripts/label_triple_barrier.py) and tests.  ║
║  Pinned at source level by tests/integration/test_triple_barrier_            ║
║  isolation_adr052.py. The class is named LookaheadTripleBarrierLabeler so    ║
║  that the name alone cannot be mistaken for a PIT-safe reader.               ║
╚══════════════════════════════════════════════════════════════════════════════╝

What it produces
    For each (symbol, decision_date, direction) one label row: did a 2–7 trading-day
    swing setup reach its profit barrier before its stop barrier ("win"), or not ("loss").

Time convention (ADR-052 Amendment A1 — why the input key is `decision_date`)
    PITDataLoader shows a decision made on date T only the bars with trade date < T.
    The Gold rows a model is trained on (mtf_alignment, signal_aggregation, ...) are
    stamped with the pipeline RUN date, i.e. that same T. This module therefore keys on
    `decision_date` = T and derives the *anchor bar* itself: the last bar with trade date
    strictly < T — exactly the last bar the model could see. Barriers are scanned over the
    bars with trade date > anchor_date (ADR-052: "timestamp > entry_date", with the ADR's
    entry_date being this anchor bar). Feeding a run date straight into a `> run_date`
    scan would skip the first tradable session (bar T) — a one-bar misalignment between
    features and labels. PIT visibility `< T` and label visibility `> anchor` partition
    time at T with no overlap and no gap.

    Trading date of a Silver 1D bar = the timestamp's OWN calendar date in its own zone
    (stamped 00:00 local exchange time; src/utils/trade_date.py). Polars `.dt.date()` on
    the tz-aware column returns exactly that — never convert the zone.

Barrier construction (jointly, not three independent ranges)
    s = clip(stop_vol_mult   * vol_bps, 50, 100)            stop distance, bps
    R = clip(profit_vol_mult * vol_bps / s, 2.0, 3.0)       reward:risk
    p = R * s                                               profit distance, bps
    Because s ∈ [50,100] and R ∈ [2,3], p ∈ [100,300] holds BY CONSTRUCTION — the profit
    band is implied, never filtered afterwards, and a 100/100 (R:R 1:1) pair cannot be
    produced. BarrierConfig refuses (ValueError) any band combination for which that
    guarantee would not hold.
    vol_bps = mean true range / previous close over the last `vol_lookback` bars ending
    at the anchor bar (PIT-safe: uses nothing after the anchor).

Touch rules (daily bars)
    Long:  profit when high >= entry*(1+p), stop when low <= entry*(1-s). Short mirrored.
    Gap-through: a bar that OPENS beyond a barrier hits it at the open (fill = open, so a
    stop gap realises more than s) and that barrier is first, whatever the rest of the bar
    does. Same-bar double touch (neither barrier hit by the open, both by the range) is
    unresolvable on daily bars: flagged `same_bar_ambiguous` and decided by
    `same_bar_policy` (default stop_first — conservative).
    No touch within the horizon → time-barrier exit (see policy below).

Explicit choices the ADR left open (each is a stated parameter, recorded on every row in
`label_spec`, never a silent default)
    time_barrier_policy = "loss"    ("exclude" drops those rows to `skipped` instead).
        Rationale: success means the target was reached. Excluding time exits keeps only
        decisive outcomes and inflates the hit rate that ADR-054's >= 0.51 gate measures;
        at inference the model cannot know a setup will be decisive. realized_bps and
        bars_held are always stored so ADR-054 can re-derive any alternative without
        relabeling.
    entry_mode = "next_open"        entry at the open of the first bar after the anchor
        (the only entry a daily pipeline can actually take; its high/low count for that
        bar). "signal_close" = entry at the anchor close (what BacktestEngine does today).
    horizon_days ∈ [2, 7], default 7: the time barrier. A touch on bar 1 is a real touch;
        `bars_held` is stored so ADR-054 may filter on holding time.
    Bars with open/close outside [low, high] are repaired to the enclosing range, not dropped.
    Bars are judged by structure (finite, > 0, high >= low), NOT by Silver `is_clean`:
        is_clean=False also marks real |z|>4 moves — exactly the bars most likely to touch
        a barrier — and dropping them would bias labels. The count is reported as
        n_unclean_in_horizon. A structurally invalid bar inside the horizon skips the row
        (the horizon cannot be assessed honestly).

Leakage hygiene for ADR-054
    `label_end_date` (resolution bar, or last horizon bar for a time exit) is stored so
    training folds can be purged/embargoed on [decision_date, label_end_date]. Rows whose
    full horizon is not yet observed are NOT labeled (skipped: horizon_incomplete) —
    a partial horizon would mislabel genuine time exits.

Out of scope (GD §0.3): costs, slippage, position sizing — labels are pure price geometry
in bps/ratios, never dollar amounts.

Calibration status: first-pass. stop_vol_mult / profit_vol_mult come from two live
symbols only (see BarrierConfig); scripts/label_triple_barrier.py --grid reports clamp
shares and base rates over the whole universe so they can be checked empirically
(KNOWN_RISKS RISK-39).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import polars as pl
from loguru import logger

LABELER_VERSION = "1.0"

SILVER_OHLCV_ROOT = Path("data/silver/market_ohlcv")
CONTEXT_MARKET = "context"          # Layer 2 — never labeled (ADR-003: not tradeable here)

LONG = "long"
SHORT = "short"
DIRECTIONS = (LONG, SHORT)

OUTCOME_PROFIT = "profit"
OUTCOME_STOP = "stop"
OUTCOME_TIME = "time"

MIN_HORIZON_DAYS = 2
MAX_HORIZON_DAYS = 7

ENTRY_MODES = ("next_open", "signal_close")
TIME_BARRIER_POLICIES = ("loss", "exclude")
SAME_BAR_POLICIES = ("stop_first", "profit_first", "exclude")

# integer outcome codes used inside the vectorised resolver
_PROFIT, _STOP, _TIME, _EXCLUDED_SAME_BAR = 0, 1, 2, 3
_OUTCOME_NAMES = {_PROFIT: OUTCOME_PROFIT, _STOP: OUTCOME_STOP, _TIME: OUTCOME_TIME}

# skip reasons (every unlabeled input row is reported with exactly one of these)
SKIP_NOT_LAYER1 = "not_layer1"
SKIP_NO_SILVER_FILE = "no_silver_file"
SKIP_AMBIGUOUS_MARKET = "ambiguous_market"
SKIP_NO_PRIOR_BAR = "no_prior_bar"
SKIP_VOL_UNAVAILABLE = "vol_unavailable"
SKIP_ANCHOR_STALE = "anchor_stale"
SKIP_HORIZON_INCOMPLETE = "horizon_incomplete"
SKIP_INVALID_BAR = "invalid_bar_in_horizon"
SKIP_INVALID_ENTRY = "invalid_entry_price"
SKIP_EXCLUDED_TIME = "excluded_time_barrier"
SKIP_EXCLUDED_SAME_BAR = "excluded_same_bar"

_LABEL_COLUMNS = [
    "symbol", "market", "decision_date", "direction",
    "anchor_date", "first_bar_date", "entry_price",
    "vol_bps", "stop_bps", "profit_bps", "rr", "stop_clamp", "rr_clamp",
    "outcome", "label", "label_binary", "bars_held", "label_end_date",
    "exit_price", "realized_bps", "same_bar_ambiguous", "gap_through",
    "span_calendar_days", "n_unclean_in_horizon",
    "label_spec", "labeler_version",
]
_SKIP_COLUMNS = ["symbol", "decision_date", "direction", "reason"]


# ═════════════════════════════════════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class BarrierConfig:
    """Every knob of the labeler. Validated at construction."""

    horizon_days:      int   = 7
    stop_floor_bps:    float = 50.0
    stop_ceil_bps:     float = 100.0
    profit_floor_bps:  float = 100.0
    profit_ceil_bps:   float = 300.0
    rr_min:            float = 2.0
    rr_max:            float = 3.0
    # First-pass calibration (RISK-39): chosen so the typical Layer 1 equity lands inside the
    # band rather than on its edge — measured on live Silver (AAPL, BBCA, 2023-10..2026-10)
    # median true range ≈ 185 bps, so 0.375 * 185 ≈ 69 bps (band midpoint 75). The
    # profit/stop multiplier ratio 2.5 = the midpoint R:R. At 0.5/1.25 the same two symbols
    # sat on the 100 bps ceiling 53% of days (vol scaling carried no information); at
    # 0.375/0.9375 it is 17.5% (floor 4.2%).
    # Validate on the full universe with scripts/label_triple_barrier.py --grid.
    stop_vol_mult:     float = 0.375
    profit_vol_mult:   float = 0.9375
    vol_lookback:      int   = 14
    min_vol_obs:       int   = 10
    max_anchor_staleness_days: int = 7
    entry_mode:           str = "next_open"
    time_barrier_policy:  str = "loss"
    same_bar_policy:      str = "stop_first"

    def __post_init__(self) -> None:
        if not (MIN_HORIZON_DAYS <= self.horizon_days <= MAX_HORIZON_DAYS):
            raise ValueError(
                f"horizon_days must be in [{MIN_HORIZON_DAYS}, {MAX_HORIZON_DAYS}] "
                f"(ADR-052), got {self.horizon_days}"
            )
        if not (0 < self.stop_floor_bps <= self.stop_ceil_bps):
            raise ValueError("need 0 < stop_floor_bps <= stop_ceil_bps")
        if not (0 < self.profit_floor_bps <= self.profit_ceil_bps):
            raise ValueError("need 0 < profit_floor_bps <= profit_ceil_bps")
        if not (0 < self.rr_min <= self.rr_max):
            raise ValueError("need 0 < rr_min <= rr_max")
        # The joint-construction guarantee: every achievable p = R*s must land inside the
        # profit band. If it cannot, the constructor refuses rather than letting a caller
        # discover it through filtered-out rows later.
        tol = 1e-9
        if self.rr_min * self.stop_floor_bps < self.profit_floor_bps - tol:
            raise ValueError(
                "rr_min * stop_floor_bps falls below profit_floor_bps: the profit band "
                "cannot be guaranteed by construction"
            )
        if self.rr_max * self.stop_ceil_bps > self.profit_ceil_bps + tol:
            raise ValueError(
                "rr_max * stop_ceil_bps exceeds profit_ceil_bps: the profit band "
                "cannot be guaranteed by construction"
            )
        if self.stop_vol_mult <= 0 or self.profit_vol_mult <= 0:
            raise ValueError("stop_vol_mult and profit_vol_mult must be > 0")
        if self.vol_lookback < 2:
            raise ValueError("vol_lookback must be >= 2")
        if not (1 <= self.min_vol_obs <= self.vol_lookback):
            raise ValueError("need 1 <= min_vol_obs <= vol_lookback")
        if self.max_anchor_staleness_days < 1:
            raise ValueError("max_anchor_staleness_days must be >= 1")
        if self.entry_mode not in ENTRY_MODES:
            raise ValueError(f"entry_mode must be one of {ENTRY_MODES}")
        if self.time_barrier_policy not in TIME_BARRIER_POLICIES:
            raise ValueError(f"time_barrier_policy must be one of {TIME_BARRIER_POLICIES}")
        if self.same_bar_policy not in SAME_BAR_POLICIES:
            raise ValueError(f"same_bar_policy must be one of {SAME_BAR_POLICIES}")

    @property
    def spec(self) -> str:
        """Compact self-description stamped on every label row (auditability)."""
        return (
            f"tb{LABELER_VERSION}|h{self.horizon_days}|{self.entry_mode}"
            f"|time={self.time_barrier_policy}|same={self.same_bar_policy}"
            f"|tr{self.vol_lookback}x{self.stop_vol_mult:g}/{self.profit_vol_mult:g}"
            f"|s{self.stop_floor_bps:g}-{self.stop_ceil_bps:g}"
            f"|rr{self.rr_min:g}-{self.rr_max:g}"
        )


# ═════════════════════════════════════════════════════════════════════════════
# Barrier widths (pure, vectorised — single source of truth)
# ═════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class BarrierWidths:
    stop_bps:   float
    profit_bps: float
    rr:         float
    stop_clamp: int     # -1 at floor, 0 free, +1 at ceiling
    rr_clamp:   int     # -1 at rr_min, 0 free, +1 at rr_max


def compute_barrier_widths_vec(vol_bps: np.ndarray, cfg: BarrierConfig) -> dict:
    """
    Vectorised (stop, profit, rr) from volatility. Non-finite / non-positive vol yields
    NaN widths and valid=False — the caller skips those rows.
    """
    vol = np.asarray(vol_bps, dtype=float)
    valid = np.isfinite(vol) & (vol > 0)
    safe = np.where(valid, vol, 1.0)

    s_raw = cfg.stop_vol_mult * safe
    stop = np.clip(s_raw, cfg.stop_floor_bps, cfg.stop_ceil_bps)
    stop_clamp = np.where(s_raw < cfg.stop_floor_bps, -1,
                          np.where(s_raw > cfg.stop_ceil_bps, 1, 0)).astype(np.int8)

    rr_raw = (cfg.profit_vol_mult * safe) / stop
    rr = np.clip(rr_raw, cfg.rr_min, cfg.rr_max)
    rr_clamp = np.where(rr_raw < cfg.rr_min, -1,
                        np.where(rr_raw > cfg.rr_max, 1, 0)).astype(np.int8)

    # Float-noise guard only: by construction rr*stop already lies in the profit band
    # (BarrierConfig refuses configs where it could not), so this never changes a value
    # by more than rounding error.
    profit = np.clip(rr * stop, cfg.profit_floor_bps, cfg.profit_ceil_bps)

    nan = np.full_like(stop, np.nan)
    return {
        "valid":      valid,
        "stop_bps":   np.where(valid, stop, nan),
        "profit_bps": np.where(valid, profit, nan),
        "rr":         np.where(valid, profit / stop, nan),
        "stop_clamp": stop_clamp,
        "rr_clamp":   rr_clamp,
    }


def compute_barrier_widths(vol_bps: float, cfg: BarrierConfig) -> Optional[BarrierWidths]:
    """Scalar wrapper over the vectorised implementation. None when vol is unusable."""
    out = compute_barrier_widths_vec(np.array([vol_bps], dtype=float), cfg)
    if not bool(out["valid"][0]):
        return None
    return BarrierWidths(
        stop_bps=float(out["stop_bps"][0]),
        profit_bps=float(out["profit_bps"][0]),
        rr=float(out["rr"][0]),
        stop_clamp=int(out["stop_clamp"][0]),
        rr_clamp=int(out["rr_clamp"][0]),
    )


# ═════════════════════════════════════════════════════════════════════════════
# Barrier resolution (pure, vectorised)
# ═════════════════════════════════════════════════════════════════════════════

def resolve_barriers_vec(
    o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray,
    entry: np.ndarray, direction: str,
    stop_bps: np.ndarray, profit_bps: np.ndarray,
    same_bar_policy: str = "stop_first",
) -> dict:
    """
    Resolve the first barrier touched for m setups over an (m, H) forward window.

    o/h/l/c: (m, H) open/high/low/close of the H bars after the anchor.
    entry, stop_bps, profit_bps: (m,).
    Returns arrays: code (0 profit, 1 stop, 2 time, 3 excluded same-bar), bar_index
    (0-based position of the resolving bar; H-1 for a time exit), exit_price,
    same_bar_ambiguous, gap_through.
    """
    if direction not in DIRECTIONS:
        raise ValueError(f"direction must be one of {DIRECTIONS}")
    if same_bar_policy not in SAME_BAR_POLICIES:
        raise ValueError(f"same_bar_policy must be one of {SAME_BAR_POLICIES}")

    m, horizon = o.shape
    rows = np.arange(m)
    sign = 1.0 if direction == LONG else -1.0

    pt = entry * (1.0 + sign * profit_bps / 1e4)         # profit barrier price (m,)
    st = entry * (1.0 - sign * stop_bps / 1e4)           # stop barrier price (m,)
    pt2, st2 = pt[:, None], st[:, None]

    if direction == LONG:
        gap_p = o >= pt2
        gap_s = o <= st2
        hit_p = gap_p | (h >= pt2)
        hit_s = gap_s | (l <= st2)
    else:
        gap_p = o <= pt2
        gap_s = o >= st2
        hit_p = gap_p | (l <= pt2)
        hit_s = gap_s | (h >= st2)

    any_hit = hit_p | hit_s
    has_hit = any_hit.any(axis=1)
    k = np.where(has_hit, np.argmax(any_hit, axis=1), horizon - 1)

    gp = gap_p[rows, k] & has_hit
    gs = gap_s[rows, k] & has_hit
    hp = hit_p[rows, k] & has_hit
    hs = hit_s[rows, k] & has_hit

    ambiguous = hp & hs & ~gp & ~gs                      # both inside the bar, neither at the open
    profit_m = gp | (hp & ~hs)
    stop_m = (~gp) & (gs | (hs & ~hp))
    if same_bar_policy == "profit_first":
        profit_m = profit_m | ambiguous
    elif same_bar_policy == "stop_first":
        stop_m = stop_m | ambiguous
    excluded = ambiguous & (same_bar_policy == "exclude")

    code = np.full(m, _TIME, dtype=np.int8)
    code[profit_m] = _PROFIT
    code[stop_m] = _STOP
    code[excluded] = _EXCLUDED_SAME_BAR

    open_k = o[rows, k]
    exit_price = c[rows, horizon - 1].astype(float).copy()          # time exit default
    exit_price = np.where(profit_m, np.where(gp, open_k, pt), exit_price)
    exit_price = np.where(stop_m, np.where(gs, open_k, st), exit_price)

    return {
        "code":               code,
        "bar_index":          k.astype(np.int64),
        "exit_price":         exit_price,
        "same_bar_ambiguous": ambiguous,
        "gap_through":        gp | gs,
    }


# ═════════════════════════════════════════════════════════════════════════════
# Result container
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class LabelingResult:
    labels:  pl.DataFrame
    skipped: pl.DataFrame
    spec:    str

    def summary(self) -> dict:
        """Calibration / audit summary — the empirical-validation entry point."""
        lab, sk = self.labels, self.skipped
        out: dict = {
            "label_spec": self.spec,
            "n_labeled": lab.height,
            "n_skipped": sk.height,
            "skipped_by_reason": (
                {r["reason"]: r["len"] for r in sk.group_by("reason").len().to_dicts()}
                if sk.height else {}
            ),
        }
        if lab.is_empty():
            return out

        n = lab.height
        s, p = lab["stop_bps"], lab["profit_bps"]
        out.update({
            "win_rate": float(lab["label_binary"].mean()),
            "win_rate_by_direction": {
                r["direction"]: float(r["win_rate"])
                for r in lab.group_by("direction")
                .agg(pl.col("label_binary").mean().alias("win_rate")).to_dicts()
            },
            "outcome_share": {
                r["outcome"]: r["len"] / n for r in lab.group_by("outcome").len().to_dicts()
            },
            # zero-drift continuous-monitoring reference for "profit before stop";
            # ignores the time barrier, so it is a reference point, not a prediction
            "zero_drift_reference_win_rate": float((s / (s + p)).mean()),
            "same_bar_ambiguous_rate": float(lab["same_bar_ambiguous"].mean()),
            "gap_through_rate": float(lab["gap_through"].mean()),
            "stop_clamp_share": {
                "floor": float((lab["stop_clamp"] == -1).mean()),
                "free": float((lab["stop_clamp"] == 0).mean()),
                "ceiling": float((lab["stop_clamp"] == 1).mean()),
            },
            "rr_clamp_share": {
                "min": float((lab["rr_clamp"] == -1).mean()),
                "free": float((lab["rr_clamp"] == 0).mean()),
                "max": float((lab["rr_clamp"] == 1).mean()),
            },
            "mean_bars_held": float(lab["bars_held"].mean()),
            "mean_stop_bps": float(s.mean()),
            "mean_profit_bps": float(p.mean()),
            "mean_rr": float(lab["rr"].mean()),
        })
        return out


# ═════════════════════════════════════════════════════════════════════════════
# Labeler
# ═════════════════════════════════════════════════════════════════════════════

class LookaheadTripleBarrierLabeler:
    """
    Forward-looking triple-barrier labeler. LABEL CONSTRUCTION ONLY — see module header.

    labeler = LookaheadTripleBarrierLabeler()
    result  = labeler.label(triples)     # triples: symbol, decision_date, direction[, market]
    result.labels / result.skipped / result.summary()
    """

    def __init__(
        self,
        config: Optional[BarrierConfig] = None,
        silver_root: Path | str = SILVER_OHLCV_ROOT,
        allowed_markets: Optional[Iterable[str]] = None,
    ) -> None:
        self.config = config or BarrierConfig()
        self.silver_root = Path(silver_root)
        # None → derived lazily from the instrument registry (Layer 1 only).
        self._allowed_markets = list(allowed_markets) if allowed_markets is not None else None

    # ── public ──────────────────────────────────────────────────────────────

    def label(self, triples: pl.DataFrame) -> LabelingResult:
        triples = self._normalise_triples(triples)
        spec = self.config.spec
        labels: list[pl.DataFrame] = []
        skipped: list[pl.DataFrame] = []

        for (symbol, market_hint), grp in triples.group_by(
            ["symbol", "market"], maintain_order=True
        ):
            market, reason = self.resolve_market(symbol, market_hint)
            if market is None:
                skipped.append(self._skip_frame(grp, reason))
                continue
            bars = self.load_bars(symbol, market)
            lab, sk = self._label_symbol(symbol, market, bars, grp)
            if lab is not None and lab.height:
                labels.append(lab)
            if sk is not None and sk.height:
                skipped.append(sk)

        labels_df = (
            pl.concat(labels, how="vertical_relaxed").sort(
                ["symbol", "decision_date", "direction"]
            ).select(_LABEL_COLUMNS)
            if labels else self._empty_labels()
        )
        skipped_df = (
            pl.concat(skipped, how="vertical_relaxed").sort(
                ["symbol", "decision_date", "direction"]
            ).select(_SKIP_COLUMNS)
            if skipped else self._empty_skipped()
        )
        logger.info(
            f"[TripleBarrier] {labels_df.height} labeled, {skipped_df.height} skipped | {spec}"
        )
        return LabelingResult(labels=labels_df, skipped=skipped_df, spec=spec)

    # ── input handling ──────────────────────────────────────────────────────

    @staticmethod
    def _normalise_triples(triples: pl.DataFrame) -> pl.DataFrame:
        cols = set(triples.columns)
        if "decision_date" not in cols:
            hint = (
                " The ADR's `entry_date` is the ANCHOR bar this module derives itself; the"
                " input key is the decision/run date (the trade_date handed to PITDataLoader)."
                " See module header, Amendment A1."
                if "entry_date" in cols else ""
            )
            raise ValueError(
                "triples must have columns symbol, decision_date, direction"
                f" (optional: market).{hint}"
            )
        missing = {"symbol", "direction"} - cols
        if missing:
            raise ValueError(f"triples missing columns: {sorted(missing)}")

        df = triples
        dtype = df.schema["decision_date"]
        if dtype == pl.String:
            df = df.with_columns(pl.col("decision_date").str.to_date())
        elif isinstance(dtype, pl.Datetime):
            df = df.with_columns(pl.col("decision_date").dt.date())
        elif dtype != pl.Date:
            raise ValueError(f"decision_date must be Date/Datetime/String, got {dtype}")

        if "market" not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.String).alias("market"))
        df = df.select(
            pl.col("symbol").cast(pl.String),
            pl.col("market").cast(pl.String),
            pl.col("decision_date"),
            pl.col("direction").cast(pl.String).str.to_lowercase(),
        )
        bad = df.filter(~pl.col("direction").is_in(list(DIRECTIONS)))
        if bad.height:
            raise ValueError(
                f"direction must be 'long' or 'short'; got {bad['direction'].unique().to_list()}"
            )
        n0 = df.height
        df = df.unique(subset=["symbol", "market", "decision_date", "direction"],
                       keep="first", maintain_order=True)
        if df.height < n0:
            logger.debug(f"[TripleBarrier] dropped {n0 - df.height} duplicate input triples")
        return df

    # ── market / file resolution ────────────────────────────────────────────

    def _layer1_markets(self) -> list[str]:
        if self._allowed_markets is None:
            from src.utils.silver_scope import layer1_markets
            self._allowed_markets = layer1_markets()
        return self._allowed_markets

    def _bar_path(self, market: str, symbol: str) -> Path:
        return self.silver_root / market / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"

    def resolve_market(
        self, symbol: str, market_hint: Optional[str] = None
    ) -> tuple[Optional[str], Optional[str]]:
        """(market, None) when the symbol resolves to exactly one Layer 1 Silver 1D file,
        else (None, skip_reason)."""
        allowed = self._layer1_markets()
        if market_hint:
            if market_hint == CONTEXT_MARKET or market_hint not in allowed:
                return None, SKIP_NOT_LAYER1
            return (market_hint, None) if self._bar_path(market_hint, symbol).exists() \
                else (None, SKIP_NO_SILVER_FILE)

        found = [m for m in allowed if self._bar_path(m, symbol).exists()]
        if len(found) == 1:
            return found[0], None
        if len(found) > 1:
            # e.g. 'CL' is both the commodity WTI proxy and the us_stocks Colgate ticker —
            # a bare symbol cannot say which; the caller must pass `market`.
            return None, SKIP_AMBIGUOUS_MARKET
        if self._bar_path(CONTEXT_MARKET, symbol).exists():
            return None, SKIP_NOT_LAYER1
        return None, SKIP_NO_SILVER_FILE

    def layer1_symbols(self) -> list[tuple[str, str]]:
        """[(market, symbol)] for every Layer 1 Silver 1D file on disk (sorted)."""
        out: list[tuple[str, str]] = []
        for market in self._layer1_markets():
            root = self.silver_root / market
            if not root.exists():
                continue
            for d in sorted(root.glob("symbol=*")):
                sym = d.name.split("=", 1)[1]
                if self._bar_path(market, sym).exists():
                    out.append((market, sym))
        return out

    def load_bars(self, symbol: str, market: str) -> pl.DataFrame:
        """Silver 1D bars with `bar_date` (the stamp's own date), sorted, de-duplicated."""
        df = pl.read_parquet(self._bar_path(market, symbol))
        keep = [c for c in ("timestamp", "open", "high", "low", "close", "is_clean")
                if c in df.columns]
        df = df.select(keep)
        if "is_clean" not in df.columns:
            df = df.with_columns(pl.lit(True).alias("is_clean"))
        # trading date = the stamp's OWN calendar date (never convert the zone)
        df = df.with_columns(pl.col("timestamp").dt.date().alias("bar_date"))
        return (
            df.sort("bar_date")
            .unique(subset=["bar_date"], keep="last", maintain_order=True)
            .with_columns(pl.col("is_clean").fill_null(False))
        )

    # ── per-symbol labeling ─────────────────────────────────────────────────

    @staticmethod
    def _skip_frame(grp: pl.DataFrame, reason: str, mask: Optional[np.ndarray] = None) -> pl.DataFrame:
        g = grp if mask is None else grp.filter(pl.Series(mask))
        return g.select("symbol", "decision_date", "direction").with_columns(
            pl.lit(reason).alias("reason")
        )

    def _trailing_vol_bps(self, o, h, l, c, valid) -> np.ndarray:
        """
        Mean true range / previous close (bps) over `vol_lookback` bars ending at each
        index, from valid bars only; NaN until `min_vol_obs` valid observations exist.
        Value at index i uses bars <= i only (PIT-safe).
        """
        n = len(c)
        cfg = self.config
        tr = np.full(n, np.nan)
        if n > 1:
            pc = c[:-1]
            ok = valid[1:] & valid[:-1]
            rng = np.maximum.reduce([
                h[1:] - l[1:], np.abs(h[1:] - pc), np.abs(l[1:] - pc)
            ])
            with np.errstate(invalid="ignore", divide="ignore"):
                trp = np.where(ok, rng / pc * 1e4, np.nan)
            tr[1:] = trp
        good = np.isfinite(tr)
        csum = np.concatenate([[0.0], np.cumsum(np.where(good, tr, 0.0))])
        ccnt = np.concatenate([[0], np.cumsum(good.astype(np.int64))])
        idx = np.arange(n)
        lo = np.maximum(idx - cfg.vol_lookback + 1, 0)
        s = csum[idx + 1] - csum[lo]
        k = ccnt[idx + 1] - ccnt[lo]
        with np.errstate(invalid="ignore", divide="ignore"):
            vol = np.where(k >= cfg.min_vol_obs, s / np.maximum(k, 1), np.nan)
        return vol

    def _label_symbol(
        self, symbol: str, market: str, bars: pl.DataFrame, grp: pl.DataFrame
    ) -> tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
        cfg = self.config
        horizon = cfg.horizon_days
        n = bars.height

        o = bars["open"].cast(pl.Float64).to_numpy()
        h = bars["high"].cast(pl.Float64).to_numpy()
        l = bars["low"].cast(pl.Float64).to_numpy()
        c = bars["close"].cast(pl.Float64).to_numpy()
        clean = bars["is_clean"].to_numpy()
        bar_dates = bars["bar_date"].to_numpy()                      # datetime64[D]

        with np.errstate(invalid="ignore"):
            valid = (
                np.isfinite(o) & np.isfinite(h) & np.isfinite(l) & np.isfinite(c)
                & (o > 0) & (h > 0) & (l > 0) & (c > 0) & (h >= l)
            )
        # A bar's extremes must contain its open and close. Feeds that print open/close
        # outside [low, high] (common in forex; the dominant is_clean=False cause) are
        # repaired to the enclosing range instead of discarded — the traded range was at
        # least that wide, and discarding would thin out whole forex horizons.
        h = np.where(valid, np.maximum.reduce([h, o, c]), h)
        l = np.where(valid, np.minimum.reduce([l, o, c]), l)
        vol_all = self._trailing_vol_bps(o, h, l, c, valid)

        dec_np = grp["decision_date"].to_numpy().astype("datetime64[D]")
        dir_np = grp["direction"].to_numpy()
        m_all = grp.height

        reason = np.full(m_all, "", dtype=object)

        anchor = np.searchsorted(bar_dates, dec_np, side="left") - 1  # last bar date < decision
        no_prior = anchor < 0
        reason[no_prior] = SKIP_NO_PRIOR_BAR
        a = np.clip(anchor, 0, max(n - 1, 0))

        vol = vol_all[a] if n else np.full(m_all, np.nan)
        bad_vol = (reason == "") & ~np.isfinite(vol)
        reason[bad_vol] = SKIP_VOL_UNAVAILABLE

        age_days = (dec_np - bar_dates[a]).astype("timedelta64[D]").astype(np.int64) if n \
            else np.zeros(m_all, dtype=np.int64)
        stale = (reason == "") & (age_days > cfg.max_anchor_staleness_days)
        reason[stale] = SKIP_ANCHOR_STALE

        incomplete = (reason == "") & (a + horizon > n - 1)
        reason[incomplete] = SKIP_HORIZON_INCOMPLETE

        # window indices (clipped so fancy-indexing is safe; masked rows are discarded below)
        win = np.clip(a[:, None] + np.arange(1, horizon + 1)[None, :], 0, max(n - 1, 0))
        win_valid = valid[win].all(axis=1)
        bad_win = (reason == "") & ~win_valid
        reason[bad_win] = SKIP_INVALID_BAR

        if cfg.entry_mode == "next_open":
            entry = o[win[:, 0]]
        else:
            entry = c[a]
            bad_entry = (reason == "") & ~valid[a]
            reason[bad_entry] = SKIP_INVALID_ENTRY

        widths = compute_barrier_widths_vec(vol, cfg)
        bad_w = (reason == "") & ~widths["valid"]
        reason[bad_w] = SKIP_VOL_UNAVAILABLE

        ok = reason == ""
        per_direction: list[dict] = []        # local — no shared mutable state (GD §17.7)

        for direction in DIRECTIONS:
            sel = ok & (dir_np == direction)
            if not sel.any():
                continue
            idx = np.flatnonzero(sel)
            res = resolve_barriers_vec(
                o[win[idx]], h[win[idx]], l[win[idx]], c[win[idx]],
                entry[idx], direction,
                widths["stop_bps"][idx], widths["profit_bps"][idx],
                cfg.same_bar_policy,
            )
            code = res["code"]
            reason[idx[code == _EXCLUDED_SAME_BAR]] = SKIP_EXCLUDED_SAME_BAR
            if cfg.time_barrier_policy == "exclude":
                reason[idx[code == _TIME]] = SKIP_EXCLUDED_TIME
            per_direction.append({**res, "_idx": idx, "_direction": direction})

        # assemble labeled rows
        label_frames: list[pl.DataFrame] = []
        for res in per_direction:
            idx, direction = res["_idx"], res["_direction"]
            keep = reason[idx] == ""
            if not keep.any():
                continue
            ii = idx[keep]
            kb = res["bar_index"][keep]
            code = res["code"][keep]
            wcols = win[ii]
            end_idx = wcols[np.arange(len(ii)), kb]
            first_idx = wcols[:, 0]
            sign = 1.0 if direction == LONG else -1.0
            e = entry[ii]
            exit_price = res["exit_price"][keep]
            realized = sign * (exit_price / e - 1.0) * 1e4
            is_win = code == _PROFIT

            label_frames.append(pl.DataFrame({
                "symbol": [symbol] * len(ii),
                "market": [market] * len(ii),
                "decision_date": grp["decision_date"].gather(ii),
                "direction": [direction] * len(ii),
                "anchor_date": bars["bar_date"].gather(a[ii]),
                "first_bar_date": bars["bar_date"].gather(first_idx),
                "entry_price": e,
                "vol_bps": vol[ii],
                "stop_bps": widths["stop_bps"][ii],
                "profit_bps": widths["profit_bps"][ii],
                "rr": widths["rr"][ii],
                "stop_clamp": widths["stop_clamp"][ii],
                "rr_clamp": widths["rr_clamp"][ii],
                "outcome": [_OUTCOME_NAMES[int(x)] for x in code],
                "label": np.where(is_win, "win", "loss"),
                "label_binary": is_win.astype(np.int8),
                "bars_held": (kb + 1).astype(np.int32),
                "label_end_date": bars["bar_date"].gather(end_idx),
                "exit_price": exit_price,
                "realized_bps": realized,
                "same_bar_ambiguous": res["same_bar_ambiguous"][keep],
                "gap_through": res["gap_through"][keep],
                "span_calendar_days": (
                    (bar_dates[wcols[:, -1]] - bar_dates[a[ii]])
                    .astype("timedelta64[D]").astype(np.int64)
                ).astype(np.int32),
                "n_unclean_in_horizon": (~clean[wcols]).sum(axis=1).astype(np.int32),
                "label_spec": [cfg.spec] * len(ii),
                "labeler_version": [LABELER_VERSION] * len(ii),
            }).with_columns(
                pl.col("stop_clamp").cast(pl.Int8),
                pl.col("rr_clamp").cast(pl.Int8),
            ))

        # skipped rows (every non-empty reason, including policy exclusions)
        skip_mask = reason != ""
        sk_df = None
        if skip_mask.any():
            sk_df = grp.filter(pl.Series(skip_mask)).select(
                "symbol", "decision_date", "direction"
            ).with_columns(pl.Series("reason", reason[skip_mask].astype(str)))

        lab_df = pl.concat(label_frames, how="vertical_relaxed") if label_frames else None
        return lab_df, sk_df

    # ── empties ─────────────────────────────────────────────────────────────

    @staticmethod
    def _empty_labels() -> pl.DataFrame:
        return pl.DataFrame(schema={
            "symbol": pl.String, "market": pl.String, "decision_date": pl.Date,
            "direction": pl.String, "anchor_date": pl.Date, "first_bar_date": pl.Date,
            "entry_price": pl.Float64, "vol_bps": pl.Float64, "stop_bps": pl.Float64,
            "profit_bps": pl.Float64, "rr": pl.Float64, "stop_clamp": pl.Int8,
            "rr_clamp": pl.Int8, "outcome": pl.String, "label": pl.String,
            "label_binary": pl.Int8, "bars_held": pl.Int32, "label_end_date": pl.Date,
            "exit_price": pl.Float64, "realized_bps": pl.Float64,
            "same_bar_ambiguous": pl.Boolean, "gap_through": pl.Boolean,
            "span_calendar_days": pl.Int32, "n_unclean_in_horizon": pl.Int32,
            "label_spec": pl.String, "labeler_version": pl.String,
        })

    @staticmethod
    def _empty_skipped() -> pl.DataFrame:
        return pl.DataFrame(schema={
            "symbol": pl.String, "decision_date": pl.Date,
            "direction": pl.String, "reason": pl.String,
        })


__all__ = [
    "BarrierConfig", "BarrierWidths", "LabelingResult", "LookaheadTripleBarrierLabeler",
    "compute_barrier_widths", "compute_barrier_widths_vec", "resolve_barriers_vec",
    "LABELER_VERSION", "DIRECTIONS", "LONG", "SHORT",
]
