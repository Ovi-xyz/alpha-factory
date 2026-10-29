"""
scripts/label_triple_barrier.py — ADR-052 (GMI-TB-001) offline labeling / calibration driver.

OFFLINE ONLY. Wraps src/backtest/triple_barrier.py (LOOKAHEAD BY DESIGN — never part of a
pipeline job or an inference path). Writes under data/backtest/triple_barrier/ and nothing
else; reads Silver 1D bars and (optionally) Gold mtf_alignment files.

Two ways to obtain (symbol, decision_date, direction) triples:

  --from-mtf     the production-shaped sample: every historical gold_mtf row that clears the
                 same gate BacktestEngine/screener use (|mtf_score| >= --min-abs-score and
                 signal_quality in --qualities); direction = sign of mtf_score. `decision_date`
                 is the mtf row's own `date` (the pipeline run date = the trade_date handed
                 to PITDataLoader). Only as deep as the mtf_alignment history on disk.

  --grid         the calibration sample: every --step-th Silver trading day of the last
                 --years years, BOTH directions, for the listed symbols (or --grid-all for
                 every Layer 1 symbol on disk). Unconditional — answers "what do these
                 barriers do to an uninformed entry", which is the base rate any model must
                 beat, and reports clamp shares per market so stop_vol_mult/profit_vol_mult
                 can be checked on the whole universe (KNOWN_RISKS RISK-39).

    python scripts/label_triple_barrier.py --from-mtf
    python scripts/label_triple_barrier.py --grid AAPL,BBCA --years 3
    python scripts/label_triple_barrier.py --grid-all --years 3 --step 5 --inspect 25

--inspect N additionally writes N random labeled trades with their full forward bars to a CSV,
for the manual chart-versus-label check ADR-052 requires before any model trains on the output.

Output tag = the latest decision_date in the sample (not wall-clock), so reruns are
reproducible (FIX BCK-PIT-001 precedent). Exit code 0 on success, 1 on empty input.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from loguru import logger

from src.backtest.pit_data import GOLD_MTF_PATH
from src.backtest.triple_barrier import (
    DIRECTIONS,
    BarrierConfig,
    LabelingResult,
    LookaheadTripleBarrierLabeler,
)
from src.utils.atomic_io import atomic_write_parquet

OUT_DIR = Path("data/backtest/triple_barrier")
# ADR-053 D3: where the weekly fit jobs read labels. A neutral path on purpose — Gold/pipeline
# source may not even name this module (ADR-052 isolation pin), so it names a directory only.
PUBLISH_DIR = Path("data/training/labels")


# ── triple sources ────────────────────────────────────────────────────────────

def triples_from_mtf(
    mtf_glob: str = GOLD_MTF_PATH,
    min_abs_score: int = 3,
    qualities: tuple[str, ...] = ("A", "B"),
) -> pl.DataFrame:
    """(symbol, decision_date, direction) from historical mtf_alignment rows."""
    con = duckdb.connect()
    try:
        raw = con.execute(
            """
            SELECT symbol, CAST(date AS DATE) AS decision_date, mtf_score, signal_quality
            FROM read_parquet($path, hive_partitioning=false, union_by_name=true)
            """,
            {"path": mtf_glob},
        ).pl()
    except Exception as e:                       # no files / unreadable → empty, caller reports
        logger.warning(f"[label_triple_barrier] mtf_alignment not readable ({e})")
        raw = pl.DataFrame(schema={"symbol": pl.String, "decision_date": pl.Date,
                                   "mtf_score": pl.Float64, "signal_quality": pl.String})
    finally:
        con.close()

    return (
        raw.filter(
            (pl.col("mtf_score").abs() >= min_abs_score)
            & pl.col("signal_quality").is_in(list(qualities))
        )
        .with_columns(
            pl.when(pl.col("mtf_score") > 0).then(pl.lit("long")).otherwise(pl.lit("short"))
            .alias("direction")
        )
        .select("symbol", "decision_date", "direction")
        .unique(maintain_order=True)
    )


def grid_triples(
    labeler: LookaheadTripleBarrierLabeler,
    symbols: Optional[list[str]] = None,
    years: float = 3.0,
    step: int = 1,
    today: Optional[date] = None,
) -> pl.DataFrame:
    """
    Unconditional calibration sample: every `step`-th trading day of the last `years` years of
    each symbol's own Silver 1D calendar, both directions. Carries `market` so symbols that
    exist in two Layer 1 markets (e.g. 'CL') are never ambiguous. symbols=None → every Layer 1
    symbol on disk.
    """
    if step < 1:
        raise ValueError("step must be >= 1")
    cutoff = (today or date.today()) - timedelta(days=int(365.25 * years))
    pairs = labeler.layer1_symbols()
    if symbols is not None:
        wanted = set(symbols)
        pairs = [(m, s) for m, s in pairs if s in wanted]

    rows: list[tuple[str, str, date, str]] = []
    for market, symbol in pairs:
        dates = [d for d in labeler.load_bars(symbol, market)["bar_date"].to_list() if d >= cutoff]
        for d in dates[::step]:
            for direction in DIRECTIONS:
                rows.append((symbol, market, d, direction))
    return pl.DataFrame(
        rows, schema=["symbol", "market", "decision_date", "direction"], orient="row"
    ) if rows else pl.DataFrame(schema={"symbol": pl.String, "market": pl.String,
                                        "decision_date": pl.Date, "direction": pl.String})


# ── reporting ─────────────────────────────────────────────────────────────────

def summarise_by_market(labels: pl.DataFrame) -> pl.DataFrame:
    """Per-market calibration table: base rate vs zero-drift reference, clamp shares."""
    if labels.is_empty():
        return pl.DataFrame()
    return (
        labels.group_by("market")
        .agg(
            pl.len().alias("n"),
            pl.col("label_binary").mean().alias("win_rate"),
            (pl.col("stop_bps") / (pl.col("stop_bps") + pl.col("profit_bps"))).mean()
            .alias("zero_drift_ref"),
            (pl.col("stop_clamp") == -1).mean().alias("stop_at_floor"),
            (pl.col("stop_clamp") == 1).mean().alias("stop_at_ceiling"),
            (pl.col("outcome") == "time").mean().alias("time_exit"),
            pl.col("same_bar_ambiguous").mean().alias("same_bar_amb"),
            pl.col("gap_through").mean().alias("gap_through"),
            pl.col("bars_held").mean().alias("mean_bars"),
            pl.col("vol_bps").median().alias("median_vol_bps"),
        )
        .sort("market")
    )


def inspection_sample(
    result: LabelingResult, labeler: LookaheadTripleBarrierLabeler, n: int, seed: int = 52
) -> pl.DataFrame:
    """N random labeled trades with barrier prices and the full forward bars (for chart review)."""
    lab = result.labels
    if lab.is_empty() or n < 1:
        return pl.DataFrame()
    pick = lab.sample(n=min(n, lab.height), seed=seed).sort(["symbol", "decision_date"])
    horizon = labeler.config.horizon_days
    out = []
    for r in pick.iter_rows(named=True):
        bars = labeler.load_bars(r["symbol"], r["market"]).filter(
            pl.col("bar_date") > r["anchor_date"]
        ).head(horizon)
        sign = 1.0 if r["direction"] == "long" else -1.0
        e = r["entry_price"]
        window = " | ".join(
            f"{b['bar_date']} O{b['open']:.4f} H{b['high']:.4f} L{b['low']:.4f} C{b['close']:.4f}"
            for b in bars.iter_rows(named=True)
        )
        out.append({
            "symbol": r["symbol"], "market": r["market"], "decision_date": r["decision_date"],
            "direction": r["direction"], "outcome": r["outcome"], "label": r["label"],
            "bars_held": r["bars_held"], "entry_price": e,
            "profit_price": e * (1 + sign * r["profit_bps"] / 1e4),
            "stop_price": e * (1 - sign * r["stop_bps"] / 1e4),
            "exit_price": r["exit_price"], "realized_bps": r["realized_bps"],
            "same_bar_ambiguous": r["same_bar_ambiguous"], "gap_through": r["gap_through"],
            "forward_bars": window,
        })
    return pl.DataFrame(out)


# ── driver ────────────────────────────────────────────────────────────────────

def _parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ADR-052 triple-barrier labeling (offline, lookahead by design)")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--from-mtf", action="store_true", help="triples from historical mtf_alignment rows")
    src.add_argument("--grid", type=str, help="comma-separated symbols for an unconditional calibration grid")
    src.add_argument("--grid-all", action="store_true", help="calibration grid over every Layer 1 symbol on disk")
    p.add_argument("--mtf-glob", default=GOLD_MTF_PATH)
    p.add_argument("--min-abs-score", type=int, default=3)
    p.add_argument("--qualities", default="A,B")
    p.add_argument("--years", type=float, default=3.0)
    p.add_argument("--step", type=int, default=1)
    p.add_argument("--horizon", type=int, default=7)
    p.add_argument("--entry-mode", default="next_open", choices=["next_open", "signal_close"])
    p.add_argument("--time-policy", default="loss", choices=["loss", "exclude"])
    p.add_argument("--same-bar", default="stop_first", choices=["stop_first", "profit_first", "exclude"])
    p.add_argument("--stop-vol-mult", type=float, default=None)
    p.add_argument("--profit-vol-mult", type=float, default=None)
    p.add_argument("--silver-root", default="data/silver/market_ohlcv")
    p.add_argument("--out-dir", default=str(OUT_DIR))
    p.add_argument("--publish", action="store_true",
                   help="ADR-053 D3: also publish labels_<tag>.parquet to data/training/labels/ for the "
                        "weekly specialist fit jobs (use with --grid-all --step 1)")
    p.add_argument("--publish-dir", default=str(PUBLISH_DIR))
    p.add_argument("--inspect", type=int, default=0, help="write N random trades + forward bars to CSV")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    a = _parse_args(argv)
    overrides = {k: v for k, v in (("stop_vol_mult", a.stop_vol_mult),
                                   ("profit_vol_mult", a.profit_vol_mult)) if v is not None}
    cfg = BarrierConfig(horizon_days=a.horizon, entry_mode=a.entry_mode,
                        time_barrier_policy=a.time_policy, same_bar_policy=a.same_bar, **overrides)
    labeler = LookaheadTripleBarrierLabeler(cfg, silver_root=a.silver_root)

    if a.from_mtf:
        triples = triples_from_mtf(a.mtf_glob, a.min_abs_score, tuple(a.qualities.split(",")))
    else:
        symbols = None if a.grid_all else [s.strip() for s in a.grid.split(",") if s.strip()]
        triples = grid_triples(labeler, symbols, a.years, a.step)

    if triples.is_empty():
        print("No triples to label (empty source / no matching Silver files).")
        return 1

    result = labeler.label(triples)
    tag = triples["decision_date"].max().isoformat()
    out = Path(a.out_dir)
    atomic_write_parquet(result.labels, out / f"labels_{tag}.parquet")
    if a.publish:
        if result.labels.height == 0:
            print("--publish: no labels produced — nothing published", file=sys.stderr)
            return 1
        atomic_write_parquet(result.labels, Path(a.publish_dir) / f"labels_{tag}.parquet")
        print(f"published → {Path(a.publish_dir) / f'labels_{tag}.parquet'}")
    atomic_write_parquet(result.skipped, out / f"skipped_{tag}.parquet")
    summary = result.summary()
    (out / f"summary_{tag}.json").write_text(json.dumps(summary, indent=2, default=str))

    print(json.dumps(summary, indent=2, default=str))
    by_market = summarise_by_market(result.labels)
    if not by_market.is_empty():
        with pl.Config(tbl_rows=50, tbl_cols=20, tbl_width_chars=200):
            print(by_market)
    if a.inspect:
        sample = inspection_sample(result, labeler, a.inspect)
        if not sample.is_empty():
            path = out / f"inspection_{tag}.csv"
            sample.write_csv(path)
            print(f"inspection sample → {path}")
    print(f"labels → {out / f'labels_{tag}.parquet'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
