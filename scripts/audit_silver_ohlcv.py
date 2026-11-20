"""
scripts/audit_silver_ohlcv.py — thorough audit of the Silver OHLCV tree. READ-ONLY: it reads Parquet and
prints a report; it writes nothing (``--format json`` prints to stdout, redirect it if you want a file).

Why this exists (FIX SIL-AUDIT-OHLCV-01, RISK-50, 14 Nov 2026). Two Silver defects were found by hand, one
file at a time, after the fact:

  * 1M fragments (RISK-49). YM_F, NKD_F, NQ_F, ES_F, AU, AG and CL carry five to eight one-day bars stamped on
    the ingest days of November 2026 and NO bar stamped 2026-11-01. Measured on YM_F (14 Nov 2026): 152 of 158
    monthly stamps fall on day 1, the other six are 6, 9, 10, 11, 12 and 13 Nov. The period-dedupe fix keeps
    the earliest fragment per period; for these seven that is still a fragment, not a month-to-date bar.
  * ABBV 1W anchored on Tuesday. Measured on ABBV (14 Nov 2026): 713 of 726 weekly stamps are Tuesday
    (2013-01-01 .. 2026-08-25), 13 are Monday (from 2026-08-17); AAPL is 793 of 793 Monday. The Tuesday bars
    are NOT corrupted: against the daily bars they match Tuesday..Monday blocks (close 518 of 521, volume 520
    of 521, low 504, high 498, open 490) and fail the Monday..Friday week (close 71 of 521 at a 5e-4 tolerance).
    The values are right and the calendar is shifted, so a Friday-close check alone calls it "wrong data" and a
    weekday census alone calls it "wrong label". The audit therefore runs BOTH reconciliations (below) and
    reports which one fails. Why the source anchors that way is not established here (Bronze was not read).

Silver itself keeps no per-bar lineage (``data_source`` is the FIRST Bronze row's ``_source`` for the whole
file, so a mixed-source file is invisible), which is why every check below is a measurement against the
daily bars or against an invariant of the schema, never against a recorded cause.

Checks (``--list-checks``; ``--checks a,b`` selects; default: all). Severity: FAIL = a structural invariant is
broken (exit 1); WARN = statistical or convention finding (exit 0 unless ``--strict``); INFO = census only.

  schema          GD §4.3 columns, dtypes, tz-aware stamp, symbol / timeframe columns match the path, empty or
                  unreadable file.                                                                  [FAIL]
  timestamp       null / unsorted / exact-duplicate stamps; a 1D/1W/1M stamp later than local hour
                  _COARSE_STAMP_MAX_LOCAL_HOUR (not a date label); weekend-dated 1D bars (WARN).
  period          1W/1M: more than one bar in a period (the processor's own period_key_expr).       [FAIL]
  anchor          1M: a stamp not on day 1.                                                         [FAIL]
                  1W: a symbol whose dominant stamp weekday differs from its market's (census, judged only
                  when at least 3 symbols agree and they are at least half the market) [FAIL]; stray
                  stamps on another weekday [WARN]. Prints the census so the baseline is visible.
  window          1W/1M vs 1D, three reconciliations of the CLOSE: IMPLIED (daily bars inside [stamp,
                  next stamp), capped at one period so a gap cannot inflate it; anchor agnostic: value
                  integrity), IMPLIED END-INCLUSIVE (FX-style one-day overlap) and CALENDAR (Sunday..Saturday
                  week / calendar month: convention). Implied fails = value defect; calendar fails and
                  implied holds = anchor defect (ABBV); prints the match rate per year.             [WARN]
  gaps            1W/1M: periods between the first and last bar that hold no bar (YM_F 1M: 27 of 180). [WARN]
  recent          1W/1M (not FX): the newest RECENT_PERIODS closed bars each reconcile to a daily close on at
                  least one window. The overall --min-match share cannot see two frozen months out of 122
                  (98%); the recent window can. FAIL on a Layer 1 market when a bar is off by more than
                  RECENT_FAIL_GAP, else WARN (FIX SIL-AUDIT-OHLCV-03).
  seam            a split factor in a close-to-close ratio. REVERSING jumps are a FAIL on 1D only (the
                  processor quarantines the 1D whole); on 1W/1M a reversal is an ordinary move of a
                  volatile name and is a WARN (SIL-SEAM-SCOPE-01).
  current_period  1W/1M: the period that holds the newest 1D bar must have a bar stamped at its start whose
                  open equals the first daily open (a month-to-date bar). Only fragments => FAIL (YM_F).
  ohlc            non-finite / sanity-violating bars carrying is_clean=True [FAIL]; negative volume [FAIL];
                  non-positive price [WARN, CL traded below zero on 2020-04-20]; flat-bar and is_clean=False
                  counts [INFO].
  derived         log_return, dollar_volume, spread_hl recomputed from OHLCV (they are derived on every
                  write, so a mismatch is a stale or hand-edited file).                             [FAIL]
  meta            processing_version != the processor's CURRENT_SILVER_VERSION or mixed [WARN]; null
                  is_adjusted / adj_factor [WARN]; several data_source values / adj_factor != 1 [INFO].
  freshness       1D: symbols whose newest stamp trails their market's newest by more than --stale-days.
  coverage        registry symbols with no Silver file [FAIL]; Silver symbols not in the registry and
                  symbols missing a timeframe that most of their market has [WARN]. Skipped when
                  --market / --symbol narrow the run.

Thresholds: ``--tol`` (relative tolerance, default 1e-3), ``--vol-tol``, ``--min-match`` (default 0.95),
``--stale-days`` are STARTING VALUES [U], calibrated on five real series only (see the comment above
DEFAULT_TOL); the report prints the measured rates so they can be set from data. The window check judges
CLOSE only: open/high/low/volume are reported, not judged (AAPL: open misses 6% of weeks, 4 a year clustered
around Feb/May/Aug/Nov, while close agrees to 6e-7).

    python scripts/audit_silver_ohlcv.py                          # 1D/1W/1M, every market, text report
    python scripts/audit_silver_ohlcv.py --tf all --market idx    # every timeframe, one market
    python scripts/audit_silver_ohlcv.py --symbol ABBV --symbol YM_F --checks anchor,window,current_period
    python scripts/audit_silver_ohlcv.py --format json > silver_audit.json
    python scripts/audit_silver_ohlcv.py --list-checks

Exit code 1 if any FAIL (or any WARN with --strict), 0 otherwise.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable, Optional

import polars as pl

from src.silver.ohlcv_processor import (
    _COARSE_STAMP_MAX_LOCAL_HOUR,
    _PERIOD_DEDUP_TIMEFRAMES,
    BASIS_BAND,
    BASIS_FAIL_LOG,
    BASIS_MIN_PERIODS,
    BASIS_SHARE,
    CURRENT_SILVER_VERSION,
    SEAM_REVERT_BARS,
    SEAM_TOL,
    SPLIT_FACTORS,
    coarse_rebuild_exclusion,
    find_split_seams,
    period_key_expr,
    reversing_seams,
)

SILVER_ROOT = Path("data/silver/market_ohlcv")
COARSE_TFS = ("1D", "1W", "1M")
FAIL, WARN, INFO = "FAIL", "WARN", "INFO"
_SEVERITY_ORDER = {FAIL: 0, WARN: 1, INFO: 2}
WEEKDAY = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}

# GD §4.3 Silver OHLCV schema (ohlcv_processor module docstring).
SILVER_COLUMNS = (
    "symbol", "timestamp", "timeframe", "open", "high", "low", "close", "volume", "is_adjusted", "adj_factor",
    "vwap", "log_return", "dollar_volume", "spread_hl", "is_clean", "data_source", "processing_version",
)
_PRICE_COLS = ("open", "high", "low", "close")
_BLOCKING_COLS = ("timestamp",) + _PRICE_COLS

# [U] starting values, see the module docstring. Printed next to the measured rates, never silently applied.
# Calibration (14 Nov 2026, close match of 1W/1M bars vs 1D, tol 1e-3, five real series): AAPL 1W 1.000,
# BBCA 1W 0.987, YM_F 1M 0.991, ABBV 1W 0.99 (own window) / 0.156 (calendar week), EUR_USD 1W 0.583 (end-inclusive).
# Healthy series sit at >= 0.987, the defects at <= 0.58, so 0.95 has a wide margin; n = 5 series, hence [U].
DEFAULT_TOL = 1e-3          # relative tolerance, weekly/monthly bar vs the aggregated daily bars (10 bps)
DEFAULT_VOL_TOL = 1e-2      # volume relative tolerance (sum of daily volume)
DEFAULT_MIN_MATCH = 0.95    # share of verifiable bars whose close must reconcile before a WARN is raised
DEFAULT_STALE_DAYS = 4      # calendar days a 1D file may trail its market (weekend + one holiday)
_DERIVED_TOL = 1e-9         # derived fields are recomputed exactly; this only absorbs float noise
_ANCHOR_MIN_AGREE = 3       # symbols that must share a weekday before a market "has" an anchor

# FIX SIL-AUDIT-OHLCV-02 [chat thread, 17 Nov 2026] — all [U], calibrated on six real series (ALL, CSX, MNST,
# ABBV, KOSPI, plus the 14 Nov JSON): ALL / CSX sit at ratio 1.00416 / 1.0027 on 519 of 533 weeks, MNST 1M at
# exactly 0.5 on 118 of 123 months.
LAYER1_MARKETS = frozenset({"commodity", "forex", "idx", "us_stocks", "index_futures"})   # src.utils.silver_scope
# BASIS_MIN_PERIODS, BASIS_SHARE, BASIS_BAND, BASIS_FAIL_LOG, SPLIT_FACTORS, SEAM_TOL, SEAM_REVERT_BARS and the
# seam / constant-ratio functions are imported from src.silver.ohlcv_processor (below): the processor acts on the
# same rules (SIL-COARSE-REBUILD-01, SIL-SEAM-QUARANTINE-01), so the yardstick and the code cannot drift.
SPLIT_NEAR = 0.03           # a ratio within 3% of k or 1/k is "split-like" (wording only)
# FIX SIL-AUDIT-OHLCV-03 [chat thread, 19 Nov 2026] - [U]: the recent-period check. Measured on MARA / CLSK 1M:
# Aug and Sep 2026 closes frozen at a first snapshot (2 of 122 bars, up to 14% off) pass the 0.95 overall share.
RECENT_PERIODS = {"1W": 26, "1M": 12}    # half a year of weekly bars, a year of monthly bars
RECENT_FAIL_GAP = 0.01                   # a recent close this far from every window's daily close is a stale value

# Silver -> Gold gate. A consumer is BLOCKed by any FAIL on a series it reads, and needs ATTENTION for a WARN of
# the listed checks. technical_xgboost reads tech_signals 1D/1W/1M of the Layer 1 markets
# (src/gold/intelligence/features_technical.py: MARKETS, TIMEFRAMES); the cross-asset engine reads 1D only, of
# Layer 1 and Layer 2 (src/gold/cross_asset/*: every reader globs *_1D_silver.parquet).
GATE_CONSUMERS: dict[str, tuple[Optional[frozenset], frozenset]] = {
    "technical_xgboost": (LAYER1_MARKETS, frozenset({"1D", "1W", "1M"})),
    "cross_asset_xgboost": (None, frozenset({"1D"})),
}
GATE_ATTENTION = frozenset({"basis", "seam", "gaps", "window", "timestamp", "freshness", "last_bar", "ohlc", "recent"})

_UNSET = object()


@dataclass(frozen=True)
class Finding:
    check: str
    severity: str
    market: str
    symbol: str
    timeframe: str
    message: str
    detail: dict = field(default_factory=dict, compare=False, hash=False)


@dataclass(frozen=True)
class SilverFile:
    market: str
    symbol: str          # from the directory name symbol=<SYM>
    timeframe: str
    path: Path
    name_symbol: str     # from the file name <SYM>_<TF>_silver.parquet


@dataclass
class Options:
    silver_root: Path = SILVER_ROOT
    tol: float = DEFAULT_TOL
    vol_tol: float = DEFAULT_VOL_TOL
    min_match: float = DEFAULT_MIN_MATCH
    stale_days: int = DEFAULT_STALE_DAYS
    # FIX SIL-AUDIT-OHLCV-03: the default was () = ZERO checks, and a gate built from zero checks reads PASS. An
    # audit run through the Python API (not the CLI, which always filled the list) judged nothing and passed.
    checks: tuple[str, ...] = field(default_factory=lambda: tuple(CHECKS))
    use_registry: bool = True
    filtered: bool = False      # --market / --symbol narrowed the run (coverage is then meaningless)


@dataclass
class AuditResult:
    findings: list[Finding]
    facts: dict[str, list]
    n_files: int
    n_read: int
    symbols: int
    timeframes: tuple[str, ...]
    checks: tuple[str, ...]


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Discovery and loading
# ─────────────────────────────────────────────────────────────────────────────────────────────────

def discover(
    silver_root: Path,
    timeframes: Optional[tuple[str, ...]] = COARSE_TFS,
    market: Optional[str] = None,
    symbols: Optional[set[str]] = None,
) -> list[SilverFile]:
    """{market}/symbol={SYM}/{SYM}_{TF}_silver.parquet -> sorted SilverFile list. ``timeframes=None`` = all.
    The timeframe is the text after the LAST underscore of the stem (symbols such as EUR_USD / YM_F hold
    underscores themselves)."""
    out: list[SilverFile] = []
    for path in silver_root.glob("*/symbol=*/*_silver.parquet"):
        mkt = path.parent.parent.name
        if market and mkt != market:
            continue
        sym = path.parent.name.split("=", 1)[1]
        if symbols and sym not in symbols:
            continue
        stem = path.name[: -len("_silver.parquet")]
        if "_" not in stem:
            continue
        name_sym, tf = stem.rsplit("_", 1)
        if timeframes is not None and tf not in timeframes:
            continue
        out.append(SilverFile(mkt, sym, tf, path, name_sym))
    return sorted(out, key=lambda r: (r.market, r.symbol, r.timeframe))


def prepare(df: pl.DataFrame) -> pl.DataFrame:
    """Add the stamp's OWN local calendar fields (no zone conversion, src/utils/trade_date.py convention)."""
    if isinstance(df.schema.get("timestamp"), pl.Datetime):
        df = df.with_columns(
            pl.col("timestamp").dt.date().alias("_stamp"),
            pl.col("timestamp").dt.weekday().alias("_wd"),
            pl.col("timestamp").dt.day().alias("_dom"),
            pl.col("timestamp").dt.hour().alias("_hour"),
        )
    return df


class Ctx:
    """What one check sees for one Silver file."""

    def __init__(self, f: SilverFile, df: pl.DataFrame, opts: Options,
                 daily_loader: Callable[[], Optional[pl.DataFrame]], facts: dict[str, list]):
        self.f, self.df, self.opts, self.facts = f, df, opts, facts
        self._daily_loader = daily_loader
        self._daily: Any = _UNSET
        self._recon: Any = _UNSET

    def recon(self) -> Optional[pl.DataFrame]:
        """reconcile_windows of this coarse file against 1D, computed once for every check that needs it."""
        if self._recon is _UNSET:
            d = self.daily
            self._recon = (None if d is None or self.f.timeframe not in _PERIOD_DEDUP_TIMEFRAMES
                           else reconcile_windows(self.df, d, self.f.timeframe, self.opts.tol, self.opts.vol_tol))
        return self._recon

    @property
    def daily(self) -> Optional[pl.DataFrame]:
        if self._daily is _UNSET:
            self._daily = self._daily_loader()
        return self._daily

    def finding(self, check: str, severity: str, message: str, **detail) -> Finding:
        return Finding(check, severity, self.f.market, self.f.symbol, self.f.timeframe, message, detail)


def _examples(values, n: int = 3) -> list[str]:
    return [str(v) for v in list(values)[:n]]


def _span(values) -> str:
    vals = [v for v in values if v is not None]
    return f"{min(vals)}..{max(vals)}" if vals else "-"


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Per-file checks
# ─────────────────────────────────────────────────────────────────────────────────────────────────

def check_schema(c: Ctx) -> list[Finding]:
    f, df = c.f, c.df
    out: list[Finding] = []
    if f.name_symbol != f.symbol:
        out.append(c.finding("schema", FAIL, f"file name symbol {f.name_symbol!r} differs from directory "
                             f"symbol={f.symbol!r}"))
    if df.height == 0:
        out.append(c.finding("schema", FAIL, "file holds no rows", blocking=True))
        return out
    missing = [col for col in SILVER_COLUMNS if col not in df.columns]
    if missing:
        out.append(c.finding("schema", FAIL, f"missing GD §4.3 column(s): {', '.join(missing)}",
                             blocking=any(m in _BLOCKING_COLS for m in missing), missing=missing))
    ts = df.schema.get("timestamp")
    if ts is not None:
        if not isinstance(ts, pl.Datetime):
            out.append(c.finding("schema", FAIL, f"timestamp dtype is {ts}, not Datetime", blocking=True))
        elif ts.time_zone is None:
            out.append(c.finding("schema", FAIL, "timestamp is tz-naive; Silver stamps carry their local zone"))
    for col in _PRICE_COLS:
        dt = df.schema.get(col)
        if dt is not None and not dt.is_numeric():
            out.append(c.finding("schema", FAIL, f"{col} dtype is {dt}, not numeric", blocking=True))
    for col in ("volume",):
        dt = df.schema.get(col)
        if dt is not None and not dt.is_numeric():
            out.append(c.finding("schema", FAIL, f"{col} dtype is {dt}, not numeric"))
    if "is_clean" in df.columns:
        if df.schema["is_clean"] != pl.Boolean:
            out.append(c.finding("schema", FAIL, f"is_clean dtype is {df.schema['is_clean']}, not Boolean"))
        elif df["is_clean"].null_count():
            # _flag_is_clean ANDs a z-score that is null on the first bar (log_return is null there), and
            # Kleene logic makes True & null = null: measured on 10 of 10 real files, always row 0.
            nulls = df.with_row_index("_i").filter(pl.col("is_clean").is_null())
            first_null_lr = "log_return" in df.columns and df["log_return"][0] is None
            other = nulls.filter(~((pl.col("_i") == 0) & first_null_lr))
            if other.height:
                out.append(c.finding("schema", FAIL, f"is_clean has {other.height} null(s) other than the first "
                                     f"bar's", n_rows=other.height))
            if nulls.height > other.height:
                out.append(c.finding("schema", INFO, "first bar's is_clean is null (its log_return is null, so "
                                     "_flag_is_clean ANDs a null z-score); `is_clean = TRUE` readers drop that bar"))
    for col, want in (("symbol", f.symbol), ("timeframe", f.timeframe)):
        if col in df.columns:
            got = set(df[col].drop_nulls().unique().to_list())
            if got != {want}:
                out.append(c.finding("schema", FAIL, f"{col} column holds {sorted(map(str, got))}, path says "
                                     f"{want!r}"))
    return out


def check_timestamp(c: Ctx) -> list[Finding]:
    f, df = c.f, c.df
    out: list[Finding] = []
    ts = df["timestamp"]
    if ts.null_count():
        out.append(c.finding("timestamp", FAIL, f"{ts.null_count()} null stamp(s)"))
    inversions = int((ts.diff().dt.total_microseconds() < 0).sum())
    if inversions:
        out.append(c.finding("timestamp", FAIL, f"{inversions} out-of-order stamp(s); Silver is written sorted"))
    dup_mask = ts.is_duplicated() & ts.is_not_null()
    if dup_mask.any():
        stamps = ts.filter(dup_mask).unique().sort()
        out.append(c.finding("timestamp", FAIL, f"{int(dup_mask.sum())} row(s) share {len(stamps)} exact stamp(s), "
                             f"e.g. {', '.join(_examples(stamps))}", n_rows=int(dup_mask.sum())))
    if f.timeframe in COARSE_TFS and "_hour" in df.columns:
        late = df.filter(pl.col("_hour") > _COARSE_STAMP_MAX_LOCAL_HOUR)
        if late.height:
            out.append(c.finding("timestamp", FAIL, f"{late.height} {f.timeframe} stamp(s) later than local hour "
                                 f"{_COARSE_STAMP_MAX_LOCAL_HOUR} (not a date label), e.g. "
                                 f"{', '.join(_examples(late['timestamp']))}", n_rows=late.height))
    if f.timeframe == "1D" and "_wd" in df.columns:
        wk = df.filter(pl.col("_wd") >= 6)
        if wk.height:
            flat = ""
            if all(col in df.columns for col in _PRICE_COLS):
                n_flat = wk.filter((pl.col("open") == pl.col("high")) & (pl.col("high") == pl.col("low"))
                                   & (pl.col("low") == pl.col("close"))).height
                flat = f", {n_flat} fully flat (O=H=L=C)"
            out.append(c.finding("timestamp", WARN, f"{wk.height} weekend-dated 1D bar(s) "
                                 f"({_span(wk['_stamp'])}){flat}", n_rows=wk.height,
                                 first=str(wk["_stamp"].min()), last=str(wk["_stamp"].max())))
    return out


def check_period(c: Ctx) -> list[Finding]:
    tf = c.f.timeframe
    if tf not in _PERIOD_DEDUP_TIMEFRAMES or "_stamp" not in c.df.columns:
        return []
    dup = (c.df.with_columns(period_key_expr(tf).alias("_p"))
           .group_by("_p").agg(pl.len().alias("n"), pl.col("_stamp").sort().alias("stamps"))
           .filter(pl.col("n") > 1).sort("_p"))
    if not dup.height:
        return []
    first = dup.row(0, named=True)
    return [c.finding(
        "period", FAIL,
        f"{dup.height} period(s) hold more than one bar; first {first['_p']}: {first['n']} bars stamped "
        f"{', '.join(str(s) for s in first['stamps'])}",
        periods=[{"period": str(r["_p"]), "n": r["n"], "stamps": [str(s) for s in r["stamps"]]}
                 for r in dup.head(10).iter_rows(named=True)],
        n_periods=dup.height)]


def check_anchor(c: Ctx) -> list[Finding]:
    """1M: stamps must sit on day 1. 1W: collect the weekday histogram (judged against the market in
    post_anchor)."""
    tf, df = c.f.timeframe, c.df
    if tf not in _PERIOD_DEDUP_TIMEFRAMES or "_stamp" not in df.columns:
        return []
    if tf == "1M":
        off = df.filter(pl.col("_dom") != 1)
        if not off.height:
            return []
        return [c.finding(
            "anchor", FAIL,
            f"{off.height} of {df.height} monthly stamp(s) are not on day 1 ({_span(off['_stamp'])}): "
            f"{', '.join(_examples(off['_stamp'], 6))} — partial-window bars, not month-start bars",
            n_off=off.height, stamps=[str(s) for s in off["_stamp"].to_list()[:12]])]
    hist = {}
    for wd, grp in df.group_by("_wd"):
        hist[int(wd[0])] = (grp.height, grp["_stamp"].min(), grp["_stamp"].max())
    c.facts["weekday"].append((c.f.market, tf, c.f.symbol, hist))
    return []


def _rate_pair(a: pl.Expr, b: pl.Expr, tol: float) -> pl.Expr:
    return ((a - b).abs() <= tol * b.abs()).fill_null(False)


def _agg_daily(daily: pl.DataFrame, key: str) -> pl.DataFrame:
    return (daily.sort("_stamp").group_by(key, maintain_order=True)
            .agg(pl.col("open").first().alias("d_open"), pl.col("high").max().alias("d_high"),
                 pl.col("low").min().alias("d_low"), pl.col("close").last().alias("d_close"),
                 pl.col("volume").sum().alias("d_vol"), pl.len().alias("n_days"),
                 pl.col("_stamp").last().alias("d_last")))


def reconcile_windows(bars: pl.DataFrame, daily: pl.DataFrame, tf: str,
                      tol: float = DEFAULT_TOL, vol_tol: float = DEFAULT_VOL_TOL) -> pl.DataFrame:
    """One row per weekly/monthly bar, reconciled against the daily bars three ways.

    IMPLIED ``i``: the daily bars in [stamp, window end), the end being the next stamp CAPPED at one nominal
    period (7 days / the next month start) so a missing bar cannot inflate its neighbour's window. Anchor
    agnostic: tests whether the bar's VALUES aggregate real daily bars whatever week it claims to be.
    IMPLIED END-INCLUSIVE ``e``: the same plus the daily bar stamped on the window end. Measured on EUR_USD 1W:
    the Monday-stamped bar closes at the NEXT Monday's daily close (a one-day FX convention overlap), which
    an exclusive window can never match.
    CALENDAR ``c``: the processor's own period (Sunday..Saturday week keyed to Monday / calendar month) —
    tests the CONVENTION.
    The newest bar is open-ended (still forming): flagged ``open_ended`` and left out of the rates."""
    nominal = (pl.col("_stamp").dt.offset_by("7d") if tf == "1W"
               else pl.col("_stamp").dt.month_start().dt.offset_by("1mo"))
    b = bars.sort("timestamp").with_row_index("_bid").with_columns(period_key_expr(tf).alias("_period"))
    b = b.with_columns(pl.min_horizontal(pl.col("_stamp").shift(-1), nominal).alias("_wend"))
    last_id = b["_bid"].max()
    d = daily.sort("_stamp")

    keys = b.select("_bid", pl.col("_stamp").alias("_wstart"), "_wend").sort("_wstart")
    assigned = (d.join_asof(keys, left_on="_stamp", right_on="_wstart", strategy="backward")
                .filter(pl.col("_bid").is_not_null() & (pl.col("_stamp") < pl.col("_wend"))))
    imp = _agg_daily(assigned, "_bid")
    imp = imp.rename({c: f"i_{c}" for c in imp.columns if c != "_bid"})

    endrow = d.select(pl.col("_stamp").alias("_wend"), pl.col("high").alias("end_h"), pl.col("low").alias("end_l"),
                      pl.col("close").alias("end_c"), pl.col("volume").cast(pl.Float64).alias("end_v"))

    cal = _agg_daily(d.with_columns(period_key_expr(tf).alias("_period")), "_period")
    cal = cal.rename({c: f"c_{c}" for c in cal.columns if c != "_period"})

    out = (b.join(imp, on="_bid", how="left").join(endrow, on="_wend", how="left")
           .join(cal, on="_period", how="left"))
    out = out.with_columns(
        pl.col("i_d_open").alias("e_d_open"),
        pl.max_horizontal("i_d_high", "end_h").alias("e_d_high"),
        pl.min_horizontal("i_d_low", "end_l").alias("e_d_low"),
        pl.coalesce("end_c", "i_d_close").alias("e_d_close"),
        (pl.col("i_d_vol").cast(pl.Float64) + pl.col("end_v").fill_null(0.0)).alias("e_d_vol"),
        pl.col("i_n_days").alias("e_n_days"),
    )
    exprs = [(pl.col("_bid") == last_id).alias("open_ended")]
    for p in ("i", "e", "c"):
        exprs += [
            _rate_pair(pl.col("open"), pl.col(f"{p}_d_open"), tol).alias(f"{p}_open"),
            _rate_pair(pl.col("high"), pl.col(f"{p}_d_high"), tol).alias(f"{p}_high"),
            _rate_pair(pl.col("low"), pl.col(f"{p}_d_low"), tol).alias(f"{p}_low"),
            _rate_pair(pl.col("close"), pl.col(f"{p}_d_close"), tol).alias(f"{p}_close"),
            _rate_pair(pl.col("volume").cast(pl.Float64), pl.col(f"{p}_d_vol").cast(pl.Float64), vol_tol)
            .alias(f"{p}_vol"),
            (pl.col(f"{p}_n_days").fill_null(0) > 0).alias(f"{p}_verifiable"),
        ]
    full = out.with_columns(exprs)
    keep = ["_bid", "timestamp", "_stamp", "_period", "_wend", "open", "high", "low", "close", "volume",
            "open_ended"]
    keep += [c for c in full.columns if c.startswith(("i_", "e_", "c_"))]
    return full.select(keep)


def match_rates(rec: pl.DataFrame, prefix: str) -> dict:
    """Share of verifiable, non-open-ended bars whose field reconciles. ``n`` = bars judged."""
    ok = rec.filter(~pl.col("open_ended") & pl.col(f"{prefix}_verifiable"))
    n = ok.height
    rates: dict[str, Any] = {"n": n, "unverifiable": int(rec.filter(~pl.col("open_ended")
                                                                     & ~pl.col(f"{prefix}_verifiable")).height)}
    for fld in ("open", "high", "low", "close", "vol"):
        rates[fld] = (ok[f"{prefix}_{fld}"].sum() / n) if n else None
    return rates


def _by_year(rec: pl.DataFrame, pre: str) -> dict[int, float]:
    """Close-match rate per stamp year for window variant ``pre`` (where reconciliation breaks = the era of the
    Bronze file that carries the defect)."""
    ok = rec.filter(~pl.col("open_ended") & pl.col(f"{pre}_verifiable"))
    if not ok.height:
        return {}
    g = ok.group_by(pl.col("_stamp").dt.year().alias("y")).agg(pl.col(f"{pre}_close").mean().alias("r")).sort("y")
    return {int(y): float(r) for y, r in g.iter_rows()}


def _fmt_rates(r: dict) -> str:
    if not r["n"]:
        return "no verifiable bars"
    return " ".join(f"{k}={r[k]:.3f}" for k in ("open", "high", "low", "close", "vol")) + f" (n={r['n']})"


def check_window(c: Ctx) -> list[Finding]:
    """Judged on CLOSE only. Measured on the AAPL control (531 weeks) close agrees with the daily bars to 6e-7
    while open reconciles on 94% (33 misses, 4 a year, clustered around Feb/May/Aug/Nov with three stragglers
    in late Jan / Oct — quarterly ex-dividend timing, so a different adjustment basis between the 1W and 1D
    series [I]); open, high, low and volume are therefore reported, not judged."""
    tf = c.f.timeframe
    if tf not in _PERIOD_DEDUP_TIMEFRAMES or "_stamp" not in c.df.columns:
        return []
    daily = c.daily
    if daily is None:
        return [c.finding("window", INFO, "no readable 1D file with a stamp: reconciliation skipped")]
    rec = c.recon()
    ri, re_, rc = match_rates(rec, "i"), match_rates(rec, "e"), match_rates(rec, "c")
    inclusive = bool(re_["n"]) and (not ri["n"] or re_["close"] > ri["close"])
    best, variant, pre = (re_, "end-inclusive", "e") if inclusive else (ri, "exclusive", "i")
    c.facts["recon"].append((c.f.market, tf, c.f.symbol, best, rc, variant))
    if not best["n"] and not rc["n"]:
        return [c.finding("window", INFO, "no bar overlaps the 1D history: nothing to reconcile")]
    m = c.opts.min_match
    unit = "week" if tf == "1W" else "month"
    if best["n"] and best["close"] < m:
        bad = (rec.filter(~pl.col("open_ended") & pl.col(f"{pre}_verifiable") & ~pl.col(f"{pre}_close"))
               .select("_stamp", "close", f"{pre}_d_close").head(3))
        ex = "; ".join(f"{r['_stamp']}: bar {r['close']:.6g} vs daily {r[f'{pre}_d_close']:.6g}"
                       for r in bad.iter_rows(named=True))
        years = _by_year(rec, pre)
        step = [b for b in (basis_step(rec, q, c.opts.tol) for q in ("i", "c")) if b]
        hint = (f"; constant ratio {max(step, key=lambda x: x['n_constant'])['ratio']:.5g} on most mismatching bars — "
                f"an adjustment-basis step, see the basis check, not a window defect" if step else "")
        return [c.finding("window", WARN,
                          f"close does not equal the daily close of the bar's own window ({variant}): "
                          f"{best['close']:.3f} of {best['n']} bars [min-match {m}, tol {c.opts.tol}]; "
                          f"{_fmt_rates(best)}; by year: {' '.join(f'{y} {r:.2f}' for y, r in years.items())}; "
                          f"e.g. {ex}{hint}", implied=best, calendar=rc, variant=variant, by_year=years)]
    if variant == "end-inclusive":
        return [c.finding("window", INFO, f"bars reconcile only with the window end INCLUDED (the daily bar "
                          f"stamped on the next bar's date belongs to this bar, FX-style): {_fmt_rates(best)}; "
                          f"calendar-{unit} verdict not applied", implied=best, calendar=rc, variant=variant)]
    if rc["n"] and rc["close"] < m:
        return [c.finding("window", WARN,
                          f"anchor defect: close equals the daily close of the bar's own stamp window "
                          f"({best['close']:.3f} of {best['n']}) but not of the calendar {unit} "
                          f"({rc['close']:.3f} of {rc['n']}) — values consistent, calendar shifted "
                          f"[min-match {m}, tol {c.opts.tol}]; {_fmt_rates(rc)}",
                          implied=best, calendar=rc, variant=variant)]
    return []


def check_current_period(c: Ctx) -> list[Finding]:
    """The period holding the newest 1D bar needs a bar stamped at (or, for Sunday-stamped forex weeks,
    before) its start. Presence only: comparing that bar's open with the first daily open was tried and
    dropped — on a dividend payer the two series sit on different adjustment bases (AAPL 2026-11-09: 345.00
    vs 344.74, a 7.6e-4 gap on a perfectly formed bar)."""
    tf = c.f.timeframe
    if tf not in _PERIOD_DEDUP_TIMEFRAMES or "_stamp" not in c.df.columns:
        return []
    daily = c.daily
    if daily is None or not daily.height:
        return []
    ref = daily["_stamp"].max()
    period = daily.with_columns(period_key_expr(tf).alias("_period")).filter(pl.col("_stamp") == ref)["_period"][0]
    bars = c.df.with_columns(period_key_expr(tf).alias("_period")).filter(pl.col("_period") == period)
    if not bars.height:
        return [c.finding("current_period", WARN,
                          f"no {tf} bar for the current period {period} (newest 1D bar is {ref}); the {tf} file "
                          f"trails the 1D file", period=str(period), ref=str(ref))]
    if bars.filter(pl.col("_stamp") <= period).height:
        return []
    stamps = [str(s) for s in bars.sort("timestamp")["_stamp"].to_list()]
    return [c.finding("current_period", FAIL,
                      f"no bar stamped at the start of the current period {period}; only {len(stamps)} bar(s) "
                      f"stamped {', '.join(stamps)} — fragments, not a {'month' if tf == '1M' else 'week'}-to-date "
                      f"bar", period=str(period), stamps=stamps, ref=str(ref))]


def check_gaps(c: Ctx) -> list[Finding]:
    """1W/1M: periods between the first and last bar that hold no bar. Measured on YM_F 1M (14 Nov 2026):
    27 of 180 months have no bar (2012-01, 2012-04, ... 2026-02, 2026-03). A weekly series can lose a week
    only to a full-week closure, so any gap is worth a look; WARN, not FAIL, because Bronze may never have
    held the period."""
    tf = c.f.timeframe
    if tf not in _PERIOD_DEDUP_TIMEFRAMES or "_stamp" not in c.df.columns or not c.df.height:
        return []
    keys = c.df.select(period_key_expr(tf).alias("_p")).unique().sort("_p")["_p"]
    expected = pl.date_range(keys.min(), keys.max(), "1w" if tf == "1W" else "1mo", eager=True)
    missing = expected.filter(~expected.is_in(keys.implode()))
    if not missing.len():
        return []
    base = (f"{missing.len()} of {expected.len()} {tf} period(s) between {keys.min()} and "
            f"{keys.max()} have no bar, e.g. {', '.join(_examples(missing, 6))}")
    daily = c.daily
    if daily is None or not daily.height:
        return [c.finding("gaps", WARN, base, n_missing=missing.len(), n_expected=expected.len(),
                          missing=[str(x) for x in missing.to_list()[:24]])]
    # FIX SIL-AUDIT-OHLCV-02: a period with no bar AND no 1D bar is a closure (JKSE, SSEC, TWSE holiday weeks:
    # the 14 Nov audit WARNed on all of them); a period that holds 1D bars is a hole that can be rebuilt; a period
    # before the 1D history cannot be told apart.
    first_daily = daily.select(period_key_expr(tf).alias("_p"))["_p"].min()
    dperiods = daily.select(period_key_expr(tf).alias("_p")).unique()["_p"]
    holes = [m for m in missing.to_list() if m >= first_daily and m in set(dperiods.to_list())]
    closed = [m for m in missing.to_list() if m >= first_daily and m not in set(dperiods.to_list())]
    pre = [m for m in missing.to_list() if m < first_daily]
    detail = dict(n_missing=missing.len(), n_expected=expected.len(), n_holes=len(holes), n_closed=len(closed),
                  n_pre_1d=len(pre), missing=[str(x) for x in missing.to_list()[:24]],
                  holes=[str(x) for x in holes[:24]])
    explain = f"{len(holes)} hold 1D bars (rebuildable), {len(pre)} predate the 1D history, {len(closed)} have no 1D bar either (closure)"
    if holes or pre:
        return [c.finding("gaps", WARN, f"{base}; {explain}", **detail)]
    return [c.finding("gaps", INFO, f"{base}; {explain}", **detail)]


def check_ohlc(c: Ctx) -> list[Finding]:
    df = c.df
    if not all(col in df.columns for col in _PRICE_COLS):
        return []
    out: list[Finding] = []
    clean = pl.col("is_clean").fill_null(False) if "is_clean" in df.columns else pl.lit(False)
    finite = pl.all_horizontal([(pl.col(x).is_not_null() & pl.col(x).is_finite()).fill_null(False)
                                for x in _PRICE_COLS])
    nonfinite = df.filter(~finite)
    if nonfinite.height:
        n_clean = nonfinite.filter(clean).height
        if n_clean:
            out.append(c.finding("ohlc", FAIL, f"{n_clean} bar(s) with null/non-finite OHLC carry is_clean=True, "
                                 f"e.g. {', '.join(_examples(nonfinite.filter(clean)['timestamp']))}",
                                 n_rows=n_clean))
        else:
            out.append(c.finding("ohlc", INFO, f"{nonfinite.height} bar(s) with null/non-finite OHLC, all "
                                 f"flagged is_clean=False"))
    o, h, lo, cl = (pl.col(x) for x in _PRICE_COLS)
    sane = ((h >= lo) & (o >= lo) & (o <= h) & (cl >= lo) & (cl <= h)).fill_null(False)
    viol = df.filter(finite & ~sane)
    if viol.height:
        n_clean = viol.filter(clean).height
        if n_clean:
            out.append(c.finding("ohlc", FAIL, f"{n_clean} bar(s) violate high>=low / open,close within the "
                                 f"range yet carry is_clean=True, e.g. "
                                 f"{', '.join(_examples(viol.filter(clean)['timestamp']))}", n_rows=n_clean))
        else:
            out.append(c.finding("ohlc", INFO, f"{viol.height} bar(s) violate the price-sanity rules, all "
                                 f"flagged is_clean=False"))
    nonpos = df.filter(finite & pl.any_horizontal([pl.col(x) <= 0 for x in _PRICE_COLS]))
    if nonpos.height:
        # FIX SIL-AUDIT-OHLCV-02: KOSPI 1M 2026-11-01 holds low = 0.0 and is_clean = TRUE (Silver's price-sanity rule
        # only orders the four prices), so Gold reads it. A non-positive price that is_clean lets through is a FAIL.
        n_clean = nonpos.filter(clean).height
        span = _span(nonpos['_stamp']) if '_stamp' in nonpos.columns else '-'
        if n_clean:
            out.append(c.finding("ohlc", FAIL, f"{n_clean} bar(s) with a price <= 0 carry is_clean=True "
                                 f"({span}), e.g. {', '.join(_examples(nonpos.filter(clean)['timestamp']))}",
                                 n_rows=n_clean))
        out.append(c.finding("ohlc", WARN, f"{nonpos.height} bar(s) with a price <= 0 ({span}); "
                             f"genuine only for a sign-crossing instrument (CL, 2020-04-20)", n_rows=nonpos.height))
    if "volume" in df.columns and df.schema["volume"].is_numeric():
        neg = df.filter(pl.col("volume") < 0)
        if neg.height:
            out.append(c.finding("ohlc", FAIL, f"{neg.height} bar(s) with negative volume", n_rows=neg.height))
    flat = df.filter(finite & (o == h) & (h == lo) & (lo == cl))
    if flat.height:
        out.append(c.finding("ohlc", INFO, f"{flat.height} of {df.height} bar(s) are fully flat (O=H=L=C) "
                             f"({flat.height / df.height:.1%})", n_rows=flat.height))
    if "is_clean" in df.columns and df.schema["is_clean"] == pl.Boolean:
        dirty = df.filter(~pl.col("is_clean").fill_null(True))
        if dirty.height:
            out.append(c.finding("ohlc", INFO, f"{dirty.height} of {df.height} bar(s) flagged is_clean=False "
                                 f"({dirty.height / df.height:.2%})", n_rows=dirty.height))
    return out


def _derived_mismatch(df: pl.DataFrame, col: str, expected: pl.Expr, rel: bool) -> pl.DataFrame:
    """Rows where a stored derived field differs from its recomputation (both finite) or where exactly one of
    the two is finite. A row whose recomputation is null (first bar of log_return) is not judged."""
    exp = expected.alias("_exp")
    t = df.with_columns(exp).filter(pl.col("_exp").is_not_null())
    got, ex = pl.col(col), pl.col("_exp")
    both = (got.is_finite() & ex.is_finite()).fill_null(False)
    scale = pl.max_horizontal(ex.abs(), pl.lit(1.0)) if rel else pl.lit(1.0)
    differs = both & ((got - ex).abs() > _DERIVED_TOL * scale)
    one_sided = (got.is_finite().fill_null(False) != ex.is_finite().fill_null(False))
    return t.filter(differs | one_sided)


def check_derived(c: Ctx) -> list[Finding]:
    df = c.df
    need = ("close", "volume", "high", "low", "log_return", "dollar_volume", "spread_hl")
    if not all(col in df.columns for col in need) or any(not df.schema[col].is_numeric() for col in need):
        return []
    df = df.sort("timestamp")
    out: list[Finding] = []
    specs = (
        ("log_return", (pl.col("close") / pl.col("close").shift(1)).log(), False),
        ("dollar_volume", pl.col("close") * pl.col("volume"), True),
        ("spread_hl", (pl.col("high") - pl.col("low")) / pl.col("close"), True),
    )
    for col, expected, rel in specs:
        bad = _derived_mismatch(df, col, expected, rel)
        if bad.height:
            out.append(c.finding("derived", FAIL, f"{bad.height} row(s) where {col} differs from its "
                                 f"recomputation from OHLCV, e.g. {', '.join(_examples(bad['timestamp']))}",
                                 field=col, n_rows=bad.height))
    return out


def check_meta(c: Ctx) -> list[Finding]:
    df = c.df
    out: list[Finding] = []
    if "processing_version" in df.columns:
        vers = sorted(map(str, df["processing_version"].drop_nulls().unique().to_list()))
        if vers != [CURRENT_SILVER_VERSION]:
            out.append(c.finding("meta", WARN, f"processing_version {vers or 'null'} != current "
                                 f"{CURRENT_SILVER_VERSION!r}: rebuild pending", versions=vers))
    if "data_source" in df.columns:
        srcs = sorted(map(str, df["data_source"].drop_nulls().unique().to_list()))
        if len(srcs) > 1:
            out.append(c.finding("meta", INFO, f"{len(srcs)} data_source values: {', '.join(srcs)}",
                                 sources=srcs))
    for col in ("is_adjusted", "adj_factor"):
        if col in df.columns and df[col].null_count():
            out.append(c.finding("meta", WARN, f"{col} has {df[col].null_count()} null(s)"))
    if "adj_factor" in df.columns and df.schema["adj_factor"].is_numeric():
        n_adj = df.filter(pl.col("adj_factor") != 1.0).height
        if n_adj:
            out.append(c.finding("meta", INFO, f"{n_adj} row(s) with adj_factor != 1.0", n_rows=n_adj))
    return out


def check_freshness(c: Ctx) -> list[Finding]:
    """Collects the newest stamp of each 1D file; judged against the market in post_freshness."""
    if c.f.timeframe == "1D" and "_stamp" in c.df.columns and c.df.height:
        c.facts["last_stamp"].append((c.f.market, c.f.symbol, c.df["_stamp"].max()))
    return []



def _split_kind(ratio: float) -> str:
    for k in SPLIT_FACTORS:
        if abs(ratio * k - 1) <= SPLIT_NEAR or abs(ratio / k - 1) <= SPLIT_NEAR:
            return f"split-like, factor {k:g}"
    return "dividend-sized" if abs(math.log(ratio)) < BASIS_FAIL_LOG else "unexplained factor"


def basis_step(rec: pl.DataFrame, pre: str, tol: float) -> Optional[dict]:
    """The constant-ratio mismatch between a coarse bar and its aggregated daily bars for window variant ``pre``,
    or None. A WINDOW or STALENESS defect makes the ratio vary bar to bar; an ADJUSTMENT-BASIS difference (the two
    series were fetched on either side of a split or dividend, so one is rebased and the other is not) makes it
    one constant over the whole stretch. Measured 17 Nov 2026: ALL 1W 1.00416 on 519 of 533 weeks, then exactly
    1.0 from the week of 2026-08-17; MNST 1M exactly 0.5 on 118 of 123 months."""
    ok = rec.filter(~pl.col("open_ended") & pl.col(f"{pre}_verifiable") & (pl.col(f"{pre}_d_close") > 0)
                    & (pl.col("close") > 0))
    if not ok.height:
        return None
    r = ok.with_columns((pl.col("close") / pl.col(f"{pre}_d_close")).alias("_r"))
    mis = r.filter((pl.col("_r") - 1).abs() > tol)
    if mis.height < BASIS_MIN_PERIODS:
        return None
    med = float(mis["_r"].median())
    near = mis.filter((pl.col("_r") / med - 1).abs() <= BASIS_BAND)
    if near.height < BASIS_SHARE * mis.height:
        return None
    last = near["_stamp"].max()
    later = r.filter((pl.col("_stamp") > last) & ((pl.col("_r") - 1).abs() <= tol))
    return {"variant": pre, "ratio": med, "n": ok.height, "n_mismatch": mis.height, "n_constant": near.height,
            "first": near["_stamp"].min(), "last": last, "matches_again_from": later["_stamp"].min()}


def check_basis(c: Ctx) -> list[Finding]:
    """1W/1M vs 1D: one constant ratio over many bars is an adjustment-basis step, not a window defect. Which
    side is stale is NOT decided here: ALL / CSX 1W/1M history is the older vintage (1D carries the newer
    dividend rebase); MNST 1M is right and 1D is the stale side (not split-adjusted, see the ``seam`` check)."""
    if c.f.timeframe not in _PERIOD_DEDUP_TIMEFRAMES:
        return []
    rec = c.recon()
    if rec is None:
        return []
    # A basis step makes the series fail EVERY window variant. A bar that reconciles on its own stamp window (ABBV:
    # Tue..Mon blocks) or end-inclusive window has a window/anchor defect instead — and on a trending series that
    # defect can look like a constant ratio against the calendar window, so it is excluded first.
    best_own = max((r["close"] for r in (match_rates(rec, "i"), match_rates(rec, "e")) if r["n"]), default=0.0)
    if best_own >= c.opts.min_match:
        return []
    cands = [b for b in (basis_step(rec, p, c.opts.tol) for p in ("i", "c")) if b]
    if not cands:
        return []
    b = max(cands, key=lambda x: x["n_constant"])
    big = abs(math.log(b["ratio"])) >= BASIS_FAIL_LOG
    again = (f"matches 1D again from {b['matches_again_from']}" if b["matches_again_from"] is not None
             else "never matches 1D again")
    side = "higher" if b["ratio"] > 1 else "lower"
    return [c.finding("basis", FAIL if big else WARN,
                      f"constant-ratio mismatch with 1D: the {c.f.timeframe} close is {b['ratio']:.5g}x the daily "
                      f"close on {b['n_constant']} of {b['n']} bars ({b['first']}..{b['last']}), {again} — an "
                      f"adjustment-basis step ({_split_kind(b['ratio'])}), not a window or staleness defect; "
                      f"this timeframe sits {side} than 1D; which side is stale is not decided here",
                      **b)]


def find_seams(df: pl.DataFrame) -> list[tuple]:
    """(stamp, k, direction, position) for every close-to-close ratio within SEAM_TOL of a split factor k or 1/k,
    from the processor's own find_split_seams (the same rule it quarantines on)."""
    d = df.sort("timestamp").select("_stamp", "close").filter(pl.col("close").is_finite() & (pl.col("close") > 0))
    stamps = d["_stamp"].to_list()
    return [(stamps[i], k, direction, i) for i, k, direction in find_split_seams(d["close"].to_list())]


def check_seam(c: Ctx) -> list[Finding]:
    """A split factor in a close-to-close ratio of an ADJUSTED series. Measured on MNST 1D (17 Nov 2026): close
    97.5 on 2026-07-17, 47.7 on 07-20, then alternating ~47 / ~93 until 08-11 (seven k=2 jumps) — the Bronze file
    fetched on 2026-09-01 already holds it, Silver is an exact copy, so the vendor returned a partly unadjusted
    history. Jumps that reverse within SEAM_REVERT_BARS bars cannot be a market move [FAIL]; a single persistent
    jump is an unadjusted split or a genuine move [WARN: check the other timeframes]."""
    if "_stamp" not in c.df.columns or not {"close", "timestamp"} <= set(c.df.columns):
        return []
    seams = find_seams(c.df)
    if not seams:
        return []
    flip = reversing_seams([(sm[3], sm[1], sm[2]) for sm in seams])
    ex = "; ".join(f"{s[0]} x{s[1]:g} {s[2]}" for s in seams[:4])
    detail = dict(n_jumps=len(seams), n_reversing=len(flip), jumps=[(str(s[0]), s[1], s[2]) for s in seams[:12]])
    if flip and c.f.timeframe == "1D":
        return [c.finding("seam", FAIL, f"{len(seams)} split-factor jump(s), {len(flip)} of them reversing within "
                          f"{SEAM_REVERT_BARS} bars (a vendor adjustment glitch, not a market move): {ex}", **detail)]
    if flip:
        # FIX SIL-SEAM-SCOPE-01: 25 bars of 1W / 1M are six months / two years; CVNA 1M has six such jumps and no 1D
        # seam at all. Judged against 1D by `basis` / `window`; the processor quarantines a coarse series only when
        # it reproduces a quarantined 1D.
        return [c.finding("seam", WARN, f"{len(seams)} split-factor jump(s), {len(flip)} of them reversing within "
                          f"{SEAM_REVERT_BARS} {c.f.timeframe} bars: {ex} - on a coarse series a reversal is an "
                          f"ordinary move of a volatile name, not a glitch signature; compare with 1D "
                          f"(`basis` / `window`)", **detail)]
    return [c.finding("seam", WARN, f"{len(seams)} persistent split-factor jump(s): {ex} — an unadjusted split or a "
                      f"genuine move; compare with the other timeframes", **detail)]


def check_leak(c: Ctx) -> list[Finding]:
    """The availability rule of features_technical (ADR-053 D2): a decision on T may use a bar iff it was complete
    before T — 1W: bar date + (7 - ISO weekday) + 1 days; 1M: month end + 1 day. A bar whose values aggregate daily
    bars dated on or after that date carries LOOKAHEAD. Measured on ABBV 1W: 436 of 436 Tuesday-stamped bars that
    reconcile on their own Tue..Mon window end on the Monday the rule calls them available. Judged only on bars
    that reconcile on their own window (a bar that does not reconcile is a ``window`` finding); a FAIL for the
    markets technical_xgboost reads, INFO elsewhere (no 1W/1M consumer)."""
    tf = c.f.timeframe
    if tf not in _PERIOD_DEDUP_TIMEFRAMES:
        return []
    rec = c.recon()
    if rec is None:
        return []
    avail = (pl.col("_stamp") + pl.duration(days=(7 - pl.col("_stamp").dt.weekday()).cast(pl.Int64) + 1)
             if tf == "1W" else pl.col("_stamp").dt.month_end() + pl.duration(days=1))
    ok = (rec.filter(~pl.col("open_ended") & pl.col("i_verifiable") & pl.col("i_close"))
          .with_columns(avail.alias("_avail")))
    if not ok.height:
        return []
    bad = ok.filter(pl.col("i_d_last") >= pl.col("_avail"))
    if not bad.height:
        return []
    ahead = int((bad["i_d_last"] - bad["_avail"]).dt.total_days().max()) + 1
    first = bad.sort("_stamp").row(0, named=True)
    sev = FAIL if c.f.market in LAYER1_MARKETS else INFO
    return [c.finding("leak", sev,
                      f"{bad.height} of {ok.height} reconciling bars contain daily data on or after the date "
                      f"features_technical treats them as available: lookahead of up to {ahead} day(s); e.g. stamp "
                      f"{first['_stamp']} ends {first['i_d_last']}, available {first['_avail']}"
                      + ("" if sev == FAIL else " (no technical consumer in this market)"),
                      n_leak=bad.height, n_judged=ok.height, lookahead_days=ahead)]


def check_last_bar(c: Ctx) -> list[Finding]:
    """The newest (still forming) 1W/1M bar against the last daily close of its own window. Not judged by
    ``window`` (the open-ended bar is left out of its rates), so a frozen first snapshot only surfaced as an anchor
    FAIL: AG 1M Nov 2026 close 50.625 vs 49.121 from the daily bars."""
    if c.f.timeframe not in _PERIOD_DEDUP_TIMEFRAMES:
        return []
    rec = c.recon()
    if rec is None or not rec.height:
        return []
    last = rec.filter(pl.col("open_ended") & pl.col("i_verifiable"))
    if not last.height:
        return []
    r = last.row(0, named=True)
    if r["i_close"] or not r["i_d_close"]:
        return []
    gap = r["close"] / r["i_d_close"] - 1
    return [c.finding("last_bar", WARN, f"the newest {c.f.timeframe} bar ({r['_stamp']}) closes at {r['close']:.6g} but "
                      f"the last daily close of its window is {r['i_d_close']:.6g} ({r['i_d_last']}): {gap:+.2%}, a "
                      f"stale in-progress bar", bar_close=r["close"], daily_close=r["i_d_close"], gap=gap)]


def check_recent(c: Ctx) -> list[Finding]:
    """FIX SIL-AUDIT-OHLCV-03: the newest RECENT_PERIODS closed 1W/1M bars against the daily bars. A bar counts as
    reconciled when its close equals the daily close of ANY of the three windows (implied, end-inclusive, calendar),
    so a convention difference (`anchor` / `window` / `leak`) is not mistaken for a stale value; what fails all
    three is a value defect. Not judged for FX: the processor excludes it from the rebuild until the window rule is
    settled (GHR-11), and the gate lists it as deferred."""
    tf = c.f.timeframe
    if tf not in RECENT_PERIODS or coarse_rebuild_exclusion(c.f.market) is not None:
        return []
    rec = c.recon()
    if rec is None or not rec.height:
        return []
    ok = pl.any_horizontal([pl.col(f"{p}_close").fill_null(False) for p in ("i", "e", "c")])
    verifiable = pl.any_horizontal([pl.col(f"{p}_verifiable").fill_null(False) for p in ("i", "e", "c")])
    judged = (rec.filter(~pl.col("open_ended") & verifiable).sort("_stamp").tail(RECENT_PERIODS[tf])
              .with_columns(ok.alias("_ok"),
                            pl.coalesce("i_d_close", "c_d_close", "e_d_close").alias("_ref")))
    bad = judged.filter(~pl.col("_ok"))
    if not bad.height:
        return []
    gap = float(bad.select(((pl.col("close") / pl.col("_ref") - 1).abs()).max()).item())
    sev = FAIL if (c.f.market in LAYER1_MARKETS and gap > RECENT_FAIL_GAP) else WARN
    stamps = [str(x) for x in bad.sort("_stamp")["_stamp"].to_list()]
    return [c.finding("recent", sev, f"{bad.height} of the newest {judged.height} closed {tf} bars match no daily "
                      f"close on any window (largest gap {gap:.2%}): {', '.join(stamps[:6])} - a frozen or stale value",
                      n_bad=bad.height, n_judged=judged.height, max_gap=gap, stamps=stamps[:12])]


# (name, function, timeframes it applies to or None, one-line description)
CHECKS: dict[str, tuple[Callable[[Ctx], list[Finding]], Optional[frozenset], str]] = {
    "schema": (check_schema, None, "GD §4.3 columns, dtypes, tz-aware stamp, path/column identity"),
    "timestamp": (check_timestamp, None, "null / unsorted / duplicate stamps, coarse stamp hour, weekend 1D"),
    "period": (check_period, frozenset(_PERIOD_DEDUP_TIMEFRAMES), "1W/1M: more than one bar per period"),
    "anchor": (check_anchor, frozenset(_PERIOD_DEDUP_TIMEFRAMES), "1M stamp day, 1W stamp weekday vs market"),
    "window": (check_window, frozenset(_PERIOD_DEDUP_TIMEFRAMES), "1W/1M vs 1D: implied + calendar windows"),
    "basis": (check_basis, frozenset(_PERIOD_DEDUP_TIMEFRAMES),
              "1W/1M vs 1D: one constant ratio over many bars (adjustment-basis step)"),
    "seam": (check_seam, None, "split-factor jumps in a close series; reversing ones are a vendor glitch on 1D only"),
    "leak": (check_leak, frozenset(_PERIOD_DEDUP_TIMEFRAMES),
             "1W/1M bars that contain data on/after their features_technical availability date"),
    "gaps": (check_gaps, frozenset(_PERIOD_DEDUP_TIMEFRAMES), "1W/1M periods with no bar between first and last"),
    "last_bar": (check_last_bar, frozenset(_PERIOD_DEDUP_TIMEFRAMES),
                 "the newest 1W/1M bar vs the last daily close of its window"),
    "recent": (check_recent, frozenset(_PERIOD_DEDUP_TIMEFRAMES),
               "the newest 1W/1M closed bars each reconcile to a daily close (frozen / stale values)"),
    "current_period": (check_current_period, frozenset(_PERIOD_DEDUP_TIMEFRAMES),
                       "the period of the newest 1D bar has a period-start bar"),
    "ohlc": (check_ohlc, None, "price sanity, non-finite, volume, is_clean consistency"),
    "derived": (check_derived, None, "log_return / dollar_volume / spread_hl recomputed"),
    "meta": (check_meta, None, "processing_version, data_source, adjustment columns"),
    "freshness": (check_freshness, frozenset({"1D"}), "1D newest stamp vs the market"),
    "coverage": (lambda c: [], None, "registry vs files, orphans, missing timeframes (whole-tree)"),
}
CHECK_ORDER = tuple(CHECKS)


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Whole-tree (post) checks, run on the facts the per-file pass collected
# ─────────────────────────────────────────────────────────────────────────────────────────────────

def post_anchor(facts: dict[str, list]) -> tuple[list[Finding], dict]:
    """Judge each 1W symbol's dominant stamp weekday against its market's. A market has an anchor only when
    at least _ANCHOR_MIN_AGREE symbols agree and they are at least half the market."""
    findings: list[Finding] = []
    census: dict[str, dict] = {}
    by_market: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)
    for market, tf, symbol, hist in facts.get("weekday", []):
        by_market[(market, tf)][symbol] = hist
    for (market, tf), per_symbol in sorted(by_market.items()):
        dominant = {s: max(h, key=lambda wd: (h[wd][0], -wd)) for s, h in per_symbol.items()}
        mode_wd, mode_n = Counter(dominant.values()).most_common(1)[0]
        judged = mode_n >= _ANCHOR_MIN_AGREE and mode_n / len(per_symbol) >= 0.5
        census[f"{market}/{tf}"] = {
            "symbols": len(per_symbol), "mode": WEEKDAY[mode_wd] if judged else None,
            "dominant": {WEEKDAY[wd]: n for wd, n in sorted(Counter(dominant.values()).items())},
            "judged": judged}
        if not judged:
            findings.append(Finding("anchor", INFO, market, "*", tf,
                                    f"no dominant {tf} stamp weekday among {len(per_symbol)} symbol(s) "
                                    f"({', '.join(f'{WEEKDAY[w]} {n}' for w, n in sorted(Counter(dominant.values()).items()))});"
                                    f" anchor not judged"))
            continue
        for symbol, hist in sorted(per_symbol.items()):
            dom = dominant[symbol]
            stray = {wd: v for wd, v in hist.items() if wd != dom}
            if dom != mode_wd:
                total = sum(v[0] for v in hist.values())
                findings.append(Finding(
                    "anchor", FAIL, market, symbol, tf,
                    f"{hist[dom][0]} of {total} {tf} stamp(s) are {WEEKDAY[dom]} "
                    f"({hist[dom][1]}..{hist[dom][2]}) but {market} is {WEEKDAY[mode_wd]} "
                    f"({mode_n} of {len(per_symbol)} symbols)"
                    + (f"; {sum(v[0] for v in stray.values())} on "
                       f"{'/'.join(WEEKDAY[w] for w in sorted(stray))} ("
                       f"{min(v[1] for v in stray.values())}..{max(v[2] for v in stray.values())})"
                       if stray else ""),
                    {"dominant": WEEKDAY[dom], "market_mode": WEEKDAY[mode_wd],
                     "histogram": {WEEKDAY[w]: v[0] for w, v in sorted(hist.items())}}))
            elif stray:
                findings.append(Finding(
                    "anchor", WARN, market, symbol, tf,
                    f"{sum(v[0] for v in stray.values())} of {sum(v[0] for v in hist.values())} {tf} stamp(s) "
                    f"fall on {'/'.join(WEEKDAY[w] for w in sorted(stray))} instead of {WEEKDAY[dom]} "
                    f"({min(v[1] for v in stray.values())}..{max(v[2] for v in stray.values())})",
                    {"histogram": {WEEKDAY[w]: v[0] for w, v in sorted(hist.items())}}))
    return findings, census


def post_freshness(facts: dict[str, list], stale_days: int) -> list[Finding]:
    by_market: dict[str, dict[str, date]] = defaultdict(dict)
    for market, symbol, last in facts.get("last_stamp", []):
        by_market[market][symbol] = last
    out: list[Finding] = []
    for market, per_symbol in sorted(by_market.items()):
        newest = max(per_symbol.values())
        for symbol, last in sorted(per_symbol.items()):
            lag = (newest - last).days
            if lag > stale_days:
                out.append(Finding("freshness", WARN, market, symbol, "1D",
                                   f"newest 1D stamp {last} trails {market}'s newest ({newest}) by {lag} day(s) "
                                   f"[--stale-days {stale_days}]", {"last": str(last), "market_newest": str(newest),
                                                                    "lag_days": lag}))
    return out


def _registry_symbols() -> set[str]:
    from src.config.instrument_loader import get_loader
    loader = get_loader()
    return set(loader.symbol_list()) | {i.symbol for i in loader.all_context(include_deferred=False)}


def post_coverage(facts: dict[str, list], opts: Options,
                  registry: Optional[set[str]] = None) -> list[Finding]:
    present: set[tuple[str, str, str]] = set(facts.get("present", []))
    if opts.filtered:
        return [Finding("coverage", INFO, "*", "*", "*", "skipped: --market / --symbol narrow the run, so "
                        "absence from the file list proves nothing")]
    out: list[Finding] = []
    symbols_present = {s for _, s, _ in present}
    if opts.use_registry:
        try:
            registry = registry if registry is not None else _registry_symbols()
        except Exception as e:  # not run from the repo root / loader unavailable: say so, do not skip silently
            out.append(Finding("coverage", INFO, "*", "*", "*", f"registry unavailable ({type(e).__name__}: "
                               f"{str(e).splitlines()[0] if str(e) else ''}); registry coverage skipped"))
            registry = None
    else:
        registry = None
    if registry is not None:
        for sym in sorted(registry - symbols_present):
            out.append(Finding("coverage", FAIL, "*", sym, "*", "registered symbol has NO Silver file in the "
                               "selected timeframes"))
        for sym in sorted(symbols_present - registry):
            mk = sorted({m for m, s, _ in present if s == sym})
            out.append(Finding("coverage", WARN, "/".join(mk), sym, "*", "Silver symbol is not in the "
                               "instrument registry (orphan of a removed symbol?)"))
    by_market: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for market, sym, tf in present:
        by_market[market][sym].add(tf)
    for market, per_symbol in sorted(by_market.items()):
        if len(per_symbol) < 3:
            continue
        tf_count = Counter(tf for tfs in per_symbol.values() for tf in tfs)
        expected = {tf for tf, n in tf_count.items() if n / len(per_symbol) >= 0.5}
        for sym, tfs in sorted(per_symbol.items()):
            lacking = sorted(expected - tfs)
            if lacking:
                out.append(Finding("coverage", WARN, market, sym, "*", f"missing timeframe(s) "
                                   f"{', '.join(lacking)} that most {market} symbols have"))
    return out


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────────────────────────────────

def _new_facts() -> dict[str, list]:
    return defaultdict(list)


def _read_daily(root: Path, market: str, symbol: str) -> Optional[pl.DataFrame]:
    path = root / market / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"
    if not path.exists():
        return None
    try:
        d = pl.read_parquet(path, columns=["timestamp", "open", "high", "low", "close", "volume"])
    except Exception:
        return None
    d = prepare(d)
    if "_stamp" not in d.columns or not d.height:
        return None
    return d.unique(subset=["_stamp"], keep="last", maintain_order=True)


def audit_symbol(market: str, symbol: str, files: list[SilverFile], opts: Options
                 ) -> tuple[list[Finding], dict[str, list], int]:
    """Run every selected check on every selected file of one symbol. A check that raises becomes a FAIL
    finding (check 'internal'), never a silent skip."""
    findings: list[Finding] = []
    facts = _new_facts()
    n_read = 0
    daily_cache: Any = _UNSET

    def daily_loader() -> Optional[pl.DataFrame]:
        nonlocal daily_cache
        if daily_cache is _UNSET:
            daily_cache = _read_daily(opts.silver_root, market, symbol)
        return daily_cache

    for f in files:
        facts["present"].append((f.market, f.symbol, f.timeframe))
        try:
            df = pl.read_parquet(f.path)
        except Exception as e:
            findings.append(Finding("schema", FAIL, f.market, f.symbol, f.timeframe,
                                    f"unreadable file: {str(e).splitlines()[0] if str(e) else type(e).__name__}",
                                    {"blocking": True}))
            continue
        n_read += 1
        if "is_clean" in df.columns:
            facts["clean"].append((f.market, f.symbol, f.timeframe, int(df["is_clean"].fill_null(False).sum())))
        ctx = Ctx(f, prepare(df), opts, daily_loader, facts)
        blocked = False
        for name in CHECK_ORDER:
            if name not in opts.checks or name == "coverage":
                continue
            func, tfs, _ = CHECKS[name]
            if tfs is not None and f.timeframe not in tfs:
                continue
            if blocked and name != "schema":
                continue
            try:
                res = func(ctx)
            except Exception as e:
                res = [ctx.finding("internal", FAIL, f"check {name!r} crashed: {type(e).__name__}: {e}")]
            findings.extend(res)
            if name == "schema" and any(x.detail.get("blocking") for x in res):
                blocked = True
                findings.append(ctx.finding("schema", INFO, "blocking schema failure: remaining checks skipped "
                                            "for this file"))
    return findings, facts, n_read


def run_audit(opts: Options, files: list[SilverFile]) -> AuditResult:
    if not opts.checks:
        raise ValueError("Options.checks is empty: a gate computed from no check would PASS without judging anything")
    groups: dict[tuple[str, str], list[SilverFile]] = defaultdict(list)
    for f in files:
        groups[(f.market, f.symbol)].append(f)
    keys = sorted(groups)
    findings: list[Finding] = []
    facts = _new_facts()
    n_read = 0
    with ThreadPoolExecutor(max_workers=4) as pool:
        for fnd, fct, nr in pool.map(lambda k: audit_symbol(k[0], k[1], groups[k], opts), keys):
            findings.extend(fnd)
            for k, v in fct.items():
                facts[k].extend(v)
            n_read += nr
    census: dict = {}
    if "anchor" in opts.checks:
        fnd, census = post_anchor(facts)
        findings.extend(fnd)
    if "freshness" in opts.checks:
        findings.extend(post_freshness(facts, opts.stale_days))
    if "coverage" in opts.checks:
        findings.extend(post_coverage(facts, opts))
    facts["census"] = [census]
    findings.sort(key=lambda x: (CHECK_ORDER.index(x.check) if x.check in CHECK_ORDER else 99,
                                 _SEVERITY_ORDER[x.severity], x.market, x.symbol, x.timeframe))
    tfs = tuple(sorted({f.timeframe for f in files}))
    facts["gate"] = [build_gate(findings, tfs, facts.get("clean"))]
    return AuditResult(findings, facts, len(files), n_read, len({(f.market, f.symbol) for f in files}),
                       tfs, opts.checks)


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────────────────────────────────────────

def _median(xs: list[float]) -> Optional[float]:
    xs = sorted(xs)
    return xs[len(xs) // 2] if xs else None


def recon_summary(facts: dict[str, list]) -> dict[str, dict]:
    """Per (market, tf): symbols judged, median / worst close-match rate of the best implied window and of the
    calendar window, and how many symbols reconcile only end-inclusive."""
    agg: dict[str, dict[str, list]] = defaultdict(lambda: {"i": [], "c": [], "v": []})
    for market, tf, _sym, best, rc, variant in facts.get("recon", []):
        if best["n"]:
            agg[f"{market}/{tf}"]["i"].append(best["close"])
            agg[f"{market}/{tf}"]["v"].append(variant)
        if rc["n"]:
            agg[f"{market}/{tf}"]["c"].append(rc["close"])
    out = {}
    for key, v in sorted(agg.items()):
        out[key] = {"symbols": max(len(v["i"]), len(v["c"])),
                    "implied_close_median": _median(v["i"]), "implied_close_min": min(v["i"]) if v["i"] else None,
                    "calendar_close_median": _median(v["c"]), "calendar_close_min": min(v["c"]) if v["c"] else None,
                    "end_inclusive_symbols": sum(1 for x in v["v"] if x == "end-inclusive")}
    return out


def _jsonable(o: Any) -> Any:
    if isinstance(o, dict):
        return {str(k): _jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(v) for v in o]
    if isinstance(o, (date,)):
        return str(o)
    return o


def build_gate(findings: list[Finding], timeframes: tuple[str, ...],
               clean: Optional[list[tuple]] = None) -> dict[str, dict]:
    """Silver -> Gold readiness per Gold consumer (GATE_CONSUMERS). BLOCK = at least one FAIL on a series Gold would
    read; ATTENTION = no FAIL but a WARN of GATE_ATTENTION; PASS otherwise. A series with no is_clean = TRUE row is
    QUARANTINED (the processor's reversing-seam rule, SIL-SEAM-QUARANTINE-01): Gold reads nothing from it, so its
    findings are listed under ``quarantined_series`` instead of blocking, and so is a ``basis`` finding on a coarse
    series whose own 1D is quarantined (the stale side is already out of Gold). ``clean`` is (market, symbol,
    timeframe, n_clean) per file. ``partial`` lists consumer timeframes this run did not audit: the verdict only
    covers what was audited. Whole-tree findings (symbol "*") carry no series and are not counted."""
    gate: dict[str, dict] = {}
    audited = set(timeframes)
    quarantined = {(m, sym, tf) for m, sym, tf, n in (clean or []) if n == 0}

    def neutral(f: Finding) -> bool:
        return ((f.market, f.symbol, f.timeframe) in quarantined
                or (f.check == "basis" and (f.market, f.symbol, "1D") in quarantined))

    for name, (markets, tfs) in GATE_CONSUMERS.items():
        scope = [f for f in findings if f.symbol != "*" and f.timeframe in tfs
                 and (markets is None or f.market in markets)]
        live = [f for f in scope if not neutral(f)]
        fails = [f for f in live if f.severity == FAIL]
        attn = [f for f in live if f.severity == WARN and f.check in GATE_ATTENTION]
        verdict = "BLOCK" if fails else ("ATTENTION" if attn else "PASS")
        series = sorted({(f.market, f.symbol, f.timeframe, f.check) for f in fails})
        quar = sorted({(m, sym, tf) for m, sym, tf in quarantined
                       if tf in tfs and (markets is None or m in markets)})
        # FIX SIL-AUDIT-OHLCV-03: FX 1W/1M are excluded from the rebuild until the window rule is settled (GHR-11);
        # a consumer that reads them cannot be PASS, and the cost of both lists is stated as a number
        deferred = sorted({(m, sym, tf) for m, sym, tf, _n in (clean or [])
                           if tf in tfs and tf in RECENT_PERIODS and (markets is None or m in markets)
                           and coarse_rebuild_exclusion(m) == "fx"})
        if deferred and verdict == "PASS":
            verdict = "ATTENTION"
        gate[name] = {"verdict": verdict, "fail": len(fails), "attention": len(attn),
                      "fail_by_check": dict(Counter(f.check for f in fails)),
                      "attention_by_check": dict(Counter(f.check for f in attn)),
                      "blocking_series": [list(x) for x in series],
                      "blocked_symbols": len({(x[0], x[1]) for x in series}),
                      "quarantined_series": [list(x) for x in quar],
                      "quarantined_count": len(quar),
                      "quarantined_symbols": len({(x[0], x[1]) for x in quar}),
                      "deferred_series": [list(x) for x in deferred],
                      "deferred_reason": "fx 1W/1M window rule unsettled (GHR-11); do not read" if deferred else "",
                      "partial": sorted(tfs - audited)}
    return gate


def exit_code(result: AuditResult, strict: bool) -> int:
    bad = {FAIL, WARN} if strict else {FAIL}
    return 1 if any(f.severity in bad for f in result.findings) else 0


def render_json(result: AuditResult, opts: Options) -> str:
    payload = {
        "silver_root": str(opts.silver_root), "files": result.n_files, "files_read": result.n_read,
        "symbols": result.symbols, "timeframes": list(result.timeframes), "checks": list(result.checks),
        "thresholds": {"tol": opts.tol, "vol_tol": opts.vol_tol, "min_match": opts.min_match,
                       "stale_days": opts.stale_days},
        "summary": {chk: dict(Counter(f.severity for f in result.findings if f.check == chk))
                    for chk in CHECK_ORDER + ("internal",) if any(f.check == chk for f in result.findings)},
        "anchor_census": result.facts.get("census", [{}])[0],
        "reconciliation": recon_summary(result.facts),
        "gate": result.facts.get("gate", [{}])[0],
        "findings": [{"check": f.check, "severity": f.severity, "market": f.market, "symbol": f.symbol,
                      "timeframe": f.timeframe, "message": f.message, "detail": _jsonable(f.detail)}
                     for f in result.findings],
    }
    return json.dumps(payload, indent=2, default=str)


def render_text(result: AuditResult, opts: Options, limit: int) -> str:
    L: list[str] = []
    L.append(f"Silver OHLCV audit | root={opts.silver_root} | files={result.n_files} (read {result.n_read}) | "
             f"symbols={result.symbols} | timeframes={', '.join(result.timeframes) or '-'}")
    L.append(f"checks: {', '.join(result.checks)}")
    L.append(f"thresholds [U]: tol={opts.tol} vol-tol={opts.vol_tol} min-match={opts.min_match} "
             f"stale-days={opts.stale_days}")
    L.append("")
    L.append(f"{'CHECK':<15}{'FAIL':>6}{'WARN':>6}{'INFO':>6}   symbols affected (FAIL+WARN)")
    present_checks = [c for c in CHECK_ORDER + ("internal",) if c in result.checks or c == "internal"]
    for chk in present_checks:
        fs = [f for f in result.findings if f.check == chk]
        if chk == "internal" and not fs:
            continue
        cnt = Counter(f.severity for f in fs)
        aff = {(f.market, f.symbol) for f in fs if f.severity in (FAIL, WARN) and f.symbol != "*"}
        L.append(f"{chk:<15}{cnt[FAIL]:>6}{cnt[WARN]:>6}{cnt[INFO]:>6}   {len(aff)}")
    census = result.facts.get("census", [{}])[0]
    if census:
        L.append("")
        L.append("ANCHOR CENSUS (1W dominant stamp weekday per symbol; judged when >= "
                 f"{_ANCHOR_MIN_AGREE} symbols agree and are >= half the market)")
        for key, v in census.items():
            dom = ", ".join(f"{d} {n}" for d, n in v["dominant"].items())
            L.append(f"  {key:<22} symbols={v['symbols']:<4} mode={v['mode'] or 'not judged':<11} {dom}")
    rs = recon_summary(result.facts)
    if rs:
        L.append("")
        L.append("RECONCILIATION vs 1D (share of bars whose CLOSE reconciles; median / worst symbol; "
                 "'end-incl' = symbols that reconcile only with the window end included)")
        L.append(f"  {'market/tf':<22}{'symbols':>8}{'implied med':>13}{'implied min':>13}{'calendar med':>14}"
                 f"{'calendar min':>14}{'end-incl':>10}")
        fmt = lambda x: "-" if x is None else f"{x:.3f}"
        for key, v in rs.items():
            L.append(f"  {key:<22}{v['symbols']:>8}{fmt(v['implied_close_median']):>13}"
                     f"{fmt(v['implied_close_min']):>13}{fmt(v['calendar_close_median']):>14}"
                     f"{fmt(v['calendar_close_min']):>14}{v['end_inclusive_symbols']:>10}")
    gate = result.facts.get("gate", [{}])[0]
    if gate:
        L.append("")
        L.append("GATE (Silver -> Gold readiness: a FAIL on a series the consumer reads blocks it; a listed WARN "
                 "needs attention; covers the audited timeframes only)")
        for name, g in gate.items():
            by = ", ".join(f"{k} {v}" for k, v in sorted(g["fail_by_check"].items(), key=lambda kv: -kv[1])) or "-"
            part = f" | timeframes not audited: {', '.join(g['partial'])}" if g["partial"] else ""
            quar = (f" quarantined series={g['quarantined_count']} (symbols={g['quarantined_symbols']}, data Gold "
                    f"does not read)" if g["quarantined_series"] else "")
            quar += (f" deferred FX series={len(g['deferred_series'])}" if g["deferred_series"] else "")
            L.append(f"  {name:<22}{g['verdict']:<10} fail={g['fail']} ({by}) attention={g['attention']} "
                     f"blocked symbols={g['blocked_symbols']}{quar}{part}")
            for mk, sym, tf, chk in g["blocking_series"][:limit]:
                L.append(f"      {mk}/{sym} {tf} [{chk}]")
            if len(g["blocking_series"]) > limit:
                L.append(f"      ... +{len(g['blocking_series']) - limit} more (use --limit or --format json)")
            for mk, sym, tf in g["quarantined_series"][:limit]:
                L.append(f"      {mk}/{sym} {tf} [quarantined: no is_clean row, Gold reads nothing from it]")
    for chk in present_checks:
        fs = [f for f in result.findings if f.check == chk and f.severity in (FAIL, WARN)]
        infos = [f for f in result.findings if f.check == chk and f.severity == INFO]
        if not fs and not infos:
            continue
        L.append("")
        L.append(f"[{chk}]")
        for f in fs[:limit]:
            L.append(f"  {f.severity} {f.market}/{f.symbol} {f.timeframe}: {f.message}")
        if len(fs) > limit:
            L.append(f"  ... +{len(fs) - limit} more (use --limit)")
        for f in infos[:limit]:
            L.append(f"  INFO {f.market}/{f.symbol} {f.timeframe}: {f.message}")
        if len(infos) > limit:
            L.append(f"  ... +{len(infos) - limit} more INFO (use --limit or --format json)")
    n_fail = sum(f.severity == FAIL for f in result.findings)
    n_warn = sum(f.severity == WARN for f in result.findings)
    L.append("")
    L.append(f"RESULT: {n_fail} FAIL, {n_warn} WARN" + (" — exit 1" if n_fail else ""))
    return "\n".join(L)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--silver-root", type=Path, default=SILVER_ROOT)
    ap.add_argument("--market", default=None, help="only this market bucket (e.g. us_stocks, forex, context)")
    ap.add_argument("--symbol", action="append", default=None, help="only this symbol (repeatable)")
    ap.add_argument("--tf", action="append", default=None,
                    help="timeframe to audit (repeatable) or 'all'; default 1D, 1W, 1M")
    ap.add_argument("--checks", default=None, help="comma list of checks (see --list-checks); default all")
    ap.add_argument("--list-checks", action="store_true")
    ap.add_argument("--limit", type=int, default=15, help="findings printed per check")
    ap.add_argument("--format", choices=("text", "json"), default="text")
    ap.add_argument("--strict", action="store_true", help="exit 1 on WARN as well as FAIL")
    ap.add_argument("--tol", type=float, default=DEFAULT_TOL, help="[U] OHLC relative tolerance")
    ap.add_argument("--vol-tol", type=float, default=DEFAULT_VOL_TOL, help="[U] volume relative tolerance")
    ap.add_argument("--min-match", type=float, default=DEFAULT_MIN_MATCH,
                    help="[U] share of bars that must reconcile before a WARN")
    ap.add_argument("--stale-days", type=int, default=DEFAULT_STALE_DAYS, help="[U] 1D staleness window")
    ap.add_argument("--no-registry", action="store_true", help="skip the instrument-registry coverage check")
    args = ap.parse_args(argv)

    if args.list_checks:
        for name, (_, tfs, doc) in CHECKS.items():
            print(f"{name:<15}{('/'.join(sorted(tfs)) if tfs else 'all TFs'):<10} {doc}")
        return 0

    if args.checks:
        chosen = tuple(x.strip() for x in args.checks.split(",") if x.strip())
        unknown = [x for x in chosen if x not in CHECKS]
        if unknown:
            ap.error(f"unknown check(s): {', '.join(unknown)} (see --list-checks)")
    else:
        chosen = CHECK_ORDER
    tfs: Optional[tuple[str, ...]]
    if args.tf is None:
        tfs = COARSE_TFS
    elif "all" in args.tf:
        tfs = None
    else:
        tfs = tuple(args.tf)
    symbols = set(args.symbol) if args.symbol else None
    opts = Options(silver_root=args.silver_root, tol=args.tol, vol_tol=args.vol_tol, min_match=args.min_match,
                   stale_days=args.stale_days, checks=chosen, use_registry=not args.no_registry,
                   filtered=bool(args.market or symbols))
    files = discover(args.silver_root, tfs, args.market, symbols)
    result = run_audit(opts, files)
    print(render_json(result, opts) if args.format == "json" else render_text(result, opts, args.limit))
    return exit_code(result, args.strict)


if __name__ == "__main__":
    sys.exit(main())
