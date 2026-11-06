"""
src/silver/membership_log.py — GHR-14 (ADR-059, GMI-GHR-REC-001): append-only active-set membership log.

Problem (ADR-059 GD3/GD4): the active set is a top-N by `dollar_volume_20d`, so a symbol near the cutoff
flips in and out. The 16->17 Oct run swapped four symbols (F, WMB, CVNA, CB out; PTC, OXY, MO, SHW in), and
dated membership files existed only for 16-18 Oct — "who was a member on date D" could not be answered for
anything earlier, which is a point-in-time / survivorship hazard for every feature and label builder
(RISK-28 class).

This module records the CHANGES, so membership at any logged date is reconstructible from the log alone:

    data/silver/active_symbols/membership_log.parquet
        date, market, symbol, event ('enter' | 'leave'), rank, dollar_volume_20d, compared_with, logged_at

* `date` is the resolution (run) date the change took effect on; `compared_with` is the earlier resolution
  it was diffed against, or NULL for an INITIAL entry (the first resolution ever logged: every member is an
  `enter` with no predecessor — otherwise the log could not be replayed from nothing).
* `rank` / `dollar_volume_20d` are ranked WITHIN the market, from the file where the symbol is a member:
  the current file for `enter`, the previous file for `leave` (a leaver is not in the current file).
* Fallback resolutions (`is_fallback = True`, the full-universe placeholder written when Silver was not
  ready) are NOT membership and are neither recorded nor used as a comparison baseline.
* Append-only, idempotent: an existing row is never modified or removed; an event whose
  (date, market, symbol, event) key is already logged is skipped. The Parquet file is replaced atomically
  (tempfile + os.replace) because Parquet cannot be appended in place.
* The log is the source of truth once it exists. A new day is diffed against the membership REPLAYED FROM
  THE LOG, never against whichever dated file precedes it on disk (see `record_membership`), and the FIRST
  logged resolution of a day is authoritative — a same-day re-resolution that comes out different is not
  logged (a known limitation, recorded in KNOWN_RISKS).
* On first use the log SEEDS ITSELF, in ONE atomic write, from the dated `active_ohlcv_*.parquet` files
  already on disk.

What this does NOT decide (ADR-059 D7): what Gold does with the rows of a departed symbol. That is a
separate decision; this log only makes the question answerable. Feature and label builders (ADR-053 D2)
should take membership from `members_as_of`.

Never blocking: the resolver calls `record_membership` inside a try/except; a log failure must not fail
the resolution that produced a perfectly good active set.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
from loguru import logger

from src.utils.atomic_io import atomic_write_parquet

LOG_FILENAME = "membership_log.parquet"
_FILE_RE = re.compile(r"^active_ohlcv_(\d{4}-\d{2}-\d{2})\.parquet$")

SCHEMA = {
    "date": pl.Utf8, "market": pl.Utf8, "symbol": pl.Utf8, "event": pl.Utf8,
    "rank": pl.Int64, "dollar_volume_20d": pl.Float64, "compared_with": pl.Utf8, "logged_at": pl.Utf8,
}
_KEY = ["date", "market", "symbol", "event"]


def _dated_files(out_dir: Path) -> list[tuple[str, Path]]:
    return sorted((m.group(1), f) for f in out_dir.glob("active_ohlcv_*.parquet")
                  if (m := _FILE_RE.match(f.name)))


def _load_members(path: Path) -> pl.DataFrame | None:
    """(market, symbol, rank, dollar_volume_20d) of a REAL resolution; None for a fallback/unreadable file."""
    try:
        df = pl.read_parquet(path)
    except Exception as e:                                       # noqa: BLE001
        logger.warning(f"[MembershipLog] {path.name} unreadable ({e}) — skipped")
        return None
    if "is_fallback" in df.columns and df["is_fallback"].fill_null(False).any():
        return None
    if "market" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("market"))
    df = df.with_columns(pl.col("market").fill_null("unknown"))     # a null never matches in a join
    if "dollar_volume_20d" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("dollar_volume_20d"))
    return (df.select(["market", "symbol", pl.col("dollar_volume_20d").cast(pl.Float64)])
              .with_columns(pl.col("dollar_volume_20d").rank("ordinal", descending=True)
                            .over("market").cast(pl.Int64).alias("rank")))


def _events(day: str, cur: pl.DataFrame, prev: pl.DataFrame | None, prev_day: str | None, now: str) -> pl.DataFrame:
    key = ["market", "symbol"]
    if prev is None:
        entered, left = cur, cur.clear()
    else:
        entered = cur.join(prev.select(key), on=key, how="anti")
        left = prev.join(cur.select(key), on=key, how="anti")

    def build(frame: pl.DataFrame, event: str) -> pl.DataFrame:
        return frame.select([
            pl.lit(day).alias("date"), "market", "symbol", pl.lit(event).alias("event"), "rank",
            "dollar_volume_20d", pl.lit(prev_day, dtype=pl.Utf8).alias("compared_with"),
            pl.lit(now).alias("logged_at"),
        ])

    return pl.concat([build(entered, "enter"), build(left, "leave")]).cast(SCHEMA)


def _append(out_dir: Path, new: pl.DataFrame) -> int:
    """Add only the events whose key is not logged yet; existing rows are carried over untouched."""
    path = out_dir / LOG_FILENAME
    existing = pl.read_parquet(path).cast(SCHEMA) if path.exists() else pl.DataFrame(schema=SCHEMA)
    fresh = new.join(existing.select(_KEY), on=_KEY, how="anti")
    if fresh.is_empty():
        return 0
    atomic_write_parquet(pl.concat([existing, fresh]), path, compression="zstd", compression_level=3)
    return fresh.height


def record_membership(out_dir: Path, run_date: date) -> int:
    """Log the membership change of `run_date`. Returns the number of events appended.

    * No log yet: SEED it from every real `active_ohlcv_*` file on disk up to run_date, each diffed
      against the real file before it (the first one becomes initial entries).
    * Log present: FORWARD-ONLY and driven by the LOG, not by old files. A run_date that is not later
      than the newest logged date is a no-op (idempotent re-run / past-day backfill; the FIRST logged
      resolution of a day is authoritative — a same-day re-resolution that comes out different is NOT
      logged, a known limitation recorded in KNOWN_RISKS), and a new day is
      diffed against the membership REPLAYED FROM THE LOG as of the day before — never against whatever
      file happens to precede it on disk. Diffing against the previous file would, after an old file is
      deleted, span the gap and append events that contradict ones already logged and corrupt the replay.
      The previous file is consulted only to fill `rank` / `dollar_volume_20d` of the leavers (NULL when
      that file is gone)."""
    out_dir = Path(out_dir)
    day = run_date.isoformat()
    now = datetime.now(timezone.utc).isoformat()
    log = read_log(out_dir)

    if log.is_empty():
        real: list[tuple[str, pl.DataFrame]] = []
        for d, f in _dated_files(out_dir):
            if d > day:
                continue                                         # never log a day from the future
            members = _load_members(f)
            if members is not None:
                real.append((d, members))
        frames = [
            _events(d, cur, *((real[i - 1][1], real[i - 1][0]) if i > 0 else (None, None)), now)
            for i, (d, cur) in enumerate(real)
        ]
        # ONE atomic write for the whole seed: a crash part-way must not leave a partial log, which the
        # forward-only path below would then treat as the truth and diff the next day against.
        return _append(out_dir, pl.concat(frames)) if frames else 0

    prev_day = log["date"].max()
    if day <= prev_day:
        return 0
    cur_path = out_dir / f"active_ohlcv_{day}.parquet"
    cur = _load_members(cur_path) if cur_path.exists() else None
    if cur is None:                                              # no real resolution for run_date (fallback)
        return 0
    state = members_as_of(log, date.fromordinal(run_date.toordinal() - 1))
    prev_file = out_dir / f"active_ohlcv_{prev_day}.parquet"
    prev_members = _load_members(prev_file) if prev_file.exists() else None
    keys = pl.DataFrame({"market": [k[0] for k in state], "symbol": [k[1] for k in state]},
                        schema={"market": pl.Utf8, "symbol": pl.Utf8})
    detail = (prev_members.select(["market", "symbol", "dollar_volume_20d", "rank"])
              if prev_members is not None else
              pl.DataFrame(schema={"market": pl.Utf8, "symbol": pl.Utf8,
                                   "dollar_volume_20d": pl.Float64, "rank": pl.Int64}))
    prev = keys.join(detail, on=["market", "symbol"], how="left")
    return _append(out_dir, _events(day, cur, prev, prev_day, now))


def read_log(out_dir: Path) -> pl.DataFrame:
    path = Path(out_dir) / LOG_FILENAME
    return pl.read_parquet(path).cast(SCHEMA) if path.exists() else pl.DataFrame(schema=SCHEMA)


def members_as_of(log: pl.DataFrame, as_of: date) -> set[tuple[str | None, str]]:
    """{(market, symbol)} that were members on `as_of`, replayed from the log alone. Dates before the first
    logged resolution have no answer: returns an empty set (the log never invents history)."""
    day = as_of.isoformat()
    ev = log.filter(pl.col("date") <= day).sort("date", "event")   # 'enter' < 'leave' within a day is irrelevant:
    members: set[tuple[str | None, str]] = set()                   # a symbol cannot enter and leave on one day
    for r in ev.iter_rows(named=True):
        k = (r["market"], r["symbol"])
        members.add(k) if r["event"] == "enter" else members.discard(k)
    return members
