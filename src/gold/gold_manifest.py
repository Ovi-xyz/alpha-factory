"""
src/gold/gold_manifest.py — GHR-08 (ADR-059, GMI-GHR-REC-001)

Run manifest + key snapshots for `gold_signals`, so a change in a Gold file's row count can be
EXPLAINED instead of guessed at.

Why (ADR-059 GD1/GD2/GD3, "do not compare Gold files by row counts alone"): `tech_signals_{TF}.parquet`
is a full rewrite from Silver on every run. On the 17 Oct run the reported deltas (1D +352, 1W +285,
1M +59) looked far too large for one new trading day. They were not a defect: the active set swapped
four symbols (F, WMB, CVNA, CB left; PTC, OXY, MO, SHW entered) and each swap moves a whole history
(~2,520 1D rows per symbol). The baseline file was overwritten, so the decomposition had to be
reconstructed from Silver afterwards, leaving unattributed residuals (-8 / +17 / +5). This module
records the decomposition AT RUN TIME:

    delta_rows = n_added_persisting + n_entered_rows - n_left_rows - n_removed_persisting
                 (new bars of symbols that stayed) (whole histories of entrants) (of leavers)
                 (bars that dropped out of a persisting symbol: a flag flip / restatement)

Two files per run, both under the operational `health` tree:

    gold_manifest/gold_signals_<run_date>[.rN].parquet   one row per timeframe, kept FOREVER (tiny)
    gold_keys/tech_signals_<TF>_<run_date>[.rN].parquet  (symbol, timestamp, close), last 7 per TF

Deviation from the ADR-059 sketch, deliberate: the snapshot holds `close` as well as the key. Keys alone
cannot tell a restated bar from an unchanged one, which would make `n_changed` unknowable; a single
Float64 column buys it. A same-day RE-RUN gets its own `.rN` suffix and is compared with the previous
run of that day (so it reports n_added = 0) — overwriting would destroy the first run's decomposition,
which is exactly the evidence this module exists to keep.

Never blocking: `record_run` is called from gold_signals inside a try/except; a manifest failure
must not fail the job that produced a perfectly good Gold file.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
from loguru import logger

from src.utils.atomic_io import atomic_write_parquet
from src.utils.data_paths import health_dir

KEEP_SNAPSHOTS_PER_TF = 7
# Daily-or-coarser bars are stamped at the exchange-local midnight, so the UTC time of day of a stamp
# identifies the cohort (US 04:00/05:00, IDX 17:00, FX 23:00/00:00 — ADR-059 GD8). Intraday stamps
# carry no cohort information, so no per-cohort maximum is recorded for them.
COHORT_TIMEFRAMES = ("1D", "1W", "1M")

_SNAP_RE = re.compile(r"^tech_signals_(?P<tf>[^_]+)_(?P<day>\d{4}-\d{2}-\d{2})(?:\.r(?P<seq>\d+))?\.parquet$")


def manifest_dir() -> Path:
    return health_dir() / "gold_manifest"


def keys_dir() -> Path:
    return health_dir() / "gold_keys"


def _seq_of(match: re.Match) -> int:
    return int(match.group("seq")) if match.group("seq") else 1


def list_snapshots(timeframe: str, base: Path | None = None) -> list[tuple[str, int, Path]]:
    """[(run_date, seq, path)] of this timeframe's key snapshots, oldest first."""
    base = base or keys_dir()
    out = []
    for f in base.glob(f"tech_signals_{timeframe}_*.parquet") if base.exists() else []:
        m = _SNAP_RE.match(f.name)
        if m and m.group("tf") == timeframe:
            out.append((m.group("day"), _seq_of(m), f))
    return sorted(out)


def decompose(prev: pl.DataFrame, cur: pl.DataFrame) -> dict:
    """Explain `cur` against `prev` (both: symbol, timestamp, close). See the module docstring."""
    prev_syms = set(prev["symbol"].unique().to_list())
    cur_syms = set(cur["symbol"].unique().to_list())
    persisting = prev_syms & cur_syms
    keys = ["symbol", "timestamp"]

    cur_p = cur.filter(pl.col("symbol").is_in(list(persisting)))
    prev_p = prev.filter(pl.col("symbol").is_in(list(persisting)))
    added = cur_p.join(prev_p.select(keys), on=keys, how="anti").height
    removed = prev_p.join(cur_p.select(keys), on=keys, how="anti").height
    both = cur_p.join(prev_p.select(keys + [pl.col("close").alias("close_prev")]), on=keys, how="inner")
    changed = both.filter(pl.col("close").ne_missing(pl.col("close_prev"))).height

    entered = cur.filter(~pl.col("symbol").is_in(list(prev_syms))).group_by("symbol").len()
    left = prev.filter(~pl.col("symbol").is_in(list(cur_syms))).group_by("symbol").len()
    entered_rows, left_rows = int(entered["len"].sum() or 0), int(left["len"].sum() or 0)
    return {
        "n_added_persisting": added,
        "n_entered_rows": entered_rows,
        "n_left_rows": left_rows,
        "n_removed_persisting": removed,
        "n_changed": changed,
        "delta_rows": added + entered_rows - left_rows - removed,
        "entered_symbols": json.dumps(dict(sorted(zip(entered["symbol"], entered["len"])))),
        "left_symbols": json.dumps(dict(sorted(zip(left["symbol"], left["len"])))),
    }


def _cohort_max_stamps(df: pl.DataFrame, timeframe: str) -> str | None:
    if timeframe not in COHORT_TIMEFRAMES or df.is_empty():
        return None
    if not isinstance(df.schema["timestamp"], pl.Datetime):      # a bare Date carries no time of day
        return None
    g = (df.with_columns(pl.col("timestamp").dt.strftime("%H:%M").alias("_hhmm"))
           .group_by("_hhmm").agg(pl.col("timestamp").max().alias("mx")).sort("_hhmm"))
    return json.dumps({h: m.isoformat() for h, m in zip(g["_hhmm"], g["mx"])})


def record_run(df: pl.DataFrame, timeframe: str, run_date: date, *, keys_base: Path | None = None) -> dict:
    """Snapshot `df` (symbol, timestamp, close, ...), compare it with the previous snapshot of this
    timeframe, rotate old snapshots, and return the manifest row (a dict). Writes ONLY the snapshot;
    the caller writes the manifest rows once per run via `write_manifest`."""
    base = keys_base or keys_dir()
    cur = df.select(["symbol", "timestamp", "close"])

    existing = list_snapshots(timeframe, base)
    day = run_date.isoformat()
    same_day = [s for s in existing if s[0] == day]
    seq = (max(s[1] for s in same_day) + 1) if same_day else 1
    new_key = (day, seq)
    earlier = [s for s in existing if (s[0], s[1]) < new_key]

    row: dict = {
        "run_date": day, "run_seq": seq, "timeframe": timeframe,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "rows_total": cur.height, "n_symbols": cur["symbol"].n_unique(),
        "cohort_max_stamps": _cohort_max_stamps(cur, timeframe),
        "prev_run_date": None, "prev_run_seq": None, "prev_rows_total": None,
        "n_added_persisting": None, "n_entered_rows": None, "n_left_rows": None,
        "n_removed_persisting": None, "n_changed": None, "delta_rows": None,
        "entered_symbols": None, "left_symbols": None,
    }
    if earlier:
        p_day, p_seq, p_path = earlier[-1]
        try:
            prev = pl.read_parquet(p_path)
            row.update(decompose(prev, cur))
            row.update({"prev_run_date": p_day, "prev_run_seq": p_seq, "prev_rows_total": prev.height})
        except Exception as e:                                   # noqa: BLE001 — treat as a first run
            logger.warning(f"[gold_manifest] previous snapshot {p_path.name} unreadable ({e}) — no comparison")

    suffix = "" if seq == 1 else f".r{seq}"
    atomic_write_parquet(cur, base / f"tech_signals_{timeframe}_{day}{suffix}.parquet",
                         compression="zstd", compression_level=3)
    for _, _, old in list_snapshots(timeframe, base)[:-KEEP_SNAPSHOTS_PER_TF]:
        try:
            old.unlink()
        except OSError:
            pass
    return row


def write_manifest(rows: list[dict], run_date: date, *, manifest_base: Path | None = None) -> Path | None:
    """One manifest file per run (rows = timeframes processed in this invocation). A re-run on the
    same day gets `.rN`; an earlier manifest is never rewritten."""
    if not rows:
        return None
    base = manifest_base or manifest_dir()
    day = run_date.isoformat()
    n = 1
    path = base / f"gold_signals_{day}.parquet"
    while path.exists():
        n += 1
        path = base / f"gold_signals_{day}.r{n}.parquet"
    atomic_write_parquet(pl.DataFrame(rows), path, compression="zstd", compression_level=3)
    return path
