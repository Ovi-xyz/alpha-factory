"""
scripts/audit_silver_period_duplicates.py — census of Silver 1W / 1M periods that hold MORE THAN ONE bar.
READ-ONLY (reads the `timestamp` column of each Silver file, writes nothing).

Why this exists (FIX SIL-DEDUP-PERIOD-01, RISK-49, 13 Nov 2026): Silver deduplicated coarse bars on the raw
timestamp, so a partial-window Bronze bar stamped inside a period (VIX 1M: 2026-11-03 next to the real
2026-11-01 month bar) survived beside it. Measured on Gold 1M: a duplicated November 2026 period for 156 of
200 symbols. The fix keeps one bar per period; this script is the census before it (how many symbols, which
periods) and the check after it (a clean Silver tree reports none). The period is computed by the SAME
function the processor uses (src.silver.ohlcv_processor.period_key_expr), so the two cannot drift apart.

    python scripts/audit_silver_period_duplicates.py                  # 1W and 1M, every Silver market
    python scripts/audit_silver_period_duplicates.py --tf 1M --limit 40
    python scripts/audit_silver_period_duplicates.py --market context

Exit code 1 if any period holds more than one bar, 0 otherwise.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

import polars as pl

from src.silver.ohlcv_processor import _PERIOD_DEDUP_TIMEFRAMES, period_key_expr

SILVER_ROOT = Path("data/silver/market_ohlcv")


def silver_files(silver_root: Path, timeframes: tuple[str, ...], market: Optional[str] = None):
    """{market}/symbol={SYM}/{SYM}_{TF}_silver.parquet -> sorted (market, symbol, timeframe, path)."""
    out = []
    for tf in timeframes:
        for path in silver_root.glob(f"*/symbol=*/*_{tf}_silver.parquet"):
            mkt = path.parent.parent.name
            if market and mkt != market:
                continue
            out.append((mkt, path.parent.name.split("=", 1)[1], tf, path))
    return sorted(out, key=lambda r: (r[2], r[0], r[1]))


def find_period_duplicates(files) -> tuple[list[dict], int]:
    """Periods holding more than one bar. Returns (findings, files_read); a file that cannot be read is
    reported as a finding with period None rather than skipped."""
    findings: list[dict] = []
    read = 0
    for mkt, sym, tf, path in files:
        try:
            ts = pl.read_parquet(path, columns=["timestamp"])
        except Exception as e:  # unreadable / schema-less file: say so, never skip silently
            findings.append({"market": mkt, "symbol": sym, "timeframe": tf, "period": None,
                             "n": 0, "stamps": [], "error": str(e).splitlines()[0]})
            continue
        read += 1
        ts = ts.with_columns(
            pl.col("timestamp").dt.date().alias("stamp"),
            period_key_expr(tf).alias("period"),
        )
        dup = ts.group_by("period").agg(pl.len().alias("n"), pl.col("stamp").sort().alias("stamps")).filter(pl.col("n") > 1)
        for period, n, stamps in dup.sort("period").iter_rows():
            findings.append({"market": mkt, "symbol": sym, "timeframe": tf, "period": period,
                             "n": n, "stamps": stamps})
    return findings, read


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--silver-root", type=Path, default=SILVER_ROOT)
    ap.add_argument("--tf", action="append", choices=list(_PERIOD_DEDUP_TIMEFRAMES),
                    help="timeframe to check (repeatable); default: 1W and 1M")
    ap.add_argument("--market", default=None, help="only this market bucket (e.g. context, forex)")
    ap.add_argument("--limit", type=int, default=15, help="examples to print")
    args = ap.parse_args(argv)

    tfs = tuple(args.tf) if args.tf else _PERIOD_DEDUP_TIMEFRAMES
    files = silver_files(args.silver_root, tfs, args.market)
    findings, read = find_period_duplicates(files)
    bad = [f for f in findings if f["period"] is not None]
    unreadable = [f for f in findings if f["period"] is None]

    print(f"Silver files scanned: {len(files)} ({read} read) | timeframes: {', '.join(tfs)}")
    for tf in tfs:
        tf_bad = [f for f in bad if f["timeframe"] == tf]
        syms = {(f["market"], f["symbol"]) for f in tf_bad}
        print(f"\n{tf}: {len(syms)} symbol(s) with a period holding more than one bar | "
              f"{len(tf_bad)} (symbol, period) pair(s)")
        for mkt, n in sorted(Counter(f["market"] for f in tf_bad).items()):
            print(f"   {mkt}: {len({f['symbol'] for f in tf_bad if f['market'] == mkt})} symbol(s)")
        by_period = Counter(f["period"] for f in tf_bad)
        for period, n in sorted(by_period.items())[:10]:
            print(f"   period {period}: {n} symbol(s)")
        for f in tf_bad[: args.limit]:
            print(f"   e.g. {f['market']}/{f['symbol']} {tf} {f['period']}: {f['n']} bars stamped "
                  f"{', '.join(str(s) for s in f['stamps'])}")
    if unreadable:
        print(f"\nUNREADABLE — {len(unreadable)} file(s):")
        for f in unreadable[: args.limit]:
            print(f"   {f['market']}/{f['symbol']} {f['timeframe']}: {f['error']}")
    return 1 if (bad or unreadable) else 0


if __name__ == "__main__":
    sys.exit(main())
