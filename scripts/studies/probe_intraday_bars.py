"""
scripts/studies/probe_intraday_bars.py — ADR-053 Amendment 2 "step 0": MEASURE the 1H / 4H bar conventions before any
intraday feature builder is written. OFFLINE, READ-ONLY, imports nothing from `src`, makes no network call.

Why: the daily/weekly/monthly as-of rule is `bar date + 1 day <= decision date`. An intraday rule needs facts the
daily files never showed — the stamp timezone per market, bar-start vs bar-end, bars per session, 4H anchoring, the
state of the newest bar at fetch time (a partial candle is rewritten under the same key by the next run), and
whether the Gold copy keeps the Silver instants and a date that survives the Gold timezone. Findings measured on a
sample of this repo's Silver files (9 Nov 2026): US 1H stamped America/New_York at bar START (09:30..15:30, 7/day),
IDX Asia/Jakarta (09:00..16:00, 6-7/day, lunch gap), FX Europe/London hourly (24/day) with a flat newest bar, and the
US 4H file stamped NAIVE at 08:00 / 12:00 — not the 1H convention. This probe repeats that measurement on whatever
is on disk so the builder is written against the real files, not against this paragraph.

It prints numbers and sets NO pass/fail threshold (a threshold without a measured basis is a guess).

Usage (repo root):
    python scripts/studies/probe_intraday_bars.py [--silver-root data/silver/market_ohlcv] [--gold-dir data/gold/signals]
        [--sample 3] [--symbols AAPL,BBCA,EUR_USD] [--out probe.json]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Sequence

import polars as pl

TFS = ("1H", "4H")


def stamp_tz(df: pl.DataFrame) -> Optional[str]:
    dt = df.schema["timestamp"]
    return getattr(dt, "time_zone", None)


def sessions(df: pl.DataFrame) -> pl.DataFrame:
    """Per local calendar date (the stamp's OWN tz, or naive as written): bars, first / last stamp hour."""
    return (df.with_columns(pl.col("timestamp").dt.date().alias("d"), pl.col("timestamp").dt.hour().alias("h"))
              .group_by("d").agg(pl.len().alias("n"), pl.col("h").min().alias("h0"), pl.col("h").max().alias("h1")))


def profile_silver(path: Path) -> dict:
    df = pl.read_parquet(path).sort("timestamp")
    s = sessions(df)
    spacing = df["timestamp"].diff().drop_nulls().dt.total_minutes()
    last = df.tail(1)
    flat = bool(last.select(((pl.col("open") == pl.col("high")) & (pl.col("high") == pl.col("low"))
                             & (pl.col("low") == pl.col("close"))).alias("f"))["f"][0])
    return {
        "file": path.name, "rows": df.height, "tz": stamp_tz(df) or "NAIVE",
        "first": str(df["timestamp"].min()), "last": str(df["timestamp"].max()),
        "span_months": round((df["timestamp"].max() - df["timestamp"].min()).total_seconds() / 86400 / 30.4375, 1),
        "bars_per_day_mode": int(s["n"].mode().sort()[0]),
        "bars_per_day_dist": {int(k): int(v) for k, v in s["n"].value_counts().sort("n").head(6).iter_rows()},
        "first_hour_mode": int(s["h0"].mode().sort()[0]), "last_hour_mode": int(s["h1"].mode().sort()[0]),
        "median_spacing_min": float(spacing.median()) if spacing.len() else None,
        "is_clean_share": round(float(df["is_clean"].mean()), 4) if "is_clean" in df.columns else None,
        "newest_bar_flat_ohlc": flat, "newest_bar_stamp": str(last["timestamp"][0]),
    }


def compare_gold(gold_path: Path, symbol: str, silver_path: Path) -> dict:
    """Does the Gold copy keep the Silver INSTANTS, and does its date agree with the stamp's own local date?"""
    out: dict = {"gold_file": gold_path.name}
    if not gold_path.exists():
        return {**out, "status": "missing"}
    g = pl.scan_parquet(gold_path).filter(pl.col("symbol") == symbol).select("timestamp").collect().sort("timestamp")
    s = pl.read_parquet(silver_path).select("timestamp").sort("timestamp")
    gtz, stz = stamp_tz(g), stamp_tz(s)
    out.update({"gold_tz": gtz or "NAIVE", "silver_tz": stz or "NAIVE", "gold_rows": g.height, "silver_rows": s.height})
    if g.height == 0:
        return {**out, "status": "symbol_absent_in_gold"}
    if gtz and stz:
        # the two files may store different time units (ms vs us): compare as UTC instants at one unit
        utc = lambda c: pl.col(c).dt.convert_time_zone("UTC").dt.cast_time_unit("us")
        gi = g.with_columns(utc("timestamp").alias("utc"), pl.col("timestamp").dt.date().alias("gold_date"))
        si = s.with_columns(utc("timestamp").alias("utc"),
                            pl.col("timestamp").dt.convert_time_zone(stz).dt.date().alias("silver_date"))
        both = gi.join(si.select("utc", "silver_date"), on="utc", how="inner")
        out["gold_instants_in_silver"] = round(both.height / gi.height, 4)
        out["share_date_differs_gold_tz_vs_silver_local"] = (
            round(float((both["gold_date"] != both["silver_date"]).mean()), 4) if both.height else None)
    else:
        out["note"] = "a naive stamp cannot be compared instant-for-instant; compare the wall-clock hours instead"
        out["gold_hours"] = sorted(set(g["timestamp"].dt.hour().to_list()))[:12]
        out["silver_hours"] = sorted(set(s["timestamp"].dt.hour().to_list()))[:12]
    return {**out, "status": "ok"}


def markets(root: Path) -> list[str]:
    return sorted(p.name for p in root.iterdir() if p.is_dir())


def pick(root: Path, market: str, sample: int, wanted: Sequence[str]) -> list[str]:
    syms = sorted(p.name.split("=", 1)[1] for p in (root / market).glob("symbol=*"))
    chosen = [s for s in syms if s in wanted] if wanted else []
    return chosen + [s for s in syms if s not in chosen][: max(0, sample - len(chosen))]


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--silver-root", type=Path, default=Path("data/silver/market_ohlcv"))
    ap.add_argument("--gold-dir", type=Path, default=Path("data/gold/signals"))
    ap.add_argument("--sample", type=int, default=3, help="symbols per market")
    ap.add_argument("--symbols", default="", help="comma-separated symbols to include first")
    ap.add_argument("--out", type=Path, default=None)
    a = ap.parse_args(argv)
    wanted = [s for s in a.symbols.split(",") if s]
    report: list[dict] = []
    for market in markets(a.silver_root):
        for sym in pick(a.silver_root, market, a.sample, wanted):
            for tf in TFS:
                path = a.silver_root / market / f"symbol={sym}" / f"{sym}_{tf}_silver.parquet"
                if not path.exists():
                    continue
                row = {"market": market, "symbol": sym, "tf": tf, **profile_silver(path),
                       "gold": compare_gold(a.gold_dir / f"tech_signals_{tf}.parquet", sym, path)}
                report.append(row)
                g = row["gold"]
                print(f"{market:14s} {sym:10s} {tf}  tz={row['tz']:<18s} rows={row['rows']:>6d} span={row['span_months']:>5.1f}m "
                      f"bars/day mode={row['bars_per_day_mode']:>2d} hours {row['first_hour_mode']:>2d}-{row['last_hour_mode']:<2d} "
                      f"clean={row['is_clean_share']} newest_flat={row['newest_bar_flat_ohlc']} | gold: {g.get('status')} "
                      f"tz={g.get('gold_tz')} date_differs={g.get('share_date_differs_gold_tz_vs_silver_local')}")
    if a.out is not None:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        tmp = a.out.with_suffix(a.out.suffix + ".tmp")
        tmp.write_text(json.dumps(report, indent=2, default=str))
        tmp.replace(a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
