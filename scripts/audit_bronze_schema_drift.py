"""
scripts/audit_bronze_schema_drift.py — census of Bronze OHLCV schema drift and
Bronze-without-Silver gaps. READ-ONLY (parquet footers only, writes nothing).

Why this exists (FIX SIL-BRZ-SCHEMA-01, RISK-31 update 17 Oct 2026): Bronze is
append-only, so a symbol whose adapter changed a column dtype keeps both dtypes
on disk forever. Before the fix, Silver's `scan_parquet(glob)` raised SchemaError
on such a symbol and the error was swallowed as "no Bronze data" — CNH vanished
from Silver with no failed-checkpoint row and a still-green coverage check. The
fix covers every symbol the reader sees; this script answers the census
questions the fix cannot: WHICH symbols drift, and which Bronze (market, tf,
symbol) groups still have NO Silver file after a re-run.

    python scripts/audit_bronze_schema_drift.py                 # registry symbols only
    python scripts/audit_bronze_schema_drift.py --all-symbols   # include removed symbols
    python scripts/audit_bronze_schema_drift.py --market context

Exit code 1 if any drift or gap is found, 0 otherwise.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq

BRONZE_ROOT = Path("data/bronze/market/ohlcv")
SILVER_ROOT = Path("data/silver/market_ohlcv")

GroupKey = tuple[str, str, str]  # (market, timeframe, symbol)


def group_bronze_files(bronze_root: Path) -> dict[GroupKey, list[Path]]:
    """{market}/timeframe={tf}/source={src}/symbol={sym}/year=../month=../*.parquet
    -> files grouped across sources, exactly as Silver's wildcard glob groups them."""
    groups: dict[GroupKey, list[Path]] = defaultdict(list)
    for f in sorted(bronze_root.rglob("*.parquet")):
        rel = f.relative_to(bronze_root).parts
        kv = {p.split("=", 1)[0]: p.split("=", 1)[1] for p in rel if "=" in p}
        if "timeframe" in kv and "symbol" in kv:
            groups[(rel[0], kv["timeframe"], kv["symbol"])].append(f)
    return dict(groups)


def _schema_of(path: Path) -> dict[str, str]:
    schema = pq.read_schema(path)
    return {name: str(schema.field(name).type) for name in schema.names}


def find_drift(groups: dict[GroupKey, list[Path]], workers: int = 8) -> list[dict]:
    """Groups whose files disagree on a column's dtype or presence."""
    out = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for key, files in sorted(groups.items()):
            schemas = list(pool.map(_schema_of, files))
            seen: dict[str, set[str]] = defaultdict(set)
            for sc in schemas:
                for col, typ in sc.items():
                    seen[col].add(typ)
            cols = {c: sorted(t) for c, t in seen.items() if len(t) > 1}
            for c in seen:
                if sum(c in sc for sc in schemas) < len(schemas):
                    cols.setdefault(c, sorted(seen[c]) + ["<absent in some files>"])
            if cols:
                out.append({"key": key, "n_files": len(files), "columns": cols})
    return out


def find_silver_gaps(
    groups: dict[GroupKey, list[Path]],
    silver_root: Path,
    registry_symbols: Optional[set[str]] = None,
) -> list[GroupKey]:
    """Bronze groups with no Silver {SYM}_{TF}_silver.parquet."""
    gaps = []
    for (market, tf, sym) in sorted(groups):
        if registry_symbols is not None and sym not in registry_symbols:
            continue
        if not (silver_root / market / f"symbol={sym}" / f"{sym}_{tf}_silver.parquet").exists():
            gaps.append((market, tf, sym))
    return gaps


def _registry_symbols() -> set[str]:
    from src.config.instrument_loader import get_loader
    loader = get_loader()
    return set(loader.symbol_list()) | {i.symbol for i in loader.all_context(include_deferred=False)}


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--bronze-root", type=Path, default=BRONZE_ROOT)
    ap.add_argument("--silver-root", type=Path, default=SILVER_ROOT)
    ap.add_argument("--market", default=None, help="only this market bucket (e.g. context, forex)")
    ap.add_argument("--all-symbols", action="store_true",
                    help="do not restrict to the current instrument registry")
    args = ap.parse_args(argv)

    groups = group_bronze_files(args.bronze_root)
    if args.market:
        groups = {k: v for k, v in groups.items() if k[0] == args.market}
    registry = None if args.all_symbols else _registry_symbols()
    if registry is not None:
        groups = {k: v for k, v in groups.items() if k[2] in registry}

    print(f"Bronze groups scanned: {len(groups)} "
          f"({sum(len(v) for v in groups.values())} files)")
    drift = find_drift(groups)
    gaps = find_silver_gaps(groups, args.silver_root, registry)

    print(f"\nSCHEMA DRIFT — {len(drift)} group(s) (the reader reconciles String/Date "
          "timestamps and polygon-naive vs tz-aware; any other unresolvable supertype "
          "or a non-numeric price still raises and is marked FAILED):")
    for d in drift:
        m, tf, sym = d["key"]
        print(f"  {m}/{sym} {tf}  files={d['n_files']}  " +
              "; ".join(f"{c}: {t}" for c, t in d["columns"].items()))
    print(f"\nBRONZE WITHOUT SILVER — {len(gaps)} group(s):")
    for m, tf, sym in gaps:
        print(f"  {m}/{sym} {tf}")
    return 1 if (drift or gaps) else 0


if __name__ == "__main__":
    sys.exit(main())
