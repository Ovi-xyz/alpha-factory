"""
scripts/preflight/check_yfinance_tickers.py

ADD ADR-025 (GMI_Decision_Document_v2.docx, 2026-07-11): "Write per-source
connectivity-and-shape pre-flight scripts now ... as a checked-in,
repeatable artifact. Do not block this work on solving the sandbox's
network restriction — execution is deferred to whenever real network
access exists (target M1 hardware, or a separate non-blocking scheduled
CI smoke-test)."

This script has NOT been executed against live yfinance — the sandbox
that authored it has no network egress to finance.yahoo.com (confirmed:
network allowlist covers only pypi.org/github.com/npmjs.com-class package
registries — the same constraint documented in every prior GMI checkpoint,
Section 12.1 in each). Authoring does not require network access; running
it does. Run this on the target M1 hardware, or wire it into a separate,
non-blocking scheduled CI job (NOT the blocking validate-and-test
workflow — a live external API being temporarily unreachable should not
fail every PR).

Checks every Layer 2 context instrument's yfinance_symbol, with particular
attention to the 7 currencies added in this same implementation pass
(GMI_Decision_Document_v1.docx ADR-013/014: CNH/KRW/SGD/HKD/TWD/NOK;
GMI_Decision_Document_v2.docx ADR-024: MYR) — Gate 2 in both decision
documents explicitly flags these tickers as live-unconfirmed (except CNH,
per ADR-013's own stated verification) and MYR (per ADR-024's own stated
verification).

Usage:
    python scripts/preflight/check_yfinance_tickers.py
    python scripts/preflight/check_yfinance_tickers.py --group dollar_basket
    python scripts/preflight/check_yfinance_tickers.py --symbol MYR

Exit code 0 = every checked ticker returned a non-empty, correctly-shaped
DataFrame. Exit code 1 = at least one ticker failed.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

REQUIRED_COLUMNS = {"Open", "High", "Low", "Close", "Volume"}

# Gate 2 (GMI_Decision_Document_v1.docx §6 / v2.docx §8): tickers whose live
# existence/shape is explicitly flagged as unconfirmed at authoring time.
# CNH and MYR are the two exceptions — each ADR states its ticker was
# confirmed live via web search against Yahoo Finance's own listing pages,
# not via an actual API call from this sandbox (no network access either
# way) — "confirmed live" there means "confirmed to exist as a listed
# ticker," not "confirmed to return OHLCV data shaped correctly," which is
# exactly the gap this script closes.
GATE_2_UNCONFIRMED_SYMBOLS = frozenset({"KRW", "SGD", "HKD", "TWD", "NOK"})


def _check_one(symbol: str, yfinance_symbol: str) -> tuple[bool, str]:
    """Return (ok, message) for one ticker. Import yfinance lazily — this
    script may be authored/lint-checked in an environment without it
    installed."""
    try:
        import yfinance as yf
    except ImportError:
        return False, "yfinance not installed in this environment"

    try:
        df = yf.download(
            yfinance_symbol, period="5d", interval="1d", progress=False,
        )
    except Exception as e:
        return False, f"download() raised: {e}"

    if df is None or df.empty:
        return False, "empty DataFrame returned — ticker may not exist or has no recent data"

    # yfinance sometimes returns MultiIndex columns for a single ticker
    # depending on version/call shape — normalize before checking.
    cols = set(df.columns.get_level_values(0)) if hasattr(df.columns, "get_level_values") else set(df.columns)
    missing = REQUIRED_COLUMNS - cols
    if missing:
        return False, f"missing expected OHLCV columns: {missing} (got: {sorted(cols)})"

    return True, f"OK — {len(df)} rows, columns {sorted(cols)}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", default=None, help="Only check one context_group (e.g. dollar_basket)")
    parser.add_argument("--symbol", default=None, help="Only check one symbol (e.g. MYR)")
    args = parser.parse_args()

    from src.config.instrument_loader import get_loader

    loader = get_loader()
    instruments = loader.all_context(include_deferred=False)

    if args.symbol:
        instruments = [i for i in instruments if i.symbol == args.symbol]
    elif args.group:
        instruments = [i for i in instruments if i.context_group == args.group]

    if not instruments:
        print("No matching Layer 2 instruments found for the given filter.")
        return 1

    failures = []
    for inst in sorted(instruments, key=lambda i: i.symbol):
        flag = " [Gate 2: unconfirmed]" if inst.symbol in GATE_2_UNCONFIRMED_SYMBOLS else ""
        ok, msg = _check_one(inst.symbol, inst.yfinance_symbol)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {inst.symbol:8s} ({inst.yfinance_symbol:14s}){flag}  {msg}")
        if not ok:
            failures.append(inst.symbol)

    print()
    if failures:
        print(f"{len(failures)}/{len(instruments)} tickers FAILED: {failures}")
        return 1

    print(f"All {len(instruments)} tickers PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
