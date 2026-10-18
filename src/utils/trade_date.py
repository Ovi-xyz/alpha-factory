"""
trade_date.py — canonical "trading date of a Silver 1D bar" for DuckDB SQL.

FIX TS-DATE-01 [chat thread, 16 Oct 2026] — no version bump.

CONTRACT (measured on live Silver 1D files, 16 Oct 2026: AAPL, AU, BBCA,
EUR_USD, VIX): a Silver 1D `timestamp` is stamped at 00:00 LOCAL EXCHANGE time
in the instrument's own IANA timezone, kept as a tz-aware Datetime —
America/New_York (US stocks, commodity), Asia/Jakarta (IDX), Europe/London
(forex), America/Chicago (VIX). It is NOT UTC midnight as Grand Design §4.3
("Datetime (UTC)") says. The stamp's OWN calendar date is the trading date.
The zone must never be converted to get it: converting a local-midnight stamp
to UTC or New York moves the date back a day for every zone east of the target
(BBCA: 2494 of 2494 rows under either; EUR_USD: 1563 of 2639 under UTC, all
under New York). Polars `.dt.date()` on the tz-aware column already returns
the stamp's own date — that is the correct Polars idiom and needs no helper.

WHY THIS MODULE EXISTS: DuckDB reads these columns as TIMESTAMP WITH TIME ZONE
and discards the zone name, keeping only the UTC instant. `CAST(timestamp AS
DATE)` on a TIMESTAMPTZ then converts into the *session* TimeZone — by default
the machine's own. So every such query silently depended on the host zone:
  - M1 on WIB (UTC+7): correct for every zone <= +7 (US, IDX, forex) but one
    day EARLY for the Layer 2 Asia-Pacific indices (N225, KOSPI, HSI, SSEC,
    TWSE: +8/+9; AXJO: +10/+11);
  - a UTC host (CI, any server): one day early for all of IDX and for forex
    during BST;
  - a New York host: one day early for IDX and all forex.
In the PIT guard (`< trade_date`) "one day early" is look-ahead: the bar stamped
D is treated as D-1 and returned for trade_date D.

HOW: for a TIMESTAMPTZ column, view the instant in a fixed UTC+14 zone and take
that wall-clock date. A bar stamped local midnight in any zone whose offset o
satisfies -10h < o <= +14h lands at (14h - o) after midnight, i.e. on the same
calendar date — independent of the session TimeZone. The universe spans
-6h..+11h. `Etc/GMT-14` is a fixed offset with no DST (POSIX sign is inverted).
Non-TIMESTAMPTZ columns (naive Datetime, Date — e.g. test fixtures) keep the
plain CAST, which never involved a zone.

USAGE (static SQL literals, no f-strings — GD §17.7): write the token
`__TRADE_DATE__` (column `timestamp`) or `__TRADE_DATE(s.timestamp)__` in the
query text and wrap the literal in `with_trade_date(...)`. Do NOT write
`CAST(timestamp AS DATE)` against Silver 1D in src/ — tests/unit/test_trade_date.py
enforces this.
"""

from __future__ import annotations

import re

# Fixed UTC+14, no DST. See module docstring for the valid offset range.
PROBE_TZ = "Etc/GMT-14"

# Optional table alias, optional dotted — identifiers cannot be bound as
# DuckDB parameters, so the column name is validated instead of trusted.
_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?")
_TOKEN = re.compile(r"__TRADE_DATE(?:\(([A-Za-z_][A-Za-z0-9_.]*)\))?__")


def trade_date_sql(col: str = "timestamp") -> str:
    """SQL expression: the stamp's own calendar date of `col`, independent of
    the DuckDB session TimeZone. Returns a DATE."""
    if not _IDENT.fullmatch(col):
        raise ValueError(f"trade_date_sql: not a plain column reference: {col!r}")
    return (
        "CASE WHEN typeof(" + col + ") = 'TIMESTAMP WITH TIME ZONE' "
        "THEN CAST(" + col + " AT TIME ZONE '" + PROBE_TZ + "' AS DATE) "
        "ELSE CAST(" + col + " AS DATE) END"
    )


def with_trade_date(sql: str) -> str:
    """Expand every `__TRADE_DATE__` / `__TRADE_DATE(col)__` token in `sql`.

    Raises if the text holds no token: a wrapped query that lost its token
    would silently go back to the session-zone cast."""
    expanded, n = _TOKEN.subn(
        lambda m: trade_date_sql(m.group(1) or "timestamp"), sql
    )
    if n == 0:
        raise ValueError("with_trade_date: no __TRADE_DATE__ token in the SQL text")
    return expanded
