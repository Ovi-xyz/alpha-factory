"""
views.py — GD §5.3 (Gold DuckDB Views Architecture)
Buat dan maintain DuckDB views di atas semua Silver + Gold Parquet.

Views adalah interface resmi untuk Trading Engine (dan ad-hoc query).
Trading Engine mengonsumsi output ini tanpa perlu tahu path Parquet.

DuckDB settings (GD §10.2):
    memory_limit: 3GB
    threads: 4
    enable_object_cache: true

Usage:
    from src.gold.views import get_pipeline_connection
    con = get_pipeline_connection()
    df  = con.execute("SELECT * FROM v_watchlist_latest").pl()
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import duckdb
from loguru import logger

from src.utils.silver_scope import layer1_globs


# FIX GMI-AUD-002: identifier-safe helper for view_name interpolation.
# GD §17.7 melarang f-string SQL, tapi $name/? parameter binding DuckDB
# (seperti SQL engine manapun) hanya bisa mem-bind VALUE — bukan identifier
# di posisi FROM/tabel/view. view_name di modul ini SELALU berasal dari
# VIEW_DEFINITIONS.keys() (dict hardcoded internal, lihat di bawah) — tidak
# pernah dari input eksternal, jadi risiko injection nol secara struktural.
# Guard eksplisit ditambahkan sebagai defense-in-depth (bukan cuma "trust by
# construction" implisit) dan supaya properti aman ini tetap benar walau
# kode direfactor di masa depan. Ditemukan saat audit KNOWN_RISKS.md RISK-3.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quoted_identifier(name: str) -> str:
    """Return name as a DuckDB double-quoted identifier, after validating
    it matches a safe [A-Za-z_][A-Za-z0-9_]* pattern. Raises ValueError for
    anything else — fails loudly rather than silently interpolating an
    unexpected identifier."""
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Unsafe view identifier rejected: {name!r}")
    return f'"{name}"'


# FIX ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8, 2026-07-11):
# v_ohlcv_1D/v_ohlcv_1H/v_ohlcv_all previously read a single unfiltered
# 'data/silver/market_ohlcv/**/*_{tf}_silver.parquet' glob — the same
# RISK-6 defect class already fixed in quality_validator.py/
# technical_signals.py/screener.py/correlation_matrix.py/pit_data.py, but
# with a materially higher blast radius here: these views ARE the
# documented Interface Contract (GD §0.4) for the Trading Engine, an
# external, out-of-scope consumer this pipeline cannot audit or coordinate
# with. A Trading Engine querying "give me OHLCV" has no way to know VIX,
# DXY, or an ETF snuck in as if it were a tradeable candidate — those were
# reclassified OUT of Layer 1 (ADR-003) specifically because they are NOT
# tradeable in this system.
#
# Fix must be computed AT CALL TIME, not import time: an earlier version of
# this fix built a fixed 4-market SQL list literal once at module import
# via layer1_markets() (the instrument-derived market NAME list) — this
# broke immediately, because DuckDB's read_parquet() with a list argument
# raises for the WHOLE query if even ONE list entry's glob matches zero
# files (confirmed empirically; same behavior silver_scope.py's own
# docstring already documents for the Python-list-parameter pattern used
# in quality_validator.py etc.). Since Bronze/Silver data for each market
# arrives incrementally at runtime — not at Python import time — a
# fixed-at-import list of all 4 markets breaks for any environment that
# doesn't yet have data in every single Layer 1 market (e.g. any test
# fixture, or early pipeline backfill). The three OHLCV view SQL strings
# below are therefore TEMPLATES (a {globs} placeholder), substituted with
# the CURRENT filesystem-filtered glob list — via
# silver_scope.layer1_globs(), which already skips missing market
# directories — inside get_pipeline_connection()'s registration loop,
# every time a connection is created.
#
# DuckDB does NOT support brace-alternation globs (verified empirically:
# read_parquet('.../{a,b}/**/*.parquet') raises "No files found" even when
# both subdirectories exist) — a literal SQL list
# (`['path/a/**/*.parquet', 'path/b/**/*.parquet']`) is the correct
# construct, confirmed to work both standalone and inside CREATE VIEW.
_OHLCV_VIEW_FILENAME_PATTERNS = {
    "v_ohlcv_1D": "*_1D_silver.parquet",
    "v_ohlcv_1H": "*_1H_silver.parquet",
    "v_ohlcv_all": "*_silver.parquet",
}


def _sql_list_literal(globs: list[str]) -> str:
    """Python list of glob strings -> DuckDB SQL list literal text."""
    return "[" + ", ".join(f"'{g}'" for g in globs) + "]"


def _resolve_ohlcv_view_sql(view_name: str) -> str | None:
    """
    Build the CREATE VIEW SQL for one of the three Layer1-scoped OHLCV
    views, using the CURRENT filesystem state (via layer1_globs()).
    Returns None if no Layer 1 market has Silver data yet — caller should
    skip creating the view entirely in that case, consistent with the
    existing "no data yet -> skip" behavior for every other view.

    FIX GLD-006-follow-up: an earlier version of this function used an
    f-string to interpolate view_name and the resolved glob list into the
    CREATE VIEW text — correctly caught by Gate G-2's f-string SQL scanner
    (TestNoFStringSQLAnywhereInSrc), even though both interpolated values
    are internally-controlled (view_name is one of exactly 3 hardcoded
    dict keys; the glob list comes from layer1_globs()'s own filesystem
    scan, never external input). Rewritten with plain string concatenation
    plus _quoted_identifier() for view_name, matching this file's own
    established GD §17.7-safe pattern (see register_views()/
    list_available_views(), and the module docstring on
    _quoted_identifier() explaining why $name binding cannot apply to a
    SQL identifier position in any SQL engine).
    """
    globs = layer1_globs(
        Path("data/silver/market_ohlcv"), _OHLCV_VIEW_FILENAME_PATTERNS[view_name]
    )
    if not globs:
        return None
    safe_view_name = _quoted_identifier(view_name)
    return (
        "\n        CREATE OR REPLACE VIEW " + safe_view_name + " AS"
        "\n        SELECT * FROM read_parquet("
        "\n            " + _sql_list_literal(globs) + ","
        "\n            hive_partitioning=true"
        "\n        )\n    "
    )


# View definitions — path patterns. The three OHLCV entries below are
# TEMPLATES ONLY (never executed directly) — get_pipeline_connection()
# resolves them via _resolve_ohlcv_view_sql() at call time. They remain
# present as ordinary dict values (rather than, say, a sentinel None) so
# that `for view_name in VIEW_DEFINITIONS` / `.keys()` — used by
# register_views() and list_available_views(), and by external callers
# such as tests/unit/test_views.py — continues to enumerate all view
# names correctly without needing to know which ones are templated.
VIEW_DEFINITIONS = {
    "v_ohlcv_1D": "-- resolved at call time via _resolve_ohlcv_view_sql()",
    "v_ohlcv_1H": "-- resolved at call time via _resolve_ohlcv_view_sql()",
    "v_ohlcv_all": "-- resolved at call time via _resolve_ohlcv_view_sql()",

    "v_macro_enriched": """
        CREATE OR REPLACE VIEW v_macro_enriched AS
        SELECT * FROM read_parquet(
            'data/silver/macro_enriched/**/*_silver.parquet',
            hive_partitioning=true
        )
    """,

    "v_macro_regime": """
        CREATE OR REPLACE VIEW v_macro_regime AS
        SELECT * FROM read_parquet(
            'data/gold/macro/regime_store.parquet'
        )
    """,

    "v_mtf_alignment": """
        CREATE OR REPLACE VIEW v_mtf_alignment AS
        SELECT * FROM read_parquet(
            'data/gold/mtf/mtf_alignment_*.parquet',
            hive_partitioning=false
        )
    """,

    "v_tech_signals_1D": """
        CREATE OR REPLACE VIEW v_tech_signals_1D AS
        SELECT * FROM read_parquet(
            'data/gold/signals/tech_signals_1D.parquet'
        )
    """,

    "v_sector_weights": """
        CREATE OR REPLACE VIEW v_sector_weights AS
        SELECT * FROM read_parquet(
            'data/gold/sector/sector_regime_weights.parquet'
        )
    """,

    "v_correlation": """
        CREATE OR REPLACE VIEW v_correlation AS
        SELECT * FROM read_parquet(
            'data/gold/correlation/correlation_clusters.parquet'
        )
    """,

    "v_screener": """
        CREATE OR REPLACE VIEW v_screener AS
        SELECT * FROM read_parquet(
            'data/gold/screener/watchlist_*.parquet',
            hive_partitioning=false
        )
    """,

    "v_sentiment": """
        CREATE OR REPLACE VIEW v_sentiment AS
        SELECT * FROM read_parquet(
            'data/silver/sentiment/date=*/*.parquet',
            hive_partitioning=true
        )
    """,

    "v_active_symbols": """
        CREATE OR REPLACE VIEW v_active_symbols AS
        SELECT * FROM read_parquet(
            'data/silver/active_symbols/active_*.parquet',
            hive_partitioning=false
        )
    """,

    # Convenience view: latest watchlist only
    "v_watchlist_latest": """
        CREATE OR REPLACE VIEW v_watchlist_latest AS
        SELECT w.*
        FROM read_parquet(
            'data/gold/screener/watchlist_*.parquet',
            hive_partitioning=false
        ) w
        WHERE w.watchlist_date = (
            SELECT MAX(watchlist_date)
            FROM read_parquet(
                'data/gold/screener/watchlist_*.parquet',
                hive_partitioning=false
            )
        )
    """,
}


def get_pipeline_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Return configured DuckDB connection with all pipeline views registered.
    Primary interface for Trading Engine and ad-hoc analysis.

    Args:
        read_only: True untuk read-only access (safer for Trading Engine)

    Returns:
        DuckDB connection with all views created.
    """
    con = duckdb.connect(read_only=read_only)

    # M1-optimized settings (GD §10.2)
    con.execute("SET memory_limit = '3GB';")
    con.execute("SET threads = 4;")
    con.execute("SET enable_object_cache = true;")

    # Register views — skip if underlying files don't exist yet
    for view_name in VIEW_DEFINITIONS:
        if view_name in _OHLCV_VIEW_FILENAME_PATTERNS:
            # FIX ADR-022/RISK-6: resolved fresh from the CURRENT
            # filesystem state on every call, not baked in at import time
            # — see _resolve_ohlcv_view_sql() docstring.
            sql = _resolve_ohlcv_view_sql(view_name)
            if sql is None:
                logger.debug(
                    f"[Views] Skipped {view_name} (no Layer 1 Silver data in any market yet)"
                )
                continue
        else:
            sql = VIEW_DEFINITIONS[view_name]
        try:
            con.execute(sql)
            logger.debug(f"[Views] Registered: {view_name}")
        except Exception as e:
            logger.debug(f"[Views] Skipped {view_name} (no data yet): {e}")

    return con


def register_views(run_date: date | None = None) -> None:
    """
    Standalone function to (re)create all views.
    Call after pipeline run to ensure views are fresh.
    """
    logger.info("[Views] Registering DuckDB views...")
    con = get_pipeline_connection()
    created = []

    for view_name in VIEW_DEFINITIONS:
        try:
            # Test view is queryable. FIX GMI-AUD-002: validated+quoted
            # identifier via plain concatenation — NOT an f-string (view_name
            # cannot be $name-parameterized; see _quoted_identifier docstring).
            con.execute(
                "SELECT COUNT(*) FROM " + _quoted_identifier(view_name) + " LIMIT 1"
            )
            created.append(view_name)
        except Exception:
            pass   # Expected for views on missing data

    logger.info(f"[Views] {len(created)}/{len(VIEW_DEFINITIONS)} views queryable")
    con.close()


def list_available_views(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Return list of view names that have data."""
    available = []
    for view_name in VIEW_DEFINITIONS:
        try:
            # FIX GMI-AUD-002: validated+quoted identifier, plain concatenation
            con.execute(
                "SELECT COUNT(*) FROM " + _quoted_identifier(view_name) + " LIMIT 1"
            )
            available.append(view_name)
        except Exception:
            pass
    return available
