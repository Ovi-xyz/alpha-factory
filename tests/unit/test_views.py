"""
tests/unit/test_views.py — GMI Wave 1 Cycle 3 Audit
Regression guard for FIX GMI-AUD-002: views.py's f-string SQL identifier
interpolation (`f"SELECT COUNT(*) FROM {view_name} LIMIT 1"`).

Dokumen referensi: KNOWN_RISKS.md RISK-3

No test file existed for src/gold/views.py before this (verified empty
before writing this file). view_name cannot be $name-parameterized (SQL
parameter binding is for values, not identifiers, in any engine) — the
correct fix is validated + quoted identifier interpolation via plain
string concatenation, not an f-string. These tests prove BOTH halves of
that claim: the helper validates correctly, AND the two call sites that
use it still behave identically to the pre-fix version for legitimate
view names.
"""

from __future__ import annotations

import pytest

from src.gold.views import (
    VIEW_DEFINITIONS,
    _quoted_identifier,
    list_available_views,
    register_views,
)


class TestQuotedIdentifierHelper:

    def test_valid_identifier_quoted(self):
        assert _quoted_identifier("v_ohlcv_1D") == '"v_ohlcv_1D"'

    def test_valid_identifier_with_underscore_prefix(self):
        assert _quoted_identifier("_internal") == '"_internal"'

    @pytest.mark.parametrize("bad", [
        "v_bad; DROP TABLE x --",
        "v with space",
        "1_starts_with_digit",
        "v-with-dash",
        'v"quote',
        "",
    ])
    def test_unsafe_identifier_rejected(self, bad):
        with pytest.raises(ValueError, match="Unsafe view identifier"):
            _quoted_identifier(bad)

    def test_every_real_view_definition_key_is_valid(self):
        """All keys actually used in VIEW_DEFINITIONS must pass validation —
        proves the fix doesn't accidentally break real, legitimate views."""
        for view_name in VIEW_DEFINITIONS:
            assert _quoted_identifier(view_name) == f'"{view_name}"'


class TestViewFunctionsUseValidatedIdentifiers:
    """register_views()/list_available_views() must not regress to raw
    f-string interpolation — exercised against real (empty) DuckDB state,
    not just source-text inspection."""

    def test_register_views_runs_without_raising(self, tmp_path, monkeypatch):
        """No Silver/Gold data exists in tmp_path — every view query should
        fail gracefully (caught, logged) rather than raise, exactly as the
        pre-fix version did for missing data.

        UPD ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8):
        the three OHLCV views now resolve their glob list via
        layer1_globs() -> layer1_markets() -> get_loader(), which reads
        config/instruments.yaml via a path relative to the process CWD.
        get_loader() is pre-warmed here (called once, from the repo root,
        BEFORE chdir) so the Layer 1 market NAME list (a stable
        architectural fact — "us_stocks", "idx", etc. — independent of
        which tmp_path fixture files happen to exist) remains resolvable
        after chdir. This does not defeat the test: the actual glob ROOT
        used to scan for Parquet files is still a path relative to the
        post-chdir CWD (tmp_path), so "no data in tmp_path" is still
        genuinely exercised."""
        from src.config.instrument_loader import get_loader
        get_loader()  # pre-warm lru_cache before chdir changes relative-path resolution
        monkeypatch.chdir(tmp_path)
        register_views()   # must not raise

    def test_list_available_views_empty_when_no_data(self, tmp_path, monkeypatch):
        from src.config.instrument_loader import get_loader
        get_loader()  # pre-warm — see test_register_views_runs_without_raising docstring
        monkeypatch.chdir(tmp_path)
        from src.gold.views import get_pipeline_connection
        con = get_pipeline_connection()
        available = list_available_views(con)
        con.close()
        assert available == []

    def test_list_available_views_finds_populated_view(self, tmp_path, monkeypatch):
        """With real Silver data present, the view must be reported
        available — proves the quoted-identifier query actually executes
        successfully against a real view, not just 'doesn't crash on
        missing data'.

        UPD ADR-022/RISK-6: get_loader() pre-warmed before chdir — see
        test_register_views_runs_without_raising docstring."""
        import polars as pl
        from datetime import date
        from src.config.instrument_loader import get_loader

        get_loader()  # pre-warm before chdir
        monkeypatch.chdir(tmp_path)
        ohlcv_dir = (
            tmp_path / "data" / "silver" / "market_ohlcv" / "us_stocks"
            / "symbol=AAPL"
        )
        ohlcv_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": ["AAPL"], "timestamp": [date(2026, 7, 1)],
            "close": [150.0],
        }).write_parquet(ohlcv_dir / "AAPL_1D_silver.parquet")

        from src.gold.views import get_pipeline_connection
        con = get_pipeline_connection()
        available = list_available_views(con)
        con.close()
        assert "v_ohlcv_1D" in available


class TestOhlcvViewsGlobScope:
    """
    NEW — ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8,
    2026-07-11). v_ohlcv_1D/v_ohlcv_1H/v_ohlcv_all previously read a single
    unfiltered 'market_ohlcv/**/*_{tf}_silver.parquet' glob — the same
    defect class already fixed in quality_validator.py/technical_signals.py
    /screener.py/correlation_matrix.py/pit_data.py, found here by Gate G-8's
    static scanner. These tests prove the actual correctness property
    (Layer 2 rows excluded), not just "doesn't crash."
    """

    def test_v_ohlcv_1d_excludes_layer2_context_data(self, tmp_path, monkeypatch):
        """The single most important property this fix guarantees: a
        Layer 2 context instrument's Silver OHLCV (e.g. VIX, living under
        market_ohlcv/context/) must NOT appear in v_ohlcv_1D — the
        documented Trading Engine Interface Contract (GD §0.4) for
        tradeable-candidate OHLCV data."""
        import polars as pl
        from datetime import date
        from src.config.instrument_loader import get_loader

        get_loader()  # pre-warm before chdir — see TestViewFunctionsUseValidatedIdentifiers
        monkeypatch.chdir(tmp_path)

        l1_dir = tmp_path / "data" / "silver" / "market_ohlcv" / "us_stocks" / "symbol=AAPL"
        l1_dir.mkdir(parents=True)
        pl.DataFrame({
            "symbol": ["AAPL"], "timestamp": [date(2026, 7, 1)], "close": [150.0],
        }).write_parquet(l1_dir / "AAPL_1D_silver.parquet")

        l2_dir = tmp_path / "data" / "silver" / "market_ohlcv" / "context" / "symbol=VIX"
        l2_dir.mkdir(parents=True)
        pl.DataFrame({
            "symbol": ["VIX"], "timestamp": [date(2026, 7, 1)], "close": [18.5],
        }).write_parquet(l2_dir / "VIX_1D_silver.parquet")

        from src.gold.views import get_pipeline_connection
        con = get_pipeline_connection()
        result = con.execute("SELECT DISTINCT symbol FROM v_ohlcv_1D").fetchall()
        con.close()

        symbols = {r[0] for r in result}
        assert symbols == {"AAPL"}, (
            f"v_ohlcv_1D must contain only Layer 1 symbols, got {symbols} "
            f"— VIX (Layer 2) leaking in is exactly the RISK-6 defect class"
        )

    def test_resolve_ohlcv_view_sql_returns_none_when_no_layer1_data(self, tmp_path, monkeypatch):
        """If Layer 1 has no Silver data in ANY market yet (e.g. only
        Layer 2 context data exists so far), the view must be skipped
        entirely — not created with an empty/invalid glob list."""
        import polars as pl
        from datetime import date
        from src.config.instrument_loader import get_loader

        get_loader()
        monkeypatch.chdir(tmp_path)

        l2_dir = tmp_path / "data" / "silver" / "market_ohlcv" / "context" / "symbol=VIX"
        l2_dir.mkdir(parents=True)
        pl.DataFrame({
            "symbol": ["VIX"], "timestamp": [date(2026, 7, 1)], "close": [18.5],
        }).write_parquet(l2_dir / "VIX_1D_silver.parquet")

        from src.gold.views import _resolve_ohlcv_view_sql
        assert _resolve_ohlcv_view_sql("v_ohlcv_1D") is None

    def test_v_ohlcv_1d_works_with_only_one_of_four_layer1_markets_populated(self, tmp_path, monkeypatch):
        """Regression guard for the FIRST (incorrect) version of this fix:
        hardcoding all 4 Layer 1 market globs into a SQL list literal at
        import time broke immediately, because DuckDB's read_parquet()
        with a list argument raises for the WHOLE query if even one
        entry's glob matches zero files — and market data arrives
        incrementally at runtime, not all 4 markets at once. Only
        us_stocks/ has data here; idx/forex/commodity do not exist at
        all. The view must still work, scoped to what actually exists."""
        import polars as pl
        from datetime import date
        from src.config.instrument_loader import get_loader

        get_loader()
        monkeypatch.chdir(tmp_path)

        l1_dir = tmp_path / "data" / "silver" / "market_ohlcv" / "us_stocks" / "symbol=AAPL"
        l1_dir.mkdir(parents=True)
        pl.DataFrame({
            "symbol": ["AAPL"], "timestamp": [date(2026, 7, 1)], "close": [150.0],
        }).write_parquet(l1_dir / "AAPL_1D_silver.parquet")
        # idx/, forex/, commodity/ deliberately do NOT exist

        from src.gold.views import get_pipeline_connection
        con = get_pipeline_connection()  # must not raise
        result = con.execute("SELECT symbol FROM v_ohlcv_1D").fetchall()
        con.close()
        assert result == [("AAPL",)]


class TestRegisterViewsSuccessPath:
    """Coverage tranche (17 Aug 2026) — created.append(view_name) on the
    success path. test_register_views_runs_without_raising only exercises
    the all-missing-data except branch; this exercises at least one view
    actually resolving, using the same fixture proven queryable by
    test_list_available_views_finds_populated_view."""

    def test_register_views_with_real_data_appends_created(self, tmp_path, monkeypatch):
        import polars as pl
        from datetime import date
        from src.config.instrument_loader import get_loader

        get_loader()  # pre-warm before chdir
        monkeypatch.chdir(tmp_path)
        ohlcv_dir = (
            tmp_path / "data" / "silver" / "market_ohlcv" / "us_stocks"
            / "symbol=AAPL"
        )
        ohlcv_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": ["AAPL"], "timestamp": [date(2026, 7, 1)],
            "close": [150.0],
        }).write_parquet(ohlcv_dir / "AAPL_1D_silver.parquet")

        register_views()   # must not raise; v_ohlcv_1D resolves -> created.append() hit


class TestViewsAcrossTheAdr055Cutover:
    """ADR-055 v2: the regime view is v_context_regime (not v_macro_regime), the sector view is
    gone with gold_sector, and every glob over dated files whose schema changed at the cutover (MTF,
    watchlist) must use union_by_name=true — a plain glob over old+new files fails."""

    def test_view_names(self):
        assert "v_context_regime" in VIEW_DEFINITIONS
        for retired in ("v_macro_regime", "v_sector_weights", "v_sector_map"):
            assert retired not in VIEW_DEFINITIONS, retired

    def test_no_view_reads_a_retired_store(self):
        for name, sql in VIEW_DEFINITIONS.items():
            assert "regime_store" not in sql and "sector_regime_weights" not in sql, name
            assert "data/gold/sector" not in sql, name

    @pytest.mark.parametrize("name", ["v_mtf_alignment", "v_screener", "v_watchlist_latest"])
    def test_dated_file_globs_use_union_by_name(self, name):
        assert name in VIEW_DEFINITIONS
        assert VIEW_DEFINITIONS[name].count("union_by_name=true") == VIEW_DEFINITIONS[name].count("read_parquet(")

    def test_v_mtf_alignment_reads_a_mix_of_old_and_new_files(self, tmp_path, monkeypatch):
        import duckdb
        import polars as pl
        from src.config.instrument_loader import get_loader

        get_loader()
        monkeypatch.chdir(tmp_path)
        d = tmp_path / "data" / "gold" / "mtf"
        d.mkdir(parents=True)
        pl.DataFrame({"symbol": ["A"], "mtf_score": [3], "regime_compatible": [True],
                      "active_regime": ["RISK_ON"], "date": ["2026-10-16"]}
                     ).write_parquet(d / "mtf_alignment_2026-10-16.parquet")
        pl.DataFrame({"symbol": ["A"], "mtf_score": [4], "date": ["2026-10-19"]}
                     ).write_parquet(d / "mtf_alignment_2026-10-19.parquet")
        con = duckdb.connect()
        con.execute(VIEW_DEFINITIONS["v_mtf_alignment"])
        rows = con.execute("SELECT date, mtf_score FROM v_mtf_alignment ORDER BY date").fetchall()
        assert rows == [("2026-10-16", 3), ("2026-10-19", 4)]
