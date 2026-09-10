"""
tests/unit/test_silver_scope.py — silver_scope.py Unit Tests
ADD GMI-SCOPE-001 — see src/utils/silver_scope.py module docstring for the
three empirically-confirmed masking/pollution bugs this utility fixes.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.utils.silver_scope import (
    CONTEXT_MARKET,
    context_glob,
    context_peer_groups,
    layer1_globs,
    layer1_markets,
    layer1_peer_groups,
)


def _touch_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["X"], "timestamp": ["2026-01-01"]}).write_parquet(path)


class TestLayer1Markets:
    def test_returns_real_layer1_markets(self):
        """Sanity against the actual instruments.yaml v1.4 universe —
        empirically confirmed set (checkpoint + direct verification)."""
        assert layer1_markets() == ["commodity", "forex", "idx", "us_stocks"]

    def test_context_never_appears(self):
        assert CONTEXT_MARKET not in layer1_markets()

    def test_index_never_appears(self):
        """Permanently empty since ADR-003 (SPX/VIX reclassified to Layer 2)."""
        assert "index" not in layer1_markets()


class TestLayer1Globs:
    def test_skips_nonexistent_market_directories(self, tmp_path):
        """Only us_stocks/ exists on disk — the other 3 known Layer 1
        markets must be silently skipped, not included as dead globs."""
        _touch_parquet(tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet")
        globs = layer1_globs(tmp_path, "*_1D_silver.parquet")
        assert len(globs) == 1
        assert "us_stocks" in globs[0]

    def test_returns_empty_list_when_nothing_exists(self, tmp_path):
        """Fresh install / pre-backfill state — must return [], not raise
        and not silently include a dead glob that would break a DuckDB
        list-bound read_parquet() call downstream."""
        assert layer1_globs(tmp_path, "*_1D_silver.parquet") == []

    def test_never_includes_context_directory(self, tmp_path):
        """The whole point of this helper: context/ must never leak into
        a 'Layer 1' glob list, even when it exists on disk alongside
        Layer 1 markets.

        Uses a subdirectory (not tmp_path directly) for the glob root:
        pytest's tmp_path fixture is itself named after the test function
        ("test_never_includes_context_directory..."), so a naive
        substring check on the full glob string would spuriously match
        the fixture's own path, not the market segment this test actually
        cares about. Checking the constructed market-path prefix directly
        avoids that false positive.
        """
        root = tmp_path / "market_ohlcv"
        _touch_parquet(root / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet")
        _touch_parquet(root / "context" / "symbol=VIX" / "VIX_1D_silver.parquet")
        globs = layer1_globs(root, "*_1D_silver.parquet")
        context_prefix = str(root / "context")
        assert all(not g.startswith(context_prefix) for g in globs)

    def test_glob_has_single_double_star_only(self, tmp_path):
        """Guard against reintroducing the double-'**' DuckDB defect class
        (KNOWN_RISKS.md RISK-2 / checkpoint Section 4.3) — each returned
        glob string must contain exactly one '**' occurrence."""
        _touch_parquet(tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet")
        globs = layer1_globs(tmp_path, "*_1D_silver.parquet")
        for g in globs:
            assert g.count("**") == 1


class TestContextGlob:
    def test_returns_none_when_context_dir_absent(self, tmp_path):
        assert context_glob(tmp_path, "*_1D_silver.parquet") is None

    def test_returns_glob_when_context_dir_present(self, tmp_path):
        _touch_parquet(tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet")
        g = context_glob(tmp_path, "*_1D_silver.parquet")
        assert g is not None
        assert "context" in g
        assert g.count("**") == 1

    def test_never_matches_layer1_markets(self, tmp_path):
        _touch_parquet(tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet")
        _touch_parquet(tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet")
        g = context_glob(tmp_path, "*_1D_silver.parquet")
        assert "us_stocks" not in g


class TestScopingCorrectnessEndToEnd:
    """Reproduces the exact empirical probe used to discover the masking
    bugs (see silver_scope.py module docstring) — now as a permanent
    regression guard rather than an ad hoc sandbox script."""

    def test_context_rows_excluded_from_layer1_scoped_query(self, tmp_path):
        import duckdb
        from datetime import date

        aapl_path = tmp_path / "us_stocks" / "symbol=AAPL" / "AAPL_1D_silver.parquet"
        vix_path = tmp_path / "context" / "symbol=VIX" / "VIX_1D_silver.parquet"
        aapl_path.parent.mkdir(parents=True)
        vix_path.parent.mkdir(parents=True)

        pl.DataFrame({
            "symbol": ["AAPL"], "timestamp": [date(2026, 6, 1)],
        }).write_parquet(aapl_path)
        pl.DataFrame({
            "symbol": ["VIX"], "timestamp": [date(2026, 6, 20)],  # fresher
        }).write_parquet(vix_path)

        globs = layer1_globs(tmp_path, "*_1D_silver.parquet")
        con = duckdb.connect()
        result = con.execute(
            "SELECT MAX(CAST(timestamp AS DATE)) AS latest, "
            "COUNT(DISTINCT symbol) AS n FROM read_parquet($globs, hive_partitioning=true)",
            {"globs": globs},
        ).fetchone()
        # Must see ONLY AAPL's date, not VIX's fresher one — this is the
        # exact defect that let a fresh Layer 2 anchor mask Layer 1 staleness.
        assert result[0] == date(2026, 6, 1)
        assert result[1] == 1


class TestLayer1PeerGroups:
    """ADD GAP-PEER-01 — see src/utils/gap_analysis.py for the consumer
    (quality_validator.py's _check_gap_detection()) and rationale."""

    def test_commodity_members_share_one_peer_group(self):
        """Empirically confirmed universe: commodity = {AU, AG, CL}. CL is
        excluded here deliberately — see test_colliding_symbol_is_unclassified
        below; AU and AG are the clean case."""
        peer_map, sizes = layer1_peer_groups()
        assert peer_map["AU"] == "commodity"
        assert peer_map["AG"] == "commodity"
        assert sizes["commodity"] == 3

    def test_colliding_symbol_is_unclassified_not_silently_assigned(self):
        """'CL' is a REAL, confirmed cross-market collision in the live
        universe: WTI crude oil (commodity) AND Colgate-Palmolive
        (us_stocks) both use ticker 'CL'. A bare SQL `symbol` string
        cannot trace back to which market's file it came from, so any
        single-market assignment would be a guess dressed up as a fact.
        Must be None (gap_analysis.py treats that as "never suppress"),
        not silently pinned to whichever market layer1_markets() happens
        to iterate last."""
        peer_map, _ = layer1_peer_groups()
        assert peer_map["CL"] is None

    def test_every_layer1_market_present_in_sizes(self):
        peer_map, sizes = layer1_peer_groups()
        assert set(sizes.keys()) == set(layer1_markets())

    def test_peer_map_covers_every_layer1_symbol(self):
        from src.config.instrument_loader import get_loader
        peer_map, _ = layer1_peer_groups()
        assert set(peer_map.keys()) == {i.symbol for i in get_loader().all_symbols()}

    def test_sizes_match_by_market_counts(self):
        from src.config.instrument_loader import get_loader
        loader = get_loader()
        _, sizes = layer1_peer_groups()
        for market in layer1_markets():
            assert sizes[market] == len(loader.by_market(market))

    def test_symbols_from_different_markets_are_different_groups(self):
        peer_map, _ = layer1_peer_groups()
        assert peer_map["AU"] != peer_map["EUR_USD"]


class TestContextPeerGroups:
    """ADD GAP-PEER-01 — grouped by context_category (finer than
    context_group), see context_peer_groups() docstring for why."""

    def test_asia_pacific_equity_indices_share_one_peer_group(self):
        """Empirically confirmed universe: context_equity_em = {HSI, JKSE,
        KOSPI, SSEC, TWSE} — exactly the 5-symbol cluster the live
        diagnostic (8 Sep 2026) found responsible for 69 of the 87
        reported context_gap_detection occurrences."""
        peer_map, sizes = context_peer_groups()
        em_symbols = {"HSI", "JKSE", "KOSPI", "SSEC", "TWSE"}
        assert {peer_map[s] for s in em_symbols} == {"context_equity_em"}
        assert sizes["context_equity_em"] == 5

    def test_dm_and_em_equity_are_different_groups(self):
        """The whole point of using context_category over the coarser
        context_group='equity': DAX (DM) must never be pooled with
        TWSE (EM) — they don't share a holiday calendar."""
        peer_map, _ = context_peer_groups()
        assert peer_map["DAX"] != peer_map["TWSE"]
        assert peer_map["DAX"] == "context_equity_dm"
        assert peer_map["TWSE"] == "context_equity_em"

    def test_aluminium_grouped_with_its_real_metal_peers(self):
        peer_map, sizes = context_peer_groups()
        assert peer_map["ALUMINIUM"] == "context_commodity_metals"
        assert sizes["context_commodity_metals"] >= 4  # at least COPPER/NICKEL/ZINC/IRON_ORE too

    def test_singleton_categories_have_size_one(self):
        """DXY (context_dollar) and VIX (context_volatility) are each the
        sole member of their category — confirms the "no peers to compare
        against" path in gap_analysis.py is reachable with real data, not
        just synthetic test fixtures."""
        peer_map, sizes = context_peer_groups()
        assert sizes[peer_map["DXY"]] == 1
        assert sizes[peer_map["VIX"]] == 1

    def test_deferred_instruments_excluded(self):
        """TIN and RUBBER (context_available=False, ADR-034) must not
        appear at all — including them would only inflate a group's
        denominator with permanently-absent members."""
        peer_map, _ = context_peer_groups()
        assert "TIN" not in peer_map
        assert "RUBBER" not in peer_map

    def test_sizes_sum_matches_total_active_context_count(self):
        from src.config.instrument_loader import get_loader
        _, sizes = context_peer_groups()
        assert sum(sizes.values()) == len(get_loader().all_context(include_deferred=False))
