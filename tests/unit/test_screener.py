"""
tests/unit/test_screener.py — gold_screener real-function coverage suite.

Decision C (GMI_Decision_Document_v5.docx §3, tranche item #2 — "the actual
terminal deliverable... highest consequence of the 7 if buggy"). Prior to
this file, screener.py had only tests/unit/test_screener_gld005.py, which
covers _check_data_freshness() exclusively — build_watchlist() (the actual
multi-source join), _simplified_watchlist(), _deduplicate_by_cluster(),
_enrich_earnings(), _enrich_sentiment(), run(), and load_watchlist() had
zero coverage.

Two genuine, previously-undetected bugs were found empirically while
building these fixtures (both fixed in this same thread — see CHANGELOG):

  - FIX GLD-SCR-001: the regime join used CROSS JOIN against a subquery
    that legitimately produces zero rows whenever regime_store.parquet is
    missing or has no row for the exact run_date. A Cartesian product
    against an empty relation is empty — this silently discarded the
    ENTIRE watchlist regardless of how many valid MTF/sector/active
    candidates existed. Fixed to LEFT JOIN ... ON TRUE.
  - FIX GLD-SCR-003: _deduplicate_by_cluster() used pl.int_ranges()
    (plural — produces a List[Int64] broadcast per group) where
    pl.int_range() (singular — per-row position within group) was
    intended. The resulting cluster_rank < MAX_PER_CLUSTER comparison
    raised polars SchemaError on every real invocation, silently
    swallowed by the function's own except/pass — meaning the GD §15.1
    Correlation Concentration Guard has never actually executed.

TestBuildWatchlistRegimeJoinRegression and TestClusterDeduplication are the
permanent regression guards for these two fixes.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pytest

import src.gold.screener as scr_mod
from src.gold.screener import (
    _check_data_freshness,
    _deduplicate_by_cluster,
    _enrich_earnings,
    _enrich_sentiment,
    _simplified_watchlist,
    build_watchlist,
    load_watchlist,
    run,
)


# ── Fixture helpers ────────────────────────────────────────────────────────

def _mtf_row(
    symbol: str,
    mtf_score: int,
    signal_quality: str,
    entry_low: float = 95.0,
    entry_high: float = 105.0,
    stop: float = 90.0,
    rrr: float = 1.2,
    last_close: float = 100.0,
    regime_compatible: bool = True,
) -> dict:
    return {
        "symbol": symbol,
        "mtf_score": mtf_score,
        "signal_quality": signal_quality,
        "regime_compatible": regime_compatible,
        "entry_zone_low": entry_low,
        "entry_zone_high": entry_high,
        "stop_zone_1H": stop,
        "reward_risk_ratio": rrr,
        "last_close": last_close,
    }


def _write_mtf(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def _mtf_path(mtf_dir, run_date: date):
    return mtf_dir / f"mtf_alignment_{run_date.isoformat()}.parquet"


def _active_symbols_path(run_date: date):
    path = scr_mod.SILVER_ACTIVE_SYMBOLS_ROOT / f"active_{run_date.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _patch_all_optional_sources(tmp_path, monkeypatch):
    """Redirect every optional-source path constant to an isolated tmp_path
    location that does not exist yet (has_* all False by default)."""
    monkeypatch.setattr(scr_mod, "GOLD_MTF_PATH", tmp_path / "mtf")
    monkeypatch.setattr(scr_mod, "GOLD_REGIME_PATH", tmp_path / "regime_store.parquet")
    monkeypatch.setattr(scr_mod, "GOLD_SECTOR_PATH", tmp_path / "sector.parquet")
    monkeypatch.setattr(scr_mod, "GOLD_CORR_PATH", tmp_path / "corr.parquet")
    monkeypatch.setattr(scr_mod, "SILVER_ACTIVE_SYMBOLS_ROOT", tmp_path / "active_symbols")
    monkeypatch.setattr(scr_mod, "SILVER_SENTIMENT_ROOT", tmp_path / "sentiment")
    monkeypatch.setattr(scr_mod, "GOLD_SCREENER_PATH", tmp_path / "screener")
    return tmp_path / "mtf"


# ── build_watchlist(): no MTF input ─────────────────────────────────────────

class TestBuildWatchlistNoMtf:

    def test_no_mtf_file_returns_empty(self, tmp_path, monkeypatch):
        _patch_all_optional_sources(tmp_path, monkeypatch)
        result = build_watchlist(date(2026, 6, 1))
        assert result.is_empty()


# ── FIX GLD-SCR-001 regression ──────────────────────────────────────────────

class TestBuildWatchlistRegimeJoinRegression:
    """Regime data being unavailable must degrade gracefully (regime
    columns null), never wipe out an otherwise-valid watchlist. See
    FIX GLD-SCR-001 in the module docstring above."""

    def test_missing_regime_file_still_returns_candidates(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        # GOLD_REGIME_PATH deliberately left non-existent.
        result = build_watchlist(run_date)
        assert not result.is_empty()
        assert result["symbol"].to_list() == ["AAPL"]
        assert result["regime"].to_list() == [None]

    def test_regime_file_exists_but_no_row_for_run_date(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        pl.DataFrame({
            "date": [date(2026, 5, 20)], "regime": ["RISK_ON"],
            "composite_score": [0.5], "confidence": [0.8],
            "regime_transition": [False], "transition_alert": [None],
        }).write_parquet(scr_mod.GOLD_REGIME_PATH)
        result = build_watchlist(run_date)
        assert not result.is_empty(), (
            "GLD-SCR-001 REGRESSION: a regime file with no row for this "
            "run_date must not zero out the watchlist"
        )
        assert result["regime"].to_list() == [None]

    def test_regime_row_present_broadcasts_to_all_candidates(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("AAPL", 7, "A"), _mtf_row("MSFT", -6, "A"),
        ])
        pl.DataFrame({
            "date": [run_date], "regime": ["RISK_OFF"],
            "composite_score": [-0.4], "confidence": [0.7],
            "regime_transition": [True], "transition_alert": ["RISK_ON -> RISK_OFF"],
        }).write_parquet(scr_mod.GOLD_REGIME_PATH)
        result = build_watchlist(run_date)
        assert result["regime"].to_list() == ["RISK_OFF", "RISK_OFF"]
        assert result["regime_transition"].to_list() == [True, True]
        assert result["transition_alert"].to_list() == ["RISK_ON -> RISK_OFF"] * 2

    def test_corrupt_regime_file_degrades_to_empty_placeholder(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.GOLD_REGIME_PATH.parent.mkdir(parents=True, exist_ok=True)
        scr_mod.GOLD_REGIME_PATH.write_text("not a parquet file")
        result = build_watchlist(run_date)
        assert not result.is_empty()
        assert result["regime"].to_list() == [None]

    def test_corrupt_sector_file_degrades_to_defaults(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.GOLD_SECTOR_PATH.parent.mkdir(parents=True, exist_ok=True)
        scr_mod.GOLD_SECTOR_PATH.write_text("not a parquet file")
        result = build_watchlist(run_date)
        assert result.row(0, named=True)["sector"] == "Unknown"

    def test_corrupt_active_symbols_file_degrades_to_defaults(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        path = _active_symbols_path(run_date)
        path.write_text("not a parquet file")
        result = build_watchlist(run_date)
        assert result.row(0, named=True)["dollar_volume_20d"] == 0


# ── Filtering ────────────────────────────────────────────────────────────

class TestBuildWatchlistFiltering:

    def test_low_grade_signal_excluded(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("AAPL", 7, "A"),     # qualifies
            _mtf_row("TSLA", 4, "C"),     # |4| < MIN_MTF_SCORE=5 -> excluded
            _mtf_row("NFLX", -5, "D"),    # grade D excluded regardless of score
        ])
        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["AAPL"]

    def test_negative_score_grade_b_qualifies(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("MSFT", -5, "B")])
        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["MSFT"]

    def test_sector_weight_at_or_below_threshold_excluded(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("AAPL", 7, "A"), _mtf_row("XOM", 7, "A"),
        ])
        pl.DataFrame({
            "symbol": ["AAPL", "XOM"],
            "sector": ["Technology", "Energy"],
            "sector_weight_adj": [1.2, 0.5],   # XOM exactly at threshold -> excluded (strict >)
        }).write_parquet(scr_mod.GOLD_SECTOR_PATH)
        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["AAPL"]

    def test_dollar_volume_below_threshold_excluded_when_known(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("AAPL", 7, "A"), _mtf_row("PENY", 7, "A"),
        ])
        pl.DataFrame({
            "symbol": ["AAPL", "PENY"],
            "dollar_volume_20d": [50_000_000.0, 100.0],  # PENY far below MIN_DOLLAR_VOLUME
        }).write_parquet(_active_symbols_path(run_date))
        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["AAPL"]

    def test_unknown_dollar_volume_defaults_permissive(self, tmp_path, monkeypatch):
        """A symbol absent from active_symbols (no dollar_volume_20d known)
        gets the COALESCE(..., 1e9) default and passes the filter — an
        intentional 'assume fine if unknown' degrade, not a bug."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("NEWSYM", 7, "A")])
        # No active_symbols file at all for this run_date.
        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["NEWSYM"]
        assert result["dollar_volume_20d"].to_list() == [0]   # COALESCE default in SELECT

    def test_no_qualifying_candidates_returns_empty(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("WEAK1", 3, "D"), _mtf_row("WEAK2", -2, "D"),
        ])
        result = build_watchlist(run_date)
        assert result.is_empty()


class TestCheckDataFreshnessCorruptData:

    def test_non_runtime_exception_during_query_logs_and_returns(self, tmp_path, monkeypatch):
        """A genuine query/IO failure (not the RuntimeError the gate itself
        raises on low coverage) must be caught and logged, not propagate —
        this is the 'Silver directory exists but is somehow unreadable'
        branch, distinct from the pre-check 'no Layer 1 directory at all
        yet' early return covered in test_screener_gld005.py. Must use a
        non-RuntimeError type: the gate's own except RuntimeError: raise
        clause re-raises RuntimeError deliberately (that's its own
        low-coverage signal, not something to swallow)."""
        with patch("src.gold.screener.layer1_globs", return_value=["bogus/**/glob.parquet"]), \
             patch("duckdb.connect") as mock_con:
            mock_con.return_value.execute.side_effect = OSError("duckdb IO error")
            result = _check_data_freshness(date(2026, 6, 1))
        assert result is None   # must not raise


# ── Enrichment (sector / active / sentiment / earnings via full join) ──────

class TestBuildWatchlistEnrichment:

    def test_sector_and_dollar_volume_enriched(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        pl.DataFrame({
            "symbol": ["AAPL"], "sector": ["Technology"], "sector_weight_adj": [1.3],
        }).write_parquet(scr_mod.GOLD_SECTOR_PATH)
        pl.DataFrame({
            "symbol": ["AAPL"], "dollar_volume_20d": [80_000_000.0],
        }).write_parquet(_active_symbols_path(run_date))
        result = build_watchlist(run_date)
        row = result.row(0, named=True)
        assert row["sector"] == "Technology"
        assert row["sector_weight_adj"] == pytest.approx(1.3)
        assert row["dollar_volume_20d"] == pytest.approx(80_000_000.0)

    def test_missing_sector_defaults_unknown_and_neutral_weight(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        result = build_watchlist(run_date)
        row = result.row(0, named=True)
        assert row["sector"] == "Unknown"
        assert row["sector_weight_adj"] == pytest.approx(1.0)

    def test_top_n_and_ordering(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        rows = [_mtf_row(f"SYM{i}", 7 - (i % 2), "A" if i % 2 == 0 else "B")
                for i in range(25)]
        # Give each a distinct score so ORDER BY ABS(mtf_score) DESC is
        # unambiguous: descending scores 30, 29, ..., 6.
        rows = [_mtf_row(f"SYM{i}", 30 - i, "A") for i in range(25)]
        _write_mtf(_mtf_path(mtf_dir, run_date), rows)
        result = build_watchlist(run_date)
        assert len(result) == scr_mod.TOP_N_WATCHLIST
        assert result["mtf_score"].to_list() == sorted(
            result["mtf_score"].to_list(), reverse=True
        )
        assert result["symbol"].to_list()[0] == "SYM0"   # highest score (30)


# ── FIX GLD-SCR-003 regression (correlation cluster dedup) ──────────────────

class TestClusterDeduplication:
    """See FIX GLD-SCR-003 in the module docstring above."""

    def test_direct_call_enforces_max_per_cluster(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scr_mod, "GOLD_CORR_PATH", tmp_path / "corr.parquet")
        pl.DataFrame({
            "symbol": ["A", "B", "C", "D", "E"],
            "cluster_id": [1, 1, 1, 2, 2],
        }).write_parquet(scr_mod.GOLD_CORR_PATH)
        # Pre-ordered exactly as build_watchlist's ORDER BY would deliver —
        # highest-priority candidate per cluster listed first.
        df = pl.DataFrame({"symbol": ["A", "B", "C", "D", "E"], "mtf_score": [7, 6, 5, 7, 6]})
        con = duckdb.connect()
        result = _deduplicate_by_cluster(df, date(2026, 6, 1), con)
        assert not result.is_empty(), (
            "GLD-SCR-003 REGRESSION: dedup must not raise/no-op on real "
            "correlation data — it must actually filter"
        )
        counts = result["symbol"].to_list()
        assert set(counts) == {"A", "B", "D", "E"}   # C (3rd in its cluster) dropped
        assert "C" not in counts

    def test_no_correlation_file_leaves_candidates_untouched(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scr_mod, "GOLD_CORR_PATH", tmp_path / "nonexistent.parquet")
        df = pl.DataFrame({"symbol": ["A", "B", "C"], "mtf_score": [7, 6, 5]})
        con = duckdb.connect()
        result = _deduplicate_by_cluster(df, date(2026, 6, 1), con)
        assert result["symbol"].to_list() == ["A", "B", "C"]

    def test_full_build_watchlist_applies_dedup_when_corr_present(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("A", 9, "A"), _mtf_row("B", 8, "A"), _mtf_row("C", 7, "A"),
        ])
        pl.DataFrame({
            "symbol": ["A", "B", "C"], "cluster_id": [1, 1, 1],
        }).write_parquet(scr_mod.GOLD_CORR_PATH)
        result = build_watchlist(run_date)
        assert len(result) == scr_mod.MAX_PER_CLUSTER
        assert set(result["symbol"].to_list()) == {"A", "B"}   # top 2 by score kept


# ── Main-query failure -> simplified fallback ───────────────────────────────

class TestBuildWatchlistFallback:

    def test_main_query_failure_falls_back_to_simplified(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])

        original_execute = duckdb.DuckDBPyConnection.execute

        def flaky_execute(self_conn, query, *args, **kwargs):
            if "FROM mtf m" in query:
                raise RuntimeError("simulated DuckDB failure")
            return original_execute(self_conn, query, *args, **kwargs)

        with patch.object(duckdb.DuckDBPyConnection, "execute", flaky_execute):
            result = build_watchlist(run_date)

        assert not result.is_empty()
        assert result["symbol"].to_list() == ["AAPL"]


class TestSimplifiedWatchlistDirect:

    def test_filters_and_sorts_by_score(self, tmp_path):
        path = tmp_path / "mtf.parquet"
        pl.DataFrame([
            _mtf_row("LOW", 5, "B"), _mtf_row("HIGH", 7, "A"), _mtf_row("SKIP", 3, "D"),
        ]).write_parquet(path)
        result = _simplified_watchlist(path, date(2026, 6, 1))
        assert result["symbol"].to_list() == ["HIGH", "LOW"]
        assert "watchlist_date" in result.columns

    def test_corrupt_file_returns_empty(self, tmp_path):
        path = tmp_path / "mtf.parquet"
        path.write_text("not a parquet file")
        result = _simplified_watchlist(path, date(2026, 6, 1))
        assert result.is_empty()


# ── Earnings enrichment ──────────────────────────────────────────────────────

class TestEnrichEarnings:

    def test_populates_days_to_earnings_and_near_flag(self):
        df = pl.DataFrame({"symbol": ["AAPL", "MSFT"], "mtf_score": [7, 6]})
        upcoming = pl.DataFrame({
            "symbol": ["AAPL"], "earnings_date": ["2026-06-03"], "days_to_earnings": [2],
        })
        with patch(
            "src.silver.fundamental_processor.FundamentalProcessor.get_upcoming_earnings",
            return_value=upcoming,
        ):
            result = _enrich_earnings(df, date(2026, 6, 1))
        row = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        assert row["days_to_earnings"] == 2
        assert row["near_earnings_flag"] is True
        msft = result.filter(pl.col("symbol") == "MSFT").row(0, named=True)
        assert msft["days_to_earnings"] is None
        assert msft["near_earnings_flag"] is False

    def test_beyond_near_threshold_flag_false(self):
        df = pl.DataFrame({"symbol": ["AAPL"], "mtf_score": [7]})
        upcoming = pl.DataFrame({
            "symbol": ["AAPL"], "earnings_date": ["2026-07-01"], "days_to_earnings": [30],
        })
        with patch(
            "src.silver.fundamental_processor.FundamentalProcessor.get_upcoming_earnings",
            return_value=upcoming,
        ):
            result = _enrich_earnings(df, date(2026, 6, 1))
        row = result.row(0, named=True)
        assert row["days_to_earnings"] == 30
        assert row["near_earnings_flag"] is False

    def test_exception_leaves_df_unchanged(self):
        df = pl.DataFrame({"symbol": ["AAPL"], "mtf_score": [7]})
        with patch(
            "src.silver.fundamental_processor.FundamentalProcessor.get_upcoming_earnings",
            side_effect=RuntimeError("boom"),
        ):
            result = _enrich_earnings(df, date(2026, 6, 1))
        assert result["symbol"].to_list() == ["AAPL"]
        assert "days_to_earnings" not in result.columns


# ── Sentiment enrichment ──────────────────────────────────────────────────────

class TestEnrichSentiment:

    def test_joins_sentiment_when_file_present(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scr_mod, "SILVER_SENTIMENT_ROOT", tmp_path / "sentiment")
        run_date = date(2026, 6, 1)
        path = tmp_path / "sentiment" / f"date={run_date.isoformat()}" / "sentiment_silver.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": ["AAPL"], "sentiment_score": [0.6], "buzz_score": [1.2],
        }).write_parquet(path)
        df = pl.DataFrame({"symbol": ["AAPL"], "mtf_score": [7]})
        result = _enrich_sentiment(df, run_date)
        row = result.row(0, named=True)
        assert row["sentiment_score"] == pytest.approx(0.6)
        assert row["buzz_score"] == pytest.approx(1.2)

    def test_missing_file_returns_unchanged(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scr_mod, "SILVER_SENTIMENT_ROOT", tmp_path / "sentiment")
        df = pl.DataFrame({"symbol": ["AAPL"], "mtf_score": [7]})
        result = _enrich_sentiment(df, date(2026, 6, 1))
        assert result.equals(df)

    def test_preexisting_sentiment_columns_resolved_not_duplicated(self, tmp_path, monkeypatch):
        """If df already carries sentiment_score/buzz_score (defensive:
        re-entrant call, or upstream already populated them), the join
        must resolve the name collision rather than leaving _sent-suffixed
        duplicates behind."""
        monkeypatch.setattr(scr_mod, "SILVER_SENTIMENT_ROOT", tmp_path / "sentiment")
        run_date = date(2026, 6, 1)
        path = tmp_path / "sentiment" / f"date={run_date.isoformat()}" / "sentiment_silver.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": ["AAPL"], "sentiment_score": [0.9], "buzz_score": [2.0],
        }).write_parquet(path)
        df = pl.DataFrame({
            "symbol": ["AAPL"], "sentiment_score": [None], "buzz_score": [None],
        })
        result = _enrich_sentiment(df, run_date)
        assert "sentiment_score_sent" not in result.columns
        assert result.row(0, named=True)["sentiment_score"] == pytest.approx(0.9)

    def test_corrupt_sentiment_file_exception_leaves_df_unchanged(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scr_mod, "SILVER_SENTIMENT_ROOT", tmp_path / "sentiment")
        run_date = date(2026, 6, 1)
        path = tmp_path / "sentiment" / f"date={run_date.isoformat()}" / "sentiment_silver.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not a parquet file")
        df = pl.DataFrame({"symbol": ["AAPL"], "mtf_score": [7]})
        result = _enrich_sentiment(df, run_date)
        assert result["symbol"].to_list() == ["AAPL"]
        assert "sentiment_score" not in result.columns


# ── run() integration ────────────────────────────────────────────────────────

class TestRunIntegration:

    def test_happy_path_writes_watchlist(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        # No Silver market_ohlcv dir at all -> freshness gate skips gracefully
        # (layer1_globs() returns [] for a fresh/pre-backfill tree).
        monkeypatch.setattr(scr_mod, "SILVER_OHLCV_ROOT", tmp_path / "silver_ohlcv_missing")
        run(run_date)
        out = scr_mod.GOLD_SCREENER_PATH / f"watchlist_{run_date.isoformat()}.parquet"
        assert out.exists()
        written = pl.read_parquet(out)
        assert written["symbol"].to_list() == ["AAPL"]

    def test_no_candidates_logs_warning_and_writes_nothing(self, tmp_path, monkeypatch):
        _patch_all_optional_sources(tmp_path, monkeypatch)
        monkeypatch.setattr(scr_mod, "SILVER_OHLCV_ROOT", tmp_path / "silver_ohlcv_missing")
        run_date = date(2026, 6, 1)
        # No MTF file at all -> build_watchlist() returns empty.
        run(run_date)
        out = scr_mod.GOLD_SCREENER_PATH / f"watchlist_{run_date.isoformat()}.parquet"
        assert not out.exists()


class TestLoadWatchlist:

    def test_loads_existing_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scr_mod, "GOLD_SCREENER_PATH", tmp_path)
        run_date = date(2026, 6, 1)
        pl.DataFrame({"symbol": ["AAPL"]}).write_parquet(
            tmp_path / f"watchlist_{run_date.isoformat()}.parquet"
        )
        result = load_watchlist(run_date)
        assert result["symbol"].to_list() == ["AAPL"]

    def test_missing_file_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scr_mod, "GOLD_SCREENER_PATH", tmp_path)
        with pytest.raises(FileNotFoundError):
            load_watchlist(date(2099, 1, 1))
