"""
tests/unit/test_screener.py — gold_screener real-function coverage suite.

Decision C (GMI_Decision_Document_v5.docx §3, tranche item #2 — "the actual
terminal deliverable... highest consequence of the 7 if buggy"). Prior to
this file, screener.py had only tests/unit/test_screener_gld005.py, which
covers _check_data_freshness() exclusively — build_watchlist() (the actual
multi-source join), _simplified_watchlist(), _deduplicate_by_cluster(),
run(), and load_watchlist() had zero coverage.

FIX ADR-043/ADR-044 (GMI_Decision_Document_v10.docx): TestEnrichEarnings and
TestEnrichSentiment (direct-unit tests for _enrich_earnings()/_enrich_sentiment())
were removed from this file along with the functions themselves — Finnhub was
retired in full. TestRunIntegration gained a schema-stability assertion in
their place, confirming the watchlist's 5 Finnhub-derived output columns
remain present and permanently null via the main query's own placeholders.

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

ADR-055 v2 (GMI-HMM2-CUT-001): the regime source is now the dual-HMM context_regime store
(as-of join, staleness-bounded) and the sector weights are gone; the regime-join regression
class below was rewritten for the new source with the SAME degrade-never-zero-the-watchlist
contract (GLD-SCR-001 still applies to a missing/stale/corrupt context store).
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
) -> dict:
    return {
        "symbol": symbol,
        "mtf_score": mtf_score,
        "signal_quality": signal_quality,
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
    monkeypatch.setattr(scr_mod, "GOLD_CONTEXT_REGIME_PATH", tmp_path / "hmm" / "context_regime.parquet")
    monkeypatch.setattr(scr_mod, "CROSS_ASSET_CORR_PATH", tmp_path / "cross_asset" / "cross_asset_corr.parquet")
    monkeypatch.setattr(scr_mod, "SILVER_ACTIVE_SYMBOLS_ROOT", tmp_path / "active_symbols")
    monkeypatch.setattr(scr_mod, "GOLD_SCREENER_PATH", tmp_path / "screener")
    # ADD GMI Wave 1 Cycle 4 — CrossAssetEngine optional sources.
    monkeypatch.setattr(scr_mod, "GLOBAL_REGIME_PATH", tmp_path / "cross_asset" / "global_regime.parquet")
    monkeypatch.setattr(scr_mod, "LEAD_LAG_PATH", tmp_path / "cross_asset" / "lead_lag_matrix.parquet")
    monkeypatch.setattr(scr_mod, "CROSS_ASSET_FORECAST_PATH", tmp_path / "cross_asset" / "cross_asset_forecast.parquet")
    # ADD GMI-SIGAGG-001 — optional source, directory of per-date files.
    monkeypatch.setattr(scr_mod, "SIGNAL_AGG_PATH", tmp_path / "signal_aggregation")
    return tmp_path / "mtf"


# ── build_watchlist(): no MTF input ─────────────────────────────────────────

class TestBuildWatchlistNoMtf:

    def test_no_mtf_file_returns_empty(self, tmp_path, monkeypatch):
        _patch_all_optional_sources(tmp_path, monkeypatch)
        result = build_watchlist(date(2026, 6, 1))
        assert result.is_empty()


# ── FIX GLD-SCR-001 regression ──────────────────────────────────────────────

def _context_row(day: date, **over) -> dict:
    row = {
        "date": day, "macro_label": "expansion", "macro_activity": 0.4, "macro_momentum": 0.1,
        "market_label": "risk_on", "market_probability": 0.8, "market_score": 0.6,
        "market_age_days": 0, "context_label": "expansion/risk_on",
        "context_transition": False, "context_transition_alert": None, "in_sample": False,
    }
    row.update(over)
    return row


def _write_context(rows: list[dict]) -> None:
    scr_mod.GOLD_CONTEXT_REGIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(scr_mod.GOLD_CONTEXT_REGIME_PATH)


REGIME_OUTPUT_COLUMNS = [
    "context_date", "context_age_days", "macro_label", "macro_activity", "macro_momentum",
    "market_label", "market_probability", "market_score", "market_age_days", "context_label",
    "context_transition", "context_transition_alert", "regime_in_sample",
]
LEGACY_OUTPUT_COLUMNS = [
    "regime", "regime_composite", "regime_confidence", "regime_transition", "transition_alert",
    "sector_weight_adj", "sector",
]


class TestBuildWatchlistRegimeJoinRegression:
    """Context-regime data being unavailable must degrade gracefully (regime columns null),
    never wipe out an otherwise-valid watchlist. See FIX GLD-SCR-001 in the module docstring
    above — the contract now applies to the dual-HMM context_regime store (ADR-055 v2)."""

    def test_missing_regime_file_still_returns_candidates(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        # GOLD_CONTEXT_REGIME_PATH deliberately left non-existent.
        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["AAPL"]
        assert result["context_label"].to_list() == [None]
        assert result["context_date"].to_list() == [None]

    def test_output_has_context_columns_and_no_legacy_columns(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        result = build_watchlist(run_date)
        for col in REGIME_OUTPUT_COLUMNS:
            assert col in result.columns, f"{col} missing from the watchlist"
        for col in LEGACY_OUTPUT_COLUMNS:
            assert col not in result.columns, f"retired column {col} is still in the watchlist"

    def test_context_row_present_broadcasts_to_all_candidates(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("AAPL", 7, "A"), _mtf_row("MSFT", -6, "A"),
        ])
        _write_context([_context_row(
            run_date, macro_label="slowdown", market_label="risk_off",
            context_label="slowdown/risk_off", context_transition=True,
            context_transition_alert="expansion/risk_on -> slowdown/risk_off", in_sample=True,
        )])
        result = build_watchlist(run_date)
        assert result["context_label"].to_list() == ["slowdown/risk_off"] * 2
        assert result["macro_label"].to_list() == ["slowdown"] * 2
        assert result["market_label"].to_list() == ["risk_off"] * 2
        assert result["context_transition"].to_list() == [True, True]
        assert result["context_transition_alert"].to_list() == ["expansion/risk_on -> slowdown/risk_off"] * 2
        assert result["regime_in_sample"].to_list() == [True, True]
        assert result["context_age_days"].to_list() == [0, 0]

    def test_asof_picks_latest_row_on_or_before_run_date_within_tolerance(self, tmp_path, monkeypatch):
        """run_date is a Sunday: the Friday row (2 days old) is used, a LATER row is never read
        (point-in-time safe) and the age is reported."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 10, 18)                     # Sunday
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        _write_context([
            _context_row(date(2026, 10, 15), context_label="expansion/mixed"),
            _context_row(date(2026, 10, 16), context_label="expansion/risk_on"),   # Friday
            _context_row(date(2026, 10, 19), context_label="recession/risk_off"),  # FUTURE
        ])
        result = build_watchlist(run_date)
        assert result["context_label"].to_list() == ["expansion/risk_on"]
        assert result["context_date"].to_list() == [date(2026, 10, 16)]
        assert result["context_age_days"].to_list() == [2]

    def test_stale_context_row_beyond_tolerance_degrades_to_null(self, tmp_path, monkeypatch):
        """A month-old state must not be presented as the current regime (never zero rows either)."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        _write_context([_context_row(date(2026, 5, 1))])   # 31 days old
        result = build_watchlist(run_date)
        assert not result.is_empty(), (
            "GLD-SCR-001 REGRESSION: a stale context_regime store must not zero out the watchlist"
        )
        assert result["context_label"].to_list() == [None]

    def test_only_future_rows_degrades_to_null(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        _write_context([_context_row(date(2026, 6, 2))])
        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["AAPL"]
        assert result["context_label"].to_list() == [None]

    def test_corrupt_regime_file_degrades_to_empty_placeholder(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.GOLD_CONTEXT_REGIME_PATH.parent.mkdir(parents=True, exist_ok=True)
        scr_mod.GOLD_CONTEXT_REGIME_PATH.write_text("not a parquet file")
        result = build_watchlist(run_date)
        assert not result.is_empty()
        assert result["context_label"].to_list() == [None]

    def test_store_missing_a_required_column_degrades_loudly_not_silently(self, tmp_path, monkeypatch):
        """A schema problem (e.g. a store from an older engine version) must still degrade to NULL
        but be logged as a warning — it must not look like 'no file yet'."""
        from loguru import logger
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        _write_context([{"date": run_date, "context_label": "expansion/risk_on"}])   # columns missing
        messages: list[str] = []
        sink = logger.add(lambda m: messages.append(str(m)), level="WARNING")
        try:
            result = build_watchlist(run_date)
        finally:
            logger.remove(sink)
        assert result["context_label"].to_list() == [None]
        assert any("context_regime unreadable" in m for m in messages)

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
            _mtf_row("AAPL", 5, "A"),     # qualifies
            _mtf_row("TSLA", 2, "C"),     # |2| < MIN_MTF_SCORE=3 -> excluded
            _mtf_row("NFLX", -4, "C"),    # grade C excluded regardless of score
        ])
        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["AAPL"]

    def test_negative_score_grade_b_qualifies(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("MSFT", -3, "B")])
        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["MSFT"]

    def test_sector_is_neither_a_filter_nor_an_output_column(self, tmp_path, monkeypatch):
        """ADR-055 v2: the sector concentration guard (sector_weight_adj <= 0.5 excluded) was
        retired with the regime-keyed weights, and the `sector` column was dropped with gold_sector
        (Ovi, 21 Oct 2026). Energy used to be excluded in some regimes — it must pass like any
        other candidate, and no sector label may appear in the watchlist."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("AAPL", 7, "A"), _mtf_row("XOM", 7, "A"),
        ])
        result = build_watchlist(run_date)
        assert sorted(result["symbol"].to_list()) == ["AAPL", "XOM"]
        assert "sector" not in result.columns

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
            _mtf_row("WEAK1", 2, "C"), _mtf_row("WEAK2", -2, "C"),
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

    def test_dollar_volume_enriched(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        pl.DataFrame({
            "symbol": ["AAPL"], "dollar_volume_20d": [80_000_000.0],
        }).write_parquet(_active_symbols_path(run_date))
        result = build_watchlist(run_date)
        row = result.row(0, named=True)
        assert row["dollar_volume_20d"] == pytest.approx(80_000_000.0)

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


# ── ADD GMI Wave 1 Cycle 4 — CrossAssetEngine integration ────────────────────

class TestCrossAssetEngineIntegration:
    """global_regime_tbl / lead_lag_tbl / forecast_tbl — same graceful-
    degrade contract as sector_tbl/active_tbl/regime_tbl above: missing or
    partial CrossAssetEngine output must never shrink the watchlist or
    raise, only leave the new columns null/False. See module docstring's
    "ADD GMI Wave 1 Cycle 4" section for the full column list."""

    def test_all_three_absent_watchlist_unaffected(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        result = build_watchlist(run_date)
        row = result.row(0, named=True)
        assert row["global_risk_score"] is None
        assert row["global_regime_label"] is None
        assert row["dm_em_divergence"] is None
        assert row["lead_lag_signal"] is False
        assert row["lead_lag_top_leader"] is None
        assert row["lead_lag_top_lag"] is None
        assert row["forecast_return_1d"] is None
        assert row["forecast_stable"] is None

    def test_global_regime_present_broadcasts_to_all_candidates(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("AAPL", 7, "A"), _mtf_row("MSFT", -6, "A"),
        ])
        scr_mod.GLOBAL_REGIME_PATH.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "date": [run_date], "global_risk_score": [71.4],
            "global_regime_label": ["RISK_ON"], "dm_em_divergence": [12.5],
        }).write_parquet(scr_mod.GLOBAL_REGIME_PATH)
        result = build_watchlist(run_date)
        assert result["global_risk_score"].to_list() == [pytest.approx(71.4)] * 2
        assert result["global_regime_label"].to_list() == ["RISK_ON", "RISK_ON"]
        assert result["dm_em_divergence"].to_list() == [pytest.approx(12.5)] * 2

    def test_global_regime_file_exists_but_no_row_for_run_date(self, tmp_path, monkeypatch):
        """Same GLD-SCR-001-class hazard as regime_tbl: a global_regime
        store with no row for this exact run_date must not zero the
        watchlist — LEFT JOIN ... ON TRUE, not CROSS JOIN."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.GLOBAL_REGIME_PATH.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "date": [date(2026, 5, 20)], "global_risk_score": [50.0],
            "global_regime_label": ["MIXED"], "dm_em_divergence": [0.0],
        }).write_parquet(scr_mod.GLOBAL_REGIME_PATH)
        result = build_watchlist(run_date)
        assert not result.is_empty(), (
            "a global_regime file with no row for this run_date must not "
            "zero out the watchlist"
        )
        assert result["global_risk_score"].to_list() == [None]

    def test_corrupt_global_regime_file_degrades_to_null(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.GLOBAL_REGIME_PATH.parent.mkdir(parents=True, exist_ok=True)
        scr_mod.GLOBAL_REGIME_PATH.write_text("not a parquet file")
        result = build_watchlist(run_date)
        assert not result.is_empty()
        assert result["global_risk_score"].to_list() == [None]

    def test_lead_lag_significant_relationship_flagged(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.LEAD_LAG_PATH.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "leader":            ["DXY", "SPX"],
            "follower":          ["AAPL", "AAPL"],
            "optimal_lag":       [2, 1],
            "p_value_raw":       [0.001, 0.02],
            "p_value_adjusted":  [0.01, 0.028],
            "r_squared":         [0.4, 0.2],
            "bh_significant":    [True, True],
            "max_cross_corr":    [0.5, 0.3],
        }).write_parquet(scr_mod.LEAD_LAG_PATH)
        result = build_watchlist(run_date)
        row = result.row(0, named=True)
        assert row["lead_lag_signal"] is True
        # DXY has the smaller p_value_adjusted (0.01 < 0.028) -> "top".
        assert row["lead_lag_top_leader"] == "DXY"
        assert row["lead_lag_top_lag"] == 2

    def test_lead_lag_no_significant_relationship_not_flagged(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.LEAD_LAG_PATH.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "leader": ["DXY"], "follower": ["AAPL"], "optimal_lag": [3],
            "p_value_raw": [0.4], "p_value_adjusted": [0.6], "r_squared": [0.01],
            "bh_significant": [False], "max_cross_corr": [0.05],
        }).write_parquet(scr_mod.LEAD_LAG_PATH)
        result = build_watchlist(run_date)
        row = result.row(0, named=True)
        assert row["lead_lag_signal"] is False
        assert row["lead_lag_top_leader"] is None

    def test_corrupt_lead_lag_file_degrades_to_not_flagged(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.LEAD_LAG_PATH.parent.mkdir(parents=True, exist_ok=True)
        scr_mod.LEAD_LAG_PATH.write_text("not a parquet file")
        result = build_watchlist(run_date)
        assert not result.is_empty()
        assert result.row(0, named=True)["lead_lag_signal"] is False

    def test_forecast_horizon_one_populates_columns(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.CROSS_ASSET_FORECAST_PATH.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol":       ["AAPL", "AAPL"],
            "horizon_days": [1, 2],
            "forecast_return": [0.0123, 0.0200],
            "stable":       [True, True],
        }).write_parquet(scr_mod.CROSS_ASSET_FORECAST_PATH)
        result = build_watchlist(run_date)
        row = result.row(0, named=True)
        assert row["forecast_return_1d"] == pytest.approx(0.0123)
        assert row["forecast_stable"] is True

    def test_forecast_missing_horizon_one_row_degrades_to_null(self, tmp_path, monkeypatch):
        """A symbol with only horizon=2..5 rows (no horizon=1) must not
        error and must simply leave forecast columns null for it."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.CROSS_ASSET_FORECAST_PATH.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": ["AAPL"], "horizon_days": [2],
            "forecast_return": [0.02], "stable": [False],
        }).write_parquet(scr_mod.CROSS_ASSET_FORECAST_PATH)
        result = build_watchlist(run_date)
        assert not result.is_empty()
        assert result.row(0, named=True)["forecast_return_1d"] is None

    def test_corrupt_forecast_file_degrades_to_null(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.CROSS_ASSET_FORECAST_PATH.parent.mkdir(parents=True, exist_ok=True)
        scr_mod.CROSS_ASSET_FORECAST_PATH.write_text("not a parquet file")
        result = build_watchlist(run_date)
        assert not result.is_empty()
        assert result.row(0, named=True)["forecast_return_1d"] is None

    def test_all_three_present_together_no_row_count_change(self, tmp_path, monkeypatch):
        """All three CrossAssetEngine sources present simultaneously must
        not change the candidate count or MTF-driven ordering — purely
        additive columns, never a filter."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("AAPL", 7, "A"), _mtf_row("MSFT", 6, "A"),
        ])
        scr_mod.GLOBAL_REGIME_PATH.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "date": [run_date], "global_risk_score": [64.3],
            "global_regime_label": ["RISK_ON"], "dm_em_divergence": [8.0],
        }).write_parquet(scr_mod.GLOBAL_REGIME_PATH)
        pl.DataFrame({
            "leader": ["DXY"], "follower": ["AAPL"], "optimal_lag": [1],
            "p_value_raw": [0.001], "p_value_adjusted": [0.01], "r_squared": [0.3],
            "bh_significant": [True], "max_cross_corr": [0.4],
        }).write_parquet(scr_mod.LEAD_LAG_PATH)
        pl.DataFrame({
            "symbol": ["AAPL", "MSFT"], "horizon_days": [1, 1],
            "forecast_return": [0.01, -0.005], "stable": [True, False],
        }).write_parquet(scr_mod.CROSS_ASSET_FORECAST_PATH)

        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["AAPL", "MSFT"]
        assert result["mtf_score"].to_list() == [7, 6]
        aapl = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        msft = result.filter(pl.col("symbol") == "MSFT").row(0, named=True)
        assert aapl["lead_lag_signal"] is True
        assert msft["lead_lag_signal"] is False
        assert aapl["forecast_stable"] is True
        assert msft["forecast_stable"] is False


class TestSignalAggregationIntegration:
    """signal_agg_tbl — same graceful-degrade contract as the three
    CrossAssetEngine sources above (ADD GMI-SIGAGG-001, see module
    docstring). Per-symbol LEFT JOIN (not a broadcast-single-row LEFT
    JOIN ... ON TRUE like regime_tbl/global_regime_tbl), same shape as
    forecast_tbl."""

    def test_absent_degrades_to_null_not_dropped(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        result = build_watchlist(run_date)
        assert not result.is_empty()
        row = result.row(0, named=True)
        assert row["composite_score"] is None
        assert row["composite_grade"] is None
        assert row["sector_breadth_pct"] is None
        assert row["sector_momentum"] is None
        assert row["breadth_divergence"] is None

    def test_present_populates_columns_per_symbol(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("AAPL", 7, "A"), _mtf_row("MSFT", -6, "A"),
        ])
        scr_mod.SIGNAL_AGG_PATH.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol":              ["AAPL", "MSFT"],
            "composite_score":     [0.62, -0.41],
            "composite_grade":     ["A", "B"],
            "sector_breadth_pct":  [80.0, 80.0],
            "sector_momentum":     [5.0, 5.0],
            "breadth_divergence":  [0.18, -0.19],
        }).write_parquet(scr_mod.SIGNAL_AGG_PATH / f"signal_aggregation_{run_date.isoformat()}.parquet")

        result = build_watchlist(run_date)
        aapl = result.filter(pl.col("symbol") == "AAPL").row(0, named=True)
        msft = result.filter(pl.col("symbol") == "MSFT").row(0, named=True)
        assert aapl["composite_score"] == pytest.approx(0.62)
        assert aapl["composite_grade"] == "A"
        assert msft["composite_score"] == pytest.approx(-0.41)
        assert msft["composite_grade"] == "B"
        assert aapl["sector_breadth_pct"] == pytest.approx(80.0)

    def test_symbol_not_in_signal_agg_output_degrades_to_null(self, tmp_path, monkeypatch):
        """A watchlist candidate absent from signal_aggregation's own
        output (e.g. not in the active_ohlcv universe that day) must not
        be dropped from the watchlist — only its new columns go null."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.SIGNAL_AGG_PATH.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": ["MSFT"], "composite_score": [0.1], "composite_grade": ["D"],
            "sector_breadth_pct": [50.0], "sector_momentum": [0.0], "breadth_divergence": [0.0],
        }).write_parquet(scr_mod.SIGNAL_AGG_PATH / f"signal_aggregation_{run_date.isoformat()}.parquet")

        result = build_watchlist(run_date)
        assert result["symbol"].to_list() == ["AAPL"]
        assert result.row(0, named=True)["composite_score"] is None

    def test_wrong_date_file_absent_degrades_to_null(self, tmp_path, monkeypatch):
        """Only signal_aggregation_{run_date}.parquet is read — a file for
        a different date must not leak in."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.SIGNAL_AGG_PATH.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": ["AAPL"], "composite_score": [0.9], "composite_grade": ["A"],
            "sector_breadth_pct": [90.0], "sector_momentum": [1.0], "breadth_divergence": [0.1],
        }).write_parquet(scr_mod.SIGNAL_AGG_PATH / f"signal_aggregation_{date(2026, 5, 31).isoformat()}.parquet")

        result = build_watchlist(run_date)
        assert result.row(0, named=True)["composite_score"] is None

    def test_corrupt_signal_agg_file_degrades_to_null(self, tmp_path, monkeypatch):
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        scr_mod.SIGNAL_AGG_PATH.mkdir(parents=True, exist_ok=True)
        (scr_mod.SIGNAL_AGG_PATH / f"signal_aggregation_{run_date.isoformat()}.parquet").write_text(
            "not a parquet file"
        )
        result = build_watchlist(run_date)
        assert not result.is_empty()
        assert result.row(0, named=True)["composite_score"] is None


# ── FIX GLD-SCR-003 regression (correlation cluster dedup) ──────────────────

def _pairwise_corr(clusters: dict, path):
    """Write a cross_asset_corr.parquet-shaped frame (upper triangle, one row per unordered
    pair, cluster id of each side) for {symbol: cluster_id}, exactly as
    CorrelationModule._to_long_format() lays it out."""
    syms = list(clusters)
    rows = [(syms[i], syms[j]) for i in range(len(syms)) for j in range(i + 1, len(syms))]
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol_a": [a for a, _ in rows],
        "symbol_b": [b for _, b in rows],
        "correlation": [0.5] * len(rows),
        "regime": ["mixed"] * len(rows),
        "computation_date": ["2026-05-31"] * len(rows),
        "cluster_id_a": [clusters[a] for a, _ in rows],
        "cluster_id_b": [clusters[b] for _, b in rows],
    }).write_parquet(path)
    return path


class TestClusterDeduplication:
    """See FIX GLD-SCR-003 in the module docstring above. Cluster source is
    cross_asset_corr.parquet (pairwise), read directly — the legacy
    correlation_clusters.parquet bridge is archived (RISK-31, 29 Oct 2026)."""

    def test_direct_call_enforces_max_per_cluster(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scr_mod, "CROSS_ASSET_CORR_PATH", tmp_path / "cross_asset_corr.parquet")
        _pairwise_corr({"A": 1, "B": 1, "C": 1, "D": 2, "E": 2}, scr_mod.CROSS_ASSET_CORR_PATH)
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

    def test_symbol_only_ever_in_symbol_b_is_still_clustered(self, tmp_path, monkeypatch):
        """In the upper triangle the LAST symbol appears only as symbol_b. Reading
        symbol_a alone would leave it cluster -1 (never deduped)."""
        monkeypatch.setattr(scr_mod, "CROSS_ASSET_CORR_PATH", tmp_path / "cross_asset_corr.parquet")
        _pairwise_corr({"A": 1, "B": 1, "C": 1}, scr_mod.CROSS_ASSET_CORR_PATH)
        pairs = pl.read_parquet(scr_mod.CROSS_ASSET_CORR_PATH)
        assert "C" not in pairs["symbol_a"].to_list()   # fixture premise: C is symbol_b only
        df = pl.DataFrame({"symbol": ["A", "B", "C"], "mtf_score": [7, 6, 5]})
        result = _deduplicate_by_cluster(df, date(2026, 6, 1), duckdb.connect())
        assert result["symbol"].to_list() == ["A", "B"]

    def test_conflicting_cluster_ids_never_fan_out_the_join(self, tmp_path, monkeypatch):
        """Cardinality guard: one symbol carrying two cluster ids must not duplicate rows."""
        monkeypatch.setattr(scr_mod, "CROSS_ASSET_CORR_PATH", tmp_path / "cross_asset_corr.parquet")
        pl.DataFrame({
            "symbol_a": ["A", "A"], "symbol_b": ["B", "C"],
            "cluster_id_a": [1, 2], "cluster_id_b": [1, 3],
        }).write_parquet(scr_mod.CROSS_ASSET_CORR_PATH)
        df = pl.DataFrame({"symbol": ["A", "B", "C"], "mtf_score": [7, 6, 5]})
        result = _deduplicate_by_cluster(df, date(2026, 6, 1), duckdb.connect())
        assert result["symbol"].to_list().count("A") == 1
        assert len(result) <= len(df)

    def test_legacy_schema_file_leaves_candidates_untouched(self, tmp_path, monkeypatch):
        """A file in the retired (symbol, cluster_id) shape is not this source's schema:
        dedup degrades (no raise, no row loss) rather than mis-reading it."""
        monkeypatch.setattr(scr_mod, "CROSS_ASSET_CORR_PATH", tmp_path / "cross_asset_corr.parquet")
        pl.DataFrame({"symbol": ["A", "B", "C"], "cluster_id": [1, 1, 1]}).write_parquet(
            scr_mod.CROSS_ASSET_CORR_PATH)
        df = pl.DataFrame({"symbol": ["A", "B", "C"], "mtf_score": [7, 6, 5]})
        result = _deduplicate_by_cluster(df, date(2026, 6, 1), duckdb.connect())
        assert result["symbol"].to_list() == ["A", "B", "C"]

    def test_no_correlation_file_leaves_candidates_untouched(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scr_mod, "CROSS_ASSET_CORR_PATH", tmp_path / "nonexistent.parquet")
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
        _pairwise_corr({"A": 1, "B": 1, "C": 1}, scr_mod.CROSS_ASSET_CORR_PATH)
        result = build_watchlist(run_date)
        assert len(result) == scr_mod.MAX_PER_CLUSTER
        assert set(result["symbol"].to_list()) == {"A", "B"}   # top 2 by score kept

    def test_full_build_watchlist_ignores_a_stale_legacy_clusters_file(self, tmp_path, monkeypatch):
        """The retired correlation_clusters.parquet must not influence the watchlist even if
        it is still on disk (it is no longer refreshed)."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [
            _mtf_row("A", 9, "A"), _mtf_row("B", 8, "A"), _mtf_row("C", 7, "A"),
        ])
        monkeypatch.chdir(tmp_path)
        legacy = tmp_path / "data" / "gold" / "correlation" / "correlation_clusters.parquet"
        legacy.parent.mkdir(parents=True)
        pl.DataFrame({"symbol": ["A", "B", "C"], "cluster_id": [1, 1, 1]}).write_parquet(legacy)
        result = build_watchlist(run_date)          # no cross_asset_corr.parquet -> no dedup
        assert set(result["symbol"].to_list()) == {"A", "B", "C"}


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
            _mtf_row("LOW", 5, "B"), _mtf_row("HIGH", 7, "A"), _mtf_row("SKIP", 3, "C"),
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




# ── Earnings & sentiment enrichment: retired in full (ADR-043/ADR-044, GMI_Decision_Document_v10.docx) -- see KNOWN_RISKS.md RISK-4 and dev-log/2026-08-22-adr043-044-finnhub-full-retirement.md for full detail ──────────────────────────────────────────────────────




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

    def test_watchlist_schema_stable_without_finnhub_enrichment(self, tmp_path, monkeypatch):
        """FIX ADR-044 (GMI_Decision_Document_v10.docx): with _enrich_earnings()
        and _enrich_sentiment() removed entirely, the watchlist output schema
        must be byte-identical to before — column names and types unchanged,
        values permanently null instead of sometimes-populated. The NULL
        placeholders live in build_watchlist()'s own main query, not in
        either (now-deleted) enrichment step, so this must hold with zero
        Silver fundamental/sentiment data present at all."""
        mtf_dir = _patch_all_optional_sources(tmp_path, monkeypatch)
        run_date = date(2026, 6, 1)
        _write_mtf(_mtf_path(mtf_dir, run_date), [_mtf_row("AAPL", 7, "A")])
        monkeypatch.setattr(scr_mod, "SILVER_OHLCV_ROOT", tmp_path / "silver_ohlcv_missing")
        run(run_date)
        out = scr_mod.GOLD_SCREENER_PATH / f"watchlist_{run_date.isoformat()}.parquet"
        written = pl.read_parquet(out)

        expected_dtypes = {
            "days_to_earnings":   pl.Int32,
            "next_earnings_date": pl.Date,
            "near_earnings_flag": pl.Boolean,
            "sentiment_score":    pl.Float64,
            "buzz_score":         pl.Float64,
        }
        for col, dtype in expected_dtypes.items():
            assert col in written.columns, f"{col} missing from watchlist output"
            assert written.schema[col] == dtype, (
                f"{col} dtype changed: expected {dtype}, got {written.schema[col]}"
            )
            assert written[col].null_count() == len(written), (
                f"{col} expected to be permanently null (no enrichment source), "
                f"found {len(written) - written[col].null_count()} non-null value(s)"
            )


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
