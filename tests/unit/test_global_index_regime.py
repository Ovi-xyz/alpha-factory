"""
tests/unit/test_global_index_regime.py — GlobalIndexRegimeModule unit tests
(Architecture v2.0 §6.5, GMI Wave 1 Cycle 4 — first CrossAssetEngine module)

Test matrix:
    - _resolve_universe() against the REAL instruments_taxonomy.yaml (live
      config, no mocking — matches test_context_anchors.py's own precedent
      of exercising the real InstrumentLoader rather than a fixture).
    - compute(): all-DM-up/all-EM-down breadth math, asia_pac_breadth
      scoping, RISK_ON/MIXED/RISK_OFF threshold boundaries.
    - No Layer 2 Silver data yet -> compute() returns None, run() writes
      nothing (mirrors technical_signals.py's own "no data yet" contract).
    - regime_transition: False on first-ever row (no prior), True when
      label changes, False when label repeats.
    - Idempotent same-day re-run: row count does not grow.
    - partial_coverage_flag: True when a DM/EM symbol has no Silver rows.
    - run() job entry point delegates and writes the expected schema.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

import src.gold.cross_asset.global_index_regime as gir
from src.gold.cross_asset.global_index_regime import GlobalIndexRegimeModule, run

DM_SYMBOLS = ["SPX", "NYA", "DJI", "IXIC", "FTSE", "DAX", "CAC", "N225", "AXJO"]
EM_SYMBOLS = ["TWSE", "KOSPI", "HSI", "SSEC", "JKSE"]
ALL_SYMBOLS = DM_SYMBOLS + EM_SYMBOLS


def _write_context_symbol(
    silver_root: Path,
    symbol: str,
    base_close: float,
    trend: float,
    n_days: int = 80,
    start: date = date(2026, 1, 1),
    is_clean: bool = True,
) -> None:
    """Write one Layer 2 context 1D Silver fixture file — same directory
    shape (context/symbol=X/X_1D_silver.parquet) test_technical_signals.py
    uses for its own Layer 2 fixtures."""
    path = silver_root / "context" / f"symbol={symbol}" / f"{symbol}_1D_silver.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol":    [symbol] * n_days,
        "timestamp": [start + timedelta(days=d) for d in range(n_days)],
        "close":     [base_close + trend * d for d in range(n_days)],
        "is_clean":  [is_clean] * n_days,
    }).write_parquet(path)


def _write_all_trending(silver_root: Path, dm_trend: float, em_trend: float, n_days: int = 80) -> None:
    for s in DM_SYMBOLS:
        _write_context_symbol(silver_root, s, 100.0, dm_trend, n_days=n_days)
    for s in EM_SYMBOLS:
        _write_context_symbol(silver_root, s, 100.0, em_trend, n_days=n_days)


@pytest.fixture
def module_with_tmp_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(gir, "SILVER_OHLCV_PATH", tmp_path)
    monkeypatch.setattr(gir, "GOLD_CROSS_ASSET_PATH", tmp_path / "gold")
    monkeypatch.setattr(gir, "GLOBAL_REGIME_STORE_PATH", tmp_path / "gold" / "global_regime.parquet")
    return GlobalIndexRegimeModule(), tmp_path


class TestResolveUniverse:
    """Against the REAL instruments_taxonomy.yaml — no mocking, matches
    test_context_anchors.py's own precedent."""

    def test_dm_em_partition_matches_live_taxonomy(self):
        module = GlobalIndexRegimeModule()
        dm, em = module._resolve_universe()
        assert dm == frozenset(DM_SYMBOLS), f"DM set drifted from live taxonomy: {sorted(dm)}"
        assert em == frozenset(EM_SYMBOLS), f"EM set drifted from live taxonomy: {sorted(em)}"

    def test_dm_em_disjoint(self):
        module = GlobalIndexRegimeModule()
        dm, em = module._resolve_universe()
        assert dm & em == frozenset(), "DM and EM must never overlap"


class TestComputeBreadthMath:
    def test_all_dm_up_all_em_down(self, module_with_tmp_paths):
        """9 DM uptrending, 5 EM downtrending -> dm_score=100, em_score=0,
        global=9/14≈64.3 (RISK_ON), matches the manual smoke-test values."""
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=-1.0)
        run_date = date(2026, 1, 1) + timedelta(days=79)

        record = module.compute(run_date)

        assert record is not None
        assert record["dm_score"] == 100.0
        assert record["em_score"] == 0.0
        assert record["global_risk_score"] == pytest.approx(9 / 14 * 100, abs=0.01)
        assert record["dm_em_divergence"] == 100.0
        assert record["global_regime_label"] == "RISK_ON"
        assert record["n_dm_covered"] == 9
        assert record["n_em_covered"] == 5
        assert record["partial_coverage_flag"] is False

    def test_asia_pac_breadth_scoped_to_seven(self, module_with_tmp_paths):
        """asia_pac_breadth must only reflect N225/AXJO (DM) + all 5 EM —
        the other 7 DM indices (SPX, NYA, DJI, IXIC, FTSE, DAX, CAC) must
        NOT influence it even though they're in the global universe."""
        module, tmp_path = module_with_tmp_paths
        # Asia-Pacific 7 all up; the other 7 DM (non-APAC) all down.
        apac_dm = {"N225", "AXJO"}
        for s in DM_SYMBOLS:
            trend = 1.0 if s in apac_dm else -1.0
            _write_context_symbol(tmp_path, s, 100.0, trend)
        for s in EM_SYMBOLS:
            _write_context_symbol(tmp_path, s, 100.0, 1.0)
        run_date = date(2026, 1, 1) + timedelta(days=79)

        record = module.compute(run_date)

        assert record["asia_pac_breadth"] == 100.0
        assert record["n_asia_pac_covered"] == 7

    def test_all_flat_prices_no_ema_crossover_is_risk_off_or_mixed(self, module_with_tmp_paths):
        """Perfectly flat close (trend=0) leaves close == EMA everywhere,
        so 'close > ema' is False for all 14 -> global_risk_score=0.0 ->
        RISK_OFF (<=40 threshold). A boundary sanity check, not a claim
        about real market behavior."""
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=0.0, em_trend=0.0)
        run_date = date(2026, 1, 1) + timedelta(days=79)

        record = module.compute(run_date)

        assert record["global_risk_score"] == 0.0
        assert record["global_regime_label"] == "RISK_OFF"


class TestClassifyThresholds:
    def test_risk_on_at_or_above_sixty(self):
        assert GlobalIndexRegimeModule._classify(60.0) == "RISK_ON"
        assert GlobalIndexRegimeModule._classify(100.0) == "RISK_ON"

    def test_risk_off_at_or_below_forty(self):
        assert GlobalIndexRegimeModule._classify(40.0) == "RISK_OFF"
        assert GlobalIndexRegimeModule._classify(0.0) == "RISK_OFF"

    def test_mixed_in_neutral_band(self):
        assert GlobalIndexRegimeModule._classify(50.0) == "MIXED"
        assert GlobalIndexRegimeModule._classify(41.0) == "MIXED"
        assert GlobalIndexRegimeModule._classify(59.0) == "MIXED"


class TestNoDataYet:
    def test_compute_returns_none_when_context_dir_missing(self, module_with_tmp_paths):
        module, _ = module_with_tmp_paths
        record = module.compute(date(2026, 1, 1))
        assert record is None

    def test_run_writes_nothing_when_no_data(self, module_with_tmp_paths):
        _, tmp_path = module_with_tmp_paths
        run(date(2026, 1, 1))
        assert not (tmp_path / "gold" / "global_regime.parquet").exists()

    def test_compute_returns_none_when_all_rows_after_run_date(self, module_with_tmp_paths):
        """PIT bound: data exists but entirely AFTER run_date -> must not
        be used (no lookahead)."""
        module, tmp_path = module_with_tmp_paths
        future_start = date(2030, 1, 1)
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=1.0, n_days=10)
        for s in ALL_SYMBOLS:
            path = tmp_path / "context" / f"symbol={s}" / f"{s}_1D_silver.parquet"
            df = pl.read_parquet(path).with_columns(
                pl.date_range(future_start, future_start + timedelta(days=9), "1d").alias("timestamp")
            )
            df.write_parquet(path)

        record = module.compute(date(2026, 1, 1))
        assert record is None


class TestRegimeTransition:
    def test_first_row_transition_is_false(self, module_with_tmp_paths):
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=1.0)
        record = module.compute(date(2026, 1, 1) + timedelta(days=79))
        assert record["regime_transition"] is False
        assert record["prev_global_regime_label"] is None

    def test_transition_true_when_label_changes_across_runs(self, module_with_tmp_paths):
        _, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=1.0, n_days=80)
        d1 = date(2026, 1, 1) + timedelta(days=79)
        run(d1)

        # Day 2: flip everything down hard -> RISK_OFF
        _write_all_trending(tmp_path, dm_trend=-5.0, em_trend=-5.0, n_days=81)
        d2 = d1 + timedelta(days=1)
        run(d2)

        store = pl.read_parquet(tmp_path / "gold" / "global_regime.parquet").sort("date")
        rows = store.select(["date", "global_regime_label", "regime_transition"]).to_dicts()
        assert rows[0]["global_regime_label"] == "RISK_ON"
        assert rows[0]["regime_transition"] is False
        assert rows[1]["global_regime_label"] == "RISK_OFF"
        assert rows[1]["regime_transition"] is True

    def test_no_transition_when_label_repeats(self, module_with_tmp_paths):
        _, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=1.0, n_days=80)
        d1 = date(2026, 1, 1) + timedelta(days=79)
        run(d1)
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=1.0, n_days=81)
        d2 = d1 + timedelta(days=1)
        run(d2)

        store = pl.read_parquet(tmp_path / "gold" / "global_regime.parquet").sort("date")
        assert store.select("regime_transition").to_series().to_list() == [False, False]


class TestIdempotentRerun:
    def test_same_day_rerun_does_not_duplicate_row(self, module_with_tmp_paths):
        _, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=1.0)
        d1 = date(2026, 1, 1) + timedelta(days=79)
        run(d1)
        run(d1)
        store = pl.read_parquet(tmp_path / "gold" / "global_regime.parquet")
        assert store.height == 1


class TestPartialCoverage:
    def test_flag_true_when_a_dm_symbol_has_no_data(self, module_with_tmp_paths):
        module, tmp_path = module_with_tmp_paths
        # Only 8 of 9 DM + all 5 EM — SPX missing entirely.
        for s in DM_SYMBOLS:
            if s == "SPX":
                continue
            _write_context_symbol(tmp_path, s, 100.0, 1.0)
        for s in EM_SYMBOLS:
            _write_context_symbol(tmp_path, s, 100.0, -1.0)

        record = module.compute(date(2026, 1, 1) + timedelta(days=79))

        assert record["n_dm_covered"] == 8
        assert record["partial_coverage_flag"] is True

    def test_flag_false_when_full_coverage(self, module_with_tmp_paths):
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=-1.0)
        record = module.compute(date(2026, 1, 1) + timedelta(days=79))
        assert record["partial_coverage_flag"] is False


class TestRunEntryPoint:
    def test_run_writes_expected_schema(self, module_with_tmp_paths):
        _, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=-1.0)
        run_date = date(2026, 1, 1) + timedelta(days=79)

        run(run_date)

        out = pl.read_parquet(tmp_path / "gold" / "global_regime.parquet")
        required_cols = {
            "date", "global_risk_score", "dm_score", "em_score",
            "dm_em_divergence", "asia_pac_breadth", "global_regime_label",
            "prev_global_regime_label", "regime_transition",
            "n_dm_covered", "n_em_covered", "n_asia_pac_covered",
            "partial_coverage_flag", "processing_version",
        }
        assert required_cols.issubset(set(out.columns))
        assert out.height == 1

    def test_run_excludes_is_clean_false_rows_entirely(self, module_with_tmp_paths):
        """A symbol whose ENTIRE history is is_clean=False must be fully
        excluded from coverage — OHLCVProcessor's own self-flagging is the
        authority here, same convention as every other Gold consumer of
        Silver OHLCV (technical_signals.py's WHERE is_clean = TRUE)."""
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=-1.0)
        _write_context_symbol(
            tmp_path, "SPX", 100.0, 1.0, is_clean=False
        )  # overwrite SPX entirely dirty

        record = module.compute(date(2026, 1, 1) + timedelta(days=79))
        assert record["n_dm_covered"] == 8
        assert record["partial_coverage_flag"] is True

    def test_run_uses_last_clean_row_when_only_final_bar_is_dirty(self, module_with_tmp_paths):
        """A single dirty bar on the classification date does NOT drop the
        symbol from coverage — the last CLEAN row is used instead (graceful
        staleness, not symbol dropout), which is the behavior actually
        produced by `WHERE is_clean = TRUE` + group_by(...).last()."""
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=-1.0)
        spx_path = tmp_path / "context" / "symbol=SPX" / "SPX_1D_silver.parquet"
        df = pl.read_parquet(spx_path)
        df = df.with_columns(
            pl.when(pl.int_range(pl.len()) == pl.len() - 1)
            .then(pl.lit(False))
            .otherwise(pl.col("is_clean"))
            .alias("is_clean")
        )
        df.write_parquet(spx_path)

        record = module.compute(date(2026, 1, 1) + timedelta(days=79))
        # SPX still contributes (via its last clean bar, day 78) — coverage
        # is unaffected by excluding one dirty bar out of many.
        assert record["n_dm_covered"] == 9
