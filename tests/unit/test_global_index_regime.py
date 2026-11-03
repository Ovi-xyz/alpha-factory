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
        """9 DM uptrending, 5 EM downtrending -> dm_score=100, em_score=0.
        The equal-weight count is 9/14 (kept as global_risk_score_equal); the region-weighted
        global_risk_score (XAE-R07) gives SPX/NYA/DJI/IXIC ONE vote: 5 other DM + 1 US block up out of
        10 + 1 votes = 6/11 -> MIXED, where the old count said RISK_ON (guide G5)."""
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, dm_trend=1.0, em_trend=-1.0)
        run_date = date(2026, 1, 1) + timedelta(days=79)

        record = module.compute(run_date)

        assert record is not None
        assert record["dm_score"] == 100.0
        assert record["em_score"] == 0.0
        assert record["global_risk_score_equal"] == pytest.approx(9 / 14 * 100, abs=0.01)
        assert record["global_risk_score"] == pytest.approx(6 / 11 * 100, abs=0.01)
        assert record["dm_em_divergence"] == 100.0
        assert record["global_regime_label_raw"] == "MIXED"
        assert record["global_regime_label"] == "MIXED"
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

    def test_transition_true_when_label_changes_across_runs(self, module_with_tmp_paths, monkeypatch):
        """Transition mechanics with the debounce switched off (DEBOUNCE_DAYS=1); the debounce itself is
        covered in TestDebounceR07."""
        monkeypatch.setattr(gir, "DEBOUNCE_DAYS", 1)
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


# ═══════════════════════════════════════════════════════════════════════════
# XAE-R01 / XAE-R07 (cross_asset_recalibration_guide) — bar-date keying, debounce, region weighting
# ═══════════════════════════════════════════════════════════════════════════

from src.gold.cross_asset.global_index_regime import (  # noqa: E402
    apply_state, debounce_labels, normalize_legacy_rows, upsert_bar_row, US_VOTE_BLOCK,
)

D0 = date(2026, 1, 1)


def _store(tmp_path) -> pl.DataFrame:
    return pl.read_parquet(tmp_path / "gold" / "global_regime.parquet")


def _write_with_last_bar_date(tmp_path, last_bar: date, trend: float = 1.0, em_trend: float | None = None):
    """Every index trades up to `last_bar` inclusive (calendar-continuous fixture)."""
    n_days = (last_bar - D0).days + 1
    _write_all_trending(tmp_path, dm_trend=trend, em_trend=trend if em_trend is None else em_trend,
                        n_days=n_days)


class TestBarDateKeying:
    """XAE-R01 (guide G1): one row per BAR date; weekend re-runs add nothing."""

    def test_weekend_reruns_after_a_friday_bar_add_no_row(self, module_with_tmp_paths):
        _, tmp_path = module_with_tmp_paths
        friday = date(2026, 3, 20)
        _write_with_last_bar_date(tmp_path, friday)
        run(date(2026, 3, 21))            # Saturday run, newest bar = Friday
        run(date(2026, 3, 22))            # Sunday run, same bar
        store = _store(tmp_path)
        assert store.height == 1
        assert store["as_of_bar_date"][0] == friday

    def test_first_seen_run_date_is_kept_for_lineage(self, module_with_tmp_paths):
        """`date` is when the bar first became available — the PIT-safe choice — not the last re-run."""
        _, tmp_path = module_with_tmp_paths
        _write_with_last_bar_date(tmp_path, date(2026, 3, 20))
        run(date(2026, 3, 21))
        run(date(2026, 3, 22))
        assert _store(tmp_path)["date"].to_list() == ["2026-03-21"]

    def test_a_new_bar_adds_a_row(self, module_with_tmp_paths):
        _, tmp_path = module_with_tmp_paths
        _write_with_last_bar_date(tmp_path, date(2026, 3, 20))
        run(date(2026, 3, 21))
        _write_with_last_bar_date(tmp_path, date(2026, 3, 23))
        run(date(2026, 3, 24))
        store = _store(tmp_path).sort("as_of_bar_date")
        assert store["as_of_bar_date"].to_list() == [date(2026, 3, 20), date(2026, 3, 23)]

    def test_transition_is_counted_per_bar_date_not_per_run(self, module_with_tmp_paths, monkeypatch):
        """Friday bar scored RISK_ON; Silver then catches up and the Saturday run also sees only Friday —
        the label cannot 'transition' just because another run happened."""
        monkeypatch.setattr(gir, "DEBOUNCE_DAYS", 1)
        _, tmp_path = module_with_tmp_paths
        _write_with_last_bar_date(tmp_path, date(2026, 3, 20))
        run(date(2026, 3, 21))
        run(date(2026, 3, 22))
        run(date(2026, 3, 23))            # still the Friday bar (no Monday bar yet)
        store = _store(tmp_path)
        assert store.height == 1
        assert store["regime_transition"].to_list() == [False]

    def test_as_of_bar_date_is_the_newest_bar_any_index_contributed(self, module_with_tmp_paths):
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, 1.0, 1.0, n_days=70)
        _write_context_symbol(tmp_path, "SPX", 100.0, 1.0, n_days=75)   # one index is 5 bars ahead
        record = module.compute(D0 + timedelta(days=100))
        assert record["as_of_bar_date"] == D0 + timedelta(days=74)

    def test_late_bars_after_run_date_are_not_read(self, module_with_tmp_paths):
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, 1.0, 1.0, n_days=80)
        record = module.compute(D0 + timedelta(days=50))
        assert record["as_of_bar_date"] == D0 + timedelta(days=50)

    def test_run_writes_the_new_columns(self, module_with_tmp_paths):
        _, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, 1.0, -1.0)
        run(D0 + timedelta(days=79))
        cols = set(_store(tmp_path).columns)
        assert {"as_of_bar_date", "global_regime_label_raw", "global_risk_score_equal",
                "breadth_dist_pct", "days_in_state", "flicker_10d"} <= cols


class TestLegacyRows:
    """Rows written before processing_version 1.1 have no bar date and may be weekend duplicates."""

    @staticmethod
    def _legacy(rows):
        base = {"dm_score": 50.0, "em_score": 50.0, "asia_pac_breadth": 50.0}
        return pl.DataFrame([
            {**base, "date": d, "global_risk_score": sc, "global_regime_label": lab,
             "as_of_bar_date": None}
            for d, sc, lab in rows
        ], schema_overrides={"as_of_bar_date": pl.Date})

    def test_identical_weekend_copy_is_dropped(self):
        df = self._legacy([("2026-10-16", 50.0, "MIXED"), ("2026-10-17", 35.7, "RISK_OFF"),
                           ("2026-10-18", 35.7, "RISK_OFF")])
        out = normalize_legacy_rows(df).sort("date")
        assert out["date"].to_list() == ["2026-10-16", "2026-10-17"]

    def test_rows_with_a_bar_date_are_never_dropped(self):
        df = self._legacy([("2026-10-17", 35.7, "RISK_OFF"), ("2026-10-18", 35.7, "RISK_OFF")]).with_columns(
            pl.Series("as_of_bar_date", [date(2026, 10, 16), date(2026, 10, 16)], dtype=pl.Date))
        assert normalize_legacy_rows(df).height == 2

    def test_a_legacy_label_change_is_kept(self):
        df = self._legacy([("2026-10-16", 50.0, "MIXED"), ("2026-10-17", 35.7, "RISK_OFF")])
        assert normalize_legacy_rows(df).height == 2


class TestUpsert:
    def _row(self, run_date, bar, label="RISK_ON"):
        return pl.DataFrame([{"date": run_date, "as_of_bar_date": bar, "global_regime_label": label,
                              "global_regime_label_raw": label, "global_risk_score": 70.0}])

    def test_same_bar_replaces_and_keeps_earliest_run_date(self):
        store = self._row("2026-03-21", date(2026, 3, 20))
        out = upsert_bar_row(store, self._row("2026-03-22", date(2026, 3, 20), "MIXED"))
        assert out.height == 1
        assert out["date"][0] == "2026-03-21"
        assert out["global_regime_label_raw"][0] == "MIXED"     # latest Silver wins

    def test_backfill_with_an_earlier_run_date_takes_the_earlier_date(self):
        store = self._row("2026-03-22", date(2026, 3, 20))
        out = upsert_bar_row(store, self._row("2026-03-21", date(2026, 3, 20)))
        assert out["date"][0] == "2026-03-21"

    def test_different_bar_is_appended(self):
        store = self._row("2026-03-21", date(2026, 3, 20))
        assert upsert_bar_row(store, self._row("2026-03-24", date(2026, 3, 23))).height == 2


class TestDebounceR07:
    """XAE-R07 (guide G4): switch only after DEBOUNCE_DAYS consecutive bar dates of a new raw label."""

    def test_two_day_blip_does_not_switch_at_three(self):
        raw = ["MIXED", "MIXED", "RISK_OFF", "RISK_OFF", "MIXED", "MIXED"]
        assert debounce_labels(raw, 3) == ["MIXED"] * 6

    def test_three_consecutive_days_switch_on_the_third(self):
        raw = ["MIXED", "RISK_OFF", "RISK_OFF", "RISK_OFF", "RISK_OFF"]
        assert debounce_labels(raw, 3) == ["MIXED", "MIXED", "MIXED", "RISK_OFF", "RISK_OFF"]

    def test_a_return_to_the_current_label_cancels_a_pending_switch(self):
        raw = ["MIXED", "RISK_OFF", "RISK_OFF", "MIXED", "RISK_OFF", "RISK_OFF", "MIXED"]
        assert set(debounce_labels(raw, 3)) == {"MIXED"}

    def test_candidate_change_restarts_the_count(self):
        # OFF, OFF, then ON, ON, ON: the pending OFF never reaches 3; ON does.
        raw = ["MIXED", "RISK_OFF", "RISK_OFF", "RISK_ON", "RISK_ON", "RISK_ON"]
        assert debounce_labels(raw, 3)[-1] == "RISK_ON"
        assert debounce_labels(raw, 3)[3] == "MIXED"

    def test_n_one_is_the_identity(self):
        raw = ["A", "B", "A"]
        assert debounce_labels(raw, 1) == raw

    def test_replay_is_causal_a_later_label_never_changes_an_earlier_output(self):
        raw = ["MIXED", "RISK_OFF", "RISK_OFF", "RISK_OFF", "MIXED", "MIXED", "MIXED"]
        full = debounce_labels(raw, 3)
        for k in range(1, len(raw) + 1):
            assert debounce_labels(raw[:k], 3) == full[:k]

    def test_flicker_cuts_transitions_on_a_choppy_series(self):
        raw = (["MIXED", "RISK_OFF"] * 20)
        deb = debounce_labels(raw, 3)
        raw_changes = sum(1 for a, b in zip(raw, raw[1:]) if a != b)
        deb_changes = sum(1 for a, b in zip(deb, deb[1:]) if a != b)
        assert raw_changes == 39 and deb_changes == 0

    def test_apply_state_derives_prev_transition_days_and_flicker(self):
        labels = ["MIXED", "MIXED", "RISK_OFF", "RISK_OFF", "RISK_OFF", "RISK_OFF"]
        series = pl.DataFrame({
            "date": [f"2026-03-{20 + i}" for i in range(6)],
            "as_of_bar_date": [date(2026, 3, 20 + i) for i in range(6)],
            "global_regime_label": labels, "global_regime_label_raw": labels,
        })
        out = apply_state(series, debounce_days=3)
        assert out["global_regime_label"].to_list() == ["MIXED"] * 4 + ["RISK_OFF"] * 2
        assert out["regime_transition"].to_list() == [False, False, False, False, True, False]
        assert out["prev_global_regime_label"].to_list() == [None, "MIXED", "MIXED", "MIXED", "MIXED",
                                                             "RISK_OFF"]
        assert out["days_in_state"].to_list() == [1, 2, 3, 4, 1, 2]
        assert out["flicker_10d"].to_list() == [0, 0, 1, 1, 1, 1]

    def test_apply_state_orders_by_bar_date_not_by_row_order(self):
        series = pl.DataFrame({
            "date": ["2026-03-24", "2026-03-21"],
            "as_of_bar_date": [date(2026, 3, 23), date(2026, 3, 20)],
            "global_regime_label": ["RISK_OFF", "MIXED"],
            "global_regime_label_raw": ["RISK_OFF", "MIXED"],
        })
        out = apply_state(series, debounce_days=1)
        assert out["as_of_bar_date"].to_list() == [date(2026, 3, 20), date(2026, 3, 23)]
        assert out["regime_transition"].to_list() == [False, True]

    def test_end_to_end_one_day_crash_is_debounced(self, module_with_tmp_paths):
        """Three runs: calm -> one RISK_OFF day -> calm. The published label never leaves its state."""
        _, tmp_path = module_with_tmp_paths
        n = 70
        _write_all_trending(tmp_path, 1.0, 1.0, n_days=n)
        run(D0 + timedelta(days=n - 1))
        # one day where every index closes far below its EMA
        for s in ALL_SYMBOLS:
            path = tmp_path / "context" / f"symbol={s}" / f"{s}_1D_silver.parquet"
            df = pl.read_parquet(path)
            crash = pl.DataFrame({"symbol": [s], "timestamp": [D0 + timedelta(days=n)],
                                  "close": [10.0], "is_clean": [True]})
            pl.concat([df, crash]).write_parquet(path)
        run(D0 + timedelta(days=n))
        store = _store(tmp_path).sort("as_of_bar_date")
        assert store["global_regime_label_raw"].to_list() == ["RISK_ON", "RISK_OFF"]
        assert store["global_regime_label"].to_list() == ["RISK_ON", "RISK_ON"]
        assert store["flicker_10d"].to_list() == [0, 1]


class TestRegionWeighting:
    """XAE-R07 (guide G5): the four US indices are one vote."""

    def test_us_block_membership_exists_in_the_live_taxonomy(self):
        dm, _ = GlobalIndexRegimeModule()._resolve_universe()
        assert US_VOTE_BLOCK <= dm, f"US block symbols missing from context_equity_dm: {US_VOTE_BLOCK - dm}"

    def test_us_block_alone_cannot_flip_the_label_across_the_neutral_band(self, module_with_tmp_paths):
        """US block up, everything else down: the equal-weight count is 4/14 = 28.6 (RISK_OFF) while a
        full US swing under the old rule could add 28.6 points. Weighted it is 1/11 = 9.1."""
        module, tmp_path = module_with_tmp_paths
        for s in ALL_SYMBOLS:
            _write_context_symbol(tmp_path, s, 100.0, 1.0 if s in US_VOTE_BLOCK else -1.0)
        record = module.compute(D0 + timedelta(days=79))
        assert record["global_risk_score_equal"] == pytest.approx(4 / 14 * 100, abs=0.01)
        assert record["global_risk_score"] == pytest.approx(1 / 11 * 100, abs=0.01)

    def test_empty_block_restores_equal_weighting(self, module_with_tmp_paths, monkeypatch):
        monkeypatch.setattr(gir, "US_VOTE_BLOCK", frozenset())
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, 1.0, -1.0)
        record = module.compute(D0 + timedelta(days=79))
        assert record["global_risk_score"] == record["global_risk_score_equal"]

    def test_partially_covered_block_still_casts_one_vote(self, module_with_tmp_paths):
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, 1.0, 1.0)
        import shutil
        for s in ("NYA", "DJI"):
            shutil.rmtree(tmp_path / "context" / f"symbol={s}")
        record = module.compute(D0 + timedelta(days=79))
        assert record["global_risk_score"] == 100.0              # everything up, any weighting
        assert record["n_dm_covered"] == 7


class TestBreadthDistance:
    def test_distance_is_clipped_per_index(self, module_with_tmp_paths):
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, 1.0, 1.0)
        # one index ends 10x above its EMA: unclipped it would dominate the mean
        path = tmp_path / "context" / "symbol=SPX" / "SPX_1D_silver.parquet"
        df = pl.read_parquet(path)
        df = df.with_columns(pl.when(pl.int_range(pl.len()) == pl.len() - 1).then(pl.col("close") * 10)
                             .otherwise(pl.col("close")).alias("close"))
        df.write_parquet(path)
        record = module.compute(D0 + timedelta(days=79))
        assert record["breadth_dist_pct"] <= gir.DIST_CLIP_PCT + 1e-9

    def test_distance_is_negative_when_everything_trades_below_ema(self, module_with_tmp_paths):
        module, tmp_path = module_with_tmp_paths
        _write_all_trending(tmp_path, -1.0, -1.0)
        assert module.compute(D0 + timedelta(days=79))["breadth_dist_pct"] < 0
