"""
tests/unit/test_hmm_walk_forward_adr053.py — ADR-053 D2: the honest as-of regime history.

The load-bearing property is TRUNCATION INVARIANCE: a fold's rows are produced by a model fitted
on rows strictly before the fold start and filtered causally, so cutting the panel at the end of
fold k must not change any row of folds <= k. A walk-forward that quietly used a later model or a
later observation fails it. Synthetic panels use the real model configs' feature names.
"""
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from src.gold.hmm import hmm_macro_regime as macro_mod
from src.gold.hmm import walk_forward as wf
from src.gold.hmm.config import macro_model_config, market_model_config

MACRO_CFG = macro_model_config()
MARKET_CFG = market_model_config()
WFC = wf.WalkForwardConfig(refit_months=12, macro_first_fold=date(2006, 1, 1), market_min_train_obs=500)


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _regime(n: int, seed: int) -> np.ndarray:
    """Persistent 3-state Markov chain — enough structure for a HMM to find without degenerating."""
    rng = np.random.default_rng(seed)
    z = np.zeros(n, dtype=int)
    for i in range(1, n):
        z[i] = z[i - 1] if rng.random() < 0.985 else rng.integers(0, 3)
    return z


def _panel(features: list[str], n: int, start: date, seed: int) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    z = _regime(n, seed)
    cols = {"as_of_date": _weekdays(start, n)}
    for j, f in enumerate(features):
        mu = np.array([-1.0, 0.0, 1.0]) * (1 + 0.1 * j)
        cols[f] = mu[z] + rng.normal(scale=0.6, size=n)
    return pl.DataFrame(cols)


def _macro_features() -> list[str]:
    return sorted({f for d in MACRO_CFG.domains for f in macro_mod._feats(MACRO_CFG, d)})


@pytest.fixture(scope="module")
def macro_panel():
    return _panel(_macro_features(), 3400, date(2000, 1, 3), seed=1)       # 2000-01 .. ~2013


@pytest.fixture(scope="module")
def macro_full(macro_panel):
    return wf.build_macro(macro_panel, MACRO_CFG, WFC, date(2013, 6, 28))


@pytest.fixture(scope="module")
def market_panel():
    return _panel(MARKET_CFG.feature_names(), 1500, date(2016, 9, 1), seed=2)


class TestConfig:
    def test_fold_starts_snap_down_and_step(self):
        assert wf.fold_starts(date(2011, 2, 14), date(2012, 3, 1), 3) == [
            date(2011, 1, 1), date(2011, 4, 1), date(2011, 7, 1), date(2011, 10, 1), date(2012, 1, 1)]
        assert wf.fold_starts(date(2011, 1, 1), date(2011, 12, 31), 12) == [date(2011, 1, 1)]

    @pytest.mark.parametrize("raw", [
        {"refit_months": 5, "macro_first_fold": "2011-01-01", "market_min_train_obs": 500},
        {"refit_months": 3, "macro_first_fold": "nope", "market_min_train_obs": 500},
        {"refit_months": 3, "macro_first_fold": "2011-01-01", "market_min_train_obs": 10},
        {"refit_months": 3, "macro_first_fold": "2011-01-01"},
        {"refit_months": 3, "macro_first_fold": "2011-01-01", "market_min_train_obs": 500, "x": 1},
    ])
    def test_bad_config_rejected(self, raw):
        with pytest.raises((wf.WalkForwardError, ValueError)):
            wf.validate_config(raw)

    def test_live_config_loads(self):
        c = wf.load_config()
        assert c.refit_months == 3 and c.macro_first_fold == date(2011, 1, 1)


class TestMacroWalkForward:
    def test_rows_start_at_first_fold_and_none_is_in_sample(self, macro_full):
        assert macro_full.height > 0
        assert macro_full["date"].min() >= date(2006, 1, 1)
        assert not macro_full["in_sample"].any()

    def test_every_row_lies_in_its_fold_and_after_the_models_training_end(self, macro_full):
        nxt = macro_full["fold_start"].unique().sort()
        assert (macro_full["date"] >= macro_full["fold_start"]).all()
        assert (macro_full["wf_train_end"] < macro_full["fold_start"]).all()
        starts = nxt.to_list()
        for a, b in zip(starts, starts[1:]):
            assert macro_full.filter(pl.col("fold_start") == a)["date"].max() < b

    def test_TRUNCATION_INVARIANCE_no_row_depends_on_data_after_its_fold(self, macro_panel, macro_full):
        """Cut the panel at the end of the 3rd fold: folds 1-2 must be bit-identical."""
        starts = macro_full["fold_start"].unique().sort().to_list()
        cut = starts[3]                                              # first day of the 4th fold
        short = wf.build_macro(macro_panel.filter(pl.col("as_of_date") < cut), MACRO_CFG, WFC, cut - timedelta(days=1))
        keep = starts[:2]
        cols = ["date", "growth_score", "monetary_score", "inflation_score", "liquidity_score", "macro_activity"]
        a = macro_full.filter(pl.col("fold_start").is_in(keep)).sort("date").select(cols)
        b = short.filter(pl.col("fold_start").is_in(keep)).sort("date").select(cols)
        assert a.height == b.height > 0
        for c in cols[1:]:
            assert np.allclose(a[c].to_numpy(), b[c].to_numpy(), equal_nan=True), c

    def test_incremental_keep_reproduces_the_full_build(self, macro_panel, macro_full):
        starts = macro_full["fold_start"].unique().sort().to_list()
        keep = macro_full.filter(pl.col("fold_start") < starts[3])
        again = wf.build_macro(macro_panel, MACRO_CFG, WFC, date(2013, 6, 28), keep=keep)
        cols = ["date", "growth_score", "macro_activity", "macro_label"]
        a = macro_full.sort("date").select(cols)
        b = again.sort("date").select(cols)
        assert a.height == b.height
        assert a["macro_label"].to_list() == b["macro_label"].to_list()
        assert np.allclose(a["macro_activity"].to_numpy(), b["macro_activity"].to_numpy(), equal_nan=True)

    def test_macro_label_is_stitched_across_folds_not_restarted(self, macro_full):
        """The §F rule carries the previous label inside the +-0.10 band; restarting per fold would
        make the first row of every fold fall back to the default."""
        # The first folds have < short_history_min_obs (2,000) training rows, so growth is flagged
        # short_history, excluded from the activity average and the label is NULL (rule F) — exactly
        # what the production store does. From the first full-history fold on there is no gap.
        full_history = macro_full.filter(pl.col("fold_start") >= date(2008, 1, 1))
        assert full_history.height > 0 and full_history["macro_label"].null_count() == 0
        assert set(macro_full["macro_label"].drop_nulls().unique().to_list()) <= {
            "expansion", "slowdown", "recession", "recovery"}

    def test_stress_domain_is_skipped_when_it_has_no_history_not_invented(self, macro_panel):
        panel = macro_panel.with_columns(pl.lit(None, dtype=pl.Float64).alias("BAMLH0A0HYM2"))
        out = wf.build_macro(panel, MACRO_CFG, WFC, date(2013, 6, 28))
        assert out["stress_score"].null_count() == out.height


class TestMarketWalkForward:
    def test_first_fold_needs_min_train_obs_behind_it(self, market_panel):
        first = wf._first_market_fold(market_panel, MARKET_CFG.feature_names(), 12, 500)
        assert first is not None
        cc = market_panel.sort("as_of_date")
        assert cc.filter(pl.col("as_of_date") < first).height >= 500

    def test_too_short_panel_builds_nothing(self, market_panel):
        out = wf.build_market(market_panel.head(200), MARKET_CFG, WFC, date(2018, 1, 1))
        assert out.height == 0

    def test_no_in_sample_rows_and_training_precedes_every_fold(self, market_panel):
        out = wf.build_market(market_panel, MARKET_CFG, WFC, date(2022, 6, 30))
        assert out.height > 0 and not out["in_sample"].any()
        assert (out["wf_train_end"] < out["fold_start"]).all() and (out["date"] >= out["fold_start"]).all()

    def test_TRUNCATION_INVARIANCE(self, market_panel):
        full = wf.build_market(market_panel, MARKET_CFG, WFC, date(2022, 6, 30))
        starts = full["fold_start"].unique().sort().to_list()
        cut = starts[2]
        short = wf.build_market(market_panel.filter(pl.col("as_of_date") < cut), MARKET_CFG, WFC, cut - timedelta(days=1))
        k = starts[:2]
        a = full.filter(pl.col("fold_start").is_in(k)).sort("date")
        b = short.filter(pl.col("fold_start").is_in(k)).sort("date")
        assert a.height == b.height > 0
        assert np.allclose(a["market_score"].to_numpy(), b["market_score"].to_numpy())
        assert a["market_label"].to_list() == b["market_label"].to_list()


class TestIncrementalStore:
    def test_reusable_keeps_completed_folds_and_drops_the_open_one(self, macro_full, tmp_path):
        p = tmp_path / "m.parquet"
        macro_full.write_parquet(p)
        keep = wf._reusable(p, MACRO_CFG.fingerprint())
        last = macro_full["fold_start"].max()
        assert keep is not None and keep["fold_start"].max() < last

    def test_changed_fingerprint_or_version_forces_a_rebuild(self, macro_full, tmp_path):
        p = tmp_path / "m.parquet"
        macro_full.write_parquet(p)
        assert wf._reusable(p, "different-fingerprint") is None
        macro_full.with_columns(pl.lit("0.0").alias("processing_version")).write_parquet(p)
        assert wf._reusable(p, MACRO_CFG.fingerprint()) is None

    def test_missing_or_malformed_store_is_a_rebuild(self, tmp_path):
        assert wf._reusable(tmp_path / "none.parquet", "x") is None
        pl.DataFrame({"a": [1]}).write_parquet(tmp_path / "bad.parquet")
        assert wf._reusable(tmp_path / "bad.parquet", "x") is None


class TestRun:
    def test_run_writes_both_stores_and_a_second_run_is_stable(self, macro_panel, tmp_path, monkeypatch):
        market_panel = _panel(MARKET_CFG.feature_names(), 1500, date(2009, 9, 1), seed=2)   # overlaps the run_date
        monkeypatch.setattr(wf, "WF_MACRO_PATH", tmp_path / "wf" / "macro.parquet")
        monkeypatch.setattr(wf, "WF_MARKET_PATH", tmp_path / "wf" / "market.parquet")
        monkeypatch.setattr(wf.raw_macro, "RAW_MACRO_PATH", tmp_path / "raw_macro.parquet")
        monkeypatch.setattr(wf.raw_market, "RAW_MARKET_PATH", tmp_path / "raw_market.parquet")
        monkeypatch.setattr(wf, "load_config", lambda path=None: WFC)
        macro_panel.write_parquet(tmp_path / "raw_macro.parquet")
        market_panel.write_parquet(tmp_path / "raw_market.parquet")
        wf.run(date(2013, 6, 28))
        a = pl.read_parquet(tmp_path / "wf" / "macro.parquet")
        assert a.height > 0 and pl.read_parquet(tmp_path / "wf" / "market.parquet").height > 0
        wf.run(date(2013, 6, 28))
        b = pl.read_parquet(tmp_path / "wf" / "macro.parquet")
        assert a.height == b.height and a["date"].to_list() == b["date"].to_list()

    def test_run_without_the_raw_panels_is_a_clear_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(wf.raw_macro, "RAW_MACRO_PATH", tmp_path / "a.parquet")
        monkeypatch.setattr(wf.raw_market, "RAW_MARKET_PATH", tmp_path / "b.parquet")
        monkeypatch.setattr(wf, "load_config", lambda path=None: WFC)
        with pytest.raises(wf.WalkForwardError):
            wf.run(date(2013, 6, 28))
