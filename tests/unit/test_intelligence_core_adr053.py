"""
tests/unit/test_intelligence_core_adr053.py — ADR-053 shared specialist engine (core.py).

Pins the properties that keep a fit out-of-sample: purge/embargo, the causal calibrator, the
exactness of aggregated-row training, persistence, and the refusal to score from the past with
a model from the future. Mutation tests (a purge that purges nothing, a calibrator that sees
its own block) must FAIL the guards, not merely pass the happy path.
"""
import copy
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest
import yaml

from src.backtest.triple_barrier import BarrierConfig
from src.gold.intelligence import core
from src.gold.intelligence.core import (
    FrameSpec, IntelligenceConfig, IntelligenceError, LeakageError, apply_calibrator, assert_no_overlap,
    block_starts, feature_spec_hash, fit_calibrator, fit_final, load_bundle, load_config, metrics,
    predict_bundle, purged_train_mask, save_bundle, validate_config, walk_forward_oof, weighted_auc,
)

FEATS = ["x1", "x2"]
SPEC = FrameSpec(key_cols=("decision_date", "sym"))


def _raw(**over):
    raw = yaml.safe_load(open("config/intelligence.yaml"))
    raw = copy.deepcopy(raw)
    raw["xgb"].update(n_estimators=40, nthread=2)
    raw["walk_forward"].update(initial_train_months=12, test_block_months=6, embargo_days=7, min_train_rows=300)
    raw["bags"].update(oof=2, final=3)
    raw["calibration"]["min_rows"] = 500
    for k, v in over.items():
        raw[k].update(v)
    return raw


@pytest.fixture(scope="module")
def cfg() -> IntelligenceConfig:
    return validate_config(_raw())


def _frame(n_days=1300, n_sym=8, signal=2.0, seed=7, horizon=9) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    days = [d for d in (date(2015, 1, 1) + timedelta(days=i) for i in range(int(n_days * 7 / 5) + 10)) if d.weekday() < 5][:n_days]
    rows = []
    for d in days:
        x1 = rng.normal(size=n_sym)
        x2 = rng.normal(size=n_sym)
        pw = 1 / (1 + np.exp(-(signal * x1 - 0.8)))
        y = (rng.random(n_sym) < pw).astype(float)
        for k in range(n_sym):
            rows.append((d, f"S{k}", x1[k], x2[k], y[k], d + timedelta(days=horizon)))
    return pl.DataFrame(rows, schema=["decision_date", "sym", "x1", "x2", "label", "label_end_date"], orient="row") \
        .with_columns(pl.lit(1.0).alias("weight"))


# ── config ──────────────────────────────────────────────────────────────────

class TestConfig:
    def test_live_config_validates_and_fingerprint_is_stable(self):
        a, b = load_config(), load_config()
        assert a.fingerprint() == b.fingerprint() and len(a.fingerprint()) == 12

    def test_label_spec_matches_the_labeler_default(self):
        """Drift guard: the spec the specialists accept IS what ADR-052's labeler stamps by default."""
        assert load_config().label_spec == BarrierConfig().spec

    def test_fingerprint_changes_with_any_model_setting(self):
        base = validate_config(_raw()).fingerprint()
        assert validate_config(_raw(xgb={"max_depth": 4})).fingerprint() != base
        assert validate_config(_raw(walk_forward={"embargo_days": 9})).fingerprint() != base

    @pytest.mark.parametrize("mut", [
        lambda r: r.update(extra=1),
        lambda r: r["walk_forward"].update(test_block_months=5),
        lambda r: r["bags"].update(final=0),
        lambda r: r["confidence"].update(std_cap=0),
        lambda r: r["xgb"].pop("max_depth"),
    ])
    def test_invalid_config_is_rejected(self, mut):
        raw = _raw()
        mut(raw)
        with pytest.raises(IntelligenceError):
            validate_config(raw)

    def test_feature_hash_is_order_sensitive(self):
        assert feature_spec_hash(["a", "b"]) != feature_spec_hash(["b", "a"])


# ── folds / purging ─────────────────────────────────────────────────────────

class TestFolds:
    def test_first_block_is_initial_months_after_first_label(self, cfg):
        s = block_starts(date(2015, 1, 5), date(2018, 12, 31), cfg)
        assert s[0] == date(2016, 1, 1) and s[1] == date(2016, 7, 1)

    def test_purge_drops_windows_that_reach_the_block(self):
        ends = pl.Series([date(2020, 1, 1), date(2020, 6, 20), date(2020, 6, 24), date(2020, 7, 1)])
        # (4th: ends 2020-07-01)
        keep = purged_train_mask(ends, date(2020, 7, 1), 7).to_list()
        # embargo edge = start - 7d = 2020-06-24: a window ending ON the edge is allowed,
        # one ending on the block start itself is not
        assert keep == [True, True, True, False]

    def test_overlap_guard_raises_on_a_reaching_training_row(self):
        df = pl.DataFrame({"decision_date": [date(2020, 6, 20)], "label_end_date": [date(2020, 7, 5)]})
        with pytest.raises(LeakageError):
            assert_no_overlap(df, date(2020, 7, 1), 7, "decision_date", "label_end_date")

    def test_overlap_guard_passes_a_clean_training_set(self):
        df = pl.DataFrame({"decision_date": [date(2020, 6, 1)], "label_end_date": [date(2020, 6, 10)]})
        assert_no_overlap(df, date(2020, 7, 1), 7, "decision_date", "label_end_date")


# ── metrics exactness ───────────────────────────────────────────────────────

class TestAggregationIsExact:
    def test_weighted_metrics_equal_the_replicated_row_metrics(self):
        rng = np.random.default_rng(3)
        n = 40
        p = rng.random(n)
        k = rng.integers(2, 9, size=n)                       # rows per distinct feature vector
        wins = np.array([rng.integers(0, kk + 1) for kk in k])
        # aggregated
        ya, wa = wins / k, k.astype(float)
        # replicated
        yr = np.concatenate([np.r_[np.ones(w), np.zeros(kk - w)] for w, kk in zip(wins, k)])
        pr = np.concatenate([np.full(kk, pp) for kk, pp in zip(k, p)])
        ma, mr = metrics(ya, p, wa), metrics(yr, pr)
        for key in ("base_rate", "auc", "brier", "logloss"):
            assert ma[key] == pytest.approx(mr[key], rel=1e-9), key

    def test_auc_is_nan_when_one_class_only(self):
        assert np.isnan(weighted_auc(np.ones(5), np.linspace(0, 1, 5), np.ones(5)))


# ── calibrator ──────────────────────────────────────────────────────────────

class TestCalibrator:
    def test_monotone_and_roundtrips_through_two_arrays(self):
        rng = np.random.default_rng(1)
        p = rng.random(4000)
        y = (rng.random(4000) < p ** 2).astype(float)        # miscalibrated on purpose
        cal = fit_calibrator(p, y, None)
        out = apply_calibrator(cal, np.linspace(0, 1, 50))
        assert np.all(np.diff(out) >= -1e-12) and out.min() >= 0 and out.max() <= 1
        assert abs(apply_calibrator(cal, np.array([0.9]))[0] - 0.81) < 0.08

    def test_none_calibrator_is_identity(self):
        p = np.array([0.1, 0.5])
        assert np.array_equal(apply_calibrator(None, p), p)


# ── walk-forward engine ─────────────────────────────────────────────────────

class TestWalkForward:
    def test_learns_a_real_signal_out_of_fold(self, cfg):
        oof = walk_forward_oof(_frame(), FEATS, SPEC, cfg)
        assert oof.height > 0
        auc = weighted_auc(oof["label"].to_numpy(), oof["p_raw"].to_numpy(), oof["weight"].to_numpy())
        assert auc > 0.70

    def test_pure_noise_is_not_learned(self, cfg):
        oof = walk_forward_oof(_frame(signal=0.0), FEATS, SPEC, cfg)
        auc = weighted_auc(oof["label"].to_numpy(), oof["p_raw"].to_numpy(), oof["weight"].to_numpy())
        assert 0.46 < auc < 0.54

    def test_no_oof_row_precedes_its_block_or_the_first_block(self, cfg):
        fr = _frame()
        oof = walk_forward_oof(fr, FEATS, SPEC, cfg)
        assert (oof["decision_date"] >= oof["fold_start"]).all()
        first = block_starts(fr["decision_date"].min(), fr["decision_date"].max(), cfg)[0]
        assert oof["decision_date"].min() >= first

    def test_every_training_set_is_purged_and_strictly_earlier(self, cfg):
        seen = []

        def spy(X, y, w, feats, c, n, seed0=0):
            seen.append(len(y))
            return core.fit_bagged(X, y, w, feats, c, n, seed0)

        walk_forward_oof(_frame(), FEATS, SPEC, cfg, fit_fn=spy)
        assert len(seen) >= 3 and seen == sorted(seen)       # expanding window: never shrinks

    def test_MUTANT_a_purge_that_purges_nothing_is_caught(self, cfg):
        """Kills the 'forgot to purge' mutant: the structural guard must raise, not the AUC."""
        no_purge = lambda ends, start, emb: pl.Series([True] * len(ends))
        with pytest.raises(LeakageError):
            walk_forward_oof(_frame(), FEATS, SPEC, cfg, purge_fn=no_purge)

    def test_incremental_run_adds_only_new_blocks(self, cfg):
        fr = _frame()
        cut = date(2018, 1, 1)
        part = walk_forward_oof(fr.filter(pl.col("decision_date") < cut), FEATS, SPEC, cfg)
        again = walk_forward_oof(fr.filter(pl.col("decision_date") < cut), FEATS, SPEC, cfg, done=part)
        assert again.height == 0
        more = walk_forward_oof(fr, FEATS, SPEC, cfg, done=part)
        assert more.height > 0 and more["fold_start"].min() >= part["fold_start"].max()

    def test_first_block_is_uncalibrated_and_later_ones_calibrated_causally(self, cfg):
        oof = walk_forward_oof(_frame(), FEATS, SPEC, cfg)
        folds = oof.group_by("fold_start").agg(pl.col("calibrated").first()).sort("fold_start")["calibrated"].to_list()
        assert folds[0] is False and any(folds[1:])

    def test_deterministic(self, cfg):
        a = walk_forward_oof(_frame(), FEATS, SPEC, cfg)
        b = walk_forward_oof(_frame(), FEATS, SPEC, cfg)
        assert np.allclose(a["p_raw"].to_numpy(), b["p_raw"].to_numpy())

    def test_row_order_of_the_input_does_not_change_the_result(self, cfg):
        """XGBoost's row subsampling is positional; an unordered group_by frame must not make two
        fits of the same data differ."""
        fr = _frame()
        a = walk_forward_oof(fr, FEATS, SPEC, cfg)
        b = walk_forward_oof(fr.sample(fraction=1.0, shuffle=True, seed=3), FEATS, SPEC, cfg)
        ka = a.sort(["decision_date", "sym"])["p_raw"].to_numpy()
        kb = b.sort(["decision_date", "sym"])["p_raw"].to_numpy()
        assert np.allclose(ka, kb)

    def test_block_with_too_few_purged_rows_is_skipped_not_fitted(self):
        c = validate_config(_raw(walk_forward={"min_train_rows": 10 ** 7}))
        assert walk_forward_oof(_frame(), FEATS, SPEC, c).height == 0

    def test_missing_column_is_a_hard_error(self, cfg):
        with pytest.raises(IntelligenceError):
            walk_forward_oof(_frame().drop("x2"), FEATS, SPEC, cfg)

    def test_aggregated_rows_train_like_replicated_rows(self, cfg):
        """Date-level features shared by many rows: one aggregated row (mean label, weight=n)
        must give the same ranking as the replicated rows."""
        fr = _frame(n_sym=1, n_days=1300).with_columns(pl.lit(1.0).alias("weight"))
        rep = pl.concat([fr] * 5).sort("decision_date")                      # 5 identical copies per day
        agg = fr.with_columns(pl.lit(5.0).alias("weight"))                   # one row, weight 5
        a = walk_forward_oof(rep, FEATS, FrameSpec(key_cols=("decision_date", "sym")), cfg)
        b = walk_forward_oof(agg, FEATS, FrameSpec(key_cols=("decision_date", "sym")), cfg)
        ma = metrics(a["label"].to_numpy(), a["p_raw"].to_numpy(), a["weight"].to_numpy())
        mb = metrics(b["label"].to_numpy(), b["p_raw"].to_numpy(), b["weight"].to_numpy())
        assert abs(ma["auc"] - mb["auc"]) < 0.03


# ── final model + bundle ────────────────────────────────────────────────────

class TestBundle:
    @pytest.fixture(scope="class")
    def built(self, cfg):
        fr = _frame()
        oof = walk_forward_oof(fr, FEATS, SPEC, cfg)
        return fr, oof, fit_final(fr, FEATS, SPEC, cfg, oof, date(2019, 12, 31), {"model": "unit"})

    def test_final_fit_uses_only_rows_that_ended_before_fit_date_minus_embargo(self, built, cfg):
        fr, oof, b = built
        assert date.fromisoformat(b["train_end"]) <= date(2019, 12, 31) - timedelta(days=cfg.embargo_days)

    def test_roundtrip_predicts_identically(self, built, tmp_path):
        fr, oof, b = built
        path = tmp_path / "m" / "model.json"
        save_bundle(path, b)
        again = load_bundle(path)
        X = fr.head(50)
        a = predict_bundle(b, X, date(2020, 1, 2))
        c = predict_bundle(again, X, date(2020, 1, 2))
        assert np.allclose(a["probability"].to_numpy(), c["probability"].to_numpy())
        assert ((a["probability"] >= 0) & (a["probability"] <= 1)).all()
        assert ((a["confidence"] >= 0) & (a["confidence"] <= 1)).all()

    def test_feature_column_order_does_not_matter(self, built):
        fr, oof, b = built
        X = fr.head(30)
        a = predict_bundle(b, X.select(["x2", "x1", "label"]), date(2020, 1, 2))
        c = predict_bundle(b, X.select(["x1", "x2"]), date(2020, 1, 2))
        assert np.allclose(a["probability"].to_numpy(), c["probability"].to_numpy())

    def test_model_from_the_future_is_refused(self, built):
        fr, oof, b = built
        with pytest.raises(LeakageError):
            predict_bundle(b, fr.head(5), date(2019, 6, 1))

    def test_missing_feature_is_a_hard_error(self, built):
        fr, oof, b = built
        with pytest.raises(IntelligenceError):
            predict_bundle(b, fr.head(5).drop("x1"), date(2020, 1, 2))

    def test_bundle_carries_lineage(self, built, cfg):
        fr, oof, b = built
        assert b["config_fingerprint"] == cfg.fingerprint() and b["label_spec"] == cfg.label_spec
        assert b["feature_spec_hash"] == feature_spec_hash(FEATS) and b["n_train_rows"] > 0

    def test_load_missing_bundle_is_none(self, tmp_path):
        assert load_bundle(tmp_path / "nope.json") is None

    def test_atomic_save_leaves_no_temp_files(self, built, tmp_path):
        save_bundle(tmp_path / "x" / "m.json", built[2])
        assert [p.name for p in (tmp_path / "x").iterdir()] == ["m.json"]
