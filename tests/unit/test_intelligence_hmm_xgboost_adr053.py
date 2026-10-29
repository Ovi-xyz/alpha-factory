"""
tests/unit/test_intelligence_hmm_xgboost_adr053.py — ADR-053 specialist #1 (regime domain):
the as-of feature builder, the label reader and the fit/predict jobs, on synthetic stores.
"""
import copy
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest
import yaml

from src.gold.intelligence import core
from src.gold.intelligence import features_hmm as fh
from src.gold.intelligence import hmm_xgboost as hx
from src.gold.intelligence import labels as lab


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _cfg(**over) -> core.IntelligenceConfig:
    raw = copy.deepcopy(yaml.safe_load(open("config/intelligence.yaml")))
    raw["xgb"].update(n_estimators=30, nthread=2)
    raw["walk_forward"].update(initial_train_months=12, test_block_months=6, embargo_days=7, min_train_rows=100)
    raw["bags"].update(oof=2, final=2)
    raw["calibration"]["min_rows"] = 200
    raw["models"] = {}
    return core.validate_config(raw)


def _stores(n=900, start=date(2018, 1, 1), seed=5):
    """Macro/market frames with the production columns; the market score carries the signal."""
    rng = np.random.default_rng(seed)
    days = _weekdays(start, n)
    ms = np.cumsum(rng.normal(scale=0.15, size=n)) * 0.2
    ms = ms - ms.mean()
    macro = pl.DataFrame({"date": days, "macro_label": ["expansion"] * n, **{
        f: rng.normal(size=n) for f in fh.MACRO_FEATURES}})
    market = pl.DataFrame({"date": days, "market_label": ["risk_on"] * n, "market_score": ms,
                           "market_probability": rng.uniform(0.5, 1.0, n), "market_post_risk_off": rng.uniform(0, 0.4, n),
                           "market_post_risk_on": rng.uniform(0, 0.4, n)})
    return macro, market, ms


def _labels(days, ms, symbols_per_market=4, seed=9, horizon=9):
    rng = np.random.default_rng(seed)
    spec = core.load_config().label_spec
    rows = []
    for d, s in zip(days, ms):
        for mk in fh.MARKETS:
            for k in range(symbols_per_market):
                for direction in fh.DIRECTIONS:
                    sign = 1.0 if direction == "long" else -1.0
                    p = 1 / (1 + np.exp(-(4.0 * sign * s - 0.8)))
                    rows.append((f"{mk[:2]}{k}", mk, d, direction, int(rng.random() < p), d + timedelta(days=horizon),
                                 spec))
    return pl.DataFrame(rows, schema=["symbol", "market", "decision_date", "direction", "label_binary",
                                      "label_end_date", "label_spec"], orient="row")


# ── feature builder ──────────────────────────────────────────────────────────

class TestFeatureBuilder:
    def test_feature_list_is_pinned_and_excludes_the_short_history_stress_domain(self):
        assert not any(f.startswith("stress_") for f in fh.FEATURES)
        assert fh.FEATURES[-2:] == ("direction_sign", "market_code") and len(fh.FEATURES) == 17
        assert core.feature_spec_hash(list(fh.FEATURES)) == core.feature_spec_hash(list(fh.FEATURES))

    def test_asof_is_strictly_before_the_decision_date(self):
        macro, market, _ = _stores(60)
        feats = fh.regime_feature_table(macro, market)
        dec = pl.DataFrame({"decision_date": [macro["date"][10], macro["date"][11]]})
        out = fh.attach_asof(dec, feats)
        assert out["feature_date"][0] == macro["date"][9] and out["feature_date"][1] == macro["date"][10]
        assert (out["feature_date"] < out["decision_date"]).all()

    def test_a_feature_row_never_serves_its_own_date(self):
        macro, market, _ = _stores(30)
        out = fh.attach_asof(pl.DataFrame({"decision_date": [macro["date"][0]]}), fh.regime_feature_table(macro, market))
        assert out["feature_date"][0] is None                         # nothing strictly earlier exists

    def test_market_row_older_than_the_tolerance_is_not_attached(self):
        macro, market, _ = _stores(60)
        market = market.filter(~pl.col("date").is_between(macro["date"][20], macro["date"][30]))   # a 11-weekday hole
        feats = fh.regime_feature_table(macro, market)
        hole = feats.filter(pl.col("feature_date") == macro["date"][28])
        assert hole["market_score"][0] is None

    def test_market_age_is_reported(self):
        macro, market, _ = _stores(40)
        feats = fh.regime_feature_table(macro, market.filter(pl.col("date") != macro["date"][15]))
        assert feats.filter(pl.col("feature_date") == macro["date"][15])["market_age_days"][0] >= 1

    def test_missing_columns_are_a_clear_error(self):
        macro, market, _ = _stores(40)
        with pytest.raises(core.IntelligenceError):
            fh.regime_feature_table(macro.drop("macro_activity"), market)

    def test_training_frame_aggregates_to_date_market_direction(self):
        macro, market, ms = _stores(120)
        labels = _labels(macro["date"].to_list(), ms, symbols_per_market=3)
        fr = fh.training_frame(labels, fh.regime_feature_table(macro, market))
        assert fr["weight"].unique().to_list() == [3.0]
        assert fr.group_by(["decision_date", "market", "direction"]).len()["len"].max() == 1
        assert ((fr["label"] >= 0) & (fr["label"] <= 1)).all()
        assert set(fr["market_code"].unique().to_list()) == {0.0, 1.0, 2.0, 3.0}
        assert (fr["feature_date"] < fr["decision_date"]).all()

    def test_training_frame_drops_incomplete_rows_instead_of_imputing(self):
        macro, market, ms = _stores(120)
        macro = macro.with_columns(pl.when(pl.col("date") == macro["date"][50]).then(None).otherwise(pl.col("growth_score")).alias("growth_score"))
        labels = _labels(macro["date"].to_list(), ms, symbols_per_market=1)
        fr = fh.training_frame(labels, fh.regime_feature_table(macro, market))
        assert macro["date"][50] not in fr["feature_date"].to_list()
        assert fr.select(pl.all_horizontal(pl.col(c).is_not_null() for c in fh.FEATURES).all()).item()

    def test_aggregated_label_end_is_the_latest_window_end(self):
        macro, market, ms = _stores(40)
        labels = _labels(macro["date"].to_list(), ms, symbols_per_market=2)
        labels = labels.with_columns(pl.when(pl.col("symbol").str.ends_with("1")).then(pl.col("label_end_date") + pl.duration(days=5))
                                     .otherwise(pl.col("label_end_date")).alias("label_end_date"))
        fr = fh.training_frame(labels, fh.regime_feature_table(macro, market))
        d = fr["decision_date"][5]
        assert fr.filter(pl.col("decision_date") == d)["label_end_date"].unique().to_list() == [d + timedelta(days=14)]

    def test_prediction_grid_is_market_by_direction_from_the_latest_prior_row(self):
        macro, market, _ = _stores(60)
        feats = fh.regime_feature_table(macro, market)
        run = macro["date"][40]
        g = fh.prediction_rows(feats, run)
        assert g.height == len(fh.MARKETS) * len(fh.DIRECTIONS)
        assert g["feature_date"].unique().to_list() == [macro["date"][39]]
        assert set(g["direction_sign"].unique().to_list()) == {1.0, -1.0}

    def test_stale_or_incomplete_feature_row_is_not_scored(self):
        macro, market, _ = _stores(60)
        feats = fh.regime_feature_table(macro, market)
        assert fh.prediction_rows(feats, macro["date"][59] + timedelta(days=fh.MAX_FEATURE_AGE_DAYS + 5)) is None
        assert fh.prediction_rows(feats, macro["date"][0]) is None
        broken = feats.with_columns(pl.lit(None, dtype=pl.Float64).alias("market_score"))
        assert fh.prediction_rows(broken, macro["date"][40]) is None


# ── label reader ─────────────────────────────────────────────────────────────

class TestLabelReader:
    def _write(self, tmp_path, df, name="labels_2026-10-16.parquet"):
        tmp_path.mkdir(parents=True, exist_ok=True)
        df.write_parquet(tmp_path / name)

    def test_missing_directory_or_file(self, tmp_path):
        with pytest.raises(lab.LabelsMissing):
            lab.load_training_labels(_cfg(), date(2026, 10, 20), tmp_path / "none")

    def test_valid_labels_pass(self, tmp_path):
        macro, market, ms = _stores(30)
        self._write(tmp_path, _labels(macro["date"].to_list(), ms, 1).with_columns(pl.lit(core.load_config().label_spec).alias("label_spec")))
        out = lab.load_training_labels(_cfg(), macro["date"][-1] + timedelta(days=10), tmp_path)
        assert out.height > 0 and out.schema["label_binary"] == pl.Int8

    def test_foreign_label_spec_is_refused(self, tmp_path):
        macro, market, ms = _stores(30)
        self._write(tmp_path, _labels(macro["date"].to_list(), ms, 1).with_columns(pl.lit("tb9.9|other").alias("label_spec")))
        with pytest.raises(lab.LabelSpecMismatch):
            lab.load_training_labels(_cfg(), macro["date"][-1] + timedelta(days=10), tmp_path)

    def test_mixed_specs_in_one_file_are_refused(self, tmp_path):
        macro, market, ms = _stores(30)
        df = _labels(macro["date"].to_list(), ms, 1)
        df = df.with_columns(pl.when(pl.col("symbol").str.ends_with("0")).then(pl.lit("x")).otherwise(pl.col("label_spec")).alias("label_spec"))
        self._write(tmp_path, df)
        with pytest.raises(lab.LabelSpecMismatch):
            lab.load_training_labels(_cfg(), date(2018, 3, 1), tmp_path)

    def test_stale_labels_are_refused(self, tmp_path):
        macro, market, ms = _stores(30)
        self._write(tmp_path, _labels(macro["date"].to_list(), ms, 1))
        with pytest.raises(lab.LabelsStale):
            lab.load_training_labels(_cfg(), macro["date"][-1] + timedelta(days=200), tmp_path)

    def test_missing_column_and_bad_values_are_schema_errors(self, tmp_path):
        macro, market, ms = _stores(30)
        df = _labels(macro["date"].to_list(), ms, 1)
        self._write(tmp_path, df.drop("market"))
        with pytest.raises(lab.LabelSchemaError):
            lab.load_training_labels(_cfg(), date(2018, 3, 1), tmp_path)
        self._write(tmp_path, df.with_columns(pl.lit(2).alias("label_binary")), "labels_2026-10-17.parquet")
        with pytest.raises(lab.LabelSchemaError):
            lab.load_training_labels(_cfg(), date(2018, 3, 1), tmp_path)

    def test_newest_file_wins(self, tmp_path):
        macro, market, ms = _stores(30)
        old = _labels(macro["date"].to_list(), ms, 1).with_columns(pl.lit("old").alias("label_spec"))
        self._write(tmp_path, old, "labels_a.parquet")
        import os, time
        time.sleep(0.05)
        self._write(tmp_path, _labels(macro["date"].to_list(), ms, 1), "labels_b.parquet")
        os.utime(tmp_path / "labels_b.parquet", None)
        assert lab.latest_label_file(tmp_path).name == "labels_b.parquet"


# ── fit / predict jobs ───────────────────────────────────────────────────────

@pytest.fixture
def world(tmp_path, monkeypatch):
    macro, market, ms = _stores(1100)
    for name, df in (("WF_MACRO_PATH", macro), ("WF_MARKET_PATH", market), ("MACRO_PATH", macro), ("MARKET_PATH", market)):
        p = tmp_path / "hmm" / f"{name}.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(p)
        monkeypatch.setattr(fh, name, p)
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir()
    _labels(macro["date"].to_list(), ms, symbols_per_market=3).write_parquet(labels_dir / "labels_x.parquet")
    cfg = core.validate_config({**yaml.safe_load(open("config/intelligence.yaml")),
                                "xgb": {**yaml.safe_load(open("config/intelligence.yaml"))["xgb"], "n_estimators": 30, "nthread": 2},
                                "walk_forward": {"initial_train_months": 12, "test_block_months": 6, "embargo_days": 7, "min_train_rows": 100},
                                "bags": {"oof": 2, "final": 2}, "calibration": {"min_rows": 200},
                                "output_root": str(tmp_path / "out"), "models": {}})
    return {"cfg": cfg, "labels_dir": labels_dir, "macro": macro, "run_date": macro["date"][-1] + timedelta(days=3), "tmp": tmp_path}


class TestFitAndPredict:
    def test_fit_writes_model_oof_validation_and_status(self, world):
        st = hx.run_fit(world["run_date"], world["cfg"], world["labels_dir"])
        store = core.ModelStore(world["cfg"].output_root, hx.MODEL_NAME)
        assert st["status"] == "fitted" and store.model.exists() and store.oof.exists() and store.validation.exists()
        rep = core.json.loads(store.validation.read_text())
        assert rep["n_blocks"] >= 2 and rep["overall"]["auc"] > 0.55          # the synthetic regime signal is learnable

    def test_fit_skips_recorded_on_stale_labels_and_keeps_the_previous_model(self, world):
        hx.run_fit(world["run_date"], world["cfg"], world["labels_dir"])
        store = core.ModelStore(world["cfg"].output_root, hx.MODEL_NAME)
        before = store.model.read_text()
        st = hx.run_fit(world["run_date"] + timedelta(days=400), world["cfg"], world["labels_dir"])
        assert st["status"] == "skipped" and "LabelsStale" in st["reason"]
        assert store.model.read_text() == before                              # previous model untouched
        assert core.json.loads(store.fit_status.read_text())["status"] == "skipped"

    def test_fit_skips_when_the_walk_forward_history_is_missing(self, world, monkeypatch):
        monkeypatch.setattr(fh, "WF_MACRO_PATH", world["tmp"] / "nope.parquet")
        st = hx.run_fit(world["run_date"], world["cfg"], world["labels_dir"])
        assert st["status"] == "skipped" and "walk-forward" in st["reason"]

    def test_second_fit_reuses_completed_oof_blocks(self, world):
        hx.run_fit(world["run_date"], world["cfg"], world["labels_dir"])
        store = core.ModelStore(world["cfg"].output_root, hx.MODEL_NAME)
        first = pl.read_parquet(store.oof)
        hx.run_fit(world["run_date"], world["cfg"], world["labels_dir"])
        second = pl.read_parquet(store.oof)
        assert first.height == second.height
        assert np.allclose(first.sort(["decision_date", "market", "direction"])["p_raw"].to_numpy(),
                           second.sort(["decision_date", "market", "direction"])["p_raw"].to_numpy())

    def test_predict_without_a_model_writes_nothing(self, world):
        assert hx.run(world["run_date"], world["cfg"], quality="pass") == 0

    def test_predict_schema_and_grid(self, world):
        hx.run_fit(world["run_date"], world["cfg"], world["labels_dir"])
        n = hx.run(world["run_date"], world["cfg"], quality="warn")
        assert n == 8
        out = pl.read_parquet(core.ModelStore(world["cfg"].output_root, hx.MODEL_NAME).predictions)
        for c in ("as_of_date", "market", "direction", "probability", "confidence", "quality", "regime_tag", "age_days",
                  "model_fit_date", "lineage"):
            assert c in out.columns, c
        assert out["quality"].unique().to_list() == ["warn"]
        assert ((out["probability"] >= 0) & (out["probability"] <= 1)).all()
        assert out["regime_tag"].unique().to_list() == ["expansion/risk_on"]
        assert out["lineage"][0].startswith("hmm_xgboost|fit=") and "label=tb1.0" in out["lineage"][0]
        assert out["age_days"].min() >= 1

    def test_predict_is_idempotent_per_date_and_keeps_other_dates(self, world):
        hx.run_fit(world["run_date"], world["cfg"], world["labels_dir"])
        d = world["run_date"]
        hx.run(d, world["cfg"], quality="pass")
        hx.run(d, world["cfg"], quality="pass")
        hx.run(d + timedelta(days=1), world["cfg"], quality="pass")
        out = pl.read_parquet(core.ModelStore(world["cfg"].output_root, hx.MODEL_NAME).predictions)
        assert out.height == 16 and out["as_of_date"].n_unique() == 2

    def test_a_model_fitted_after_the_scoring_date_is_refused(self, world):
        hx.run_fit(world["run_date"], world["cfg"], world["labels_dir"])
        with pytest.raises(core.LeakageError):
            hx.run(world["run_date"] - timedelta(days=30), world["cfg"], quality="pass")

    def test_long_scores_higher_than_short_in_a_positive_regime(self, world):
        """Sanity of direction handling: the synthetic world makes long win when market_score > 0."""
        hx.run_fit(world["run_date"], world["cfg"], world["labels_dir"])
        hx.run(world["run_date"], world["cfg"], quality="pass")
        out = pl.read_parquet(core.ModelStore(world["cfg"].output_root, hx.MODEL_NAME).predictions)
        p = {d: out.filter(pl.col("direction") == d)["probability"].mean() for d in ("long", "short")}
        ms = fh.regime_feature_table(*[pl.read_parquet(fh.MACRO_PATH), pl.read_parquet(fh.MARKET_PATH)]).tail(1)["market_score"][0]
        assert (p["long"] > p["short"]) == (ms > 0)

    def test_stale_regime_stores_write_nothing(self, world):
        hx.run_fit(world["run_date"], world["cfg"], world["labels_dir"])
        assert hx.run(world["run_date"] + timedelta(days=60), world["cfg"], quality="pass") == 0
