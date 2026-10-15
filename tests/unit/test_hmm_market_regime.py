"""
tests/unit/test_hmm_market_regime.py — ADR-051 (GMI-HMM2-001): src/gold/hmm/hmm_market_regime.py.

The HMM mechanics live in hmm_core (tests/unit/test_hmm_core.py). This file pins what is specific to
the market branch:
  * the fitted model orders its states risk_off < mixed < risk_on along the signed weights and
    recovers a known latent chain (the same engine as macro, a different routing)
  * the store contract — append-only (a stored row is NEVER rewritten, not by a re-run and not by a
    refit), schema drift raises, soft degrade with no model, a foreign model file raises
  * honest history — in_sample, market_short_history, and n_features_observed (a row whose breadth
    was nulled by the guard is scored on fewer features, visibly)
"""
import json
import shutil
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest
import yaml
from loguru import logger

from src.gold.hmm import hmm_market_regime as hk
from src.gold.hmm import raw_market
from src.gold.hmm.config import FRED_SERIES_PATH, market_model_config, validate_market_model_config

FIT_DATE = date(2023, 6, 1)
LABELS = ["risk_off", "mixed", "risk_on"]
BREADTH = ["us_share_up", "us_median_ret", "us_dispersion", "us_median_ret_rms"]


def _cfg(short_min=2000):
    raw = yaml.safe_load(open(FRED_SERIES_PATH))
    raw["hmm_market"]["fit"]["short_history_min_obs"] = short_min
    return validate_market_model_config(raw)


def _panel(n=1200, seed=3, shift=1.6):
    """Sticky latent chain 0=risk_off,1=mixed,2=risk_on; each feature's mean follows the sign of its
    ordering weight (zero-weight features get a mild state dependence so they are informative but
    unordered). Returns (panel, latent states)."""
    cfg = market_model_config()
    rng = np.random.default_rng(seed)
    days, d = [], date(2019, 1, 2)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    st, s = [], 1
    for _ in range(n):
        if rng.random() < 0.03:
            s = int(rng.integers(0, 3))
        st.append(s)
    st = np.array(st)
    cols = {"as_of_date": days}
    for f in cfg.market.features:
        w = cfg.weights[f.feature]
        cols[f.feature] = ((st - 1) * (np.sign(w) if w != 0 else 0.4) * shift + rng.normal(0, 1, n)).tolist()
    schema = {"as_of_date": pl.Date, **{f.feature: pl.Float64 for f in cfg.market.features}}
    return pl.DataFrame(cols, schema=schema), st


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """A fitted world, built once: model fitted on rows <= FIT_DATE, the store holding ALL rows."""
    root = tmp_path_factory.mktemp("market_world")
    cfg = _cfg()
    panel, st = _panel()
    old = (hk.MODELS_DIR, hk.MARKET_REGIME_PATH)
    hk.MODELS_DIR, hk.MARKET_REGIME_PATH = root / "models", root / "market_regime.parquet"
    try:
        status = hk.fit(panel.filter(pl.col("as_of_date") <= FIT_DATE), FIT_DATE, cfg)
        added = hk.append_new_rows(panel, cfg, FIT_DATE)
    finally:
        hk.MODELS_DIR, hk.MARKET_REGIME_PATH = old
    return {"root": root, "cfg": cfg, "panel": panel, "latent": st, "status": status, "added": added}


@pytest.fixture
def world(built, tmp_path, monkeypatch):
    shutil.copytree(built["root"], tmp_path / "w")
    monkeypatch.setattr(hk, "MODELS_DIR", tmp_path / "w" / "models")
    monkeypatch.setattr(hk, "MARKET_REGIME_PATH", tmp_path / "w" / "market_regime.parquet")
    return {**built, "tmp": tmp_path / "w"}


class TestFit:
    def test_fitted_and_persisted_under_the_market_name(self, built):
        assert built["status"].startswith("fitted: n=")
        assert [p.name for p in (built["root"] / "models").iterdir()] == ["market.json"]

    def test_model_file_contents(self, world):
        m = hk.load_model()
        cfg = world["cfg"]
        assert m["domain"] == "market" and m["schema"] == 1 and m["labels"] == LABELS
        assert m["features"] == cfg.feature_names() and m["weights"] == [cfg.weights[f] for f in cfg.feature_names()]
        assert m["config_fingerprint"] == cfg.fingerprint() and m["fit_date"] == FIT_DATE.isoformat()
        assert m["processing_version"] == "1.0" and m["train_end"] <= FIT_DATE.isoformat()

    def test_states_are_ordered_risk_off_first_along_the_signed_weights(self, world):
        m = hk.load_model()
        comp = np.array(m["means"]) @ np.array(m["weights"])
        assert list(np.argsort(comp)) == [0, 1, 2]
        i = world["cfg"].feature_names().index
        means = np.array(m["means"])
        assert means[0, i("VIXCLS")] > means[2, i("VIXCLS")]                      # risk_off: high VIX
        assert means[0, i("us_share_up")] < means[2, i("us_share_up")]            # risk_off: narrow breadth

    def test_recovers_the_latent_chain_and_labels_it_correctly(self, world):
        df = pl.read_parquet(hk.MARKET_REGIME_PATH)
        pred = np.array([LABELS.index(l) for l in df["market_label"]])
        assert (pred == world["latent"][-len(pred):]).mean() > 0.97

    def test_short_history_follows_the_production_threshold(self, world):
        assert hk.load_model()["short_history"] is True                           # 1200-row world < 2000

    def test_not_short_history_once_the_threshold_is_met(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hk, "MODELS_DIR", tmp_path / "m")
        panel, _ = _panel()
        hk.fit(panel, FIT_DATE, _cfg(short_min=500))
        assert hk.load_model()["short_history"] is False

    def test_too_few_rows_is_skipped_and_keeps_the_existing_model(self, world):
        before = hk.model_path().read_bytes()
        status = hk.fit(world["panel"].head(50), FIT_DATE, world["cfg"])
        assert status.startswith("skipped: 50 complete rows < min_obs_to_fit 250")
        assert hk.model_path().read_bytes() == before

    def test_missing_feature_column_raises(self, world):
        with pytest.raises(hk.MarketRegimeError, match="raw_market is missing market features"):
            hk.fit(world["panel"].drop("VIXCLS"), FIT_DATE, world["cfg"])

    def test_only_complete_case_rows_train(self, tmp_path, monkeypatch):
        """`fit` trains on every row of the panel it is handed (the date bound is applied by the job's
        panel loader) that has ALL features: 40 rows with one null feature are out."""
        monkeypatch.setattr(hk, "MODELS_DIR", tmp_path / "m")
        panel, _ = _panel()
        panel = panel.with_columns(
            pl.when(pl.int_range(pl.len()) < 40).then(None).otherwise(pl.col("us_share_up")).alias("us_share_up"))
        hk.fit(panel, FIT_DATE, _cfg())
        assert hk.load_model()["n_obs"] == 1160

    def test_load_model_rejects_a_foreign_model_file(self, world):
        m = json.loads(hk.model_path().read_text())
        m["domain"] = "growth"
        hk.model_path().write_text(json.dumps(m))
        with pytest.raises(hk.MarketRegimeError, match="unexpected model schema/domain"):
            hk.load_model()


class TestRows:
    def test_columns_dtypes_and_order(self, world):
        df = pl.read_parquet(hk.MARKET_REGIME_PATH)
        assert df.columns == hk._column_order(world["cfg"])
        assert df.schema["date"] == pl.Date and df.schema["n_features_observed"] == pl.Int32
        assert df.schema["in_sample"] == pl.Boolean and df.schema["market_label"] == pl.String
        assert df["date"].is_sorted() and df["date"].n_unique() == df.height

    def test_posteriors_probability_and_score_are_consistent(self, world):
        df = pl.read_parquet(hk.MARKET_REGIME_PATH)
        post = np.column_stack([df[f"market_post_{l}"].to_numpy() for l in LABELS])
        assert np.allclose(post.sum(axis=1), 1.0)
        assert np.allclose(df["market_probability"].to_numpy(), post.max(axis=1))
        assert np.allclose(df["market_score"].to_numpy(), post @ np.array([-1.0, 0.0, 1.0]))
        assert df["market_label"].to_list() == [LABELS[i] for i in post.argmax(axis=1)]

    def test_fully_observed_rows_report_all_ten_features(self, world):
        assert pl.read_parquet(hk.MARKET_REGIME_PATH)["n_features_observed"].unique().to_list() == [10]

    def test_a_nulled_breadth_row_is_still_scored_on_fewer_features_and_says_so(self, world):
        """The breadth guard nulls the four US-stock features on a thin/partial day. The filter
        marginalises them exactly, so the row is scored — on 6 features — and n_features_observed shows it."""
        panel = world["panel"].with_columns([
            pl.when(pl.col("as_of_date") == world["panel"]["as_of_date"][-1]).then(None).otherwise(pl.col(c)).alias(c)
            for c in BREADTH])
        hk.MARKET_REGIME_PATH.unlink()
        hk.append_new_rows(panel, world["cfg"], FIT_DATE)
        df = pl.read_parquet(hk.MARKET_REGIME_PATH)
        last = df.tail(1).row(0, named=True)
        assert last["n_features_observed"] == 6 and last["market_label"] in LABELS
        assert df.head(df.height - 1)["n_features_observed"].unique().to_list() == [10]

    def test_a_row_with_nothing_observed_is_a_prediction_step_not_a_gap(self, world):
        panel = world["panel"].with_columns([
            pl.when(pl.col("as_of_date") == world["panel"]["as_of_date"][-1]).then(None).otherwise(pl.col(f)).alias(f)
            for f in world["cfg"].feature_names()])
        hk.MARKET_REGIME_PATH.unlink()
        hk.append_new_rows(panel, world["cfg"], FIT_DATE)
        last = pl.read_parquet(hk.MARKET_REGIME_PATH).tail(1).row(0, named=True)
        assert last["n_features_observed"] == 0 and last["market_label"] in LABELS

    def test_rows_before_the_first_observation_are_not_written(self, world):
        head = pl.DataFrame({"as_of_date": [date(2018, 12, 31)], **{f: [None] for f in world["cfg"].feature_names()}},
                            schema=world["panel"].schema)
        hk.MARKET_REGIME_PATH.unlink()
        hk.append_new_rows(pl.concat([head, world["panel"]]), world["cfg"], FIT_DATE)
        df = pl.read_parquet(hk.MARKET_REGIME_PATH)
        assert df["date"].min() == date(2019, 1, 2) and df.height == 1200


class TestStore:
    def test_initial_fill_covers_every_scored_row(self, built):
        assert built["added"] == 1200

    def test_in_sample_is_true_exactly_up_to_the_training_end(self, world):
        df = pl.read_parquet(hk.MARKET_REGIME_PATH)
        end = date.fromisoformat(hk.load_model()["train_end"])
        assert df.filter(pl.col("date") <= end)["in_sample"].all()
        assert not df.filter(pl.col("date") > end)["in_sample"].any()
        assert df.filter(pl.col("date") > end).height > 40                        # a genuine out-of-sample tail exists

    def test_rerun_with_nothing_new_changes_nothing(self, world):
        before = hk.MARKET_REGIME_PATH.read_bytes()
        assert hk.append_new_rows(world["panel"], world["cfg"], FIT_DATE) == 0
        assert hk.MARKET_REGIME_PATH.read_bytes() == before

    def test_new_rows_are_appended_and_stored_rows_are_never_touched(self, world):
        stored = pl.read_parquet(hk.MARKET_REGIME_PATH)
        longer, _ = _panel(n=1260)                                                 # same seed -> identical first 1200 rows
        added = hk.append_new_rows(longer, world["cfg"], date(2023, 9, 1))
        after = pl.read_parquet(hk.MARKET_REGIME_PATH)
        assert added == 60 and after.height == 1260
        assert after.head(1200).drop("computation_date").equals(stored.drop("computation_date"))
        assert after.tail(60)["computation_date"].unique().to_list() == [date(2023, 9, 1)]

    def test_a_refit_never_rewrites_out_of_sample_history(self, world):
        stored = pl.read_parquet(hk.MARKET_REGIME_PATH)
        later = date(2023, 12, 1)
        hk.fit(world["panel"], later, world["cfg"])                                # refit on ALL rows
        hk.append_new_rows(world["panel"], world["cfg"], later)
        assert pl.read_parquet(hk.MARKET_REGIME_PATH).equals(stored)               # insert-only: nothing replaced

    def test_schema_drift_raises_and_leaves_the_store_untouched(self, world):
        pl.read_parquet(hk.MARKET_REGIME_PATH).with_columns(pl.lit(1).alias("legacy_extra")).write_parquet(hk.MARKET_REGIME_PATH)
        before = hk.MARKET_REGIME_PATH.read_bytes()
        longer, _ = _panel(n=1260)
        with pytest.raises(hk.MarketRegimeError, match="append-only and cannot be migrated in place"):
            hk.append_new_rows(longer, world["cfg"], date(2023, 9, 1))
        assert hk.MARKET_REGIME_PATH.read_bytes() == before

    def test_no_model_writes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hk, "MODELS_DIR", tmp_path / "none")
        monkeypatch.setattr(hk, "MARKET_REGIME_PATH", tmp_path / "s.parquet")
        msgs = []
        sink = logger.add(lambda m: msgs.append(str(m)), level="WARNING")
        try:
            assert hk.append_new_rows(_panel()[0], _cfg(), FIT_DATE) == 0
        finally:
            logger.remove(sink)
        assert not hk.MARKET_REGIME_PATH.exists() and any("no fitted model" in m for m in msgs)

    def test_a_config_changed_since_the_fit_warns_but_still_scores(self, world):
        other = _cfg(short_min=501)
        assert other.fingerprint() != world["cfg"].fingerprint()
        hk.MARKET_REGIME_PATH.unlink()
        msgs = []
        sink = logger.add(lambda m: msgs.append(str(m)), level="WARNING")
        try:
            assert hk.append_new_rows(world["panel"], other, FIT_DATE) == 1200
        finally:
            logger.remove(sink)
        assert any("the weekly fit will realign it" in m for m in msgs)

    def test_rows_inherit_the_models_own_fingerprint_not_the_current_configs(self, world):
        hk.MARKET_REGIME_PATH.unlink()
        hk.append_new_rows(world["panel"], _cfg(short_min=501), FIT_DATE)
        assert pl.read_parquet(hk.MARKET_REGIME_PATH)["model_config_fingerprint"].unique().to_list() == [world["cfg"].fingerprint()]

    def test_panel_missing_a_model_feature_raises(self, world):
        with pytest.raises(hk.MarketRegimeError, match="raw_market is missing market features"):
            hk.append_new_rows(world["panel"].drop("DGS5_D1"), world["cfg"], FIT_DATE)


class TestJobs:
    @pytest.fixture
    def jobs(self, tmp_path, monkeypatch):
        panel, _ = _panel()
        raw = tmp_path / "hmm" / "raw_market.parquet"
        raw.parent.mkdir()
        panel.write_parquet(raw)
        monkeypatch.setattr(raw_market, "RAW_MARKET_PATH", raw)
        monkeypatch.setattr(hk, "MODELS_DIR", tmp_path / "hmm" / "models")
        monkeypatch.setattr(hk, "MARKET_REGIME_PATH", tmp_path / "hmm" / "market_regime.parquet")
        return panel

    def test_daily_job_with_no_model_is_a_soft_no_op(self, jobs):
        hk.run(FIT_DATE)
        assert not hk.MARKET_REGIME_PATH.exists()

    def test_fit_job_creates_the_model_and_the_store_then_the_daily_job_is_current(self, jobs):
        hk.run_fit(FIT_DATE)
        assert hk.model_path().exists() and hk.MARKET_REGIME_PATH.exists()
        n = pl.read_parquet(hk.MARKET_REGIME_PATH).height
        hk.run(FIT_DATE)
        assert pl.read_parquet(hk.MARKET_REGIME_PATH).height == n

    def test_panel_is_bounded_by_the_run_date(self, jobs):
        hk.run_fit(FIT_DATE)
        assert pl.read_parquet(hk.MARKET_REGIME_PATH)["date"].max() <= FIT_DATE

    def test_missing_raw_market_names_the_job_to_run(self, jobs, monkeypatch):
        monkeypatch.setattr(raw_market, "RAW_MARKET_PATH", hk.MODELS_DIR.parent / "absent.parquet")
        with pytest.raises(hk.MarketRegimeError, match="run gold_raw_market first"):
            hk.run(FIT_DATE)

    def test_run_date_before_any_row_raises(self, jobs):
        with pytest.raises(hk.MarketRegimeError, match="no rows on or before"):
            hk.run(date(2018, 1, 1))


class TestIndependence:
    def test_module_reads_no_gold_store_of_another_domain(self):
        import ast, pathlib
        src = pathlib.Path(hk.__file__).read_text()
        for bad in ("global_regime", "cross_asset", "gold_global_regime", "technical_signals", "mtf_alignment"):
            assert bad not in "\n".join(l for l in src.split("\n") if l.lstrip().startswith(("import", "from")))
        tree = ast.parse(src)
        mods = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert mods <= {"__future__", "datetime", "pathlib", "typing", "numpy", "polars", "loguru",
                        "src.gold.hmm", "src.gold.hmm.config", "src.gold.hmm.hmm_core"}, mods
