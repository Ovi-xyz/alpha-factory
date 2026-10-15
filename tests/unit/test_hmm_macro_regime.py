"""
tests/unit/test_hmm_macro_regime.py — ADR-051 (GMI-HMM2-001): src/gold/hmm/hmm_macro_regime.py.

The HMM mechanics live in hmm_core (tests/unit/test_hmm_core.py). This file pins what is
specific to the macro branch:
  * the §F business-cycle rule — strict inequalities at both thresholds, momentum flips,
    "inside the band the previous label carries", short_history domains excluded
  * the store contract — append-only (a stored row is NEVER rewritten, not by a re-run and not
    by a refit), schema drift raises, soft degrade with no model, honest-history in_sample
  * the module's public surface is unchanged by the move onto hmm_core (re-exports)
"""
import shutil
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest
import yaml
from loguru import logger

from src.gold.hmm import hmm_core as hc
from src.gold.hmm import hmm_macro_regime as hm
from src.gold.hmm import raw_macro
from src.gold.hmm.config import FRED_SERIES_PATH, MacroLabelRule, macro_model_config, validate_macro_model_config

RULE = MacroLabelRule(axes={"growth": 1, "liquidity": 1, "stress": -1}, required="growth",
                      activity_threshold=0.10, momentum_threshold=0.25, momentum_window_bd=2)


# ═══ the §F rule ═════════════════════════════════════════════════════════════

class TestLabelSequence:
    def _seq(self, act, mom=None, seed=None, rule=RULE):
        mom = mom if mom is not None else [None] * len(act)
        return hm.label_sequence(act, mom, rule, seed)

    def test_above_threshold_is_expansion_below_negative_is_recession(self):
        assert self._seq([0.5, -0.5]) == ["expansion", "recession"]

    def test_thresholds_are_strict(self):
        """Exactly +/- activity_threshold is inside the band: the previous label carries."""
        assert self._seq([0.10, -0.10], seed="slowdown") == ["slowdown", "slowdown"]
        assert self._seq([0.1000001, -0.1000001]) == ["expansion", "recession"]

    def test_inside_the_band_carries_the_previous_label_including_the_seed(self):
        assert self._seq([0.0, 0.05, -0.05], seed="recovery") == ["recovery"] * 3
        assert self._seq([0.0], seed=None) == [None]                            # nothing to carry yet

    def test_momentum_turns_expansion_into_slowdown_strictly(self):
        assert self._seq([0.5], [-0.25]) == ["expansion"]                       # exactly -m: not below
        assert self._seq([0.5], [-0.2500001]) == ["slowdown"]

    def test_momentum_turns_recession_into_recovery_strictly(self):
        assert self._seq([-0.5], [0.25]) == ["recession"]
        assert self._seq([-0.5], [0.2500001]) == ["recovery"]

    def test_momentum_does_not_act_inside_the_band_or_when_undefined(self):
        assert self._seq([0.0], [-9.0], seed="expansion") == ["expansion"]
        assert self._seq([0.5], [None]) == ["expansion"] and self._seq([-0.5], [None]) == ["recession"]

    def test_undefined_activity_carries_whether_none_or_nan(self):
        assert self._seq([0.5, None, float("nan")]) == ["expansion"] * 3

    def test_the_rule_is_sequential_each_row_sees_the_previous_label(self):
        assert self._seq([0.5, 0.0, -0.5, 0.0, 0.5]) == ["expansion", "expansion", "recession", "recession", "expansion"]


class TestActivity:
    def _df(self, rows):
        cols = {}
        for d in ("growth", "liquidity", "stress"):
            cols[f"{d}_score"] = [r.get(d, (None, False))[0] for r in rows]
            cols[f"{d}_short_history"] = [r.get(d, (None, False))[1] for r in rows]
        return pl.DataFrame(cols, schema={**{f"{d}_score": pl.Float64 for d in ("growth", "liquidity", "stress")},
                                          **{f"{d}_short_history": pl.Boolean for d in ("growth", "liquidity", "stress")}})

    def _act(self, rows):
        return self._df(rows).select(hm.activity_expr(RULE).alias("a"))["a"].to_list()

    def test_mean_of_oriented_scores_stress_enters_negated(self):
        a = self._act([{"growth": (0.6, False), "liquidity": (0.2, False), "stress": (0.8, False)}])
        assert a == [pytest.approx((0.6 + 0.2 - 0.8) / 3)]

    def test_short_history_domain_is_excluded_from_the_mean(self):
        a = self._act([{"growth": (0.6, False), "liquidity": (0.2, False), "stress": (0.8, True)}])
        assert a == [pytest.approx((0.6 + 0.2) / 2)]

    def test_required_domain_must_contribute(self):
        assert self._act([{"growth": (0.6, True), "liquidity": (0.2, False), "stress": (0.1, False)}]) == [None]
        assert self._act([{"growth": (None, False), "liquidity": (0.2, False)}]) == [None]

    def test_unscored_non_required_domain_is_skipped_not_zero(self):
        a = self._act([{"growth": (0.6, False)}])
        assert a == [pytest.approx(0.6)]


class TestAddMacroLabel:
    def _rows(self, dates, acts):
        data = {"date": dates}
        for d in ("growth", "liquidity", "stress"):
            data[f"{d}_score"] = [a if d == "growth" else None for a in acts]
            data[f"{d}_short_history"] = [False] * len(dates)
        return pl.DataFrame(data, schema={"date": pl.Date, **{f"{d}_score": pl.Float64 for d in ("growth", "liquidity", "stress")},
                                          **{f"{d}_short_history": pl.Boolean for d in ("growth", "liquidity", "stress")}})

    def test_momentum_looks_back_the_configured_weekdays_into_history_and_new_rows(self):
        hist = pl.DataFrame({"date": [date(2026, 3, 2), date(2026, 3, 3)], "macro_activity": [0.1, 0.2]})
        rows = self._rows([date(2026, 3, 4), date(2026, 3, 5), date(2026, 3, 6)], [0.5, 0.6, 0.9])
        out = hm.add_macro_label(rows, hist, None, RULE).sort("date")
        # window = 2 weekdays: 03-04 looks at 03-02 (hist 0.1); 03-05 at 03-03 (hist 0.2); 03-06 at 03-04 (new row 0.5)
        assert out["macro_momentum"].to_list() == [pytest.approx(0.4), pytest.approx(0.4), pytest.approx(0.4)]

    def test_missing_lookback_row_gives_null_momentum_and_no_flip(self):
        rows = self._rows([date(2026, 3, 4)], [0.5])
        out = hm.add_macro_label(rows, pl.DataFrame(schema={"date": pl.Date, "macro_activity": pl.Float64}), None, RULE)
        assert out["macro_momentum"].to_list() == [None] and out["macro_label"].to_list() == ["expansion"]

    def test_first_new_row_is_seeded_with_the_last_stored_label(self):
        rows = self._rows([date(2026, 3, 4)], [0.0])                              # inside the band
        out = hm.add_macro_label(rows, pl.DataFrame(schema={"date": pl.Date, "macro_activity": pl.Float64}), "recession", RULE)
        assert out["macro_label"].to_list() == ["recession"]


# ═══ module surface after the move onto hmm_core ═════════════════════════════

class TestSurface:
    @pytest.mark.parametrize("name,core_name", [
        ("fit_scaler", "fit_scaler"), ("apply_scaler", "apply_scaler"), ("fit_domain", "fit_domain"),
        ("forward_filter", "forward_filter"), ("fit_model", "fit_model"), ("domain_columns", "state_columns"),
        ("_empty_domain_frame", "empty_state_columns"), ("_atomic_write_text", "atomic_write_text"),
        ("Candidate", "Candidate"), ("_log_emission", "_log_emission"), ("_composite_init", "_composite_init"),
    ])
    def test_moved_names_are_the_core_objects(self, name, core_name):
        assert getattr(hm, name) is getattr(hc, core_name)

    def test_errors_and_constants_stay_local(self):
        assert issubclass(hm.MacroRegimeError, RuntimeError)
        assert (hm.RAW_KEY, hm.MODEL_SCHEMA, hm.PROCESSING_VERSION) == ("as_of_date", 1, "1.0")
        assert str(hm.MACRO_REGIME_PATH).endswith("data/gold/hmm/macro_regime.parquet")
        assert hm.model_path("growth").name == "macro_growth.json"


# ═══ the store ═══════════════════════════════════════════════════════════════

def _cfg(short_min=500):
    raw = yaml.safe_load(open(FRED_SERIES_PATH))
    raw["hmm"]["fit"]["short_history_min_obs"] = short_min
    return validate_macro_model_config(raw)


def _panel(n=700, seed=5, start=date(2018, 1, 1)):
    cfg = macro_model_config()
    rng = np.random.default_rng(seed)
    days, d = [], start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    st, s = [], 1
    for _ in range(n):
        if rng.random() < 0.02:
            s = int(rng.integers(0, 3))
        st.append(s)
    st = np.array(st)
    cols = {"as_of_date": days}
    for f in cfg.macro.features:
        w = cfg.weights[f.domain][f.feature]
        cols[f.feature] = ((st - 1) * (w if w != 0 else 0.3) * 1.5 + rng.normal(0, 1, n)).tolist()
    return pl.DataFrame(cols, schema={"as_of_date": pl.Date, **{f.feature: pl.Float64 for f in cfg.macro.features}})


FIT_DATE = date(2020, 3, 2)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """A fitted world, built once: models fitted on rows <= FIT_DATE, the store holding ALL rows."""
    root = tmp_path_factory.mktemp("macro_world")
    cfg, panel = _cfg(), _panel()
    old = (hm.MODELS_DIR, hm.MACRO_REGIME_PATH)
    hm.MODELS_DIR, hm.MACRO_REGIME_PATH = root / "models", root / "macro_regime.parquet"
    try:
        status = hm.fit_all(panel.filter(pl.col("as_of_date") <= FIT_DATE), FIT_DATE, cfg)
        added = hm.append_new_rows(panel, cfg, FIT_DATE)
    finally:
        hm.MODELS_DIR, hm.MACRO_REGIME_PATH = old
    return {"root": root, "cfg": cfg, "panel": panel, "status": status, "added": added}


@pytest.fixture
def world(built, tmp_path, monkeypatch):
    shutil.copytree(built["root"], tmp_path / "w")
    monkeypatch.setattr(hm, "MODELS_DIR", tmp_path / "w" / "models")
    monkeypatch.setattr(hm, "MACRO_REGIME_PATH", tmp_path / "w" / "macro_regime.parquet")
    return {**built, "tmp": tmp_path / "w"}


class TestFitAll:
    def test_every_domain_fitted_and_persisted(self, built):
        assert set(built["status"]) == set(built["cfg"].domains)
        assert all(s.startswith("fitted") for s in built["status"].values()), built["status"]
        assert sorted(p.name for p in (built["root"] / "models").iterdir()) == \
            sorted(f"macro_{d}.json" for d in built["cfg"].domains)

    def test_a_skipped_domain_keeps_its_existing_model(self, world):
        before = {p.name: p.read_bytes() for p in hm.MODELS_DIR.iterdir()}
        tiny = world["panel"].head(50)                                             # < min_obs_to_fit (250)
        st = hm.fit_all(tiny, FIT_DATE, world["cfg"])
        assert all(s.startswith("skipped") for s in st.values())
        assert {p.name: p.read_bytes() for p in hm.MODELS_DIR.iterdir()} == before

    def test_missing_feature_column_raises_naming_the_domain(self, world):
        with pytest.raises(hm.MacroRegimeError, match="raw_macro is missing monetary features"):
            hm.fit_all(world["panel"].drop("DFF"), FIT_DATE, world["cfg"])

    def test_model_file_carries_the_config_fingerprint(self, world):
        m = hm.load_model("growth")
        assert m["config_fingerprint"] == world["cfg"].fingerprint() and m["domain"] == "growth"

    def test_load_model_rejects_a_model_filed_under_the_wrong_domain(self, world):
        shutil.copy(hm.model_path("growth"), hm.model_path("stress"))
        with pytest.raises(hm.MacroRegimeError, match="unexpected model schema/domain"):
            hm.load_model("stress")


class TestStore:
    def test_initial_fill_covers_every_row_with_a_posterior(self, built):
        assert built["added"] == 700 and pl.read_parquet(built["root"] / "macro_regime.parquet").height == 700

    def test_schema_and_column_order(self, world):
        df = pl.read_parquet(hm.MACRO_REGIME_PATH)
        assert df.columns == hm._column_order(world["cfg"])
        assert df.schema["date"] == pl.Date and df.schema["in_sample"] == pl.Boolean
        assert df["date"].is_sorted() and df["date"].n_unique() == df.height

    def test_in_sample_marks_rows_up_to_the_oldest_training_end_only(self, world):
        df = pl.read_parquet(hm.MACRO_REGIME_PATH)
        ends = [date.fromisoformat(hm.load_model(d)["train_end"]) for d in world["cfg"].domains]
        # in_sample is True when the date is <= the training end of ANY contributing model
        assert df.filter(pl.col("date") <= min(ends))["in_sample"].all()
        assert not df.filter(pl.col("date") > max(ends))["in_sample"].any()
        assert df.filter(pl.col("date") > max(ends)).height > 100                  # a genuine out-of-sample tail exists

    def test_rerun_with_nothing_new_changes_nothing(self, world):
        before = hm.MACRO_REGIME_PATH.read_bytes()
        assert hm.append_new_rows(world["panel"], world["cfg"], FIT_DATE) == 0
        assert hm.MACRO_REGIME_PATH.read_bytes() == before

    def test_new_rows_are_appended_and_stored_rows_are_never_touched(self, world):
        stored = pl.read_parquet(hm.MACRO_REGIME_PATH)
        longer = _panel(n=760)                                                      # same seed => identical first 700 rows
        added = hm.append_new_rows(longer, world["cfg"], date(2020, 7, 1))
        after = pl.read_parquet(hm.MACRO_REGIME_PATH)
        assert added == 60 and after.height == 760
        assert after.head(700).drop("computation_date").equals(stored.drop("computation_date"))
        assert after.tail(60)["computation_date"].unique().to_list() == [date(2020, 7, 1)]

    def test_a_refit_never_rewrites_out_of_sample_history(self, world):
        stored = pl.read_parquet(hm.MACRO_REGIME_PATH)
        later = date(2020, 9, 1)
        hm.fit_all(world["panel"], later, world["cfg"])                            # refit on ALL rows
        hm.append_new_rows(world["panel"], world["cfg"], later)
        after = pl.read_parquet(hm.MACRO_REGIME_PATH)
        assert after.equals(stored)                                                 # insert-only: nothing replaced

    def test_a_stored_column_the_build_needs_is_missing_raises_cleanly(self, world):
        """Not a bare polars ColumnNotFoundError: the same 'cannot be migrated in place' contract."""
        pl.read_parquet(world["root"] / "macro_regime.parquet").drop("macro_label").write_parquet(hm.MACRO_REGIME_PATH)
        before = hm.MACRO_REGIME_PATH.read_bytes()
        with pytest.raises(hm.MacroRegimeError, match=r"lacks columns \['macro_label'\].*cannot be migrated in place"):
            hm.append_new_rows(_panel(n=760), world["cfg"], date(2020, 7, 1))
        assert hm.MACRO_REGIME_PATH.read_bytes() == before

    def test_schema_drift_raises_and_leaves_the_store_untouched(self, world):
        stale = pl.read_parquet(world["root"] / "macro_regime.parquet").with_columns(pl.lit(1).alias("legacy_extra"))
        stale.write_parquet(hm.MACRO_REGIME_PATH)
        before = hm.MACRO_REGIME_PATH.read_bytes()
        with pytest.raises(hm.MacroRegimeError, match="append-only and cannot be migrated in place"):
            hm.append_new_rows(_panel(n=760), world["cfg"], date(2020, 7, 1))
        assert hm.MACRO_REGIME_PATH.read_bytes() == before

    def test_macro_label_is_populated_once_domains_have_enough_history(self, world):
        df = pl.read_parquet(hm.MACRO_REGIME_PATH)
        assert df["macro_label"].drop_nulls().unique().to_list()                    # short_history_min_obs=500 < 700
        assert set(df["macro_label"].drop_nulls().unique()) <= {"expansion", "slowdown", "recession", "recovery"}

    def test_short_history_domains_are_excluded_from_the_label(self, tmp_path, monkeypatch):
        """With the PRODUCTION short_history_min_obs (2000) a 700-row world has every domain flagged
        short, so growth (the required domain) cannot contribute: no label at all, by design."""
        monkeypatch.setattr(hm, "MODELS_DIR", tmp_path / "m")
        monkeypatch.setattr(hm, "MACRO_REGIME_PATH", tmp_path / "s.parquet")
        cfg, panel = _cfg(short_min=2000), _panel()
        hm.fit_all(panel.filter(pl.col("as_of_date") <= FIT_DATE), FIT_DATE, cfg)
        hm.append_new_rows(panel, cfg, FIT_DATE)
        df = pl.read_parquet(hm.MACRO_REGIME_PATH)
        assert df["growth_short_history"].drop_nulls().all()
        assert df["macro_label"].null_count() == df.height and df["macro_activity"].null_count() == df.height

    def test_no_model_at_all_writes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hm, "MODELS_DIR", tmp_path / "none")
        monkeypatch.setattr(hm, "MACRO_REGIME_PATH", tmp_path / "s.parquet")
        msgs = []
        sink = logger.add(lambda m: msgs.append(str(m)), level="WARNING")
        try:
            assert hm.append_new_rows(_panel(), _cfg(), FIT_DATE) == 0
        finally:
            logger.remove(sink)
        assert not hm.MACRO_REGIME_PATH.exists() and any("no fitted model for any domain" in m for m in msgs)

    def test_a_domain_without_a_model_keeps_null_columns_but_the_rest_score(self, world):
        hm.model_path("stress").unlink()
        hm.MACRO_REGIME_PATH.unlink()
        hm.append_new_rows(world["panel"], world["cfg"], FIT_DATE)
        df = pl.read_parquet(hm.MACRO_REGIME_PATH)
        assert df["stress_label"].null_count() == df.height and df["growth_label"].null_count() == 0

    def test_a_config_changed_since_the_fit_warns_but_still_scores(self, world):
        other = _cfg(short_min=501)                                                 # different fingerprint
        assert other.fingerprint() != world["cfg"].fingerprint()
        hm.MACRO_REGIME_PATH.unlink()
        msgs = []
        sink = logger.add(lambda m: msgs.append(str(m)), level="WARNING")
        try:
            assert hm.append_new_rows(world["panel"], other, FIT_DATE) == 700
        finally:
            logger.remove(sink)
        assert any("the weekly fit will realign it" in m for m in msgs)

    def test_rows_inherit_the_models_own_fingerprint(self, world):
        df = pl.read_parquet(hm.MACRO_REGIME_PATH)
        assert df["model_config_fingerprint"].unique().to_list() == [world["cfg"].fingerprint()]


# ═══ job entry points ════════════════════════════════════════════════════════

class TestJobs:
    @pytest.fixture
    def jobs(self, tmp_path, monkeypatch):
        panel = _panel()
        raw = tmp_path / "hmm" / "raw_macro.parquet"
        raw.parent.mkdir()
        panel.write_parquet(raw)
        monkeypatch.setattr(raw_macro, "RAW_MACRO_PATH", raw)
        monkeypatch.setattr(hm, "MODELS_DIR", tmp_path / "hmm" / "models")
        monkeypatch.setattr(hm, "MACRO_REGIME_PATH", tmp_path / "hmm" / "macro_regime.parquet")
        return panel

    def test_daily_job_with_no_model_is_a_soft_no_op(self, jobs):
        hm.run(FIT_DATE)
        assert not hm.MACRO_REGIME_PATH.exists()

    def test_fit_job_creates_models_and_the_store_then_the_daily_job_is_current(self, jobs):
        hm.run_fit(FIT_DATE)
        assert hm.MACRO_REGIME_PATH.exists() and len(list(hm.MODELS_DIR.iterdir())) == 5
        n = pl.read_parquet(hm.MACRO_REGIME_PATH).height
        hm.run(FIT_DATE)
        assert pl.read_parquet(hm.MACRO_REGIME_PATH).height == n

    def test_panel_is_bounded_by_the_run_date(self, jobs):
        hm.run_fit(FIT_DATE)
        assert pl.read_parquet(hm.MACRO_REGIME_PATH)["date"].max() <= FIT_DATE

    def test_missing_raw_macro_names_the_job_to_run(self, jobs, monkeypatch):
        monkeypatch.setattr(raw_macro, "RAW_MACRO_PATH", hm.MODELS_DIR.parent / "absent.parquet")
        with pytest.raises(hm.MacroRegimeError, match="run gold_raw_macro first"):
            hm.run(FIT_DATE)

    def test_run_date_before_any_row_raises(self, jobs):
        with pytest.raises(hm.MacroRegimeError, match="no rows on or before"):
            hm.run(date(2017, 1, 1))
