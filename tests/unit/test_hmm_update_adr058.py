"""
tests/unit/test_hmm_update_adr058.py — ADR-058 (proposed; hmm_update_implementation_guide.md).

Pins the three work packages that change code on the live path:

  HMM-R01  catch-up safety. One classify call scores the WHOLE gap after the last stored date with the
           persisted model (in_sample = False); a fit that runs first would store the gap in-sample for
           good, because the store is insert-only. run_fit therefore refuses while the store lacks panel
           dates; the only override is HMM_ALLOW_REGIME_GAP=1 and it is logged.
  HMM-R02  provenance. `model_train_end` on both stores, `<domain>_age_days` on the macro store, and an
           ADDITIVE migration of a legacy (1.0) store: legacy rows keep every stored value, gain NULLs.
  HMM-R03  the daily FRED slice of silver_macro, and the fix that makes a same-day re-run idempotent
           (revision detection must compare against an EARLIER snapshot, never the run's own output).

The two "world" fixtures fit a model on the head of a synthetic panel and store those rows, so a
longer panel afterwards is exactly the state "classify has not run for the last N days".
"""
import shutil
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest
from loguru import logger

from src.gold.hmm import hmm_core as hc
from src.gold.hmm import hmm_macro_regime as hm
from src.gold.hmm import hmm_market_regime as hk
from src.gold.hmm import raw_macro, raw_market
from src.gold.hmm.config import macro_model_config, market_model_config
from tests.unit.test_hmm_macro_regime import _panel as macro_panel
from tests.unit.test_hmm_market_regime import _panel as market_panel

GAP = 10                                        # weekdays the classify job "missed"


def _warnings():
    msgs = []
    sink = logger.add(lambda m: msgs.append(str(m)), level="WARNING")
    return msgs, sink


# ═══ hmm_core: additive widening ═════════════════════════════════════════════

class _Err(RuntimeError):
    pass


def _rows(days, v=1.0, extra=None):
    cols = {"date": days, "v": [v] * len(days)}
    schema = {"date": pl.Date, "v": pl.Float64}
    if extra is not None:
        cols["extra"], schema["extra"] = [extra] * len(days), pl.Date
    return pl.DataFrame(cols, schema=schema)


class TestAdditiveWidening:
    def test_a_declared_new_column_is_added_as_null_and_stored_values_are_untouched(self, tmp_path):
        p = tmp_path / "s.parquet"
        hc.append_store(None, _rows([date(2026, 1, 1), date(2026, 1, 2)], v=7.0), p, _Err)
        legacy = pl.read_parquet(p)
        n = hc.append_store(legacy, _rows([date(2026, 1, 5)], v=9.0, extra=date(2026, 1, 4)), p, _Err,
                            additive=("extra",))
        after = pl.read_parquet(p)
        assert n == 1 and after.columns == ["date", "v", "extra"]
        assert after.head(2).drop("extra").equals(legacy)                       # every stored value identical
        assert after["extra"].to_list() == [None, None, date(2026, 1, 4)]       # legacy rows: NULL, not invented

    def test_an_undeclared_new_column_still_raises_and_leaves_the_file_untouched(self, tmp_path):
        p = tmp_path / "s.parquet"
        hc.append_store(None, _rows([date(2026, 1, 1)]), p, _Err)
        before = p.read_bytes()
        with pytest.raises(_Err, match="append-only and cannot be migrated in place"):
            hc.append_store(pl.read_parquet(p), _rows([date(2026, 1, 2)], extra=date(2026, 1, 1)), p, _Err,
                            additive=("something_else",))
        assert p.read_bytes() == before

    def test_a_declared_column_with_a_changed_dtype_on_a_stored_column_still_raises(self, tmp_path):
        p = tmp_path / "s.parquet"
        hc.append_store(None, _rows([date(2026, 1, 1)]), p, _Err)
        bad = _rows([date(2026, 1, 2)], extra=date(2026, 1, 1)).with_columns(pl.col("v").cast(pl.Float32))
        with pytest.raises(_Err):
            hc.append_store(pl.read_parquet(p), bad, p, _Err, additive=("extra",))

    def test_a_stored_column_the_build_dropped_is_not_widened_around(self, tmp_path):
        """additive only ever ADDS: a store with a column the new rows lack is a different schema."""
        p = tmp_path / "s.parquet"
        hc.append_store(None, _rows([date(2026, 1, 1)], extra=date(2026, 1, 1)), p, _Err)
        with pytest.raises(_Err):
            hc.append_store(pl.read_parquet(p), _rows([date(2026, 1, 2)]), p, _Err, additive=("extra",))

    def test_widening_is_logged(self, tmp_path):
        p = tmp_path / "s.parquet"
        hc.append_store(None, _rows([date(2026, 1, 1)]), p, _Err)
        msgs, sink = _warnings()
        try:
            hc.append_store(pl.read_parquet(p), _rows([date(2026, 1, 2)], extra=date(2026, 1, 1)), p, _Err,
                            additive=("extra",))
        finally:
            logger.remove(sink)
        assert any("widened additively" in m and "extra" in m for m in msgs)


class TestUnscoredGap:
    def _write(self, tmp_path, store_days, panel_days):
        sp, pp = tmp_path / "store.parquet", tmp_path / "panel.parquet"
        pl.DataFrame({"date": store_days}, schema={"date": pl.Date}).write_parquet(sp)
        pl.DataFrame({"as_of_date": panel_days}, schema={"as_of_date": pl.Date}).write_parquet(pp)
        return sp, pp

    def test_returns_panel_dates_after_the_stored_maximum_up_to_run_date(self, tmp_path):
        sp, pp = self._write(tmp_path, [date(2026, 1, 1), date(2026, 1, 2)],
                             [date(2026, 1, d) for d in (1, 2, 5, 6, 7)])
        assert hc.unscored_gap(sp, pp, "as_of_date", date(2026, 1, 6)) == [date(2026, 1, 5), date(2026, 1, 6)]

    def test_empty_when_the_store_is_current_or_either_file_is_absent(self, tmp_path):
        sp, pp = self._write(tmp_path, [date(2026, 1, 2)], [date(2026, 1, 1), date(2026, 1, 2)])
        assert hc.unscored_gap(sp, pp, "as_of_date", date(2026, 1, 9)) == []
        assert hc.unscored_gap(tmp_path / "none.parquet", pp, "as_of_date", date(2026, 1, 9)) == []
        assert hc.unscored_gap(sp, tmp_path / "none.parquet", "as_of_date", date(2026, 1, 9)) == []

    def test_an_empty_store_is_a_first_fit_not_a_gap(self, tmp_path):
        sp, pp = self._write(tmp_path, [], [date(2026, 1, 1)])
        assert hc.unscored_gap(sp, pp, "as_of_date", date(2026, 1, 9)) == []

    def test_enforce_raises_names_the_classify_job_and_the_override(self):
        with pytest.raises(_Err, match=r"2 panel date.*gold_hmm_x_regime.*HMM_ALLOW_REGIME_GAP=1"):
            hc.enforce_gap_guard([date(2026, 1, 5), date(2026, 1, 6)], "t", "gold_hmm_x_regime", _Err)

    def test_enforce_is_silent_without_a_gap(self):
        hc.enforce_gap_guard([], "t", "gold_hmm_x_regime", _Err)

    def test_only_the_literal_value_1_overrides(self, monkeypatch):
        for v in ("0", "true", "yes", ""):
            monkeypatch.setenv(hc.ALLOW_GAP_ENV, v)
            with pytest.raises(_Err):
                hc.enforce_gap_guard([date(2026, 1, 5)], "t", "j", _Err)


# ═══ market branch ═══════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def market_built(tmp_path_factory):
    """Model fitted on rows [0, N-GAP) and stored in-sample; the FULL panel is longer by GAP rows."""
    root = tmp_path_factory.mktemp("adr058_market")
    cfg = market_model_config()
    panel, _ = market_panel(n=1200)
    head = panel.head(1200 - GAP)
    old = (hk.MODELS_DIR, hk.MARKET_REGIME_PATH)
    hk.MODELS_DIR, hk.MARKET_REGIME_PATH = root / "models", root / "market_regime.parquet"
    try:
        hk.fit(head, head["as_of_date"][-1], cfg)
        hk.append_new_rows(head, cfg, head["as_of_date"][-1])
    finally:
        hk.MODELS_DIR, hk.MARKET_REGIME_PATH = old
    return {"root": root, "cfg": cfg, "panel": panel, "head": head}


@pytest.fixture
def mkt(market_built, tmp_path, monkeypatch):
    shutil.copytree(market_built["root"], tmp_path / "w")
    monkeypatch.setattr(hk, "MODELS_DIR", tmp_path / "w" / "models")
    monkeypatch.setattr(hk, "MARKET_REGIME_PATH", tmp_path / "w" / "market_regime.parquet")
    monkeypatch.setattr(raw_market, "RAW_MARKET_PATH", tmp_path / "w" / "raw_market.parquet")
    market_built["panel"].write_parquet(raw_market.RAW_MARKET_PATH)           # raw has GAP rows the store lacks
    return {**market_built, "run_date": market_built["panel"]["as_of_date"][-1]}


class TestMarketGapGuard:
    def test_the_gap_is_exactly_the_unscored_panel_dates(self, mkt):
        gap = hk.gap_dates(mkt["run_date"])
        assert len(gap) == GAP and gap == mkt["panel"]["as_of_date"].tail(GAP).to_list()

    def test_fit_first_is_refused_and_touches_nothing(self, mkt):
        store, model = hk.MARKET_REGIME_PATH.read_bytes(), hk.model_path().read_bytes()
        with pytest.raises(hk.MarketRegimeGapError, match="gold_hmm_market_regime"):
            hk.run_fit(mkt["run_date"])
        assert hk.MARKET_REGIME_PATH.read_bytes() == store and hk.model_path().read_bytes() == model

    def test_the_refusal_is_a_market_regime_error_the_job_wrapper_already_handles(self):
        assert issubclass(hk.MarketRegimeGapError, hk.MarketRegimeError)

    def test_classify_then_fit_stores_the_gap_out_of_sample(self, mkt):
        hk.run(mkt["run_date"])
        df = pl.read_parquet(hk.MARKET_REGIME_PATH)
        gap_rows = df.tail(GAP)
        assert df.height == 1200 and gap_rows["in_sample"].to_list() == [False] * GAP
        assert (gap_rows["model_train_end"] < gap_rows["date"]).all()
        assert hk.gap_dates(mkt["run_date"]) == []
        before = df
        hk.run_fit(mkt["run_date"])                                              # no longer refused
        assert pl.read_parquet(hk.MARKET_REGIME_PATH).equals(before)             # insert-only: the refit changes no stored row

    def test_the_override_runs_the_fit_logs_it_and_the_gap_becomes_in_sample_for_good(self, mkt, monkeypatch):
        monkeypatch.setenv(hc.ALLOW_GAP_ENV, "1")
        msgs, sink = _warnings()
        try:
            hk.run_fit(mkt["run_date"])
        finally:
            logger.remove(sink)
        assert any("gap guard OVERRIDDEN" in m for m in msgs)
        gap_rows = pl.read_parquet(hk.MARKET_REGIME_PATH).tail(GAP)
        assert gap_rows["in_sample"].to_list() == [True] * GAP                   # the hazard the guard exists for
        assert (gap_rows["model_train_end"] >= gap_rows["date"]).all()

    def test_no_store_or_no_model_is_not_a_gap(self, mkt, monkeypatch, tmp_path):
        monkeypatch.setattr(hk, "MARKET_REGIME_PATH", tmp_path / "absent.parquet")
        assert hk.gap_dates(mkt["run_date"]) == []                                # first fit fills history
        monkeypatch.setattr(hk, "MODELS_DIR", tmp_path / "no_models")
        assert hk.gap_dates(mkt["run_date"]) == []                                # nothing could score the gap

    def test_dates_after_the_run_date_are_not_counted(self, mkt):
        assert len(hk.gap_dates(mkt["panel"]["as_of_date"][-6])) == GAP - 5


class TestMarketProvenance:
    def test_every_row_carries_its_models_training_end(self, mkt):
        hk.run(mkt["run_date"])
        df = pl.read_parquet(hk.MARKET_REGIME_PATH)
        assert df["model_train_end"].null_count() == 0
        assert df["model_train_end"].unique().to_list() == [date.fromisoformat(hk.load_model()["train_end"])]
        assert df.schema["model_train_end"] == pl.Date and df.columns == hk._column_order(mkt["cfg"])
        assert df["processing_version"].unique().to_list() == ["1.1"]

    def test_invariant_no_out_of_sample_row_was_scored_by_a_model_that_saw_it(self, mkt):
        hk.run(mkt["run_date"])
        df = pl.read_parquet(hk.MARKET_REGIME_PATH)
        assert df.filter(~pl.col("in_sample") & (pl.col("model_train_end") >= pl.col("date"))).height == 0
        assert df.tail(GAP)["in_sample"].to_list() == [False] * GAP             # the invariant is not vacuous: the gap rows are OOS

    def test_a_legacy_store_is_widened_not_rebuilt(self, mkt):
        """The live store (1.0) has no model_train_end: the first run on 1.1 widens it additively."""
        full = pl.read_parquet(hk.MARKET_REGIME_PATH)
        legacy = full.drop("model_train_end").with_columns(pl.lit("1.0").alias("processing_version"))
        legacy.write_parquet(hk.MARKET_REGIME_PATH)
        msgs, sink = _warnings()
        try:
            added = hk.append_new_rows(mkt["panel"], mkt["cfg"], mkt["run_date"])
        finally:
            logger.remove(sink)
        after = pl.read_parquet(hk.MARKET_REGIME_PATH)
        assert added == GAP and after.height == legacy.height + GAP and after.columns == hk._column_order(mkt["cfg"])
        assert after.head(legacy.height).drop("model_train_end").equals(legacy)   # no stored value changed
        assert after.head(legacy.height)["model_train_end"].null_count() == legacy.height
        assert after.tail(GAP)["model_train_end"].null_count() == 0
        assert after["processing_version"].unique().sort().to_list() == ["1.0", "1.1"]
        assert any("widened additively" in m and "model_train_end" in m for m in msgs)

    def test_widening_a_legacy_store_does_not_hide_a_real_schema_drift(self, mkt):
        legacy = (pl.read_parquet(hk.MARKET_REGIME_PATH).drop("model_train_end")
                  .with_columns(pl.lit(1).alias("legacy_extra")))
        legacy.write_parquet(hk.MARKET_REGIME_PATH)
        before = hk.MARKET_REGIME_PATH.read_bytes()
        with pytest.raises(hk.MarketRegimeError, match="append-only and cannot be migrated in place"):
            hk.append_new_rows(mkt["panel"], mkt["cfg"], mkt["run_date"])
        assert hk.MARKET_REGIME_PATH.read_bytes() == before


# ═══ macro branch ════════════════════════════════════════════════════════════

MACRO_N = 700


@pytest.fixture(scope="module")
def macro_built(tmp_path_factory):
    root = tmp_path_factory.mktemp("adr058_macro")
    cfg = macro_model_config()
    panel = macro_panel(n=MACRO_N)
    # The stress domain's only series is missing over the last 30 rows of the training head, so its
    # model's train_end is EARLIER than the other four's — the domains must not share one training end,
    # or `model_train_end = max(...)` could not be told from `min(...)`.
    cut = MACRO_N - GAP - 30
    panel = panel.with_columns(
        pl.when(pl.int_range(pl.len()) >= cut).then(None).otherwise(pl.col("BAMLH0A0HYM2")).alias("BAMLH0A0HYM2"))
    panel = pl.concat([panel.head(MACRO_N - GAP),
                       macro_panel(n=MACRO_N).tail(GAP)])      # the gap rows are complete again
    head = panel.head(MACRO_N - GAP)
    old = (hm.MODELS_DIR, hm.MACRO_REGIME_PATH)
    hm.MODELS_DIR, hm.MACRO_REGIME_PATH = root / "models", root / "macro_regime.parquet"
    try:
        hm.fit_all(head, head["as_of_date"][-1], cfg)
        hm.append_new_rows(head, cfg, head["as_of_date"][-1])
    finally:
        hm.MODELS_DIR, hm.MACRO_REGIME_PATH = old
    return {"root": root, "cfg": cfg, "panel": panel, "head": head}


def _with_obs_dates(panel: pl.DataFrame, cfg, age_by_feature: dict[str, int]) -> pl.DataFrame:
    """Add raw_macro's obs_date__<feature> columns: each feature's carried observation is k days old."""
    return panel.with_columns([
        (pl.col("as_of_date") - pl.duration(days=age_by_feature.get(f.feature, 1))).alias(f"obs_date__{f.feature}")
        for f in cfg.macro.features
    ])


@pytest.fixture
def mac(macro_built, tmp_path, monkeypatch):
    shutil.copytree(macro_built["root"], tmp_path / "w")
    monkeypatch.setattr(hm, "MODELS_DIR", tmp_path / "w" / "models")
    monkeypatch.setattr(hm, "MACRO_REGIME_PATH", tmp_path / "w" / "macro_regime.parquet")
    monkeypatch.setattr(raw_macro, "RAW_MACRO_PATH", tmp_path / "w" / "raw_macro.parquet")
    macro_built["panel"].write_parquet(raw_macro.RAW_MACRO_PATH)
    return {**macro_built, "run_date": macro_built["panel"]["as_of_date"][-1]}


class TestMacroGapGuard:
    def test_the_gap_is_exactly_the_unscored_panel_dates(self, mac):
        gap = hm.gap_dates(mac["run_date"], mac["cfg"])
        assert len(gap) == GAP and gap == mac["panel"]["as_of_date"].tail(GAP).to_list()

    def test_fit_first_is_refused_and_touches_nothing(self, mac):
        store = hm.MACRO_REGIME_PATH.read_bytes()
        models = {p.name: p.read_bytes() for p in hm.MODELS_DIR.iterdir()}
        with pytest.raises(hm.MacroRegimeGapError, match="gold_hmm_macro_regime"):
            hm.run_fit(mac["run_date"])
        assert hm.MACRO_REGIME_PATH.read_bytes() == store
        assert {p.name: p.read_bytes() for p in hm.MODELS_DIR.iterdir()} == models

    def test_classify_then_fit_stores_the_gap_out_of_sample(self, mac):
        hm.run(mac["run_date"])
        df = pl.read_parquet(hm.MACRO_REGIME_PATH)
        assert df.height == MACRO_N and df.tail(GAP)["in_sample"].to_list() == [False] * GAP
        assert hm.gap_dates(mac["run_date"], mac["cfg"]) == []
        before = df
        hm.run_fit(mac["run_date"])
        assert pl.read_parquet(hm.MACRO_REGIME_PATH).equals(before)

    def test_the_override_logs_and_the_gap_becomes_in_sample_for_good(self, mac, monkeypatch):
        monkeypatch.setenv(hc.ALLOW_GAP_ENV, "1")
        msgs, sink = _warnings()
        try:
            hm.run_fit(mac["run_date"])
        finally:
            logger.remove(sink)
        assert any("gap guard OVERRIDDEN" in m for m in msgs)
        gap_rows = pl.read_parquet(hm.MACRO_REGIME_PATH).tail(GAP)
        assert gap_rows["in_sample"].to_list() == [True] * GAP
        assert (gap_rows["model_train_end"] >= gap_rows["date"]).all()

    def test_no_store_or_no_model_is_not_a_gap(self, mac, monkeypatch, tmp_path):
        monkeypatch.setattr(hm, "MACRO_REGIME_PATH", tmp_path / "absent.parquet")
        assert hm.gap_dates(mac["run_date"], mac["cfg"]) == []
        monkeypatch.setattr(hm, "MODELS_DIR", tmp_path / "no_models")
        assert hm.gap_dates(mac["run_date"], mac["cfg"]) == []


class TestMacroProvenance:
    def test_model_train_end_is_the_latest_domain_training_end(self, mac):
        hm.run(mac["run_date"])
        df = pl.read_parquet(hm.MACRO_REGIME_PATH)
        ends = [date.fromisoformat(hm.load_model(d)["train_end"]) for d in mac["cfg"].domains]
        assert len(set(ends)) > 1                                                # non-vacuous: the domains differ
        assert df["model_train_end"].null_count() == 0 and df["model_train_end"].unique().to_list() == [max(ends)]
        assert df.filter(~pl.col("in_sample") & (pl.col("model_train_end") >= pl.col("date"))).height == 0

    def test_domain_age_is_the_freshest_observation_in_the_domain(self, mac):
        cfg = mac["cfg"]
        ages = {"DFF": 3, "T10Y2Y": 9, "DGS5": 5, "DGS10": 5, "UNRATE": 40, "A191RL1Q225SBEA": 130,
                "T5YIE": 1, "T10YIE": 2, "CPIAUCSL_YOY": 50, "DCOILWTICO": 6, "BAMLH0A0HYM2": 4,
                "NFCI": 12, "MORTGAGE30US": 8}
        panel = _with_obs_dates(mac["panel"], cfg, ages)
        out = hm.domain_age_frame(panel, cfg)
        expected = {"monetary": 1, "growth": 40, "inflation": 1, "stress": 4, "liquidity": 8}
        # monetary: REALYIELD5Y/10Y are derived features whose obs_date column the panel here sets to 1 day
        for d, k in expected.items():
            assert out[f"{d}_age_days"].unique().to_list() == [k], d
        assert all(out.schema[f"{d}_age_days"] == pl.Int32 for d in cfg.domains)

    def test_a_panel_without_obs_dates_gives_null_ages_not_an_error(self, mac):
        out = hm.domain_age_frame(mac["panel"], mac["cfg"])
        assert all(out[f"{d}_age_days"].null_count() == out.height for d in mac["cfg"].domains)

    def test_stored_rows_carry_the_age_columns_in_the_documented_order(self, mac):
        panel = _with_obs_dates(mac["panel"], mac["cfg"], {})
        panel.write_parquet(raw_macro.RAW_MACRO_PATH)
        hm.append_new_rows(panel, mac["cfg"], mac["run_date"])
        df = pl.read_parquet(hm.MACRO_REGIME_PATH)
        assert df.columns == hm._column_order(mac["cfg"])
        assert df.tail(GAP)["growth_age_days"].unique().to_list() == [1]
        assert df.head(MACRO_N - GAP)["growth_age_days"].null_count() == MACRO_N - GAP   # legacy rows: NULL

    def test_a_legacy_macro_store_is_widened_not_rebuilt(self, mac):
        full = pl.read_parquet(hm.MACRO_REGIME_PATH)
        new_cols = list(hm.provenance_columns(mac["cfg"]))
        legacy = full.drop(new_cols).with_columns(pl.lit("1.0").alias("processing_version"))
        legacy.write_parquet(hm.MACRO_REGIME_PATH)
        added = hm.append_new_rows(mac["panel"], mac["cfg"], mac["run_date"])
        after = pl.read_parquet(hm.MACRO_REGIME_PATH)
        assert added == GAP and after.columns == hm._column_order(mac["cfg"])
        assert after.head(legacy.height).drop(new_cols).equals(legacy)
        assert all(after.head(legacy.height)[c].null_count() == legacy.height for c in new_cols)
        assert after.tail(GAP)["model_train_end"].null_count() == 0

    def test_a_legacy_store_missing_a_needed_column_still_raises_cleanly(self, mac):
        pl.read_parquet(hm.MACRO_REGIME_PATH).drop("macro_label").write_parquet(hm.MACRO_REGIME_PATH)
        with pytest.raises(hm.MacroRegimeError, match="cannot be migrated in place"):
            hm.append_new_rows(mac["panel"], mac["cfg"], mac["run_date"])


# ═══ HMM-R03: the daily FRED slice ═══════════════════════════════════════════

def _bronze_fred(tmp_path, value, obs="2026-10-13", series="VIXCLS"):
    d = tmp_path / "data" / "bronze" / "macro" / "fred" / "volatility"
    d.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"series_id": [series], "observation_date": [obs], "value": [value],
                  "release_date": [obs]}).write_parquet(d / f"{series}_fixture.parquet")


def _prior_snapshot(tmp_path, day, value, obs="2026-10-13", series="VIXCLS", seq=0):
    d = tmp_path / "data" / "silver" / "macro_enriched"
    d.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"series_id": [series], "observation_date": [obs], "value": [value],
                  "revision_seq": [seq]}).write_parquet(d / f"fred_{day}_silver.parquet")


class TestFindLatestSilverBefore:
    def _touch(self, tmp_path, monkeypatch, names):
        import src.silver.macro_processor as mp
        monkeypatch.setattr(mp, "SILVER_MACRO_PATH", tmp_path)
        for n in names:
            pl.DataFrame({"x": [1]}).write_parquet(tmp_path / n)
        return mp.MacroProcessor()

    def test_before_excludes_the_run_date_and_later(self, tmp_path, monkeypatch):
        proc = self._touch(tmp_path, monkeypatch, [f"fred_2026-10-{d}_silver.parquet" for d in (14, 15, 16)])
        assert proc._find_latest_silver("fred", before=date(2026, 10, 15)).name == "fred_2026-10-14_silver.parquet"
        assert proc._find_latest_silver("fred", before=date(2026, 10, 16)).name == "fred_2026-10-15_silver.parquet"
        assert proc._find_latest_silver("fred", before=date(2026, 10, 14)) is None
        assert proc._find_latest_silver("fred").name == "fred_2026-10-16_silver.parquet"   # legacy call unchanged

    def test_a_non_dated_file_is_ignored_when_before_is_given(self, tmp_path, monkeypatch):
        proc = self._touch(tmp_path, monkeypatch, ["fred_latest_silver.parquet", "fred_2026-10-14_silver.parquet"])
        assert proc._find_latest_silver("fred", before=date(2026, 10, 20)).name == "fred_2026-10-14_silver.parquet"


class TestSameDayRerunIsIdempotent:
    def test_a_second_run_on_the_same_date_reproduces_the_revision_flags(self, tmp_path, monkeypatch):
        """The previous vintage must be an EARLIER snapshot. Before the fix the second run found the
        first run's own output, saw no value change, and overwrote is_revision with False."""
        monkeypatch.chdir(tmp_path)
        _prior_snapshot(tmp_path, "2026-10-14", value=16.0)
        _bronze_fred(tmp_path, value=17.0)                                    # the value was revised
        from src.silver.macro_processor import MacroProcessor
        out = tmp_path / "data" / "silver" / "macro_enriched" / "fred_2026-10-15_silver.parquet"
        MacroProcessor().process_fred(date(2026, 10, 15))
        first = pl.read_parquet(out)
        MacroProcessor().process_fred(date(2026, 10, 15))
        second = pl.read_parquet(out)
        assert first["is_revision"].to_list() == [True] and first["revision_seq"].to_list() == [1]
        assert second.equals(first)

    def test_a_backfill_with_a_past_date_ignores_a_future_snapshot(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _prior_snapshot(tmp_path, "2026-10-14", value=16.0)
        _prior_snapshot(tmp_path, "2026-10-20", value=99.0)                   # a LATER vintage must not be "previous"
        _bronze_fred(tmp_path, value=17.0)
        from src.silver.macro_processor import MacroProcessor
        MacroProcessor().process_fred(date(2026, 10, 15))
        out = pl.read_parquet(tmp_path / "data" / "silver" / "macro_enriched" / "fred_2026-10-15_silver.parquet")
        assert out["revision_seq"].to_list() == [1]                           # 16.0 -> 17.0, not 99.0 -> 17.0

    def test_the_first_ever_snapshot_has_no_previous_vintage(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _bronze_fred(tmp_path, value=17.0)
        from src.silver.macro_processor import MacroProcessor
        MacroProcessor().process_fred(date(2026, 10, 15))
        out = pl.read_parquet(tmp_path / "data" / "silver" / "macro_enriched" / "fred_2026-10-15_silver.parquet")
        assert out["is_revision"].to_list() == [False] and out["revision_seq"].to_list() == [0]


class TestRunFredSlice:
    def test_run_fred_processes_fred_only(self, monkeypatch):
        import src.silver.macro_processor as mp
        called = []
        for name in ("process_fred", "process_bls", "process_bea", "process_treasury", "process_eia"):
            monkeypatch.setattr(mp.MacroProcessor, name, lambda self, rd, _n=name: called.append(_n))
        mp.run_fred(date(2026, 10, 15))
        assert called == ["process_fred"]                                     # no weekly BLS / BEA / EIA cost

    def test_the_registry_wrapper_delegates_to_run_fred(self, monkeypatch):
        import src.silver.macro_processor as mp
        from src.scheduler.job_registry import JOB_REGISTRY
        seen = []
        monkeypatch.setattr(mp, "run_fred", lambda rd: seen.append(rd))
        JOB_REGISTRY["silver_fred_daily"]["fn"](date(2026, 10, 15))
        assert seen == [date(2026, 10, 15)]
