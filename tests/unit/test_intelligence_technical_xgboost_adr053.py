"""
tests/unit/test_intelligence_technical_xgboost_adr053.py — ADR-053 specialist #2 (technical domain):
the as-of feature builder (availability rule, warm-up, breadth, no lookahead) and the fit/predict jobs.
Synthetic tech_signals files mimic the real stamping: bars at the PERIOD START (Monday / month start).
"""
import copy
from datetime import date, datetime, timedelta

import numpy as np
import polars as pl
import pytest
import yaml

from src.gold.intelligence import core
from src.gold.intelligence import features_technical as fg
from src.gold.intelligence import technical_xgboost as gx

MARKET_OF = {"AAA": "us_stocks", "BBB": "us_stocks", "CCC": "us_stocks", "DDD": "us_stocks",
             "III": "idx", "JJJ": "idx", "KKK": "idx", "FFF": "forex", "GGG": "forex", "HHH": "forex"}


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _indicators(df: pl.DataFrame, seed: int) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    n = df.height
    ew = lambda c, span: pl.col(c).ewm_mean(span=span, adjust=False).over("symbol")
    out = df.with_columns(ew("close", 9).alias("ema_9"), ew("close", 21).alias("ema_21"), ew("close", 50).alias("ema_50"),
                          ew("close", 200).alias("ema_200"))
    out = out.with_columns(
        pl.Series("rsi_14", rng.uniform(20, 80, n)), pl.Series("macd_hist", rng.normal(scale=0.3, size=n)),
        pl.Series("adx", rng.uniform(10, 40, n)), pl.Series("di_plus", rng.uniform(5, 35, n)),
        pl.Series("di_minus", rng.uniform(5, 35, n)), pl.Series("atr_pct", rng.uniform(0.005, 0.03, n)),
        (pl.col("close") * 1.02).alias("bbands_upper"), (pl.col("close") * 0.98).alias("bbands_lower"),
        pl.Series("relative_volume", rng.uniform(0.5, 2.0, n)), pl.Series("momentum_score", rng.normal(size=n)))
    return out


def _tech_dir(tmp_path, n_days=1500, start=date(2016, 1, 4), seed=3, symbols=None):
    """tech_signals_{1D,1W,1M}.parquet for a few symbols; weekly/monthly bars derived from the daily
    closes and stamped at the period start, exactly like the real files."""
    rng = np.random.default_rng(seed)
    symbols = symbols or list(MARKET_OF)
    days = _weekdays(start, n_days)
    rows = []
    for k, s in enumerate(symbols):
        px = 100 * np.exp(np.cumsum(rng.normal(0.0003, 0.012, n_days)))
        rows += [(s, datetime(d.year, d.month, d.day), float(p)) for d, p in zip(days, px)]
    daily = pl.DataFrame(rows, schema=["symbol", "timestamp", "close"], orient="row").sort(["symbol", "timestamp"])
    wk = (daily.group_by_dynamic("timestamp", every="1w", group_by="symbol").agg(pl.col("close").last())
          .sort(["symbol", "timestamp"]))
    mo = (daily.group_by_dynamic("timestamp", every="1mo", group_by="symbol").agg(pl.col("close").last())
          .sort(["symbol", "timestamp"]))
    d = tmp_path / "signals"
    d.mkdir(exist_ok=True)
    for tf, df in (("1D", daily), ("1W", wk), ("1M", mo)):
        _indicators(df, seed + 1).write_parquet(d / f"tech_signals_{tf}.parquet")
    return d, days


def _labels(days, symbols=None, seed=11, horizon=9):
    rng = np.random.default_rng(seed)
    spec = core.load_config().label_spec
    rows = []
    for d in days[300::2]:
        for s in (symbols or list(MARKET_OF)):
            for direction in ("long", "short"):
                rows.append((s, MARKET_OF[s], d, direction, int(rng.random() < 0.3), d + timedelta(days=horizon), spec))
    return pl.DataFrame(rows, schema=["symbol", "market", "decision_date", "direction", "label_binary",
                                      "label_end_date", "label_spec"], orient="row")


@pytest.fixture(scope="module")
def tech(tmp_path_factory):
    d, days = _tech_dir(tmp_path_factory.mktemp("tech"))
    return {"dir": d, "days": days, "tables": fg.load_tables(d)}


SM = pl.DataFrame({"symbol": list(MARKET_OF), "market": list(MARKET_OF.values())})


# ── builder ──────────────────────────────────────────────────────────────────

class TestFeatureSpec:
    def test_feature_list_is_pinned(self):
        assert len(fg.FEATURES) == 33 and fg.FEATURES[-2:] == ("direction_sign", "market_code")
        assert len(set(fg.FEATURES)) == len(fg.FEATURES)

    def test_run_day_market_columns_are_never_read(self):
        assert not {"vix_at_signal", "high_volatility_flag", "signal_date"} & set(fg._RAW)
        assert not any("vix" in f for f in fg.FEATURES)

    def test_only_history_deep_timeframes(self):
        assert fg.TIMEFRAMES == ("1D", "1W", "1M")           # 1H/4H history is ~2 years


class TestAvailability:
    def test_daily_weekly_monthly_avail_dates(self, tech):
        t = tech["tables"]
        d = t["1D"].filter(pl.col("symbol") == "AAA").head(3)
        assert (d["avail_date"] - d["bar_date"]).unique().to_list() == [timedelta(days=1)]
        w = t["1W"].filter(pl.col("symbol") == "AAA")
        assert (w["bar_date"].dt.weekday() == 1).all()                              # stamped Monday
        assert (w["avail_date"].dt.weekday() == 1).all()                            # available the NEXT Monday
        assert (w["avail_date"] - w["bar_date"]).unique().to_list() == [timedelta(days=7)]
        m = t["1M"].filter(pl.col("symbol") == "AAA")
        assert (m["avail_date"].dt.day() == 1).all() and (m["avail_date"] > m["bar_date"]).all()

    def test_mid_week_decision_sees_the_previous_completed_week_only(self, tech):
        w = tech["tables"]["1W"].filter(pl.col("symbol") == "AAA")
        mid = w["bar_date"][100] + timedelta(days=9)                               # Wednesday of the week after bar 100
        assert mid.weekday() == 2
        dec = pl.DataFrame({"symbol": ["AAA"], "decision_date": [mid]})
        got = fg._asof(dec, w.with_columns(pl.col("bar_date").alias("bd")), "symbol", ["bd"], fg.TOLERANCE["1W"])["bd"][0]
        assert got == w["bar_date"][100]                       # bar 101 (this week, in progress) is NOT visible

    def test_the_monday_after_a_week_sees_that_week(self, tech):
        w = tech["tables"]["1W"].filter(pl.col("symbol") == "AAA")
        monday_after = w["bar_date"][100] + timedelta(days=7)
        dec = pl.DataFrame({"symbol": ["AAA"], "decision_date": [monday_after]})
        got = fg._asof(dec, w.with_columns(pl.col("bar_date").alias("bd")), "symbol", ["bd"], fg.TOLERANCE["1W"])["bd"][0]
        assert got == w["bar_date"][100]

    def test_decision_day_bar_is_never_used(self, tech):
        d = tech["tables"]["1D"].filter(pl.col("symbol") == "AAA")
        t = d["bar_date"][400]
        dec = pl.DataFrame({"symbol": ["AAA"], "decision_date": [t]})
        got = fg._asof(dec, d.with_columns(pl.col("bar_date").alias("bd")), "symbol", ["bd"], fg.TOLERANCE["1D"])["bd"][0]
        assert got < t

    def test_stale_bar_beyond_tolerance_is_not_attached(self, tech):
        d = tech["tables"]["1D"].filter(pl.col("symbol") == "AAA")
        far = d["bar_date"].max() + timedelta(days=30)
        dec = pl.DataFrame({"symbol": ["AAA"], "decision_date": [far]})
        assert fg._asof(dec, d, "symbol", ["d_rsi_14"], fg.TOLERANCE["1D"])["d_rsi_14"][0] is None


class TestNoLookahead:
    def test_TRUNCATION_INVARIANCE_features_depend_only_on_bars_completed_before_the_decision(self, tech):
        """Features for decision T from the full tables == from tables cut to avail_date <= T."""
        T = tech["days"][900]
        dec = pl.DataFrame({"symbol": list(MARKET_OF), "decision_date": [T] * len(MARKET_OF)})
        full = fg.asof_features(dec, tech["tables"], SM).sort("symbol")
        cut = {tf: t.filter(pl.col("avail_date") <= T) for tf, t in tech["tables"].items()}
        short = fg.asof_features(dec, cut, SM).sort("symbol")
        for c in (*fg.TF_FEATURES["1D"], *fg.TF_FEATURES["1W"], *fg.TF_FEATURES["1M"], *fg.BREADTH_FEATURES):
            a, b = full[c].to_numpy(), short[c].to_numpy()
            assert np.allclose(a, b, equal_nan=True), c

    def test_MUTANT_using_the_in_progress_bar_changes_the_features(self, tech):
        """If availability were bar_date (the Monday stamp) instead of the completed-week rule, a
        mid-week decision would pick up the in-progress week — the suite must see the difference."""
        T = tech["days"][900]
        while T.weekday() != 2:
            T += timedelta(days=1)
        dec = pl.DataFrame({"symbol": ["AAA"], "decision_date": [T]})
        good = fg._asof(dec, tech["tables"]["1W"], "symbol", ["w_rsi_14"], fg.TOLERANCE["1W"])["w_rsi_14"][0]
        leaky = tech["tables"]["1W"].with_columns(pl.col("bar_date").alias("avail_date"))
        bad = fg._asof(dec, leaky, "symbol", ["w_rsi_14"], fg.TOLERANCE["1W"])["w_rsi_14"][0]
        assert good != bad

    def test_garbage_in_the_run_day_columns_changes_nothing(self, tech, tmp_path):
        d2 = tmp_path / "signals"
        d2.mkdir()
        for tf in fg.TIMEFRAMES:
            df = pl.read_parquet(tech["dir"] / f"tech_signals_{tf}.parquet").with_columns(
                pl.lit("2099-01-01").alias("signal_date"), pl.lit(99.0).alias("vix_at_signal"), pl.lit(True).alias("high_volatility_flag"))
            df.write_parquet(d2 / f"tech_signals_{tf}.parquet")
        a, b = fg.load_tables(tech["dir"]), fg.load_tables(d2)
        for tf in fg.TIMEFRAMES:
            assert a[tf].equals(b[tf])


class TestWarmupAndBreadth:
    def test_first_bars_of_a_symbol_are_masked_until_the_warmup_has_passed(self, tech):
        d = tech["tables"]["1D"].filter(pl.col("symbol") == "AAA").sort("bar_date")
        assert d["d_dist_ema50"][: fg.WARMUP_BARS["1D"]].null_count() == fg.WARMUP_BARS["1D"]
        assert d["d_dist_ema50"][fg.WARMUP_BARS["1D"]:].null_count() == 0
        w = tech["tables"]["1W"].filter(pl.col("symbol") == "AAA").sort("bar_date")
        assert w["w_trend"][: fg.WARMUP_BARS["1W"]].null_count() == fg.WARMUP_BARS["1W"]

    def test_breadth_is_the_share_above_ema50_per_market_and_needs_three_symbols(self, tech):
        br = fg.breadth_table(tech["tables"]["1D"], SM)
        assert set(br["market"].unique().to_list()) == {"us_stocks", "idx", "forex"}      # >= 3 symbols each
        assert ((br["breadth_ema50_market"] >= 0) & (br["breadth_ema50_market"] <= 1)).all()
        d = tech["tables"]["1D"]
        day = d["bar_date"][600]
        sub = d.filter((pl.col("bar_date") == day) & pl.col("symbol").is_in(["AAA", "BBB", "CCC", "DDD"]))
        expect = float((sub["close"] > sub["ema_50"]).mean())
        got = br.filter((pl.col("market") == "us_stocks") & (pl.col("bar_date") == day))["breadth_ema50_market"][0]
        assert got == pytest.approx(expect, abs=1e-6)

    def test_a_market_with_fewer_than_three_symbols_has_no_breadth(self, tech):
        sm = SM.filter(pl.col("symbol").is_in(["AAA", "BBB"]))
        assert fg.breadth_table(tech["tables"]["1D"], sm).height == 0


class TestTrainingFrameAndPredictionRows:
    def test_training_frame_is_complete_case_and_aligned(self, tech):
        labels = _labels(tech["days"])
        fr = fg.training_frame(labels, tech["tables"])
        assert fr.height > 0 and fr["weight"].unique().to_list() == [1.0]
        assert fr.select(pl.all_horizontal(pl.col(c).is_not_null() for c in fg.FEATURES).all()).item()
        assert set(fr["label"].unique().to_list()) <= {0.0, 1.0}
        assert fr["decision_date"].min() > tech["days"][fg.WARMUP_BARS["1D"] - 1]       # warm-up rows are gone
        assert set(fr["direction_sign"].unique().to_list()) == {1.0, -1.0}

    def test_each_symbol_date_has_both_directions_with_identical_features(self, tech):
        fr = fg.training_frame(_labels(tech["days"]), tech["tables"])
        g = fr.group_by(["symbol", "decision_date"]).agg(pl.col("d_rsi_14").n_unique().alias("u"), pl.len().alias("n"))
        assert g["n"].unique().to_list() == [2] and g["u"].unique().to_list() == [1]

    def test_prediction_rows_cover_known_symbols_and_both_directions(self, tech):
        run = tech["days"][-1] + timedelta(days=1)
        rows = fg.prediction_rows(run, tech["tables"], MARKET_OF)
        assert rows.height == 2 * len(MARKET_OF) and set(rows["direction"].unique().to_list()) == {"long", "short"}
        assert (rows["as_of_date"] == run).all()

    def test_unknown_symbols_are_not_scored(self, tech):
        run = tech["days"][-1] + timedelta(days=1)
        known = {k: v for k, v in MARKET_OF.items() if k != "DDD"}          # DDD has bars but is not in the model's map
        rows = fg.prediction_rows(run, tech["tables"], known)
        syms = set(rows["symbol"].unique().to_list())
        assert "DDD" not in syms and "AAA" in syms

    def test_a_stale_store_scores_nothing(self, tech):
        run = tech["days"][-1] + timedelta(days=40)
        assert fg.prediction_rows(run, tech["tables"], MARKET_OF).height == 0

    def test_missing_signal_file_or_column_is_a_clear_error(self, tmp_path):
        with pytest.raises(core.IntelligenceError):
            fg.load_tables(tmp_path)
        d, _ = _tech_dir(tmp_path, n_days=300)
        pl.read_parquet(d / "tech_signals_1M.parquet").drop("adx").write_parquet(d / "tech_signals_1M.parquet")
        with pytest.raises(core.IntelligenceError):
            fg.load_tables(d)


# ── jobs ─────────────────────────────────────────────────────────────────────

def _cfg(tmp_path) -> core.IntelligenceConfig:
    base = yaml.safe_load(open("config/intelligence.yaml"))
    raw = copy.deepcopy(base)
    raw["xgb"].update(n_estimators=25, nthread=2)
    raw["walk_forward"].update(initial_train_months=12, test_block_months=6, embargo_days=7, min_train_rows=200)
    raw["bags"].update(oof=2, final=2)
    raw["calibration"]["min_rows"] = 300
    raw["output_root"] = str(tmp_path / "out")
    raw["models"] = {}
    return core.validate_config(raw)


@pytest.fixture
def jobs(tech, tmp_path):
    days = tech["days"]
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir()
    lab = _labels(days)
    lab.write_parquet(labels_dir / "labels_x.parquet")
    return {"cfg": _cfg(tmp_path), "labels_dir": labels_dir, "sig": tech["dir"], "days": days,
            "run": lab["decision_date"].max() + timedelta(days=3)}


class TestFitAndPredict:
    def test_fit_then_predict_end_to_end(self, jobs):
        st = gx.run_fit(jobs["run"], jobs["cfg"], jobs["labels_dir"], jobs["sig"])
        assert st["status"] == "fitted", st
        store = core.ModelStore(jobs["cfg"].output_root, gx.MODEL_NAME)
        bundle = core.load_bundle(store.model)
        assert bundle["symbol_market"] == MARKET_OF and bundle["features"] == list(fg.FEATURES)
        n = gx.run(jobs["run"], jobs["cfg"], quality="pass", signals_dir=jobs["sig"])
        assert n == 2 * len(MARKET_OF)
        out = pl.read_parquet(store.predictions)
        for c in ("as_of_date", "symbol", "market", "direction", "probability", "confidence", "quality", "regime_tag",
                  "age_days", "model_fit_date", "lineage"):
            assert c in out.columns, c
        assert set(out["regime_tag"].unique().to_list()) <= {"bull", "bear", "mixed"}
        assert ((out["probability"] >= 0) & (out["probability"] <= 1)).all()
        assert out["lineage"][0].startswith("technical_xgboost|fit=")

    def test_predict_without_a_model_writes_nothing(self, jobs):
        assert gx.run(jobs["run"], jobs["cfg"], quality="pass", signals_dir=jobs["sig"]) == 0

    def test_fit_skips_recorded_on_stale_labels(self, jobs):
        st = gx.run_fit(jobs["run"] + timedelta(days=500), jobs["cfg"], jobs["labels_dir"], jobs["sig"])
        assert st["status"] == "skipped" and "LabelsStale" in st["reason"]

    def test_fit_skips_when_the_technical_store_is_missing(self, jobs, tmp_path):
        st = gx.run_fit(jobs["run"], jobs["cfg"], jobs["labels_dir"], tmp_path / "empty")
        assert st["status"] == "skipped" and "technical store" in st["reason"]

    def test_predict_is_idempotent_per_date(self, jobs):
        gx.run_fit(jobs["run"], jobs["cfg"], jobs["labels_dir"], jobs["sig"])
        gx.run(jobs["run"], jobs["cfg"], quality="warn", signals_dir=jobs["sig"])
        gx.run(jobs["run"], jobs["cfg"], quality="warn", signals_dir=jobs["sig"])
        out = pl.read_parquet(core.ModelStore(jobs["cfg"].output_root, gx.MODEL_NAME).predictions)
        assert out.height == 2 * len(MARKET_OF) and out["quality"].unique().to_list() == ["warn"]

    def test_a_model_from_the_future_is_refused(self, jobs):
        gx.run_fit(jobs["run"], jobs["cfg"], jobs["labels_dir"], jobs["sig"])
        with pytest.raises(core.LeakageError):
            gx.run(jobs["run"] - timedelta(days=60), jobs["cfg"], quality="pass", signals_dir=jobs["sig"])

    def test_stale_technical_store_writes_nothing(self, jobs):
        gx.run_fit(jobs["run"], jobs["cfg"], jobs["labels_dir"], jobs["sig"])
        assert gx.run(jobs["run"] + timedelta(days=60), jobs["cfg"], quality="pass", signals_dir=jobs["sig"]) == 0

    def test_regime_tag_buckets(self):
        s = pl.DataFrame({"x": [2.0, 1.0, 0.0, -1.0, -2.0]})
        assert s.select(gx.regime_tag(pl.col("x")).alias("t"))["t"].to_list() == ["bull", "mixed", "mixed", "mixed", "bear"]


# ── rename guards (ADR-053 Amendment 2: gold_xgboost -> technical_xgboost) ───

class TestRenameGuards:
    """The model name is a KEY in config/intelligence.yaml (`models:` overrides) and a path segment of every
    store. validate_config accepts any override name, so a name that drifts from its module is silently
    ignored — the technical model would train with the bare defaults and nothing would say so."""
    SPECIALISTS = ("hmm_xgboost", "technical_xgboost", "cross_asset_xgboost")

    def test_model_name_follows_the_domain_pattern(self):
        from src.gold.intelligence import hmm_xgboost as hx
        for mod in (gx, hx):
            assert mod.MODEL_NAME == f"{mod.GATE_SCOPE}_xgboost"
            assert mod.__name__.rsplit(".", 1)[-1] == mod.MODEL_NAME

    def test_every_shipped_override_key_names_a_specialist(self):
        with open("config/intelligence.yaml") as fh:
            raw = yaml.safe_load(fh)
        assert set(raw["models"]) <= set(self.SPECIALISTS), set(raw["models"]) - set(self.SPECIALISTS)

    def test_the_technical_override_actually_lands(self):
        cfg = core.load_config()
        base = cfg.xgb["min_child_weight"]
        got = cfg.for_model(gx.MODEL_NAME).xgb["min_child_weight"]
        assert got == 200 and got != base
        assert cfg.for_model("gold_xgboost").xgb["min_child_weight"] == base   # the retired name is a no-op

    def test_stores_live_under_the_new_name(self):
        store = core.ModelStore(core.load_config().output_root, gx.MODEL_NAME)
        assert store.dir.name == "technical_xgboost"

    def test_registry_carries_the_new_job_names_only(self):
        from src.scheduler.job_registry import DAILY_SEQUENCE, JOB_REGISTRY, WEEKLY_SEQUENCE
        for j in ("gold_technical_xgboost_fit", "gold_technical_xgboost_predict"):
            assert j in JOB_REGISTRY
        assert "gold_technical_xgboost_predict" in DAILY_SEQUENCE
        assert "gold_technical_xgboost_fit" in WEEKLY_SEQUENCE
        assert [j for j in (*JOB_REGISTRY, *DAILY_SEQUENCE, *WEEKLY_SEQUENCE) if "gold_gold" in j] == []


# ── optional feature blocks (ADR-053 Amendment 2) ────────────────────────────

def _with_ohlc(src, dst, seed=5):
    """Copy a synthetic signals dir and give the daily file open / high / low / atr_14 (as the real file has)."""
    rng = np.random.default_rng(seed)
    dst.mkdir(parents=True, exist_ok=True)
    daily = pl.read_parquet(src / "tech_signals_1D.parquet").sort(["symbol", "timestamp"])
    n = daily.height
    o = daily.with_columns((pl.col("close").shift(1).over("symbol") * (1 + pl.Series(rng.normal(0, 0.003, n))))
                           .fill_null(pl.col("close")).alias("open"))
    o = o.with_columns((pl.max_horizontal("open", "close") * (1 + pl.Series(rng.uniform(0.0, 0.01, n)))).alias("high"),
                       (pl.min_horizontal("open", "close") * (1 - pl.Series(rng.uniform(0.0, 0.01, n)))).alias("low"))
    o = o.with_columns((pl.col("atr_pct") * pl.col("close")).alias("atr_14"))
    o.write_parquet(dst / "tech_signals_1D.parquet")
    for tf in ("1W", "1M"):
        (dst / f"tech_signals_{tf}.parquet").write_bytes((src / f"tech_signals_{tf}.parquet").read_bytes())
    return dst


@pytest.fixture(scope="module")
def tech_ohlc(tech, tmp_path_factory):
    dst = _with_ohlc(tech["dir"], tmp_path_factory.mktemp("tech_ohlc") / "signals")
    blocks = (fg.BASE_BLOCK, fg.CANDLE_BLOCK)
    return {"dir": dst, "days": tech["days"], "blocks": blocks, "tables": fg.load_tables(dst, blocks)}


class TestFeatureBlocks:
    PROD_HASH = "2c0691b7333f"      # feature_spec_hash of the 33 production features BEFORE blocks existed

    def test_the_production_feature_spec_is_unchanged(self):
        assert fg.feature_list() == fg.FEATURES and len(fg.FEATURES) == 33
        assert core.feature_spec_hash(list(fg.FEATURES)) == self.PROD_HASH
        assert gx.FEATURE_BLOCKS == ("base",) and gx.FEATURES == list(fg.FEATURES)

    def test_normalize_blocks(self):
        assert fg.normalize_blocks(()) == ("base",)
        assert fg.normalize_blocks(("candle",)) == ("base", "candle")
        assert fg.normalize_blocks(["candle", "base", "candle"]) == ("base", "candle")
        with pytest.raises(core.IntelligenceError):
            fg.normalize_blocks(("base", "intraday"))

    def test_candle_block_adds_six_features_just_before_the_tail(self):
        f = fg.feature_list(("base", "candle"))
        assert len(f) == 39 and len(set(f)) == 39 and f[-2:] == ("direction_sign", "market_code")
        assert f[-8:-2] == fg.CANDLE_FEATURES == ("d_gap", "d_clv", "d_body", "d_upper_wick", "d_lower_wick", "d_range_atr")
        assert f[:-8] == fg.FEATURES[:-2]
        assert core.feature_spec_hash(list(f)) != self.PROD_HASH

    def test_base_tables_do_not_need_ohlc_but_the_candle_block_names_what_is_missing(self, tech):
        assert set(fg.load_tables(tech["dir"])) == {"1D", "1W", "1M"}                   # synthetic dir has no OHLC
        with pytest.raises(core.IntelligenceError, match="open"):
            fg.load_tables(tech["dir"], ("base", "candle"))

    def test_hand_computed_candle_values_and_the_symbol_boundary(self, monkeypatch):
        monkeypatch.setitem(fg.WARMUP_BARS, "1D", 0)                    # look at the raw values, not the masked ones
        ts = [datetime(2024, 1, 2), datetime(2024, 1, 3), datetime(2024, 1, 4)]
        bars = {"AAA": [(10.0, 12.0, 9.0, 11.0), (11.5, 13.0, 11.0, 12.5), (12.0, 12.0, 12.0, 12.0)],
                "BBB": [(50.0, 51.0, 49.0, 50.5), (50.5, 52.0, 50.0, 51.0), (51.0, 53.0, 50.5, 52.0)],
                "CCC": [(20.0, 21.0, 19.0, 20.5), (20.5, 22.0, 20.0, 21.0), (21.0, 23.0, 20.5, 22.0)],
                "DDD": [(30.0, 32.0, 30.0, 31.0), (31.0, 33.0, 31.0, 32.0), (32.0, 34.0, 32.0, 33.0)]}
        rows = [(s, t, o, h, l, c) for s, v in bars.items() for t, (o, h, l, c) in zip(ts, v)]
        df = pl.DataFrame(rows, schema=["symbol", "timestamp", "open", "high", "low", "close"], orient="row")
        df = _indicators(df, 1).with_columns(pl.when(pl.col("symbol") == "CCC").then(0.0)
                                         .when(pl.col("symbol") == "DDD").then(1e-13).otherwise(4.0).alias("atr_14"))
        out = fg._bar_features(df, "1D", ("base", "candle")).sort(["symbol", "bar_date"])
        a = out.filter(pl.col("symbol") == "AAA")
        assert a["d_gap"][0] is None                                    # first bar: no previous close
        assert a["d_gap"][1] == pytest.approx(11.5 / 11.0 - 1.0, rel=1e-5)
        assert a["d_clv"][1] == pytest.approx(0.5, rel=1e-5)            # ((12.5-11) - (13-12.5)) / 2
        assert a["d_body"][1] == pytest.approx(0.5, rel=1e-5)
        assert a["d_upper_wick"][1] == pytest.approx(0.25, rel=1e-5)
        assert a["d_lower_wick"][1] == pytest.approx(0.25, rel=1e-5)
        assert a["d_range_atr"][1] == pytest.approx(0.5, rel=1e-5)      # range 2 / atr 4
        assert a["d_clv"][2] is None and a["d_body"][2] is None         # zero-range bar: null, never 0 or inf
        assert a["d_upper_wick"][2] is None and a["d_lower_wick"][2] is None
        assert a["d_range_atr"][2] == 0.0                               # a flat bar over a live ATR IS zero, not missing
        c = out.filter(pl.col("symbol") == "CCC")
        assert c["d_range_atr"].null_count() == 3                       # atr_14 == 0 -> undefined, null (no inf)
        assert c["d_clv"][1] == pytest.approx(0.0, abs=1e-6)            # ((21-20) - (22-21)) / 2
        d = out.filter(pl.col("symbol") == "DDD")
        assert d["d_range_atr"].null_count() == 3                       # a ~0 ATR would give 2e13: finite, so only the guard saves it
        b = out.filter(pl.col("symbol") == "BBB")
        assert b["d_gap"][0] is None                                    # NOT AAA's last close (shift is per symbol)
        assert b["d_gap"][1] == pytest.approx(50.5 / 50.5 - 1.0, abs=1e-6)

    def test_wick_body_identity_holds_on_every_row(self, tech_ohlc):
        d = tech_ohlc["tables"]["1D"].drop_nulls(["d_body", "d_upper_wick", "d_lower_wick"])
        assert d.height > 1000
        resid = (d["d_body"].abs() + d["d_upper_wick"] + d["d_lower_wick"] - 1.0).abs().max()
        assert resid < 1e-4

    def test_candle_columns_share_the_base_warm_up_mask(self, tech_ohlc):
        d = tech_ohlc["tables"]["1D"].sort(["symbol", "bar_date"]).with_columns(
            pl.int_range(pl.len()).over("symbol").alias("k"))
        warm = d.filter(pl.col("k") < fg.WARMUP_BARS["1D"])
        assert warm.select(pl.all_horizontal(pl.col(c).is_null() for c in fg.CANDLE_FEATURES).all()).item()
        assert d.filter(pl.col("k") >= fg.WARMUP_BARS["1D"])["d_clv"].null_count() == 0

    def test_a_block_never_changes_the_row_set_or_the_base_values(self, tech_ohlc):
        labels = _labels(tech_ohlc["days"])
        a = fg.training_frame(labels, tech_ohlc["tables"], ("base",))
        b = fg.training_frame(labels, tech_ohlc["tables"], ("base", "candle"))
        key = ["symbol", "decision_date", "direction"]
        assert a.height == b.height > 0
        assert a.select(key).equals(b.select(key)) and a["label"].to_list() == b["label"].to_list()
        assert a.select(fg.feature_list()).equals(b.select(fg.feature_list()))      # base columns bit-identical
        assert b.select(pl.all_horizontal(pl.col(c).is_not_null() for c in fg.feature_list(("candle",))).all()).item()

    def test_candle_features_use_no_future_bar(self, tech_ohlc):
        path = tech_ohlc["dir"] / "tech_signals_1D.parquet"
        raw = pl.read_parquet(path)
        cut = tech_ohlc["days"][700]
        full = fg._bar_features(raw, "1D", ("base", "candle")).filter(pl.col("bar_date") <= cut)
        part = fg._bar_features(raw.filter(pl.col("timestamp") <= datetime(cut.year, cut.month, cut.day)), "1D",
                                ("base", "candle"))
        cols = ["symbol", "bar_date", *fg.CANDLE_FEATURES]
        assert full.select(cols).sort(["symbol", "bar_date"]).equals(part.select(cols).sort(["symbol", "bar_date"]))

    def test_a_bundle_records_its_blocks_and_predict_follows_the_bundle(self, tech_ohlc, tmp_path, monkeypatch):
        labels_dir = tmp_path / "labels"
        labels_dir.mkdir()
        lab = _labels(tech_ohlc["days"])
        lab.write_parquet(labels_dir / "labels_x.parquet")
        cfg, run = _cfg(tmp_path), lab["decision_date"].max() + timedelta(days=3)
        blocks = tech_ohlc["blocks"]
        monkeypatch.setattr(gx, "FEATURE_BLOCKS", blocks)
        monkeypatch.setattr(gx, "FEATURES", list(fg.feature_list(blocks)))
        st = gx.run_fit(run, cfg, labels_dir, tech_ohlc["dir"])
        assert st["status"] == "fitted", st
        bundle = core.load_bundle(core.ModelStore(cfg.output_root, gx.MODEL_NAME).model)
        assert bundle["blocks"] == ["base", "candle"] and bundle["features"] == list(fg.feature_list(blocks))
        monkeypatch.undo()                                             # module constants are base-only again
        assert gx.FEATURE_BLOCKS == ("base",)
        assert gx.run(run, cfg, quality="pass", signals_dir=tech_ohlc["dir"]) == 2 * len(MARKET_OF)

    def test_a_bundle_from_before_blocks_existed_is_a_base_model(self):
        assert gx.bundle_blocks({}) == ("base",)
        assert gx.bundle_blocks({"blocks": ["base", "candle"]}) == ("base", "candle")
