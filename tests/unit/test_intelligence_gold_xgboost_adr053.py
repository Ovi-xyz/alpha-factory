"""
tests/unit/test_intelligence_gold_xgboost_adr053.py — ADR-053 specialist #2 (technical domain):
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
from src.gold.intelligence import features_gold as fg
from src.gold.intelligence import gold_xgboost as gx

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
        assert out["lineage"][0].startswith("gold_xgboost|fit=")

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
