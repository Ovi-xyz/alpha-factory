"""
tests/unit/test_technical_block_ablation_adr053.py — ADR-053 Amendment 2: the same-window feature-block ablation
(scripts/studies/technical_block_ablation.py). The study REUSES the production engine, so these tests pin what
makes a comparison fair: identical rows, nested features, the production feature hash for arm A, a verdict rule
that needs BOTH metrics, and a span check that says plainly when a history is too short to produce any fold.
"""
import json
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from scripts.studies import technical_block_ablation as ab
from src.gold.intelligence import core
from src.gold.intelligence import features_technical as fg
from src.gold.intelligence import technical_xgboost as gx
from tests.unit.test_intelligence_technical_xgboost_adr053 import _cfg, _labels, _tech_dir, _with_ohlc

BOTH = ("base", "candle")


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    d, days = _tech_dir(tmp_path_factory.mktemp("abl_tech"))
    dst = _with_ohlc(d, tmp_path_factory.mktemp("abl_ohlc") / "signals")
    cfg = _cfg(tmp_path_factory.mktemp("abl_cfg")).for_model(gx.MODEL_NAME)
    return {"days": days, "dir": dst, "tables": fg.load_tables(dst, BOTH), "labels": _labels(days), "cfg": cfg}


# ── configuration and span check ─────────────────────────────────────────────

class TestSpanCheck:
    def test_the_shipped_36_month_setting_yields_no_fold_for_a_two_year_history(self):
        cfg = ab.study_config()
        assert cfg.initial_train_months == 36
        out = ab.span_check(date(2024, 10, 1), date(2026, 10, 16), cfg)
        assert out["n_blocks"] == 0 and out["oof_months_available"] == 0.0 and "zero" in out["note"]

    def test_twelve_months_yields_about_a_year_of_oof(self):
        cfg = ab.study_config(initial_train_months=12)
        out = ab.span_check(date(2024, 10, 1), date(2026, 10, 16), cfg)
        assert out["block_starts"] == ["2025-10-01", "2026-01-01", "2026-04-01", "2026-07-01", "2026-10-01"]
        assert out["oof_months_available"] == pytest.approx(12.5, abs=0.1)

    def test_a_later_first_label_shrinks_the_span(self):
        cfg = ab.study_config(initial_train_months=12)
        assert ab.span_check(date(2024, 12, 2), date(2026, 10, 16), cfg)["n_blocks"] == 4

    def test_overrides_go_through_the_engines_validator(self):
        with pytest.raises(core.IntelligenceError):
            ab.study_config(initial_train_months=3)
        with pytest.raises(core.IntelligenceError):
            ab.study_config(test_block_months=5)

    def test_the_study_runs_the_production_technical_config(self):
        cfg = ab.study_config(initial_train_months=12)
        assert cfg.initial_train_months == 12
        assert cfg.xgb["min_child_weight"] == 200            # the technical override, not the bare default
        assert cfg.embargo_days == core.load_config().embargo_days


# ── arms ─────────────────────────────────────────────────────────────────────

class TestArms:
    def test_both_arms_cover_identical_rows_and_labels(self, world):
        a, b, ca, cb = ab.build_arms(world["labels"], world["tables"], ("base",), BOTH)
        assert a.height == b.height > 0 and a.select(ab.KEY).equals(b.select(ab.KEY))
        assert a["label"].to_list() == b["label"].to_list()
        assert len(ca) == 33 and len(cb) == 39 and set(ca) < set(cb)
        assert core.feature_spec_hash(ca) == "2c0691b7333f"            # arm A IS the production feature spec

    def test_a_non_nested_pair_is_refused(self, world):
        with pytest.raises(core.IntelligenceError, match="nested"):
            ab.build_arms(world["labels"], world["tables"], BOTH, ("base",))

    def test_the_window_cuts_both_arms_the_same_way(self, world):
        cut = world["days"][900]
        a, b, _, _ = ab.build_arms(world["labels"], world["tables"], ("base",), BOTH, window_start=cut)
        assert a.height == b.height > 0
        assert a["decision_date"].min() >= cut and b["decision_date"].min() >= cut


class TestArmsEdgeCases:
    def test_arms_are_cut_to_the_intersection_when_a_block_has_a_shorter_history(self, world):
        cut = world["days"][1000]
        d1 = world["tables"]["1D"].with_columns(
            [pl.when(pl.col("bar_date") < cut).then(None).otherwise(pl.col(c)).alias(c) for c in fg.CANDLE_FEATURES])
        tables = {**world["tables"], "1D": d1}
        full_a = fg.training_frame(world["labels"], tables, ("base",))
        full_b = fg.training_frame(world["labels"], tables, BOTH)
        assert full_b.height < full_a.height                                # the block really shortens the history
        a, b, _, _ = ab.build_arms(world["labels"], tables, ("base",), BOTH)
        assert a.height == b.height == full_b.height and a.select(ab.KEY).equals(b.select(ab.KEY))

    def test_the_window_applies_to_arm_b_too(self, world):
        cut = world["days"][900]
        _, b, _, _ = ab.build_arms(world["labels"], world["tables"], ("base",), BOTH, window_start=cut)
        assert b["decision_date"].min() >= cut

    def test_arms_that_disagree_on_labels_are_refused(self, world, monkeypatch):
        real = fg.training_frame
        calls = []

        def flipped(labels, tables, blocks=("base",)):
            out = real(labels, tables, blocks)
            calls.append(blocks)
            return out.with_columns((1.0 - pl.col("label")).alias("label")) if len(calls) == 2 else out

        monkeypatch.setattr(fg, "training_frame", flipped)
        with pytest.raises(core.IntelligenceError, match="disagree on labels"):
            ab.build_arms(world["labels"], world["tables"], ("base",), BOTH)


# ── paired comparison on synthetic OOF frames ────────────────────────────────

def _oof(n_dates=240, per_date=25, seed=1):
    rng = np.random.default_rng(seed)
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(n_dates)]
    rows = [(d, f"S{k:02d}", "long", float(rng.random() < 0.3)) for d in days for k in range(per_date)]
    base = pl.DataFrame(rows, schema=["decision_date", "symbol", "direction", "label"], orient="row")
    return base.with_columns(pl.lit(1.0).alias("weight")), rng


def _with_p(base, p):
    return base.with_columns(pl.Series("p", np.clip(p, 0.01, 0.99)))


class TestPairedReport:
    def test_a_genuinely_better_arm_contributes(self):
        base, rng = _oof()
        y = base["label"].to_numpy()
        a = _with_p(base, 0.3 + 0.05 * rng.normal(size=base.height))                       # uninformative
        b = _with_p(base, 0.3 + 0.25 * (y - 0.3) + 0.05 * rng.normal(size=base.height))    # informative
        r = ab.paired_report(a, b, n_boot=60, block_days=10)
        assert r["verdict"] == "contributes" and r["delta_auc_ci95"][0] > 0 and r["delta_logloss_ci95"][1] < 0
        assert r["delta_auc"] > 0 and r["delta_logloss"] < 0                  # B - A: better B = higher AUC, lower logloss
        assert r["delta_auc_ci95"][0] <= r["delta_auc"] <= r["delta_auc_ci95"][1]
        assert r["arm_b"]["auc"] > r["arm_a"]["auc"] and r["arm_b"]["logloss"] < r["arm_a"]["logloss"]

    def test_identical_arms_show_exactly_no_difference(self):
        base, rng = _oof()
        a = _with_p(base, 0.3 + 0.1 * rng.normal(size=base.height))
        r = ab.paired_report(a, a, n_boot=40, block_days=10)
        assert r["delta_auc"] == 0.0 and r["delta_logloss"] == 0.0 and r["verdict"] == "no_evidence"

    def test_a_worse_arm_is_reported_as_worse(self):
        base, rng = _oof()
        y = base["label"].to_numpy()
        a = _with_p(base, 0.3 + 0.25 * (y - 0.3) + 0.05 * rng.normal(size=base.height))
        b = _with_p(base, 0.3 + 0.05 * rng.normal(size=base.height))
        r = ab.paired_report(a, b, n_boot=60, block_days=10)
        assert r["verdict"] == "worse" and r["delta_auc"] < 0 and r["delta_logloss"] > 0

    def test_the_verdict_needs_both_metrics(self):
        """AUC improves but logloss does not (a sharper-but-miscalibrated ranking): not `contributes`."""
        base, rng = _oof()
        y = base["label"].to_numpy()
        informative = 0.25 * (y - 0.3) + 0.05 * rng.normal(size=base.height)
        a = _with_p(base, 0.3 + 0.05 * rng.normal(size=base.height))
        b = _with_p(base, 0.9 - 0.6 * (1 - (informative > np.median(informative))) )        # same ranking power, absurd levels
        r = ab.paired_report(a, b, n_boot=60, block_days=10)
        assert r["delta_auc_ci95"][0] > 0                                   # the ranking really is better ...
        assert r["delta_logloss"] > 0 and r["verdict"] != "contributes"     # ... and still not enough alone

    def test_a_better_calibrated_arm_with_the_same_ranking_is_not_a_contribution(self):
        """Logloss improves, AUC cannot (a monotone transform): `contributes` needs a RANKING gain too."""
        base, rng = _oof()
        y = base["label"].to_numpy()
        score = 0.25 * (y - 0.3) + 0.05 * rng.normal(size=base.height)
        pct = score.argsort().argsort() / (score.size - 1)
        a = _with_p(base, 0.6 + 0.3 * pct)                                   # absurd levels, real ranking
        b = _with_p(base, 0.1 + 0.4 * pct)                                   # same ranking, sane levels
        r = ab.paired_report(a, b, n_boot=60, block_days=10)
        assert r["delta_auc"] == 0.0 and r["delta_logloss_ci95"][1] < 0
        assert r["verdict"] == "no_evidence"

    def test_the_interval_is_the_95_percent_one_and_gets_narrower_at_a_lower_level(self, monkeypatch):
        base, rng = _oof()
        a = _with_p(base, 0.3 + 0.1 * rng.normal(size=base.height))
        b = _with_p(base, 0.3 + 0.1 * rng.normal(size=base.height))
        assert ab.CI_LEVEL == 0.95
        wide = ab.paired_report(a, b, n_boot=120, block_days=10)["delta_auc_ci95"]
        monkeypatch.setattr(ab, "CI_LEVEL", 0.50)
        narrow = ab.paired_report(a, b, n_boot=120, block_days=10)["delta_auc_ci95"]
        assert (narrow[1] - narrow[0]) < 0.6 * (wide[1] - wide[0])

    def test_the_bootstrap_is_reproducible_and_seed_dependent(self):
        base, rng = _oof()
        a = _with_p(base, 0.3 + 0.1 * rng.normal(size=base.height))
        b = _with_p(base, 0.3 + 0.1 * rng.normal(size=base.height))
        r1, r2 = ab.paired_report(a, b, n_boot=40, seed=3), ab.paired_report(a, b, n_boot=40, seed=3)
        r3 = ab.paired_report(a, b, n_boot=40, seed=4)
        assert r1 == r2 and r1["delta_auc_ci95"] != r3["delta_auc_ci95"]

    def test_rows_are_paired_on_the_key_not_on_position(self):
        base, rng = _oof(n_dates=60)
        a = _with_p(base, 0.3 + 0.1 * rng.normal(size=base.height))
        b = a.sample(fraction=1.0, shuffle=True, seed=5)
        r = ab.paired_report(a, b, n_boot=20, block_days=5)
        assert r["delta_auc"] == 0.0 and r["n_rows"] == base.height

    def test_no_common_rows_is_reported_not_raised(self):
        base, rng = _oof(n_dates=40)
        a = _with_p(base, 0.3 + 0.1 * rng.normal(size=base.height))
        b = a.with_columns((pl.col("decision_date") + pl.duration(days=1000)).alias("decision_date"))
        assert ab.paired_report(a, b, n_boot=10)["status"] == "no_common_oof_rows"

    def test_block_bootstrap_groups_consecutive_dates(self):
        dates = np.array([date(2024, 1, 1) + timedelta(days=i // 3) for i in range(30)])    # 10 dates x 3 rows
        blocks = ab._blocks_of_dates(dates, 4)
        assert [len(b) for b in blocks] == [12, 12, 6]
        assert sorted(np.concatenate(blocks).tolist()) == list(range(30))


# ── end to end on the real engine ────────────────────────────────────────────

class TestRunAblation:
    def test_end_to_end_report_shape(self, world):
        r = ab.run_ablation(world["labels"], world["tables"], ("base",), BOTH, world["cfg"], n_boot=15, block_days=10)
        assert r["status"] == "ok" and r["n_folds"] >= 1
        assert r["arm_a"]["n_features"] == 33 and r["arm_b"]["n_features"] == 39
        assert r["arm_a"]["feature_hash"] == "2c0691b7333f" != r["arm_b"]["feature_hash"]
        p = r["paired"]
        assert p["verdict"] in ("contributes", "no_evidence", "worse") and p["n_rows"] > 0
        assert p["delta_auc_ci95"][0] <= p["delta_auc_ci95"][1]
        assert r["config"]["config_fingerprint"] == world["cfg"].fingerprint()
        json.dumps(r, default=str)                                         # serialisable

    def test_a_run_is_deterministic(self, world):
        kw = dict(n_boot=10, block_days=10, seed=9)
        r1 = ab.run_ablation(world["labels"], world["tables"], ("base",), BOTH, world["cfg"], **kw)
        r2 = ab.run_ablation(world["labels"], world["tables"], ("base",), BOTH, world["cfg"], **kw)
        assert r1["paired"] == r2["paired"]

    def test_a_history_too_short_for_any_fold_says_so(self, world):
        cfg = world["cfg"].__class__(**{**world["cfg"].__dict__, "initial_train_months": 120})
        r = ab.run_ablation(world["labels"], world["tables"], ("base",), BOTH, cfg, n_boot=5)
        assert r["status"] == "no_oof_blocks" and r["span"]["n_blocks"] == 0

    def test_the_window_is_reflected_in_the_report(self, world):
        cut = world["days"][700]
        r = ab.run_ablation(world["labels"], world["tables"], ("base",), BOTH, world["cfg"], window_start=cut,
                            n_boot=5, block_days=10)
        assert r["window_start"] == str(cut) and r["span"]["first_label_date"] >= str(cut)


class TestCli:
    def test_span_check_prints_json_and_exits_zero(self, capsys):
        rc = ab.main(["--span-check", "--first-label-date", "2024-10-01", "--last-decision-date", "2026-10-16",
                      "--initial-train-months", "12"])
        assert rc == 0 and json.loads(capsys.readouterr().out)["n_blocks"] == 5

    def _files(self, world, tmp_path):
        labels_dir = tmp_path / "labels"
        labels_dir.mkdir()
        world["labels"].write_parquet(labels_dir / "labels_x.parquet")
        return labels_dir, world["labels"]["label_end_date"].max()

    def test_the_cli_runs_end_to_end_and_writes_only_the_out_file(self, world, tmp_path, monkeypatch, capsys):
        labels_dir, run_date = self._files(world, tmp_path)
        monkeypatch.setattr(ab, "study_config", lambda *a, **k: world["cfg"])
        out = tmp_path / "study" / "ablation.json"
        rc = ab.main(["--labels-dir", str(labels_dir), "--signals-dir", str(world["dir"]), "--run-date", str(run_date),
                      "--n-boot", "8", "--block-days", "10", "--out", str(out)])
        assert rc == 0
        report = json.loads(out.read_text())
        assert report["status"] == "ok" and report["arm_b"]["n_features"] == 39 and report["arm_a"]["n_features"] == 33
        assert json.loads(capsys.readouterr().out)["paired"] == report["paired"]
        assert [q.name for q in out.parent.iterdir()] == ["ablation.json"]          # no temp residue, nothing else
        assert [q.name for q in labels_dir.iterdir()] == ["labels_x.parquet"]       # inputs untouched

    def test_a_history_with_no_fold_exits_two_but_still_reports(self, world, tmp_path, monkeypatch):
        labels_dir, run_date = self._files(world, tmp_path)
        cfg = world["cfg"].__class__(**{**world["cfg"].__dict__, "initial_train_months": 120})
        monkeypatch.setattr(ab, "study_config", lambda *a, **k: cfg)
        out = tmp_path / "o.json"
        rc = ab.main(["--labels-dir", str(labels_dir), "--signals-dir", str(world["dir"]), "--run-date", str(run_date),
                      "--out", str(out)])
        assert rc == 2 and json.loads(out.read_text())["status"] == "no_oof_blocks"

    def test_a_stale_label_file_is_refused_by_the_production_reader(self, world, tmp_path, monkeypatch):
        labels_dir, run_date = self._files(world, tmp_path)
        monkeypatch.setattr(ab, "study_config", lambda *a, **k: world["cfg"])
        with pytest.raises(Exception, match="older than"):
            ab.main(["--labels-dir", str(labels_dir), "--signals-dir", str(world["dir"]),
                     "--run-date", str(run_date + timedelta(days=400))])

    def test_span_check_without_dates_is_a_usage_error(self):
        with pytest.raises(SystemExit):
            ab.main(["--span-check"])

    def test_a_nonsensical_override_is_refused_before_any_work(self):
        with pytest.raises(core.IntelligenceError):
            ab.main(["--span-check", "--first-label-date", "2024-10-01", "--last-decision-date", "2026-10-16",
                     "--initial-train-months", "2"])
