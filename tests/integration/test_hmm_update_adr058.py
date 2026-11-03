"""
tests/integration/test_hmm_update_adr058.py — ADR-058 structural pins and the two offline utilities.

  * job graph   silver_fred_daily (HMM-R03): where it sits, what it depends on, that it stays on the
                Silver/Bronze side of the HMM engine's domain boundary
  * consumer rule (HMM-R02): nothing in src/ outside the HMM package filters on an `in_sample` column
  * isolation   scripts/studies/hmm_replay.py and hmm_catchup_audit.py are offline utilities: they import
                nothing from `src`, nothing in `src` imports them, no job runs them, no f-string SQL
  * replay      the independent forward filter reproduces what production stored; the lag harness moves
                only the lagged feature
  * audit       the catch-up audit fails on exactly the conditions the guide names
"""
import ast
import json
import pathlib
import re
import shutil
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from scripts.studies import hmm_catchup_audit as audit_mod
from scripts.studies import hmm_replay as replay
from src.gold.hmm import hmm_market_regime as hk
from src.scheduler.job_registry import DAILY_SEQUENCE, JOB_REGISTRY, WEEKLY_SEQUENCE
from tests.integration.test_hmm_domain_independence_adr051 import FOREIGN_JOBS, HMM_JOBS, _upstream
from tests.unit.test_hmm_market_regime import _cfg as market_cfg
from tests.unit.test_hmm_market_regime import _panel as market_panel

SRC = pathlib.Path("src")
SCRIPTS = pathlib.Path("scripts/studies")
UTILITIES = (SCRIPTS / "hmm_replay.py", SCRIPTS / "hmm_catchup_audit.py")


def _imports(path: pathlib.Path) -> list[str]:
    out = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = ("." * node.level) + (node.module or "")
            out.append(base)
            out += [f"{base}.{a.name}" for a in node.names]
    return out


# ═══ job graph: HMM-R03 ══════════════════════════════════════════════════════

class TestDailyFredSlice:
    def test_registered_on_the_silver_layer_with_exactly_its_two_bronze_sources(self):
        j = JOB_REGISTRY["silver_fred_daily"]
        assert j["layer"] == "silver" and j["depends_on"] == ["bronze_fred_daily", "bronze_treasury"]
        assert "stale_tolerance" not in j                         # exact-date dependencies: a stalled fetch must show
        assert not {"run_on_weekdays", "run_on_day_of_month"} & set(j)   # a daily job

    def test_runs_daily_after_its_sources_and_before_the_jobs_that_read_it(self):
        idx = {n: i for i, n in enumerate(DAILY_SEQUENCE)}
        assert "silver_fred_daily" in WEEKLY_SEQUENCE
        assert max(idx["bronze_fred_daily"], idx["bronze_treasury"]) < idx["silver_fred_daily"]
        assert idx["silver_fred_daily"] < idx["gold_raw_macro"] and idx["silver_fred_daily"] < idx["gold_raw_market"]

    def test_sits_after_the_technical_engine_so_its_failure_cannot_stop_it(self):
        """The runner stops at the first failing job. The slice feeds only the HMM raw jobs, so it is placed
        with them, behind the technical engine (same reasoning as ADR-051/055 for the HMM block)."""
        idx = {n: i for i, n in enumerate(DAILY_SEQUENCE)}
        assert idx["signal_aggregation"] < idx["silver_fred_daily"]

    def test_is_upstream_of_hmm_raw_jobs_only(self):
        consumers = {n for n, j in JOB_REGISTRY.items() if "silver_fred_daily" in _upstream(n) or
                     "silver_fred_daily" in j.get("depends_on", [])}
        assert {"gold_raw_macro", "gold_raw_market"} <= consumers
        assert consumers - HMM_JOBS == set(), consumers - HMM_JOBS     # nothing outside the HMM engine reads it
        for foreign in FOREIGN_JOBS:
            assert "silver_fred_daily" not in _upstream(foreign), foreign

    def test_its_own_upstream_is_bronze_only(self):
        assert all(JOB_REGISTRY[u]["layer"] == "bronze" for u in _upstream("silver_fred_daily"))

    def test_raw_jobs_keep_the_weekly_edge_and_gain_a_tight_one(self):
        for job in ("gold_raw_macro", "gold_raw_market"):
            j = JOB_REGISTRY[job]
            assert {"silver_macro", "silver_fred_daily"} <= set(j["depends_on"])
            assert j["stale_tolerance"]["silver_fred_daily"] == 3 and j["stale_tolerance"]["silver_macro"] == 7

    def test_the_hmm_gate_scope_now_tracks_the_daily_bronze_feeds(self):
        """Before ADR-058 the HMM scope's transitive upstream held only the WEEKLY macro Bronze job: a stalled
        daily FRED / Treasury fetch (the inputs that went 3-10 days stale) could not show in that scope."""
        from src.scheduler.job_registry import GATE_DOMAINS
        closure = set(GATE_DOMAINS["hmm"]) | {u for j in GATE_DOMAINS["hmm"] for u in _upstream(j)}
        assert {"silver_fred_daily", "bronze_fred_daily", "bronze_treasury"} <= closure


# ═══ consumer rule: HMM-R02 ══════════════════════════════════════════════════

class TestConsumerRule:
    FILTER = re.compile(r"(WHERE|AND|OR)\s+(NOT\s+)?[\w.]*in_sample|filter\([^)]*in_sample|\.is_in\([^)]*in_sample",
                        re.IGNORECASE)

    def test_no_src_module_outside_the_hmm_package_filters_on_in_sample(self):
        offenders = []
        for p in sorted(SRC.rglob("*.py")):
            if pathlib.Path("src/gold/hmm") in p.parents:
                continue
            if self.FILTER.search(p.read_text()):
                offenders.append(str(p))
        assert offenders == []

    def test_the_two_documented_passthroughs_say_it_is_not_an_out_of_sample_flag(self):
        for path in ("src/gold/screener.py", "src/backtest/pit_data.py"):
            text = pathlib.Path(path).read_text()
            assert "ADR-058 HMM-R02 consumer rule" in text, path

    def test_both_regime_modules_document_the_rule(self):
        for path in ("src/gold/hmm/hmm_macro_regime.py", "src/gold/hmm/hmm_market_regime.py"):
            doc = ast.get_docstring(ast.parse(pathlib.Path(path).read_text()))
            assert "CONSUMER RULE (ADR-058 HMM-R02)" in doc and "never on context_regime.in_sample" in doc, path


# ═══ isolation of the offline utilities ══════════════════════════════════════

class TestUtilitiesAreOffline:
    @pytest.mark.parametrize("path", UTILITIES, ids=lambda p: p.name)
    def test_imports_nothing_from_src(self, path):
        assert [i for i in _imports(path) if i == "src" or i.startswith("src.")] == []

    def test_no_src_module_imports_scripts_studies(self):
        offenders = [str(p) for p in sorted(SRC.rglob("*.py"))
                     if any(i.startswith("scripts") for i in _imports(p))]
        assert offenders == []

    def test_no_job_or_schedule_entry_runs_them(self):
        names = " ".join(list(JOB_REGISTRY) + DAILY_SEQUENCE + WEEKLY_SEQUENCE)
        assert "replay" not in names and "catchup" not in names
        assert "hmm_replay" not in pathlib.Path("src/scheduler/job_registry.py").read_text()

    @pytest.mark.parametrize("path", UTILITIES, ids=lambda p: p.name)
    def test_no_f_string_sql(self, path):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.JoinedStr):
                text = "".join(v.value for v in node.values if isinstance(v, ast.Constant))
                assert not re.search(r"\b(SELECT|FROM|WHERE)\b", text), text

    def test_the_replay_harness_does_not_touch_sql_at_all(self):
        assert "duckdb" not in " ".join(_imports(SCRIPTS / "hmm_replay.py"))


# ═══ replay harness ══════════════════════════════════════════════════════════

FIT_DATE = date(2023, 6, 1)


@pytest.fixture(scope="module")
def market_world(tmp_path_factory):
    root = tmp_path_factory.mktemp("adr058_replay")
    cfg = market_cfg()
    panel, _ = market_panel(n=1200)
    old = (hk.MODELS_DIR, hk.MARKET_REGIME_PATH)
    hk.MODELS_DIR, hk.MARKET_REGIME_PATH = root / "models", root / "market_regime.parquet"
    try:
        hk.fit(panel.filter(pl.col("as_of_date") <= FIT_DATE), FIT_DATE, cfg)
        hk.append_new_rows(panel, cfg, FIT_DATE)
    finally:
        hk.MODELS_DIR, hk.MARKET_REGIME_PATH = old
    return {"model": replay.load_model(root / "models" / "market.json"), "panel": panel,
            "stored": pl.read_parquet(root / "market_regime.parquet")}


class TestReproduction:
    def test_an_independent_filter_reproduces_the_production_store(self, market_world):
        rep = replay.reproduce(market_world["model"], market_world["panel"], market_world["stored"], "market")
        assert rep.n_rows == 1200 and rep.passed
        assert rep.max_abs_diff < 1e-9 and rep.label_agreement == 1.0       # the same maths, not merely close

    def test_a_tampered_posterior_fails_the_gate(self, market_world):
        s = market_world["stored"]
        bad = s.with_columns(pl.when(pl.col("date") == s["date"][600]).then(0.5).otherwise(pl.col("market_post_mixed"))
                             .alias("market_post_mixed"))
        rep = replay.reproduce(market_world["model"], market_world["panel"], bad, "market")
        assert not rep.passed and rep.max_abs_diff > replay.GATE_MAX_ABS_DIFF

    def test_a_flipped_label_fails_the_gate_even_with_matching_posteriors(self, market_world):
        s = market_world["stored"]
        flipped = s.with_columns(pl.when(pl.col("date") == s["date"][600]).then(pl.lit("risk_on"))
                                 .otherwise(pl.col("market_label")).alias("market_label"))
        if flipped["market_label"].to_list() == s["market_label"].to_list():
            pytest.skip("row 600 was already risk_on")
        rep = replay.reproduce(market_world["model"], market_world["panel"], flipped, "market")
        assert rep.label_agreement < 1.0 and not rep.passed

    def test_no_overlap_is_a_failure_not_a_pass(self, market_world):
        empty = market_world["stored"].head(0)
        assert not replay.reproduce(market_world["model"], market_world["panel"], empty, "market").passed

    def test_state_memory_is_carried_a_one_row_replay_differs_from_the_full_replay(self, market_world):
        """The orphaned hmm_regime.classify() scored one observation at a time and never used the
        transition matrix. The harness must not: filtering the last row alone is a different answer."""
        m, panel = market_world["model"], market_world["panel"].sort("as_of_date")
        full = replay.forward_filter(m, panel.select(m["features"]).to_numpy())[-1]
        alone = replay.forward_filter(m, panel.tail(1).select(m["features"]).to_numpy())[-1]
        assert not np.allclose(full, alone)


def _two_state_model():
    return {"features": ["VIXCLS"], "labels": ["calm", "stress"],
            "scaler": {"center": [0.0], "scale": [1.0], "clip": 4.0},
            "startprob": [0.5, 0.5], "transmat": [[0.97, 0.03], [0.03, 0.97]],
            "means": [[-1.0], [1.5]], "vars": [[1.0], [1.0]]}


def _step_world(n_days=400):
    """Calendar-daily reference: calm for 100 days, stress for 100, calm, stress. Panel = weekdays whose
    feature column is a placeholder (lag_sensitivity overwrites it from the reference)."""
    start = date(2024, 1, 1)
    days = [start + timedelta(days=i) for i in range(n_days)]
    vals = [(1.5 if (i // 100) % 2 else -1.0) for i in range(n_days)]
    ref = pl.DataFrame({"date": days, "value": vals}, schema={"date": pl.Date, "value": pl.Float64})
    wd = [d for d in days if d.weekday() < 5]
    panel = pl.DataFrame({"as_of_date": wd, "VIXCLS": [0.0] * len(wd)}, schema={"as_of_date": pl.Date, "VIXCLS": pl.Float64})
    return ref, panel


class TestLagSensitivity:
    def test_lagged_values_pick_the_latest_observation_on_or_before_asof_minus_lag(self):
        ref = pl.DataFrame({"date": [date(2026, 1, 1), date(2026, 1, 5)], "value": [10.0, 50.0]})
        asof = pl.Series([date(2026, 1, 6), date(2026, 1, 8), date(2026, 1, 2)], dtype=pl.Date)
        v = replay.lagged_values(ref, asof, 1)
        assert v[0] == 50.0 and v[1] == 50.0 and v[2] == 10.0
        v3 = replay.lagged_values(ref, asof, 3)                    # 01-03 -> still 10; 01-05 -> 50; before data -> NaN
        assert v3[0] == 10.0 and v3[1] == 50.0 and np.isnan(v3[2])

    def test_the_baseline_lag_against_itself_changes_nothing(self):
        ref, panel = _step_world()
        t = replay.lag_sensitivity(_two_state_model(), panel, "VIXCLS", ref, lags=[1], target="stress")
        assert t["share_labels_changed"][0] == 0.0 and t["mean_entry_delay_rows"][0] == 0.0

    def test_older_inputs_delay_the_stress_entry_and_the_delay_grows_with_the_lag(self):
        ref, panel = _step_world()
        t = replay.lag_sensitivity(_two_state_model(), panel, "VIXCLS", ref, lags=[1, 3, 7, 10], target="stress")
        delay = dict(zip(t["lag_days"], t["mean_entry_delay_rows"]))
        changed = dict(zip(t["lag_days"], t["share_labels_changed"]))
        assert delay[3] > delay[1] and delay[7] > delay[3] and delay[10] > delay[7] > 0
        assert changed[10] > changed[3] > 0

    def test_only_the_named_feature_is_replaced(self):
        ref, panel = _step_world()
        model = _two_state_model()
        model["features"] = ["VIXCLS", "OTHER"]
        model.update({"scaler": {"center": [0.0, 0.0], "scale": [1.0, 1.0], "clip": 4.0},
                      "means": [[-1.0, 0.0], [1.5, 0.0]], "vars": [[1.0, 1.0], [1.0, 1.0]]})
        panel = panel.with_columns(pl.lit(0.25).alias("OTHER"))
        _, post_a, _ = replay.filter_panel(model, panel, {"VIXCLS": replay.lagged_values(ref, panel["as_of_date"], 1)})
        _, post_b, _ = replay.filter_panel(model, panel.with_columns(pl.lit(0.25).alias("OTHER")),
                                           {"VIXCLS": replay.lagged_values(ref, panel["as_of_date"], 1)})
        assert np.allclose(post_a, post_b, equal_nan=True)

    def test_entry_matching_counts_unmatched_entries_separately(self):
        base = [None, "a", "b", "b", "a", "a", "b"]
        lag = [None, "a", "a", "a", "a", "a", "a"]                 # never enters b
        assert replay.entry_delays(base, lag, "b") == ([], 2)
        lag2 = [None, "a", "a", "b", "b", "a", "a"]                # one enters b one row later than baseline's first
        offs, un = replay.entry_delays(base, lag2, "b", max_match=15)
        assert offs[0] == 1 and un + len(offs) == 2


# ═══ catch-up audit ══════════════════════════════════════════════════════════

SINCE = date(2026, 10, 16)


def _write_hmm_dir(root, *, market_rows=None, macro_rows=None, context_rows=None,
                   market_fp="fpM", macro_fp="fpA", legacy=False):
    root.mkdir(parents=True, exist_ok=True)
    (root / "models").mkdir(exist_ok=True)
    (root / "models" / "market.json").write_text(json.dumps({"config_fingerprint": market_fp}))
    for d in audit_mod.MACRO_DOMAINS:
        (root / "models" / f"macro_{d}.json").write_text(json.dumps({"config_fingerprint": macro_fp}))

    def store(rows, fp):
        cols = {"date": [r[0] for r in rows], "in_sample": [r[1] for r in rows],
                "model_config_fingerprint": [r[3] if len(r) > 3 else fp for r in rows]}
        schema = {"date": pl.Date, "in_sample": pl.Boolean, "model_config_fingerprint": pl.String}
        if not legacy:
            cols["model_train_end"], schema["model_train_end"] = [r[2] for r in rows], pl.Date
        return pl.DataFrame(cols, schema=schema)

    day = lambda n: SINCE + timedelta(days=n)
    market_rows = market_rows or [(day(-1), True, day(-1)), (day(0), False, day(-1)), (day(3), False, day(-1))]
    macro_rows = macro_rows or [(day(-1), True, day(-1)), (day(3), False, day(-1))]
    store(market_rows, market_fp).write_parquet(root / "market_regime.parquet")
    store(macro_rows, macro_fp).write_parquet(root / "macro_regime.parquet")
    context_rows = context_rows or [(day(-1), True, True), (day(3), False, False)]
    pl.DataFrame({"date": [r[0] for r in context_rows], "macro_in_sample": [r[1] for r in context_rows],
                  "market_in_sample": [r[2] for r in context_rows]},
                 schema={"date": pl.Date, "macro_in_sample": pl.Boolean, "market_in_sample": pl.Boolean}
                 ).write_parquet(root / "context_regime.parquet")
    return root


class TestCatchupAudit:
    def test_a_clean_catch_up_passes(self, tmp_path):
        a = audit_mod.audit(_write_hmm_dir(tmp_path / "h"), SINCE, expect_context_rows=1)
        assert a.passed and a.oos_seen_by_model == {"market": 0, "macro": 0} and a.context_both_oos_since == 1

    def test_an_out_of_sample_row_scored_by_a_model_that_saw_it_fails(self, tmp_path):
        day = lambda n: SINCE + timedelta(days=n)
        root = _write_hmm_dir(tmp_path / "h", market_rows=[(day(-1), True, day(-1)), (day(3), False, day(5))])
        a = audit_mod.audit(root, SINCE)
        assert not a.passed and a.oos_seen_by_model["market"] == 1 and any("A1 market" in p for p in a.problems)

    def test_the_boundary_is_inclusive_a_model_trained_through_the_row_date_has_seen_it(self, tmp_path):
        day = lambda n: SINCE + timedelta(days=n)
        root = _write_hmm_dir(tmp_path / "h", market_rows=[(day(-1), True, day(-1)), (day(3), False, day(3))])
        a = audit_mod.audit(root, SINCE)
        assert not a.passed and a.oos_seen_by_model["market"] == 1

    def test_a_new_row_with_a_fingerprint_that_is_not_on_disk_fails(self, tmp_path):
        day = lambda n: SINCE + timedelta(days=n)
        root = _write_hmm_dir(tmp_path / "h", market_rows=[(day(-1), True, day(-1), "fpM"),
                                                           (day(3), False, day(-1), "other")])
        a = audit_mod.audit(root, SINCE)
        assert not a.passed and any("A4 market" in p and "other" in p for p in a.problems)

    def test_the_expected_context_row_count_is_enforced(self, tmp_path):
        a = audit_mod.audit(_write_hmm_dir(tmp_path / "h"), SINCE, expect_context_rows=10)
        assert not a.passed and any(p.startswith("A3") for p in a.problems)

    def test_a_legacy_store_is_reported_as_unverifiable_never_as_clean(self, tmp_path):
        a = audit_mod.audit(_write_hmm_dir(tmp_path / "h", legacy=True), SINCE)
        assert a.unverifiable_legacy_oos["market"] == 2 and a.unverifiable_legacy_oos["macro"] == 1
        assert a.oos_seen_by_model == {"market": 0, "macro": 0}      # not testable != violated; the count above says so

    def test_a_missing_store_is_a_problem(self, tmp_path):
        root = _write_hmm_dir(tmp_path / "h")
        (root / "context_regime.parquet").unlink()
        a = audit_mod.audit(root, SINCE)
        assert not a.passed and any("context_regime" in p for p in a.problems)

    def test_the_cli_exit_status_follows_the_verdict(self, tmp_path, capsys):
        root = _write_hmm_dir(tmp_path / "h")
        assert audit_mod.main(["--hmm-dir", str(root), "--since", SINCE.isoformat()]) == 0
        assert "AUDIT PASS" in capsys.readouterr().out
        assert audit_mod.main(["--hmm-dir", str(root), "--since", SINCE.isoformat(), "--expect-context-rows", "9"]) == 1
