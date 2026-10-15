"""
tests/unit/test_hmm_core.py — ADR-051 (GMI-HMM2-001): src/gold/hmm/hmm_core.py, the engine both
regime branches share.

What these pin, in order of how much a silent break would cost:
  * the forward filter equals an INDEPENDENT reference implementation, never looks ahead, and
    marginalises a null feature exactly (a missing value is missing, not zero)
  * states come out ORDERED ascending along the signed weights (state 0 is always the first label)
  * a degenerate fit is REFUSED rather than persisted, and a skipped/refused fit returns no record
  * the persisted record round-trips through JSON and reproduces the in-memory posteriors
  * persistence helpers are atomic (no temp debris, original untouched on failure) and a foreign
    or stale model file raises instead of silently scoring
"""
import json
import os
from datetime import date, timedelta
from math import exp, pi, sqrt

import numpy as np
import polars as pl
import pytest

from src.gold.hmm import hmm_core as hc
from src.gold.hmm.config import FitSettings

FIT = FitSettings(model_version="t", n_states=3, n_random_seeds=3, n_iter=100, tol=1e-4, min_obs_to_fit=100,
                  short_history_min_obs=500, min_state_share=0.05, var_floor=1e-4, scaler_clip=4.0)


class _Err(RuntimeError):
    pass


def _chain(n=700, seed=0, k=3, stay=0.97):
    rng = np.random.default_rng(seed)
    s, out = 1, []
    for _ in range(n):
        if rng.random() > stay:
            s = int(rng.integers(0, k))
        out.append(s)
    return np.array(out)


def _blobs(n=700, seed=0, d=3, sep=3.0):
    """Sticky 3-state chain; every feature's mean is (state-1)*sep, unit noise."""
    st = _chain(n, seed)
    rng = np.random.default_rng(seed + 1)
    return (st - 1)[:, None] * sep + rng.normal(0, 1, (n, d)), st


def _ref_filter(Z, start, trans, means, vars_):
    """Independent textbook forward filter (fully observed rows only)."""
    k = len(start)
    alpha, out = None, []
    for t in range(Z.shape[0]):
        pred = start if alpha is None else alpha @ trans
        lik = np.ones(k)
        for s in range(k):
            for j in range(Z.shape[1]):
                if not np.isnan(Z[t, j]):
                    lik[s] *= exp(-0.5 * (Z[t, j] - means[s, j]) ** 2 / vars_[s, j]) / sqrt(2 * pi * vars_[s, j])
        a = pred * lik
        alpha = a / a.sum()
        out.append(alpha)
    return np.array(out)


TWO = dict(start=np.array([0.5, 0.5]), trans=np.array([[0.9, 0.1], [0.2, 0.8]]),
           means=np.array([[-1.0, -0.5], [1.0, 0.5]]), variances=np.array([[1.0, 2.0], [1.5, 1.0]]))


# ═══ Robust scaling ══════════════════════════════════════════════════════════

class TestScaler:
    def test_center_is_median_and_scale_is_scaled_mad(self):
        X = np.array([[1.0], [2.0], [3.0], [4.0], [100.0]])
        c, s = hc.fit_scaler(X)
        assert c[0] == 3.0 and s[0] == pytest.approx(1.0 * 1.4826)         # MAD = median(|x-3|) = 1

    def test_zero_mad_falls_back_to_std_then_to_one(self):
        flat_with_blip = np.array([[5.0]] * 9 + [[9.0]])                   # MAD 0, std > 0
        c, s = hc.fit_scaler(flat_with_blip)
        assert s[0] == pytest.approx(np.std(flat_with_blip))
        c, s = hc.fit_scaler(np.full((6, 1), 2.0))                         # MAD 0 AND std 0
        assert s[0] == 1.0 and c[0] == 2.0

    def test_columns_scale_independently(self):
        X = np.column_stack([np.arange(11.0), 10 * np.arange(11.0)])
        c, s = hc.fit_scaler(X)
        assert s[1] == pytest.approx(10 * s[0])

    def test_apply_clips_both_tails_and_keeps_nan(self):
        Z = hc.apply_scaler(np.array([[100.0], [-100.0], [0.5], [np.nan]]), np.array([0.0]), np.array([1.0]), 4.0)
        assert Z[:, 0][:3].tolist() == [4.0, -4.0, 0.5] and np.isnan(Z[3, 0])


# ═══ Forward filter ══════════════════════════════════════════════════════════

class TestForwardFilter:
    def test_matches_an_independent_reference(self):
        Z, _ = _blobs(n=120, seed=2, d=2)
        got = hc.forward_filter(Z, TWO["start"], TWO["trans"], TWO["means"], TWO["variances"])
        assert np.allclose(got, _ref_filter(Z, TWO["start"], TWO["trans"], TWO["means"], TWO["variances"]))
        assert np.allclose(got.sum(axis=1), 1.0)

    def test_no_lookahead_prefix_equals_full(self):
        Z, _ = _blobs(n=80, seed=4, d=2)
        full = hc.forward_filter(Z, **{"start": TWO["start"], "trans": TWO["trans"], "means": TWO["means"],
                                       "variances": TWO["variances"]})
        for cut in (1, 7, 40):
            pre = hc.forward_filter(Z[:cut], TWO["start"], TWO["trans"], TWO["means"], TWO["variances"])
            assert np.array_equal(pre, full[:cut])

    def test_a_null_feature_is_marginalised_exactly(self):
        """With a diagonal covariance, dropping a dimension from the emission is EXACTLY the filter
        run on the remaining dimension alone — not a zero-fill, not an imputation."""
        Z, _ = _blobs(n=60, seed=6, d=2)
        masked = Z.copy()
        masked[:, 1] = np.nan
        got = hc.forward_filter(masked, TWO["start"], TWO["trans"], TWO["means"], TWO["variances"])
        one_dim = hc.forward_filter(Z[:, :1], TWO["start"], TWO["trans"], TWO["means"][:, :1], TWO["variances"][:, :1])
        assert np.allclose(got, one_dim)

    def test_rows_before_the_first_observation_have_no_posterior(self):
        Z = np.array([[np.nan, np.nan], [np.nan, np.nan], [0.3, 0.1]])
        post = hc.forward_filter(Z, TWO["start"], TWO["trans"], TWO["means"], TWO["variances"])
        assert np.isnan(post[:2]).all() and not np.isnan(post[2]).any()

    def test_an_unobserved_later_row_is_a_pure_prediction_step(self):
        Z = np.array([[1.2, 0.4], [np.nan, np.nan], [np.nan, np.nan]])
        post = hc.forward_filter(Z, TWO["start"], TWO["trans"], TWO["means"], TWO["variances"])
        assert np.allclose(post[1], post[0] @ TWO["trans"])
        assert np.allclose(post[2], post[1] @ TWO["trans"])

    def test_state_memory_flows_through_the_transition_matrix(self):
        """A single ambiguous observation after strong evidence stays in the sticky state."""
        Z = np.array([[3.0, 2.0]] * 5 + [[0.0, 0.0]])
        post = hc.forward_filter(Z, TWO["start"], TWO["trans"], TWO["means"], TWO["variances"])
        assert post[-1].argmax() == 1 and post[-1][1] > post[-2][0]

    def test_extreme_row_does_not_underflow_to_nan(self):
        post = hc.forward_filter(np.array([[1e6, -1e6]]), TWO["start"], TWO["trans"], TWO["means"], TWO["variances"])
        assert np.isfinite(post).all() and post.sum() == pytest.approx(1.0)


# ═══ Fitting ═════════════════════════════════════════════════════════════════

class TestFitDomain:
    def test_recovers_ordered_states_and_the_latent_chain(self):
        Z, st = _blobs(seed=1)
        w = np.array([1.0, 1.0, 0.0])
        cand, audit = hc.fit_domain(Z, w, FIT)
        assert cand is not None and not cand.degenerate
        assert list(np.argsort(cand.means @ w)) == [0, 1, 2]                    # ascending along the weights
        post = hc.forward_filter(Z, cand.start, cand.trans, cand.means, cand.vars)
        assert (post.argmax(axis=1) == st).mean() > 0.97

    def test_negative_weights_flip_the_ordering(self):
        """State 0 is the LOWEST oriented composite: with a negative weight the first state is the
        one with the highest raw feature mean."""
        Z, st = _blobs(seed=1)
        cand, _ = hc.fit_domain(Z, np.array([-1.0, -1.0, -1.0]), FIT)
        assert cand.means[0].mean() > cand.means[2].mean()

    def test_zero_weight_feature_is_in_the_model_but_out_of_the_ordering(self):
        Z, _ = _blobs(seed=1)
        w = np.array([1.0, 0.0, 0.0])
        cand, _ = hc.fit_domain(Z, w, FIT)
        assert cand.means.shape == (3, 3)                                       # all 3 features modelled
        assert list(np.argsort(cand.means[:, 0])) == [0, 1, 2]                  # ordered by feature 0 only

    def test_audit_lists_the_composite_start_and_every_random_seed(self):
        Z, _ = _blobs(seed=1)
        _, audit = hc.fit_domain(Z, np.ones(3), FIT)
        assert [a["init"] for a in audit] == ["composite", "random:0", "random:1", "random:2"]
        assert all("loglik" in a or "error" in a for a in audit)

    def test_winner_has_the_highest_likelihood_among_non_degenerate_candidates(self):
        Z, _ = _blobs(seed=1)
        cand, audit = hc.fit_domain(Z, np.ones(3), FIT)
        ok = [a["loglik"] for a in audit if "loglik" in a and not a["degenerate"]]
        assert cand.loglik == pytest.approx(max(ok))

    def test_deterministic(self):
        Z, _ = _blobs(seed=1)
        a, _ = hc.fit_domain(Z, np.ones(3), FIT)
        b, _ = hc.fit_domain(Z, np.ones(3), FIT)
        assert np.array_equal(a.means, b.means) and a.loglik == b.loglik and a.init == b.init

    def test_a_fit_where_every_candidate_is_degenerate_is_refused(self):
        """min_state_share at its ceiling (1/3) cannot be met by a 70/20/10 world: every candidate has
        a state below it, so there is NO winner — never a persisted garbage model."""
        rng = np.random.default_rng(0)
        Z = np.concatenate([rng.normal(-4, 0.3, (490, 2)), rng.normal(0, 0.3, (140, 2)), rng.normal(4, 0.3, (70, 2))])
        strict = FitSettings(**{**FIT.__dict__, "min_state_share": 0.33})
        cand, audit = hc.fit_domain(Z, np.ones(2), strict)
        assert cand is None and len(audit) == 4

    def test_composite_init_refuses_bins_too_small_to_estimate(self):
        """All tertile edges collapse onto the same value -> two empty bins -> no deterministic start."""
        Z = np.array([[0.0]] * 5 + [[9.0]])
        assert hc._composite_init(Z, np.array([1.0]), 3, 1e-4) is None

    def test_composite_init_builds_a_normalised_start_from_tertile_bins(self):
        Z, _ = _blobs(n=300, seed=3, d=2)
        start, trans, means, variances = hc._composite_init(Z, np.array([1.0, 1.0]), 3, 1e-4)
        assert start.sum() == pytest.approx(1.0) and np.allclose(trans.sum(axis=1), 1.0)
        assert (trans > 0).all()                                                # +1 smoothing: no zero entries
        assert means.shape == (3, 2) and (variances >= 1e-4).all()
        assert list(np.argsort(means.sum(axis=1))) == [0, 1, 2]                 # bins are ordered along the composite


class TestFitModel:
    def _panel(self, n=700, seed=1, null_rows=()):
        Z, _ = _blobs(n=n, seed=seed)
        days = [date(2018, 1, 1) + timedelta(days=i) for i in range(n)]
        cols = {"as_of_date": days, "a": Z[:, 0].tolist(), "b": Z[:, 1].tolist(), "c": Z[:, 2].tolist()}
        df = pl.DataFrame(cols, schema={"as_of_date": pl.Date, "a": pl.Float64, "b": pl.Float64, "c": pl.Float64})
        for r in null_rows:
            df = df.with_columns(pl.when(pl.int_range(pl.len()) == r).then(None).otherwise(pl.col("b")).alias("b"))
        return df

    def _fit(self, panel, fit=FIT):
        return hc.fit_model(panel, "as_of_date", "demo", ["a", "b", "c"], np.array([1.0, 1.0, 0.0]),
                            ["lo", "mid", "hi"], fit, "fp123", date(2020, 6, 1), "9.9", 7)

    def test_record_carries_everything_inference_and_audit_need(self):
        rec, status = self._fit(self._panel())
        assert status.startswith("fitted: n=700 2018-01-01..2019-12-01")
        assert {"schema", "domain", "processing_version", "model_version", "config_fingerprint", "features", "weights",
                "labels", "scaler", "startprob", "transmat", "means", "vars", "train_start", "train_end", "n_obs",
                "short_history", "fit_date", "loglik", "converged", "init", "state_shares", "candidates"} == set(rec)
        assert (rec["schema"], rec["domain"], rec["processing_version"], rec["model_version"]) == (7, "demo", "9.9", "t")
        assert rec["config_fingerprint"] == "fp123" and rec["fit_date"] == "2020-06-01"
        assert rec["features"] == ["a", "b", "c"] and rec["labels"] == ["lo", "mid", "hi"]
        assert rec["train_start"] == "2018-01-01" and rec["n_obs"] == 700
        assert set(rec["scaler"]) == {"center", "scale", "clip"} and rec["scaler"]["clip"] == 4.0
        json.dumps(rec)                                                           # fully serialisable

    def test_short_history_flag_follows_the_threshold(self):
        assert self._fit(self._panel())[0]["short_history"] is False              # 700 >= 500
        hi = FitSettings(**{**FIT.__dict__, "short_history_min_obs": 701})
        rec, status = self._fit(self._panel(), hi)
        assert rec["short_history"] is True and status.endswith("SHORT_HISTORY")

    def test_short_history_boundary_is_strict_below_the_threshold(self):
        """`short = n < short_history_min_obs`: a window of exactly the threshold is NOT short."""
        exactly = FitSettings(**{**FIT.__dict__, "short_history_min_obs": 700})
        assert self._fit(self._panel(), exactly)[0]["short_history"] is False

    def test_only_complete_case_rows_train(self):
        rec, _ = self._fit(self._panel(null_rows=(5, 6, 7)))
        assert rec["n_obs"] == 697

    def test_too_few_rows_is_skipped_with_no_record(self):
        rec, status = self._fit(self._panel(n=99))
        assert rec is None and status == "skipped: 99 complete rows < min_obs_to_fit 100"

    def test_exactly_min_obs_is_not_skipped(self):
        """The boundary is `n < min_obs_to_fit`: 100 rows is enough to ATTEMPT a fit (it may still be
        refused as degenerate, which is a different status)."""
        rec, status = self._fit(self._panel(n=100))
        assert not status.startswith("skipped")

    def test_refused_fit_returns_no_record(self):
        strict = FitSettings(**{**FIT.__dict__, "min_state_share": 0.33})
        rng = np.random.default_rng(0)
        Z = np.concatenate([rng.normal(-4, 0.3, (490, 3)), rng.normal(0, 0.3, (140, 3)), rng.normal(4, 0.3, (70, 3))])
        panel = pl.DataFrame({"as_of_date": [date(2018, 1, 1) + timedelta(days=i) for i in range(700)],
                              "a": Z[:, 0], "b": Z[:, 1], "c": Z[:, 2]})
        rec, status = self._fit(panel, strict)
        assert rec is None and status.startswith("refused: no non-degenerate candidate of 4")

    def test_persisted_record_reproduces_in_memory_posteriors(self):
        """The model is its JSON: scoring a panel from the round-tripped record must equal scoring it
        with the fitted arrays — nothing lives only in memory."""
        panel = self._panel()
        rec, _ = self._fit(panel)
        rt = json.loads(json.dumps(rec))
        a = hc.state_columns(panel, "demo", ["lo", "mid", "hi"], rec)
        b = hc.state_columns(panel, "demo", ["lo", "mid", "hi"], rt)
        assert all(a[k].equals(b[k]) for k in a)


# ═══ Output columns ══════════════════════════════════════════════════════════

class TestStateColumns:
    def _rec_panel(self):
        Z, _ = _blobs(n=700, seed=1)
        panel = pl.DataFrame({"as_of_date": [date(2018, 1, 1) + timedelta(days=i) for i in range(700)],
                              "a": Z[:, 0], "b": Z[:, 1], "c": Z[:, 2]})
        rec, _ = hc.fit_model(panel, "as_of_date", "demo", ["a", "b", "c"], np.array([1.0, 1.0, 0.0]),
                              ["lo", "mid", "hi"], FIT, "fp", date(2020, 6, 1), "1", 1)
        return panel, rec

    def test_no_model_gives_typed_all_null_columns(self):
        panel, _ = self._rec_panel()
        cols = hc.state_columns(panel, "demo", ["lo", "mid", "hi"], None)
        assert list(cols) == ["demo_label", "demo_probability", "demo_score", "demo_post_lo", "demo_post_mid",
                              "demo_post_hi", "demo_short_history", "demo_model_fit_date"]
        assert all(s.null_count() == panel.height and len(s) == panel.height for s in cols.values())
        assert cols["demo_label"].dtype == pl.String and cols["demo_short_history"].dtype == pl.Boolean
        assert cols["demo_model_fit_date"].dtype == pl.Date and cols["demo_score"].dtype == pl.Float64

    def test_label_probability_score_and_posteriors_are_consistent(self):
        panel, rec = self._rec_panel()
        cols = hc.state_columns(panel, "demo", ["lo", "mid", "hi"], rec)
        post = np.column_stack([cols[f"demo_post_{l}"].to_numpy() for l in ("lo", "mid", "hi")])
        assert np.allclose(post.sum(axis=1), 1.0)
        assert np.allclose(cols["demo_probability"].to_numpy(), post.max(axis=1))
        assert np.allclose(cols["demo_score"].to_numpy(), post @ np.array([-1.0, 0.0, 1.0]))   # linspace(-1, 1, 3)
        assert cols["demo_label"].to_list() == [["lo", "mid", "hi"][i] for i in post.argmax(axis=1)]
        assert cols["demo_model_fit_date"].unique().to_list() == [date(2020, 6, 1)]
        assert cols["demo_short_history"].unique().to_list() == [False]

    def test_rows_with_no_posterior_are_null_in_every_column(self):
        panel, rec = self._rec_panel()
        head = pl.DataFrame({"as_of_date": [date(2017, 12, 30), date(2017, 12, 31)],
                             "a": [None, None], "b": [None, None], "c": [None, None]},
                            schema=panel.schema)
        cols = hc.state_columns(pl.concat([head, panel]), "demo", ["lo", "mid", "hi"], rec)
        for s in cols.values():
            assert s[:2].null_count() == 2 and s[2:].null_count() == 0

    def test_score_runs_from_minus_one_first_label_to_plus_one_last(self):
        panel, rec = self._rec_panel()
        sc = hc.state_columns(panel, "demo", ["lo", "mid", "hi"], rec)["demo_score"]
        assert sc.min() < -0.9 and sc.max() > 0.9


# ═══ Persistence ═════════════════════════════════════════════════════════════

class TestPersistence:
    def test_atomic_write_text_replaces_and_leaves_no_debris(self, tmp_path):
        p = tmp_path / "sub" / "m.json"
        hc.atomic_write_text(p, "one")
        hc.atomic_write_text(p, "two")
        assert p.read_text() == "two" and [x.name for x in p.parent.iterdir()] == ["m.json"]

    def test_failure_cleans_up_and_keeps_the_original(self, tmp_path, monkeypatch):
        p = tmp_path / "m.json"
        hc.atomic_write_text(p, "original")
        monkeypatch.setattr(os, "replace", lambda *a, **k: (_ for _ in ()).throw(OSError("disk")))
        with pytest.raises(OSError):
            hc.atomic_write_text(p, "new")
        monkeypatch.undo()
        assert p.read_text() == "original" and [x.name for x in tmp_path.iterdir()] == ["m.json"]

    def test_save_json_round_trips_through_load_model_file(self, tmp_path):
        p = tmp_path / "m.json"
        hc.save_json(p, {"schema": 1, "domain": "demo", "x": [1, 2]})
        assert hc.load_model_file(p, 1, "demo", _Err)["x"] == [1, 2]

    def test_absent_model_is_none(self, tmp_path):
        assert hc.load_model_file(tmp_path / "nope.json", 1, "demo", _Err) is None

    @pytest.mark.parametrize("schema,name", [(2, "demo"), (1, "other"), (None, "demo")])
    def test_foreign_or_stale_model_raises_the_callers_error(self, tmp_path, schema, name):
        p = tmp_path / "m.json"
        p.write_text(json.dumps({"schema": schema, "domain": name}))
        with pytest.raises(_Err, match="unexpected model schema/domain"):
            hc.load_model_file(p, 1, "demo", _Err)

    def _rows(self, days, v=1.0):
        return pl.DataFrame({"date": days, "v": [v] * len(days)}, schema={"date": pl.Date, "v": pl.Float64})

    def test_append_store_creates_then_appends_sorted(self, tmp_path):
        p = tmp_path / "s.parquet"
        assert hc.append_store(None, self._rows([date(2026, 1, 2), date(2026, 1, 1)]), p, _Err) == 2
        existing = pl.read_parquet(p)
        assert existing["date"].to_list() == [date(2026, 1, 1), date(2026, 1, 2)]
        assert hc.append_store(existing, self._rows([date(2026, 1, 5)]), p, _Err) == 1
        assert pl.read_parquet(p)["date"].to_list() == [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 5)]

    def test_append_store_never_rewrites_stored_rows(self, tmp_path):
        p = tmp_path / "s.parquet"
        hc.append_store(None, self._rows([date(2026, 1, 1)], v=1.0), p, _Err)
        hc.append_store(pl.read_parquet(p), self._rows([date(2026, 1, 2)], v=9.0), p, _Err)
        assert pl.read_parquet(p)["v"].to_list() == [1.0, 9.0]

    def test_schema_change_raises_and_leaves_the_store_untouched(self, tmp_path):
        p = tmp_path / "s.parquet"
        hc.append_store(None, self._rows([date(2026, 1, 1)]), p, _Err)
        before = p.read_bytes()
        bad = self._rows([date(2026, 1, 2)]).with_columns(pl.lit(1).alias("extra"))
        with pytest.raises(_Err, match="append-only and cannot be migrated in place"):
            hc.append_store(pl.read_parquet(p), bad, p, _Err)
        assert p.read_bytes() == before

    def test_dtype_change_alone_is_a_schema_change(self, tmp_path):
        p = tmp_path / "s.parquet"
        hc.append_store(None, self._rows([date(2026, 1, 1)]), p, _Err)
        bad = self._rows([date(2026, 1, 2)]).with_columns(pl.col("v").cast(pl.Float32))
        with pytest.raises(_Err):
            hc.append_store(pl.read_parquet(p), bad, p, _Err)


class TestModuleIsPure:
    def test_core_imports_no_branch_module(self):
        import ast, pathlib
        tree = ast.parse(pathlib.Path(hc.__file__).read_text())
        mods = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        mods |= {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        forbidden = {m for m in mods if m.startswith("src.") and m not in ("src.gold.hmm.config", "src.utils.atomic_io")}
        assert not forbidden, forbidden
