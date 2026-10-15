"""
tests/unit/test_hmm_market_model_config.py — ADR-051 (GMI-HMM2-001): the market MODEL settings of
config/fred_series.yaml (`hmm_market.labels` / `.fit` / per-feature `weight`), validated by
config.validate_market_model_config.

Separate from validate_market_config on purpose (raw_market neither needs nor may depend on a model
setting): the routing validator must keep accepting a block with no weights at all.
"""
import copy

import pytest
import yaml

from src.gold.hmm.config import (FRED_SERIES_PATH, HMMConfigError, FitSettings, MarketModelConfig,
                                 market_config, market_model_config, validate_market_config,
                                 validate_market_model_config)


def _raw():
    return copy.deepcopy(yaml.safe_load(open(FRED_SERIES_PATH)))


def _mut(fn):
    r = _raw()
    fn(r)
    return r


class TestProductionBlock:
    def test_loads_as_a_typed_config(self):
        c = market_model_config()
        assert isinstance(c, MarketModelConfig) and isinstance(c.fit, FitSettings)
        assert c.labels == ("risk_off", "mixed", "risk_on") and c.fit.n_states == 3
        assert c.feature_names() == c.market.feature_names() and len(c.feature_names()) == 10

    def test_every_feature_has_a_weight_and_the_orientation_is_the_documented_one(self):
        w = market_model_config().weights
        assert set(w) == set(market_model_config().feature_names())
        # risk direction: stress measures negative, risk-on measures positive
        assert w["VIXCLS"] < 0 and w["us_dispersion"] < 0 and w["us_median_ret_rms"] < 0
        assert w["us_share_up"] > 0 and w["us_median_ret"] > 0
        # sign-ambiguous features are in the model but OUT of the ordering
        for f in ("DEXUSEU_LOGCHG20", "DEXJPUS_LOGCHG20", "DGS5_D1", "DGS10_D1", "T10Y2Y_D1"):
            assert w[f] == 0.0

    def test_routing_validator_is_unaffected_by_the_model_settings(self):
        assert validate_market_config(_raw()).feature_names() == market_config().feature_names()

    def test_routing_validator_accepts_a_block_with_no_weights_labels_or_fit(self):
        def strip(r):
            for e in r["hmm_market"]["features"]:
                e.pop("weight", None)
            r["hmm_market"].pop("labels"), r["hmm_market"].pop("fit")
        c = validate_market_config(_mut(strip))
        assert all(f.weight is None for f in c.features)

    def test_fit_settings_reuse_the_macro_schema(self):
        from src.gold.hmm.config import macro_model_config
        assert market_model_config().fit.__dataclass_fields__.keys() == macro_model_config().fit.__dataclass_fields__.keys()


class TestRequired:
    def test_weight_is_required_by_the_model(self):
        def m(r):
            r["hmm_market"]["features"][0].pop("weight")
        with pytest.raises(HMMConfigError, match="`weight` is required by the market model"):
            validate_market_model_config(_mut(m))

    def test_at_least_one_non_zero_weight(self):
        def m(r):
            for e in r["hmm_market"]["features"]:
                e["weight"] = 0.0
        with pytest.raises(HMMConfigError, match="at least one non-zero ordering weight"):
            validate_market_model_config(_mut(m))

    @pytest.mark.parametrize("bad", ["1", True, float("nan"), float("inf")])
    def test_weight_must_be_a_finite_number(self, bad):
        def m(r):
            r["hmm_market"]["features"][0]["weight"] = bad
        with pytest.raises(HMMConfigError, match="weight must be a finite number"):
            validate_market_model_config(_mut(m))

    def test_fit_block_is_required(self):
        def m(r):
            r["hmm_market"].pop("fit")
        with pytest.raises(HMMConfigError, match="hmm_market.fit is required for the market model"):
            validate_market_model_config(_mut(m))

    def test_labels_block_is_required(self):
        def m(r):
            r["hmm_market"].pop("labels")
        with pytest.raises(HMMConfigError, match="hmm_market.labels must be 3 distinct"):
            validate_market_model_config(_mut(m))


class TestLabels:
    @pytest.mark.parametrize("bad", [["a", "b"], ["a", "b", "c", "d"], ["a", "a", "b"], ["a", "", "b"], ["a", 1, "b"], "abc"])
    def test_must_be_n_states_distinct_non_empty_strings(self, bad):
        def m(r):
            r["hmm_market"]["labels"] = bad
        with pytest.raises(HMMConfigError, match="hmm_market.labels must be 3 distinct non-empty strings"):
            validate_market_model_config(_mut(m))

    def test_label_count_follows_n_states(self):
        def m(r):
            r["hmm_market"]["fit"]["n_states"] = 2
            r["hmm_market"]["fit"]["min_state_share"] = 0.02
            r["hmm_market"]["labels"] = ["off", "on"]
        assert validate_market_model_config(_mut(m)).labels == ("off", "on")


class TestFit:
    def test_messages_name_the_market_key_not_the_macro_one(self):
        def m(r):
            r["hmm_market"]["fit"]["n_iter"] = 0
        with pytest.raises(HMMConfigError, match=r"hmm_market\.fit\.n_iter must be an int >= 1"):
            validate_market_model_config(_mut(m))

    def test_unknown_or_missing_keys_rejected(self):
        def extra(r):
            r["hmm_market"]["fit"]["surprise"] = 1
        def missing(r):
            r["hmm_market"]["fit"].pop("tol")
        for fn in (extra, missing):
            with pytest.raises(HMMConfigError, match=r"hmm_market\.fit keys must be exactly"):
                validate_market_model_config(_mut(fn))

    def test_model_version_must_be_a_non_empty_string(self):
        def m(r):
            r["hmm_market"]["fit"]["model_version"] = ""
        with pytest.raises(HMMConfigError, match="model_version must be a non-empty string"):
            validate_market_model_config(_mut(m))

    @pytest.mark.parametrize("bad", [0, -0.1, 0.34, 1.0, True, "x"])
    def test_min_state_share_must_be_in_zero_to_one_over_n_states(self, bad):
        def m(r):
            r["hmm_market"]["fit"]["min_state_share"] = bad
        with pytest.raises(HMMConfigError, match="min_state_share must be in"):
            validate_market_model_config(_mut(m))

    def test_the_macro_and_market_fit_blocks_are_independent(self):
        def m(r):
            r["hmm_market"]["fit"]["n_random_seeds"] = 1
        c = validate_market_model_config(_mut(m))
        from src.gold.hmm.config import validate_macro_model_config
        assert c.fit.n_random_seeds == 1 and validate_macro_model_config(_mut(m)).fit.n_random_seeds == 8


class TestFingerprint:
    def _fp(self, fn=None):
        return validate_market_model_config(_mut(fn) if fn else _raw()).fingerprint()

    def test_stable_and_twelve_hex(self):
        assert self._fp() == self._fp() and len(self._fp()) == 12 and int(self._fp(), 16) >= 0

    @pytest.mark.parametrize("name,fn", [
        ("weight", lambda r: r["hmm_market"]["features"][0].__setitem__("weight", -0.9)),
        ("label", lambda r: r["hmm_market"].__setitem__("labels", ["down", "mixed", "up"])),
        ("fit.model_version", lambda r: r["hmm_market"]["fit"].__setitem__("model_version", "2")),
        ("fit.n_iter", lambda r: r["hmm_market"]["fit"].__setitem__("n_iter", 201)),
        ("breadth.min_symbols", lambda r: r["hmm_market"]["breadth"].__setitem__("min_symbols", 101)),
        ("breadth.rms_window", lambda r: r["hmm_market"]["breadth"].__setitem__("rms_window", 21)),
        ("transform", lambda r: r["hmm_market"]["features"][3].__setitem__("transform", "level")),
        ("pit.basis", lambda r: r["hmm"]["pit"].__setitem__("basis", "estimated")),
    ])
    def test_every_defining_setting_moves_it(self, name, fn):
        assert self._fp(fn) != self._fp(), name

    def test_description_only_changes_do_not(self):
        def m(r):
            r["hmm_market"]["version"] = "relabelled"          # routing label, not a model definition
        assert self._fp(m) == self._fp()
