"""
tests/unit/test_hmm_config.py — ADR-051 (GMI-HMM2-001): src/gold/hmm/config.py.

Two halves: the REAL config/fred_series.yaml `hmm:` block must load and satisfy
the ADR-051 variable table; and every static check in validate_macro_config
must reject the specific malformation it exists for (a validator that never
fails is not a validator).
"""
import copy

import pytest

from src.gold.hmm.config import HMMConfigError, macro_config, validate_macro_config


# ── real registry ────────────────────────────────────────────────────────────

class TestRealMacroConfig:
    def test_loads_and_has_five_domains(self):
        cfg = macro_config()
        assert cfg.domains == ("monetary", "growth", "inflation", "stress", "liquidity")

    def test_variable_table_routing(self):
        """ADR-051 'Variable -> Domain / Module Assignment' table."""
        cfg = macro_config()
        by = {d: [f.feature for f in cfg.features_in(d)] for d in cfg.domains}
        assert by["monetary"] == ["DFF", "T10Y2Y", "DGS5", "DGS10", "REALYIELD5Y", "REALYIELD10Y"]
        assert by["growth"] == ["A191RL1Q225SBEA", "UNRATE"]
        assert by["inflation"] == ["T5YIE", "T10YIE", "CPIAUCSL_YOY", "DCOILWTICO"]   # WTI -> inflation
        assert by["stress"] == ["BAMLH0A0HYM2"]
        assert by["liquidity"] == ["NFCI", "MORTGAGE30US"]                            # NFCI, not the NFCL typo

    def test_real_yields_are_derived_not_ingested(self):
        cfg = macro_config()
        f = {x.feature: x for x in cfg.features}
        assert f["REALYIELD5Y"].derived.left == "DGS5" and f["REALYIELD5Y"].derived.right == "T5YIE"
        assert f["REALYIELD10Y"].derived.left == "DGS10" and f["REALYIELD10Y"].derived.right == "T10YIE"
        assert f["REALYIELD5Y"].series is None

    def test_required_series_cover_legs_without_duplicates(self):
        req = macro_config().required_series()
        assert len(req) == len(set(req)) == 13
        assert {"DGS5", "DGS10", "T5YIE", "T10YIE", "CPIAUCSL"} <= set(req)
        assert "REALYIELD5Y" not in req            # derived: never a registry series

    def test_pit_lags_follow_amendment_2_section_c(self):
        cfg = macro_config()
        assert cfg.frequency_floor_days == {"daily": 1, "weekly": 7, "monthly": 45, "quarterly": 120}
        assert cfg.lag_days("A191RL1Q225SBEA") == 120      # quarterly floor
        assert cfg.lag_days("CPIAUCSL") == 45 and cfg.lag_days("UNRATE") == 45
        assert cfg.lag_days("NFCI") == 7
        assert cfg.lag_days("DCOILWTICO") == 8             # override beats the daily floor (1)
        assert cfg.lag_days("DEXUSEU") == 10 and cfg.lag_days("DEXJPUS") == 10

    def test_collinearity_guard_holds_on_real_routing(self):
        cfg = macro_config()
        for f in cfg.features:
            if f.derived is None:
                continue
            plain = {g.series for g in cfg.features_in(f.domain) if g.derived is None}
            assert not ({f.derived.left, f.derived.right} <= plain)

    def test_known_transforms_is_derived_from_fred_pit_not_duplicated(self):
        """Drift guard: a transform fred_pit can run must be valid config, and
        config must not accept one fred_pit cannot run."""
        from src.gold.hmm import fred_pit
        from src.gold.hmm.config import KNOWN_TRANSFORMS
        assert KNOWN_TRANSFORMS == frozenset(fred_pit.TRANSFORMS)

    def test_adr050_contributes_to_block_left_empty(self):
        """Amendment 2 §B: the ADR-050 per-domain block is left in place, empty."""
        import yaml
        raw = yaml.safe_load(open("config/fred_series.yaml"))
        assert all(v["_meta"]["contributes_to"] == [] for v in raw["domains"].values())


# ── validator negatives ──────────────────────────────────────────────────────

def _base() -> dict:
    return {
        "series": [
            {"id": "AAA", "frequency": "daily"},
            {"id": "BBB", "frequency": "daily"},
            {"id": "CCC", "frequency": "monthly"},
            {"id": "WWW", "frequency": "weekly"},
        ],
        "hmm": {
            "version": "t",
            "domains": ["d1", "d2"],
            "pit": {"basis": "recorded", "frequency_floor_days": {"daily": 1, "weekly": 7, "monthly": 45, "quarterly": 120},
                    "series_lag_days": {}},
            "features": [
                {"feature": "AAA", "series": "AAA", "domain": "d1", "transform": "level"},
                {"feature": "BBB", "series": "BBB", "domain": "d2", "transform": "level"},
            ],
        },
    }


def _mut(fn) -> dict:
    raw = copy.deepcopy(_base())
    fn(raw)
    return raw


class TestValidatorRejects:
    def test_base_is_valid(self):
        cfg = validate_macro_config(_base())
        assert cfg.feature_names() == ["AAA", "BBB"]

    def test_missing_hmm_block(self):
        with pytest.raises(HMMConfigError, match="no `hmm:` block"):
            validate_macro_config({"series": []})

    def test_empty_domains(self):
        with pytest.raises(HMMConfigError, match="domains is empty"):
            validate_macro_config(_mut(lambda r: r["hmm"].update(domains=[])))

    def test_duplicate_domains(self):
        with pytest.raises(HMMConfigError, match="duplicates"):
            validate_macro_config(_mut(lambda r: r["hmm"].update(domains=["d1", "d1"])))

    def test_unknown_domain(self):
        with pytest.raises(HMMConfigError, match="not in hmm.domains"):
            validate_macro_config(_mut(lambda r: r["hmm"]["features"][0].update(domain="nope")))

    def test_duplicate_feature_name(self):
        with pytest.raises(HMMConfigError, match="duplicate feature"):
            validate_macro_config(_mut(lambda r: r["hmm"]["features"][1].update(feature="AAA")))

    def test_series_not_in_registry(self):
        with pytest.raises(HMMConfigError, match="not in FRED registry"):
            validate_macro_config(_mut(lambda r: r["hmm"]["features"][0].update(series="ZZZ")))

    def test_series_and_derived_both(self):
        def m(r):
            r["hmm"]["features"][0]["derived"] = {"op": "subtract", "left": "AAA", "right": "BBB"}
        with pytest.raises(HMMConfigError, match="exactly one of"):
            validate_macro_config(_mut(m))

    def test_neither_series_nor_derived(self):
        def m(r):
            del r["hmm"]["features"][0]["series"]
        with pytest.raises(HMMConfigError, match="exactly one of"):
            validate_macro_config(_mut(m))

    def test_unknown_transform(self):
        with pytest.raises(HMMConfigError, match="unknown transform"):
            validate_macro_config(_mut(lambda r: r["hmm"]["features"][0].update(transform="log")))

    def test_pct_change_12m_requires_monthly(self):
        with pytest.raises(HMMConfigError, match="only valid on a monthly"):
            validate_macro_config(_mut(lambda r: r["hmm"]["features"][0].update(transform="pct_change_12m")))

    def test_pct_change_12m_ok_on_monthly(self):
        def m(r):
            r["hmm"]["features"].append({"feature": "CCC_YOY", "series": "CCC", "domain": "d1",
                                         "transform": "pct_change_12m"})
        assert "CCC_YOY" in validate_macro_config(_mut(m)).feature_names()

    def test_derived_leg_not_in_registry(self):
        def m(r):
            r["hmm"]["features"].append({"feature": "X", "domain": "d1",
                                         "derived": {"op": "subtract", "left": "AAA", "right": "NOPE"}})
        with pytest.raises(HMMConfigError, match="derived leg"):
            validate_macro_config(_mut(m))

    def test_unknown_derived_op(self):
        def m(r):
            r["hmm"]["features"].append({"feature": "X", "domain": "d1",
                                         "derived": {"op": "divide", "left": "AAA", "right": "BBB"}})
        with pytest.raises(HMMConfigError, match="unknown derived op"):
            validate_macro_config(_mut(m))

    def test_derived_takes_no_transform(self):
        def m(r):
            r["hmm"]["features"].append({"feature": "X", "domain": "d2", "transform": "pct_change_12m",
                                         "derived": {"op": "subtract", "left": "AAA", "right": "BBB"}})
        with pytest.raises(HMMConfigError, match="takes no transform"):
            validate_macro_config(_mut(m))

    def test_collinearity_guard_derived_with_both_legs(self):
        """A domain holding derived X=AAA-BBB together with AAA and BBB is
        near-singular for a GaussianHMM — rejected at load time."""
        def m(r):
            r["hmm"]["features"][1]["domain"] = "d1"          # AAA and BBB both in d1
            r["hmm"]["features"].append({"feature": "X", "domain": "d1",
                                         "derived": {"op": "subtract", "left": "AAA", "right": "BBB"}})
        with pytest.raises(HMMConfigError, match="collinearity guard"):
            validate_macro_config(_mut(m))

    def test_collinearity_guard_allows_one_leg(self):
        def m(r):                                              # d1: AAA + X ; BBB in d2
            r["hmm"]["features"].append({"feature": "X", "domain": "d1",
                                         "derived": {"op": "subtract", "left": "AAA", "right": "BBB"}})
        assert "X" in validate_macro_config(_mut(m)).feature_names()

    def test_frequency_without_floor_or_override(self):
        def m(r):
            del r["hmm"]["pit"]["frequency_floor_days"]["daily"]
        with pytest.raises(HMMConfigError, match="no publication-lag floor"):
            validate_macro_config(_mut(m))

    def test_override_rescues_missing_floor(self):
        def m(r):
            del r["hmm"]["pit"]["frequency_floor_days"]["daily"]
            r["hmm"]["pit"]["series_lag_days"] = {"AAA": 2, "BBB": 3}
        cfg = validate_macro_config(_mut(m))
        assert cfg.lag_days("AAA") == 2 and cfg.lag_days("BBB") == 3

    def test_override_for_unknown_series(self):
        with pytest.raises(HMMConfigError, match="not in registry"):
            validate_macro_config(_mut(lambda r: r["hmm"]["pit"].update(series_lag_days={"NOPE": 5})))

    def test_negative_lag_override(self):
        with pytest.raises(HMMConfigError, match="int >= 0"):
            validate_macro_config(_mut(lambda r: r["hmm"]["pit"].update(series_lag_days={"AAA": -1})))

    def test_unknown_frequency_floor_key(self):
        def m(r):
            r["hmm"]["pit"]["frequency_floor_days"]["hourly"] = 0
        with pytest.raises(HMMConfigError, match="unknown frequency"):
            validate_macro_config(_mut(m))

    def test_empty_features(self):
        with pytest.raises(HMMConfigError, match="features is empty"):
            validate_macro_config(_mut(lambda r: r["hmm"].update(features=[])))


# ═══ Phase B: market branch (hmm_market:) ══════════════════════════════════════════════════════════════════════════════════

from src.gold.hmm.config import (  # noqa: E402
    BREADTH_METRICS, KNOWN_TRANSFORMS, MARKET_SOURCE_KEYS, market_config, validate_market_config,
)


class TestRealMarketConfig:
    def test_loads_with_ten_features_in_yaml_order(self):
        cfg = market_config()
        assert cfg.feature_names() == [
            "VIXCLS", "DEXUSEU_LOGCHG20", "DEXJPUS_LOGCHG20", "DGS5_D1", "DGS10_D1", "T10Y2Y_D1",
            "us_share_up", "us_median_ret", "us_dispersion", "us_median_ret_rms",
        ]

    def test_adr051_variable_table_for_the_market_branch(self):
        """VIXCLS level; DEXUSEU/DEXJPUS 20-observation log change; DGS5/DGS10/T10Y2Y
        day-over-day change; four Layer 1 breadth metrics."""
        cfg = market_config()
        f = {x.feature: x for x in cfg.features}
        assert (f["VIXCLS"].series, f["VIXCLS"].transform) == ("VIXCLS", "level")
        for feat, sid in (("DEXUSEU_LOGCHG20", "DEXUSEU"), ("DEXJPUS_LOGCHG20", "DEXJPUS")):
            assert (f[feat].series, f[feat].transform) == (sid, "log_change_20")
        for feat, sid in (("DGS5_D1", "DGS5"), ("DGS10_D1", "DGS10"), ("T10Y2Y_D1", "T10Y2Y")):
            assert (f[feat].series, f[feat].transform) == (sid, "diff_1")
        assert {x.metric for x in cfg.breadth_features()} == BREADTH_METRICS

    def test_every_source_is_on_the_whitelist_and_none_is_gold(self):
        """Amendment 2 §A, pinned on the REAL routing: the market branch is Silver-only."""
        cfg = market_config()
        assert {f.source for f in cfg.features} <= set(MARKET_SOURCE_KEYS)
        assert not any("gold" in f.source.lower() for f in cfg.features)

    def test_shares_the_macro_branch_pit_rule(self):
        """One rule, one definition: market lags come from hmm.pit, so the Fed H.10
        batch override (10 d) applies to DEXUSEU here exactly as it does anywhere else."""
        m, mac = market_config(), macro_config()
        assert m.frequency_floor_days == mac.frequency_floor_days
        assert m.series_lag_days == mac.series_lag_days
        assert m.lag_days("DEXUSEU") == 10 and m.lag_days("VIXCLS") == 1

    def test_required_series_exclude_breadth(self):
        assert market_config().required_series() == ["VIXCLS", "DEXUSEU", "DEXJPUS", "DGS5", "DGS10", "T10Y2Y"]

    def test_breadth_settings(self):
        b = market_config().breadth
        assert (b.market, b.timeframe, b.min_symbols) == ("us_stocks", "1D", 100)
        assert (b.min_coverage_ratio, b.rms_window, b.guard_version) == (0.8, 20, "1")

    def test_new_transforms_are_known(self):
        assert {"log_change_20", "diff_1"} <= KNOWN_TRANSFORMS


def _mraw() -> dict:
    return {
        "series": [
            {"id": "AAA", "frequency": "daily"},
            {"id": "MMM", "frequency": "monthly"},
        ],
        "hmm": {"pit": {"basis": "recorded", "frequency_floor_days": {"daily": 1, "weekly": 7, "monthly": 45, "quarterly": 120},
                        "series_lag_days": {}}},
        "hmm_market": {
            "version": "t",
            "features": [
                {"feature": "AAA_D1", "source": "fred", "series": "AAA", "transform": "diff_1"},
                {"feature": "up", "source": "silver_breadth", "metric": "share_up"},
            ],
            "breadth": {"market": "us_stocks", "timeframe": "1D", "min_symbols": 5,
                        "min_coverage_ratio": 0.8, "rms_window": 5, "guard_version": "t"},
        },
    }


def _mm(fn) -> dict:
    raw = copy.deepcopy(_mraw())
    fn(raw)
    return raw


class TestMarketValidatorRejects:
    def test_base_is_valid(self):
        cfg = validate_market_config(_mraw())
        assert cfg.feature_names() == ["AAA_D1", "up"] and cfg.breadth.min_symbols == 5

    def test_fred_only_config_is_valid_and_has_no_breadth(self):
        def m(r):
            r["hmm_market"]["features"] = [r["hmm_market"]["features"][0]]
            del r["hmm_market"]["breadth"]
        assert validate_market_config(_mm(m)).breadth is None

    def test_missing_hmm_block(self):
        raw = _mraw(); del raw["hmm"]
        with pytest.raises(HMMConfigError, match="no `hmm:` block"):
            validate_market_config(raw)

    def test_missing_hmm_market_block(self):
        raw = _mraw(); del raw["hmm_market"]
        with pytest.raises(HMMConfigError, match="no `hmm_market:` block"):
            validate_market_config(raw)

    @pytest.mark.parametrize("src", ["gold_global_regime", "Gold", "data/gold/cross_asset/global_regime.parquet",
                                     "GOLD_SIGNALS"])
    def test_gold_source_is_rejected_by_name(self, src):
        """Amendment 2 §A: a feature that reads a Gold store cannot be expressed."""
        def m(r):
            r["hmm_market"]["features"].append({"feature": "global_risk_score", "source": src})
        with pytest.raises(HMMConfigError, match="reads a Gold store"):
            validate_market_config(_mm(m))

    def test_gold_series_id_is_not_a_false_positive(self):
        """A FRED series id that merely CONTAINS 'gold' (the LBMA gold price series)
        is a legitimate `series`, not a Gold-store source."""
        def m(r):
            r["series"].append({"id": "GOLDAMGBD228NLBM", "frequency": "daily"})
            r["hmm_market"]["features"].append(
                {"feature": "GOLD_D1", "source": "fred", "series": "GOLDAMGBD228NLBM", "transform": "diff_1"})
        assert "GOLD_D1" in validate_market_config(_mm(m)).feature_names()

    @pytest.mark.parametrize("src", ["bronze", "silver", "gdelt", None])
    def test_unknown_source(self, src):
        def m(r):
            r["hmm_market"]["features"].append({"feature": "X", "source": src})
        with pytest.raises(HMMConfigError, match="unknown source"):
            validate_market_config(_mm(m))

    def test_smuggled_store_key_is_rejected_not_ignored(self):
        def m(r):
            r["hmm_market"]["features"][0]["store"] = "data/gold/hmm/context_regime.parquet"
        with pytest.raises(HMMConfigError, match=r"unexpected keys \['store'\]"):
            validate_market_config(_mm(m))

    def test_fred_feature_missing_series(self):
        def m(r):
            del r["hmm_market"]["features"][0]["series"]
        with pytest.raises(HMMConfigError, match=r"missing keys \['series'\]"):
            validate_market_config(_mm(m))

    def test_breadth_feature_missing_metric(self):
        def m(r):
            del r["hmm_market"]["features"][1]["metric"]
        with pytest.raises(HMMConfigError, match=r"missing keys \['metric'\]"):
            validate_market_config(_mm(m))

    def test_breadth_feature_cannot_carry_a_series(self):
        def m(r):
            r["hmm_market"]["features"][1]["series"] = "AAA"
        with pytest.raises(HMMConfigError, match="unexpected keys"):
            validate_market_config(_mm(m))

    def test_non_mapping_feature_entry(self):
        with pytest.raises(HMMConfigError, match="must be a mapping"):
            validate_market_config(_mm(lambda r: r["hmm_market"]["features"].append("VIXCLS")))

    def test_unknown_breadth_metric(self):
        with pytest.raises(HMMConfigError, match="unknown breadth metric"):
            validate_market_config(_mm(lambda r: r["hmm_market"]["features"][1].update(metric="skew")))

    def test_duplicate_breadth_metric(self):
        def m(r):
            r["hmm_market"]["features"].append({"feature": "up2", "source": "silver_breadth", "metric": "share_up"})
        with pytest.raises(HMMConfigError, match="already used"):
            validate_market_config(_mm(m))

    def test_duplicate_feature_name(self):
        with pytest.raises(HMMConfigError, match="duplicate feature"):
            validate_market_config(_mm(lambda r: r["hmm_market"]["features"][1].update(feature="AAA_D1")))

    def test_fred_series_not_in_registry(self):
        with pytest.raises(HMMConfigError, match="not in FRED registry"):
            validate_market_config(_mm(lambda r: r["hmm_market"]["features"][0].update(series="ZZZ")))

    def test_unknown_transform(self):
        with pytest.raises(HMMConfigError, match="unknown transform"):
            validate_market_config(_mm(lambda r: r["hmm_market"]["features"][0].update(transform="ema")))

    def test_pct_change_12m_requires_monthly_on_market_too(self):
        with pytest.raises(HMMConfigError, match="only valid on a monthly"):
            validate_market_config(_mm(lambda r: r["hmm_market"]["features"][0].update(transform="pct_change_12m")))

    def test_empty_features(self):
        with pytest.raises(HMMConfigError, match="features is empty"):
            validate_market_config(_mm(lambda r: r["hmm_market"].update(features=[])))

    def test_frequency_without_floor_or_override(self):
        def m(r):
            del r["hmm"]["pit"]["frequency_floor_days"]["daily"]
        with pytest.raises(HMMConfigError, match="no publication-lag floor"):
            validate_market_config(_mm(m))

    def test_bad_pit_is_rejected_by_the_shared_validator(self):
        with pytest.raises(HMMConfigError, match="int >= 0"):
            validate_market_config(_mm(lambda r: r["hmm"]["pit"].update(series_lag_days={"AAA": -3})))

    def test_breadth_block_required_when_a_breadth_feature_exists(self):
        def m(r):
            del r["hmm_market"]["breadth"]
        with pytest.raises(HMMConfigError, match="breadth is required"):
            validate_market_config(_mm(m))

    def test_breadth_keys_must_match_exactly(self):
        def m(r):
            r["hmm_market"]["breadth"]["extra"] = 1
        with pytest.raises(HMMConfigError, match="keys must be exactly"):
            validate_market_config(_mm(m))

    @pytest.mark.parametrize("key,bad", [("min_symbols", 0), ("min_symbols", True), ("min_symbols", 2.5),
                                         ("rms_window", 0), ("rms_window", 1),
                                         ("rms_window", "20")])
    def test_breadth_int_fields_are_range_and_type_checked(self, key, bad):
        def m(r):
            r["hmm_market"]["breadth"][key] = bad
        with pytest.raises(HMMConfigError, match=key):
            validate_market_config(_mm(m))

    @pytest.mark.parametrize("bad", [0, 0.0, -0.1, 1.5, True, "0.8"])
    def test_min_coverage_ratio_must_be_in_unit_interval(self, bad):
        def m(r):
            r["hmm_market"]["breadth"]["min_coverage_ratio"] = bad
        with pytest.raises(HMMConfigError, match="min_coverage_ratio"):
            validate_market_config(_mm(m))

    def test_ratio_of_exactly_one_is_allowed(self):
        def m(r):
            r["hmm_market"]["breadth"]["min_coverage_ratio"] = 1
        assert validate_market_config(_mm(m)).breadth.min_coverage_ratio == 1.0

    def test_empty_market_or_timeframe(self):
        def m(r):
            r["hmm_market"]["breadth"]["market"] = ""
        with pytest.raises(HMMConfigError, match="market must be"):
            validate_market_config(_mm(m))

    @pytest.mark.parametrize("bad", ["", None, 1, 1.0])
    def test_guard_version_must_be_a_non_empty_string(self, bad):
        """The threshold set is versioned: an unlabeled set is rejected, not defaulted."""
        def m(r):
            r["hmm_market"]["breadth"]["guard_version"] = bad
        with pytest.raises(HMMConfigError, match="guard_version"):
            validate_market_config(_mm(m))

    def test_legacy_breadth_keys_are_rejected_not_ignored(self):
        """partial_day_* / realized_vol_window were replaced by the coverage-ratio guard and
        rms_window; a config still carrying them must fail loudly, not be half-read."""
        def m(r):
            b = r["hmm_market"]["breadth"]
            b["partial_day_window"] = 10
        with pytest.raises(HMMConfigError, match="keys must be exactly"):
            validate_market_config(_mm(m))


class TestBreadthSettingsFingerprint:
    """guard_version is the human label; fingerprint() is the automatic digest of EVERY field,
    so a threshold edited without bumping the label is still visible in the data."""

    @staticmethod
    def _st(**kw):
        from src.gold.hmm.config import BreadthSettings
        base = dict(market="us_stocks", timeframe="1D", min_symbols=100, min_coverage_ratio=0.8,
                    rms_window=20, guard_version="1")
        base.update(kw)
        return BreadthSettings(**base)

    def test_stable_and_twelve_hex_chars(self):
        a, b = self._st(), self._st()
        assert a.fingerprint() == b.fingerprint()
        assert len(a.fingerprint()) == 12 and int(a.fingerprint(), 16) >= 0

    @pytest.mark.parametrize("field,value", [
        ("market", "idx"), ("timeframe", "1W"), ("min_symbols", 101),
        ("min_coverage_ratio", 0.81), ("rms_window", 21), ("guard_version", "2"),
    ])
    def test_every_field_moves_the_fingerprint(self, field, value):
        assert self._st().fingerprint() != self._st(**{field: value}).fingerprint()
