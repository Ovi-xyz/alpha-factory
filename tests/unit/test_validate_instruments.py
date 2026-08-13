"""tests/unit/test_validate_instruments.py — G7 validate_instruments test suite"""

import tempfile
from pathlib import Path

import pytest
import yaml

from scripts.validate_instruments import validate, validate_split


class TestValidateInstruments:

    def test_valid_file_passes(self):
        """IDD §10.2: valid file → exit code 0, total == 643."""
        # UPD Decision B Step 2 (GMI_Decision_Document_v5.docx): the real
        # file is now split into instruments_identity.yaml +
        # instruments_taxonomy.yaml — validate_split() is the real-file
        # entry point post-split (validate() is legacy, single-combined-
        # file only, used by every synthetic fixture below unchanged).
        assert validate_split() is True

    def test_index_key_absent_from_real_files(self):
        """ADR-035 (GMI_Decision_Document_v8.docx, 10 Aug 2026): the
        vestigial 'index: []' market category (empty since ADR-003) is
        removed entirely from both real config files, not just emptied."""
        identity = yaml.safe_load(Path("config/instruments_identity.yaml").read_text())
        taxonomy = yaml.safe_load(Path("config/instruments_taxonomy.yaml").read_text())
        assert "index" not in identity
        assert "index" not in taxonomy

    def test_index_not_in_required_fields_or_layer1_markets(self):
        """ADR-035: REQUIRED_FIELDS and _validate_layer1()'s layer1_markets
        tuple must no longer reference 'index'."""
        import inspect
        from scripts import validate_instruments as vi
        assert "index" not in vi.REQUIRED_FIELDS
        assert "index" not in inspect.getsource(vi._validate_layer1).split(
            "layer1_markets = ", 1
        )[1].split("\n", 1)[0]

    def test_symbol_with_dot_fails(self, tmp_path):
        """IDD §10.2: symbol dengan titik → error + exit code 1."""
        bad_yaml = {
            "version": "1.2",
            "us_stocks": {
                "Technology": [{"symbol": "BRK.B"}]  # dot is unsafe
            }
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(bad_yaml))

        result = validate(str(path))
        assert result is False

    def test_missing_required_field_fails(self, tmp_path):
        """Forex entry without raw_symbol → validation fails."""
        bad_yaml = {
            "version": "1.2",
            "forex": {
                "Usd/Eur": [
                    {
                        "symbol": "EUR_USD",
                        # missing raw_symbol and yfinance_symbol
                    }
                ]
            }
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(bad_yaml))
        result = validate(str(path))
        assert result is False

    def test_idx_without_jk_suffix_fails(self, tmp_path):
        """IDX yfinance_symbol must end with .JK."""
        bad_yaml = {
            "version": "1.2",
            "idx_stocks": {
                "IDX30": [
                    {"symbol": "BBCA", "yfinance_symbol": "BBCA"}  # missing .JK
                ]
            }
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(bad_yaml))
        result = validate(str(path))
        assert result is False


class TestValidateInstrumentsLayer2:
    """ADD GMI-VAL-001 — Layer 2 / dual-universe validator coverage."""

    def _minimal_valid_context(self) -> dict:
        """Minimal context skeleton with all 20 subcategories present and
        correctly shaped, used as a base for negative-test mutation."""
        return {
            "dollar": {
                "_meta": {"subcategory_id": "context_dollar"},
                "instruments": [{"symbol": "DXY", "yfinance_symbol": "DX-Y.NYB"}],
            },
            "rates": {
                "fed": {"_meta": {"subcategory_id": "context_rates_fed",
                                   "series": ["SOFR"]}},
                "curve": {"_meta": {"subcategory_id": "context_rates_curve"}},
                "spread": {"_meta": {"subcategory_id": "context_rates_spread"}},
                "dm_cb": {"_meta": {
                    "subcategory_id": "context_rates_dm_cb",
                    "central_banks": ["ECB", "BOE", "BOJ", "BOC", "RBA",
                                       "RBNZ", "SNB", "NORGES", "RIKSBANK"],
                }},
                "em_cb": {"_meta": {
                    "subcategory_id": "context_rates_em_cb",
                    "central_banks": ["PBOC", "BOK", "BI"],
                }},
            },
            "equity": {
                "dm": {"_meta": {"subcategory_id": "context_equity_dm"},
                       "instruments": [{"symbol": "SPX", "yfinance_symbol": "^GSPC"}]},
                "em": {"_meta": {"subcategory_id": "context_equity_em"},
                       "instruments": []},
                "volatility": {"_meta": {"subcategory_id": "context_volatility"},
                               "instruments": [{"symbol": "VIX", "yfinance_symbol": "^VIX"}]},
            },
            "commodity": {
                "energy": {"_meta": {"subcategory_id": "context_commodity_energy"},
                           "instruments": []},
                "metals": {"_meta": {"subcategory_id": "context_commodity_metals"},
                           "instruments": []},
                "agri": {"_meta": {"subcategory_id": "context_commodity_agri"},
                         "instruments": []},
                "coal": {"_meta": {"subcategory_id": "context_commodity_coal"},
                         "instruments": []},
            },
            "etf": {
                "broad_market": {"_meta": {"subcategory_id": "context_etf_broad"},
                                  "instruments": [{"symbol": "SPY",
                                                    "include_in_forecast": False}]},
                "sector": {"_meta": {"subcategory_id": "context_etf_sector"},
                           "instruments": []},
                "factor": {"_meta": {"subcategory_id": "context_etf_factor"},
                           "instruments": []},
                "credit": {"_meta": {"subcategory_id": "context_etf_credit"},
                           "instruments": []},
                "commodity_etf": {"_meta": {"subcategory_id": "context_etf_commodity"},
                                   "instruments": []},
                "international": {"_meta": {"subcategory_id": "context_etf_international"},
                                    "instruments": []},
                "thematic": {"_meta": {"subcategory_id": "context_etf_thematic"},
                              "instruments": []},
            },
        }

    def _write_full_yaml(self, tmp_path, context_override: dict) -> str:
        """Build a complete (Layer1+Layer2) YAML matching EXPECTED_TOTAL minus
        the 2 context instruments already in the base fixture, then override
        the context section for targeted negative testing."""
        full = {
            "version": "1.4",
            "us_stocks": {}, "idx_stocks": {}, "commodity": {}, "forex": {},
            "index": [],
            "context": context_override,
        }
        path = tmp_path / "test_full.yaml"
        path.write_text(yaml.dump(full))
        return str(path)

    def test_real_file_has_22_subcategories_and_passes(self):
        """Real instruments.yaml v1.5 must pass with 699 total — updated
        ground truth post ADR-013/014/019 (GMI_Decision_Document_v1.docx)
        and ADR-023/024 (GMI_Decision_Document_v2.docx)."""
        # UPD Decision B Step 2: split file, validate_split() not validate().
        assert validate_split() is True

    def test_reclassified_symbol_in_layer1_fails(self, tmp_path):
        """ADR-003: SPX/VIX/DXY must NOT remain in Layer 1 us_stocks/index/forex."""
        bad_yaml = {
            "version": "1.4",
            "us_stocks": {"Index": [{"symbol": "SPX", "yfinance_symbol": "^GSPC"}]},
            "context": self._minimal_valid_context(),
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(bad_yaml))
        assert validate(str(path)) is False

    def test_deferred_without_reason_fails(self, tmp_path):
        """Extension v1.0 §8.3: context_available=false requires deferred_reason + planned_wave."""
        ctx = self._minimal_valid_context()
        ctx["commodity"]["metals"]["instruments"] = [
            {"symbol": "TIN", "context_available": False}  # missing both required fields
        ]
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_proxy_without_proxy_instrument_fails(self, tmp_path):
        """Extension v1.0 §8.3: proxy_for set requires proxy_instrument + proxy_correlation_expected."""
        ctx = self._minimal_valid_context()
        ctx["commodity"]["metals"]["instruments"] = [
            {"symbol": "IRON_ORE", "proxy_for": "IRON_ORE_SGX_FE62"}  # missing proxy_instrument
        ]
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_etf_broad_with_forecast_true_fails(self, tmp_path):
        """ADR-002: context_etf_broad instruments must have include_in_forecast=false."""
        ctx = self._minimal_valid_context()
        ctx["etf"]["broad_market"]["instruments"] = [
            {"symbol": "SPY", "include_in_forecast": True}  # violates ADR-002
        ]
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_missing_subcategory_fails(self, tmp_path):
        """All 22 subcategories must be present — missing one fails coverage
        check. (This fixture's _minimal_valid_context() predates ADR-014/024
        and legitimately omits context_dollar_basket/context_fx_normalization
        too — still correctly triggers the missing-subcategory error.)"""
        ctx = self._minimal_valid_context()
        del ctx["etf"]["thematic"]  # remove context_etf_thematic
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_context_rates_policy_present_fails(self, tmp_path):
        """Rates Adjustment v1.0 §5.1: old subcategory_id 'context_rates_policy' must be absent."""
        ctx = self._minimal_valid_context()
        ctx["rates"]["fed"]["_meta"]["subcategory_id"] = "context_rates_policy"
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_dm_cb_wrong_count_fails(self, tmp_path):
        """Rates Adjustment v1.0 §11.2: dm_cb must have exactly 9 central banks."""
        ctx = self._minimal_valid_context()
        ctx["rates"]["dm_cb"]["_meta"]["central_banks"] = ["ECB", "BOE"]  # only 2
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_em_cb_wrong_count_fails(self, tmp_path):
        """Rates Adjustment v1.0 §11.2: em_cb must have exactly 3 central banks."""
        ctx = self._minimal_valid_context()
        ctx["rates"]["em_cb"]["_meta"]["central_banks"] = ["PBOC"]  # only 1
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_ohlcv_instruments_in_cb_subcategory_fails(self, tmp_path):
        """Rates Adjustment v1.0 §11.2: CB-rate subcategories must carry NO OHLCV instruments."""
        ctx = self._minimal_valid_context()
        ctx["rates"]["dm_cb"]["instruments"] = [{"symbol": "ECB_RATE"}]
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_missing_context_section_fails(self, tmp_path):
        """v1.4 onward, 'context' section is mandatory."""
        bad_yaml = {"version": "1.4", "us_stocks": {}}
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump(bad_yaml))
        assert validate(str(path)) is False


class TestCommodityTaxonomyValidation:
    """
    NEW — GMI_Decision_Document_v3.docx Decision B Step 1: closes the
    Architecture v2.1 Addendum §7.1/§8 gap. commodity_role/
    commodity_subcategory were specified down to the code in that document
    but had zero occurrences anywhere in the live repo before this change
    (confirmed via empirical grep). This class covers both Layer 1
    commodity_trading and Layer 2 commodity_context call sites, since
    _validate_commodity_taxonomy() is shared between them by design.
    """

    def _minimal_valid_context(self) -> dict:
        return TestValidateInstrumentsLayer2._minimal_valid_context(self)

    def _write_full_yaml(self, tmp_path, context_override: dict, commodity_l1: dict = None) -> str:
        """Same shape as TestValidateInstrumentsLayer2._write_full_yaml, but
        allows overriding Layer 1 'commodity' (default fixture hardcodes it
        to {}, which is unusable for Layer-1-side commodity taxonomy tests)."""
        full = {
            "version": "1.5",
            "us_stocks": {}, "idx_stocks": {},
            "commodity": commodity_l1 if commodity_l1 is not None else {},
            "forex": {}, "index": [],
            "context": context_override,
        }
        path = tmp_path / "test_full.yaml"
        path.write_text(yaml.dump(full))
        return str(path)

    # ── Layer 1 commodity_trading ────────────────────────────────────────

    def test_layer1_missing_commodity_role_fails(self, tmp_path):
        ctx = self._minimal_valid_context()
        l1 = {"Gold/Silver/Oil": [
            {"symbol": "AU", "yfinance_symbol": "GC=F",
             "commodity_subcategory": "precious_metals"}  # missing commodity_role
        ]}
        path = Path(self._write_full_yaml(tmp_path, ctx, l1))
        assert validate(str(path)) is False

    def test_layer1_missing_commodity_subcategory_fails(self, tmp_path):
        ctx = self._minimal_valid_context()
        l1 = {"Gold/Silver/Oil": [
            {"symbol": "AU", "yfinance_symbol": "GC=F",
             "commodity_role": "trading"}  # missing commodity_subcategory
        ]}
        path = Path(self._write_full_yaml(tmp_path, ctx, l1))
        assert validate(str(path)) is False

    def test_layer1_invalid_commodity_role_enum_fails(self, tmp_path):
        ctx = self._minimal_valid_context()
        l1 = {"Gold/Silver/Oil": [
            {"symbol": "AU", "yfinance_symbol": "GC=F",
             "commodity_role": "bogus", "commodity_subcategory": "precious_metals"}
        ]}
        path = Path(self._write_full_yaml(tmp_path, ctx, l1))
        assert validate(str(path)) is False

    def test_layer1_invalid_commodity_subcategory_enum_fails(self, tmp_path):
        ctx = self._minimal_valid_context()
        l1 = {"Gold/Silver/Oil": [
            {"symbol": "AU", "yfinance_symbol": "GC=F",
             "commodity_role": "trading", "commodity_subcategory": "nonsense"}
        ]}
        path = Path(self._write_full_yaml(tmp_path, ctx, l1))
        assert validate(str(path)) is False

    def test_layer1_valid_taxonomy_does_not_add_commodity_errors(self, tmp_path):
        """Correct role+subcategory must not itself trigger a taxonomy
        error (other unrelated errors — e.g. total-count mismatch on a
        deliberately tiny fixture — are expected and not asserted away
        here; this isolates JUST the taxonomy check via the internal
        function, same pattern as TestDomainScoreWeightSumValidation)."""
        from scripts.validate_instruments import _validate_commodity_taxonomy
        errors: list[str] = []
        _validate_commodity_taxonomy(
            {"commodity_role": "trading", "commodity_subcategory": "precious_metals"},
            "AU", "commodity", errors,
        )
        assert errors == []

    # ── Layer 2 commodity_context ────────────────────────────────────────

    def test_layer2_missing_commodity_role_fails(self, tmp_path):
        ctx = self._minimal_valid_context()
        ctx["commodity"]["metals"]["instruments"] = [
            {"symbol": "NICKEL", "yfinance_symbol": "NI=F",
             "commodity_subcategory": "base_metals"}  # missing commodity_role
        ]
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_layer2_missing_commodity_subcategory_fails(self, tmp_path):
        ctx = self._minimal_valid_context()
        ctx["commodity"]["metals"]["instruments"] = [
            {"symbol": "NICKEL", "yfinance_symbol": "NI=F",
             "commodity_role": "context"}  # missing commodity_subcategory
        ]
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_layer2_deferred_instrument_still_requires_taxonomy(self, tmp_path):
        """Addendum §7.1: 'Required For: ALL commodity' — deferred
        (context_available=False) instruments are not exempt."""
        ctx = self._minimal_valid_context()
        ctx["commodity"]["agri"]["instruments"] = [
            {"symbol": "CPO", "context_available": False,
             "deferred_reason": "MYR normalization pending",
             "planned_wave": 2}
            # missing commodity_role AND commodity_subcategory
        ]
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False

    def test_layer2_coal_newc_subcategory_energy_not_coal_passes(self, tmp_path):
        """Addendum §8.2: COAL_NEWC's commodity_subcategory is 'energy',
        deliberately different from its coal context_category — must
        validate cleanly, not be flagged as inconsistent."""
        from scripts.validate_instruments import _validate_commodity_taxonomy
        errors: list[str] = []
        _validate_commodity_taxonomy(
            {"commodity_role": "context", "commodity_subcategory": "energy"},
            "COAL_NEWC", "context.commodity", errors,
        )
        assert errors == []

    # ── Cross-check against real file + sector_rotation.py ──────────────

    def test_real_file_all_14_commodity_instruments_have_valid_taxonomy(self):
        """Direct reproduction of the audit this change closes: walk the
        real instruments.yaml and confirm all 3 Layer 1 + 11 Layer 2
        commodity instruments carry valid, enum-matching taxonomy fields —
        not just that validate() overall returns True (which could pass
        for unrelated reasons)."""
        import yaml as _yaml
        from scripts.validate_instruments import (
            VALID_COMMODITY_ROLES, VALID_COMMODITY_SUBCATEGORIES,
        )
        from src.config.yaml_split_merge import merge_split_trees

        # UPD Decision B Step 2: real file is now split; merge positionally
        # (same utility InstrumentLoader/validate_split() use) before
        # walking it the same way this test always has.
        identity = _yaml.safe_load(Path("config/instruments_identity.yaml").read_text())
        taxonomy = _yaml.safe_load(Path("config/instruments_taxonomy.yaml").read_text())
        data = merge_split_trees(identity, taxonomy)

        l1_commodities = data["commodity"]["Gold/Silver/Oil"]
        assert len(l1_commodities) == 3
        for item in l1_commodities:
            assert item["commodity_role"] == "trading"
            assert item["commodity_subcategory"] in VALID_COMMODITY_SUBCATEGORIES

        l2_commodities = []
        for subcat_block in data["context"]["commodity"].values():
            l2_commodities.extend(subcat_block.get("instruments", []))
        assert len(l2_commodities) == 11
        for item in l2_commodities:
            assert item["commodity_role"] in VALID_COMMODITY_ROLES
            assert item["commodity_subcategory"] in VALID_COMMODITY_SUBCATEGORIES

    def test_subcategory_to_weight_key_map_matches_sector_rotation_keys(self):
        """
        Cross-module consistency guard — this is the exact check that
        would have caught the 'commodity_precious' vs
        'commodity_precious_metals' naming mismatch found empirically
        while implementing this change (Architecture v2.1 Addendum §8.2's
        own key-name table did not match its own §7.1 enum value). Proves
        validate_instruments.py's independently-declared
        COMMODITY_SUBCATEGORY_TO_WEIGHT_KEY map and
        sector_rotation.py's independently-declared REGIME_SECTOR_WEIGHTS
        keys have not drifted apart from each other.
        """
        from scripts.validate_instruments import COMMODITY_SUBCATEGORY_TO_WEIGHT_KEY
        from src.gold.sector_rotation import REGIME_SECTOR_WEIGHTS

        for regime, weights in REGIME_SECTOR_WEIGHTS.items():
            for subcat, weight_key in COMMODITY_SUBCATEGORY_TO_WEIGHT_KEY.items():
                assert weight_key in weights, (
                    f"commodity_subcategory={subcat!r} maps to"
                    f" {weight_key!r}, which is missing from"
                    f" REGIME_SECTOR_WEIGHTS[{regime!r}]"
                )


class TestDomainScoreWeightSumValidation:
    """
    NEW — GMI_Decision_Document_v1.docx ADR-019 (literal restoration of
    domain-score weight sums) and §9 Definition of Done: "All 8 domain
    scores' _meta.contributes_to weights sum to exactly 1.00, verified by
    a new regression test." This is that test, plus coverage for the
    ZERO_WEIGHT_SUBCATEGORIES guard added alongside it (ADR-014/024).
    """

    def test_real_file_all_domain_scores_sum_to_one(self):
        """Direct reproduction of the exact audit performed in
        GMI_Decision_Document_v1.docx §3.4, which found 5 of 8 scores
        (score_dollar_strength, score_yield_curve, score_global_growth,
        score_inflation_pressure, score_risk_appetite) summing to
        1.05-1.30 due to undocumented contributor weights. All 8 must
        now sum to exactly 1.00 against the real instruments.yaml."""
        import yaml as _yaml
        from collections import defaultdict
        from scripts.validate_instruments import _validate_domain_score_weights
        from src.config.yaml_split_merge import merge_split_trees

        # UPD Decision B Step 2: real file is now split; merge positionally.
        identity = _yaml.safe_load(Path("config/instruments_identity.yaml").read_text())
        taxonomy = _yaml.safe_load(Path("config/instruments_taxonomy.yaml").read_text())
        data = merge_split_trees(identity, taxonomy)
        errors: list[str] = []
        _validate_domain_score_weights(data, errors)
        assert errors == [], f"Domain score weight-sum violations: {errors}"

        # Independent re-derivation (does not call the function under test)
        # of the same 8 scores, confirming the fix at the data level too.
        sums = defaultdict(float)

        def walk(node):
            if isinstance(node, dict):
                meta = node.get("_meta")
                if isinstance(meta, dict):
                    for c in meta.get("contributes_to") or []:
                        sums[c["score"]] += float(c["weight"])
                for k, v in node.items():
                    if k != "_meta":
                        walk(v)

        walk(data["context"])
        expected_scores = {
            "score_dollar_strength", "score_yield_curve", "score_global_growth",
            "score_credit_stress", "score_em_risk", "score_risk_appetite",
            "score_commodity_cycle", "score_inflation_pressure",
        }
        assert set(sums.keys()) == expected_scores
        for score, total in sums.items():
            assert abs(total - 1.00) < 1e-9, f"{score}: {total} != 1.00"

    def test_detects_domain_score_weight_drift(self):
        """Negative test: reintroduce ONE of the exact undocumented
        contributors ADR-019 removed (context_rates_fed -> score_dollar_strength,
        weight 0.30) into an otherwise-correct minimal fixture and confirm
        the validator catches the resulting 1.30 sum."""
        from scripts.validate_instruments import _validate_domain_score_weights

        data = {
            "context": {
                "dollar": {
                    "_meta": {"contributes_to": [
                        {"score": "score_dollar_strength", "weight": 0.5},
                    ]},
                },
                "rates": {
                    "curve": {"_meta": {"contributes_to": [
                        {"score": "score_dollar_strength", "weight": 0.3},
                    ]}},
                    "fed": {"_meta": {"contributes_to": [
                        # drift: undocumented contributor, mirrors the exact
                        # bug ADR-019 fixed
                        {"score": "score_dollar_strength", "weight": 0.3},
                    ]}},
                },
            }
        }
        errors: list[str] = []
        _validate_domain_score_weights(data, errors)
        assert len(errors) == 1
        assert "score_dollar_strength" in errors[0]
        assert "1.10" in errors[0] or "1.1" in errors[0]

    def _minimal_valid_context(self) -> dict:
        """Same base fixture as TestValidateInstrumentsLayer2 — duplicated
        rather than cross-referenced to keep each test class self-contained,
        consistent with this file's existing style."""
        return TestValidateInstrumentsLayer2._minimal_valid_context(self)

    def _write_full_yaml(self, tmp_path, context_override: dict) -> str:
        return TestValidateInstrumentsLayer2._write_full_yaml(self, tmp_path, context_override)

    def test_zero_weight_subcategories_enforced(self, tmp_path):
        """ADR-014/024: context_dollar_basket / context_fx_normalization must
        never carry a contributes_to weight — reintroducing one must fail
        validation, guarding against the exact triple-counting risk ADR-014
        rejected when it declined to fold basket currencies into
        context_dollar."""
        ctx = self._minimal_valid_context()
        ctx["dollar_basket"] = {
            "_meta": {
                "subcategory_id": "context_dollar_basket",
                "contributes_to": [
                    {"score": "score_dollar_strength", "weight": 0.1}
                ],
            },
            "instruments": [{"symbol": "CNH", "yfinance_symbol": "USDCNH=X"}],
        }
        ctx["fx_normalization"] = {
            "_meta": {"subcategory_id": "context_fx_normalization", "contributes_to": []},
            "instruments": [{"symbol": "MYR", "yfinance_symbol": "MYR=X"}],
        }
        path = Path(self._write_full_yaml(tmp_path, ctx))
        assert validate(str(path)) is False


class TestValidateSplit:
    """NEW (Decision B Step 2-3, GMI_Decision_Document_v5.docx §2.1-§2.2) —
    validate_split(), the real production entry point, and its interaction
    with the shared merge utility + jsonschema layer. Uses small, self-
    contained synthetic fixtures (not _minimal_valid_context()'s 20-
    subcategory skeleton) since these tests target the split/merge/schema
    plumbing specifically, not the full rule set already covered elsewhere
    in this file via validate()/validate_data()."""

    def _write(self, tmp_path, name: str, content: dict) -> str:
        path = tmp_path / name
        path.write_text(yaml.dump(content))
        return str(path)

    def test_real_split_files_pass_via_default_paths(self):
        """No-arg call reads the real config/instruments_identity.yaml +
        config/instruments_taxonomy.yaml — the same invocation ci.yml
        Gate G-3 uses (`python scripts/validate_instruments.py`)."""
        assert validate_split() is True

    def test_explicit_paths_are_honored(self, tmp_path):
        identity = {
            "version": "1.0", "last_updated": "2026-01-01",
            "us_stocks": {}, "idx_stocks": {}, "commodity": {}, "forex": {},
            "index": [], "context": {},
        }
        taxonomy = {
            "version": "1.0", "last_updated": "2026-01-01",
            "us_stocks": {}, "idx_stocks": {}, "commodity": {}, "forex": {},
            "index": [], "context": {},
        }
        id_path = self._write(tmp_path, "id.yaml", identity)
        tax_path = self._write(tmp_path, "tax.yaml", taxonomy)
        # This will fail the EXPECTED_TOTAL check (0 symbols) — the point
        # here is only that the explicit paths were actually read, not
        # silently ignored in favor of the real config/ defaults.
        result = validate_split(identity_path=id_path, taxonomy_path=tax_path)
        assert result is False

    def test_merge_misalignment_between_files_raises_not_silently_fails(self, tmp_path):
        """A misaligned split (different symbol at the same list index) is
        not a normal validation finding — merge_split_trees() raises, and
        validate_split() deliberately does not swallow that exception."""
        identity = {
            "version": "1.0", "last_updated": "2026-01-01",
            "us_stocks": {"Technology": [{"symbol": "AAPL"}]},
            "idx_stocks": {}, "commodity": {}, "forex": {}, "index": [],
            "context": {},
        }
        taxonomy = {
            "version": "1.0", "last_updated": "2026-01-01",
            # Wrong symbol at the same index — files edited out of sync.
            "us_stocks": {"Technology": [{"symbol": "MSFT"}]},
            "idx_stocks": {}, "commodity": {}, "forex": {}, "index": [],
            "context": {},
        }
        id_path = self._write(tmp_path, "id.yaml", identity)
        tax_path = self._write(tmp_path, "tax.yaml", taxonomy)
        with pytest.raises(ValueError, match="anchor key 'symbol' mismatch"):
            validate_split(identity_path=id_path, taxonomy_path=tax_path)


class TestJsonSchemaLayer:
    """NEW (Decision B Step 3) — the jsonschema structural layer added
    alongside the hand-written checks. Confirms it has real teeth (catches
    a genuine type error the hand-written Python duck-typing wouldn't
    necessarily flag) and that the 3 schema documents themselves are
    valid Draft 7 schemas."""

    def _write(self, tmp_path, name: str, content: dict) -> str:
        path = tmp_path / name
        path.write_text(yaml.dump(content))
        return str(path)

    def test_all_three_schema_files_are_valid_draft7(self):
        import jsonschema
        schemas_dir = Path("config/schemas/instruments")
        for name in (
            "identity.schema.yaml", "taxonomy.schema.yaml",
            "regime_sector_weights.schema.yaml",
        ):
            schema = yaml.safe_load((schemas_dir / name).read_text())
            jsonschema.Draft7Validator.check_schema(schema)  # raises if invalid

    def test_index_not_required_by_schema(self):
        """ADR-035 (GMI_Decision_Document_v8.docx, 10 Aug 2026): 'index' was
        a required top-level property in both schemas — dropping the empty
        'index: []' section from the real config files without this fix
        would have made validate_split() fail on them. Confirms the fix
        directly against the schema documents themselves."""
        import jsonschema
        schemas_dir = Path("config/schemas/instruments")
        for name in ("identity.schema.yaml", "taxonomy.schema.yaml"):
            schema = yaml.safe_load((schemas_dir / name).read_text())
            assert "index" not in schema.get("required", [])
            assert "index" not in schema.get("properties", {})

    def test_split_file_without_index_key_still_validates(self):
        """ADR-035: a minimal dict with no 'index' key at all (the real
        post-ADR-035 shape) must pass jsonschema validation directly —
        tested against jsonschema.validate() rather than the full
        validate_split() pipeline, since the latter also enforces full
        22-subcategory coverage (an unrelated concern to 'index')."""
        import jsonschema
        schemas_dir = Path("config/schemas/instruments")
        minimal = {
            "version": "1.0", "last_updated": "2026-01-01",
            "us_stocks": {}, "idx_stocks": {}, "commodity": {}, "forex": {},
            "context": {},
        }
        for name in ("identity.schema.yaml", "taxonomy.schema.yaml"):
            schema = yaml.safe_load((schemas_dir / name).read_text())
            jsonschema.validate(minimal, schema)  # raises if 'index' still required

    def test_wrong_type_on_context_available_is_caught(self, tmp_path):
        """context_available written as the string 'true' instead of a
        real boolean — exactly the class of bug jsonschema exists to
        catch that plain dict access wouldn't necessarily flag (Python
        would happily treat the string 'true' as truthy without erroring
        anywhere in the hand-written checks)."""
        identity = {
            "version": "1.0", "last_updated": "2026-01-01",
            "us_stocks": {}, "idx_stocks": {}, "commodity": {}, "forex": {},
            "index": [], "context": {
                "dollar": {"instruments": [{"symbol": "DXY", "yfinance_symbol": "DX-Y.NYB"}]},
            },
        }
        taxonomy = {
            "version": "1.0", "last_updated": "2026-01-01",
            "us_stocks": {}, "idx_stocks": {}, "commodity": {}, "forex": {},
            "index": [], "context": {
                "dollar": {
                    "_meta": {"subcategory_id": "context_dollar", "contributes_to": []},
                    "instruments": [{"symbol": "DXY", "context_available": "true"}],
                },
            },
        }
        id_path = self._write(tmp_path, "id.yaml", identity)
        tax_path = self._write(tmp_path, "tax.yaml", taxonomy)
        assert validate_split(identity_path=id_path, taxonomy_path=tax_path) is False

    def test_invalid_commodity_role_enum_is_caught(self, tmp_path):
        identity = {
            "version": "1.0", "last_updated": "2026-01-01",
            "us_stocks": {}, "idx_stocks": {},
            "commodity": {"Gold/Silver/Oil": [{"symbol": "AU", "yfinance_symbol": "GC=F"}]},
            "forex": {}, "index": [], "context": {},
        }
        taxonomy = {
            "version": "1.0", "last_updated": "2026-01-01",
            "us_stocks": {}, "idx_stocks": {},
            "commodity": {"Gold/Silver/Oil": [
                {"symbol": "AU", "commodity_role": "not_a_valid_role",
                 "commodity_subcategory": "precious_metals"},
            ]},
            "forex": {}, "index": [], "context": {},
        }
        id_path = self._write(tmp_path, "id.yaml", identity)
        tax_path = self._write(tmp_path, "tax.yaml", taxonomy)
        assert validate_split(identity_path=id_path, taxonomy_path=tax_path) is False
