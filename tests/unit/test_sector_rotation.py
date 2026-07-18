"""tests/unit/test_sector_rotation.py — sector rotation test suite"""

import pytest

from src.gold.sector_rotation import REGIME_SECTOR_WEIGHTS, NEUTRAL_WEIGHTS


class TestRegimeSectorWeights:

    def test_all_regimes_defined(self):
        """All 5 regimes must be present."""
        required = {"RISK_ON", "RISK_OFF", "STAGFLATION", "REFLATION", "DISINFLATION"}
        assert required == set(REGIME_SECTOR_WEIGHTS.keys())

    def test_all_sectors_per_regime(self):
        """
        Every regime must define weights for all markets/sectors.

        UPD Decision B Step 1 (GMI_Decision_Document_v3.docx, Architecture
        v2.1 Addendum §8.1): flat 'commodity' key replaced by 5 disaggregated
        commodity_* subcategory keys. This asserts the NEW contract — the
        flat key is gone by design, not a regression.
        """
        expected_keys = {
            "Technology", "Consumer Discretionary", "Communication Services",
            "Financials", "Industrials", "Health Care", "Energy", "Materials",
            "Consumer Staples", "Real Estate", "Utilities",
            "idx", "forex",
            "commodity_precious_metals", "commodity_energy", "commodity_base_metals",
            "commodity_agricultural", "commodity_bulks",
            "index", "High Growth & Popular",
        }
        for regime, weights in REGIME_SECTOR_WEIGHTS.items():
            missing = expected_keys - set(weights.keys())
            assert not missing, f"Regime {regime} missing: {missing}"
            assert "commodity" not in weights, (
                f"Regime {regime} still carries the old flat 'commodity' key"
                " — should have been fully replaced by Decision B Step 1"
            )

    def test_weights_in_valid_range(self):
        """All weights must be between 0.0 and 2.0."""
        for regime, weights in REGIME_SECTOR_WEIGHTS.items():
            for sector, w in weights.items():
                assert 0.0 <= w <= 2.0, (
                    f"Regime {regime}, sector {sector}: weight={w} out of [0, 2]"
                )

    def test_risk_on_tech_overweight(self):
        """RISK_ON: Technology should be overweight (>= 1.3)."""
        assert REGIME_SECTOR_WEIGHTS["RISK_ON"]["Technology"] >= 1.3

    def test_risk_off_defensives_overweight(self):
        """RISK_OFF: Utilities, Consumer Staples, Health Care > 1.0."""
        ro = REGIME_SECTOR_WEIGHTS["RISK_OFF"]
        assert ro["Utilities"]        > 1.0
        assert ro["Consumer Staples"] > 1.0
        assert ro["Health Care"]      > 1.0

    def test_risk_off_tech_underweight(self):
        """RISK_OFF: Technology should be underweight (< 1.0)."""
        assert REGIME_SECTOR_WEIGHTS["RISK_OFF"]["Technology"] < 1.0

    def test_stagflation_energy_commodity_overweight(self):
        """
        STAGFLATION: Energy + commodity_energy primary beneficiaries.

        UPD Decision B Step 1: asserts commodity_energy (was flat
        'commodity') — matches the Addendum §8.3 matrix's own STAGFLATION
        row (commodity_energy=1.5, the only commodity_* key >= 1.5 in that
        regime; commodity_precious_metals=1.4 is close but distinct).
        """
        s = REGIME_SECTOR_WEIGHTS["STAGFLATION"]
        assert s["Energy"]           >= 1.5
        assert s["commodity_energy"] >= 1.5

    def test_neutral_weights_all_ones(self):
        """NEUTRAL_WEIGHTS should all be 1.0 (no bias)."""
        assert all(v == 1.0 for v in NEUTRAL_WEIGHTS.values())

    def test_idx_risk_on_vs_risk_off(self):
        """IDX30 weight: RISK_ON > RISK_OFF (foreign fund flow logic)."""
        assert (
            REGIME_SECTOR_WEIGHTS["RISK_ON"]["idx"]
            > REGIME_SECTOR_WEIGHTS["RISK_OFF"]["idx"]
        )


class TestCommoditySubcategoryDisaggregation:
    """
    Decision B Step 1 (GMI_Decision_Document_v3.docx): closes the
    Architecture v2.1 Addendum §7.1/§8 gap — commodity_role/
    commodity_subcategory fields + 5-key REGIME_SECTOR_WEIGHTS
    disaggregation were specified down to the code in that document but
    never actually implemented anywhere in the live repo (confirmed via
    empirical grep — zero occurrences — before this change).
    """

    EXPECTED_COMMODITY_KEYS = {
        "commodity_precious_metals", "commodity_energy", "commodity_base_metals",
        "commodity_agricultural", "commodity_bulks",
    }

    # Addendum §8.3's exact matrix — spot-checked per regime, not just shape.
    EXPECTED_MATRIX = {
        "RISK_ON":       {"commodity_precious_metals": 0.7, "commodity_energy": 1.0, "commodity_base_metals": 1.4, "commodity_agricultural": 1.1, "commodity_bulks": 1.3},
        "RISK_OFF":      {"commodity_precious_metals": 1.4, "commodity_energy": 0.9, "commodity_base_metals": 0.6, "commodity_agricultural": 0.9, "commodity_bulks": 0.5},
        "STAGFLATION":   {"commodity_precious_metals": 1.4, "commodity_energy": 1.5, "commodity_base_metals": 0.8, "commodity_agricultural": 1.3, "commodity_bulks": 0.7},
        "REFLATION":     {"commodity_precious_metals": 0.9, "commodity_energy": 1.3, "commodity_base_metals": 1.4, "commodity_agricultural": 1.1, "commodity_bulks": 1.2},
        "DISINFLATION":  {"commodity_precious_metals": 1.2, "commodity_energy": 0.6, "commodity_base_metals": 0.7, "commodity_agricultural": 0.8, "commodity_bulks": 0.6},
    }

    def test_matrix_matches_addendum_exactly(self):
        """Every regime's 5 commodity_* weights match Addendum §8.3 verbatim."""
        for regime, expected in self.EXPECTED_MATRIX.items():
            actual = REGIME_SECTOR_WEIGHTS[regime]
            for key, val in expected.items():
                assert actual[key] == pytest.approx(val), (
                    f"{regime}.{key}: expected {val}, got {actual[key]}"
                )

    def test_no_orphaned_commodity_subcategory(self):
        """
        Every commodity instrument's commodity_subcategory (Layer 1 + Layer
        2) must resolve to a real REGIME_SECTOR_WEIGHTS key. A typo here
        would silently fall back to weights.get(key, 1.0) = neutral instead
        of erroring — this test exists specifically to catch that failure
        mode (half-fixes are worse than no fix).
        """
        from src.config.instrument_loader import get_loader

        get_loader.cache_clear()
        loader = get_loader()

        checked = 0
        for inst in loader.all_symbols():
            if inst.is_commodity:
                assert inst.commodity_subcategory is not None, (
                    f"{inst.symbol}: commodity_role=trading but "
                    "commodity_subcategory is None"
                )
                key = f"commodity_{inst.commodity_subcategory}"
                assert key in self.EXPECTED_COMMODITY_KEYS, (
                    f"{inst.symbol}: commodity_subcategory="
                    f"{inst.commodity_subcategory!r} -> {key!r} is not a "
                    "valid REGIME_SECTOR_WEIGHTS key"
                )
                checked += 1
        assert checked == 3, f"Expected 3 Layer 1 commodity_trading instruments, found {checked}"

        for inst in loader.all_context(include_deferred=True):
            if inst.context_group == "commodity":
                assert inst.commodity_subcategory is not None, (
                    f"{inst.symbol}: context_group=commodity but "
                    "commodity_subcategory is None"
                )
                key = f"commodity_{inst.commodity_subcategory}"
                assert key in self.EXPECTED_COMMODITY_KEYS, (
                    f"{inst.symbol}: commodity_subcategory="
                    f"{inst.commodity_subcategory!r} -> {key!r} is not a "
                    "valid REGIME_SECTOR_WEIGHTS key"
                )
                checked += 1
        assert checked == 14, f"Expected 14 total commodity instruments (3 L1 + 11 L2), found {checked}"

    def test_coal_newc_subcategory_is_energy_not_coal(self):
        """
        Addendum §8.2's explicit mapping: COAL_NEWC -> commodity_energy, NOT
        a coal-specific key — deliberately different from its
        context_category (context_commodity_coal), which stays untouched.
        Two different taxonomies, same instrument. (context_group for ALL
        Layer 2 commodity instruments is the coarse 'commodity' group
        regardless of energy/metals/agri/coal subgroup — the fine subgroup
        lives in context_category, not context_group.)
        """
        from src.config.instrument_loader import get_loader

        get_loader.cache_clear()
        loader = get_loader()
        coal_newc = next(
            i for i in loader.all_context(include_deferred=True)
            if i.symbol == "COAL_NEWC"
        )
        assert coal_newc.commodity_subcategory == "energy"
        assert coal_newc.context_group == "commodity"                  # unchanged
        assert coal_newc.context_category == "context_commodity_coal"  # unchanged

    def test_iron_ore_primary_subcategory_is_base_metals(self):
        """
        Addendum §11 OD-C6 resolution: IRON_ORE's PRIMARY
        commodity_subcategory is base_metals, not bulks — true dual
        registration was explicitly left as a future CrossAssetEngine
        enhancement (OD-C6 status: DEFERRED), not built here.
        """
        from src.config.instrument_loader import get_loader

        get_loader.cache_clear()
        loader = get_loader()
        iron_ore = next(
            i for i in loader.all_context(include_deferred=True)
            if i.symbol == "IRON_ORE"
        )
        assert iron_ore.commodity_subcategory == "base_metals"

    def test_run_produces_correct_disaggregated_weights_for_layer1_commodities(
        self, tmp_path, monkeypatch
    ):
        """
        End-to-end: run() against a synthetic RISK_OFF regime must give
        AU/AG (precious_metals) weight 1.4 and CL (energy) weight 0.9 —
        the only 3 commodity instruments that actually reach gold_screener.
        """
        import polars as pl
        from datetime import date as date_cls
        from src.gold.sector_rotation import run, GOLD_SECTOR_PATH

        monkeypatch.chdir(tmp_path)
        regime_dir = tmp_path / "data" / "gold" / "macro"
        regime_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "date": ["2026-07-17"], "regime": ["RISK_OFF"],
        }).write_parquet(regime_dir / "regime_store.parquet")

        run(date_cls(2026, 7, 17))

        out = pl.read_parquet(tmp_path / GOLD_SECTOR_PATH / "sector_regime_weights.parquet")
        by_symbol = {r["symbol"]: r["sector_weight_adj"] for r in out.to_dicts()}
        assert by_symbol["AU"] == pytest.approx(1.4)
        assert by_symbol["AG"] == pytest.approx(1.4)
        assert by_symbol["CL"] == pytest.approx(0.9)


class TestGetActiveRegimeParameterizedQuery:
    """
    FIX GMI-AUD-001: _get_active_regime() previously used f-string SQL
    (`f"SELECT regime FROM read_parquet('{regime_path}')" f" WHERE date =
    '{run_date}' LIMIT 1"`) — a GD §17.7 violation discovered during the
    RISK-3 audit (KNOWN_RISKS.md), NOT caught by the earlier GLD-003 audit
    (test_fstring_sql_absence.py's own docstring explicitly scoped that
    audit to a different file list). Regression guard: real query against
    a real fixture, not just "no f-string substring present" — proves the
    $name-parameterized version returns identical results to what the old
    f-string version would have.
    """

    def test_reads_regime_via_parameterized_query(self, tmp_path, monkeypatch):
        import polars as pl
        from datetime import date as date_cls
        from src.gold.sector_rotation import _get_active_regime

        monkeypatch.chdir(tmp_path)
        regime_dir = tmp_path / "data" / "gold" / "macro"
        regime_dir.mkdir(parents=True, exist_ok=True)
        df = pl.DataFrame({
            "date":   ["2026-06-29", "2026-06-30", "2026-07-01"],
            "regime": ["RISK_ON",    "RISK_OFF",   "STAGFLATION"],
        })
        df.write_parquet(regime_dir / "regime_store.parquet")

        result = _get_active_regime(date_cls(2026, 7, 1))
        assert result == "STAGFLATION"

    def test_falls_back_to_risk_on_when_store_missing(self, tmp_path, monkeypatch):
        from datetime import date as date_cls
        from src.gold.sector_rotation import _get_active_regime

        monkeypatch.chdir(tmp_path)
        result = _get_active_regime(date_cls(2026, 7, 1))
        assert result == "RISK_ON"

    def test_falls_back_when_date_not_present(self, tmp_path, monkeypatch):
        """A date with no matching row must fall back, not raise or
        silently return an unrelated row — proves $run_date binding
        actually filters correctly, not just 'query runs without error'."""
        import polars as pl
        from datetime import date as date_cls
        from src.gold.sector_rotation import _get_active_regime

        monkeypatch.chdir(tmp_path)
        regime_dir = tmp_path / "data" / "gold" / "macro"
        regime_dir.mkdir(parents=True, exist_ok=True)
        df = pl.DataFrame({
            "date":   ["2026-06-29"],
            "regime": ["RISK_ON"],
        })
        df.write_parquet(regime_dir / "regime_store.parquet")

        result = _get_active_regime(date_cls(2099, 1, 1))
        assert result == "RISK_ON"   # fallback default, not a stale match
