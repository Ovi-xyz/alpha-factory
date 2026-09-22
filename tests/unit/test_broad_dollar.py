"""
tests/unit/test_broad_dollar.py — regression-lock tests for
gold/cross_asset/broad_dollar.py.

NEW (chat thread, 22 Sep 2026): this module had no dedicated unit test
file before this thread -- only indirect coverage via
test_forecast_module.py / test_correlation_module.py, which exercise its
outputs but never assert its own invariants directly. Added while wiring
ADR-049's NZD weight into BIS_WEIGHTS (KNOWN_RISKS.md RISK-16), so the
14-currency basket, NZD's sign convention, and the renormalization
invariant can't silently regress in a future refactor.
"""

import src.gold.cross_asset.broad_dollar as bd


class TestRawBisWeights:
    def test_fourteen_currencies_present_adr049(self):
        """ADR-049 (chat thread, 5 Sep 2026; wired 22 Sep 2026): the
        Layer-1-reuse currency set extends from 6 to 7 (adds NZD_USD),
        bringing the total BIS-sourced currency set from 13 to 14.
        Locks in the count so it can't silently drop back to 13."""
        assert len(bd._RAW_BIS_WEIGHTS_PCT) == 14
        assert "NZD" in bd._RAW_BIS_WEIGHTS_PCT

    def test_nzd_raw_weight_matches_live_extraction_22_sep_2026(self):
        """Real value from Ovi's --extract-weights run on the M1, 22 Sep
        2026 (2020_2022 BIS vintage, REF_AREA NZ) -- not guessed or
        interpolated, per this module's own "read live output, don't
        theorize" discipline (module docstring)."""
        assert bd._RAW_BIS_WEIGHTS_PCT["NZD"] == 0.069725

    def test_thirteen_pre_existing_values_unchanged_by_nzd_addition(self):
        """The 22 Sep 2026 extraction re-confirmed all 13 pre-existing
        currencies digit-for-digit identical to the original 12 Sep 2026
        run (same static 2020_2022 vintage sheet) -- locks in that this
        session's edit only ADDED a key, never touched an existing one."""
        unchanged = {
            "AUD": 0.326985, "CAD": 8.054349, "CHF": 2.885704,
            "CNH": 22.579565, "EUR": 16.048545, "GBP": 2.939921,
            "HKD": 0.026992, "IDR": 0.929609, "JPY": 5.897981,
            "KRW": 4.183282, "NOK": 0.168854, "SGD": 1.341145,
            "TWD": 3.216667,
        }
        for ccy, pct in unchanged.items():
            assert bd._RAW_BIS_WEIGHTS_PCT[ccy] == pct, ccy


class TestCurrencySymbolMap:
    def test_nzd_maps_to_nzd_usd_negated(self):
        """NZD_USD quotes USD as the QUOTE currency (a pair RISE means
        USD weakened) -- same convention as AUD_USD/EUR_USD/GBP_USD, so
        negate=True (module docstring's SIGN CONVENTION paragraph)."""
        assert bd._CURRENCY_SYMBOL_MAP["NZD"] == ("NZD_USD", True)

    def test_four_negated_currencies_exactly(self):
        """AUD, EUR, GBP, NZD are the only USD-is-quote (negated)
        currencies; the other 10 are USD-is-base (not negated). Locks in
        the post-ADR-049 4/10 split so a future currency addition can't
        silently land on the wrong side without a test failing."""
        negated = {k for k, (_, neg) in bd._CURRENCY_SYMBOL_MAP.items() if neg}
        assert negated == {"AUD", "EUR", "GBP", "NZD"}

    def test_every_raw_weight_key_has_a_symbol_map_entry(self):
        """No currency in _RAW_BIS_WEIGHTS_PCT can be missing from
        _CURRENCY_SYMBOL_MAP -- BIS_WEIGHTS's dict comprehension would
        raise a bare KeyError at import time otherwise; this test fails
        with a clear message instead."""
        missing = set(bd._RAW_BIS_WEIGHTS_PCT) - set(bd._CURRENCY_SYMBOL_MAP)
        assert not missing, f"currencies with no symbol-map entry: {missing}"


class TestBisWeightsRenormalization:
    def test_fourteen_symbols_in_bis_weights(self):
        assert len(bd.BIS_WEIGHTS) == 14
        assert "NZD_USD" in bd.BIS_WEIGHTS

    def test_magnitude_sums_to_one(self):
        """BIS_WEIGHTS is renormalized so sum(|values|) == 1.0 (module
        docstring's DECISION paragraph) -- this is what keeps Broad
        Dollar at a comparable scale to DXY for the divergence signal to
        be meaningful, not an artifact of a shrinking/growing basket."""
        mag_sum = sum(abs(v) for v in bd.BIS_WEIGHTS.values())
        assert abs(mag_sum - 1.0) < 1e-9

    def test_nzd_usd_weight_is_negative(self):
        """NZD_USD is a negated (USD-is-quote) leg -- its signed weight
        in BIS_WEIGHTS must be negative."""
        assert bd.BIS_WEIGHTS["NZD_USD"] < 0.0

    def test_bis_weights_keys_match_expected_silver_symbols(self):
        """Full expected symbol set post-ADR-049 -- 7 Layer 1 forex
        majors (AUD_USD, USD_CAD, USD_CHF, EUR_USD, GBP_USD, USD_JPY,
        NZD_USD) + 7 Layer 2 dollar_basket currencies (CNH, HKD, IDR,
        KRW, NOK, SGD, TWD)."""
        assert set(bd.BIS_WEIGHTS.keys()) == {
            "AUD_USD", "USD_CAD", "USD_CHF", "EUR_USD", "GBP_USD",
            "USD_JPY", "NZD_USD",
            "CNH", "HKD", "IDR", "KRW", "NOK", "SGD", "TWD",
        }


class TestGate1Metadata:
    def test_extraction_date_updated_to_nzd_run(self):
        """UPD ADR-049 (22 Sep 2026): GATE_1_EXTRACTION_DATE moved from
        the original 12 Sep 2026 run to the 22 Sep 2026 run that added
        NZD, since the latter is now the complete 14-currency source."""
        assert bd.GATE_1_EXTRACTION_DATE == "2026-09-22"

    def test_vintage_unchanged(self):
        """Same BIS "2020_2022" sheet both times -- only the target
        currency set changed, not the underlying vintage."""
        assert bd.BIS_WEIGHTS_VINTAGE == "2020_2022"
