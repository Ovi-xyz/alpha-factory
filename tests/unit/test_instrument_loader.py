"""tests/unit/test_instrument_loader.py — InstrumentLoader test suite"""

from pathlib import Path

import pytest
import yaml

from src.config.instrument_loader import get_loader, InstrumentLoader


class TestInstrumentLoader:

    def setup_method(self):
        self.loader = get_loader()

    def test_total_count_639(self):
        """
        FIX GMI-IL-001: was test_total_count_643, asserted count()==643.
        Architecture Extension v1.0 ADR-003 reclassifies SPX, VIX (Layer 1
        us_stocks.Index) and DXY (Layer 1 forex) to Layer 2 context anchors.
        Layer 1 trading universe: 643 - 2 (SPX, VIX) - 1 (DXY) = 640.
        UPD ADR-036 (GMI_Decision_Document_v8.docx, 10 Aug 2026): USD_IDR
        reclassified forex -> context.dollar_basket (renamed IDR) — 640 -> 639.
        This is an intentional, documented contract change — not a regression.
        Layer 2 (context_available=True) count is asserted separately in
        TestInstrumentLoaderLayer2.test_all_context_default_count.
        """
        assert self.loader.count() == 639

    def test_get_aapl(self):
        inst = self.loader.get("AAPL")
        assert inst.symbol == "AAPL"
        assert inst.market == "us_stocks"
        assert inst.yfinance_symbol == "AAPL"
        assert inst.timezone == "America/New_York"

    def test_get_bbca_idx(self):
        inst = self.loader.get("BBCA")
        assert inst.market == "idx"
        assert inst.yfinance_symbol == "BBCA.JK"
        assert inst.timezone == "Asia/Jakarta"
        assert inst.tvfeed_symbol == "BBCA"

    def test_get_eur_usd_forex(self):
        inst = self.loader.get("EUR_USD")
        assert inst.market == "forex"
        assert inst.raw_symbol == "EUR/USD"
        assert inst.yfinance_symbol == "EURUSD=X"
        assert inst.timezone == "UTC"

    def test_spx_reclassified_to_layer2(self):
        """
        FIX GMI-IL-001: was test_get_spx_index, asserted
        loader.get('SPX', market='index') == Instrument(yf='^GSPC').
        Architecture Extension v1.0 ADR-003: SPX is no longer Layer 1
        (market='index' is now empty — no tradeable index candidates).
        SPX is now exclusively a Layer 2 context anchor under
        context_equity_dm, retrieved via get_context().
        """
        with pytest.raises(KeyError):
            self.loader.get("SPX", market="index")

        inst = self.loader.get_context("SPX")
        assert inst.market == "context"
        assert inst.yfinance_symbol == "^GSPC"
        assert inst.layer == 2
        assert inst.context_category == "context_equity_dm"
        assert inst.reclassified_from == "layer_1_index"

    def test_by_market_counts(self):
        """
        FIX GMI-IL-001: forex 20->19 (DXY removed), index 2->0 (SPX/VIX removed).
        us_stocks/idx/commodity unchanged — see ADR-003.
        UPD ADR-036 (GMI_Decision_Document_v8.docx, 10 Aug 2026): forex
        19->18 (USD_IDR reclassified to context.dollar_basket).
        """
        assert len(self.loader.by_market("us_stocks")) == 588
        assert len(self.loader.by_market("idx"))       == 30
        assert len(self.loader.by_market("forex"))     == 18
        assert len(self.loader.by_market("commodity")) == 3
        assert len(self.loader.by_market("index"))     == 0

    def test_sectors_not_empty(self):
        sectors = self.loader.sectors()
        assert len(sectors) > 0
        assert "Technology" in sectors

    def test_instrument_is_frozen(self):
        """Instrument dataclass adalah immutable."""
        inst = self.loader.get("AAPL")
        with pytest.raises(Exception):   # frozen=True → AttributeError
            inst.symbol = "CHANGED"

    def test_cl_requires_market_param(self):
        """CL ada di us_stocks (Colgate) DAN commodity (WTI) — butuh market param."""
        with pytest.raises(KeyError):
            self.loader.get("CL")   # ambiguous — must specify market

        cl_stock = self.loader.get("CL", market="us_stocks")
        cl_commo = self.loader.get("CL", market="commodity")
        assert cl_stock.market == "us_stocks"
        assert cl_commo.market == "commodity"
        assert cl_commo.eia_series == "PET.RWTC.W"

    def test_market_map_returns_dict(self):
        mkt_map = self.loader.market_map()
        assert isinstance(mkt_map, dict)
        assert mkt_map.get("AAPL") == "us_stocks"
        assert mkt_map.get("BBCA") == "idx"

    def test_market_map_excludes_layer2(self):
        """
        ADD GMI-IL-001: market_map() must remain Layer-1-only — ActiveSymbolsResolver
        joins this against Silver OHLCV for dollar_volume_20d screening, which is a
        Layer 1 concept only (Layer 2 context is always-on, GD §0.2).
        """
        mkt_map = self.loader.market_map()
        assert "VIX" not in mkt_map
        assert "DXY" not in mkt_map
        assert "SPX" not in mkt_map

    def test_singleton(self):
        """get_loader() returns same instance via lru_cache."""
        l1 = get_loader()
        l2 = get_loader()
        assert l1 is l2


class TestInstrumentLoaderLayer2:
    """
    ADD GMI-IL-001 — Architecture Extension v1.0 §8.1 Layer 2 context API.
    Covers all_context, by_context_category, by_context_group, forecast_context,
    correlation_context, deferred_count, get_context, subcategory_meta.
    """

    def setup_method(self):
        self.loader = get_loader()

    def test_all_context_default_count(self):
        """all_context() default excludes deferred.
        UPD ADR-034 (GMI_Decision_Document_v8.docx, 10 Aug 2026): TIN and
        RUBBER re-deferred (weak proxy correlation, +0.139/+0.229 over
        120mo — see check_proxy_correlation.py's 10 Aug 2026 live run).
        CPO and NICKEL remain active (retained with correlation caveats,
        +0.405/+0.586). Combined with ADR-036's IDR addition to Layer 2
        (60 total slots, up from 59): 60 - 2 deferred = 58 active."""
        ctx = self.loader.all_context()
        assert len(ctx) == 58
        assert all(i.context_available for i in ctx)

    def test_all_context_include_deferred(self):
        """all_context(include_deferred=True) returns all 60 — Extension
        v1.0 §3.1 (52) extended by ADR-014 (+6 context_dollar_basket),
        ADR-024 (+1 context_fx_normalization), and ADR-036 (+1 IDR into
        dollar_basket, 10 Aug 2026) = 60. FIX ADR-034: TIN and RUBBER are
        deferred again (2 of the 60); CPO and NICKEL remain active."""
        ctx = self.loader.all_context(include_deferred=True)
        assert len(ctx) == 60
        symbols = {i.symbol for i in ctx}
        assert {"TIN", "CPO", "RUBBER", "NICKEL"}.issubset(symbols)
        assert {"CNH", "KRW", "SGD", "HKD", "TWD", "NOK", "IDR"}.issubset(symbols)
        assert "THB" in symbols
        assert "MYR" not in symbols   # ADR-037: MYR removed, replaced by THB

    def test_count_context(self):
        """UPD ADR-034/036 (10 Aug 2026): active (58) now genuinely differs
        from total-with-deferred (60) — previously coincided at 59 only
        because deferred_count() happened to be 0."""
        assert self.loader.count_context() == 58
        assert self.loader.count_context(include_deferred=True) == 60

    def test_count_total(self):
        """Layer 1 (639) + Layer 2 active (58) = 697 OHLCV-bearing instruments.
        UPD ADR-034/036 (GMI_Decision_Document_v8.docx, 10 Aug 2026): this
        now genuinely diverges from EXPECTED_TOTAL (699, validate_instruments.py)
        for the first time — 699 counts ALL declared Layer 2 slots including
        the 2 now-deferred (TIN, RUBBER), while count_total() intentionally
        counts only what's actually active/ingested. Previously these two
        numbers coincided (699) purely because deferred_count() was 0 at
        the time — see count_total()'s own docstring for the distinction."""
        assert self.loader.count_total() == 697

    def test_deferred_count_is_2(self):
        """FIX ADR-034 (GMI_Decision_Document_v8.docx, 10 Aug 2026):
        REPLACES test_deferred_count_is_0. TIN and RUBBER re-deferred after
        check_proxy_correlation.py's live 10 Aug 2026 run found their equity
        proxies correlate too weakly with their FRED Track 2 benchmark
        (+0.139/120mo and +0.229/120mo respectively) to trust as unbiased
        context anchors. CPO and NICKEL remain active (+0.405, +0.586)."""
        assert self.loader.deferred_count() == 2

    def test_deferred_instruments_are_tin_and_rubber(self):
        """
        FIX ADR-034 (GMI_Decision_Document_v8.docx, 10 Aug 2026):
        REPLACES test_no_deferred_instruments_remain. TIN and RUBBER are
        deferred again; CPO and NICKEL remain active with correlation
        caveats (proxy_for/proxy_correlation_expected now set on both).
        """
        deferred = {
            i.symbol: i for i in self.loader.all_context(include_deferred=True)
            if not i.context_available
        }
        assert set(deferred) == {"TIN", "RUBBER"}, (
            f"Expected exactly TIN and RUBBER deferred, got: {sorted(deferred)}"
        )
        for sym in ("TIN", "RUBBER"):
            inst = deferred[sym]
            assert inst.is_deferred is True
            assert inst.deferred_reason
            assert inst.planned_wave is not None

        for sym in ("CPO", "NICKEL"):
            inst = self.loader.get_context(sym)
            assert inst.context_available is True
            assert inst.is_deferred is False
            assert inst.proxy_for is not None
            assert inst.meta.get("proxy_correlation_expected") is not None
            assert inst.meta.get("requires_fx_normalization") is not True, (
                f"{sym} is now an equity proxy, not a raw currency-denominated "
                "commodity feed -- requires_fx_normalization must not be True"
            )
        assert self.loader.get_context("CPO").meta.get("proxy_correlation_expected") == 0.405
        assert self.loader.get_context("NICKEL").meta.get("proxy_correlation_expected") == 0.586

    def test_get_context_vix(self):
        inst = self.loader.get_context("VIX")
        assert inst.yfinance_symbol == "^VIX"
        assert inst.context_category == "context_volatility"
        assert inst.context_group == "equity"
        assert inst.layer == 2
        assert inst.reclassified_from == "layer_1_index"

    def test_get_context_dxy(self):
        inst = self.loader.get_context("DXY")
        assert inst.yfinance_symbol == "DX-Y.NYB"
        assert inst.context_category == "context_dollar"
        assert inst.context_group == "dollar"
        assert inst.reclassified_from == "layer_1_forex"

    def test_get_context_raises_keyerror_for_layer1_symbol(self):
        """AAPL is Layer 1 only — must not leak into Layer 2 lookup."""
        with pytest.raises(KeyError):
            self.loader.get_context("AAPL")

    def test_by_context_category_etf_sector(self):
        sector_etfs = self.loader.by_context_category("context_etf_sector")
        assert len(sector_etfs) == 10
        symbols = {i.symbol for i in sector_etfs}
        assert symbols == {
            "XLK", "XLF", "XLV", "XLY", "XLP",
            "XLI", "XLB", "XLU", "XLRE", "XLC",
        }

    def test_by_context_group_etf_total_25(self):
        """Architecture Extension v1.0 §2.3: 25 ETF context instruments total."""
        all_etf = self.loader.by_context_group("etf")
        assert len(all_etf) == 25

    def test_by_context_group_commodity_total_11(self):
        """Architecture Extension v1.0 §2.4: 11 commodity context instruments.
        FIX ADR-030-033 (30 Jul 2026): all 11 now operational (0 deferred) --
        was 8 operational + 3 deferred, then 7 + 4 after NICKEL's deferral.
        Total count is invariant to the active/deferred split either way."""
        all_commodity = self.loader.by_context_group("commodity")
        assert len(all_commodity) == 11

    def test_forecast_context_excludes_broad_sector_factor_etf(self):
        """
        ADR-002: context_etf_broad/sector/factor (17 ETFs) excluded from
        ForecastModule VAR input — multicollinearity with Layer 1 holdings.
        """
        fc = self.loader.forecast_context()
        fc_symbols = {i.symbol for i in fc}
        assert "SPY" not in fc_symbols     # broad
        assert "XLK" not in fc_symbols     # sector
        assert "SCHD" not in fc_symbols    # factor
        assert "HYG" in fc_symbols         # credit — included
        assert "EIDO" in fc_symbols        # international — included
        assert "ARKK" in fc_symbols        # thematic — included

    def test_forecast_context_cpo_nickel_active_tin_rubber_deferred(self):
        """FIX ADR-034 (GMI_Decision_Document_v8.docx, 10 Aug 2026):
        REPLACES test_forecast_context_now_includes_former_deferred. TIN and
        RUBBER are deferred again (excluded from forecast_context() via the
        context_available filter); CPO and NICKEL remain active and
        include_in_forecast=true, so they still appear."""
        fc_symbols = {i.symbol for i in self.loader.forecast_context()}
        assert {"CPO", "NICKEL"}.issubset(fc_symbols)
        assert "TIN" not in fc_symbols
        assert "RUBBER" not in fc_symbols

    def test_correlation_context_includes_deferred_excluded_instruments(self):
        """
        correlation_context() filters ONLY on context_available — broad/sector/
        factor ETFs (include_in_forecast=False) ARE included here, unlike
        forecast_context(). Architecture v2.0 §8.2 design constraint: PCA
        pre-processing applies ONLY to ForecastModule, not CorrelationModule.
        UPD ADR-034/036 (10 Aug 2026): 58, not 59 — TIN/RUBBER excluded
        (deferred), IDR included (ADR-036 moved it into Layer 2).
        """
        cc_symbols = {i.symbol for i in self.loader.correlation_context()}
        assert "SPY" in cc_symbols
        assert "XLK" in cc_symbols
        assert "TIN" not in cc_symbols
        assert "RUBBER" not in cc_symbols
        assert len(cc_symbols) == 58

    def test_subcategory_meta_dm_cb(self):
        """Data Source & Rates Adjustment v1.0 §6.1: 9 DM central banks via BIS."""
        meta = self.loader.subcategory_meta("context_rates_dm_cb")
        assert meta["source"] == "bis_cbpol_d"
        assert set(meta["central_banks"]) == {
            "ECB", "BOE", "BOJ", "BOC", "RBA", "RBNZ", "SNB", "NORGES", "RIKSBANK",
        }
        assert len(meta["central_banks"]) == 9

    def test_subcategory_meta_em_cb(self):
        meta = self.loader.subcategory_meta("context_rates_em_cb")
        assert set(meta["central_banks"]) == {"PBOC", "BOK", "BI"}
        assert len(meta["central_banks"]) == 3

    def test_subcategory_meta_fed_renamed(self):
        """
        Rates Adjustment v1.0 §5.1: context_rates_policy RENAMED to
        context_rates_fed. The old subcategory_id must not resolve.
        """
        assert self.loader.subcategory_meta("context_rates_policy") == {}
        fed_meta = self.loader.subcategory_meta("context_rates_fed")
        assert fed_meta["series"] == ["SOFR", "FEDFUNDS", "IORB", "EFFR"]

    def test_subcategory_meta_unknown_returns_empty_dict(self):
        assert self.loader.subcategory_meta("not_a_real_subcategory") == {}

    def test_all_subcategory_ids_count_20(self):
        """Rates Adjustment v1.0 §5.1: Layer 2 taxonomy is 18 -> 20 subcategories,
        extended to 22 by ADR-014 (context_dollar_basket) and ADR-024
        (context_fx_normalization)."""
        ids = self.loader.all_subcategory_ids()
        assert len(ids) == 22
        assert "context_rates_dm_cb" in ids
        assert "context_rates_em_cb" in ids
        assert "context_dollar_basket" in ids
        assert "context_fx_normalization" in ids
        assert "context_rates_fed" in ids
        assert "context_rates_policy" not in ids

    def test_reliability_flag_ssec(self):
        """Architecture v2.0 §3.5: Shanghai Composite requires reliability_flag."""
        ssec = self.loader.get_context("SSEC")
        assert ssec.reliability_flag is True
        assert ssec.exclude_from_lead_lag_leader is True

    def test_proxy_instruments_iron_ore_coal(self):
        """ADR-005/ADR-006: IRON_ORE proxies VALE, COAL_NEWC proxies WHC.AX."""
        iron_ore = self.loader.get_context("IRON_ORE")
        assert iron_ore.proxy_for == "IRON_ORE_SGX_FE62"
        assert iron_ore.proxy_instrument == "VALE"
        assert iron_ore.yfinance_symbol == "VALE"

        coal = self.loader.get_context("COAL_NEWC")
        assert coal.proxy_for == "NEWC_THERMAL_COAL"
        assert coal.proxy_instrument == "WHC.AX"

    def test_is_layer1_is_layer2_properties(self):
        l1_inst = self.loader.get("AAPL")
        l2_inst = self.loader.get_context("VIX")
        assert l1_inst.is_layer1 is True
        assert l1_inst.is_layer2 is False
        assert l2_inst.is_layer1 is False
        assert l2_inst.is_layer2 is True

    def test_is_deferred_property_false_for_active_instruments(self):
        """FIX ADR-034 (GMI_Decision_Document_v8.docx, 10 Aug 2026):
        REPLACES test_is_deferred_property_false_for_active_instruments'
        prior TIN example — TIN is deferred again as of this ADR (weak
        proxy correlation). NICKEL and COPPER remain always-active real
        instruments; the True branch (TIN, RUBBER) is now covered directly
        against live data by
        TestInstrumentLoaderLayer2.test_deferred_instruments_are_tin_and_rubber
        rather than only via synthetic fixtures."""
        nickel = self.loader.get_context("NICKEL")
        copper = self.loader.get_context("COPPER")
        assert nickel.is_deferred is False
        assert copper.is_deferred is False

    def test_context_instrument_is_frozen(self):
        inst = self.loader.get_context("VIX")
        with pytest.raises(Exception):
            inst.symbol = "CHANGED"


class TestGMIDecisionDocumentsV1V2:
    """
    NEW — GMI_Decision_Document_v1.docx (ADR-013/014/015/016/019) and
    GMI_Decision_Document_v2.docx (ADR-023/024). Both documents describe
    themselves as "DECIDED — Nothing implemented" at authorship time; this
    class is the first-ever test coverage for their actual implementation
    against instruments.yaml v1.5.
    """

    def setup_method(self):
        self.loader = get_loader()

    def test_dollar_basket_subcategory_has_seven_currencies(self):
        """ADR-014: context_dollar_basket completes Broad Dollar Index's
        10-pair input design (Architecture v2.0 §7.2) — 6 net-new EM/DM
        legs beyond the 6 (+ USD_IDR) that already existed in Layer 1.
        UPD ADR-036 (GMI_Decision_Document_v8.docx, 10 Aug 2026): IDR
        (was Layer 1 USD_IDR) joined this group too — 6 -> 7."""
        basket = self.loader.by_context_group("dollar_basket")
        assert {i.symbol for i in basket} == {
            "CNH", "KRW", "SGD", "HKD", "TWD", "NOK", "IDR"
        }
        assert all(i.layer == 2 for i in basket)
        assert all(i.context_available for i in basket)
        assert all(i.context_category == "context_dollar_basket" for i in basket)

    def test_idr_reclassified_from_layer1_forex(self):
        """ADR-036 (GMI_Decision_Document_v8.docx, 10 Aug 2026): USD_IDR ->
        IDR, same reclassified_from audit-trail pattern as DXY/SPX/VIX."""
        idr = self.loader.get_context("IDR")
        assert idr.yfinance_symbol == "USDIDR=X"
        assert idr.reclassified_from == "layer_1_forex"
        assert idr.context_category == "context_dollar_basket"
        with pytest.raises(KeyError):
            self.loader.get("USD_IDR")

    def test_dollar_basket_contributes_to_is_empty(self):
        """ADR-014: zero direct domain-score weight — this subcategory is a
        raw-data foundation for a future CrossAssetEngine
        compute_broad_dollar(), not a domain-score contributor itself.
        Prevents triple-counting DM/EM dollar strength across DXY + raw
        pairs + the future derived feature."""
        meta = self.loader.subcategory_meta("context_dollar_basket")
        assert meta["contributes_to"] == []

    def test_cnh_uses_offshore_ticker_not_onshore(self):
        """ADR-013: CNH (offshore), not CNY (onshore, PBOC-managed) — avoids
        double-counting PBOC policy stance via context_rates_em_cb AND a
        managed FX rate simultaneously."""
        cnh = self.loader.get_context("CNH")
        assert cnh.symbol == "CNH"
        assert cnh.yfinance_symbol == "USDCNH=X"
        assert cnh.include_in_forecast is True

    def test_hkd_pegged_currency_reliability_flag(self):
        """ADR-015: HKD included (not excluded) with reliability_flag=true —
        same pattern as SSEC (Architecture v2.0 §3.5) — so a future
        peg-break event is not structurally invisible, while still being
        flagged as reduced-reliability under normal (pegged) conditions."""
        hkd = self.loader.get_context("HKD")
        assert hkd.reliability_flag is True
        assert hkd.context_available is True
        assert hkd.include_in_forecast is True

    def test_sgd_included_and_documented(self):
        """ADR-016: SGD included on standalone FX-policy grounds (S$NEER
        band), not because of a pre-existing cross-taxonomy anchor."""
        sgd = self.loader.get_context("SGD")
        assert sgd.context_available is True
        notes = sgd.meta.get("notes", "")
        assert "FX-policy" in notes or "S$NEER" in notes

    def test_fx_normalization_subcategory_has_thb_only(self):
        """UPD ADR-037 (GMI_Decision_Document_v8.docx, 10 Aug 2026):
        REPLACES test_fx_normalization_subcategory_has_myr_only. MYR
        (ADR-024, for CPO's original FCPO plan) removed — orphaned since
        ADR-030 re-sourced CPO to F34.SI/SGD. THB added for a future
        STA.BK/RUBBER normalization step. Still a dedicated single-purpose
        subcategory, deliberately NOT folded into context_dollar_basket."""
        fx_norm = self.loader.by_context_group("fx_normalization")
        assert {i.symbol for i in fx_norm} == {"THB"}
        thb = fx_norm[0]
        assert thb.yfinance_symbol == "THB=X"
        assert thb.layer == 2
        assert thb.context_available is True

    def test_thb_excluded_from_forecast(self):
        """ADR-037: THB (like MYR before it) is a Silver-layer normalization
        input only — never a CrossAssetEngine/ForecastModule feature."""
        thb = self.loader.get_context("THB")
        assert thb.include_in_forecast is False

    def test_fx_normalization_contributes_to_is_empty(self):
        """ADR-024: zero domain-score weight, mirroring context_dollar_basket."""
        meta = self.loader.subcategory_meta("context_fx_normalization")
        assert meta["contributes_to"] == []

    def test_thb_ticker_matches_bare_currency_convention(self):
        """UPD ADR-037: THB=X follows the same bare <currency>=X convention
        MYR=X used (same pattern as JPY=X/HKD=X/SGD=X/IDR=X), distinct from
        the USD<CCY>=X convention used for context_dollar_basket."""
        thb = self.loader.get_context("THB")
        hkd = self.loader.get_context("HKD")
        assert thb.yfinance_symbol == "THB=X"
        assert hkd.yfinance_symbol == "USDHKD=X"

    def test_fx_normalization_does_not_duplicate_aud_cad_sgd(self):
        """ADR-037: AUD, CAD, SGD deliberately NOT added to
        context.fx_normalization — already reachable via Layer 1 forex
        (AUD_USD, USD_CAD) or context.dollar_basket (SGD, ADR-016)."""
        fx_norm_symbols = {i.symbol for i in self.loader.by_context_group("fx_normalization")}
        assert fx_norm_symbols == {"THB"}
        assert "AUD" not in fx_norm_symbols
        assert "CAD" not in fx_norm_symbols
        assert "SGD" not in fx_norm_symbols
        meta = self.loader.subcategory_meta("context_fx_normalization")
        assert "AUD" in meta.get("note", "") or "CAD" in meta.get("note", "")

    def test_adr023_history_superseded_by_adr030_034(self):
        """
        FIX ADR-030-033 (GMI_Decision_Document_v7.docx, 30 Jul 2026) +
        ADR-034 (GMI_Decision_Document_v8.docx, 10 Aug 2026):
        REPLACES test_adr023_only_cpo_is_myr_dependent. ADR-023's finding
        (only CPO was MYR-dependent; TIN/RUBBER were USD-native but ticker-
        blocked) was about the ORIGINAL raw-commodity-price sourcing plan
        (Bursa Malaysia FCPO, LME SN, SICOM TSR20 — all via tvdatafeed).
        tvdatafeed was retired entirely (ADR-029) before any of the three
        got live wiring, and all three (+ NICKEL) were re-sourced as
        yfinance equity proxies instead (ADR-030-033). ADR-034 then
        re-deferred TIN and RUBBER specifically on proxy-correlation
        grounds (a different, later reason than the original ticker/
        FX blockers) — CPO and NICKEL remain active. This test locks in
        the current, twice-superseded reality rather than deleting the
        history outright.
        """
        cpo = self.loader.get_context("CPO")
        nickel = self.loader.get_context("NICKEL")
        tin = self.loader.get_context("TIN")
        rubber = self.loader.get_context("RUBBER")

        for inst in (cpo, nickel):
            assert inst.context_available is True
            assert inst.is_deferred is False
            assert inst.meta.get("requires_fx_normalization") is not True, (
                f"{inst.symbol}: equity proxy, not a raw currency-denominated "
                "commodity feed — requires_fx_normalization must not be True"
            )

        for inst in (tin, rubber):
            assert inst.context_available is False
            assert inst.is_deferred is True
            assert inst.deferred_reason

        assert cpo.yfinance_symbol == "F34.SI"
        assert rubber.yfinance_symbol == "STA.BK"
        assert tin.yfinance_symbol == "AFM.V"
        assert nickel.yfinance_symbol == "NIC.AX"

    def test_all_subcategory_ids_includes_new_groups(self):
        ids = self.loader.all_subcategory_ids()
        assert len(ids) == 22
        assert "context_dollar_basket" in ids
        assert "context_fx_normalization" in ids


class TestInstrumentLoaderCoverageGaps:
    """NEW (2026-07-22) — targeted coverage for branches the real
    config/instruments_identity.yaml + instruments_taxonomy.yaml never
    exercise in practice (mis. index: is empty since ADR-003 reclassified
    SPX/VIX/DXY out of Layer 1), found while closing Gate G-6's coverage
    gap alongside the Decision B split (GMI_Decision_Document_v5.docx).
    Uses the constructor's identity_path/taxonomy_path override — added
    by the same split — to build small synthetic pairs, rather than
    touching the real config files."""

    def setup_method(self):
        self.loader = get_loader()

    def test_is_idx_is_forex_is_index_hive_key_properties(self):
        idx_inst = self.loader.by_market("idx")[0]
        assert idx_inst.is_idx is True
        assert idx_inst.is_forex is False

        fx_inst = self.loader.by_market("forex")[0]
        assert fx_inst.is_forex is True
        assert fx_inst.is_idx is False

        assert fx_inst.hive_key == fx_inst.symbol

    def test_get_with_market_filter_raises_when_market_not_present(self):
        with pytest.raises(KeyError, match="tidak ditemukan di market"):
            self.loader.get("AAPL", market="idx")

    def test_by_sector_filters_us_stocks_only(self):
        tech = self.loader.by_sector("Technology")
        assert len(tech) > 0
        assert all(i.is_us_stock and i.sector == "Technology" for i in tech)

    def test_symbol_list_with_market_filter(self):
        idx_symbols = self.loader.symbol_list(market="idx")
        assert "PTBA" in idx_symbols
        assert all(isinstance(s, str) for s in idx_symbols)

    def _write_pair(self, tmp_path, identity: dict, taxonomy: dict):
        id_path = tmp_path / "identity.yaml"
        tax_path = tmp_path / "taxonomy.yaml"
        id_path.write_text(yaml.dump(identity))
        tax_path.write_text(yaml.dump(taxonomy))
        return id_path, tax_path

    def _base_pair(self):
        base_identity = {
            "version": "1.0", "last_updated": "2026-01-01",
            "us_stocks": {}, "idx_stocks": {}, "commodity": {},
            "forex": {}, "index": [], "context": {},
        }
        base_taxonomy = {
            "version": "1.0", "last_updated": "2026-01-01",
            "us_stocks": {}, "idx_stocks": {}, "commodity": {},
            "forex": {}, "index": [], "context": {},
        }
        return base_identity, base_taxonomy

    def test_build_index_path_with_known_and_unknown_symbol(self, tmp_path):
        """index: list — empty in the real file post-ADR-003 (SPX/VIX
        reclassified to Layer 2), so _build_index() is dead code against
        real data. Covers both the YFINANCE_INDEX_MAP hit and the
        f'^{symbol}' fallback for an unmapped symbol."""
        identity, taxonomy = self._base_pair()
        identity["index"] = [
            {"symbol": "SPX"},          # in YFINANCE_INDEX_MAP -> ^GSPC
            {"symbol": "MADE_UP_IDX"},  # not mapped -> fallback ^MADE_UP_IDX
        ]
        id_path, tax_path = self._write_pair(tmp_path, identity, taxonomy)
        loader = InstrumentLoader(identity_path=id_path, taxonomy_path=tax_path)
        spx = loader.get("SPX")
        assert spx.yfinance_symbol == "^GSPC"
        assert spx.is_index is True
        fallback = loader.get("MADE_UP_IDX")
        assert fallback.yfinance_symbol == "^MADE_UP_IDX"

    def test_malformed_context_blocks_are_skipped_not_raised(self, tmp_path):
        """Defensive isinstance() guards in _load_layer2()/
        _load_subcategory_meta(): a malformed context block (wrong type
        where a dict is expected) must be skipped, not raise — YAML typos
        at this level should degrade to 'that subcategory contributed
        nothing' rather than crash the whole loader."""
        identity, taxonomy = self._base_pair()
        identity["context"] = {"dollar": "oops_a_string_not_a_dict"}
        taxonomy["context"] = {
            "dollar": "oops_a_string_not_a_dict",
            "equity": "also_not_a_dict",
            "commodity": {"metals": "still_not_a_dict"},
            "rates": {"fed": "not_a_dict_either"},
        }
        id_path, tax_path = self._write_pair(tmp_path, identity, taxonomy)
        # Must not raise despite the malformed blocks above.
        loader = InstrumentLoader(identity_path=id_path, taxonomy_path=tax_path)
        assert loader.all_context() == []
        assert loader.subcategory_meta("context_dollar") == {}

    def test_is_deferred_property_true_for_deferred_instrument(self, tmp_path):
        """FIX ADR-030-033 (GMI_Decision_Document_v7.docx, 30 Jul 2026):
        ADD -- companion to
        TestInstrumentLoaderLayer2.test_is_deferred_property_false_for_active_instruments.
        Real config has zero deferred Layer 2 instruments as of this thread
        (CPO/RUBBER/TIN/NICKEL all un-deferred), so Instrument.is_deferred's
        True branch is dead against live data. Synthetic pair constructs one
        deferred + one active Layer 2 instrument to keep both branches
        covered, matching this class's own stated purpose (targeted coverage
        for branches the real config doesn't currently exercise). Verified
        directly against InstrumentLoader before being written here (not
        assumed) -- see dev-log/2026-07-30-tvdatafeed-retirement-adr029-033.md.
        """
        identity, taxonomy = self._base_pair()
        identity["context"] = {
            "dollar": {"instruments": [{"symbol": "FAKE_DEFERRED"}, {"symbol": "FAKE_ACTIVE"}]},
        }
        taxonomy["context"] = {
            "dollar": {
                "_meta": {"contributes_to": []},
                "instruments": [
                    {
                        "symbol": "FAKE_DEFERRED", "layer": 2,
                        "context_category": "context_dollar", "context_group": "dollar",
                        "context_available": False,
                        "deferred_reason": "synthetic test fixture", "planned_wave": 9,
                    },
                    {
                        "symbol": "FAKE_ACTIVE", "layer": 2,
                        "context_category": "context_dollar", "context_group": "dollar",
                        "context_available": True,
                    },
                ],
            },
        }
        id_path, tax_path = self._write_pair(tmp_path, identity, taxonomy)
        loader = InstrumentLoader(identity_path=id_path, taxonomy_path=tax_path)
        deferred_inst = loader.get_context("FAKE_DEFERRED")
        active_inst = loader.get_context("FAKE_ACTIVE")
        assert deferred_inst.is_deferred is True
        assert active_inst.is_deferred is False
