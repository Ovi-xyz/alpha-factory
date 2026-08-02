"""
tests/unit/test_preflight_scripts.py

NEW -- ADR-025 (GMI_Decision_Document_v2.docx, 2026-07-11). Covers the
network-INDEPENDENT logic in scripts/preflight/*.py -- date-math, response-
shape checks against mocked clients, missing-prerequisite handling. Does
NOT attempt to test actual live network calls, consistent with ADR-025's
own framing: these scripts are authored now and executed later, on
network-enabled hardware/CI, not from this sandbox.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_PREFLIGHT_DIR = Path(__file__).resolve().parents[2] / "scripts" / "preflight"
sys.path.insert(0, str(_PREFLIGHT_DIR))


class TestCheckBisCbpolD:

    def test_daily_resolution_true_for_daily_dates(self):
        import check_bis_cbpol_d as mod
        dates = [date(2026, 1, 1) + __import__("datetime").timedelta(days=i) for i in range(10)]
        assert mod._daily_resolution(dates) is True

    def test_daily_resolution_false_for_monthly_dates(self):
        import check_bis_cbpol_d as mod
        dates = [date(2026, m, 1) for m in range(1, 7)]
        assert mod._daily_resolution(dates) is False

    def test_daily_resolution_false_for_single_observation(self):
        import check_bis_cbpol_d as mod
        assert mod._daily_resolution([date(2026, 1, 1)]) is False

    def test_expected_ref_areas_has_12_entries(self):
        """Locks in the 12-REF_AREA universe this script checks against —
        Data Source & Rates Adjustment v1.0 §3.1."""
        import check_bis_cbpol_d as mod
        assert len(mod.EXPECTED_REF_AREAS) == 12
        assert mod.EXPECTED_REF_AREAS["ID"] == "BI"
        assert mod.EXPECTED_REF_AREAS["XM"] == "ECB"

    def test_main_returns_1_on_fetch_failure(self, monkeypatch):
        import check_bis_cbpol_d as mod

        def raise_error(*a, **kw):
            raise ConnectionError("simulated: no network access to stats.bis.org")

        monkeypatch.setattr(mod, "_fetch_csv", raise_error)
        assert mod.main() == 1

    def test_endpoint_uses_correct_dataflow_id(self):
        """Regression guard for FIX BIS-1 (1 Aug 2026): the dataflow is
        WS_CBPOL, not WS_CBPOL_D -- confirmed against data.bis.org's own
        indexed URLs (BIS,WS_CBPOL,1.0) across 8 countries, and a live
        third-party code example for the sibling WS_CBTA dataflow. A
        prior thread (28 Jul) claimed WS_CBPOL_D was independently
        confirmed correct; that claim was never actually verified and the
        29 Jul live preflight run still 404'd, disproving it."""
        import check_bis_cbpol_d as mod
        assert "/data/dataflow/BIS/WS_CBPOL/1.0/" in mod.BIS_ENDPOINT
        assert "WS_CBPOL_D" not in mod.BIS_ENDPOINT

    def test_endpoint_key_wildcards_freq_and_includes_all_ref_areas(self):
        """Key must wildcard FREQ (leading empty segment) rather than
        hardcode a frequency -- sampled countries show a MIX of Monthly
        and Daily base cadence on BIS, and 4 of our 12 (GB/CH/NO/JP) came
        back Monthly, not Daily, in the sample. "all" as a literal key
        segment (the previous value) is not valid SDMX key syntax."""
        import check_bis_cbpol_d as mod
        key = mod.BIS_ENDPOINT.rsplit("/", 1)[-1]
        assert key.startswith(".")
        assert "all" not in key.split(".")
        for ref_area in mod.EXPECTED_REF_AREAS:
            assert ref_area in key


class TestCheckFinnhubShape:

    def test_check_quote_detects_missing_fields(self):
        import check_finnhub_shape as mod
        client = MagicMock()
        client.quote.return_value = {"c": 1.0, "d": 0.1}  # missing most fields
        ok, msg = mod._check_quote(client, "AAPL")
        assert ok is False
        assert "missing expected fields" in msg

    def test_check_quote_passes_with_all_fields(self):
        import check_finnhub_shape as mod
        client = MagicMock()
        client.quote.return_value = {
            "c": 152.3, "d": 1.2, "dp": 0.8, "h": 155.0, "l": 149.0,
            "o": 150.0, "pc": 151.1, "t": 1751500000,
        }
        ok, msg = mod._check_quote(client, "AAPL")
        assert ok is True

    def test_check_quote_flags_all_zero_response(self):
        """Documented Finnhub quirk (Checkpoint v3 §4.6): invalid/delisted
        symbols return all-zero numeric fields, not a missing-key error."""
        import check_finnhub_shape as mod
        client = MagicMock()
        client.quote.return_value = {
            "c": 0, "d": 0, "dp": 0, "h": 0, "l": 0, "o": 0, "pc": 0, "t": 0,
        }
        ok, msg = mod._check_quote(client, "INVALIDTICKER")
        assert ok is True  # shape is still correct, just flagged
        assert "all-zero" in msg

    def test_check_earnings_no_upcoming_is_not_a_failure(self):
        import check_finnhub_shape as mod
        client = MagicMock()
        client.earnings_calendar.return_value = {"earningsCalendar": []}
        ok, msg = mod._check_earnings(client, "AAPL")
        assert ok is True
        assert "no upcoming earnings" in msg

    def test_main_returns_1_without_api_key(self, monkeypatch):
        import check_finnhub_shape as mod
        monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
        monkeypatch.setattr(sys, "argv", ["check_finnhub_shape.py"])
        assert mod.main() == 1


class TestCheckYfinanceTickers:

    def test_gate_2_unconfirmed_symbols_matches_decision_docs(self):
        """Locks in the exact symbol set both decision documents flag as
        live-unconfirmed (Gate 2) -- CNH and MYR are the two confirmed
        exceptions, per ADR-013/ADR-024 respectively."""
        import check_yfinance_tickers as mod
        assert mod.GATE_2_UNCONFIRMED_SYMBOLS == {"KRW", "SGD", "HKD", "TWD", "NOK"}
        assert "CNH" not in mod.GATE_2_UNCONFIRMED_SYMBOLS
        assert "MYR" not in mod.GATE_2_UNCONFIRMED_SYMBOLS

    def test_check_one_returns_false_for_empty_dataframe(self):
        import check_yfinance_tickers as mod
        import pandas as pd

        with patch("yfinance.download", return_value=pd.DataFrame()):
            ok, msg = mod._check_one("FAKE", "FAKE=X")
        assert ok is False
        assert "empty" in msg.lower()

    def test_check_one_returns_true_for_valid_ohlcv(self):
        import check_yfinance_tickers as mod
        import pandas as pd

        df = pd.DataFrame({
            "Open": [1.0], "High": [1.1], "Low": [0.9],
            "Close": [1.05], "Volume": [1000],
        })
        with patch("yfinance.download", return_value=df):
            ok, msg = mod._check_one("MYR", "MYR=X")
        assert ok is True

    def test_main_returns_1_for_unknown_symbol_filter(self, monkeypatch):
        import check_yfinance_tickers as mod
        monkeypatch.setattr(sys, "argv", ["check_yfinance_tickers.py", "--symbol", "NOT_A_REAL_SYMBOL"])
        assert mod.main() == 1

    def test_candidate_proxy_tickers_has_4_entries(self):
        """Locks in the 4 not-yet-decided proxy candidates researched to
        replace tvdatafeed for CPO/RUBBER/TIN/NICKEL (30 Jul 2026 thread)."""
        import check_yfinance_tickers as mod
        assert mod.CANDIDATE_PROXY_TICKERS == {
            "CPO": "F34.SI", "RUBBER": "STA.BK", "TIN": "AFM.V", "NICKEL": "NIC.AX",
        }

    def test_candidates_flag_independent_of_instrument_loader(self, monkeypatch):
        """--candidates must never touch InstrumentLoader/config -- that
        independence is the entire point (verify before deciding, not
        after). A broken/missing config must not block this check."""
        import check_yfinance_tickers as mod
        import pandas as pd

        def _boom(*a, **k):
            raise AssertionError("get_loader() must not be called in --candidates mode")

        monkeypatch.setattr("src.config.instrument_loader.get_loader", _boom)
        df = pd.DataFrame({
            "Open": [1.0], "High": [1.1], "Low": [0.9], "Close": [1.05], "Volume": [1000],
        })
        with patch("yfinance.download", return_value=df):
            monkeypatch.setattr(sys, "argv", ["check_yfinance_tickers.py", "--candidates"])
            assert mod.main() == 0

    def test_candidates_flag_returns_1_if_any_candidate_fails(self):
        import check_yfinance_tickers as mod
        import pandas as pd

        good = pd.DataFrame({
            "Open": [1.0], "High": [1.1], "Low": [0.9], "Close": [1.05], "Volume": [1000],
        })

        def _fake_download(symbol, **kwargs):
            return pd.DataFrame() if symbol == "AFM.V" else good

        with patch("yfinance.download", side_effect=_fake_download):
            assert mod._check_candidates() == 1


# REMOVED FIX ADR-029 (GMI_Decision_Document_v7.docx, 30 Jul 2026):
# TestCheckTvdatafeedSymbols deleted -- tvdatafeed retired entirely,
# scripts/preflight/check_tvdatafeed_symbols.py archived to
# scripts/archive/check_tvdatafeed_symbols.py (no longer imported/tested).
# See KNOWN_RISKS.md RISK-1 (RESOLVED).


class TestCheckBisEerWeights:

    def test_mxn_removed_idr_added(self):
        """UPD (Ovi, this thread): MXN was a stale Architecture v2.0 §7.2
        placeholder with zero relevance to this Indonesia-focused
        platform and zero other occurrences anywhere in the repo. IDR is
        the economically relevant EM currency here (Layer 1 forex pair
        USD_IDR, ADR-018 basket-weight override) -- locks in the fix so
        it can't silently regress."""
        import check_bis_eer_weights as mod
        assert "MXN" not in mod.BROAD_DOLLAR_REF_AREAS
        assert mod.BROAD_DOLLAR_REF_AREAS["IDR"] == "ID"

    def test_hkd_twd_nok_completes_dollar_basket(self):
        """Ovi (this thread): HKD/TWD/NOK were explicitly flagged as a
        known gap on 28 Jul ("Ovi's instruction was specifically
        MXN->IDR") rather than guessed at, then explicitly requested this
        thread. Locks in all 13 currencies of the *current*
        context_dollar_basket design (instruments_taxonomy.yaml) so the
        gap can't silently reopen."""
        import check_bis_eer_weights as mod
        assert mod.BROAD_DOLLAR_REF_AREAS["HKD"] == "HK"
        assert mod.BROAD_DOLLAR_REF_AREAS["TWD"] == "TW"
        assert mod.BROAD_DOLLAR_REF_AREAS["NOK"] == "NO"
        assert len(mod.BROAD_DOLLAR_REF_AREAS) == 13

    def test_endpoint_uses_correct_dataflow_id(self):
        """Regression guard for the dataflow-ID root-cause fix (FIX BIS-1,
        1 Aug 2026): the flow is WS_EER, not WS_EER_M. The "_M" was a
        monthly-cadence label mistaken for part of the dataflow
        identifier -- the previous "v1->v2 path fix" (28 Jul) corrected
        the URL shape but kept this wrong name, which is why it still
        404'd on the 29 Jul live run. Confirmed against data.bis.org's own
        indexed URLs (BIS,WS_EER,1.0) across 7 countries."""
        import check_bis_eer_weights as mod
        assert "/api/v2/" in mod.BIS_EER_ENDPOINT_MONTHLY
        assert "/data/dataflow/BIS/WS_EER/1.0/" in mod.BIS_EER_ENDPOINT_MONTHLY
        assert "WS_EER_M" not in mod.BIS_EER_ENDPOINT_MONTHLY
        assert not hasattr(mod, "BIS_EER_ENDPOINT_DAILY")

    def test_structure_endpoint_uses_structure_prefix(self):
        """FIX BIS-1: the --discover endpoint was missing the "structure/"
        path segment entirely (/api/v2/dataflow/... instead of
        /api/v2/structure/dataflow/...), which is why it 501'd rather
        than 404'd -- a malformed v2 path, not a clean not-found. Confirmed
        against a real SDMX 2025 conference paper's worked example for a
        sibling BIS dataflow (WS_XRU) using this exact structure/dataflow
        shape."""
        import check_bis_eer_weights as mod
        assert "/api/v2/structure/dataflow/BIS/WS_EER/1.0" in mod.BIS_EER_DATAFLOW_STRUCTURE_URL

    def test_key_wildcards_freq_and_fixes_broad_basket(self):
        """The key must wildcard FREQ (BIS only publishes EER monthly --
        no daily variant found in any sampled country) and fix
        BASKET=B (broad), matching "Broad Dollar Index" directly. TYPE is
        deliberately left wildcarded -- Real vs Nominal is an unresolved
        design question, not something to bake into this endpoint fix."""
        import check_bis_eer_weights as mod
        key = mod.BIS_EER_ENDPOINT_MONTHLY.rsplit("/", 1)[-1]
        assert key.startswith("M..B.")
        for ref_area in mod.BROAD_DOLLAR_REF_AREAS.values():
            assert ref_area in key

    def test_main_returns_1_for_unknown_currency_filter(self, monkeypatch):
        import check_bis_eer_weights as mod
        monkeypatch.setattr(sys, "argv", ["check_bis_eer_weights.py", "--currency", "NOT_REAL"])
        assert mod.main() == 1

    def test_check_one_returns_false_for_missing_ref_area(self, monkeypatch):
        import check_bis_eer_weights as mod
        monkeypatch.setattr(mod, "_fetch_csv", lambda url: "some,other,data\n1,2,3\n")
        ok, msg = mod._check_one("ID")
        assert ok is False

    def test_check_one_returns_true_when_ref_area_present(self, monkeypatch):
        import check_bis_eer_weights as mod
        monkeypatch.setattr(mod, "_fetch_csv", lambda url: "REF_AREA,TIME_PERIOD,OBS_VALUE\nID,2026-01-01,101.2\n")
        ok, msg = mod._check_one("ID")
        assert ok is True
