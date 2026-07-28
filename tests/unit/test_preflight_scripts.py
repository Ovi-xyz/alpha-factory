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


class TestCheckTvdatafeedSymbols:
    """NEW -- these 2 scripts (this class + TestCheckBisEerWeights below)
    had zero test coverage since their own authoring thread; added now
    while already touching this file for the BIS endpoint / MXN->IDR
    fixes, matching the pattern established for the other 3 scripts."""

    def test_routing_table_has_4_entries(self):
        """OD-C1 scope: CPO, RUBBER, TIN (blocking) + COAL_NEWC (informational)."""
        import check_tvdatafeed_symbols as mod
        assert set(mod.ROUTING_TABLE) == {"CPO", "RUBBER", "TIN", "COAL_NEWC"}
        assert mod.INFORMATIONAL_ONLY == frozenset({"COAL_NEWC"})

    def test_main_returns_1_without_credentials(self, monkeypatch):
        import check_tvdatafeed_symbols as mod
        monkeypatch.delenv("TV_USERNAME", raising=False)
        monkeypatch.delenv("TV_PASSWORD", raising=False)
        monkeypatch.setattr(sys, "argv", ["check_tvdatafeed_symbols.py"])
        assert mod.main() == 1

    def test_main_returns_1_for_unknown_symbol_filter(self, monkeypatch):
        import check_tvdatafeed_symbols as mod
        monkeypatch.setenv("TV_USERNAME", "fake")
        monkeypatch.setenv("TV_PASSWORD", "fake")
        monkeypatch.setattr(sys, "argv", ["check_tvdatafeed_symbols.py", "--symbol", "NOT_REAL"])
        # TV_AVAILABLE gates before argument validation if tvDatafeed isn't
        # installed in this environment -- either a clean 1 (no routing
        # entry) or a clean 1 (package not installed) is an acceptable
        # outcome here; what matters is it never raises.
        assert mod.main() == 1

    def test_check_one_returns_false_for_empty_result(self):
        import check_tvdatafeed_symbols as mod
        client = MagicMock()
        client.get_hist.return_value = None
        ok, msg = mod._check_one("SN", "LME", client)
        assert ok is False
        assert "empty" in msg.lower()

    def test_check_one_returns_true_for_valid_ohlcv(self):
        import check_tvdatafeed_symbols as mod
        import pandas as pd
        df = pd.DataFrame({
            "open": [1.0], "high": [1.1], "low": [0.9],
            "close": [1.05], "volume": [1000],
        })
        client = MagicMock()
        client.get_hist.return_value = df
        ok, msg = mod._check_one("SN", "LME", client)
        assert ok is True


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

    def test_endpoint_uses_v2_path_structure(self):
        """Regression guard for the v1->v2 BIS API fix -- both the daily
        variant (WS_EER_D) and the old /api/v1/ shape are confirmed gone;
        WS_EER_M is the only remaining target."""
        import check_bis_eer_weights as mod
        assert "/api/v2/" in mod.BIS_EER_ENDPOINT_MONTHLY
        assert "WS_EER_M" in mod.BIS_EER_ENDPOINT_MONTHLY
        assert not hasattr(mod, "BIS_EER_ENDPOINT_DAILY")

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
