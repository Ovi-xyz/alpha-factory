"""
tests/unit/test_base_ingester.py — BronzeIngester.write()/write_macro()
idempotency-skip coverage. Coverage tranche (17 Aug 2026) — previously zero
test coverage for this module (only exercised indirectly via concrete
ingester subclasses' happy paths, which never triggered the same-day skip).

FIX GMI-BI-DATE-01 (Ovi, 7 Sep 2026): added TestWriteMacroDateOwnership —
regression guard for the WIB/UTC day-boundary bug. write_macro() gained a
required run_date parameter; all existing write_macro() calls below updated
accordingly (date(2026, 9, 6) picked arbitrarily — no calendar meaning).
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from src.bronze.base_ingester import BronzeIngester

_RUN_DATE = date(2026, 9, 6)


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(BronzeIngester, "BASE_PATH", tmp_path / "bronze")
    return tmp_path


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({"close": [100.0, 101.0]})


def _fixed_utcnow(module, monkeypatch, fixed: datetime) -> None:
    """Patch module.datetime.utcnow() to always return `fixed`, leaving the
    real datetime class (and date construction) otherwise untouched."""

    class _FixedDatetime(datetime):
        @classmethod
        def utcnow(cls):
            return fixed

    monkeypatch.setattr(module, "datetime", _FixedDatetime)


class TestWriteIdempotency:
    def test_first_write_returns_path(self, sample_df):
        result = BronzeIngester().write(
            sample_df, source="yfinance", asset_class="market/ohlcv/us_stocks", symbol="AAPL"
        )
        assert result is not None
        assert result.exists()

    def test_second_write_same_day_skips(self, sample_df):
        ingester = BronzeIngester()
        first = ingester.write(
            sample_df, source="yfinance", asset_class="market/ohlcv/us_stocks", symbol="AAPL"
        )
        second = ingester.write(
            sample_df, source="yfinance", asset_class="market/ohlcv/us_stocks", symbol="AAPL"
        )
        assert first is not None
        assert second is None   # FIX GD-F08: idempotent skip

    def test_extra_metadata_columns_added(self, sample_df):
        result = BronzeIngester().write(
            sample_df, source="yfinance", asset_class="market/ohlcv/us_stocks",
            symbol="AAPL", extra_metadata={"_tz_hint": "America/New_York"},
        )
        written = pl.read_parquet(result)
        assert written["_tz_hint"].to_list() == ["America/New_York", "America/New_York"]


class TestWriteMacroIdempotency:
    def test_first_write_macro_returns_path(self, sample_df):
        result = BronzeIngester().write_macro(
            sample_df, source="fred", domain="monetary_policy", series_id="FEDFUNDS",
            run_date=_RUN_DATE,
        )
        assert result is not None
        assert result.exists()

    def test_second_write_macro_same_day_skips(self, sample_df):
        ingester = BronzeIngester()
        first = ingester.write_macro(
            sample_df, source="fred", domain="monetary_policy", series_id="FEDFUNDS",
            run_date=_RUN_DATE,
        )
        second = ingester.write_macro(
            sample_df, source="fred", domain="monetary_policy", series_id="FEDFUNDS",
            run_date=_RUN_DATE,
        )
        assert first is not None
        assert second is None   # FIX BI-1: idempotent skip

    def test_different_series_id_not_skipped(self, sample_df):
        ingester = BronzeIngester()
        ingester.write_macro(
            sample_df, source="fred", domain="monetary_policy", series_id="FEDFUNDS",
            run_date=_RUN_DATE,
        )
        other = ingester.write_macro(
            sample_df, source="fred", domain="monetary_policy", series_id="DGS10",
            run_date=_RUN_DATE,
        )
        assert other is not None


class TestWriteMacroDateOwnership:
    """FIX GMI-BI-DATE-01: date_prefix/filename must be keyed off run_date,
    never datetime.utcnow(). Live-repo evidence: files created 03:06/04:11
    WIB (created-time confirmed via filesystem metadata) were named one
    calendar day earlier because utcnow() was still on the prior UTC day —
    e.g. VIXCLS_20260830_200632.parquet actually created Mon Aug 31 03:06
    WIB. These tests reproduce that exact day-boundary shape with a fixed,
    mocked utcnow() rather than relying on wall-clock timing at test time."""

    def test_filename_date_uses_run_date_not_utcnow(self, sample_df, monkeypatch):
        import src.bronze.base_ingester as bi

        # utcnow() lands on the day BEFORE run_date — the exact WIB early-
        # morning shape observed live (04:xx WIB == previous day, late UTC).
        _fixed_utcnow(bi, monkeypatch, datetime(2026, 9, 5, 20, 30, 0))

        result = BronzeIngester().write_macro(
            sample_df, source="fred", domain="volatility", series_id="VIXCLS",
            run_date=date(2026, 9, 6),
        )
        assert result is not None
        assert result.name.startswith("VIXCLS_20260906_")   # run_date, not utcnow's 20260905

    def test_idempotency_keyed_by_run_date_survives_utcnow_day_rollover(
        self, sample_df, monkeypatch
    ):
        import src.bronze.base_ingester as bi

        ingester = BronzeIngester()

        _fixed_utcnow(bi, monkeypatch, datetime(2026, 9, 5, 23, 59, 0))
        first = ingester.write_macro(
            sample_df, source="fred", domain="volatility", series_id="VIXCLS",
            run_date=date(2026, 9, 6),
        )

        # Wall clock rolls into the next UTC day between calls; run_date is
        # unchanged (same pipeline run_date). Must still be an idempotent skip.
        _fixed_utcnow(bi, monkeypatch, datetime(2026, 9, 6, 0, 5, 0))
        second = ingester.write_macro(
            sample_df, source="fred", domain="volatility", series_id="VIXCLS",
            run_date=date(2026, 9, 6),
        )

        assert first is not None
        assert second is None

    def test_different_run_date_not_skipped(self, sample_df, monkeypatch):
        """Sanity check: the idempotency key is run_date, so genuinely
        different run_dates (e.g. --date backfill) must NOT collide."""
        import src.bronze.base_ingester as bi

        _fixed_utcnow(bi, monkeypatch, datetime(2026, 9, 6, 4, 0, 0))
        ingester = BronzeIngester()
        first = ingester.write_macro(
            sample_df, source="fred", domain="volatility", series_id="VIXCLS",
            run_date=date(2026, 9, 5),
        )
        second = ingester.write_macro(
            sample_df, source="fred", domain="volatility", series_id="VIXCLS",
            run_date=date(2026, 9, 6),
        )
        assert first is not None
        assert second is not None
