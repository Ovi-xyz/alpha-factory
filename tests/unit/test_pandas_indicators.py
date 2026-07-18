"""
tests/unit/test_pandas_indicators.py — add_adx() / add_bbands() unit tests

FIX GLD-ADX-001: this file did not exist before that fix. Its absence is
exactly why a P0 bug (add_adx() crashing on every real invocation due to
an ADX_14/ADXR_14_2 column-name collision — see pandas_indicators.py
module docstring on add_adx for the full root cause) went undetected
through every prior audit cycle. Tests here deliberately do NOT mock
pandas_ta_classic: the whole point is to exercise the REAL library's REAL
output shape, since that shape (not our assumptions about it) is what
caused the bug in the first place.

UPD ADR-020 (GMI_Decision_Document_v2.docx, 2026-07-11): migrated from
pandas-ta to pandas-ta-classic. pandas-ta-classic's ta.adx() was verified
to emit only THREE columns (no ADXR) — the original collision cannot occur
in this fork. test_wrapper_adx_matches_raw_adx_column and
test_pandas_ta_classic_does_not_emit_adxr_column replace the old
ADXR-collision-focused test accordingly (see their docstrings).
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from src.gold.indicators.pandas_indicators import add_adx, add_bbands


def _sample_df(symbols: list[str], n: int = 30) -> pl.DataFrame:
    base = date(2026, 5, 1)
    frames = []
    for sym in symbols:
        frames.append(pl.DataFrame({
            "symbol":    [sym] * n,
            "timestamp": [base + timedelta(days=d) for d in range(n)],
            "open":      [150.0 + d for d in range(n)],
            "high":      [155.0 + d for d in range(n)],
            "low":       [145.0 + d for d in range(n)],
            "close":     [152.0 + d for d in range(n)],
        }))
    return pl.concat(frames)


class TestAddAdx:
    def test_produces_expected_columns(self):
        df = add_adx(_sample_df(["AAPL"]))
        for col in ["adx", "di_plus", "di_minus"]:
            assert col in df.columns

    def test_no_duplicate_columns_in_output(self):
        """FIX GLD-ADX-001 direct regression guard: the bug's exact
        symptom was a duplicate 'adx' column name reaching
        pl.from_pandas(), which raised before this fix. If the collision
        ever recurs (e.g. a future pandas-ta version adds yet another
        ADX-prefixed column), this assertion catches it structurally
        rather than relying on the crash to surface it."""
        df = add_adx(_sample_df(["AAPL"]))
        assert len(df.columns) == len(set(df.columns)), (
            f"Duplicate columns in add_adx() output: {df.columns}"
        )

    def test_wrapper_adx_matches_raw_adx_column(self):
        """Confirms our wrapper's 'adx' column is sourced from ta.adx()'s
        ADX_{length} column, not silently swapped for some other column.

        UPD ADR-020: replaces the pre-migration
        test_adxr_is_not_silently_used_as_adx, which asserted that
        pandas-ta's ta.adx() output included an 'ADXR_14_2' column — true
        under the abandoned pandas-ta 0.4.x line, but pandas-ta-classic's
        ta.adx() does not emit one (see
        test_pandas_ta_classic_does_not_emit_adxr_column below), so that
        assumption no longer holds and would fail here. Basic correctness
        — that 'adx' really is ADX and not something else — is still worth
        guarding permanently, which is what this test does now.
        """
        import pandas as pd
        import pandas_ta_classic as ta

        df = _sample_df(["AAPL"])
        pd_df = df.to_pandas().sort_values("timestamp")
        raw = ta.adx(pd_df["high"], pd_df["low"], pd_df["close"], length=14)
        assert "ADX_14" in raw.columns, (
            "Test assumption about installed pandas-ta-classic's column "
            "shape no longer holds — re-verify ADR-020's migration is still "
            "correct for the currently installed pandas-ta-classic version."
        )
        adx_series = raw["ADX_14"].reset_index(drop=True)

        result = add_adx(df)
        wrapper_adx = result["adx"].to_pandas().reset_index(drop=True)
        # Compare on overlapping non-null tail (pandas-ta-classic warms up
        # with NaNs for the first `length` rows).
        valid = wrapper_adx.notna()
        pd.testing.assert_series_equal(
            wrapper_adx[valid].reset_index(drop=True),
            adx_series[valid].reset_index(drop=True),
            check_names=False,
        )

    def test_pandas_ta_classic_does_not_emit_adxr_column(self):
        """Documents, as a live check rather than only a comment, that the
        ADX_14/ADXR_14_2 collision behind FIX GLD-ADX-001 does not exist in
        pandas-ta-classic (ADR-020). If this ever starts failing (e.g. a
        future pandas-ta-classic release reintroduces an ADXR-like column),
        the startswith("adx_") guard in add_adx() is what prevents the
        original P0 duplicate-column crash from recurring — see
        test_wrapper_adx_matches_raw_adx_column and the add_adx() module
        docstring.
        """
        import pandas_ta_classic as ta

        df = _sample_df(["AAPL"])
        pd_df = df.to_pandas().sort_values("timestamp")
        raw = ta.adx(pd_df["high"], pd_df["low"], pd_df["close"], length=14)
        adxr_cols = [c for c in raw.columns if c.lower().startswith("adxr")]
        assert adxr_cols == [], (
            f"pandas-ta-classic now emits an ADXR-like column {adxr_cols} — "
            "the startswith('adx_') guard in add_adx() should already "
            "exclude it, but re-run test_no_duplicate_columns_in_output and "
            "test_wrapper_adx_matches_raw_adx_column to confirm."
        )

    def test_multiple_symbols_processed_independently(self):
        df = add_adx(_sample_df(["AAPL", "MSFT"]))
        assert set(df["symbol"].unique().to_list()) == {"AAPL", "MSFT"}
        assert len(df) == 60

    def test_graceful_fallback_when_pandas_ta_classic_not_installed(self, monkeypatch):
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pandas_ta_classic":
                raise ImportError("simulated missing pandas_ta_classic")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        df = add_adx(_sample_df(["AAPL"]))
        assert df["adx"].is_null().all()
        assert df["di_plus"].is_null().all()
        assert df["di_minus"].is_null().all()


class TestAddBbands:
    def test_produces_expected_columns(self):
        df = add_bbands(_sample_df(["AAPL"]))
        for col in ["bbands_upper", "bbands_mid", "bbands_lower"]:
            assert col in df.columns

    def test_no_duplicate_columns_in_output(self):
        """Same structural guard as ADX's — audited during the GLD-ADX-001
        investigation and confirmed clean (BBB_/BBP_ columns match none of
        the upper/mid/lower patterns), but a permanent test costs little
        and catches any future pandas-ta column-naming change immediately."""
        df = add_bbands(_sample_df(["AAPL"]))
        assert len(df.columns) == len(set(df.columns))

    def test_upper_band_above_lower_band(self):
        df = add_bbands(_sample_df(["AAPL"])).drop_nulls(
            subset=["bbands_upper", "bbands_lower"]
        )
        assert (df["bbands_upper"] >= df["bbands_lower"]).all()

    def test_multiple_symbols_processed_independently(self):
        df = add_bbands(_sample_df(["AAPL", "MSFT"]))
        assert set(df["symbol"].unique().to_list()) == {"AAPL", "MSFT"}
        assert len(df) == 60

    def test_graceful_fallback_when_pandas_ta_classic_not_installed(self, monkeypatch):
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pandas_ta_classic":
                raise ImportError("simulated missing pandas_ta_classic")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        df = add_bbands(_sample_df(["AAPL"]))
        assert df["bbands_upper"].is_null().all()
        assert df["bbands_mid"].is_null().all()
        assert df["bbands_lower"].is_null().all()
