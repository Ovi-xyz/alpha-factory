"""
tests/unit/test_technical_signals_vix_path.py — GMI Wave 1 Cycle 3
Focused regression test for _get_latest_vix() — FIX GMI-GLD-001.

Dokumen referensi: GMI_Implementation_Checkpoint.docx (Task 9.1, item 5)
                   alpha_factory_architecture_extension_v1.docx ADR-003

Sebelum fix ini, _get_latest_vix() membaca dari
data/silver/market_ohlcv/index/ — path Layer 1 yang PERMANENTLY EMPTY
sejak ADR-003 mereklasifikasi VIX ke Layer 2 context (market='context').
Primary read selalu silently gagal (glob kosong -> DuckDB read_parquet
exception -> except: pass -> fallback FRED VIXCLS SELALU dipakai).

Tidak ada file test khusus untuk technical_signals.py sebelumnya
(diverifikasi kosong sebelum menulis file ini) — scope test ini SENGAJA
dibatasi hanya ke _get_latest_vix(), bukan seluruh modul gold_signals
(di luar scope Task 9.1).
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest


class TestGetLatestVixContextPath:

    def test_reads_from_context_path_not_index_path(self, tmp_path, monkeypatch):
        """
        GMI-GLD-001: writing Silver VIX data to the NEW Layer 2 path
        (market_ohlcv/context/) must be picked up by _get_latest_vix().
        Writing the SAME data to the OLD Layer 1 path (market_ohlcv/index/)
        must NOT be read — proves the glob was actually changed, not just
        made more permissive.
        """
        from src.gold.technical_signals import _get_latest_vix

        monkeypatch.chdir(tmp_path)

        base = date(2026, 6, 1)
        df = pl.DataFrame({
            "symbol":    ["VIX"] * 5,
            "timestamp": [base + timedelta(days=i) for i in range(5)],
            "close":     [15.0, 16.0, 17.0, 18.0, 42.5],   # last close = 42.5
            "is_clean":  [True] * 5,
        })

        context_dir = (
            tmp_path / "data" / "silver" / "market_ohlcv" / "context"
            / "symbol=VIX"
        )
        context_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(context_dir / "VIX_1D_silver.parquet")

        result = _get_latest_vix(base + timedelta(days=4))
        assert result == 42.5

    def test_old_index_path_alone_is_not_read(self, tmp_path, monkeypatch):
        """Data written ONLY to the old Layer 1 index/ path must not be
        found by the fixed glob — confirms the path was actually moved,
        not duplicated to read from both."""
        from src.gold.technical_signals import _get_latest_vix

        monkeypatch.chdir(tmp_path)

        base = date(2026, 6, 1)
        df = pl.DataFrame({
            "symbol":    ["VIX"] * 3,
            "timestamp": [base + timedelta(days=i) for i in range(3)],
            "close":     [15.0, 16.0, 17.0],
            "is_clean":  [True] * 3,
        })
        old_index_dir = (
            tmp_path / "data" / "silver" / "market_ohlcv" / "index" / "symbol=VIX"
        )
        old_index_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(old_index_dir / "VIX_1D_silver.parquet")

        # No FRED fallback data written either -> must return None, not 17.0
        result = _get_latest_vix(base + timedelta(days=2))
        assert result is None

    def test_falls_back_to_fred_when_context_data_absent(self, tmp_path, monkeypatch):
        """Fallback path (Silver macro FRED VIXCLS) must still work when
        Layer 2 context Silver data doesn't exist yet."""
        from src.gold.technical_signals import _get_latest_vix

        monkeypatch.chdir(tmp_path)

        base = date(2026, 6, 1)
        macro_df = pl.DataFrame({
            "series_id":        ["VIXCLS"] * 3,
            "observation_date": [base + timedelta(days=i) for i in range(3)],
            "value":            [14.0, 14.5, 15.25],
        })
        macro_dir = tmp_path / "data" / "silver" / "macro_enriched"
        macro_dir.mkdir(parents=True, exist_ok=True)
        macro_df.write_parquet(macro_dir / "fred_vix_silver.parquet")

        result = _get_latest_vix(base + timedelta(days=2))
        assert result == 15.25

    def test_returns_none_when_no_data_anywhere(self, tmp_path, monkeypatch):
        from src.gold.technical_signals import _get_latest_vix

        monkeypatch.chdir(tmp_path)
        result = _get_latest_vix(date(2026, 6, 1))
        assert result is None
