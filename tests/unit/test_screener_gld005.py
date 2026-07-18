"""
tests/unit/test_screener_gld005.py — Test suite untuk GLD-005 fix.

FIX GLD-005: TOTAL_INSTRUMENTS hardcoded 643 di screener._check_data_freshness().
Audit finding: hardcode 643 menyebabkan coverage gate salah saat universe
diperluas ke 692 (GMI Architecture Extension, Layer 2 reclassification).

Post-fix: TOTAL_INSTRUMENTS = get_loader().count() — dinamis dari instruments.yaml.

Implikasi kritis:
  - 692-instrument universe: threshold = 0.95 × 692 = 657.4 → 657 symbols minimum
  - 643-instrument universe: threshold = 0.95 × 643 = 610.85 → 610 symbols minimum
  - Jika hardcoded 643 saat universe = 692: screener bisa jalan dengan hanya 610
    symbols padahal 49 symbols dari Layer 2 universe tidak ter-cover (false OK).
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest


class TestTotalInstrumentsDynamic:
    """GLD-005: TOTAL_INSTRUMENTS harus dari get_loader().count(), bukan 643."""

    def test_screener_uses_loader_count_not_hardcoded(self):
        """
        Verifikasi source code screener.py tidak mengandung hardcoded 643.
        Jika 643 ditemukan di konteks TOTAL_INSTRUMENTS, fix belum diapply.
        """
        import pathlib, re
        screener_path = pathlib.Path("src/gold/screener.py")
        if not screener_path.exists():
            pytest.skip("screener.py tidak ditemukan")

        src = screener_path.read_text(encoding="utf-8")

        # Cari pattern: TOTAL_INSTRUMENTS = 643 (hardcode lama)
        hardcode_pattern = re.compile(r"TOTAL_INSTRUMENTS\s*=\s*643\b")
        assert not hardcode_pattern.search(src), (
            "GLD-005 REGRESSION: TOTAL_INSTRUMENTS masih hardcoded 643 di screener.py. "
            "Harus: TOTAL_INSTRUMENTS = get_loader().count()"
        )

    def test_screener_imports_get_loader(self):
        """screener.py harus import get_loader dari instrument_loader."""
        import pathlib
        screener_path = pathlib.Path("src/gold/screener.py")
        if not screener_path.exists():
            pytest.skip("screener.py tidak ditemukan")

        src = screener_path.read_text(encoding="utf-8")
        assert "get_loader" in src, (
            "GLD-005: screener.py harus import dan menggunakan get_loader() "
            "untuk mendapatkan TOTAL_INSTRUMENTS yang dinamis"
        )

    def test_total_instruments_reflects_loader_count(self):
        """
        Dengan mock loader.count() = 692 (GMI extended universe),
        coverage gate harus menggunakan 692, bukan 643.
        """
        from src.gold import screener as scr_module

        # Mock get_loader untuk return 692 (extended universe)
        mock_loader = MagicMock()
        mock_loader.count.return_value = 692

        called_with_692 = []

        original_fn = scr_module._check_data_freshness

        def capture_total(*args, **kwargs):
            # Baca nilai TOTAL_INSTRUMENTS yang akan dipakai
            with patch("src.gold.screener.get_loader", return_value=mock_loader):
                try:
                    return original_fn(*args, **kwargs)
                except Exception:
                    pass

        with patch("src.gold.screener.get_loader", return_value=mock_loader):
            # Panggil _check_data_freshness dengan fresh connection
            # Karena data tidak ada, fungsi akan return False/None tapi
            # yang penting count() harus dipanggil ke loader
            try:
                scr_module._check_data_freshness(date(2025, 6, 1))
            except Exception:
                pass  # Tidak ada data — hanya memastikan loader.count() dipanggil

        mock_loader.count.assert_called(), (
            "GLD-005: get_loader().count() harus dipanggil di _check_data_freshness()"
        )

    def test_coverage_threshold_scales_with_universe(self):
        """
        Dengan universe 692, fresh_count = 610 (88.2% < 95%) → RuntimeError.
        Pre-fix (hardcode 643): 610 > 610.85 threshold → false OK (screener runs).
        Post-fix (dynamic 692): 610 < 657.4 threshold → RuntimeError (screener blocked).

        UPD ADR-022/RISK-6 (GMI_Decision_Document_v2.docx CI Gate G-8):
        _check_data_freshness() now pre-checks layer1_globs() and returns
        early (skip, not raise) if no Layer 1 Silver directory exists yet —
        correct pre-backfill behavior, but it means this test (which only
        mocks duckdb.connect, not the filesystem) must also mock
        layer1_globs() directly to reach the DuckDB-mocked code path at
        all, regardless of whether data/silver/market_ohlcv happens to
        exist in the environment running the test.
        """
        from src.gold.screener import _check_data_freshness

        mock_loader = MagicMock()
        mock_loader.count.return_value = 692

        with patch("src.gold.screener.get_loader", return_value=mock_loader), \
             patch("src.gold.screener.layer1_globs",
                   return_value=["data/silver/market_ohlcv/us_stocks/**/*_1D_silver.parquet"]):
            with patch("duckdb.connect") as mock_con:
                mock_result = MagicMock()
                mock_result.fetchone.return_value = (610,)
                mock_con.return_value.execute.return_value = mock_result

                with pytest.raises(RuntimeError, match="88.*%.*<.*95"):
                    _check_data_freshness(date(2025, 6, 1))

    def test_coverage_passes_with_correct_threshold_for_692(self):
        """
        Dengan universe 692, fresh_count = 660 (95.4% > 95%) → no exception (returns None).
        UPD ADR-022/RISK-6: layer1_globs() mocked — see docstring on
        test_coverage_threshold_scales_with_universe above.
        """
        from src.gold.screener import _check_data_freshness

        mock_loader = MagicMock()
        mock_loader.count.return_value = 692

        with patch("src.gold.screener.get_loader", return_value=mock_loader), \
             patch("src.gold.screener.layer1_globs",
                   return_value=["data/silver/market_ohlcv/us_stocks/**/*_1D_silver.parquet"]):
            with patch("duckdb.connect") as mock_con:
                mock_result = MagicMock()
                mock_result.fetchone.return_value = (660,)  # 660/692 = 95.4%
                mock_con.return_value.execute.return_value = mock_result

                # Should NOT raise — 95.4% is above 95% threshold
                try:
                    result = _check_data_freshness(date(2025, 6, 1))
                    # _check_data_freshness returns None on success (void function)
                    assert result is None
                except RuntimeError as e:
                    pytest.fail(
                        f"GLD-005: 660/692 = 95.4% >= 95% → should NOT raise, "
                        f"but got RuntimeError: {e}"
                    )

    def test_freshness_gate_skips_gracefully_pre_backfill(self):
        """NEW — ADR-022/RISK-6: when no Layer 1 Silver market directory
        exists yet (layer1_globs() returns []), the gate must skip
        gracefully (log + return), never raise and never pass an empty
        list into DuckDB's read_parquet (which raises on that)."""
        from src.gold.screener import _check_data_freshness

        mock_loader = MagicMock()
        mock_loader.count.return_value = 692

        with patch("src.gold.screener.get_loader", return_value=mock_loader), \
             patch("src.gold.screener.layer1_globs", return_value=[]):
            with patch("duckdb.connect") as mock_con:
                result = _check_data_freshness(date(2025, 6, 1))
                assert result is None
                mock_con.assert_not_called()
