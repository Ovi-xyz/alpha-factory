"""
tests/unit/test_atomic_io.py — Test suite untuk atomic_io.py

FIX GLD-004: Non-atomic Parquet writes di Gold layer.
Verifikasi bahwa atomic_write_parquet():
    1. Menulis file dengan benar saat sukses
    2. Tidak meninggalkan corrupt/partial file saat exception
    3. Membuat parent directory jika belum ada
    4. atomic_write_parquet_safe() mengembalikan True/False sesuai
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from src.utils.atomic_io import atomic_write_parquet, atomic_write_parquet_safe


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Minimal DataFrame untuk testing."""
    return pl.DataFrame({
        "symbol":    ["AAPL", "MSFT"],
        "value":     [150.0, 300.0],
        "date":      [str(date(2025, 1, 1))] * 2,
    })


class TestAtomicWriteParquet:
    """Unit tests untuk atomic_write_parquet() — FIX GLD-004."""

    def test_write_succeeds_and_readable(self, tmp_path: Path, sample_df: pl.DataFrame):
        """File yang ditulis harus bisa dibaca kembali dengan data yang benar."""
        out = tmp_path / "test.parquet"
        atomic_write_parquet(sample_df, out, compression="zstd")

        assert out.exists(), "Output file harus ada setelah write sukses"
        result = pl.read_parquet(out)
        assert len(result) == len(sample_df)
        assert result["symbol"].to_list() == ["AAPL", "MSFT"]

    def test_creates_parent_directory(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Harus membuat parent directory jika belum ada."""
        nested_path = tmp_path / "a" / "b" / "c" / "output.parquet"
        assert not nested_path.parent.exists()

        atomic_write_parquet(sample_df, nested_path, compression="zstd")
        assert nested_path.exists()

    def test_no_corrupt_file_on_exception(self, tmp_path: Path, sample_df: pl.DataFrame):
        """
        FIX GLD-004 critical: jika exception terjadi mid-write,
        output path harus TIDAK ada (tidak pernah partial/corrupt).
        """
        out = tmp_path / "output.parquet"
        assert not out.exists()

        # Simulasi crash saat write_parquet
        with patch.object(pl.DataFrame, "write_parquet", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                atomic_write_parquet(sample_df, out, compression="zstd")

        # Target path harus tidak ada (bukan partial)
        assert not out.exists(), (
            "FAIL GLD-004: output file tidak boleh ada jika write gagal — "
            "atomic_write_parquet harus cleanup tmpfile dan tidak touch target"
        )

    def test_no_tmpfile_left_on_exception(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Tidak boleh ada tmpfile yang tersisa setelah exception."""
        out = tmp_path / "output.parquet"

        with patch.object(pl.DataFrame, "write_parquet", side_effect=OSError("crash")):
            with pytest.raises(OSError):
                atomic_write_parquet(sample_df, out, compression="zstd")

        # Pastikan tidak ada file *.parquet.tmp yang tersisa
        leftover = list(tmp_path.glob("*.parquet.tmp"))
        assert len(leftover) == 0, f"Tmpfile tersisa: {leftover}"

    def test_overwrites_existing_file_atomically(self, tmp_path: Path):
        """Jika target sudah ada, harus overwrite secara atomic."""
        out = tmp_path / "existing.parquet"
        old_df = pl.DataFrame({"symbol": ["OLD"], "value": [0.0]})
        old_df.write_parquet(out)

        new_df = pl.DataFrame({"symbol": ["NEW"], "value": [999.0]})
        atomic_write_parquet(new_df, out, compression="zstd")

        result = pl.read_parquet(out)
        assert result["symbol"].to_list() == ["NEW"], "File lama harus diganti"

    def test_accepts_path_as_string(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Harus menerima path sebagai string maupun Path object."""
        out_str = str(tmp_path / "string_path.parquet")
        atomic_write_parquet(sample_df, out_str, compression="zstd")
        assert Path(out_str).exists()

    def test_default_kwargs_applied(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Tanpa kwargs, default Gold layer settings harus diapply."""
        out = tmp_path / "default.parquet"
        # Ini tidak boleh raise — default kwargs harus valid
        atomic_write_parquet(sample_df, out)
        assert out.exists()

    def test_caller_kwargs_override_default(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Caller kwargs harus override default values."""
        out = tmp_path / "level1.parquet"
        # Override compression_level=1 (lebih cepat dari default 3)
        # Gunakan zstd agar compatible dengan compression_level parameter
        atomic_write_parquet(sample_df, out, compression="zstd", compression_level=1)
        assert out.exists()


class TestAtomicWriteParquetSafe:
    """Tests untuk atomic_write_parquet_safe() helper."""

    def test_returns_true_on_success(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Return True jika write sukses."""
        out = tmp_path / "success.parquet"
        result = atomic_write_parquet_safe(sample_df, out, compression="zstd")
        assert result is True
        assert out.exists()

    def test_returns_false_on_failure(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Return False dan tidak raise jika write gagal."""
        out = tmp_path / "fail.parquet"
        with patch.object(pl.DataFrame, "write_parquet", side_effect=OSError("fail")):
            result = atomic_write_parquet_safe(sample_df, out, compression="zstd")
        assert result is False
        assert not out.exists()

    def test_does_not_raise_on_failure(self, tmp_path: Path, sample_df: pl.DataFrame):
        """safe variant tidak boleh raise exception ke caller."""
        out = tmp_path / "no_raise.parquet"
        with patch.object(pl.DataFrame, "write_parquet", side_effect=RuntimeError("boom")):
            # Tidak boleh raise
            result = atomic_write_parquet_safe(sample_df, out, job_name="test_job")
        assert result is False


class TestCleanupFailureDuringExceptionHandling:
    """Coverage tranche (17 Aug 2026) — except OSError: pass around
    tmp_path.unlink() inside the outer exception handler: a double
    failure (write fails, THEN cleanup also fails) must still re-raise
    the original exception rather than crash on the cleanup itself."""

    def test_unlink_failure_during_cleanup_still_reraises_original(
        self, tmp_path: Path, sample_df: pl.DataFrame
    ):
        target = tmp_path / "out.parquet"
        with patch.object(
            pl.DataFrame, "write_parquet", side_effect=ValueError("write failed")
        ), patch.object(
            Path, "unlink", side_effect=OSError("cleanup also failed")
        ):
            with pytest.raises(ValueError, match="write failed"):
                atomic_write_parquet(sample_df, target)
