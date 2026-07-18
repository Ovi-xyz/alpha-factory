"""
atomic_io.py — Utilitas atomic write untuk Parquet files.

FIX GLD-004: Semua Gold Parquet write harus menggunakan pattern
tempfile.NamedTemporaryFile + os.replace untuk mencegah data corruption
jika pipeline crash atau OOM di tengah write (M1 8GB RAM).

Supplementary Design G2 §3.5 mewajibkan pattern ini untuk Silver/Gold layer.
GD §17.7: anti-pattern adalah direct write_parquet() tanpa atomic guarantee.

Usage:
    from src.utils.atomic_io import atomic_write_parquet

    # Ganti: df.write_parquet(path, compression="zstd", compression_level=3)
    # Dengan:
    atomic_write_parquet(df, path, compression="zstd", compression_level=3)

Behavior:
    1. Tulis ke file tmpfile di direktori yang sama dengan target
    2. os.replace() untuk atomic rename (POSIX: atomic jika same filesystem)
    3. Jika terjadi exception: hapus tmpfile, re-raise exception
    4. Target path tidak pernah dalam keadaan partial/corrupt
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

# Default compression untuk Gold layer (GD §7.1)
_DEFAULT_GOLD_KWARGS: dict[str, Any] = {
    "compression":       "zstd",
    "compression_level": 3,
    "row_group_size":    50_000,
    "statistics":        True,
    "use_pyarrow":       True,
}


def atomic_write_parquet(
    df: pl.DataFrame,
    path: Path | str,
    **kwargs: Any,
) -> None:
    """
    Tulis DataFrame ke Parquet secara atomic via tempfile + os.replace.

    FIX GLD-004: mencegah partial/corrupt file jika pipeline crash
    atau OOM di M1 8GB saat memproses 643 symbols × 7 TF.

    Args:
        df:   Polars DataFrame yang akan ditulis.
        path: Path target Parquet file (final location).
        **kwargs: Forward ke pl.DataFrame.write_parquet(). Jika tidak
                  disediakan, menggunakan default Gold layer settings
                  (zstd level-3, row_group_size=50_000, statistics=True).

    Raises:
        Exception: Re-raise exception apapun dari write_parquet(),
                   setelah cleanup tempfile.

    Note:
        - os.replace() adalah atomic pada POSIX jika source dan dest
          berada di filesystem yang sama (guaranteed karena tmpfile
          dibuat di direktori parent yang sama dengan target).
        - Pada Windows, os.replace() juga atomic sejak Python 3.3.
        - Jika parent directory tidak ada, akan dibuat otomatis.
    """
    # FIX GLD-004: gabungkan default kwargs dengan caller overrides
    write_kwargs = {**_DEFAULT_GOLD_KWARGS, **kwargs}

    path = Path(path)
    # Pastikan direktori parent ada sebelum membuat tmpfile di sana
    path.parent.mkdir(parents=True, exist_ok=True)

    # Buat tmpfile di direktori yang SAMA dengan target
    # (penting: os.replace atomic hanya jika same filesystem)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent,
            suffix=".parquet.tmp",
            delete=False,        # Kita manage lifecycle manually
        ) as tmp:
            tmp_path = Path(tmp.name)

        # Tulis ke tmpfile dulu
        df.write_parquet(tmp_path, **write_kwargs)

        # Atomic rename: jika ini sukses, target langsung tersedia
        os.replace(tmp_path, path)  # FIX GLD-004: atomic pada POSIX

        logger.debug(
            f"[atomic_io] ✓ {path.name} ({len(df):,} rows,"
            f" {path.stat().st_size:,} bytes)"
        )

    except Exception:
        # Cleanup: hapus tmpfile yang mungkin partial/corrupt
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass   # Best-effort cleanup
        raise   # Re-raise original exception — caller harus handle


def atomic_write_parquet_safe(
    df: pl.DataFrame,
    path: Path | str,
    job_name: str = "",
    **kwargs: Any,
) -> bool:
    """
    Wrapper atomic_write_parquet() dengan error handling.

    Returns True jika sukses, False jika gagal (exception di-log, tidak di-raise).
    Untuk job yang tidak ingin abort pipeline saat write gagal.
    """
    try:
        atomic_write_parquet(df, path, **kwargs)
        return True
    except Exception as e:
        logger.error(
            f"[atomic_io] WRITE FAILED"
            + (f" [{job_name}]" if job_name else "")
            + f" → {path}: {e}"
        )
        return False
