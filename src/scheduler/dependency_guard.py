"""
dependency_guard.py — GD §14.3.3 (Dependency Guard — File Sentinel Based)
Verifikasi upstream job selesai via file sentinel (.done files).

Tidak ada database polling — cukup cek keberadaan file.
Reset otomatis setiap hari baru (filename include date).

Sentinel path: data/.sentinels/{job_name}_{YYYY-MM-DD}.done

Keuntungan vs SQLite polling:
    - Tidak ada race condition di manual mode
    - Debugging mudah: ls data/.sentinels/
    - Reset manual: rm data/.sentinels/*.done

FIX NEW-1 [BLOCKING] (audit_v1_7_3_uncovered_findings.docx, Section 2, Opsi A):
    check_dependencies() sebelumnya HANYA mengecek sentinel pada run_date PERSIS.
    Ini benar untuk dependency dengan cadence harian, tapi salah untuk dependency
    lintas-cadence — silver_validate dan gold_regime (DAILY_SEQUENCE) hard-depend
    pada silver_macro (cadence mingguan, GD §3.3.1, TIDAK ada di DAILY_SEQUENCE).
    Akibatnya `python runner.py --job all` selalu sys.exit(1) di hari Senin-Sabtu,
    bahkan jika silver_macro sudah pernah berjalan sukses Minggu lalu — sentinel
    bernama silver_macro_{tanggal-Minggu}.done tidak pernah cocok dengan
    pencarian tanggal hari ini.

    Fix: is_done_within() mencari sentinel dalam window mundur N hari, dipakai
    via parameter opsional stale_tolerance di check_dependencies() — dependency
    yang TIDAK dikonfigurasi stale_tolerance tetap memakai exact-date match
    (perilaku lama, 100% backward compatible untuk semua job lain).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from loguru import logger


class DependencyGuard:
    """
    File sentinel based dependency checker.
    Setiap job yang selesai menulis .done file.
    Re-run otomatis bersih setiap hari baru (date dalam filename).
    """

    def __init__(self, sentinel_dir: Path | None = None) -> None:
        self.sentinel_dir = sentinel_dir or Path("data/.sentinels")
        self.sentinel_dir.mkdir(parents=True, exist_ok=True)

    def _sentinel_path(self, job_name: str, run_date: date) -> Path:
        return self.sentinel_dir / f"{job_name}_{run_date.isoformat()}.done"

    def is_done(self, job_name: str, run_date: date) -> bool:
        """Return True jika job sudah selesai hari ini."""
        return self._sentinel_path(job_name, run_date).exists()

    def is_done_within(
        self,
        job_name: str,
        run_date: date,
        max_age_days: int,
    ) -> bool:
        """
        FIX NEW-1: Return True jika ada sentinel job_name untuk run_date ATAU
        salah satu dari max_age_days hari SEBELUM run_date.

        Untuk dependency dengan cadence tidak-harian (mis. silver_macro,
        cadence mingguan per GD §3.3.1) yang TIDAK diharapkan menulis sentinel
        baru setiap hari. Tanpa ini, exact-date check_dependencies() akan
        selalu menganggap dependency tersebut "belum selesai" pada hari-hari
        di luar cadence-nya, walau sudah pernah sukses dalam window yang wajar.

        Args:
            job_name:     Nama job dependency yang dicari sentinel-nya.
            run_date:     Tanggal acuan (biasanya run_date job yang sedang dicek).
            max_age_days: Jumlah hari mundur yang masih dianggap valid (>= 0).
                          0 = identik dengan is_done() (exact-date only).

        Returns:
            True jika sentinel ditemukan pada run_date atau salah satu dari
            max_age_days hari sebelumnya.
        """
        if max_age_days < 0:
            raise ValueError(
                f"max_age_days harus >= 0, got {max_age_days}"
            )
        for offset in range(max_age_days + 1):
            if self.is_done(job_name, run_date - timedelta(days=offset)):
                return True
        return False

    def mark_done(self, job_name: str, run_date: date) -> None:
        """Tulis sentinel file — menandai job selesai."""
        path = self._sentinel_path(job_name, run_date)
        path.write_text(
            f"completed at {date.today().isoformat()}\n"
            f"job: {job_name}\n"
            f"run_date: {run_date.isoformat()}\n"
        )
        logger.debug(f"[DependencyGuard] Sentinel written: {path.name}")

    def check_dependencies(
        self,
        depends_on: list[str],
        run_date: date,
        stale_tolerance: dict[str, int] | None = None,
    ) -> list[str]:
        """
        Return list job yang BELUM selesai.
        Empty list = semua dependency terpenuhi.

        FIX NEW-1: stale_tolerance adalah dict opsional {dep_name: max_age_days}.
        Dependency yang TIDAK terdaftar di stale_tolerance (atau dipanggil
        dengan stale_tolerance=None, default) tetap memakai exact-date match
        via is_done() — perilaku tidak berubah untuk seluruh job existing.
        Hanya dependency yang eksplisit dikonfigurasi (mis. silver_macro untuk
        silver_validate/gold_regime) yang mendapat staleness window.
        """
        stale_tolerance = stale_tolerance or {}
        missing: list[str] = []
        for dep in depends_on:
            max_age = stale_tolerance.get(dep, 0)
            if max_age > 0:
                if not self.is_done_within(dep, run_date, max_age):
                    missing.append(dep)
            else:
                if not self.is_done(dep, run_date):
                    missing.append(dep)
        return missing

    def get_all_statuses(
        self,
        job_names: list[str],
        run_date: date,
    ) -> dict[str, bool]:
        """Return {job_name: is_done} untuk semua job."""
        return {name: self.is_done(name, run_date) for name in job_names}

    def reset_job(self, job_name: str, run_date: date) -> None:
        """Hapus sentinel satu job — aktifkan re-run tanpa --force."""
        path = self._sentinel_path(job_name, run_date)
        if path.exists():
            path.unlink()
            logger.info(f"[DependencyGuard] Sentinel reset: {job_name} ({run_date})")
        else:
            logger.warning(
                f"[DependencyGuard] No sentinel found for {job_name} ({run_date})"
            )

    def reset_all(self, run_date: date) -> int:
        """
        Hapus semua sentinel untuk run_date.
        Return jumlah sentinel yang dihapus.
        """
        pattern = f"*_{run_date.isoformat()}.done"
        sentinels = list(self.sentinel_dir.glob(pattern))
        for s in sentinels:
            s.unlink()
        logger.info(
            f"[DependencyGuard] Reset {len(sentinels)} sentinels for {run_date}"
        )
        return len(sentinels)
