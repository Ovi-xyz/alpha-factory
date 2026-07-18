"""
tests/integration/test_gold_audit_integration.py

Integration tests untuk semua 6 BLOCKING fixes dari audit_gold_layer_v1_7_4.

Setiap test class merepresentasikan satu finding ID:
    GLD-001: BEA NIPA unit-mixing
    GLD-002: DXY score hardcoded
    GLD-003: F-string SQL anti-pattern
    GLD-004: Non-atomic Parquet writes
    GLD-005: TOTAL_INSTRUMENTS hardcoded
    GLD-006: CI Gate G-2 blind spot

Tests menggunakan in-memory atau tmp_path fixtures — tidak ada external API call.
Baseline: v1.7.4 (676 passed / 0 failed / 0 error).
"""

from __future__ import annotations

import os
import pathlib
import re
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest


# ══════════════════════════════════════════════════════════════════════════════
# GLD-001: BEA NIPA unit-mixing
# ══════════════════════════════════════════════════════════════════════════════

class TestGLD001BEANIPAFilterIntegration:
    """
    GLD-001 integration: Bronze BEA parquet tidak lagi menyimpan
    campuran level vs %-change vs komponen GDP.
    """

    def test_line_filter_constant_defined_and_populated(self):
        """LINE_FILTER harus terdefinisi di bea_ingester dan tidak kosong."""
        from src.bronze.bea_ingester import LINE_FILTER
        assert isinstance(LINE_FILTER, dict), "LINE_FILTER harus dict"
        assert len(LINE_FILTER) > 0, "LINE_FILTER harus tidak kosong"

    def test_real_gdp_entry_present(self):
        """'real_gdp' harus ada di LINE_FILTER dengan value non-empty string."""
        from src.bronze.bea_ingester import LINE_FILTER
        assert "real_gdp" in LINE_FILTER, "LINE_FILTER harus punya entry 'real_gdp'"
        assert LINE_FILTER["real_gdp"] == "Gross domestic product", (
            "real_gdp filter value harus 'Gross domestic product'"
        )

    def test_pce_deflator_entry_present(self):
        """'pce_deflator' harus ada di LINE_FILTER."""
        from src.bronze.bea_ingester import LINE_FILTER
        assert "pce_deflator" in LINE_FILTER
        assert LINE_FILTER["pce_deflator"]  # Non-empty

    def test_trade_balance_entry_present(self):
        """'trade_balance' harus ada di LINE_FILTER."""
        from src.bronze.bea_ingester import LINE_FILTER
        assert "trade_balance" in LINE_FILTER
        assert LINE_FILTER["trade_balance"]  # Non-empty

    def test_line_filter_applied_in_source_code(self):
        """_fetch_nipa() source harus mengandung LINE_FILTER logic."""
        bea_path = pathlib.Path("src/bronze/bea_ingester.py")
        if not bea_path.exists():
            pytest.skip("bea_ingester.py tidak ditemukan")
        src = bea_path.read_text()
        assert "LINE_FILTER" in src, "LINE_FILTER harus direferensikan di bea_ingester.py"
        assert "LineDescription" in src, "Filter harus menggunakan kolom 'LineDescription'"


# ══════════════════════════════════════════════════════════════════════════════
# GLD-002: DXY score hardcoded
# ══════════════════════════════════════════════════════════════════════════════

class TestGLD002DXYScoreIntegration:
    """GLD-002 integration: _classify() menggunakan DXY dari indicators dict."""

    def _classify(self, indicators: dict) -> dict:
        """Wrap MacroRegimeDetector._classify() into a flat dict."""
        from src.gold.macro_regime import MacroRegimeDetector
        det = MacroRegimeDetector()
        regime, scores, confidence = det._classify(indicators)
        return {"regime": regime, "confidence": confidence, **scores}

    def test_dxy_key_read_from_indicators(self):
        """indicators dict dengan key 'dxy' harus digunakan di _classify()."""
        base = {"vix": 20.0, "yield_spread": 0.5, "cpi": 2.5, "gdp": 2.0}
        result_weak   = self._classify({**base, "dxy": 90.0})
        result_strong = self._classify({**base, "dxy": 115.0})
        assert result_weak.get("dxy", 0) != result_strong.get("dxy", 0.5), (
            "GLD-002: dxy_score harus berbeda untuk DXY=90 vs DXY=115"
        )

    def test_no_hardcoded_dxy_05_in_source(self):
        """Verifikasi source code macro_regime.py tidak mengandung 'dxy': 0.5 hardcode."""
        import pathlib, re
        regime_path = pathlib.Path("src/gold/macro_regime.py")
        if not regime_path.exists():
            pytest.skip("macro_regime.py tidak ditemukan")
        src = regime_path.read_text()
        hardcode_pattern = re.compile(
            r"""['"]\s*dxy\s*['"]\s*:\s*0\.5\s*,?\s*#\s*Neutral""", re.IGNORECASE
        )
        assert not hardcode_pattern.search(src), (
            "GLD-002 REGRESSION: 'dxy': 0.5 hardcode masih ada di macro_regime.py"
        )

    def test_dxy_score_in_classify_output(self):
        """_classify() output harus menyertakan 'dxy' key."""
        from src.gold.macro_regime import MacroRegimeDetector
        det = MacroRegimeDetector()
        _, scores, _ = det._classify({
            "vix": 20.0, "yield_spread": 0.5, "cpi": 2.5, "gdp": 2.0, "dxy": 100.0
        })
        assert "dxy" in scores


# ══════════════════════════════════════════════════════════════════════════════
# GLD-003: F-string SQL anti-pattern
# ══════════════════════════════════════════════════════════════════════════════

class TestGLD003NoFStringSQLIntegration:
    """
    GLD-003 integration: semua f-string SQL di src/gold/ harus sudah
    dikonversi ke $name parameterized queries.
    """

    FSTRING_SQL_PATTERN = re.compile(r'[fF]"""')
    SQL_KW = re.compile(
        r"\b(SELECT|FROM\s+read_parquet)\b", re.IGNORECASE
    )

    def _violations_in(self, filepath: pathlib.Path) -> list[tuple[int, str]]:
        """Cari f-string SQL violations di file tertentu."""
        if not filepath.exists():
            return []
        src = filepath.read_text(encoding="utf-8", errors="replace")
        violations = []
        for m in self.FSTRING_SQL_PATTERN.finditer(src):
            snippet = src[m.start() : m.start() + 400]
            if self.SQL_KW.search(snippet):
                ln = src[: m.start()].count("\n") + 1
                violations.append((ln, snippet[:80]))
        return violations

    GOLD_FILES = [
        "src/gold/technical_signals.py",
        "src/gold/mtf_alignment.py",
        "src/gold/screener.py",
        "src/gold/correlation_matrix.py",
        "src/gold/hmm_regime.py",
        "src/gold/macro_regime.py",
    ]

    @pytest.mark.parametrize("gold_file", GOLD_FILES)
    def test_gold_file_no_fstring_sql(self, gold_file: str):
        """Setiap Gold layer file harus bebas f-string SQL."""
        violations = self._violations_in(pathlib.Path(gold_file))
        assert violations == [], (
            f"GLD-003: {gold_file} masih mengandung {len(violations)} f-string SQL:\n"
            + "\n".join(f"  line {ln}: {snip!r}" for ln, snip in violations)
        )


# ══════════════════════════════════════════════════════════════════════════════
# GLD-004: Non-atomic Parquet writes
# ══════════════════════════════════════════════════════════════════════════════

class TestGLD004AtomicWriteIntegration:
    """
    GLD-004 integration: semua Gold layer write harus melalui atomic_write_parquet().
    """

    GOLD_WRITE_FILES = [
        "src/gold/macro_regime.py",
        "src/gold/technical_signals.py",
        "src/gold/mtf_alignment.py",
        "src/gold/screener.py",
        "src/gold/correlation_matrix.py",
        "src/gold/sector_rotation.py",
    ]

    @pytest.mark.parametrize("gold_file", GOLD_WRITE_FILES)
    def test_file_imports_atomic_write(self, gold_file: str):
        """Setiap Gold file yang menulis Parquet harus import atomic_write_parquet."""
        fpath = pathlib.Path(gold_file)
        if not fpath.exists():
            pytest.skip(f"{gold_file} tidak ditemukan")
        src = fpath.read_text()
        assert "atomic_write_parquet" in src, (
            f"GLD-004: {gold_file} harus menggunakan atomic_write_parquet, "
            f"bukan direct df.write_parquet()"
        )

    @pytest.mark.parametrize("gold_file", GOLD_WRITE_FILES)
    def test_no_bare_write_parquet_in_gold_file(self, gold_file: str):
        """
        Setiap Gold file tidak boleh mengandung bare df.write_parquet()
        sebagai pengganti atomic_write_parquet() untuk output utama.
        """
        fpath = pathlib.Path(gold_file)
        if not fpath.exists():
            pytest.skip(f"{gold_file} tidak ditemukan")

        src = fpath.read_text()

        # Cari pattern: direct .write_parquet( yang bukan dalam komentar
        # (atomic_write_parquet sendiri memanggil write_parquet di dalamnya — OK)
        # Exclude: line yang ada di atomic_io.py (internal call)
        direct_write = re.compile(
            r"(?<!atomic_write_parquet\()df\.write_parquet\("
        )
        # Simplifikasi: cari .write_parquet( di baris yang TIDAK mengandung "atomic"
        lines_with_direct = []
        for i, line in enumerate(src.splitlines(), 1):
            if ".write_parquet(" in line and "atomic" not in line and not line.strip().startswith("#"):
                lines_with_direct.append((i, line.strip()))

        assert lines_with_direct == [], (
            f"GLD-004: {gold_file} mengandung direct write_parquet() tanpa atomic:\n"
            + "\n".join(f"  line {ln}: {code!r}" for ln, code in lines_with_direct)
        )

    def test_atomic_io_module_exists(self):
        """src/utils/atomic_io.py harus ada sebagai prerequisite GLD-004."""
        assert pathlib.Path("src/utils/atomic_io.py").exists(), (
            "GLD-004: src/utils/atomic_io.py tidak ditemukan"
        )

    def test_atomic_write_no_partial_file_on_crash(self, tmp_path: Path):
        """
        E2E: crash simulation — target file tidak boleh dalam keadaan partial.
        """
        from src.utils.atomic_io import atomic_write_parquet

        out = tmp_path / "gold_output.parquet"
        df  = pl.DataFrame({"symbol": ["AAPL"], "score": [6.5]})

        with patch.object(pl.DataFrame, "write_parquet", side_effect=MemoryError("OOM")):
            with pytest.raises(MemoryError):
                atomic_write_parquet(df, out)

        assert not out.exists(), (
            "GLD-004: target file tidak boleh ada setelah crash — partial file corruption"
        )
        # Verifikasi tidak ada tmpfile tersisa
        assert not any(tmp_path.glob("*.parquet.tmp")), "Tidak boleh ada orphaned tmpfile"


# ══════════════════════════════════════════════════════════════════════════════
# GLD-005: TOTAL_INSTRUMENTS hardcoded
# ══════════════════════════════════════════════════════════════════════════════

class TestGLD005DynamicTotalInstruments:
    """
    GLD-005 integration: screener menggunakan get_loader().count() bukan 643.
    """

    def test_screener_does_not_hardcode_643(self):
        """screener.py tidak boleh mengandung TOTAL_INSTRUMENTS = 643."""
        screener_path = pathlib.Path("src/gold/screener.py")
        if not screener_path.exists():
            pytest.skip("screener.py tidak ditemukan")
        src = screener_path.read_text()
        bad_pattern = re.compile(r"TOTAL_INSTRUMENTS\s*=\s*643\b")
        assert not bad_pattern.search(src), (
            "GLD-005 REGRESSION: TOTAL_INSTRUMENTS masih hardcoded 643"
        )

    def test_screener_uses_get_loader(self):
        """screener.py harus referensikan get_loader untuk TOTAL_INSTRUMENTS."""
        screener_path = pathlib.Path("src/gold/screener.py")
        if not screener_path.exists():
            pytest.skip("screener.py tidak ditemukan")
        src = screener_path.read_text()
        assert "get_loader" in src, (
            "GLD-005: screener.py harus import dan menggunakan get_loader()"
        )

    def test_loader_count_callable_returns_integer(self):
        """get_loader().count() harus return integer > 0."""
        from src.config.instrument_loader import get_loader
        loader = get_loader()
        count  = loader.count()
        assert isinstance(count, int), f"count() harus return int, bukan {type(count)}"
        assert count > 0, "count() harus > 0"


# ══════════════════════════════════════════════════════════════════════════════
# GLD-006: CI Gate G-2 blind spot
# ══════════════════════════════════════════════════════════════════════════════

class TestGLD006CIGateIntegration:
    """
    GLD-006 integration: ci.yml menggunakan Python-based detection.
    """

    def test_ci_yml_exists(self):
        """.github/workflows/ci.yml harus ada."""
        assert pathlib.Path(".github/workflows/ci.yml").exists(), (
            "GLD-006: .github/workflows/ci.yml tidak ditemukan"
        )

    def test_ci_yml_has_python_detection(self):
        """Gate G-2 harus menggunakan Python, bukan grep-only."""
        ci_path = pathlib.Path(".github/workflows/ci.yml")
        if not ci_path.exists():
            pytest.skip("ci.yml tidak ditemukan")
        content = ci_path.read_text()
        assert "python -c" in content, (
            "GLD-006: ci.yml Gate G-2 harus menggunakan Python script, bukan grep"
        )

    def test_ci_yml_has_collect_count_gate(self):
        """ci.yml harus memiliki collection count monitoring (mencegah NEW-4 terulang)."""
        ci_path = pathlib.Path(".github/workflows/ci.yml")
        if not ci_path.exists():
            pytest.skip("ci.yml tidak ditemukan")
        content = ci_path.read_text()
        assert "BASELINE" in content or "collect" in content.lower(), (
            "GLD-006: ci.yml harus memiliki test collection count monitoring"
        )

    def test_all_six_findings_have_test_coverage(self):
        """
        Meta-test: verifikasi semua 6 finding IDs memiliki test file yang sesuai.
        Mapping finding → test file:
          GLD-001 → test_bea_ingester_gld001.py
          GLD-002 → test_macro_regime_gld002.py
          GLD-003 → test_fstring_sql_absence.py
          GLD-004 → test_atomic_io.py
          GLD-005 → test_screener_gld005.py
          GLD-006 → test_ci_gate_gld006.py
        """
        EXPECTED_FILES = {
            "test_bea_ingester_gld001.py",
            "test_macro_regime_gld002.py",
            "test_fstring_sql_absence.py",
            "test_atomic_io.py",
            "test_screener_gld005.py",
            "test_ci_gate_gld006.py",
        }
        existing_names = {f.name for f in pathlib.Path("tests").rglob("*.py")}
        missing = EXPECTED_FILES - existing_names
        assert not missing, (
            f"Missing test files for audit findings: {missing}"
        )
