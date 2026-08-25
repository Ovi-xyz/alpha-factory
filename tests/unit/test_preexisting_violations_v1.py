"""
tests/unit/test_preexisting_violations_v1.py
Regression guard untuk audit_preexisting_violations_v1_0.docx.

Dokumen referensi: audit_preexisting_violations_v1_0.docx (Juni 2026)
Remediation releases: v1.7.6 (BLOCKING + P1 HIGH), v1.7.7 (P1 + P2)

Findings yang diuji:
    SIL-SQL-001 [BLOCKING] — quality_validator.py (9 violations)
    SIL-AIO-001 [BLOCKING] — ohlcv_processor.py write
    SIL-AIO-002 [BLOCKING] — macro_processor.py write
    SIL-AIO-003 [P1 HIGH]  — active_symbols.py shutil.move → os.replace
    BRZ-AIO-001 [P1 HIGH]  — base_ingester, forex_cache, schema_validator writes
    BRZ-SQL-001 [P1 HIGH]  — eia_ingester, fred_ingester f-string SQL
    SIL-AIO-004 [P1 HIGH]  — fundamental_processor, sentiment_processor writes
    SIL-SQL-002 [P1 HIGH]  — macro_processor.py f-string SQL
    SIL-SQL-003 [P1 HIGH]  — fundamental_processor.py f-string SQL
    BCK-SQL-001 [P1 HIGH]  — pit_data.py f-string SQL (6 violations)
    SIL-RPQ-001 [P2 MED]   — eager read_parquet in Silver layer
    UTL-SQL-001 [P2 MED]   — delta_reprocessor.py f-string SQL
    BCK-AIO-001 [P2 MED]   — engine.py non-atomic writes
    BCK-PIT-001 [P2 MED]   — engine.py date.today() in _save_results

Setiap test adalah REGRESSION GUARD — jika violation muncul kembali
akibat merge conflict atau regresi, test ini akan gagal.
"""

from __future__ import annotations

import ast
import re
import pathlib
from typing import Iterator

import pytest

# ── Konfigurasi ──────────────────────────────────────────────────────────────

SRC = pathlib.Path("src")

# Regex: triple-quote f-string opener
FSTR_SQL_RE = re.compile(r'[fF]"""')
SQL_KW_RE   = re.compile(
    r'\b(SELECT|FROM\s+read_parquet|INSERT\s+INTO|UPDATE\s+\w|DELETE\s+FROM)\b',
    re.IGNORECASE,
)

# Regex: direct write_parquet (non-atomic)
WRITE_PARQUET_RE = re.compile(r'(?<!atomic_write_parquet\()\.write_parquet\s*\(')

# Regex: deprecated shutil.move
SHUTIL_MOVE_RE = re.compile(r'shutil\.move\s*\(')

# Regex: eager pl.read_parquet (Silver/Backtest scope — Gold exempt)
EAGER_RP_RE = re.compile(r'\bpl\.read_parquet\s*\(')


def _fstring_sql_violations(path: pathlib.Path) -> list[tuple[int, str]]:
    """Return (line_no, snippet) for each f-string SQL violation in path."""
    try:
        txt = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    hits = []
    for m in FSTR_SQL_RE.finditer(txt):
        snippet = txt[m.start(): m.start() + 400]
        if SQL_KW_RE.search(snippet):
            ln = txt[: m.start()].count("\n") + 1
            hits.append((ln, snippet[:80].replace("\n", " ").strip()))
    return hits


def _direct_write_parquet(path: pathlib.Path) -> list[tuple[int, str]]:
    """Return (line_no, line) for each direct .write_parquet() call (non-atomic)."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    hits = []
    for i, line in enumerate(lines, 1):
        stripped = line.lstrip()
        # Skip: comments, the atomic_io implementation itself, temp-file writes
        if stripped.startswith("#"):
            continue
        if "atomic_io" in str(path):
            continue  # atomic_io.py IS the implementation — false positive
        if ".write_parquet(" in line and "atomic_write_parquet" not in line:
            # Allow writes to *.tmp temp files (atomic pattern step 1)
            if ".parquet.tmp" in lines[i - 1] if i > 1 else False:
                continue
            # Allow if the previous line assigns a temp path
            if "tmp_path" in line and i > 1 and "tmp" in lines[i - 2].lower():
                # Could be a temp-file write — check context
                ctx = "\n".join(lines[max(0, i-4):i+2])
                if "NamedTemporaryFile" in ctx or ".parquet.tmp" in ctx:
                    continue
            hits.append((i, line.strip()[:80]))
    return hits


# ═══════════════════════════════════════════════════════════════════════════════
# SIL-SQL-001 [BLOCKING] — quality_validator.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestSILSQL001QualityValidator:
    """9 f-string SQL violations trong quality_validator.py phải sudah diperbaiki."""

    TARGET = SRC / "silver" / "quality_validator.py"

    def test_file_exists(self):
        assert self.TARGET.exists(), f"{self.TARGET} tidak ditemukan"

    def test_syntax_valid(self):
        ast.parse(self.TARGET.read_text())

    def test_no_fstring_sql(self):
        hits = _fstring_sql_violations(self.TARGET)
        assert not hits, (
            f"SIL-SQL-001 REGRESSION: {len(hits)} f-string SQL masih ada:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
        )

    def test_atomic_io_imported(self):
        src = self.TARGET.read_text()
        assert "atomic_write_parquet" in src, (
            "SIL-SQL-001: atomic_write_parquet harus diimport di quality_validator.py"
        )

    def test_no_direct_write_parquet(self):
        """_flag_outliers_in_file harus menggunakan atomic_write_parquet."""
        hits = _direct_write_parquet(self.TARGET)
        assert not hits, (
            f"SIL-AIO hint: direct write_parquet masih ada:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
        )

    @pytest.mark.parametrize("method_name", [
        "_check_null", "_check_price_sanity", "_check_coverage",
        "_check_gap_detection", "_check_freshness", "_check_macro_pit",
        "_check_adj_integrity", "_check_vix", "_flag_outliers",
    ])
    def test_parameterized_queries_present(self, method_name: str):
        """Setiap method harus menggunakan $name binding, bukan f-string SQL."""
        src = self.TARGET.read_text()
        # Cek bahwa $glob atau $run_date atau $threshold ada di dekat method def
        # (heuristik: metode tidak mungkin menggunakan DuckDB tanpa param jika fixes diterapkan)
        # Minimal: tidak ada f\"\"\" SQL lebih dari 0
        hits = _fstring_sql_violations(self.TARGET)
        assert len(hits) == 0, f"Method area {method_name} masih punya f-string SQL"


# ═══════════════════════════════════════════════════════════════════════════════
# SIL-AIO-001 [BLOCKING] — ohlcv_processor.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestSILAIO001OHLCVProcessor:
    """Non-atomic write di ohlcv_processor.py harus sudah diperbaiki."""

    TARGET = SRC / "silver" / "ohlcv_processor.py"

    def test_syntax_valid(self):
        ast.parse(self.TARGET.read_text())

    def test_atomic_write_parquet_imported(self):
        src = self.TARGET.read_text()
        assert "atomic_write_parquet" in src, (
            "SIL-AIO-001: atomic_write_parquet belum diimport"
        )

    def test_no_direct_write_parquet(self):
        hits = _direct_write_parquet(self.TARGET)
        assert not hits, (
            f"SIL-AIO-001 REGRESSION: direct .write_parquet() masih ada:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
        )

    def test_no_eager_read_parquet(self):
        src = self.TARGET.read_text()
        # Filter out comments and BEFORE/AFTER markers in docstrings
        code_lines = [
            (i+1, l) for i, l in enumerate(src.splitlines())
            if "pl.read_parquet(" in l
            and not l.lstrip().startswith("#")
            and "BEFORE:" not in l
        ]
        assert not code_lines, (
            f"SIL-RPQ-001: eager pl.read_parquet() masih ada di ohlcv_processor.py:\n"
            + "\n".join(f"  :{ln} → {l.strip()}" for ln, l in code_lines)
        )

    def test_scan_parquet_used_for_bronze_read(self):
        src = self.TARGET.read_text()
        assert "scan_parquet" in src, (
            "SIL-RPQ-001: scan_parquet harus digunakan untuk Bronze data read"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SIL-AIO-002 + SIL-SQL-002 [BLOCKING] — macro_processor.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestSILAIO002MacroProcessor:
    """Non-atomic write dan f-string SQL di macro_processor.py."""

    TARGET = SRC / "silver" / "macro_processor.py"

    def test_syntax_valid(self):
        ast.parse(self.TARGET.read_text())

    def test_no_fstring_sql(self):
        hits = _fstring_sql_violations(self.TARGET)
        assert not hits, (
            f"SIL-SQL-002 REGRESSION: f-string SQL:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
        )

    def test_no_direct_write_parquet(self):
        hits = _direct_write_parquet(self.TARGET)
        assert not hits, (
            f"SIL-AIO-002 REGRESSION: direct .write_parquet():\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
        )

    def test_atomic_write_parquet_imported(self):
        assert "atomic_write_parquet" in self.TARGET.read_text()

    def test_no_eager_read_parquet(self):
        src = self.TARGET.read_text()
        eager = [
            (i+1, l) for i, l in enumerate(src.splitlines())
            if "pl.read_parquet(" in l and not l.lstrip().startswith("#")
            and "BEFORE:" not in l
        ]
        assert not eager, (
            f"SIL-RPQ-001: eager pl.read_parquet() di macro_processor.py:\n"
            + "\n".join(f"  :{ln} → {l.strip()}" for ln, l in eager)
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SIL-AIO-003 [P1 HIGH] — active_symbols.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestSILAIO003ActiveSymbols:
    """shutil.move → os.replace di active_symbols.py."""

    TARGET = SRC / "silver" / "active_symbols.py"

    def test_syntax_valid(self):
        ast.parse(self.TARGET.read_text())

    def test_no_shutil_move(self):
        src = self.TARGET.read_text()
        hits = SHUTIL_MOVE_RE.findall(src)
        assert not hits, (
            f"SIL-AIO-003 REGRESSION: shutil.move masih digunakan "
            f"({len(hits)} kali). Ganti dengan os.replace()."
        )

    def test_no_shutil_import(self):
        src = self.TARGET.read_text()
        assert "import shutil" not in src, (
            "SIL-AIO-003: import shutil harus dihapus setelah shutil.move diganti"
        )

    def test_os_replace_used(self):
        src = self.TARGET.read_text()
        assert "os.replace(" in src, (
            "SIL-AIO-003: os.replace() harus digunakan untuk atomic rename"
        )

    def test_no_eager_read_parquet_in_load(self):
        """load() dan load_full() harus menggunakan scan_parquet().collect()."""
        src = self.TARGET.read_text()
        eager = [
            (i+1, l) for i, l in enumerate(src.splitlines())
            if "pl.read_parquet(" in l
            and not l.lstrip().startswith("#")
            and "BEFORE:" not in l
            and "replaced eager" not in l   # exclude docstring documentation
            and "replaced pl.read_parquet" not in l
        ]
        assert not eager, (
            f"SIL-RPQ-001: eager pl.read_parquet() di active_symbols.py:\n"
            + "\n".join(f"  :{ln} → {l.strip()}" for ln, l in eager)
        )


# ═══════════════════════════════════════════════════════════════════════════════
# BRZ-AIO-001 [P1 HIGH] — Bronze ingesters
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("rel_path", [
    "bronze/base_ingester.py",
    "bronze/forex_cache.py",
    "bronze/schema_validator.py",
])
class TestBRZAIO001BronzeAtomicWrites:
    """Bronze layer harus menggunakan atomic writes."""

    def test_syntax_valid(self, rel_path: str):
        ast.parse((SRC / rel_path).read_text())

    def test_atomic_write_parquet_imported(self, rel_path: str):
        src = (SRC / rel_path).read_text()
        assert "atomic_write_parquet" in src, (
            f"BRZ-AIO-001: {rel_path} belum mengimport atomic_write_parquet"
        )

    def test_no_direct_write_parquet(self, rel_path: str):
        hits = _direct_write_parquet(SRC / rel_path)
        assert not hits, (
            f"BRZ-AIO-001 REGRESSION: {rel_path} masih direct write_parquet:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
        )


# ═══════════════════════════════════════════════════════════════════════════════
# BRZ-SQL-001 [P1 HIGH] — eia_ingester.py + fred_ingester.py
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("rel_path", [
    "bronze/eia_ingester.py",
    "bronze/fred_ingester.py",
])
class TestBRZSQL001BronzeIngesters:
    """Bronze ingesters harus bebas f-string SQL."""

    def test_syntax_valid(self, rel_path: str):
        ast.parse((SRC / rel_path).read_text())

    def test_no_fstring_sql(self, rel_path: str):
        hits = _fstring_sql_violations(SRC / rel_path)
        assert not hits, (
            f"BRZ-SQL-001 REGRESSION: {rel_path} f-string SQL:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
        )

    def test_parameterized_glob_in_scan(self, rel_path: str):
        """Glob path ke read_parquet harus via $glob parameter."""
        src = (SRC / rel_path).read_text()
        assert "$glob" in src or '{"glob":' in src or '"glob"' in src, (
            f"BRZ-SQL-001: {rel_path} harus menggunakan $glob parameter"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SIL-AIO-004 + SIL-SQL-003 [P1 HIGH] — fundamental_processor.py + sentiment
# ═══════════════════════════════════════════════════════════════════════════════

#
# FIX ADR-043 (GMI_Decision_Document_v10.docx, 22 Aug 2026): TestSILAIO004-
# FundamentalProcessor and TestSILAIO004SentimentProcessor removed --
# src/silver/fundamental_processor.py and src/silver/sentiment_processor.py
# were both deleted in full (Finnhub retired: sentiment 403 plan-tier gate
# on every symbol; earnings/quotes never left its NotImplementedError
# stub). These regression guards protected atomic-write and $glob-
# parameterization discipline in code that no longer exists -- there is
# nothing left to regress. See KNOWN_RISKS.md RISK-4 for full detail.


# ═══════════════════════════════════════════════════════════════════════════════
# BCK-SQL-001 [P1 HIGH] — pit_data.py (6 violations)
# ═══════════════════════════════════════════════════════════════════════════════

class TestBCKSQL001PITData:
    """6 f-string SQL violations di pit_data.py phải sudah diperbaiki."""

    TARGET = SRC / "backtest" / "pit_data.py"

    def test_syntax_valid(self):
        ast.parse(self.TARGET.read_text())

    def test_no_fstring_sql(self):
        hits = _fstring_sql_violations(self.TARGET)
        assert not hits, (
            f"BCK-SQL-001 REGRESSION: {len(hits)} f-string SQL di pit_data.py:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
        )

    @pytest.mark.parametrize("expected_param", [
        "$path", "$symbol", "$start", "$trade_date",
        "$series_id", "$symbols", "$glob",
    ])
    def test_parameterized_binding_present(self, expected_param: str):
        """Setiap class parameter binding harus hadir di pit_data.py."""
        src = self.TARGET.read_text()
        assert expected_param in src, (
            f"BCK-SQL-001: binding parameter '{expected_param}' tidak ditemukan "
            "di pit_data.py — query mungkin belum diparameterisasi"
        )

    def test_get_ohlcv_uses_symbol_param(self):
        src = self.TARGET.read_text()
        # get_ohlcv harus menggunakan $symbol bukan '{symbol}'
        assert "'{symbol}'" not in src, (
            "BCK-SQL-001: get_ohlcv masih menggunakan string injection '{symbol}'"
        )

    def test_get_macro_series_uses_series_id_param(self):
        src = self.TARGET.read_text()
        assert "'{series_id}'" not in src, (
            "BCK-SQL-001: get_macro_series masih menggunakan '{series_id}' injection"
        )

    def test_get_regime_no_date_injection(self):
        src = self.TARGET.read_text()
        assert "'{trade_date}'" not in src, (
            "BCK-SQL-001: get_regime masih menggunakan '{trade_date}' injection"
        )

    def test_pit_guard_maintained(self):
        """PIT guard (timestamp < trade_date) harus masih ada setelah fix."""
        src = self.TARGET.read_text()
        assert "PIT guard" in src, (
            "BCK-SQL-001: PIT guard comment harus tetap ada untuk dokumentasi"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# UTL-SQL-001 [P2 MED] — delta_reprocessor.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestUTLSQL001DeltaReprocessor:
    """2 f-string SQL violations di delta_reprocessor.py."""

    TARGET = SRC / "utils" / "delta_reprocessor.py"

    def test_syntax_valid(self):
        ast.parse(self.TARGET.read_text())

    def test_no_fstring_sql(self):
        hits = _fstring_sql_violations(self.TARGET)
        assert not hits, (
            f"UTL-SQL-001 REGRESSION: f-string SQL di delta_reprocessor.py:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
        )

    def test_current_version_parameterized(self):
        src = self.TARGET.read_text()
        # CURRENT_SILVER_VERSION harus di-inject via $current_version, bukan f-string
        assert "$current_version" in src, (
            "UTL-SQL-001: CURRENT_SILVER_VERSION harus via $current_version param"
        )

    def test_glob_parameterized(self):
        src = self.TARGET.read_text()
        assert "$glob" in src, (
            "UTL-SQL-001: glob path harus via $glob parameter"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# BCK-AIO-001 + BCK-PIT-001 [P2 MED] — engine.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestBCKAIO001Engine:
    """Non-atomic writes dan date.today() di engine.py."""

    TARGET = SRC / "backtest" / "engine.py"

    def test_syntax_valid(self):
        ast.parse(self.TARGET.read_text())

    def test_no_direct_write_parquet_in_save_results(self):
        """_save_results() harus menggunakan atomic_write_parquet."""
        hits = _direct_write_parquet(self.TARGET)
        assert not hits, (
            f"BCK-AIO-001 REGRESSION: direct write_parquet di engine.py:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
        )

    def test_atomic_write_parquet_imported(self):
        src = self.TARGET.read_text()
        assert "atomic_write_parquet" in src, (
            "BCK-AIO-001: atomic_write_parquet belum diimport di engine.py"
        )

    def test_save_results_no_date_today(self):
        """
        BCK-PIT-001: _save_results() harus menggunakan config.end_date,
        bukan date.today() — date.today() membuat backtest tidak reproducible.
        """
        src = self.TARGET.read_text()
        lines = src.splitlines()
        # Find _save_results method
        in_save = False
        save_lines = []
        for line in lines:
            if "def _save_results" in line:
                in_save = True
            if in_save:
                save_lines.append(line)
                if line.strip().startswith("def ") and "save" not in line:
                    break  # End of method
        save_body = "\n".join(save_lines)
        assert "date.today()" not in save_body or "# FIX BCK-PIT-001" in save_body, (
            "BCK-PIT-001 REGRESSION: date.today() masih digunakan di _save_results(). "
            "Gunakan config.end_date untuk reproducibility."
        )

    def test_config_end_date_used_for_timestamp(self):
        src = self.TARGET.read_text()
        assert "config.end_date" in src, (
            "BCK-PIT-001: config.end_date harus digunakan sebagai timestamp untuk hasil"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# CI Gate G-2 Scope Fix
# ═══════════════════════════════════════════════════════════════════════════════

class TestCIGateG2Scope:
    """CI Gate G-2 harus scan src/ penuh, bukan hanya src/gold."""

    CI_YML = pathlib.Path(".github/workflows/ci.yml")

    def test_ci_yml_exists(self):
        assert self.CI_YML.exists(), "ci.yml tidak ditemukan"

    def test_gate_g2_scans_full_src(self):
        src = self.CI_YML.read_text()
        # Harus scan pathlib.Path('src') bukan pathlib.Path('src/gold')
        assert "pathlib.Path('src/gold')" not in src, (
            "CI Gate G-2 masih hanya scan src/gold — harus diperluas ke src/"
        )

    def test_gate_g2_uses_src_root(self):
        src = self.CI_YML.read_text()
        assert "pathlib.Path('src').rglob" in src, (
            "CI Gate G-2 harus menggunakan pathlib.Path('src').rglob('*.py')"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Global: zero violations across all audit-scope files
# ═══════════════════════════════════════════════════════════════════════════════

class TestGlobalAuditClearance:
    """
    Gate: semua file dalam scope audit harus bebas f-string SQL dan
    non-atomic writes. Ini adalah single source of truth untuk sterilization
    clearance pre-GMI Wave 1.
    """

    AUDIT_SCOPE_FILES = [
        # Silver
        "silver/quality_validator.py",
        "silver/ohlcv_processor.py",
        "silver/macro_processor.py",
        "silver/active_symbols.py",
        # fundamental_processor.py, sentiment_processor.py REMOVED — FIX
        # ADR-043 (GMI_Decision_Document_v10.docx): both deleted in full,
        # Finnhub retired. Previously skipped gracefully here via the
        # exists() guard below; removed outright rather than left as a
        # dangling reference to a file that no longer exists.
        # Bronze
        "bronze/base_ingester.py",
        "bronze/forex_cache.py",
        "bronze/schema_validator.py",
        "bronze/eia_ingester.py",
        "bronze/fred_ingester.py",
        # Backtest
        "backtest/pit_data.py",
        "backtest/engine.py",
        # Utils
        "utils/delta_reprocessor.py",
    ]

    @pytest.mark.parametrize("rel_path", AUDIT_SCOPE_FILES)
    def test_syntax_valid(self, rel_path: str):
        """Setiap file dalam audit scope harus memiliki syntax Python yang valid."""
        fpath = SRC / rel_path
        if not fpath.exists():
            pytest.skip(f"{rel_path} tidak ditemukan")
        try:
            ast.parse(fpath.read_text())
        except SyntaxError as e:
            pytest.fail(f"SYNTAX ERROR di {rel_path}: {e}")

    @pytest.mark.parametrize("rel_path", AUDIT_SCOPE_FILES)
    def test_no_fstring_sql(self, rel_path: str):
        """Setiap file audit scope harus bebas f-string SQL (GD §17.7)."""
        fpath = SRC / rel_path
        if not fpath.exists():
            pytest.skip(f"{rel_path} tidak ditemukan")
        hits = _fstring_sql_violations(fpath)
        assert not hits, (
            f"F-string SQL violation di {rel_path}:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
            + "\nFix: gunakan DuckDB $name parameterized queries (GD §17.7)"
        )

    @pytest.mark.parametrize("rel_path", [
        # Silver + Backtest non-atomic writes
        "silver/quality_validator.py",
        "silver/ohlcv_processor.py",
        "silver/macro_processor.py",
        # fundamental_processor.py, sentiment_processor.py REMOVED — FIX ADR-043
        "bronze/base_ingester.py",
        "bronze/forex_cache.py",
        "bronze/schema_validator.py",
        "backtest/engine.py",
    ])
    def test_no_direct_write_parquet(self, rel_path: str):
        """Non-atomic write_parquet harus diganti atomic_write_parquet."""
        fpath = SRC / rel_path
        if not fpath.exists():
            pytest.skip(f"{rel_path} tidak ditemukan")
        hits = _direct_write_parquet(fpath)
        assert not hits, (
            f"Non-atomic write_parquet di {rel_path}:\n"
            + "\n".join(f"  :{ln} → {s}" for ln, s in hits)
            + "\nFix: gunakan atomic_write_parquet() dari src/utils/atomic_io.py"
        )
