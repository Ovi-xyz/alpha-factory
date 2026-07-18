"""
tests/unit/test_fstring_sql_absence.py — Regression guard untuk GLD-003 + RISK-3.

FIX GLD-003 (audit sebelumnya): F-string SQL anti-pattern di Gold layer.
Audit menemukan 10 lokasi f-string SQL di src/gold/ dan src/bronze/:
    - technical_signals.py (2 × _get_latest_vix, 1 × _process_timeframe)
    - mtf_alignment.py (1 × _compute_mtf_alignment, 1 × _apply_regime_compatible)
    - screener.py (1 × _check_data_freshness, 1 × build_watchlist, 1 × _deduplicate)
    - correlation_matrix.py (1 × compute_correlation_matrix)
    - hmm_regime.py (1 × _load_features)

FIX GMI-AUD-001/002 (audit RISK-3, GMI Wave 1 Cycle 3): dua lokasi TAMBAHAN
ditemukan yang TIDAK tercakup GLD-003's scope — sector_rotation.py:193
(_get_active_regime, value interpolation, di-fix ke $name binding) dan
views.py:182+196 (register_views/list_available_views, IDENTIFIER
interpolation — tidak bisa $name-bind, di-fix via _quoted_identifier()
validated helper). KNOWN_RISKS.md RISK-3 sekarang CLOSED.

Root cause GLD-003 tidak menemukan keduanya: docstring test-test di bawah
ini (sebelum fix ini) secara eksplisit menyatakan quality_validator.py,
macro_processor.py, dan Bronze ingester selain bea_ingester.py "di luar
scope" — DAN scanner _scan_fstring_sql_violations() yang lama hanya
mendeteksi f-string berpembuka TIGA tanda kutip ganda (bukan satu) —
sector_rotation.py dan views.py memakai SINGLE-quote f-string (f"...")
yang bahkan tidak akan terdeteksi scanner lama itu SEKALIPUN filenya ada
di daftar scope.

Semua harus dikonversi ke $name parameterized queries (GD §17.7), KECUALI
untuk identifier (nama tabel/view) yang secara fundamental tidak bisa
di-bind sebagai parameter di SQL engine manapun — untuk kasus itu, pattern
yang benar adalah validated+quoted identifier via string concatenation
biasa (bukan f-string), bukan $name binding yang mustahil secara teknis.

Test ini adalah REGRESSION GUARD — jika f-string SQL muncul kembali
di src/, test ini akan gagal dan CI Gate G-2 juga akan menangkapnya.

CI/CD Guide §5 Table 6:
    Gate G-2 blocking pada setiap push — mencegah NEW anti-pattern.
    Test ini adds unit-level verification di atas CI-level grep.
"""

from __future__ import annotations

import ast
import pathlib
import re
import sys
from typing import Iterator

import pytest

# ── Source scan configuration ──────────────────────────────────────────────────

SRC_ROOT = pathlib.Path("src")

# SQL keywords yang menjadi target pattern (GD §17.7)
SQL_KEYWORDS = re.compile(
    r"\b(SELECT|FROM\s+read_parquet|INSERT\s+INTO|UPDATE\s+\w|DELETE\s+FROM)\b",
    re.IGNORECASE,
)

# F-string openers: f""", f''', f", f' (semua varian)
# Triple-quote adalah main vector yang sebelumnya lolos grep sederhana (GLD-006)
FSTRING_OPENERS = re.compile(r'[fF](?:"""|\'\'\')') 


def _scan_fstring_sql_violations(root: pathlib.Path) -> list[tuple[pathlib.Path, int, str]]:
    """
    Scan semua .py files di root untuk f-string SQL (TRIPLE-QUOTE saja).

    NOTE: fungsi ini dipertahankan untuk kompatibilitas dengan test
    spot-check yang sudah ada di bawah (semuanya menyasar file yang memang
    memakai triple-quote f-string). Untuk coverage penuh single-quote DAN
    triple-quote di SELURUH src/, lihat _scan_fstring_sql_violations_ast()
    — precision-based (AST), dipakai oleh TestNoFStringSQLAnywhereInSrc.
    """
    violations = []
    for py_file in sorted(root.rglob("*.py")):
        try:
            src = py_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        for m in FSTRING_OPENERS.finditer(src):
            # Ambil 400 char setelah opener untuk multi-line SQL
            snippet = src[m.start() : m.start() + 400]
            if SQL_KEYWORDS.search(snippet):
                line_no = src[: m.start()].count("\n") + 1
                violations.append((py_file, line_no, snippet[:120].replace("\n", " ")))

    return violations


def _scan_fstring_sql_violations_ast(root: pathlib.Path) -> list[tuple[pathlib.Path, int, str]]:
    """
    ADD GMI-AUD-003 — AST-based scan (precise, whole-repo, single- AND
    triple-quote f-strings).

    Untuk setiap f-string (ast.JoinedStr) di setiap .py file di bawah root,
    flag jika BAGIAN LITERAL (bukan interpolasi) f-string itu SENDIRI
    mengandung SQL keyword — bukan sekadar berada dalam N karakter dari SQL
    asli di tempat lain dalam fungsi yang sama.

    Lebih presisi dari scan berbasis character-window
    (_scan_fstring_sql_violations di atas): window-based scan menghasilkan
    false positive setiap kali f-string log message kebetulan berada dalam
    jarak N karakter dari SQL asli yang SUDAH diparameterisasi dengan benar
    di fungsi yang sama — dikonfirmasi empiris saat audit RISK-3: 7 false
    positive lintas pit_data.py, mtf_alignment.py, screener.py,
    fundamental_processor.py, delta_reprocessor.py, health_reporter.py,
    progress_checkpoint.py — semua diverifikasi manual aman satu per satu
    dengan membaca kode sekitarnya sebelum disimpulkan sebagai false positive.

    Divalidasi sebelum dipakai sebagai regression guard: (1) menghasilkan
    NOL violation terhadap src/ tree SETELAH fix GMI-AUD-001/002 — cocok
    dengan verifikasi manual; (2) mendeteksi contoh sintetis dengan benar.
    """
    violations = []
    for py_file in sorted(root.rglob("*.py")):
        try:
            src = py_file.read_text(encoding="utf-8", errors="replace")
            tree = ast.parse(src, filename=str(py_file))
        except (OSError, SyntaxError):
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.JoinedStr):
                continue
            literal_text = "".join(
                part.value for part in node.values if isinstance(part, ast.Constant)
            )
            if SQL_KEYWORDS.search(literal_text):
                violations.append((py_file, node.lineno, literal_text[:120]))

    return violations


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestNoFStringSQLInSourceCode:
    """GLD-003: src/ harus bebas f-string SQL (GD §17.7 anti-pattern)."""

    def test_no_fstring_sql_in_gold_layer(self):
        """
        Scan src/gold/ untuk f-string SQL violations.
        Setelah FIX GLD-003, semua 10 violations harus sudah dikonversi.
        """
        violations = _scan_fstring_sql_violations(SRC_ROOT / "gold")
        if violations:
            msgs = [
                f"  {p}:{ln} → {snip!r}"
                for p, ln, snip in violations
            ]
            pytest.fail(
                f"GLD-003 REGRESSION: {len(violations)} f-string SQL ditemukan di src/gold/:\n"
                + "\n".join(msgs)
                + "\n\nSemua SQL harus menggunakan $name parameterized queries (GD §17.7)."
            )

    def test_no_fstring_sql_in_bronze_layer(self):
        """
        Scan src/bronze/bea_ingester.py untuk f-string SQL violations
        (GLD-003's original targeted scope — kept as a specific spot-check).
        Bronze ingester LAIN sekarang tercakup oleh
        TestNoFStringSQLAnywhereInSrc di bawah (AST-based, whole-src/ scan,
        ADD GMI-AUD-003) — TIDAK lagi "di luar scope" seperti sebelumnya.
        """
        # GLD-003 audit scope: Gold layer + bea_ingester (terkait GLD-001 fix)
        bea_path = pathlib.Path("src/bronze/bea_ingester.py")
        violations = _scan_fstring_sql_violations(bea_path) if bea_path.exists() else []
        if violations:
            msgs = [f"  {p}:{ln} → {snip!r}" for p, ln, snip in violations]
            pytest.fail(
                f"GLD-003: {len(violations)} f-string SQL di bea_ingester.py:\n"
                + "\n".join(msgs)
            )

    def test_no_fstring_sql_in_silver_layer(self):
        """
        Scan src/silver/active_symbols.py (dalam scope Supplementary Design G2,
        kept as a specific spot-check). quality_validator.py dan
        macro_processor.py sekarang tercakup oleh TestNoFStringSQLAnywhereInSrc
        di bawah (AST-based, whole-src/ scan, ADD GMI-AUD-003) — TIDAK lagi
        "di luar scope" seperti sebelumnya. (Audit RISK-3 sudah memverifikasi
        keduanya BERSIH — tidak ada f-string SQL literal, hanya SQL yang
        sudah correctly parameterized menggunakan $name/? binding.)
        """
        # Hanya cek active_symbols.py yang terkait langsung dengan G2 audit
        target = pathlib.Path("src/silver/active_symbols.py")
        violations = _scan_fstring_sql_violations(target) if target.exists() else []
        if violations:
            msgs = [f"  {p}:{ln} → {snip!r}" for p, ln, snip in violations]
            pytest.fail(
                f"GLD-003: {len(violations)} f-string SQL di active_symbols.py:\n"
                + "\n".join(msgs)
            )

    def test_technical_signals_no_fstring_sql(self):
        """Spot check: technical_signals.py (3 violations pre-fix)."""
        violations = _scan_fstring_sql_violations(
            SRC_ROOT / "gold" / "technical_signals.py"
            if (SRC_ROOT / "gold" / "technical_signals.py").exists()
            else SRC_ROOT / "gold"
        )
        tech_violations = [
            (p, ln, s) for p, ln, s in violations
            if "technical_signals" in str(p)
        ]
        assert len(tech_violations) == 0, (
            f"GLD-003: technical_signals.py masih mengandung f-string SQL:\n"
            + "\n".join(f"  :{ln} → {s!r}" for _, ln, s in tech_violations)
        )

    def test_mtf_alignment_no_fstring_sql(self):
        """Spot check: mtf_alignment.py (2 violations pre-fix)."""
        violations = _scan_fstring_sql_violations(SRC_ROOT / "gold")
        mtf_violations = [
            (p, ln, s) for p, ln, s in violations
            if "mtf_alignment" in str(p)
        ]
        assert len(mtf_violations) == 0, (
            f"GLD-003: mtf_alignment.py masih mengandung f-string SQL:\n"
            + "\n".join(f"  :{ln} → {s!r}" for _, ln, s in mtf_violations)
        )

    def test_screener_no_fstring_sql(self):
        """Spot check: screener.py (3 violations pre-fix)."""
        violations = _scan_fstring_sql_violations(SRC_ROOT / "gold")
        scr_violations = [
            (p, ln, s) for p, ln, s in violations
            if "screener" in str(p)
        ]
        assert len(scr_violations) == 0, (
            f"GLD-003: screener.py masih mengandung f-string SQL:\n"
            + "\n".join(f"  :{ln} → {s!r}" for _, ln, s in scr_violations)
        )

    def test_correlation_matrix_no_fstring_sql(self):
        """Spot check: correlation_matrix.py (1 violation + symbol injection pre-fix)."""
        violations = _scan_fstring_sql_violations(SRC_ROOT / "gold")
        corr_violations = [
            (p, ln, s) for p, ln, s in violations
            if "correlation_matrix" in str(p)
        ]
        assert len(corr_violations) == 0, (
            f"GLD-003: correlation_matrix.py masih mengandung f-string SQL:\n"
            + "\n".join(f"  :{ln} → {s!r}" for _, ln, s in corr_violations)
        )

    def test_hmm_regime_no_fstring_sql(self):
        """Spot check: hmm_regime.py (1 violation pre-fix)."""
        violations = _scan_fstring_sql_violations(SRC_ROOT / "gold")
        hmm_violations = [
            (p, ln, s) for p, ln, s in violations
            if "hmm_regime" in str(p)
        ]
        assert len(hmm_violations) == 0, (
            f"GLD-003: hmm_regime.py masih mengandung f-string SQL:\n"
            + "\n".join(f"  :{ln} → {s!r}" for _, ln, s in hmm_violations)
        )


class TestSymbolInjectionFixed:
    """
    GLD-003 extended: correlation_matrix.py juga melakukan symbol list
    injection via f"symbol IN ({', '.join(f\"'{s}'\" for s in active_symbols)})".
    Verifikasi bahwa pattern ini juga tidak ada (diganti register Arrow table).
    """

    def test_no_symbol_string_injection_in_correlation_matrix(self):
        """f-string SQL injection via symbol list tidak boleh ada."""
        corr_file = SRC_ROOT / "gold" / "correlation_matrix.py"
        if not corr_file.exists():
            pytest.skip("correlation_matrix.py tidak ditemukan")

        src = corr_file.read_text(encoding="utf-8")

        # Pattern lama: "symbol IN ({symbols_sql})"  atau  "IN ({...'}" (injection)
        injection_pattern = re.compile(
            r"IN\s*\(\s*\{.*?symbols.*?\}\s*\)",
            re.DOTALL | re.IGNORECASE,
        )
        assert not injection_pattern.search(src), (
            "GLD-003: symbol list injection pattern masih ada di correlation_matrix.py. "
            "Gunakan Arrow table registration: con.register('active_symbols_tbl', df)"
        )


class TestNoFStringSQLAnywhereInSrc:
    """
    ADD GMI-AUD-003 — RISK-3 closure. AST-based, whole-src/ tree, single-
    AND triple-quote coverage — this is the permanent structural fix for
    the "known gap, addressed piecemeal, file by file" pattern that let
    sector_rotation.py and views.py go unaudited for as long as they did.
    Any NEW f-string SQL anywhere in src/ (not just Gold, not just the
    specific files earlier audits happened to name) now fails this test.
    """

    def test_no_fstring_sql_anywhere_in_src(self):
        violations = _scan_fstring_sql_violations_ast(SRC_ROOT)
        if violations:
            msgs = [f"  {p}:{ln} → {snip!r}" for p, ln, snip in violations]
            pytest.fail(
                f"{len(violations)} f-string SQL ditemukan di src/ "
                f"(AST-based whole-tree scan, GD §17.7):\n"
                + "\n".join(msgs)
                + "\n\nGunakan $name parameterized binding untuk VALUE, atau "
                "validated+quoted identifier via concatenation biasa untuk "
                "IDENTIFIER (nama tabel/view — tidak bisa di-$name-bind di "
                "SQL engine manapun, lihat src/gold/views.py::_quoted_identifier "
                "untuk pattern yang benar)."
            )

    def test_ast_scanner_detects_synthetic_violation(self, tmp_path):
        """Regression guard for the scanner itself — proves it actually
        detects a real violation, not just 'always returns empty'."""
        bad_file = tmp_path / "bad.py"
        bad_file.write_text(
            'def f(path):\n'
            '    con.execute(f"SELECT * FROM read_parquet(\'{path}\')")\n'
        )
        violations = _scan_fstring_sql_violations_ast(tmp_path)
        assert len(violations) == 1
        assert violations[0][0] == bad_file

    def test_ast_scanner_does_not_flag_parameterized_queries(self, tmp_path):
        """A properly $name-parameterized query in a plain (non-f) string
        must NOT be flagged."""
        good_file = tmp_path / "good.py"
        good_file.write_text(
            'def f(path):\n'
            '    con.execute("SELECT * FROM read_parquet($path)", {"path": path})\n'
        )
        violations = _scan_fstring_sql_violations_ast(tmp_path)
        assert violations == []

    def test_ast_scanner_does_not_flag_nearby_unrelated_fstrings(self, tmp_path):
        """The specific false-positive shape confirmed during the RISK-3
        audit: an f-string LOG message sitting near (but not containing)
        genuine parameterized SQL in the same function must not be flagged
        — this is exactly what the old character-window scanner got wrong."""
        ok_file = tmp_path / "ok.py"
        ok_file.write_text(
            'def f(path, tf):\n'
            '    logger.debug(f"[thing] processing TF={tf}")\n'
            '    con.execute(\n'
            '        "SELECT * FROM read_parquet($path)",\n'
            '        {"path": path},\n'
            '    )\n'
        )
        violations = _scan_fstring_sql_violations_ast(tmp_path)
        assert violations == []
