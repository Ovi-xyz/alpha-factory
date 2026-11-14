"""
FIX SIL-DEDUP-PERIOD-01 [chat thread, 13 Nov 2026]: scripts/audit_silver_period_duplicates.py — the census
before the fix and the check after it. Read-only; the period comes from the processor's own key.
"""
from __future__ import annotations

import ast
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from audit_silver_period_duplicates import find_period_duplicates, main, silver_files  # noqa: E402


def _silver(root: Path, market: str, sym: str, tf: str, stamps: list[str], tz: str | None = None):
    ts = pl.Series("timestamp", [datetime.fromisoformat(s) for s in stamps], dtype=pl.Datetime("ms"))
    if tz:
        ts = ts.dt.replace_time_zone(tz)
    d = root / market / f"symbol={sym}"
    d.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"timestamp": ts, "close": [1.0] * len(stamps)}).write_parquet(d / f"{sym}_{tf}_silver.parquet")


@pytest.fixture
def tree(tmp_path):
    _silver(tmp_path, "us_stocks", "AAPL", "1M", ["2026-10-01", "2026-11-01", "2026-11-03"])          # the live case
    _silver(tmp_path, "context", "SPX", "1M", ["2026-10-01", "2026-11-01"])                          # clean
    _silver(tmp_path, "us_stocks", "ABBV", "1W", ["2026-08-17", "2026-08-18", "2026-08-24"])        # Mon + Tue, one week
    _silver(tmp_path, "forex", "EUR_USD", "1W", ["2026-11-01", "2026-11-08", "2026-11-15"])         # Sunday-stamped weeks
    _silver(tmp_path, "idx", "BBCA", "1M", ["2026-10-01", "2026-11-01"], tz="Asia/Jakarta")         # UTC-date trap
    _silver(tmp_path, "us_stocks", "AAPL", "1D", ["2026-11-02", "2026-11-02"])                       # 1D is not checked
    return tmp_path


class TestFindPeriodDuplicates:

    def test_finds_exactly_the_duplicated_periods(self, tree):
        findings, read = find_period_duplicates(silver_files(tree, ("1W", "1M")))
        assert read == 5
        got = {(f["market"], f["symbol"], f["timeframe"], str(f["period"]), f["n"]) for f in findings}
        assert got == {
            ("us_stocks", "AAPL", "1M", "2026-11-01", 2),
            ("us_stocks", "ABBV", "1W", "2026-08-17", 2),
        }

    def test_reports_the_stamps_of_the_duplicate(self, tree):
        findings, _ = find_period_duplicates(silver_files(tree, ("1M",)))
        (f,) = findings
        assert [str(s) for s in f["stamps"]] == ["2026-11-01", "2026-11-03"]

    def test_stamps_are_reported_in_order_whatever_the_file_order(self, tmp_path):
        _silver(tmp_path, "us_stocks", "ZZ", "1M", ["2026-11-03", "2026-10-01", "2026-11-01"])
        (f,) = find_period_duplicates(silver_files(tmp_path, ("1M",)))[0]
        assert [str(x) for x in f["stamps"]] == ["2026-11-01", "2026-11-03"]

    def test_sunday_stamped_weeks_and_local_dates_are_not_false_positives(self, tree):
        findings, _ = find_period_duplicates(silver_files(tree, ("1W", "1M")))
        assert not [f for f in findings if f["symbol"] in ("EUR_USD", "BBCA", "SPX")]

    def test_daily_files_are_never_read(self, tree):
        assert all(tf != "1D" for _, _, tf, _ in silver_files(tree, ("1W", "1M")))

    def test_an_unreadable_file_is_a_finding_not_a_skip(self, tmp_path):
        d = tmp_path / "us_stocks" / "symbol=BAD"
        d.mkdir(parents=True)
        (d / "BAD_1M_silver.parquet").write_bytes(b"not parquet")
        findings, read = find_period_duplicates(silver_files(tmp_path, ("1M",)))
        assert read == 0 and len(findings) == 1 and findings[0]["period"] is None and findings[0]["error"]


class TestMain:

    def test_exit_1_and_report_when_duplicates_exist(self, tree, capsys):
        assert main(["--silver-root", str(tree)]) == 1
        out = capsys.readouterr().out
        assert "1M: 1 symbol(s)" in out and "1W: 1 symbol(s)" in out
        assert "us_stocks/AAPL 1M 2026-11-01: 2 bars stamped 2026-11-01, 2026-11-03" in out
        assert "period 2026-11-01: 1 symbol(s)" in out

    def test_exit_0_on_a_clean_tree(self, tmp_path, capsys):
        _silver(tmp_path, "context", "SPX", "1M", ["2026-10-01", "2026-11-01"])
        assert main(["--silver-root", str(tmp_path)]) == 0
        assert "0 symbol(s)" in capsys.readouterr().out

    def test_tf_and_market_filters(self, tree, capsys):
        assert main(["--silver-root", str(tree), "--tf", "1W"]) == 1
        out = capsys.readouterr().out
        assert "1M:" not in out and "ABBV" in out and "AAPL" not in out
        assert main(["--silver-root", str(tree), "--market", "context"]) == 0

    def test_an_unreadable_file_fails_the_run(self, tmp_path, capsys):
        d = tmp_path / "us_stocks" / "symbol=BAD"
        d.mkdir(parents=True)
        (d / "BAD_1M_silver.parquet").write_bytes(b"not parquet")
        assert main(["--silver-root", str(tmp_path)]) == 1
        assert "UNREADABLE" in capsys.readouterr().out

    def test_limit_caps_the_examples(self, tmp_path, capsys):
        for i in range(5):
            _silver(tmp_path, "us_stocks", f"S{i}", "1M", ["2026-11-01", "2026-11-03"])
        main(["--silver-root", str(tmp_path), "--limit", "2"])
        out = capsys.readouterr().out
        assert out.count("e.g. ") == 2 and "1M: 5 symbol(s)" in out

    def test_period_key_is_the_processors(self):
        """One implementation: the script imports the processor's key, it does not re-derive it."""
        import audit_silver_period_duplicates as mod
        import src.silver.ohlcv_processor as proc
        assert mod.period_key_expr is proc.period_key_expr and mod._PERIOD_DEDUP_TIMEFRAMES is proc._PERIOD_DEDUP_TIMEFRAMES


def test_no_src_module_imports_the_audit_script():
    """Offline utility: nothing in src/ may import it (AST on imports, not a name scan)."""
    root = Path(__file__).resolve().parents[2] / "src"
    offenders = []
    for py in root.rglob("*.py"):
        tree = ast.parse(py.read_text())
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""] + [a.name for a in node.names]
            if any("audit_silver_period_duplicates" in n for n in names):
                offenders.append(str(py))
    assert offenders == []
