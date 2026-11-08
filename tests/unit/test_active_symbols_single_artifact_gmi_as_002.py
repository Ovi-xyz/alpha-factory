"""
GMI-AS-002 — the legacy ``active_{date}.parquet`` is retired; ``active_ohlcv_{date}.parquet`` is the
ONLY dated artifact of ``ActiveSymbolsResolver``.

Pins (each one is mutation-checked — see the dev-log):
  * resolve() and the cold-start fallback write exactly one dated file (+ the GHR-14 membership log);
  * load() (deprecated alias), load_ohlcv() and load_full() all read that file;
  * ``v_active_symbols`` no longer double-counts: its old glob ``active_*.parquet`` also matched
    ``active_ohlcv_*.parquet``;
  * the dashboard freshness probe looks at the canonical file only;
  * no string literal in ``src/`` builds or globs the legacy name (AST scan, docstrings exempt).
"""

from __future__ import annotations

import ast
import re
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pytest

from src.silver.active_symbols import RESOLVER_VERSION, ActiveSymbolsResolver

REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_DATE = date(2025, 3, 1)


# ── fixtures ──────────────────────────────────────────────────────────────────

def _silver_1d(tmp_path: Path) -> str:
    """Minimal Silver 1D parquet: one liquid US stock, one IDX stock, one forex always-in symbol."""
    configs = [
        ("AAPL", 152.0, 50_000_000),
        ("BBCA", 8_500.0, 200_000_000),
        ("EUR_USD", 1.08, 0),
    ]
    rows = [
        {"symbol": sym, "timestamp": date(2025, 1, 2) + timedelta(days=i),
         "close": close + i * 0.01, "volume": vol, "is_clean": True}
        for sym, close, vol in configs for i in range(45)
    ]
    out = tmp_path / "silver_1d" / "data.parquet"
    out.parent.mkdir(parents=True)
    pl.DataFrame(rows).with_columns(pl.col("timestamp").cast(pl.Date)).write_parquet(out)
    return str(out.parent / "*.parquet")


def _resolver(tmp_path: Path, monkeypatch) -> tuple[ActiveSymbolsResolver, Path]:
    monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
    resolver = ActiveSymbolsResolver()
    out = tmp_path / "silver" / "active_symbols"
    monkeypatch.setattr(type(resolver), "OUTPUT_PATH", property(lambda s: out))
    return resolver, out


def _loader_mock() -> MagicMock:
    mi = MagicMock()
    mi.market_map.return_value = {"AAPL": "us_stocks", "BBCA": "idx", "EUR_USD": "forex"}
    mi.symbol_list.return_value = ["AAPL", "BBCA", "EUR_USD"]
    mi.all_context.return_value = []
    return mi


def _resolve(tmp_path: Path, monkeypatch, silver: str | None = None) -> tuple[ActiveSymbolsResolver, Path, list[str]]:
    resolver, out = _resolver(tmp_path, monkeypatch)
    with patch("src.silver.active_symbols.get_loader", return_value=_loader_mock()):
        symbols = resolver.resolve(silver if silver is not None else _silver_1d(tmp_path), RUN_DATE)
    return resolver, out, symbols


def _dated_files(out: Path) -> list[str]:
    """Every file whose name starts with `active` — the dated artifacts, whatever their spelling."""
    return sorted(p.name for p in out.glob("active*"))


# ── writers ───────────────────────────────────────────────────────────────────

class TestSingleDatedArtifact:

    def test_resolve_writes_only_the_canonical_file(self, tmp_path, monkeypatch):
        _, out, symbols = _resolve(tmp_path, monkeypatch)
        assert symbols                                                       # non-vacuous
        assert _dated_files(out) == [f"active_ohlcv_{RUN_DATE.isoformat()}.parquet"]

    def test_the_membership_log_is_still_written_beside_it(self, tmp_path, monkeypatch):
        _, out, _ = _resolve(tmp_path, monkeypatch)
        assert (out / "membership_log.parquet").exists()                     # GHR-14 is untouched

    def test_cold_start_fallback_writes_only_the_canonical_file(self, tmp_path, monkeypatch):
        resolver, out = _resolver(tmp_path, monkeypatch)
        with patch("src.silver.active_symbols.get_loader", return_value=_loader_mock()):
            symbols = resolver.resolve(str(tmp_path / "nothing" / "*.parquet"), RUN_DATE)   # Silver not ready
        assert symbols == ["AAPL", "BBCA", "EUR_USD"]
        assert _dated_files(out) == [f"active_ohlcv_{RUN_DATE.isoformat()}.parquet"]
        assert resolver.load_full(RUN_DATE)["is_fallback"].to_list() == [True, True, True]

    def test_output_schema_and_version_are_unchanged_apart_from_the_bump(self, tmp_path, monkeypatch):
        resolver, _, _ = _resolve(tmp_path, monkeypatch)
        df = resolver.load_full(RUN_DATE)
        assert set(df.columns) == {
            "symbol", "market", "dollar_volume_20d", "clean_days", "last_close", "eligibility_reason",
            "resolved_date", "resolver_version", "unknown_market_count", "is_fallback",
        }
        assert set(df["resolver_version"].to_list()) == {RESOLVER_VERSION} == {"1.3"}

    def test_a_pre_existing_legacy_file_is_left_alone(self, tmp_path, monkeypatch):
        """The resolver never deletes anything: a stray legacy file is inert, archived by the script."""
        resolver, out = _resolver(tmp_path, monkeypatch)
        out.mkdir(parents=True)
        stray = out / f"active_{RUN_DATE.isoformat()}.parquet"
        pl.DataFrame({"symbol": ["OLD"]}).write_parquet(stray)
        with patch("src.silver.active_symbols.get_loader", return_value=_loader_mock()):
            resolver.resolve(_silver_1d(tmp_path), RUN_DATE)
        assert pl.read_parquet(stray)["symbol"].to_list() == ["OLD"]         # untouched ...
        assert "OLD" not in resolver.load_ohlcv(RUN_DATE)                    # ... and never read back


# ── loaders ───────────────────────────────────────────────────────────────────

class TestLoadersReadTheCanonicalFile:

    def test_all_three_loaders_agree_with_the_canonical_file(self, tmp_path, monkeypatch):
        resolver, out, symbols = _resolve(tmp_path, monkeypatch)
        on_disk = pl.read_parquet(out / f"active_ohlcv_{RUN_DATE.isoformat()}.parquet")["symbol"].to_list()
        assert sorted(on_disk) == sorted(symbols) != []
        assert resolver.load(RUN_DATE) == on_disk
        assert resolver.load_ohlcv(RUN_DATE) == on_disk
        assert resolver.load_full(RUN_DATE)["symbol"].to_list() == on_disk

    def test_a_legacy_only_directory_counts_as_not_resolved(self, tmp_path, monkeypatch):
        """With only the retired file on disk, every loader must say 'not resolved' — no silent read."""
        resolver, out = _resolver(tmp_path, monkeypatch)
        out.mkdir(parents=True)
        pl.DataFrame({"symbol": ["OLD"]}).write_parquet(out / f"active_{RUN_DATE.isoformat()}.parquet")
        for loader in (resolver.load, resolver.load_ohlcv, resolver.load_full):
            with pytest.raises(FileNotFoundError):
                loader(RUN_DATE)

    def test_load_is_a_pure_alias_of_load_ohlcv(self, tmp_path, monkeypatch):
        resolver, _, _ = _resolve(tmp_path, monkeypatch)
        with patch.object(ActiveSymbolsResolver, "load_ohlcv", return_value=["SENTINEL"]) as spy:
            assert resolver.load(RUN_DATE) == ["SENTINEL"]
        spy.assert_called_once_with(RUN_DATE)


# ── v_active_symbols (the double-count defect) ────────────────────────────────

class TestActiveSymbolsView:

    @staticmethod
    def _rows_through_the_view(tmp_path: Path, monkeypatch) -> int:
        from src.gold.views import VIEW_DEFINITIONS

        monkeypatch.chdir(tmp_path)                                  # the view SQL uses repo-relative paths
        with duckdb.connect() as con:
            con.execute(VIEW_DEFINITIONS["v_active_symbols"])
            return con.execute("SELECT COUNT(*) FROM v_active_symbols").fetchone()[0]

    def test_view_counts_each_symbol_once_even_beside_a_stray_legacy_twin(self, tmp_path, monkeypatch):
        out = tmp_path / "data" / "silver" / "active_symbols"
        out.mkdir(parents=True)
        frame = pl.DataFrame({"symbol": ["AAPL", "BBCA", "EUR_USD"], "dollar_volume_20d": [1.0, 2.0, 3.0]})
        frame.write_parquet(out / "active_ohlcv_2026-10-16.parquet")
        frame.write_parquet(out / "active_2026-10-16.parquet")      # the retired twin, still on disk
        assert self._rows_through_the_view(tmp_path, monkeypatch) == 3        # not 6

    def test_view_spans_every_canonical_date(self, tmp_path, monkeypatch):
        out = tmp_path / "data" / "silver" / "active_symbols"
        out.mkdir(parents=True)
        frame = pl.DataFrame({"symbol": ["AAPL", "BBCA"], "dollar_volume_20d": [1.0, 2.0]})
        for d in ("2026-10-16", "2026-10-17"):
            frame.write_parquet(out / f"active_ohlcv_{d}.parquet")
        assert self._rows_through_the_view(tmp_path, monkeypatch) == 4


# ── dashboard freshness probe ─────────────────────────────────────────────────

class TestDashboardFreshness:

    @staticmethod
    def _active_line(capsys) -> str:
        from src.utils import pipeline_dashboard as dash

        dash._section_data_freshness()
        lines = [ln for ln in capsys.readouterr().out.splitlines() if "Active Symbols" in ln]
        assert len(lines) == 1
        return lines[0]

    def test_a_legacy_only_directory_reads_as_not_found(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        out = tmp_path / "data" / "silver" / "active_symbols"
        out.mkdir(parents=True)
        pl.DataFrame({"symbol": ["A"]}).write_parquet(out / "active_2026-10-16.parquet")
        assert "not found" in self._active_line(capsys)

    def test_the_canonical_file_is_found(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        out = tmp_path / "data" / "silver" / "active_symbols"
        out.mkdir(parents=True)
        pl.DataFrame({"symbol": ["A"]}).write_parquet(out / "active_ohlcv_2026-10-16.parquet")
        assert "not found" not in self._active_line(capsys)


# ── source pin: nothing in src/ builds or globs the legacy name ───────────────

_LEGACY = re.compile(r"(?<![A-Za-z0-9_])active_(\{\}|\*|\d)")


def _docstring_nodes(tree: ast.AST) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                ids.add(id(body[0].value))
    return ids


def _string_literals(source: str) -> list[str]:
    """Every non-docstring string literal; an f-string is flattened with `{}` for each placeholder."""
    tree = ast.parse(source)
    skip = _docstring_nodes(tree)
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            found.append("".join(
                v.value if isinstance(v, ast.Constant) and isinstance(v.value, str) else "{}"
                for v in node.values
            ))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in skip:
            found.append(node.value)
    return found


class TestNoLegacyNameInSource:

    def test_the_scanner_catches_every_spelling_it_exists_to_catch(self):
        """Self-test: a pin that cannot fail is worthless."""
        for bad in (
            'x = f"active_{run_date.isoformat()}.parquet"',
            'x = "data/silver/active_symbols/active_*.parquet"',
            'x = "active_2026-10-16.parquet"',
            "x = f'active_{d}.parquet'",
        ):
            assert any(_LEGACY.search(s) for s in _string_literals(bad)), bad
        for ok in (
            'x = f"active_ohlcv_{run_date.isoformat()}.parquet"',
            'x = "active_ohlcv_*.parquet"',
            'x = "active_context_2026.parquet"',
            'def f():\n    """mentions active_{date}.parquet in prose"""\n    return 1',
        ):
            assert not any(_LEGACY.search(s) for s in _string_literals(ok)), ok

    def test_no_src_literal_builds_or_globs_the_legacy_name(self):
        offenders = []
        for path in sorted((REPO_ROOT / "src").rglob("*.py")):
            for lit in _string_literals(path.read_text(encoding="utf-8")):
                if _LEGACY.search(lit):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}: {lit!r}")
        assert offenders == [], "legacy active_{date}.parquet is retired (GMI-AS-002):\n" + "\n".join(offenders)
