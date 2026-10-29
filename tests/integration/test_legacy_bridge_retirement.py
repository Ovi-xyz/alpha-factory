"""
tests/integration/test_legacy_bridge_retirement.py — retirement of
src/gold/cross_asset/legacy_correlation_bridge.py (29 Oct 2026, no version bump;
KNOWN_RISKS.md RISK-31 update 29 Oct 2026).

The bridge existed only to keep gold_correlation's two consumers working unchanged by writing
data/gold/correlation/correlation_clusters.parquet. Both now read cross_asset_corr.parquet
directly (screener) or are gone (v_correlation), so "the bridge cannot come back by accident" and
"nothing reads the retired file" are pinned structurally:

  * the module lives in the archive, not under src/, and nothing imports it
  * no executable string in src/ or scripts/ names the retired output file or its directory
    (docstrings are exempt — they explain the retirement)
  * gold_cross_asset_correlation's run() writes nothing under data/gold/correlation
  * the screener's cluster source is CorrelationModule's own output path (single source of truth)
"""
import ast
import pathlib
from datetime import date

import polars as pl

import src.gold.cross_asset.correlation_module as corr_mod
import src.gold.cross_asset.history as hist
import src.gold.screener as scr_mod

SRC = pathlib.Path("src")
SCRIPTS = pathlib.Path("scripts")
ARCHIVE = pathlib.Path("archive/legacy_correlation_bridge_2026_10")
MODULE = "legacy_correlation_bridge"
RETIRED_FRAGMENTS = ("correlation_clusters.parquet", "data/gold/correlation")


def _py_files(root: pathlib.Path):
    # scripts/archive holds retired scripts that are not on any live path
    return [p for p in sorted(root.rglob("*.py")) if "archive" not in p.parts] if root.exists() else []


def _docstring_nodes(tree: ast.AST) -> set:
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant):
                out.add(id(body[0].value))
    return out


class TestBridgeIsArchivedAndUnreferenced:

    def test_module_lives_in_the_archive_only(self):
        assert not (SRC / "gold" / "cross_asset" / f"{MODULE}.py").exists()
        assert (ARCHIVE / "src" / "gold" / "cross_asset" / f"{MODULE}.py").exists()
        assert (ARCHIVE / "tests" / "unit" / f"test_{MODULE}.py").exists()
        assert (ARCHIVE / "README.md").exists()

    def test_nothing_imports_the_bridge(self):
        offenders = []
        for path in _py_files(SRC) + _py_files(SCRIPTS) + _py_files(pathlib.Path("tests")):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                names = []
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""] + [a.name for a in node.names]
                if any(MODULE in n or n == "write_legacy_correlation_clusters" for n in names):
                    offenders.append(str(path))
        assert offenders == [], offenders

    def test_no_executable_string_names_the_retired_file(self):
        offenders = []
        for path in _py_files(SRC) + _py_files(SCRIPTS):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            docs = _docstring_nodes(tree)
            for node in ast.walk(tree):
                if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docs:
                    if any(f in node.value for f in RETIRED_FRAGMENTS):
                        offenders.append(f"{path}: {node.value[:80]!r}")
        assert offenders == [], offenders


class TestCorrelationRunWritesOnlyItsOwnOutput:

    def test_run_creates_nothing_under_data_gold_correlation(self, tmp_path, monkeypatch):
        d = date(2026, 9, 27)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(hist, "HISTORY_ROOT", tmp_path / "h")
        monkeypatch.setattr(corr_mod, "GOLD_CROSS_ASSET_PATH", tmp_path / "s")
        monkeypatch.setattr(corr_mod, "CROSS_ASSET_CORR_PATH", tmp_path / "s" / "cross_asset_corr.parquet")
        frame = pl.DataFrame({
            "symbol_a": ["A", "A", "B"], "symbol_b": ["B", "C", "C"], "correlation": [0.5, 0.4, 0.3],
            "regime": ["mixed"] * 3, "computation_date": [str(d)] * 3,
            "cluster_id_a": [1, 1, 1], "cluster_id_b": [1, 1, 1],
        })
        monkeypatch.setattr(corr_mod.CorrelationModule, "compute", lambda self, rd: frame)
        corr_mod.run(d)
        assert (tmp_path / "s" / "cross_asset_corr.parquet").exists()
        assert not (tmp_path / "data" / "gold" / "correlation").exists()


class TestScreenerClusterSource:

    def test_screener_reads_correlation_modules_own_output(self):
        """One definition of where the pairwise store lives."""
        assert scr_mod.CROSS_ASSET_CORR_PATH == corr_mod.CROSS_ASSET_CORR_PATH
        assert not hasattr(scr_mod, "GOLD_CORR_PATH")
