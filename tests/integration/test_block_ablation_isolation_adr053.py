"""
tests/integration/test_block_ablation_isolation_adr053.py — ADR-053 Amendment 2 isolation pins.

scripts/studies/technical_block_ablation.py reuses the production engine ON PURPOSE (an ablation of a
re-implementation would say nothing about the real model) and is therefore the one study script allowed to import
from src.gold.intelligence. What it must NOT be: a pipeline stage, an importer-of-last-resort for src, a writer
into the model stores, or a reader of Silver/Bronze. Pinned on IMPORTS and CALLS (AST), not on text, with a
self-test that the scanners catch the bad spellings.
"""
import ast
import pathlib
import re

import pytest

from src.scheduler.job_registry import DAILY_SEQUENCE, JOB_REGISTRY, WEEKLY_SEQUENCE

SRC = pathlib.Path("src")
SCRIPT = pathlib.Path("scripts/studies/technical_block_ablation.py")
ALLOWED_INTERNAL = {"src.gold.intelligence.core", "src.gold.intelligence.features_technical",
                    "src.gold.intelligence.labels", "src.gold.intelligence.technical_xgboost"}
WRITE_ATTRS = {"write_text", "write_bytes", "write_parquet", "to_parquet", "sink_parquet", "write_csv", "write_json"}


def _tree(source: str) -> ast.AST:
    return ast.parse(source)


def imports_of(source: str) -> list[str]:
    out: list[str] = []
    for node in ast.walk(_tree(source)):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = ("." * node.level) + (node.module or "")
            out.append(base)
            out += [f"{base}.{a.name}" for a in node.names]
    return out


def write_calls_of(source: str) -> list[str]:
    """Calls that write a file: .write_text / .write_parquet / ... or open(..., 'w'/'a'/'x'/'+')."""
    bad: list[str] = []
    for node in ast.walk(_tree(source)):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Attribute) and f.attr in WRITE_ATTRS:
            bad.append(f.attr)
        if isinstance(f, ast.Name) and f.id == "open":
            mode = node.args[1] if len(node.args) > 1 else next((k.value for k in node.keywords if k.arg == "mode"), None)
            if isinstance(mode, ast.Constant) and isinstance(mode.value, str) and set(mode.value) & set("wax+"):
                bad.append("open(write)")
    return bad


def string_constants_of(source: str) -> list[str]:
    return [n.value for n in ast.walk(_tree(source)) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


class TestScannersCatchTheBadSpellings:
    def test_imports(self):
        got = imports_of("import duckdb\nfrom src.scheduler import job_registry\nfrom src.gold.intelligence import core")
        assert "duckdb" in got and "src.scheduler" in got and "src.gold.intelligence.core" in got

    @pytest.mark.parametrize("src", ["Path('x').write_text('y')", "df.write_parquet('p')", "open('f', 'w')",
                                     "open('f', mode='a')", "open('f', 'r+')", "df.to_parquet('p')"])
    def test_write_calls_are_detected(self, src):
        assert write_calls_of(src) != []

    @pytest.mark.parametrize("src", ["open('f')", "open('f', 'r')", "core.write_json_atomic(p, d)", "p.mkdir()"])
    def test_reads_and_the_one_sanctioned_writer_are_not_flagged(self, src):
        assert write_calls_of(src) == []


class TestTheStudyStaysOffline:
    SOURCE = SCRIPT.read_text()

    def test_internal_imports_are_whitelisted(self):
        internal = {i for i in imports_of(self.SOURCE) if i == "src" or i.startswith("src.")}
        stray = {i for i in internal if not any(i == a or i.startswith(a + ".") for a in ALLOWED_INTERNAL)
                 and i not in {"src", "src.gold", "src.gold.intelligence"}}
        assert stray == set(), stray

    def test_no_scheduler_gatekeeper_or_other_domain_is_touched(self):
        names = " ".join(imports_of(self.SOURCE))
        for forbidden in ("scheduler", "gatekeeper", "hmm_xgboost", "hmm", "cross_asset", "pit_data", "silver", "bronze"):
            assert forbidden not in names, forbidden

    def test_no_sql_no_database(self):
        names = " ".join(imports_of(self.SOURCE))
        assert "duckdb" not in names and "sqlite3" not in names

    def test_no_f_string_sql(self):
        for node in ast.walk(_tree(self.SOURCE)):
            if isinstance(node, ast.JoinedStr):
                text = "".join(v.value for v in node.values if isinstance(v, ast.Constant))
                assert not re.search(r"\b(SELECT|FROM|WHERE)\b", text), text

    def test_it_writes_nothing_itself(self):
        assert write_calls_of(self.SOURCE) == []          # the only write is core.write_json_atomic behind --out

    def test_it_names_no_store_or_layer_path(self):
        for s in string_constants_of(self.SOURCE):
            assert "data/gold" not in s and "data/silver" not in s and "data/bronze" not in s, s

    def test_the_only_output_is_an_explicit_out_argument(self):
        assert re.search(r'"--out".*default=None', self.SOURCE)
        assert "core.write_json_atomic" in self.SOURCE


class TestNothingInSrcOrThePipelineRunsIt:
    def test_no_src_module_imports_scripts(self):
        offenders = [str(p) for p in sorted(SRC.rglob("*.py")) if any(i.startswith("scripts") for i in imports_of(p.read_text()))]
        assert offenders == []

    def test_no_job_or_schedule_entry(self):
        names = " ".join(list(JOB_REGISTRY) + DAILY_SEQUENCE + WEEKLY_SEQUENCE)
        assert "ablation" not in names
        assert "technical_block_ablation" not in pathlib.Path("src/scheduler/job_registry.py").read_text()

    def test_src_only_points_at_it_in_documentation_never_in_code(self):
        """The two docstring pointers (features_technical, technical_xgboost) are legitimate; what is pinned is that no
        src module IMPORTS or CALLS it — a text scan would trip on those pointers (the 2026-11-08 lesson)."""
        for p in sorted(SRC.rglob("*.py")):
            tree = _tree(p.read_text())
            doc_nodes = {id(n.body[0].value) for n in ast.walk(tree)
                         if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                         and n.body and isinstance(n.body[0], ast.Expr)
                         and isinstance(n.body[0].value, ast.Constant) and isinstance(n.body[0].value.value, str)}
            in_code = [n.value for n in ast.walk(tree)
                       if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in doc_nodes
                       and "technical_block_ablation" in n.value]
            assert in_code == [], (str(p), in_code)

    def test_the_docstring_exclusion_does_not_hide_a_real_string(self, tmp_path):
        """Self-test: a docstring pointer is excluded, a string in code is not."""
        src = '"""see technical_block_ablation.py"""\nx = "technical_block_ablation"\n'
        tree = _tree(src)
        docs = {id(tree.body[0].value)}
        hits = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)
                and id(n) not in docs and "technical_block_ablation" in n.value]
        assert hits == ["technical_block_ablation"]


class TestTheStudyDoesNotSmuggleTheLabelerInSrc:
    """ADR-052 pin: the labeler module is named only by scripts/label_*.py. This study reads PUBLISHED labels."""
    def test_the_study_never_names_the_labeler(self):
        text = SCRIPT.read_text()
        assert "triple_barrier" not in text and "LookaheadTripleBarrierLabeler" not in text
