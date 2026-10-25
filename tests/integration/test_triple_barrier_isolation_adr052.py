"""
tests/integration/test_triple_barrier_isolation_adr052.py — ADR-052 isolation pins.

triple_barrier.py reads bars AFTER the decision date by design. The distinction from
PITDataLoader (bars strictly BEFORE trade_date) must be a structural fact, not a convention
someone has to remember. Pinned at source level (imports, constants, file/class separation)
and job-graph level (no job, no schedule), in the same style as ADR-051 A2 §A.
"""
import ast
import pathlib

from src.scheduler.job_registry import DAILY_SEQUENCE, JOB_REGISTRY, WEEKLY_SEQUENCE

SRC = pathlib.Path("src")
MODULE = SRC / "backtest" / "triple_barrier.py"
PIT = SRC / "backtest" / "pit_data.py"

# the only src modules the labeler may import
ALLOWED_INTERNAL_IMPORTS = {"src.utils.silver_scope"}


def _imports(path: pathlib.Path) -> list[str]:
    out: list[str] = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = ("." * node.level) + (node.module or "")
            out.append(base)
            out += [f"{base}.{a.name}" for a in node.names]
    return out


def _mentions_labeler(path: pathlib.Path) -> bool:
    text = path.read_text()
    return "triple_barrier" in text or "LookaheadTripleBarrierLabeler" in text


class TestNoLiveConsumer:

    def test_no_src_module_imports_the_labeler(self):
        offenders = [str(p) for p in sorted(SRC.rglob("*.py"))
                     if p != MODULE and any("triple_barrier" in i for i in _imports(p))]
        assert offenders == []

    def test_pit_data_loader_never_references_it(self):
        assert not _mentions_labeler(PIT)

    def test_no_pipeline_job_or_schedule_entry(self):
        assert not any("barrier" in name or "triple" in name for name in JOB_REGISTRY)
        for seq in (DAILY_SEQUENCE, WEEKLY_SEQUENCE):
            assert not any("barrier" in name or "triple" in name for name in seq)
        assert not _mentions_labeler(pathlib.Path("src/scheduler/job_registry.py"))

    def test_gold_silver_bronze_and_scheduler_packages_never_mention_it(self):
        for pkg in ("gold", "silver", "bronze", "scheduler", "utils", "config"):
            for p in sorted((SRC / pkg).rglob("*.py")):
                assert not _mentions_labeler(p), p

    def test_only_scripts_and_tests_consume_it(self):
        consumers = sorted(str(p) for p in pathlib.Path("scripts").rglob("*.py") if _mentions_labeler(p))
        assert consumers == ["scripts/label_triple_barrier.py"]


class TestLabelerStaysSelfContained:

    def test_internal_imports_are_whitelisted(self):
        internal = {i for i in _imports(MODULE) if i.startswith("src")}
        stray = {i for i in internal
                 if not any(i == a or i.startswith(a + ".") for a in ALLOWED_INTERNAL_IMPORTS)}
        assert stray == set(), stray

    def test_does_not_reuse_the_pit_loader(self):
        assert not any("pit_data" in i or "PITDataLoader" in i for i in _imports(MODULE))
        assert "PITDataLoader" not in "".join(
            n.id for n in ast.walk(ast.parse(MODULE.read_text())) if isinstance(n, ast.Name)
        )

    def test_reads_silver_only_and_writes_nothing(self):
        tree = ast.parse(MODULE.read_text())
        strings = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        code_strings = [s for s in strings if "\n" not in s]           # exclude docstrings
        assert not any("data/gold" in s or "data/bronze" in s for s in code_strings)
        attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        assert not attrs & {"write_parquet", "write_csv", "write_json", "sink_parquet"}
        assert "duckdb" not in {i.split(".")[0] for i in _imports(MODULE)}   # no SQL surface at all

    def test_warning_banner_and_class_name(self):
        text = MODULE.read_text()
        assert "LOOKAHEAD BY DESIGN" in text.split('"""')[1]
        assert "class LookaheadTripleBarrierLabeler" in text
