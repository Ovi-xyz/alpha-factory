"""
tests/integration/test_intelligence_domain_independence_adr053.py — ADR-053: each specialist reads
ONE domain, and the shared engine knows none.

Same philosophy as the ADR-051 A2 §A pins: independence of CODE and STORES, enforced at source
level (imports, string literals) and at job-graph level. decision_xgboost (ADR-054) is the only
place the three domains meet; until it exists, nothing here may let two of them touch.
"""
import ast
import pathlib

import pytest

from src.scheduler.job_registry import GATE_DOMAINS, JOB_REGISTRY, LAYER_JOB_NAMES

PKG = pathlib.Path("src/gold/intelligence")

# module -> domain it belongs to ("shared" = no domain)
OWNERS = {"core.py": "shared", "labels.py": "shared", "__init__.py": "shared",
          "features_hmm.py": "hmm", "hmm_xgboost.py": "hmm",
          "features_gold.py": "technical", "gold_xgboost.py": "technical"}
# any file added under the package must be classified here — the guard below fails otherwise
ALLOWED_INTERNAL = {
    "shared": ("src.gold.intelligence",),
    "hmm": ("src.gold.intelligence", "src.utils.gatekeeper"),
    "technical": ("src.gold.intelligence", "src.utils.gatekeeper"),
}
DOMAIN_STORE_FRAGMENTS = {
    "hmm": ("data/gold/hmm",),
    "technical": ("data/gold/signals", "data/gold/mtf", "data/gold/signal_aggregation"),
    "cross_asset": ("data/gold/cross_asset",),
}
FOREIGN_FRAGMENTS_ALWAYS = ("data/bronze", "data/silver", "regime_store", "global_regime", "data/gold/screener",
                            "data/gold/sector", "data/gold/macro", "data/gold/correlation")


def _sources():
    return sorted(PKG.glob("*.py"))


def _imports(path: pathlib.Path) -> list[str]:
    out = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.append(node.module)
    return out


def _literals(path: pathlib.Path) -> list[str]:
    tree = ast.parse(path.read_text())
    doc_ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)):
            b = node.body
            if b and isinstance(b[0], ast.Expr) and isinstance(getattr(b[0], "value", None), ast.Constant):
                doc_ids.add(id(b[0].value))
    return [n.value for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in doc_ids]


class TestSourceLevel:
    def test_every_module_in_the_package_is_classified(self):
        assert {p.name for p in _sources()} <= set(OWNERS), {p.name for p in _sources()} - set(OWNERS)

    @pytest.mark.parametrize("path", _sources(), ids=lambda p: p.name)
    def test_internal_imports_are_limited_to_the_owner_domain(self, path):
        allowed = ALLOWED_INTERNAL[OWNERS[path.name]]
        for mod in _imports(path):
            if mod.startswith("src."):
                assert any(mod == a or mod.startswith(a + ".") for a in allowed), f"{path.name} imports {mod}"

    @pytest.mark.parametrize("path", _sources(), ids=lambda p: p.name)
    def test_a_domain_module_never_imports_a_sibling_of_another_domain(self, path):
        owner = OWNERS[path.name]
        for mod in _imports(path):
            if mod.startswith("src.gold.intelligence."):
                leaf = mod.split(".")[-1] + ".py"
                assert OWNERS.get(leaf, "shared") in ("shared", owner), f"{path.name} -> {mod}"

    @pytest.mark.parametrize("path", _sources(), ids=lambda p: p.name)
    def test_code_never_names_another_domains_store_or_a_lower_layer(self, path):
        owner = OWNERS[path.name]
        foreign = [f for dom, frs in DOMAIN_STORE_FRAGMENTS.items() if dom != owner for f in frs]
        for lit in _literals(path):
            for frag in (*FOREIGN_FRAGMENTS_ALWAYS, *foreign):
                assert frag not in lit, f"{path.name}: literal {lit!r} names {frag!r}"

    def test_the_shared_engine_names_no_domain_store_at_all(self):
        everything = [f for frs in DOMAIN_STORE_FRAGMENTS.values() for f in frs]
        for name in ("core.py", "labels.py"):
            for lit in _literals(PKG / name):
                assert not any(f in lit for f in everything), (name, lit)

    def test_no_module_outside_the_package_imports_a_specialist(self):
        offenders = []
        for p in pathlib.Path("src").rglob("*.py"):
            if PKG in p.parents or p.name == "job_registry.py":
                continue
            if any(m.startswith("src.gold.intelligence") for m in _imports(p)):
                offenders.append(str(p))
        assert offenders == []


class TestJobGraph:
    SPECIALIST_JOBS = {j for j in JOB_REGISTRY if "xgboost" in j}

    def test_specialist_jobs_are_gold_and_sequenced(self):
        for j in self.SPECIALIST_JOBS:
            assert JOB_REGISTRY[j]["layer"] == "gold" and j in LAYER_JOB_NAMES["gold"], j

    def test_specialist_jobs_are_not_gate_domain_members(self):
        """A model tags its output with the health of its INPUT chain; it cannot be part of it."""
        members = {j for jobs in GATE_DOMAINS.values() for j in jobs}
        assert self.SPECIALIST_JOBS.isdisjoint(members)

    def test_no_job_depends_on_a_specialist_yet(self):
        """decision_xgboost (ADR-054) will be the first; until then nothing is downstream of a specialist."""
        for name, job in JOB_REGISTRY.items():
            assert not (set(job.get("depends_on", [])) & self.SPECIALIST_JOBS), name

    def test_each_specialist_sits_in_exactly_one_domain_by_upstream(self):
        def upstream(j):
            seen, st = set(), list(JOB_REGISTRY[j].get("depends_on", []))
            while st:
                x = st.pop()
                if x not in seen:
                    seen.add(x)
                    st += JOB_REGISTRY[x].get("depends_on", [])
            return seen
        owners = {dom: set(jobs) for dom, jobs in GATE_DOMAINS.items()}
        for j in self.SPECIALIST_JOBS:
            gold_up = {u for u in upstream(j) if JOB_REGISTRY[u]["layer"] == "gold"}
            touching = [d for d, jobs in owners.items() if gold_up & jobs]
            assert len(touching) <= 1, (j, touching)
