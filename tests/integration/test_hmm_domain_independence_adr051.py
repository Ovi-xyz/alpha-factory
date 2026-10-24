"""
tests/integration/test_hmm_domain_independence_adr051.py — ADR-051 Amendment 2 §A.

The intelligence layer has three specialists (hmm / technical-gold /
cross-asset), each a model over ITS OWN domain's input, synthesised only by
decision_xgboost (ADR-053/054). The dual-HMM engine must therefore be
independent — in CODE, in STORES and in the JOB GRAPH — of the CrossAssetEngine
and of the technical chain, in both directions. This is independence of
code and stores, not of information: the domains are driven by the same
markets and will correlate.

Pinned at three levels (as ADR-051 A2 §A requires): source, job graph, schedule.
HMM_JOBS grows as later ADR-051 phases add jobs; the checks are written
against that set so adding a job extends coverage automatically.
"""
import ast
import pathlib

import pytest

from src.scheduler.job_registry import DAILY_SEQUENCE, JOB_REGISTRY, WEEKLY_SEQUENCE

HMM_DIR = pathlib.Path("src/gold/hmm")
GOLD_DIR = pathlib.Path("src/gold")

HMM_JOBS = {"gold_raw_macro", "gold_raw_market", "gold_hmm_macro_regime", "gold_hmm_macro_fit",
            "gold_hmm_market_regime", "gold_hmm_market_fit", "gold_context_regime"}
CROSS_ASSET_JOBS = {"gold_global_regime", "gold_cross_asset_correlation", "gold_lead_lag", "gold_forecast"}
# ADR-055 v2: "gold_regime" left this set — the job no longer exists (macro_regime.py archived).
# gold_screener stays: it reads the HMM context regime as a SOFT source (no depends_on edge), so
# no HMM job is upstream of it in the graph; see tests/integration/test_regime_cutover_adr055.py.
TECHNICAL_CHAIN_JOBS = {"gold_signals", "gold_mtf", "signal_aggregation", "gold_screener"}
FOREIGN_JOBS = CROSS_ASSET_JOBS | TECHNICAL_CHAIN_JOBS

# Modules the HMM package must never import (other domains' Gold code).
FOREIGN_MODULE_PREFIXES = (
    "src.gold.cross_asset", "src.gold.technical_signals", "src.gold.mtf_alignment",
    "src.gold.signal_aggregation", "src.gold.screener",
    "src.gold.macro_regime", "src.gold.indicators",
)
# Gold stores the HMM package must never read.
FORBIDDEN_STORE_FRAGMENTS = (
    "data/gold/cross_asset", "data/gold/signals", "data/gold/mtf", "data/gold/macro",
    "data/gold/screener", "data/gold/sector", "global_regime", "data/bronze", "regime_store",
)


def _hmm_sources():
    return sorted(HMM_DIR.glob("*.py"))


def _imports(path: pathlib.Path) -> list[str]:
    out = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.append(node.module)
    return out


def _upstream(job: str) -> set[str]:
    seen, stack = set(), list(JOB_REGISTRY[job].get("depends_on", []))
    while stack:
        j = stack.pop()
        if j in seen:
            continue
        seen.add(j)
        stack += JOB_REGISTRY[j].get("depends_on", [])
    return seen


# ── source level ─────────────────────────────────────────────────────────────

class TestSourceLevel:
    def test_hmm_package_exists_and_is_nonempty(self):
        assert any(p.name == "raw_macro.py" for p in _hmm_sources())

    @pytest.mark.parametrize("path", _hmm_sources(), ids=lambda p: p.name)
    def test_hmm_module_imports_no_foreign_domain_code(self, path):
        for mod in _imports(path):
            assert not mod.startswith(FOREIGN_MODULE_PREFIXES), f"{path.name} imports {mod}"

    @pytest.mark.parametrize("path", _hmm_sources(), ids=lambda p: p.name)
    def test_hmm_module_never_names_a_foreign_or_bronze_store(self, path):
        """Docstrings may DISCUSS other stores; code may not name them. Strip
        docstrings/comments before scanning string literals."""
        tree = ast.parse(path.read_text())
        doc_ids = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)):
                body = node.body
                if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant):
                    doc_ids.add(id(body[0].value))
        literals = [n.value for n in ast.walk(tree)
                    if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in doc_ids]
        for lit in literals:
            for frag in FORBIDDEN_STORE_FRAGMENTS:
                assert frag not in lit, f"{path.name}: string literal {lit!r} names {frag!r}"

    def test_no_other_gold_module_imports_the_hmm_package(self):
        offenders = []
        for p in GOLD_DIR.rglob("*.py"):
            if HMM_DIR in p.parents:
                continue
            if any(m.startswith("src.gold.hmm") for m in _imports(p)):
                offenders.append(str(p))
        assert offenders == []

    def test_only_scheduler_wiring_references_hmm_outside_the_package(self):
        """job_registry's lazy `from src.gold.hmm...` wrappers are the sole
        permitted reference from other src/ code."""
        offenders = []
        for p in pathlib.Path("src").rglob("*.py"):
            if HMM_DIR in p.parents or p.name == "job_registry.py":
                continue
            if any(m.startswith("src.gold.hmm") for m in _imports(p)):
                offenders.append(str(p))
        assert offenders == []


# ── job graph ────────────────────────────────────────────────────────────────

class TestJobGraph:
    @pytest.mark.parametrize("job", sorted(HMM_JOBS))
    def test_hmm_job_registered_on_gold_layer(self, job):
        assert JOB_REGISTRY[job]["layer"] == "gold"

    @pytest.mark.parametrize("job", sorted(HMM_JOBS))
    def test_no_hmm_job_has_a_foreign_domain_job_upstream(self, job):
        assert _upstream(job).isdisjoint(FOREIGN_JOBS), _upstream(job) & FOREIGN_JOBS

    @pytest.mark.parametrize("job", sorted(FOREIGN_JOBS))
    def test_no_non_hmm_job_depends_on_an_hmm_job(self, job):
        assert _upstream(job).isdisjoint(HMM_JOBS), _upstream(job) & HMM_JOBS

    @pytest.mark.parametrize("job", sorted(HMM_JOBS))
    def test_every_gold_job_upstream_of_an_hmm_job_is_itself_an_hmm_job(self, job):
        """Generic form of the rule: the only Gold jobs an HMM job may sit downstream of
        are other HMM jobs (raw_* -> classify -> context). Everything else upstream is
        Silver/Bronze, which all three intelligence domains legitimately share."""
        gold_upstream = {j for j in _upstream(job) if JOB_REGISTRY[j]["layer"] == "gold"}
        assert gold_upstream <= HMM_JOBS, gold_upstream - HMM_JOBS

    def test_hmm_jobs_set_matches_the_registry_by_naming_convention(self):
        """Drift guard: HMM_JOBS is hand-listed above; a job added to the registry under the engine's
        naming convention but forgotten here would silently escape every independence check."""
        by_name = {n for n in JOB_REGISTRY
                   if n.startswith(("gold_raw_", "gold_hmm_")) or n == "gold_context_regime"}
        assert by_name == HMM_JOBS, (by_name ^ HMM_JOBS)

    def test_other_non_hmm_jobs_do_not_depend_on_hmm_jobs_either(self):
        for name in JOB_REGISTRY:
            if name in HMM_JOBS:
                continue
            assert _upstream(name).isdisjoint(HMM_JOBS), name


# ── schedule ─────────────────────────────────────────────────────────────────

class TestSchedule:
    def test_hmm_jobs_sit_after_technical_engine_and_before_screener(self):
        """ADR-055 v2 (supersedes ADR-051's 'after the screener, it is in burn-in'): the HMM engine
        is now the platform's regime source, so it runs BEFORE gold_screener — but after the
        technical engine, because the runner stops at the first failing job and an HMM failure
        must not stop the technical engine (independent domains)."""
        idx = {n: i for i, n in enumerate(DAILY_SEQUENCE)}
        for job in HMM_JOBS & set(DAILY_SEQUENCE):
            assert idx["signal_aggregation"] < idx[job] < idx["gold_screener"] < idx["gatekeeper"], job

    def test_raw_macro_is_daily_with_weekly_macro_stale_tolerance(self):
        j = JOB_REGISTRY["gold_raw_macro"]
        assert "gold_raw_macro" in DAILY_SEQUENCE and "gold_raw_macro" in WEEKLY_SEQUENCE
        assert j["depends_on"] == ["silver_macro"]
        assert j["stale_tolerance"] == {"silver_macro": 7}
        assert "run_on_weekdays" not in j                  # daily job

    def test_raw_market_is_daily_and_silver_only(self):
        """ADR-051 job table: depends on silver_ohlcv (Layer 1 US-stock bars) and
        silver_macro (FRED market series, weekly cadence -> stale_tolerance 7). NO Gold
        job upstream at all — in particular not gold_global_regime."""
        j = JOB_REGISTRY["gold_raw_market"]
        assert "gold_raw_market" in DAILY_SEQUENCE and "gold_raw_market" in WEEKLY_SEQUENCE
        assert j["depends_on"] == ["silver_ohlcv", "silver_macro"]
        assert j["stale_tolerance"] == {"silver_macro": 7}
        assert "run_on_weekdays" not in j
        assert "gold_global_regime" not in _upstream("gold_raw_market")
        assert all(JOB_REGISTRY[u]["layer"] in ("bronze", "silver") for u in _upstream("gold_raw_market"))

    def test_macro_classify_is_daily_and_has_no_hard_dependency_on_its_fit_job(self):
        """A stale-but-present model still scores and no model at all writes nothing, so a failed or
        skipped weekly fit can never block the daily chain."""
        j = JOB_REGISTRY["gold_hmm_macro_regime"]
        assert j["depends_on"] == ["gold_raw_macro"] and "stale_tolerance" not in j
        assert "run_on_weekdays" not in j
        assert "gold_hmm_macro_regime" in DAILY_SEQUENCE
        assert "gold_hmm_macro_fit" not in _upstream("gold_hmm_macro_regime")

    def test_macro_fit_is_sunday_only_weekly_prefix_with_a_cross_cadence_tolerance(self):
        j = JOB_REGISTRY["gold_hmm_macro_fit"]
        assert j["depends_on"] == ["gold_raw_macro"] and j["stale_tolerance"] == {"gold_raw_macro": 3}
        assert j["run_on_weekdays"] == [6]
        assert "gold_hmm_macro_fit" in WEEKLY_SEQUENCE and "gold_hmm_macro_fit" not in DAILY_SEQUENCE
        # prefix = before the daily jobs it would otherwise be mistaken for
        assert WEEKLY_SEQUENCE.index("gold_hmm_macro_fit") < WEEKLY_SEQUENCE.index("bronze_ohlcv_daily")

    def test_market_pair_mirrors_the_macro_pair(self):
        c, f = JOB_REGISTRY["gold_hmm_market_regime"], JOB_REGISTRY["gold_hmm_market_fit"]
        assert c["depends_on"] == ["gold_raw_market"] and "stale_tolerance" not in c and "run_on_weekdays" not in c
        assert f["depends_on"] == ["gold_raw_market"] and f["stale_tolerance"] == {"gold_raw_market": 3}
        assert f["run_on_weekdays"] == [6]
        assert "gold_hmm_market_regime" in DAILY_SEQUENCE and "gold_hmm_market_fit" not in DAILY_SEQUENCE
        assert "gold_hmm_market_fit" in WEEKLY_SEQUENCE
        assert WEEKLY_SEQUENCE.index("gold_hmm_market_fit") < WEEKLY_SEQUENCE.index("bronze_ohlcv_daily")
        assert "gold_hmm_market_fit" not in _upstream("gold_hmm_market_regime")

    def test_context_regime_is_a_pure_join_of_the_two_classify_jobs(self):
        j = JOB_REGISTRY["gold_context_regime"]
        assert j["depends_on"] == ["gold_hmm_macro_regime", "gold_hmm_market_regime"]
        assert "stale_tolerance" not in j and "run_on_weekdays" not in j
        # no fit job anywhere upstream: nothing is learned in the join
        assert not {n for n in _upstream("gold_context_regime") if n.endswith("_fit")}
        # and it comes after BOTH sides in the daily order
        d = DAILY_SEQUENCE
        assert d.index("gold_context_regime") > max(d.index("gold_hmm_macro_regime"), d.index("gold_hmm_market_regime"))

    def test_hmm_jobs_run_in_gold_layer_scope(self):
        from src.scheduler.job_registry import LAYER_JOB_NAMES
        assert HMM_JOBS <= set(LAYER_JOB_NAMES["gold"])
