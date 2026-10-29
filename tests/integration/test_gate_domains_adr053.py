"""
tests/integration/test_gate_domains_adr053.py — drift guard for job_registry.GATE_DOMAINS (ADR-053 D4).

Every sequenced Gold job must be accounted for: a data job of exactly ONE gate domain, a specialist
model job (named *_xgboost_*), or the screener (the synthesis tip). A gold job added to the registry
and forgotten here would silently escape every domain's health status.
"""
from src.scheduler.job_registry import GATE_DOMAINS, JOB_REGISTRY, LAYER_JOB_NAMES

SYNTHESIS = {"gold_screener"}


def test_gate_domain_jobs_exist_are_gold_and_are_sequenced():
    for dom, jobs in GATE_DOMAINS.items():
        for j in jobs:
            assert j in JOB_REGISTRY, (dom, j)
            assert JOB_REGISTRY[j]["layer"] == "gold", (dom, j)
            assert j in LAYER_JOB_NAMES["gold"], (dom, j)


def test_no_job_belongs_to_two_domains():
    seen: dict[str, str] = {}
    for dom, jobs in GATE_DOMAINS.items():
        for j in jobs:
            assert j not in seen, f"{j} in both {seen[j]} and {dom}"
            seen[j] = dom


def test_every_sequenced_gold_job_is_classified():
    classified = {j for jobs in GATE_DOMAINS.values() for j in jobs}
    for j in LAYER_JOB_NAMES["gold"]:
        assert j in classified or "xgboost" in j or j in SYNTHESIS, f"unclassified gold job: {j}"


def test_domain_chains_do_not_share_a_gold_job():
    """Upstream closure may share Bronze/Silver (all domains legitimately rest on them) but never Gold."""
    def closure(jobs):
        seen, st = set(), list(jobs)
        while st:
            x = st.pop()
            if x not in seen:
                seen.add(x)
                st += JOB_REGISTRY[x].get("depends_on", [])
        return {j for j in seen if JOB_REGISTRY[j]["layer"] == "gold"}
    doms = list(GATE_DOMAINS)
    for i, a in enumerate(doms):
        for b in doms[i + 1:]:
            assert closure(GATE_DOMAINS[a]).isdisjoint(closure(GATE_DOMAINS[b])), (a, b)
