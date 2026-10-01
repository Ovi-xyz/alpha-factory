"""
tests/unit/test_fred_series_contributes_to_adr050.py

FIX GMI-FSR-001 (ADR-050): config/fred_series.yaml gets its own `domains`
block with a per-domain `_meta.contributes_to` list, structurally
mirroring instruments_taxonomy.yaml's per-subcategory `_meta.contributes_to`
shape -- but owned by this registry, keyed by this file's own `domain:`
vocabulary (monetary_policy, inflation, growth, labor, credit, housing,
volatility, commodity), not instruments_taxonomy.yaml's context
subcategories.

Covers the structural invariant only (schema present, 1:1 domain
coverage, still empty) -- ADR-050's own Consequences state this block has
"zero consumers until ADR-051 ships". No weight/aggregation values are
asserted here because none exist yet by design (see the module comment
in fred_series.yaml itself for why, citing FIX ADR-019's precedent in
instruments_taxonomy.yaml).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REGISTRY_PATH = Path("config/fred_series.yaml")

EXPECTED_DOMAINS = {
    "monetary_policy", "inflation", "growth", "labor",
    "credit", "housing", "volatility", "commodity",
}


@pytest.fixture
def registry() -> dict:
    return yaml.safe_load(REGISTRY_PATH.read_text())


class TestDomainsBlockPresent:

    def test_domains_key_exists(self, registry):
        assert "domains" in registry

    def test_all_expected_domains_present(self, registry):
        assert set(registry["domains"].keys()) == EXPECTED_DOMAINS

    def test_every_domain_has_meta_contributes_to(self, registry):
        for name, spec in registry["domains"].items():
            assert "_meta" in spec, f"{name} missing _meta"
            assert "contributes_to" in spec["_meta"], f"{name} missing contributes_to"
            assert isinstance(spec["_meta"]["contributes_to"], list)

    def test_contributes_to_deliberately_empty(self, registry):
        """ADR-050 Consequences: zero consumers until ADR-051 ships --
        no domain/weight/aggregation value should be invented ahead of
        the ADR that actually builds the consumer."""
        for name, spec in registry["domains"].items():
            assert spec["_meta"]["contributes_to"] == [], (
                f"{name}: contributes_to should still be empty pending "
                f"ADR-051 — see fred_series.yaml's own comment for why"
            )


class TestDomainCoverageMatchesSeriesRegistry:
    """The `domains` block must stay in lockstep with the `domain:` values
    every series entry already carries — no domain block entry that no
    series references, and no series domain missing a block entry."""

    def test_no_series_domain_missing_from_domains_block(self, registry):
        series_domains = {s["domain"] for s in registry["series"]}
        missing = series_domains - set(registry["domains"].keys())
        assert not missing, f"domain(s) referenced by series but undeclared: {missing}"

    def test_no_orphaned_domain_block_entry(self, registry):
        series_domains = {s["domain"] for s in registry["series"]}
        orphaned = set(registry["domains"].keys()) - series_domains
        assert not orphaned, f"domain(s) declared but no series uses them: {orphaned}"


class TestExistingRegistryUnaffected:
    """Regression guard: adding the `domains` block must not disturb the
    existing series list (ADR-041/042 invariants)."""

    def test_series_count_still_68(self, registry):
        assert len(registry["series"]) == 68

    def test_no_duplicate_series_ids(self, registry):
        ids = [s["id"] for s in registry["series"]]
        assert len(ids) == len(set(ids))

    def test_regime_inputs_block_untouched(self, registry):
        assert "regime_inputs" in registry
        assert registry["regime_inputs"]["vix_proxy"] == "VIXCLS"
