"""
tests/unit/test_yaml_split_merge.py

NEW (Decision B Step 2, GMI_Decision_Document_v5.docx §2.1) — unit tests
for src/config/yaml_split_merge.py in isolation, with small synthetic
fixtures. Real-file-level correctness (identity + taxonomy actually
reconstruct the pre-split instruments.yaml exactly) is covered separately
by tests/unit/test_instrument_loader.py (real file, via InstrumentLoader)
and tests/unit/test_validate_instruments.py's TestValidateSplit class.
"""
from __future__ import annotations

import pytest

from src.config.yaml_split_merge import merge_split_trees


class TestMergeHappyPath:

    def test_disjoint_scalar_fields_combine(self):
        identity = {"a": {"symbol": "X", "yfinance_symbol": "X.US"}}
        taxonomy = {"a": {"symbol": "X", "layer": 2}}
        merged = merge_split_trees(identity, taxonomy)
        assert merged == {"a": {"symbol": "X", "yfinance_symbol": "X.US", "layer": 2}}

    def test_key_present_only_in_one_side_survives(self):
        identity = {"a": 1, "b": 2}
        taxonomy = {"c": 3}
        merged = merge_split_trees(identity, taxonomy)
        assert merged == {"a": 1, "b": 2, "c": 3}

    def test_instrument_list_merges_index_wise(self):
        identity = {"items": [{"symbol": "AAPL"}, {"symbol": "MSFT"}]}
        taxonomy = {"items": [{"symbol": "AAPL", "layer": 1}, {"symbol": "MSFT", "layer": 1}]}
        merged = merge_split_trees(identity, taxonomy)
        assert merged == {
            "items": [
                {"symbol": "AAPL", "layer": 1},
                {"symbol": "MSFT", "layer": 1},
            ]
        }

    def test_meta_wholesale_from_taxonomy_side_only(self):
        identity = {"rates": {"fed": {}}}
        taxonomy = {"rates": {"fed": {"_meta": {"subcategory_id": "context_rates_fed"}}}}
        merged = merge_split_trees(identity, taxonomy)
        assert merged == {"rates": {"fed": {"_meta": {"subcategory_id": "context_rates_fed"}}}}

    def test_empty_taxonomy_side_list_falls_back_to_identity(self):
        # Mirrors the real us_stocks case: taxonomy has no per-instrument
        # fields at all for regular stocks, so its list is empty.
        identity = {"sector": [{"symbol": "AAPL"}, {"symbol": "MSFT"}]}
        taxonomy = {"sector": []}
        merged = merge_split_trees(identity, taxonomy)
        assert merged == identity

    def test_one_side_entirely_absent_at_a_path(self):
        identity = {"us_stocks": {"Technology": [{"symbol": "AAPL"}]}}
        taxonomy = {}   # taxonomy carries zero us_stocks keys at all
        merged = merge_split_trees(identity, taxonomy)
        assert merged == identity

    def test_identical_scalar_list_on_both_sides_is_fine(self):
        identity = {"x": {"central_banks": ["ECB", "BOE"]}}
        taxonomy = {"x": {"central_banks": ["ECB", "BOE"]}}
        merged = merge_split_trees(identity, taxonomy)
        assert merged == identity

    def test_identical_scalar_at_same_path_is_fine(self):
        identity = {"version": "1.6"}
        taxonomy = {"version": "1.6"}
        merged = merge_split_trees(identity, taxonomy)
        assert merged == {"version": "1.6"}


class TestMergeRaisesOnMisalignment:
    """These are the corruption-detection cases — the whole reason 'symbol'
    is a shared anchor key instead of a pure index-only join."""

    def test_anchor_symbol_mismatch_raises(self):
        identity = {"items": [{"symbol": "AAPL"}]}
        taxonomy = {"items": [{"symbol": "MSFT", "layer": 1}]}  # wrong entry at index 0
        with pytest.raises(ValueError, match="anchor key 'symbol' mismatch"):
            merge_split_trees(identity, taxonomy)

    def test_list_length_mismatch_raises(self):
        identity = {"items": [{"symbol": "AAPL"}, {"symbol": "MSFT"}]}
        taxonomy = {"items": [{"symbol": "AAPL", "layer": 1}]}
        with pytest.raises(ValueError, match="length mismatch"):
            merge_split_trees(identity, taxonomy)

    def test_unexpected_field_overlap_raises(self):
        # Both sides claim to own 'layer' for the same instrument — split
        # was not field-disjoint.
        identity = {"items": [{"symbol": "AAPL", "layer": 1}]}
        taxonomy = {"items": [{"symbol": "AAPL", "layer": 2}]}
        with pytest.raises(ValueError, match="unexpected field overlap"):
            merge_split_trees(identity, taxonomy)

    def test_conflicting_scalar_at_same_path_raises(self):
        identity = {"version": "1.6"}
        taxonomy = {"version": "1.5"}
        with pytest.raises(ValueError, match="conflicting scalar"):
            merge_split_trees(identity, taxonomy)

    def test_non_dict_list_item_mismatch_raises(self):
        identity = {"x": {"series": ["SOFR", "FEDFUNDS"]}}
        taxonomy = {"x": {"series": ["SOFR", "IORB"]}}
        with pytest.raises(ValueError, match="list item mismatch"):
            merge_split_trees(identity, taxonomy)


class TestMergeNoneHandling:

    def test_both_none_at_root_is_none(self):
        assert merge_split_trees(None, None) is None

    def test_identity_none_returns_taxonomy(self):
        assert merge_split_trees(None, {"a": 1}) == {"a": 1}

    def test_taxonomy_none_returns_identity(self):
        assert merge_split_trees({"a": 1}, None) == {"a": 1}

    def test_does_not_mutate_inputs(self):
        identity = {"items": [{"symbol": "AAPL"}]}
        taxonomy = {"items": [{"symbol": "AAPL", "layer": 1}]}
        identity_copy = {"items": [{"symbol": "AAPL"}]}
        taxonomy_copy = {"items": [{"symbol": "AAPL", "layer": 1}]}
        merge_split_trees(identity, taxonomy)
        assert identity == identity_copy
        assert taxonomy == taxonomy_copy
