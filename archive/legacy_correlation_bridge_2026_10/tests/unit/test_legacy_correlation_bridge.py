"""
tests/unit/test_legacy_correlation_bridge.py

FIX GMI-CORR-RETIRE-01 (KNOWN_RISKS.md RISK-31, 20 Sep 2026).

Coverage for src/gold/cross_asset/legacy_correlation_bridge.py: the
derivation that projects CorrelationModule's pairwise
cross_asset_corr.parquet schema into gold_correlation's legacy per-symbol
schema, so screener.py::_deduplicate_by_cluster() and views.py's
v_correlation (Trading Engine Interface Contract, GD §0.4) keep working
unchanged after gold_correlation's retirement.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.gold.cross_asset.legacy_correlation_bridge import (
    LEGACY_CORR_CLUSTERS_PATH,
    derive_legacy_correlation_clusters,
    write_legacy_correlation_clusters,
)


def _pairwise(rows: list[tuple]) -> pl.DataFrame:
    """Build a minimal pairwise frame matching CorrelationModule's
    _to_long_format() output schema."""
    return pl.DataFrame(
        {
            "symbol_a":         [r[0] for r in rows],
            "symbol_b":         [r[1] for r in rows],
            "correlation":      [r[2] for r in rows],
            "regime":           [r[3] for r in rows],
            "computation_date": [r[4] for r in rows],
            "cluster_id_a":     [r[5] for r in rows],
            "cluster_id_b":     [r[6] for r in rows],
        }
    )


class TestDeriveLegacyCorrelationClusters:
    def test_legacy_schema_columns(self):
        """Output has exactly gold_correlation's five legacy columns, in order."""
        pairwise = _pairwise([("AAPL", "MSFT", 0.5, "RISK_ON", "2026-09-14", 1, 1)])
        out = derive_legacy_correlation_clusters(pairwise)
        assert out.columns == [
            "symbol", "cluster_id", "correlation_avg", "n_cluster_members", "computed_date",
        ]

    def test_every_symbol_from_both_sides_appears(self):
        """A symbol seen only as symbol_a and one seen only as symbol_b both surface."""
        pairwise = _pairwise([("AAPL", "MSFT", 0.5, "RISK_ON", "2026-09-14", 1, 1)])
        out = derive_legacy_correlation_clusters(pairwise)
        assert set(out["symbol"].to_list()) == {"AAPL", "MSFT"}

    def test_correlation_avg_uses_absolute_value(self):
        """A symbol with a negative-correlation pair must not average toward
        zero against a positive one purely from sign cancellation — matches
        the old correlation_matrix.py semantics of mean(abs(correlation))."""
        pairwise = _pairwise([
            ("AAPL", "MSFT", -0.8, "RISK_ON", "2026-09-14", 1, 1),
            ("AAPL", "GOOG", 0.8, "RISK_ON", "2026-09-14", 1, 2),
        ])
        out = derive_legacy_correlation_clusters(pairwise)
        aapl_avg = out.filter(pl.col("symbol") == "AAPL")["correlation_avg"][0]
        assert aapl_avg == pytest.approx(0.8)

    def test_correlation_avg_is_mean_across_all_of_a_symbols_pairs(self):
        """Three pairs involving AAPL (two as symbol_a, one as symbol_b) ->
        correlation_avg is the mean of all three |correlation| values."""
        pairwise = _pairwise([
            ("AAPL", "MSFT", 0.6, "RISK_ON", "2026-09-14", 1, 1),
            ("AAPL", "GOOG", 0.2, "RISK_ON", "2026-09-14", 1, 2),
            ("TSLA", "AAPL", 0.4, "RISK_ON", "2026-09-14", 3, 1),
        ])
        out = derive_legacy_correlation_clusters(pairwise)
        aapl_avg = out.filter(pl.col("symbol") == "AAPL")["correlation_avg"][0]
        assert aapl_avg == pytest.approx((0.6 + 0.2 + 0.4) / 3)

    def test_cluster_id_consistent_across_a_symbols_pairs(self):
        """A symbol's cluster_id is fixed for the run (assigned once by
        CorrelationModule._cluster()) regardless of which side of which
        pair it appears on — grouping by (symbol, cluster_id) must not
        silently fragment one symbol into two output rows."""
        pairwise = _pairwise([
            ("AAPL", "MSFT", 0.6, "RISK_ON", "2026-09-14", 5, 1),
            ("TSLA", "AAPL", 0.4, "RISK_ON", "2026-09-14", 3, 5),
        ])
        out = derive_legacy_correlation_clusters(pairwise)
        assert out.filter(pl.col("symbol") == "AAPL").height == 1
        assert out.filter(pl.col("symbol") == "AAPL")["cluster_id"][0] == 5

    def test_n_cluster_members_counts_distinct_symbols_in_cluster(self):
        """cluster 1 = {AAPL, MSFT, TSLA}; cluster 2 = {GOOG} — sizes must
        reflect that, not the pair count."""
        pairwise = _pairwise([
            ("AAPL", "MSFT", 0.6, "RISK_ON", "2026-09-14", 1, 1),
            ("MSFT", "TSLA", 0.5, "RISK_ON", "2026-09-14", 1, 1),
            ("AAPL", "GOOG", 0.1, "RISK_ON", "2026-09-14", 1, 2),
        ])
        out = derive_legacy_correlation_clusters(pairwise)
        cluster_1_sizes = out.filter(pl.col("cluster_id") == 1)["n_cluster_members"].unique().to_list()
        assert cluster_1_sizes == [3]
        assert out.filter(pl.col("symbol") == "GOOG")["n_cluster_members"][0] == 1

    def test_computed_date_passed_through(self):
        pairwise = _pairwise([("AAPL", "MSFT", 0.5, "RISK_ON", "2026-09-14", 1, 1)])
        out = derive_legacy_correlation_clusters(pairwise)
        assert out["computed_date"].unique().to_list() == ["2026-09-14"]

    def test_empty_input_returns_empty_frame_with_legacy_schema(self):
        """Empty pairwise input -> empty output, not an exception — matches
        every other Gold module's 'no data, skip' convention."""
        out = derive_legacy_correlation_clusters(pl.DataFrame())
        assert out.is_empty()
        assert out.columns == [
            "symbol", "cluster_id", "correlation_avg", "n_cluster_members", "computed_date",
        ]

    def test_malformed_input_missing_columns_returns_empty(self):
        """A frame missing the expected pairwise columns must degrade to
        empty output rather than raising — this bridge is best-effort."""
        out = derive_legacy_correlation_clusters(pl.DataFrame({"symbol": ["AAPL"]}))
        assert out.is_empty()

    def test_layer2_symbols_pass_through_untouched(self):
        """Layer 2 symbols (VIX, DXY, ...) are not filtered out here —
        screener.py's own MTF join is what makes them harmless downstream,
        not this derivation. Confirms the bridge doesn't quietly drop
        anything based on symbol identity."""
        pairwise = _pairwise([("AAPL", "VIX", 0.3, "RISK_OFF", "2026-09-14", 1, 4)])
        out = derive_legacy_correlation_clusters(pairwise)
        assert "VIX" in out["symbol"].to_list()


class TestWriteLegacyCorrelationClusters:
    def test_writes_to_gold_correlations_legacy_path(self, tmp_path, monkeypatch):
        """Must write to gold_correlation's exact old output path — the
        one screener.py and views.py's v_correlation already read."""
        monkeypatch.chdir(tmp_path)
        import src.gold.cross_asset.legacy_correlation_bridge as mod
        monkeypatch.setattr(mod, "LEGACY_CORR_CLUSTERS_PATH", tmp_path / "data/gold/correlation/correlation_clusters.parquet")

        pairwise = _pairwise([("AAPL", "MSFT", 0.5, "RISK_ON", "2026-09-14", 1, 1)])
        result_path = mod.write_legacy_correlation_clusters(pairwise)

        assert result_path is not None
        assert result_path.exists()
        written = pl.read_parquet(result_path)
        assert set(written["symbol"].to_list()) == {"AAPL", "MSFT"}

    def test_legacy_path_constant_matches_screener_and_views(self):
        """Regression guard against future drift: the constant this module
        writes to must be byte-identical to the path screener.py and
        views.py independently hardcode for the same file."""
        import src.gold.screener as screener_mod
        import src.gold.views as views_mod

        screener_src = Path(screener_mod.__file__).read_text(encoding="utf-8")
        views_src = Path(views_mod.__file__).read_text(encoding="utf-8")
        assert "data/gold/correlation/correlation_clusters.parquet" in screener_src
        assert "data/gold/correlation/correlation_clusters.parquet" in views_src
        assert str(LEGACY_CORR_CLUSTERS_PATH) == "data/gold/correlation/correlation_clusters.parquet"

    def test_empty_pairwise_writes_nothing_and_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import src.gold.cross_asset.legacy_correlation_bridge as mod
        target = tmp_path / "data/gold/correlation/correlation_clusters.parquet"
        monkeypatch.setattr(mod, "LEGACY_CORR_CLUSTERS_PATH", target)

        result_path = mod.write_legacy_correlation_clusters(pl.DataFrame())

        assert result_path is None
        assert not target.exists()

    def test_write_failure_is_caught_not_raised(self, monkeypatch):
        """This bridge is best-effort: it must never propagate an exception
        up into gold_cross_asset_correlation's run() — cross_asset_corr.parquet,
        not this legacy file, is that job's real deliverable."""
        import src.gold.cross_asset.legacy_correlation_bridge as mod

        def _boom(*a, **kw):
            raise RuntimeError("disk full")

        monkeypatch.setattr(mod, "atomic_write_parquet", _boom)
        pairwise = _pairwise([("AAPL", "MSFT", 0.5, "RISK_ON", "2026-09-14", 1, 1)])

        result_path = mod.write_legacy_correlation_clusters(pairwise)  # must not raise
        assert result_path is None


class TestCorrelationModuleWiresTheBridge:
    def test_run_module_imports_legacy_bridge(self):
        """correlation_module.py must call the bridge from its own run() —
        a static import-and-call check, not a full pipeline run."""
        src = Path("src/gold/cross_asset/correlation_module.py").read_text(encoding="utf-8")
        assert "write_legacy_correlation_clusters" in src
        assert "from src.gold.cross_asset.legacy_correlation_bridge import" in src
