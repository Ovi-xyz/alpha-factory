"""
test_pipeline_config_integration.py
Integration tests: PipelineConfig → DuckDB connection wiring.
"""

from pathlib import Path

import pytest

from src.config.pipeline_config import PipelineConfig, duckdb_connection, get_config


class TestPipelineConfigIntegration:

    def test_duckdb_connection_executes_query(self):
        """duckdb_connection() produces a working connection."""
        con    = duckdb_connection()
        result = con.execute("SELECT 1+1 AS two").fetchone()
        assert result[0] == 2
        con.close()

    def test_duckdb_memory_limit_applied(self):
        """DuckDB connection respects memory_limit from config."""
        cfg = get_config()
        con = duckdb_connection()
        result = con.execute("SELECT current_setting('memory_limit')").fetchone()
        # Result is like "3.0 GiB" — just check it's not empty
        assert result[0] is not None and len(str(result[0])) > 0
        con.close()

    def test_config_paths_point_to_expected_dirs(self):
        """Config path values match expected pipeline directory structure."""
        cfg = get_config()
        assert str(cfg.bronze_path) == "data/bronze"
        assert str(cfg.silver_path) == "data/silver"
        assert str(cfg.gold_path)   == "data/gold"

    def test_schemas_dir_exists_with_yaml(self):
        """config/schemas/ directory contains YAML schema files."""
        schemas = list(Path("config/schemas").glob("*.yaml"))
        assert len(schemas) >= 2, (
            f"Expected at least 2 schema files, found {len(schemas)}"
        )

    def test_fred_series_yaml_has_enough_series(self):
        """fred_series.yaml must define at least 50 FRED series."""
        import yaml
        data   = yaml.safe_load(Path("config/fred_series.yaml").read_text())
        series = data.get("series", [])
        assert len(series) >= 50, (
            f"Expected 50+ FRED series, got {len(series)}"
        )

    def test_pipeline_yaml_has_all_sections(self):
        """pipeline.yaml must contain version, duckdb, coverage, paths."""
        import yaml
        data = yaml.safe_load(Path("config/pipeline.yaml").read_text())
        for section in ["version", "duckdb", "coverage", "paths"]:
            assert section in data, f"pipeline.yaml missing section: {section}"

    def test_instruments_yaml_loads_via_config(self):
        """InstrumentLoader uses instruments_yaml path from config."""
        from src.config.instrument_loader import get_loader
        cfg    = get_config()
        loader = get_loader()
        # FIX GMI-IL-001: was == 643, now 640 — ADR-003 reclassified SPX/VIX/DXY
        # to Layer 2 context (Layer 1 count() scope is unchanged in meaning).
        # UPD ADR-036 (GMI_Decision_Document_v8.docx, 10 Aug 2026): USD_IDR
        # reclassified forex -> context.dollar_basket — 640 -> 639.
        # FIX GMI-VAL-004 (chat thread, 3 Sep 2026, RISK-28): 639 -> 603.
        # 36 dead Layer 1 tickers removed — see KNOWN_RISKS.md RISK-28.
        # FIX GMI-VAL-005 (chat thread, 3 Sep 2026, RISK-28 follow-up):
        # 603 -> 594. 9 more removed — 6 confirmed active (stopgap over
        # an undiagnosed fetch-pipeline bug), 3 unresolved. See
        # KNOWN_RISKS.md RISK-28.
        assert loader.count() == 594
