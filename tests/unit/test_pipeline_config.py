"""tests/unit/test_pipeline_config.py — PipelineConfig unit tests"""

from pathlib import Path

import pytest
import yaml

from src.config.pipeline_config import PipelineConfig, _load_config, get_config


class TestPipelineConfig:

    def test_default_config_valid(self):
        cfg = PipelineConfig()
        assert cfg.duckdb_memory_limit_gb  == 3
        assert cfg.duckdb_threads          == 4
        assert cfg.min_symbol_coverage_pct == 95.0
        assert cfg.bronze_compression      == "snappy"
        assert cfg.silver_compression      == "zstd"
        assert cfg.min_mtf_score_screener  == 5
        assert cfg.screener_top_n          == 20
        assert cfg.backtest_commission_pct == 0.001

    def test_config_paths_are_path_objects(self):
        cfg = PipelineConfig()
        for attr in ["bronze_path", "silver_path", "gold_path",
                     "health_path", "quarantine_path"]:
            assert isinstance(getattr(cfg, attr), Path), \
                f"{attr} should be a Path, got {type(getattr(cfg, attr))}"

    def test_config_is_frozen(self):
        """PipelineConfig is immutable (frozen dataclass)."""
        cfg = PipelineConfig()
        with pytest.raises((AttributeError, TypeError)):
            cfg.duckdb_threads = 8

    def test_load_from_yaml(self, tmp_path):
        """_load_config reads from custom YAML path."""
        config_data = {
            "duckdb": {"memory_limit_gb": 2, "threads": 2},
            "coverage": {"min_symbol_pct": 90.0, "forex_null_alert": 3},
            "paths": {
                "bronze": "data/bronze",
                "silver": "data/silver",
                "gold":   "data/gold",
            },
        }
        p = tmp_path / "pipeline.yaml"
        p.write_text(yaml.dump(config_data))

        import src.config.pipeline_config as pcmod
        orig = pcmod.CONFIG_PATH
        pcmod.CONFIG_PATH = p
        try:
            cfg = _load_config()
            assert cfg.duckdb_memory_limit_gb  == 2
            assert cfg.duckdb_threads          == 2
            assert cfg.min_symbol_coverage_pct == 90.0
        finally:
            pcmod.CONFIG_PATH = orig

    def test_load_missing_yaml_returns_defaults(self, tmp_path):
        """Missing config/pipeline.yaml → use all defaults."""
        import src.config.pipeline_config as pcmod
        orig = pcmod.CONFIG_PATH
        pcmod.CONFIG_PATH = tmp_path / "nonexistent.yaml"
        try:
            cfg = _load_config()
            assert cfg.duckdb_memory_limit_gb == 3   # default
        finally:
            pcmod.CONFIG_PATH = orig

    def test_singleton_returns_same_instance(self):
        """get_config() returns the same object on repeated calls."""
        c1 = get_config()
        c2 = get_config()
        assert c1 is c2

    def test_storage_thresholds_ordered(self):
        """Alert threshold must be lower than warn threshold."""
        cfg = PipelineConfig()
        assert cfg.storage_alert_gb < cfg.storage_warn_gb

    def test_coverage_pct_range(self):
        """Coverage thresholds must be in (0, 100]."""
        cfg = PipelineConfig()
        assert 0 < cfg.min_symbol_coverage_pct  <= 100
        assert 0 < cfg.checkpoint_min_coverage  <= 100

    def test_duckdb_connection_helper(self, monkeypatch):
        """duckdb_connection() returns a connected DuckDB instance."""
        from src.config.pipeline_config import duckdb_connection
        con = duckdb_connection()
        result = con.execute("SELECT 42 AS answer").fetchone()
        assert result[0] == 42
        con.close()
