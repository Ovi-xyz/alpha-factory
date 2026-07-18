"""tests/unit/test_package_exports.py — src package public API tests"""

import pytest


class TestPackageExports:

    def test_version_importable(self):
        import src
        assert hasattr(src, "__version__")
        assert src.__version__ == "1.0.0"

    def test_grand_design_version(self):
        import src
        assert src.__grand_design_version__ == "1.2"

    def test_get_loader_importable(self):
        from src import get_loader
        assert callable(get_loader)

    def test_instrument_loader_importable(self):
        from src import InstrumentLoader
        assert InstrumentLoader is not None

    def test_instrument_importable(self):
        from src import Instrument
        assert Instrument is not None

    def test_get_config_importable(self):
        from src import get_config
        assert callable(get_config)

    def test_pipeline_config_importable(self):
        from src import PipelineConfig
        assert PipelineConfig is not None

    def test_get_pipeline_connection_importable(self):
        from src import get_pipeline_connection
        assert callable(get_pipeline_connection)

    def test_all_exports_in_all(self):
        import src
        for name in src.__all__:
            assert hasattr(src, name), f"{name!r} in __all__ but not in module"

    def test_get_loader_returns_640(self):
        """FIX GMI-IL-001: was test_get_loader_returns_643 — ADR-003 reclassification."""
        from src import get_loader
        loader = get_loader()
        assert loader.count() == 640

    def test_get_config_returns_config(self):
        from src import get_config, PipelineConfig
        cfg = get_config()
        assert isinstance(cfg, PipelineConfig)

    def test_get_pipeline_connection_works(self):
        from src import get_pipeline_connection
        con    = get_pipeline_connection()
        result = con.execute("SELECT 1+1").fetchone()
        assert result[0] == 2
        con.close()
