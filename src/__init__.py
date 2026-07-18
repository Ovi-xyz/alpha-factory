"""
src — Data Platform
Bronze → Silver → Gold Medallion Architecture

Exports for external consumers (Trading Engine, notebooks, scripts).
"""

__version__ = "1.0.0"
__grand_design_version__ = "1.2"

# Primary public APIs (lazy imports to avoid circular issues)
from src.config.instrument_loader import get_loader, InstrumentLoader, Instrument
from src.config.pipeline_config import get_config, PipelineConfig


def get_pipeline_connection(read_only: bool = False):
    """Return configured DuckDB connection with all pipeline views."""
    from src.gold.views import get_pipeline_connection as _get
    return _get(read_only=read_only)


__all__ = [
    "get_loader",
    "InstrumentLoader",
    "Instrument",
    "get_config",
    "PipelineConfig",
    "get_pipeline_connection",
    "__version__",
    "__grand_design_version__",
]
