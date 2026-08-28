"""
pipeline_config.py — Pipeline Configuration Loader
Reads config/pipeline.yaml and provides typed access to all settings.

Provides sensible defaults if config file is absent (dev-friendly).
Singleton pattern — YAML parsed once per process.

Usage:
    from src.config.pipeline_config import get_config
    cfg = get_config()
    print(cfg.duckdb_memory_limit_gb)      # 3
    print(cfg.min_symbol_coverage_pct)     # 95.0
    print(cfg.bronze_path)                 # Path("data/bronze")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Optional

import yaml


CONFIG_PATH = Path("config/pipeline.yaml")


@dataclass(frozen=True)
class PipelineConfig:
    """
    Typed configuration for the Data Platform.
    All fields have sensible defaults aligned with Grand Design v1.2.
    """

    # ── DuckDB settings (GD §10.2) ────────────────────────────────────────────
    duckdb_memory_limit_gb: int   = 3
    duckdb_threads:         int   = 4

    # ── Coverage thresholds (GD §13.2 / §15.1) ───────────────────────────────
    min_symbol_coverage_pct:   float = 95.0   # Stop screener below this
    checkpoint_min_coverage:   float = 95.0   # ProgressCheckpoint minimum
    forex_null_alert_threshold: int  = 5       # Alert if > N forex pairs null

    # ── Data paths ────────────────────────────────────────────────────────────
    bronze_path:     Path = field(default_factory=lambda: Path("data/bronze"))
    silver_path:     Path = field(default_factory=lambda: Path("data/silver"))
    gold_path:       Path = field(default_factory=lambda: Path("data/gold"))
    health_path:     Path = field(default_factory=lambda: Path("data/health"))
    quarantine_path: Path = field(default_factory=lambda: Path("data/quarantine"))
    sentinels_path:  Path = field(default_factory=lambda: Path("data/.sentinels"))

    # ── Schema paths ──────────────────────────────────────────────────────────
    schemas_dir:           Path = field(default_factory=lambda: Path("config/schemas"))
    # UPD Decision B Step 2 (GMI_Decision_Document_v5.docx, 2026-07-22):
    # config/instruments.yaml no longer exists — split into 2 files. This
    # field was already a documentation-only knob before the split (grep-
    # confirmed: InstrumentLoader hardcodes its own paths, does not read
    # this config) — kept that way, just updated so it's not a stale
    # reference to a deleted file.
    instruments_identity_yaml: Path = field(default_factory=lambda: Path("config/instruments_identity.yaml"))
    instruments_taxonomy_yaml: Path = field(default_factory=lambda: Path("config/instruments_taxonomy.yaml"))
    fred_series_yaml:      Path = field(default_factory=lambda: Path("config/fred_series.yaml"))

    # ── Bronze settings ───────────────────────────────────────────────────────
    bronze_compression:        str   = "snappy"
    yfinance_calls_per_minute: int   = 100    # ~2000/hr conservative daily
    default_lookback_days:     int   = 7      # IncFetchProtocol overlap

    # ── Silver settings ───────────────────────────────────────────────────────
    silver_compression:        str   = "zstd"
    silver_compression_level:  int   = 3
    silver_row_group_size:     int   = 50_000
    outlier_zscore_threshold:  float = 4.0
    null_tolerance_pct:        float = 0.001   # 0.1% max null rate

    # ── Gold settings ─────────────────────────────────────────────────────────
    min_mtf_score_screener: int   = 3         # FIX ADR-046 Path C: Screener: |score| >= 3 (was 5)
    screener_top_n:          int   = 20        # Max watchlist size
    max_per_cluster:         int   = 2         # Correlation concentration guard
    correlation_lookback:    int   = 65        # 60D + buffer
    n_correlation_clusters:  int   = 10        # Hierarchical clustering target

    # ── Scheduler ─────────────────────────────────────────────────────────────
    timezone:                str   = "Asia/Jakarta"   # WIB

    # ── Backtest ──────────────────────────────────────────────────────────────
    backtest_commission_pct:  float = 0.001    # 0.1% per side round-trip
    backtest_max_position_pct: float = 0.10    # 10% of capital per position
    backtest_train_months:    int   = 3
    backtest_test_months:     int   = 1

    # ── System health (GD §15.2) ──────────────────────────────────────────────
    storage_alert_gb:   float = 70.0
    storage_warn_gb:    float = 150.0
    max_pipeline_lag_h: float = 2.0    # Alert if > 2h behind schedule


def _load_config() -> PipelineConfig:
    """Load and parse pipeline.yaml. Fall back to defaults if missing."""
    if not CONFIG_PATH.exists():
        return PipelineConfig()

    with open(CONFIG_PATH) as f:
        raw = yaml.safe_load(f) or {}

    # Extract nested sections
    duckdb   = raw.get("duckdb", {})
    coverage = raw.get("coverage", {})
    paths    = raw.get("paths", {})
    chk      = raw.get("checkpoint", {})

    def _path(key: str, default: str) -> Path:
        return Path(paths.get(key, default))

    return PipelineConfig(
        duckdb_memory_limit_gb    = duckdb.get("memory_limit_gb", 3),
        duckdb_threads            = duckdb.get("threads", 4),
        min_symbol_coverage_pct   = coverage.get("min_symbol_pct", 95.0),
        checkpoint_min_coverage   = chk.get("min_coverage_pct", 95.0),
        forex_null_alert_threshold= coverage.get("forex_null_alert", 5),
        bronze_path               = _path("bronze",     "data/bronze"),
        silver_path               = _path("silver",     "data/silver"),
        gold_path                 = _path("gold",       "data/gold"),
        health_path               = _path("health",     "data/health"),
        quarantine_path           = _path("quarantine", "data/quarantine"),
        sentinels_path            = _path("sentinels",  "data/.sentinels"),
    )


@lru_cache(maxsize=1)
def get_config() -> PipelineConfig:
    """
    Return singleton PipelineConfig.
    YAML is parsed once per process. Changes require process restart.
    """
    return _load_config()


def duckdb_connection(read_only: bool = False):
    """
    Return a DuckDB connection pre-configured with pipeline settings.
    Convenience helper — applies memory_limit and threads from config.
    """
    import duckdb
    cfg = get_config()
    con = duckdb.connect(read_only=read_only)
    con.execute(f"SET memory_limit = '{cfg.duckdb_memory_limit_gb}GB';")
    con.execute(f"SET threads = {cfg.duckdb_threads};")
    con.execute("SET enable_object_cache = true;")
    return con
