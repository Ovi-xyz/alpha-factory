"""tests/integration/test_cross_asset_recalibration_adr056.py — structural pins for the cross-asset
recalibration (cross_asset_recalibration_guide, proposed ADR-056/057, work packages XAE-R01..R10).

These pin SHAPE, not numbers: which module may import what, which config files exist and agree with the
registry, and which store contracts the screener depends on. Behavioural tests live next to each module."""

import ast
from pathlib import Path

import pytest
import yaml

SRC = Path("src")
CROSS_ASSET = SRC / "gold" / "cross_asset"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module)
    return out


class TestPureHelpersStayPure:
    """robust.py and calendar_oracle.py are stateless helpers: no I/O into the medallion stores."""

    def test_robust_imports_only_numeric_libraries(self):
        mods = _imports(CROSS_ASSET / "robust.py")
        assert not [m for m in mods if m.startswith("src.")], mods
        assert mods <= {"__future__", "functools", "numpy", "polars", "scipy"}, mods

    def test_robust_has_no_filesystem_or_sql_access(self):
        text = (CROSS_ASSET / "robust.py").read_text()
        for needle in ("read_parquet", "write_parquet", "duckdb", "open(", "Path("):
            assert needle not in text.split('"""', 2)[-1], needle

    def test_calendar_oracle_has_no_internal_imports_and_loads_the_library_lazily(self):
        path = SRC / "utils" / "calendar_oracle.py"
        mods = _imports(path)
        assert not [m for m in mods if m.startswith("src.")], mods
        top_level = {n.names[0].name for n in ast.parse(path.read_text()).body if isinstance(n, ast.Import)}
        assert "exchange_calendars" not in top_level, "must be imported lazily so a missing install degrades"
        assert "import exchange_calendars" in path.read_text()

    def test_gap_analysis_stays_free_of_calendar_libraries_and_filesystem(self):
        mods = _imports(SRC / "utils" / "gap_analysis.py")
        assert not [m for m in mods if "calendar" in m or m.startswith("duckdb") or m.startswith("src.")], mods


class TestDomainIndependenceOfNewFiles:
    """ADR-051 A2 §A / ADR-055: cross_asset must not read or be read by the HMM or technical chains."""

    FORBIDDEN = ("src.gold.hmm", "src.gold.intelligence", "src.gold.technical_signals",
                 "src.gold.mtf_alignment", "src.gold.signal_aggregation")

    @pytest.mark.parametrize("name", ["robust.py", "global_index_regime.py", "lead_lag_module.py",
                                      "forecast_module.py", "correlation_module.py", "regime_tag.py"])
    def test_cross_asset_module_does_not_import_other_domains(self, name):
        mods = _imports(CROSS_ASSET / name)
        bad = [m for m in mods if any(m.startswith(f) for f in self.FORBIDDEN)]
        assert not bad, f"{name} imports another domain: {bad}"

    @pytest.mark.parametrize("name", ["lead_lag_module.py", "forecast_module.py", "correlation_module.py",
                                      "global_index_regime.py", "robust.py"])
    def test_cross_asset_never_reads_bronze(self, name):
        text = (CROSS_ASSET / name).read_text()
        assert "data/bronze" not in text.split('"""', 2)[-1]


class TestConfigFiles:

    def test_exchange_calendars_config_exists_and_is_versioned(self):
        cfg = yaml.safe_load(Path("config/exchange_calendars.yaml").read_text())
        assert cfg["version"] and cfg["calendars"]

    def test_relations_config_exists_and_is_versioned(self):
        cfg = yaml.safe_load(Path("config/cross_asset_relations.yaml").read_text())
        assert cfg["version"] and cfg["tautologies"]
        for entry in cfg["tautologies"]:
            assert {"relation", "leader", "followers"} <= set(entry)

    def test_every_calendar_symbol_is_a_layer2_equity_index_or_vix(self):
        """The calendars exist for the indices peer agreement cannot classify; a stray Layer 1 symbol here
        would silently change Layer 1 gap classification."""
        from src.config.instrument_loader import get_loader
        cfg = yaml.safe_load(Path("config/exchange_calendars.yaml").read_text())["calendars"]
        ctx = {i.symbol: i.context_category for i in get_loader().all_context(include_deferred=True)}
        allowed = {"context_equity_dm", "context_equity_em", "context_volatility"}
        bad = {s: ctx.get(s) for s in cfg if ctx.get(s) not in allowed}
        assert not bad, bad


class TestStoreContracts:

    def test_global_regime_store_columns_are_a_superset_of_the_legacy_schema(self):
        from src.gold.cross_asset.global_index_regime import STORE_COLUMNS
        legacy = ["date", "global_risk_score", "dm_score", "em_score", "dm_em_divergence",
                  "asia_pac_breadth", "global_regime_label", "prev_global_regime_label",
                  "regime_transition", "n_dm_covered", "n_em_covered", "n_asia_pac_covered",
                  "partial_coverage_flag", "processing_version"]
        assert list(STORE_COLUMNS[:len(legacy)]) == legacy, "legacy columns keep their order (additive only)"
        assert {"as_of_bar_date", "global_regime_label_raw", "global_risk_score_equal",
                "breadth_dist_pct", "days_in_state", "flicker_10d"} <= set(STORE_COLUMNS)

    def test_screener_reads_global_regime_as_of_not_by_exact_date(self):
        """Pin for the weekend regression: one row per BAR date means `date == run_date` finds nothing."""
        text = (SRC / "gold" / "screener.py").read_text()
        assert "_load_global_regime" in text
        assert 'filter(pl.col("date") == str(run_date))' not in text

    def test_forecast_lag_zero_rows_cannot_reach_the_watchlist_as_a_forecast(self):
        text = (SRC / "gold" / "screener.py").read_text()
        assert "forecast_has_signal" in text and "_load_forecast_1d" in text

    def test_screener_and_tag_reader_share_one_staleness_tolerance(self):
        """The 5-day rule exists in two places (regime_tag for the weekly modules, the screener's as-of read);
        a drift between them would tag a row UNKNOWN in one and use it in the other."""
        from src.gold import screener
        from src.gold.cross_asset import regime_tag
        assert screener.GLOBAL_REGIME_TOLERANCE_DAYS == regime_tag.MAX_TAG_AGE_DAYS

    def test_processing_versions_were_bumped_where_semantics_changed(self):
        from src.gold.cross_asset import global_index_regime as g
        assert g.PROCESSING_VERSION == "1.1"
