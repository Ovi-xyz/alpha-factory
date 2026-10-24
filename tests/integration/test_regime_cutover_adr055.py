"""
tests/integration/test_regime_cutover_adr055.py — ADR-055 v2 (GMI-HMM2-CUT-001).

"Completely replacing macro_regime.py with the dual-HMM regime engine" is a statement about the
STRUCTURE of the Gold layer, so it is pinned structurally — at source, job-graph and schedule level
(the same three levels as the ADR-051 independence test):

  technical engine      gold_signals, gold_mtf, signal_aggregation
  cross-asset engine    gold_global_regime, gold_cross_asset_correlation, gold_lead_lag, gold_forecast
  dual-HMM regime       gold_raw_macro, gold_hmm_macro_regime, gold_raw_market,
                        gold_hmm_market_regime, gold_context_regime, gold_hmm_macro_fit,
                        gold_hmm_market_fit
  (later ADRs)          specialist xgboost x3 -> decision_xgboost -> gold_screener

and that the retired rule-based regime cannot come back by accident.
"""
import ast
import pathlib

import pytest

from src.scheduler.job_registry import (
    DAILY_SEQUENCE, JOB_REGISTRY, PIPELINE_SEQUENCE, WEEKLY_SEQUENCE,
)

SRC = pathlib.Path("src")
ARCHIVE = pathlib.Path("archive/macro_regime_cutover_2026_10")

TECHNICAL_ENGINE = ["gold_signals", "gold_mtf", "signal_aggregation"]
CROSS_ASSET_ENGINE = ["gold_global_regime", "gold_cross_asset_correlation", "gold_lead_lag", "gold_forecast"]
HMM_ENGINE = ["gold_raw_macro", "gold_hmm_macro_regime", "gold_raw_market", "gold_hmm_market_regime",
              "gold_context_regime", "gold_hmm_macro_fit", "gold_hmm_market_fit"]

TECHNICAL_MODULES = ["technical_signals.py", "mtf_alignment.py", "signal_aggregation.py"]

# String literals (code, not docstrings) that would mean a retired store/job is still wired in.
RETIRED_LITERALS = ("regime_store", "sector_regime_weights", "regime_sector_weights", "data/gold/macro",
                    "data/gold/sector", "sector_map", "gold_regime", "gold_sector",
                    "macro_regime.py", "sector_rotation.py")
RETIRED_MODULES = ("src.gold.macro_regime", "src.gold.sector_rotation", "src.gold.sector_map")


def _code_literals(path: pathlib.Path) -> list[str]:
    """String constants that are not docstrings."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    doc_ids = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)):
            if n.body and isinstance(n.body[0], ast.Expr) and isinstance(getattr(n.body[0], "value", None), ast.Constant):
                doc_ids.add(id(n.body[0].value))
    return [n.value for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in doc_ids]


def _imports(path: pathlib.Path) -> list[str]:
    out = []
    for n in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(n, ast.Import):
            out += [a.name for a in n.names]
        elif isinstance(n, ast.ImportFrom) and n.module:
            out.append(n.module)
    return out


def _upstream(job: str) -> set[str]:
    seen, stack = set(), list(JOB_REGISTRY[job].get("depends_on", []))
    while stack:
        j = stack.pop()
        if j not in seen:
            seen.add(j)
            stack += JOB_REGISTRY[j].get("depends_on", [])
    return seen


# ── the retired producer is gone ─────────────────────────────────────────────

class TestLegacyRegimeIsGone:
    @pytest.mark.parametrize("retired", ["gold_regime", "gold_sector"])
    def test_retired_job_is_not_a_job_anywhere(self, retired):
        """gold_regime (macro_regime.py) and gold_sector (sector_rotation.py: regime -> sector
        weights; regime-free it had nothing left to do) are both archived."""
        assert retired not in JOB_REGISTRY
        for seq in (DAILY_SEQUENCE, WEEKLY_SEQUENCE, PIPELINE_SEQUENCE):
            assert retired not in seq
        for name, job in JOB_REGISTRY.items():
            assert retired not in job.get("depends_on", []), name
            assert retired not in job.get("stale_tolerance", {}), name

    def test_retired_modules_are_not_in_src_but_are_in_the_archive(self):
        for retired in ("macro_regime.py", "sector_rotation.py", "sector_map.py"):
            assert not (SRC / "gold" / retired).exists(), retired
        assert (ARCHIVE / "src/gold/macro_regime.py").exists()
        assert (ARCHIVE / "src/gold/sector_rotation.py").exists()
        assert (ARCHIVE / "tests/unit/test_sector_rotation.py").exists()
        assert (ARCHIVE / "tests/unit/test_macro_regime.py").exists()
        assert (ARCHIVE / "config/regime_sector_weights.yaml").exists()
        assert (ARCHIVE / "README.md").exists()

    def test_retired_config_is_not_in_config(self):
        assert not pathlib.Path("config/regime_sector_weights.yaml").exists()
        assert not pathlib.Path("config/schemas/instruments/regime_sector_weights.schema.yaml").exists()

    @pytest.mark.parametrize("path", sorted(SRC.rglob("*.py")), ids=lambda p: str(p.relative_to(SRC)))
    def test_no_src_code_names_a_retired_store_job_or_module(self, path):
        for lit in _code_literals(path):
            for frag in RETIRED_LITERALS:
                assert frag not in lit, f"{path}: literal {lit!r} names retired {frag!r}"
        for mod in _imports(path):
            assert not mod.startswith(RETIRED_MODULES), f"{path} imports retired {mod}"

    def test_screener_names_context_regime_and_no_regime_store(self):
        """The designated (future) Trading Engine interface is gold_screener
        (watchlist_{date}.parquet); regime_store.parquet is no longer an output of the platform."""
        from src.gold import screener
        text = pathlib.Path(screener.__file__).read_text(encoding="utf-8")
        assert "context_regime.parquet" in text and "regime_store" not in "\n".join(
            _code_literals(pathlib.Path(screener.__file__)))


# ── structure of the Gold layer ──────────────────────────────────────────────

class TestGoldEngines:
    @pytest.mark.parametrize("job", TECHNICAL_ENGINE + CROSS_ASSET_ENGINE + HMM_ENGINE + ["gold_screener"])
    def test_every_engine_job_is_registered_on_the_gold_layer(self, job):
        assert JOB_REGISTRY[job]["layer"] == "gold"

    @pytest.mark.parametrize("job", TECHNICAL_ENGINE)
    def test_technical_engine_has_no_hmm_or_regime_job_upstream(self, job):
        up = _upstream(job)
        assert up.isdisjoint(set(HMM_ENGINE)), up
        assert "gold_regime" not in up

    def test_watchlist_carries_no_sector_and_signal_aggregation_reads_no_sector_store(self):
        """gold_sector is archived and the screener's `sector` column is dropped (Ovi, 21 Oct 2026);
        signal_aggregation keeps a registry-direct sector map for its breadth columns and reads no
        Gold store for it."""
        from src.gold import screener, signal_aggregation
        assert not hasattr(screener, "GOLD_SECTOR_PATH") and not hasattr(screener, "load_sector_labels")
        assert not hasattr(signal_aggregation, "GOLD_SECTOR_PATH")
        assert not (SRC / "utils" / "sector_labels.py").exists()
        sql_and_literals = "\n".join(_code_literals(SRC / "gold" / "screener.py"))
        assert "sector_tbl" not in sql_and_literals and "AS sector" not in sql_and_literals

    @pytest.mark.parametrize("name", TECHNICAL_MODULES)
    def test_technical_engine_modules_never_touch_the_hmm_engine(self, name):
        path = SRC / "gold" / name
        for mod in _imports(path):
            assert not mod.startswith("src.gold.hmm"), f"{name} imports {mod}"
        for lit in _code_literals(path):
            assert "data/gold/hmm" not in lit and "context_regime" not in lit, f"{name}: {lit!r}"

    def test_mtf_output_schema_carries_no_regime_columns(self):
        text = (SRC / "gold" / "mtf_alignment.py").read_text(encoding="utf-8")
        code = "\n".join(_code_literals(SRC / "gold" / "mtf_alignment.py"))
        assert "regime_compatible" not in code and "active_regime" not in code
        assert "_apply_regime_compatible" not in text.split('"""', 2)[2]

    def test_cross_asset_engine_has_no_hmm_dependency(self):
        for job in CROSS_ASSET_ENGINE:
            assert _upstream(job).isdisjoint(set(HMM_ENGINE)), job


class TestScreenerIsTheSynthesisTip:
    def test_screener_has_no_hard_edge_to_the_hmm_engine(self):
        """HMM context is a SOFT source (as-of, staleness-bounded, degrades to NULL) exactly like the
        CrossAssetEngine sources — so the independence rule ('no non-HMM job depends on an HMM job')
        holds without an exception for the screener."""
        assert set(JOB_REGISTRY["gold_screener"]["depends_on"]) == {"gold_mtf"}
        assert _upstream("gold_screener").isdisjoint(set(HMM_ENGINE))

    def test_screener_reads_exactly_one_hmm_store_and_it_is_context_regime(self):
        from src.gold import screener
        lits = [l for l in _code_literals(pathlib.Path(screener.__file__)) if "data/gold/hmm" in l]
        assert lits == ["data/gold/hmm/context_regime.parquet"]

    def test_screener_path_matches_the_producers_path(self):
        from src.gold import screener
        from src.gold.hmm.context_regime import CONTEXT_REGIME_PATH
        assert screener.GOLD_CONTEXT_REGIME_PATH == CONTEXT_REGIME_PATH

    def test_pit_loader_path_matches_the_producers_path(self):
        import src.backtest.pit_data as pit
        from src.gold.hmm.context_regime import CONTEXT_REGIME_PATH
        assert pathlib.Path(pit.GOLD_CONTEXT_REGIME_PATH) == CONTEXT_REGIME_PATH


# ── schedule ─────────────────────────────────────────────────────────────────

class TestDailySchedule:
    IDX = {n: i for i, n in enumerate(DAILY_SEQUENCE)}

    def test_engines_run_in_the_documented_order(self):
        """technical engine -> dual-HMM classify/context -> gold_screener -> gatekeeper."""
        i = self.IDX
        assert i["gold_signals"] < i["gold_mtf"] < i["signal_aggregation"]
        hmm_daily = [j for j in HMM_ENGINE if j in i]
        assert set(hmm_daily) == {"gold_raw_macro", "gold_hmm_macro_regime", "gold_raw_market",
                                  "gold_hmm_market_regime", "gold_context_regime"}
        for j in hmm_daily:
            assert i["signal_aggregation"] < i[j] < i["gold_screener"], j
        assert i["gold_context_regime"] > max(i["gold_hmm_macro_regime"], i["gold_hmm_market_regime"])
        assert i["gold_screener"] < i["gatekeeper"]

    def test_hmm_fit_jobs_are_weekly_only(self):
        for j in ("gold_hmm_macro_fit", "gold_hmm_market_fit"):
            assert j in WEEKLY_SEQUENCE and j not in DAILY_SEQUENCE
            assert JOB_REGISTRY[j]["run_on_weekdays"] == [6]

    def test_a_failing_hmm_job_cannot_stop_the_technical_engine(self):
        """The runner stops at the first failing job: everything in the technical engine must come
        BEFORE the first HMM job, so an HMM failure leaves the technical outputs written."""
        first_hmm = min(self.IDX[j] for j in HMM_ENGINE if j in self.IDX)
        assert all(self.IDX[j] < first_hmm for j in TECHNICAL_ENGINE)
