"""
test_job_registry_integrity.py — Job Registry Integration Test
Validates that all jobs in JOB_REGISTRY and PIPELINE_SEQUENCE are
correctly wired: no missing dependencies, no circular deps, all
job functions importable without error.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.scheduler.job_registry import (
    JOB_REGISTRY,
    PIPELINE_SEQUENCE,
    _passes_schedule,
)


class TestJobRegistryCompleteness:

    def test_all_sequence_jobs_in_registry(self):
        """Every job in PIPELINE_SEQUENCE must exist in JOB_REGISTRY."""
        missing = [j for j in PIPELINE_SEQUENCE if j not in JOB_REGISTRY]
        assert not missing, f"Jobs in PIPELINE_SEQUENCE missing from registry: {missing}"

    def test_all_dependency_targets_exist(self):
        """Every dependency target must exist as a registered job."""
        broken = []
        for job_name, job in JOB_REGISTRY.items():
            for dep in job.get("depends_on", []):
                if dep not in JOB_REGISTRY:
                    broken.append(f"{job_name} → {dep}")
        assert not broken, f"Broken dependency targets: {broken}"

    def test_all_jobs_have_required_fields(self):
        """Every job must have: description, fn, depends_on, layer, est_minutes."""
        required = {"description", "fn", "depends_on", "layer", "est_minutes"}
        incomplete = []
        for name, job in JOB_REGISTRY.items():
            missing = required - set(job.keys())
            if missing:
                incomplete.append(f"{name}: missing {missing}")
        assert not incomplete, f"Incomplete job definitions: {incomplete}"

    def test_all_layers_valid(self):
        """All job layers must be one of: bronze, silver, gold, util."""
        valid_layers = {"bronze", "silver", "gold", "util"}
        invalid = [
            f"{name}: layer={job['layer']!r}"
            for name, job in JOB_REGISTRY.items()
            if job.get("layer") not in valid_layers
        ]
        assert not invalid, f"Invalid layer assignments: {invalid}"

    def test_no_circular_dependencies(self):
        """No job should have itself as a (transitive) dependency."""
        def has_cycle(job_name: str, visited: set, path: set) -> bool:
            if job_name in path:
                return True
            if job_name in visited:
                return False
            visited.add(job_name)
            path.add(job_name)
            for dep in JOB_REGISTRY.get(job_name, {}).get("depends_on", []):
                if has_cycle(dep, visited, path):
                    return True
            path.discard(job_name)
            return False

        cycles = []
        for name in JOB_REGISTRY:
            if has_cycle(name, set(), set()):
                cycles.append(name)
        assert not cycles, f"Circular dependencies detected: {cycles}"

    def test_pipeline_sequence_respects_dependencies(self):
        """In PIPELINE_SEQUENCE, each job's dependencies appear before it."""
        position = {job: i for i, job in enumerate(PIPELINE_SEQUENCE)}
        violations = []
        for job_name in PIPELINE_SEQUENCE:
            job_pos = position[job_name]
            for dep in JOB_REGISTRY.get(job_name, {}).get("depends_on", []):
                if dep in position and position[dep] >= job_pos:
                    violations.append(
                        f"{job_name} (pos {job_pos}) depends on"
                        f" {dep} (pos {position[dep]})"
                    )
        assert not violations, f"Dependency order violations: {violations}"

    def test_finnhub_jobs_do_not_exist_in_registry(self):
        """
        FIX ADR-043 (GMI_Decision_Document_v10.docx): Finnhub retired in full
        — sentiment (403 plan-tier gate on every symbol) and earnings/quotes
        (never-activated NotImplementedError stub, FIX R-F04) alike.
        bronze_finnhub, bronze_finnhub_sentiment, silver_fundamental, and
        silver_sentiment no longer exist in JOB_REGISTRY at all — this
        supersedes FIX NEW-2's narrower contract, where bronze_finnhub and
        silver_fundamental still existed but were deliberately excluded from
        PIPELINE_SEQUENCE.
        """
        retired = {
            "bronze_finnhub", "bronze_finnhub_sentiment",
            "silver_fundamental", "silver_sentiment",
        }
        present = retired & set(JOB_REGISTRY.keys())
        assert not present, f"Retired Finnhub jobs still in JOB_REGISTRY: {present}"

    def test_finnhub_jobs_absent_from_both_sequences(self):
        """FIX ADR-043: the four retired jobs must not appear in
        PIPELINE_SEQUENCE (alias DAILY_SEQUENCE) or WEEKLY_SEQUENCE."""
        from src.scheduler.job_registry import WEEKLY_SEQUENCE
        retired = {
            "bronze_finnhub", "bronze_finnhub_sentiment",
            "silver_fundamental", "silver_sentiment",
        }
        for seq_name, seq in (("PIPELINE_SEQUENCE", PIPELINE_SEQUENCE),
                               ("WEEKLY_SEQUENCE", WEEKLY_SEQUENCE)):
            present = retired & set(seq)
            assert not present, f"Retired Finnhub jobs still in {seq_name}: {present}"

    def test_gold_screener_not_dependent_on_finnhub_jobs(self):
        """
        FIX ADR-043: gold_screener.depends_on must not reference any retired
        Finnhub-derived job. Supersedes FIX NEW-2 [BLOCKING], which guarded
        only against silver_fundamental — GD §5.2.4 already designs
        earnings_calendar/sentiment as informational DATA fields (LEFT
        JOIN, "data boleh null"); a hard dependency on either would
        permanently lock gold_screener since neither upstream job can ever
        complete (sentiment: 403 on every symbol; earnings/quotes: never
        implemented).
        """
        retired = {"silver_fundamental", "silver_sentiment"}
        deps = set(JOB_REGISTRY["gold_screener"]["depends_on"])
        assert not (retired & deps), f"gold_screener still depends on: {retired & deps}"

    def test_regime_before_sector_before_screener(self):
        """gold_regime → gold_sector → gold_screener ordering."""
        seq = PIPELINE_SEQUENCE
        names = ["gold_regime", "gold_sector", "gold_screener"]
        positions = [seq.index(n) for n in names if n in seq]
        assert positions == sorted(positions), (
            "Gold regime→sector→screener order violated in PIPELINE_SEQUENCE"
        )

    def test_callable_functions(self):
        """All job fn values must be callable."""
        non_callable = [
            name for name, job in JOB_REGISTRY.items()
            if not callable(job.get("fn"))
        ]
        assert not non_callable, f"Non-callable job functions: {non_callable}"


class TestScheduleGuardIntegration:

    def test_bronze_eia_only_wednesday(self):
        """bronze_eia must only run on Wednesday."""
        from datetime import date
        job = JOB_REGISTRY["bronze_eia"]
        monday    = date(2025, 1, 6)    # Monday
        wednesday = date(2025, 1, 8)    # Wednesday
        assert monday.weekday()    == 0
        assert wednesday.weekday() == 2
        assert not _passes_schedule(job, monday)
        assert _passes_schedule(job, wednesday)

    def test_daily_jobs_run_every_day(self):
        """bronze_ohlcv_daily has no schedule constraints — runs every day."""
        job = JOB_REGISTRY["bronze_ohlcv_daily"]
        for wd in range(7):   # All days of week
            from datetime import date, timedelta
            d = date(2025, 1, 6) + timedelta(days=wd)
            assert _passes_schedule(job, d), \
                f"bronze_ohlcv_daily failed schedule check on weekday={wd}"

    def test_bls_cpi_day_range(self):
        """bronze_bls_cpi only runs on days 10-15 of month."""
        job = JOB_REGISTRY["bronze_bls_cpi"]
        from datetime import date
        assert _passes_schedule(job, date(2025, 2, 10))
        assert _passes_schedule(job, date(2025, 2, 15))
        assert not _passes_schedule(job, date(2025, 2, 9))
        assert not _passes_schedule(job, date(2025, 2, 16))

    def test_bea_gdp_quarterly(self):
        """bronze_bea_gdp runs in months 1,4,7,10 last week."""
        job = JOB_REGISTRY["bronze_bea_gdp"]
        from datetime import date
        assert _passes_schedule(job, date(2025, 1, 28))     # January last week
        assert not _passes_schedule(job, date(2025, 2, 28)) # February — wrong month
        assert _passes_schedule(job, date(2025, 4, 27))     # April last week
        assert not _passes_schedule(job, date(2025, 4, 10)) # April early — wrong day


class TestGMIJR001BISRatesWiring:
    """
    ADD GMI-JR-001 — Data Source & Rates Adjustment v1.0 §8.2.
    Verifies bronze_bis_rates / silver_global_rates job wiring correctness.
    """

    def test_bronze_bis_rates_registered(self):
        assert "bronze_bis_rates" in JOB_REGISTRY

    def test_bronze_bis_rates_has_no_dependencies(self):
        """GD §17.3.1: Bronze ingesters must not depend on each other."""
        assert JOB_REGISTRY["bronze_bis_rates"]["depends_on"] == []

    def test_bronze_bis_rates_layer_is_bronze(self):
        assert JOB_REGISTRY["bronze_bis_rates"]["layer"] == "bronze"

    def test_silver_global_rates_registered(self):
        assert "silver_global_rates" in JOB_REGISTRY

    def test_silver_global_rates_depends_on_bronze_bis_rates(self):
        assert JOB_REGISTRY["silver_global_rates"]["depends_on"] == ["bronze_bis_rates"]

    def test_silver_global_rates_layer_is_silver(self):
        assert JOB_REGISTRY["silver_global_rates"]["layer"] == "silver"

    def test_both_jobs_in_weekly_sequence(self):
        from src.scheduler.job_registry import WEEKLY_SEQUENCE
        assert "bronze_bis_rates" in WEEKLY_SEQUENCE
        assert "silver_global_rates" in WEEKLY_SEQUENCE

    def test_bronze_bis_rates_not_in_daily_sequence(self):
        """Weekly cadence — must NOT be in DAILY_SEQUENCE (matches bronze_macro_weekly pattern)."""
        from src.scheduler.job_registry import DAILY_SEQUENCE
        assert "bronze_bis_rates" not in DAILY_SEQUENCE
        assert "silver_global_rates" not in DAILY_SEQUENCE

    def test_weekly_sequence_order_bis_rates_before_global_rates(self):
        """silver_global_rates must run after bronze_bis_rates (dependency order)."""
        from src.scheduler.job_registry import WEEKLY_SEQUENCE
        bis_idx = WEEKLY_SEQUENCE.index("bronze_bis_rates")
        global_rates_idx = WEEKLY_SEQUENCE.index("silver_global_rates")
        assert bis_idx < global_rates_idx

    def test_weekly_sequence_order_bronze_before_silver(self):
        """Both bronze jobs must precede both silver jobs (layer ordering)."""
        from src.scheduler.job_registry import WEEKLY_SEQUENCE
        bronze_bis_idx  = WEEKLY_SEQUENCE.index("bronze_bis_rates")
        silver_macro_idx = WEEKLY_SEQUENCE.index("silver_macro")
        assert bronze_bis_idx < silver_macro_idx

    def test_bronze_bis_rates_fn_is_callable(self):
        assert callable(JOB_REGISTRY["bronze_bis_rates"]["fn"])

    def test_silver_global_rates_fn_is_callable(self):
        assert callable(JOB_REGISTRY["silver_global_rates"]["fn"])

    def test_bronze_bis_rates_fn_delegates_correctly(self):
        """Wrapper must call src.bronze.bis_rates_ingester.run(run_date)."""
        from unittest.mock import patch
        with patch("src.bronze.bis_rates_ingester.run") as mock_run:
            JOB_REGISTRY["bronze_bis_rates"]["fn"](date(2026, 6, 30))
        mock_run.assert_called_once_with(date(2026, 6, 30))

    def test_silver_global_rates_fn_delegates_correctly(self):
        """Wrapper must call src.silver.global_rates_processor.run(run_date)."""
        from unittest.mock import patch
        with patch("src.silver.global_rates_processor.run") as mock_run:
            JOB_REGISTRY["silver_global_rates"]["fn"](date(2026, 6, 30))
        mock_run.assert_called_once_with(date(2026, 6, 30))

    def test_silver_active_symbols_fn_delegates_to_module_run(self):
        """
        FIX GMI-JR-001: silver_active_symbols wrapper must delegate to the
        module-level run() (which resolves BOTH Layer 1 and Layer 2), not
        call resolver.resolve() directly (which would silently skip Layer 2).
        """
        from unittest.mock import patch
        with patch("src.silver.active_symbols.run") as mock_run:
            JOB_REGISTRY["silver_active_symbols"]["fn"](date(2026, 6, 30))
        mock_run.assert_called_once_with(date(2026, 6, 30))


class TestGMIJR002ContextOHLCVWiring:
    """
    ADD GMI-JR-002 — Architecture v2.0 §4, Architecture Extension v1.0 §2-3, §8.
    Verifies bronze_ohlcv_context_daily / silver_ohlcv_context job wiring —
    the Bronze/Silver Layer 2 OHLCV pipeline (GMI-BRZ-001 / GMI-SIL-001).
    """

    def test_bronze_ohlcv_context_daily_registered(self):
        assert "bronze_ohlcv_context_daily" in JOB_REGISTRY

    def test_bronze_ohlcv_context_daily_has_no_dependencies(self):
        """GD §17.3.1: Bronze ingesters must not depend on each other —
        independent of bronze_ohlcv_daily (Layer 1)."""
        assert JOB_REGISTRY["bronze_ohlcv_context_daily"]["depends_on"] == []

    def test_bronze_ohlcv_context_daily_layer_is_bronze(self):
        assert JOB_REGISTRY["bronze_ohlcv_context_daily"]["layer"] == "bronze"

    def test_silver_ohlcv_context_registered(self):
        assert "silver_ohlcv_context" in JOB_REGISTRY

    def test_silver_ohlcv_context_depends_on_bronze_ohlcv_context_daily(self):
        assert JOB_REGISTRY["silver_ohlcv_context"]["depends_on"] == [
            "bronze_ohlcv_context_daily"
        ]

    def test_silver_ohlcv_context_layer_is_silver(self):
        assert JOB_REGISTRY["silver_ohlcv_context"]["layer"] == "silver"

    def test_both_jobs_in_daily_sequence(self):
        """Unlike BIS rates (weekly), Layer 2 context OHLCV is DAILY —
        GlobalIndexRegimeModule and gold_domain_scores (Architecture v2.0
        §6.5, Architecture Extension v1.0 §5) are daily-cadence consumers."""
        from src.scheduler.job_registry import DAILY_SEQUENCE
        assert "bronze_ohlcv_context_daily" in DAILY_SEQUENCE
        assert "silver_ohlcv_context" in DAILY_SEQUENCE

    def test_daily_sequence_order_bronze_before_silver_context(self):
        from src.scheduler.job_registry import DAILY_SEQUENCE
        bronze_idx = DAILY_SEQUENCE.index("bronze_ohlcv_context_daily")
        silver_idx = DAILY_SEQUENCE.index("silver_ohlcv_context")
        assert bronze_idx < silver_idx

    def test_bronze_ohlcv_context_daily_before_layer1_silver_ohlcv_unrelated(self):
        """Sanity: context Bronze job position must not accidentally depend
        on, or be blocked by, Layer 1's silver_ohlcv position."""
        from src.scheduler.job_registry import DAILY_SEQUENCE
        assert DAILY_SEQUENCE.index("bronze_ohlcv_daily") < DAILY_SEQUENCE.index(
            "silver_ohlcv_context"
        )

    def test_bronze_ohlcv_context_daily_fn_is_callable(self):
        assert callable(JOB_REGISTRY["bronze_ohlcv_context_daily"]["fn"])

    def test_silver_ohlcv_context_fn_is_callable(self):
        assert callable(JOB_REGISTRY["silver_ohlcv_context"]["fn"])

    def test_bronze_ohlcv_context_daily_fn_delegates_correctly(self):
        """Wrapper must call MarketOHLCVIngester().run_context(run_date)."""
        from unittest.mock import patch
        with patch(
            "src.bronze.market_ingester.MarketOHLCVIngester.run_context"
        ) as mock_run:
            JOB_REGISTRY["bronze_ohlcv_context_daily"]["fn"](date(2026, 7, 1))
        mock_run.assert_called_once_with(date(2026, 7, 1))

    def test_silver_ohlcv_context_fn_delegates_correctly(self):
        """Wrapper must call src.silver.ohlcv_processor.run_context(run_date)."""
        from unittest.mock import patch
        with patch("src.silver.ohlcv_processor.run_context") as mock_run:
            JOB_REGISTRY["silver_ohlcv_context"]["fn"](date(2026, 7, 1))
        mock_run.assert_called_once_with(date(2026, 7, 1))

    def test_context_ohlcv_jobs_do_not_disturb_existing_daily_sequence_length_floor(self):
        """Anti-pattern guard (CI/CD Ops Guide §Anti-Pattern Test table):
        assert a floor, not an exact count — sequence grows over cycles.

        Floor lowered 15 -> 13 (FIX ADR-043, GMI_Decision_Document_v10.docx):
        DAILY_SEQUENCE dropped from 16 to 14 entries when
        bronze_finnhub_sentiment and silver_sentiment were retired in full
        (Finnhub 403 plan-tier gate). This is a deliberate, documented
        reduction, not a regression the floor should mask — 13 is the new
        correct floor (14 actual, with 1 unit of headroom matching this
        test's own original margin above the then-16-job actual count)."""
        from src.scheduler.job_registry import DAILY_SEQUENCE
        assert len(DAILY_SEQUENCE) >= 13


class TestGMIJR003ContextAnchorsWiring:
    """
    MOVED GMI-CTX-001 — Architecture v2.0 §4.4. Verifies silver_context_anchors
    job wiring after Layer 2 context-anchor resolution was extracted from
    active_symbols.py (ActiveSymbolsResolver.resolve_context()) into its own
    module (src/silver/context_anchors.py::ContextAnchorsResolver) and its
    own job, separate from silver_active_symbols (Layer 1).
    """

    def test_silver_context_anchors_registered(self):
        assert "silver_context_anchors" in JOB_REGISTRY

    def test_silver_context_anchors_has_no_dependencies(self):
        """resolve() is pure InstrumentLoader enumeration — zero Silver
        read, so a fake dependency would only add needless blocking risk
        (see context_anchors.py::run() docstring)."""
        assert JOB_REGISTRY["silver_context_anchors"]["depends_on"] == []

    def test_silver_context_anchors_layer_is_silver(self):
        assert JOB_REGISTRY["silver_context_anchors"]["layer"] == "silver"

    def test_silver_context_anchors_in_daily_sequence(self):
        from src.scheduler.job_registry import DAILY_SEQUENCE
        assert "silver_context_anchors" in DAILY_SEQUENCE

    def test_silver_context_anchors_fn_is_callable(self):
        assert callable(JOB_REGISTRY["silver_context_anchors"]["fn"])

    def test_silver_context_anchors_fn_delegates_correctly(self):
        """Wrapper must call src.silver.context_anchors.run(run_date)."""
        from unittest.mock import patch
        with patch("src.silver.context_anchors.run") as mock_run:
            JOB_REGISTRY["silver_context_anchors"]["fn"](date(2026, 7, 5))
        mock_run.assert_called_once_with(date(2026, 7, 5))

    def test_silver_active_symbols_still_registered_and_unaffected(self):
        """Sanity: splitting Layer 2 out must not have disturbed Layer 1's
        job entry (dependencies, layer, callability)."""
        assert "silver_active_symbols" in JOB_REGISTRY
        assert JOB_REGISTRY["silver_active_symbols"]["depends_on"] == [
            "silver_ohlcv", "silver_validate"
        ]
        assert callable(JOB_REGISTRY["silver_active_symbols"]["fn"])

    def test_active_symbols_resolver_no_longer_exposes_layer2_methods(self):
        """Negative test — proves the extraction actually happened, not
        just that a new module was added alongside a still-present old one
        (the same discipline RISK-3's fix verification used: grep/introspect
        for ABSENCE of the old pattern, not just presence of the new one)."""
        from src.silver.active_symbols import ActiveSymbolsResolver
        assert not hasattr(ActiveSymbolsResolver, "resolve_context")
        assert not hasattr(ActiveSymbolsResolver, "load_context")
        assert not hasattr(ActiveSymbolsResolver, "load_context_full")

    def test_context_anchors_resolver_exposes_expected_api(self):
        from src.silver.context_anchors import ContextAnchorsResolver
        assert hasattr(ContextAnchorsResolver, "resolve")
        assert hasattr(ContextAnchorsResolver, "load")
        assert hasattr(ContextAnchorsResolver, "load_full")

    def test_silver_context_anchors_job_does_not_disturb_daily_sequence_length_floor(self):
        """Floor lowered 15 -> 13 (FIX ADR-043) — see the sibling test in
        TestGMIJR002ContextOHLCVWiring above for the full rationale;
        DAILY_SEQUENCE's actual length dropped from 16 to 14 when
        bronze_finnhub_sentiment/silver_sentiment were retired in full."""
        from src.scheduler.job_registry import DAILY_SEQUENCE
        assert len(DAILY_SEQUENCE) >= 13


class TestLayerJobNames:
    """
    GMI-JR-003 — LAYER_JOB_NAMES integrity, backing `--job bronze/silver/gold`.
    Derived from WEEKLY_SEQUENCE, not hand-maintained — these tests guard
    against layer_sequence() silently drifting from JOB_REGISTRY/DAILY_
    SEQUENCE/WEEKLY_SEQUENCE as jobs are added, removed, or re-tagged.
    """

    def test_exactly_three_layers_present(self):
        from src.scheduler.job_registry import LAYER_JOB_NAMES
        assert set(LAYER_JOB_NAMES.keys()) == {"bronze", "silver", "gold"}

    def test_every_listed_job_exists_in_registry(self):
        from src.scheduler.job_registry import LAYER_JOB_NAMES
        for layer, names in LAYER_JOB_NAMES.items():
            missing = [n for n in names if n not in JOB_REGISTRY]
            assert not missing, f"{layer}: unregistered job names {missing}"

    def test_every_listed_job_matches_its_own_layer_field(self):
        """LAYER_JOB_NAMES['bronze'] must contain only layer='bronze' jobs
        (and so on) — a mismatch here would mean layer_sequence() is
        grouping by the wrong key."""
        from src.scheduler.job_registry import LAYER_JOB_NAMES
        for layer, names in LAYER_JOB_NAMES.items():
            mismatched = [n for n in names if JOB_REGISTRY[n]["layer"] != layer]
            assert not mismatched, f"{layer}: layer-field mismatch on {mismatched}"

    def test_no_duplicate_job_names_within_a_layer(self):
        from src.scheduler.job_registry import LAYER_JOB_NAMES
        for layer, names in LAYER_JOB_NAMES.items():
            assert len(names) == len(set(names)), f"{layer}: duplicate job names {names}"

    def test_deliberately_unsequenced_jobs_are_excluded(self):
        """The 3 manual-only BLS/BEA jobs are absent from both DAILY_SEQUENCE
        and WEEKLY_SEQUENCE by design — they must not leak into any layer
        list. (bronze_finnhub and silver_fundamental were previously in this
        same "registered but unsequenced" category; per ADR-043 they no
        longer exist in JOB_REGISTRY at all — see
        test_finnhub_jobs_do_not_exist_in_registry above — so they are not
        checked here.)"""
        from src.scheduler.job_registry import LAYER_JOB_NAMES
        all_listed = set().union(*LAYER_JOB_NAMES.values())
        deliberately_excluded = {
            "bronze_bls_cpi", "bronze_bls_nfp", "bronze_bea_gdp",
        }
        leaked = all_listed & deliberately_excluded
        assert not leaked, f"Deliberately unsequenced jobs leaked into LAYER_JOB_NAMES: {leaked}"

    def test_util_layer_jobs_excluded_from_all_three_lists(self):
        """health_report (layer='util') must not appear under bronze/silver/
        gold — `--job gold` scope is literally the gold layer, nothing else."""
        from src.scheduler.job_registry import LAYER_JOB_NAMES
        util_jobs = {n for n, j in JOB_REGISTRY.items() if j["layer"] == "util"}
        all_listed = set().union(*LAYER_JOB_NAMES.values())
        assert not (all_listed & util_jobs)

    def test_layer_lists_cover_every_sequenced_bronze_silver_gold_job(self):
        """Every job that IS sequenced (in DAILY_SEQUENCE or WEEKLY_SEQUENCE)
        with layer in {bronze, silver, gold} must appear in the matching
        LAYER_JOB_NAMES list — i.e. layer_sequence() never silently drops a
        real, scheduled job."""
        from src.scheduler.job_registry import LAYER_JOB_NAMES, WEEKLY_SEQUENCE
        all_listed = set().union(*LAYER_JOB_NAMES.values())
        for job_name in set(WEEKLY_SEQUENCE):
            layer = JOB_REGISTRY[job_name]["layer"]
            if layer in LAYER_JOB_NAMES:
                assert job_name in all_listed, (
                    f"{job_name} (layer={layer}) is sequenced but missing "
                    f"from LAYER_JOB_NAMES['{layer}']"
                )

    def test_layer_sequence_helper_matches_precomputed_dict(self):
        """LAYER_JOB_NAMES is a snapshot taken at import time — confirm it's
        still exactly what calling layer_sequence() fresh would produce."""
        from src.scheduler.job_registry import LAYER_JOB_NAMES, layer_sequence
        for layer in ("bronze", "silver", "gold"):
            assert layer_sequence(layer) == LAYER_JOB_NAMES[layer]

    def test_layer_sequence_deduplicates_repeated_job_names(self, monkeypatch):
        """Defensive dedup branch in layer_sequence(): a job name appearing
        twice in WEEKLY_SEQUENCE must only be counted once. Not reachable
        via the current WEEKLY_SEQUENCE (no job is listed twice there), so
        this constructs a synthetic duplicate to exercise the branch
        directly rather than leaving it silently untested."""
        import src.scheduler.job_registry as job_registry_module

        synthetic_sequence = [
            "bronze_ohlcv_daily", "bronze_treasury", "bronze_ohlcv_daily",
        ]
        monkeypatch.setattr(job_registry_module, "WEEKLY_SEQUENCE", synthetic_sequence)

        result = job_registry_module.layer_sequence("bronze")
        assert result == ["bronze_ohlcv_daily", "bronze_treasury"]
