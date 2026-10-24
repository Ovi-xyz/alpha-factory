"""
tests/unit/test_hmm_context_regime.py — ADR-051 (GMI-HMM2-001): src/gold/hmm/context_regime.py.

A PURE JOIN of the macro and market regime stores. What these pin:
  * the spine is the macro store; the market state is attached AS-OF, backward, within the tolerance
    — and beyond it every market column (including the date and the age) is null, never a stale carry
  * market_age_days is the explicit staleness of what was attached
  * in_sample is True if ANY side actually used is in-sample (a row is out-of-sample only if all are)
  * missing input writes NOTHING and leaves the existing store untouched; a store lacking a needed
    column raises; the write is atomic
  * the producer's path IS the path PITDataLoader.get_context_regime reads (round trip through it)
"""
from datetime import date, timedelta

import polars as pl
import pytest
from loguru import logger

from src.gold.hmm import context_regime as cr
from src.gold.hmm import hmm_macro_regime, hmm_market_regime

RUN = date(2026, 3, 31)


def _macro(rows):
    """rows: (date, label, activity, momentum, in_sample[, fingerprint])"""
    return pl.DataFrame({
        "date": [r[0] for r in rows], "macro_label": [r[1] for r in rows], "macro_activity": [r[2] for r in rows],
        "macro_momentum": [r[3] for r in rows], "in_sample": [r[4] for r in rows],
        "model_config_fingerprint": [r[5] if len(r) > 5 else "mfp" for r in rows],
    }, schema={"date": pl.Date, "macro_label": pl.String, "macro_activity": pl.Float64, "macro_momentum": pl.Float64,
               "in_sample": pl.Boolean, "model_config_fingerprint": pl.String})


def _market(rows):
    """rows: (date, label, prob, score, in_sample, n_obs[, fingerprint])"""
    return pl.DataFrame({
        "date": [r[0] for r in rows], "market_label": [r[1] for r in rows], "market_probability": [r[2] for r in rows],
        "market_score": [r[3] for r in rows], "in_sample": [r[4] for r in rows], "n_features_observed": [r[5] for r in rows],
        "model_config_fingerprint": [r[6] if len(r) > 6 else "kfp" for r in rows],
    }, schema={"date": pl.Date, "market_label": pl.String, "market_probability": pl.Float64, "market_score": pl.Float64,
               "in_sample": pl.Boolean, "n_features_observed": pl.Int32, "model_config_fingerprint": pl.String})


D = lambda day: date(2026, 3, day)                                     # noqa: E731  (Mon 2 Mar 2026 = D(2))


def _one(macro_in=False, market_in=False, ml="expansion", kl="risk_on"):
    return cr.build(_macro([(D(2), ml, 0.4, 0.1, macro_in)]), _market([(D(2), kl, 0.9, 0.8, market_in, 10)]), RUN).row(0, named=True)


class TestSchema:
    def test_columns_order_and_dtypes(self):
        out = cr.build(_macro([(D(2), "expansion", 0.4, 0.1, True)]), _market([(D(2), "risk_on", 0.9, 0.8, False, 10)]), RUN)
        assert out.columns == cr.COLUMNS
        assert out.schema["market_age_days"] == pl.Int32 and out.schema["market_date"] == pl.Date
        assert out.schema["in_sample"] == pl.Boolean and out.schema["market_n_features_observed"] == pl.Int32
        assert out.schema["date"] == pl.Date and out.schema["context_label"] == pl.String

    def test_values_are_carried_from_each_side_under_prefixed_names(self):
        r = cr.build(_macro([(D(2), "slowdown", 0.33, -0.4, True, "MFP")]),
                     _market([(D(2), "mixed", 0.71, 0.12, False, 7, "KFP")]), RUN).row(0, named=True)
        assert (r["macro_label"], r["macro_activity"], r["macro_momentum"], r["macro_in_sample"]) == ("slowdown", 0.33, -0.4, True)
        assert (r["market_label"], r["market_probability"], r["market_score"]) == ("mixed", 0.71, 0.12)
        assert (r["market_in_sample"], r["market_n_features_observed"]) == (False, 7)
        assert (r["macro_config_fingerprint"], r["market_config_fingerprint"]) == ("MFP", "KFP")
        assert (r["computation_date"], r["processing_version"]) == (RUN, cr.PROCESSING_VERSION)

    def test_one_row_per_macro_date_sorted(self):
        out = cr.build(_macro([(D(x), "expansion", 0.4, 0.1, False) for x in (2, 3, 4, 5)]),
                       _market([(D(x), "risk_on", 0.9, 0.8, False, 10) for x in (2, 3, 4, 5)]), RUN)
        assert out["date"].to_list() == [D(2), D(3), D(4), D(5)]


class TestAsOfJoin:
    def test_exact_date_match_has_zero_age(self):
        r = _one()
        assert r["market_date"] == D(2) and r["market_age_days"] == 0

    def test_a_missing_market_day_carries_the_previous_session_with_its_age(self):
        out = cr.build(_macro([(D(x), "expansion", 0.4, 0.1, False) for x in (2, 3, 4)]),
                       _market([(D(2), "risk_off", 0.9, -0.8, False, 10)]), RUN).sort("date")
        assert out["market_date"].to_list() == [D(2)] * 3
        assert out["market_age_days"].to_list() == [0, 1, 2]
        assert out["market_label"].to_list() == ["risk_off"] * 3

    def test_the_latest_market_row_on_or_before_wins_never_a_later_one(self):
        out = cr.build(_macro([(D(4), "expansion", 0.4, 0.1, False)]),
                       _market([(D(2), "risk_off", 0.9, -0.8, False, 10), (D(3), "mixed", 0.6, 0.0, False, 10),
                                (D(5), "risk_on", 0.9, 0.8, False, 10)]), RUN)
        r = out.row(0, named=True)
        assert r["market_date"] == D(3) and r["market_label"] == "mixed"        # D(5) is the FUTURE: not used

    @pytest.mark.parametrize("gap,attached", [(0, True), (4, True), (5, True), (6, False), (30, False)])
    def test_tolerance_boundary_is_inclusive_at_five_days(self, gap, attached):
        macro_day = date(2026, 3, 2) + timedelta(days=gap)
        r = cr.build(_macro([(macro_day, "expansion", 0.4, 0.1, False)]),
                     _market([(D(2), "risk_on", 0.9, 0.8, False, 10)]), RUN).row(0, named=True)
        if attached:
            assert r["market_age_days"] == gap and r["market_label"] == "risk_on"
        else:
            assert all(r[c] is None for c in ("market_date", "market_age_days", "market_label", "market_probability",
                                              "market_score", "market_in_sample", "market_n_features_observed",
                                              "market_config_fingerprint"))

    def test_tolerance_constant_is_five(self):
        assert cr.MARKET_TOLERANCE_DAYS == 5

    def test_a_custom_tolerance_is_honoured(self):
        r = cr.build(_macro([(D(5), "expansion", 0.4, 0.1, False)]), _market([(D(2), "risk_on", 0.9, 0.8, False, 10)]),
                     RUN, tolerance_days=2).row(0, named=True)
        assert r["market_label"] is None                                        # 3 days > 2

    def test_dates_before_the_market_store_begins_are_macro_only_rows(self):
        out = cr.build(_macro([(D(2), "recession", -0.4, 0.0, True), (D(9), "recovery", -0.2, 0.4, False)]),
                       _market([(D(9), "mixed", 0.5, 0.0, False, 10)]), RUN).sort("date")
        early = out.row(0, named=True)
        assert early["market_label"] is None and early["context_label"] is None and early["macro_label"] == "recession"
        assert out.row(1, named=True)["context_label"] == "recovery/mixed"

    def test_market_only_dates_are_not_in_the_output_because_the_spine_is_macro(self):
        out = cr.build(_macro([(D(2), "expansion", 0.4, 0.1, False)]),
                       _market([(D(2), "risk_on", 0.9, 0.8, False, 10), (D(3), "risk_off", 0.9, -0.8, False, 10)]), RUN)
        assert out["date"].to_list() == [D(2)]


class TestLabelsAndSampleFlags:
    def test_context_label_joins_the_two_labels(self):
        assert _one(ml="slowdown", kl="risk_off")["context_label"] == "slowdown/risk_off"

    def test_context_label_is_null_if_either_side_has_no_label(self):
        assert _one(ml=None)["context_label"] is None
        assert _one(kl=None)["context_label"] is None

    @pytest.mark.parametrize("macro_in,market_in,expected", [
        (False, False, False), (True, False, True), (False, True, True), (True, True, True)])
    def test_in_sample_is_true_if_any_side_used_is(self, macro_in, market_in, expected):
        assert _one(macro_in=macro_in, market_in=market_in)["in_sample"] is expected

    def test_a_missing_market_side_does_not_make_the_row_in_sample(self):
        r = cr.build(_macro([(D(2), "expansion", 0.4, 0.1, False)]), _market([]), RUN).row(0, named=True)
        assert r["market_in_sample"] is None and r["in_sample"] is False

    def test_a_missing_market_side_leaves_the_macro_flag_in_charge(self):
        r = cr.build(_macro([(D(2), "expansion", 0.4, 0.1, True)]), _market([]), RUN).row(0, named=True)
        assert r["in_sample"] is True


# ═══ run ═════════════════════════════════════════════════════════════════════

@pytest.fixture
def stores(tmp_path, monkeypatch):
    macro_p, market_p, ctx_p = tmp_path / "macro.parquet", tmp_path / "market.parquet", tmp_path / "hmm" / "context.parquet"
    monkeypatch.setattr(hmm_macro_regime, "MACRO_REGIME_PATH", macro_p)
    monkeypatch.setattr(hmm_market_regime, "MARKET_REGIME_PATH", market_p)
    monkeypatch.setattr(cr, "CONTEXT_REGIME_PATH", ctx_p)
    days = [date(2026, 3, 2) + timedelta(days=i) for i in range(30) if (date(2026, 3, 2) + timedelta(days=i)).weekday() < 5]
    _macro([(d, "expansion", 0.4, 0.1, d < date(2026, 3, 10)) for d in days]).write_parquet(macro_p)
    _market([(d, "risk_on", 0.9, 0.8, d < date(2026, 3, 10), 10) for d in days if d != date(2026, 3, 16)]).write_parquet(market_p)
    return {"macro": macro_p, "market": market_p, "ctx": ctx_p, "days": days}


class TestRun:
    def test_writes_the_join(self, stores):
        cr.run(date(2026, 4, 30))
        out = pl.read_parquet(stores["ctx"])
        assert out.height == len(stores["days"]) and out.columns == cr.COLUMNS
        gap = out.filter(pl.col("date") == date(2026, 3, 16)).row(0, named=True)
        assert gap["market_date"] == date(2026, 3, 13) and gap["market_age_days"] == 3          # Friday carried over the gap

    def test_run_date_bounds_both_sides(self, stores):
        cr.run(date(2026, 3, 9))
        out = pl.read_parquet(stores["ctx"])
        assert out["date"].max() == date(2026, 3, 9) and out["computation_date"].unique().to_list() == [date(2026, 3, 9)]

    def test_idempotent_rebuild(self, stores):
        cr.run(date(2026, 4, 30))
        first = pl.read_parquet(stores["ctx"])
        cr.run(date(2026, 4, 30))
        assert pl.read_parquet(stores["ctx"]).equals(first)

    def test_rebuild_picks_up_a_corrected_source_unlike_an_append_only_store(self, stores):
        cr.run(date(2026, 4, 30))
        m = pl.read_parquet(stores["market"]).with_columns(pl.lit("risk_off").alias("market_label"))
        m.write_parquet(stores["market"])
        cr.run(date(2026, 4, 30))
        assert pl.read_parquet(stores["ctx"])["market_label"].drop_nulls().unique().to_list() == ["risk_off"]

    @pytest.mark.parametrize("missing", ["macro", "market"])
    def test_a_missing_store_writes_nothing_and_leaves_the_existing_context_alone(self, stores, missing):
        cr.run(date(2026, 4, 30))
        before = stores["ctx"].read_bytes()
        stores[missing].unlink()
        msgs = []
        sink = logger.add(lambda m: msgs.append(str(m)), level="WARNING")
        try:
            cr.run(date(2026, 4, 30))
        finally:
            logger.remove(sink)
        assert stores["ctx"].read_bytes() == before and any("nothing written" in m for m in msgs)

    def test_no_stores_and_no_context_means_no_file(self, stores):
        stores["macro"].unlink()
        cr.run(date(2026, 4, 30))
        assert not stores["ctx"].exists()

    def test_macro_store_with_no_rows_by_the_run_date_writes_nothing(self, stores):
        cr.run(date(2026, 2, 1))
        assert not stores["ctx"].exists()

    @pytest.mark.parametrize("store,drop", [("macro", "macro_label"), ("market", "n_features_observed")])
    def test_a_store_missing_a_needed_column_raises_naming_it(self, stores, store, drop):
        pl.read_parquet(stores[store]).drop(drop).write_parquet(stores[store])
        with pytest.raises(cr.ContextRegimeError, match=f"{store} store is missing columns \\['{drop}'\\]"):
            cr.run(date(2026, 4, 30))

    def test_atomic_write_leaves_no_tmp_files(self, stores):
        cr.run(date(2026, 4, 30))
        assert [p.name for p in stores["ctx"].parent.iterdir()] == ["context.parquet"]

    def test_a_failed_write_does_not_clobber_the_existing_context(self, stores, monkeypatch):
        cr.run(date(2026, 4, 30))
        before = stores["ctx"].read_bytes()
        monkeypatch.setattr(cr, "atomic_write_parquet", lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
        with pytest.raises(OSError):
            cr.run(date(2026, 4, 30))
        assert stores["ctx"].read_bytes() == before


class TestConsumerContract:
    def test_the_producer_path_is_the_path_pitdataloader_reads(self):
        from src.backtest import pit_data
        assert str(cr.CONTEXT_REGIME_PATH) == pit_data.GOLD_CONTEXT_REGIME_PATH

    def test_round_trip_through_the_real_pit_accessor(self, stores, monkeypatch):
        from src.backtest import pit_data
        cr.run(date(2026, 4, 30))
        monkeypatch.setattr(pit_data, "GOLD_CONTEXT_REGIME_PATH", str(stores["ctx"]))
        ldr = pit_data.PITDataLoader()
        row = ldr.get_context_regime(date(2026, 3, 17))
        assert row is not None and row["date"] == date(2026, 3, 17) and row["context_label"] == "expansion/risk_on"
        # strictly point-in-time: a date before the first row has nothing; a later one never leaks the future
        assert ldr.get_context_regime(date(2026, 3, 1)) is None
        assert ldr.get_context_regime(date(2026, 3, 3))["date"] == date(2026, 3, 3)

    def test_the_accessor_degrades_to_none_before_the_store_exists(self, tmp_path, monkeypatch):
        from src.backtest import pit_data
        monkeypatch.setattr(pit_data, "GOLD_CONTEXT_REGIME_PATH", str(tmp_path / "nope.parquet"))
        assert pit_data.PITDataLoader().get_context_regime(date(2026, 3, 17)) is None


class TestIndependence:
    def test_reads_only_the_two_regime_stores(self):
        import ast, pathlib
        tree = ast.parse(pathlib.Path(cr.__file__).read_text())
        mods = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert mods <= {"__future__", "datetime", "pathlib", "polars", "loguru", "src.gold.hmm", "src.utils.atomic_io"}, mods


# ═══ transition signal (ADR-055 v2) ══════════════════════════════════════════

class TestTransitionSignal:
    """context_transition / context_transition_alert replace the retired regime_store's
    regime_transition / transition_alert as a data fact for downstream consumers (GD §5.2.1)."""

    @staticmethod
    def _built(macro_labels, market_labels):
        days = [D(2 + i) for i in range(len(macro_labels))]
        macro = _macro([(d, m, 0.4, 0.1, False) for d, m in zip(days, macro_labels)])
        market = _market([(d, k, 0.9, 0.8, False, 10) for d, k in zip(days, market_labels) if k is not None])
        return cr.build(macro, market, RUN)

    def test_columns_and_dtypes(self):
        out = self._built(["expansion"], ["risk_on"])
        assert {"context_transition", "context_transition_alert"} <= set(cr.COLUMNS)
        assert out.schema["context_transition"] == pl.Boolean
        assert out.schema["context_transition_alert"] == pl.String

    def test_processing_version_marks_the_schema_change(self):
        assert cr.PROCESSING_VERSION == "1.1"
        assert self._built(["expansion"], ["risk_on"])["processing_version"][0] == "1.1"

    def test_first_known_label_is_not_a_transition(self):
        out = self._built(["expansion"], ["risk_on"])
        assert out["context_transition"].to_list() == [False]
        assert out["context_transition_alert"].to_list() == [None]

    def test_change_in_either_axis_is_a_transition_with_a_readable_alert(self):
        out = self._built(["expansion", "expansion", "slowdown", "slowdown"],
                          ["risk_on", "mixed", "mixed", "mixed"])
        assert out["context_transition"].to_list() == [False, True, True, False]
        assert out["context_transition_alert"].to_list() == [
            None, "expansion/risk_on -> expansion/mixed", "expansion/mixed -> slowdown/mixed", None]

    def test_unchanged_label_is_never_a_transition(self):
        out = self._built(["recession"] * 4, ["risk_off"] * 4)
        assert not any(out["context_transition"].to_list())

    def test_rows_without_a_label_neither_start_nor_end_a_transition(self):
        """With tolerance 0 the market side is missing on D3/D4 -> context_label is null those days.
        The next known label (D5) is compared with the last KNOWN one (D2), not with null; the
        null days themselves are never a transition."""
        days = [D(2), D(3), D(4), D(5)]
        macro = _macro([(d, "expansion", 0.4, 0.1, False) for d in days])
        market = _market([(D(2), "risk_on", 0.9, 0.8, False, 10), (D(5), "risk_off", 0.9, -0.8, False, 10)])
        out = cr.build(macro, market, RUN, tolerance_days=0)
        assert out["context_label"].to_list() == ["expansion/risk_on", None, None, "expansion/risk_off"]
        assert out["context_transition"].to_list() == [False, False, False, True]
        assert out["context_transition_alert"].to_list() == [
            None, None, None, "expansion/risk_on -> expansion/risk_off"]
