"""
tests/unit/test_global_rates_processor.py — GMI Wave 1
Test suite untuk src/silver/global_rates_processor.py.

Dokumen referensi: Data Source & Rates Adjustment v1.0 §9
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from src.silver.global_rates_processor import (
    GlobalRatesProcessor,
    PROCESSOR_VERSION,
    _load_structural_breaks,
)


def _bronze_row(cb: str, ref_area: str, d: date, rate: float) -> dict:
    return {
        "ref_area": ref_area,
        "central_bank": cb,
        "obs_date": d,
        "rate_pct": rate,
        "_source": "bis_cbpol_d",
        "_ingested_at": datetime(2026, 6, 30).isoformat(),
    }


@pytest.fixture
def bronze_bis_dir(tmp_path):
    """Synthetic Bronze BIS directory with 2 CBs, 2 meetings each."""
    bronze_dir = tmp_path / "bronze" / "macro" / "bis_cb_rates"
    bronze_dir.mkdir(parents=True)
    rows = [
        _bronze_row("ECB", "XM", date(2026, 1, 15), 3.50),
        _bronze_row("ECB", "XM", date(2026, 4, 15), 3.75),
        _bronze_row("BOE", "GB", date(2026, 2, 1),  4.75),
        _bronze_row("BOE", "GB", date(2026, 5, 1),  4.50),
    ]
    df = pl.DataFrame(rows).with_columns([
        pl.col("obs_date").cast(pl.Date),
        pl.col("rate_pct").cast(pl.Float64),
    ])
    df.write_parquet(bronze_dir / "synthetic.parquet")
    return bronze_dir


@pytest.fixture
def processor(tmp_path, bronze_bis_dir, monkeypatch):
    """GlobalRatesProcessor wired to isolated tmp_path Bronze/Silver dirs."""
    import src.silver.global_rates_processor as mod
    monkeypatch.setattr(mod, "_BRONZE_PATH", bronze_bis_dir)
    monkeypatch.setattr(mod, "_OUTPUT_PATH", tmp_path / "silver" / "global_rates")
    return GlobalRatesProcessor()


class TestStructuralBreakRegistry:
    """Verify structural break registry loads correctly from bis_cb_rates.yaml."""

    def test_loads_3_entries(self):
        breaks = _load_structural_breaks()
        assert len(breaks) == 3

    def test_bi_rate_entry_present(self):
        breaks = _load_structural_breaks()
        bi = next(b for b in breaks if b["break_id"] == "BI_RATE")
        assert bi["central_bank"] == "BI"
        assert bi["break_date"] == date(2016, 8, 19)
        assert bi["end_date"] is None  # ongoing
        assert bi["severity"] == "HIGH"

    def test_pboc_rate_entry_has_end_date(self):
        breaks = _load_structural_breaks()
        pboc = next(b for b in breaks if b["break_id"] == "PBOC_RATE")
        assert pboc["break_date"] == date(2020, 1, 15)
        assert pboc["end_date"] == date(2021, 3, 31)
        assert pboc["severity"] == "MEDIUM"

    def test_boj_ycc_entry_present(self):
        breaks = _load_structural_breaks()
        boj = next(b for b in breaks if b["break_id"] == "BOJ_YCC")
        assert boj["central_bank"] == "BOJ"
        assert boj["break_date"] == date(2016, 9, 21)
        assert boj["end_date"] == date(2024, 3, 19)


class TestLoadBronze:
    """Test _load_bronze() deduplication and scan logic."""

    def test_loads_all_rows(self, processor):
        df = processor._load_bronze()
        assert df is not None
        assert len(df) == 4

    def test_deduplicates_by_cb_and_date_keeping_latest(self, tmp_path, monkeypatch):
        """Two ingestion runs for same (CB, date) — latest _ingested_at wins."""
        import src.silver.global_rates_processor as mod
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)
        rows = [
            _bronze_row("ECB", "XM", date(2026, 1, 15), 3.50),  # older ingestion, stale value
        ]
        rows[0]["_ingested_at"] = datetime(2026, 6, 1).isoformat()
        rows.append({**_bronze_row("ECB", "XM", date(2026, 1, 15), 3.55),
                     "_ingested_at": datetime(2026, 6, 30).isoformat()})  # newer, corrected value
        df = pl.DataFrame(rows).with_columns([
            pl.col("obs_date").cast(pl.Date), pl.col("rate_pct").cast(pl.Float64)
        ])
        df.write_parquet(bronze_dir / "dup.parquet")

        monkeypatch.setattr(mod, "_BRONZE_PATH", bronze_dir)
        proc = GlobalRatesProcessor()
        result = proc._load_bronze()
        assert len(result) == 1
        assert result["rate_pct"][0] == 3.55  # latest wins

    def test_returns_none_when_no_bronze(self, tmp_path, monkeypatch):
        import src.silver.global_rates_processor as mod
        monkeypatch.setattr(mod, "_BRONZE_PATH", tmp_path / "nonexistent")
        proc = GlobalRatesProcessor()
        assert proc._load_bronze() is None


class TestTransformForwardFill:
    """Test the core _transform() forward-fill and derived column logic."""

    def test_forward_fill_holds_last_meeting_value(self, processor):
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        ecb = result.filter(pl.col("central_bank") == "ECB").sort("observation_date")

        # Between meetings, rate should equal the last meeting's rate
        mid_period = ecb.filter(
            (pl.col("observation_date") > date(2026, 1, 15))
            & (pl.col("observation_date") < date(2026, 4, 15))
        )
        assert (mid_period["rate_pct"] == 3.50).all()

    def test_rate_updates_on_new_meeting(self, processor):
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        ecb = result.filter(pl.col("central_bank") == "ECB").sort("observation_date")
        after_second_meeting = ecb.filter(pl.col("observation_date") >= date(2026, 4, 15))
        assert (after_second_meeting["rate_pct"] == 3.75).all()

    def test_rate_bps_is_100x_rate_pct(self, processor):
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        row = result.filter(
            (pl.col("central_bank") == "ECB")
            & (pl.col("observation_date") == date(2026, 1, 15))
        )
        assert row["rate_bps"][0] == pytest.approx(350.0)

    def test_is_meeting_day_only_on_bronze_dates(self, processor):
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        meeting_days = result.filter(pl.col("is_meeting_day"))
        assert len(meeting_days) == 4  # 2 CBs x 2 meetings each
        non_meeting = result.filter(~pl.col("is_meeting_day"))
        assert len(non_meeting) == len(result) - 4

    def test_direction_change_hike_and_cut(self, processor):
        """ECB hikes (3.50->3.75, +1), BOE cuts (4.75->4.50, -1)."""
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))

        ecb_second_meeting = result.filter(
            (pl.col("central_bank") == "ECB")
            & (pl.col("observation_date") == date(2026, 4, 15))
        )
        assert ecb_second_meeting["direction_change"][0] == 1
        assert ecb_second_meeting["magnitude_bps"][0] == pytest.approx(25.0)

        boe_second_meeting = result.filter(
            (pl.col("central_bank") == "BOE")
            & (pl.col("observation_date") == date(2026, 5, 1))
        )
        assert boe_second_meeting["direction_change"][0] == -1
        assert boe_second_meeting["magnitude_bps"][0] == pytest.approx(25.0)

    def test_direction_change_zero_on_non_meeting_days(self, processor):
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        non_meeting = result.filter(~pl.col("is_meeting_day"))
        assert (non_meeting["direction_change"] == 0).all()
        assert non_meeting["magnitude_bps"].null_count() == len(non_meeting)

    def test_first_meeting_has_null_direction_and_magnitude(self, processor):
        """First observation for a CB has no prior rate to diff against."""
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        first_ecb = result.filter(
            (pl.col("central_bank") == "ECB")
            & (pl.col("observation_date") == date(2026, 1, 15))
        )
        # diff() on first element is null -> direction_change falls to 0 branch
        # since is_meeting_day=True but _rate_bps_diff is null (not >0 or <0)
        assert first_ecb["direction_change"][0] == 0

    def test_forward_fill_days_increments(self, processor):
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        ecb = result.filter(pl.col("central_bank") == "ECB").sort("observation_date")

        day0 = ecb.filter(pl.col("observation_date") == date(2026, 1, 15))
        day5 = ecb.filter(pl.col("observation_date") == date(2026, 1, 20))
        assert day0["forward_fill_days"][0] == 0
        assert day5["forward_fill_days"][0] == 5

    def test_is_stale_flag_beyond_90_days(self, processor):
        bronze_df = processor._load_bronze()
        # Push run_date far beyond 90 days from last ECB meeting (2026-04-15)
        result = processor._transform(bronze_df, run_date=date(2026, 4, 15) + timedelta(days=100))
        ecb_latest = (
            result.filter(pl.col("central_bank") == "ECB")
            .sort("observation_date")
            .tail(1)
        )
        assert ecb_latest["is_stale"][0] is True

    def test_effective_date_tracks_last_meeting(self, processor):
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        ecb = result.filter(pl.col("central_bank") == "ECB").sort("observation_date")
        mid = ecb.filter(pl.col("observation_date") == date(2026, 3, 1))
        assert mid["effective_date"][0] == date(2026, 1, 15)

    def test_vintage_date_equals_run_date(self, processor):
        bronze_df = processor._load_bronze()
        run_date = date(2026, 6, 30)
        result = processor._transform(bronze_df, run_date=run_date)
        assert (result["vintage_date"] == run_date).all()

    def test_processing_version_stamped(self, processor):
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        assert (result["processing_version"] == PROCESSOR_VERSION).all()

    def test_final_schema_columns(self, processor):
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        expected_cols = {
            "central_bank", "observation_date", "rate_pct", "rate_bps",
            "effective_date", "is_meeting_day", "direction_change",
            "magnitude_bps", "has_structural_break", "structural_break_id",
            "forward_fill_days", "is_stale", "vintage_date", "processing_version",
        }
        assert set(result.columns) == expected_cols


class TestStructuralBreakApplication:
    """Test _apply_structural_breaks() flags rows within registered windows."""

    def test_bi_flagged_ongoing_break(self, tmp_path, monkeypatch):
        """BI_RATE break has no end_date — everything from 2016-08-19 onward flagged."""
        import src.silver.global_rates_processor as mod
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)
        rows = [_bronze_row("BI", "ID", date(2026, 1, 1), 6.00)]
        df = pl.DataFrame(rows).with_columns([
            pl.col("obs_date").cast(pl.Date), pl.col("rate_pct").cast(pl.Float64)
        ])
        df.write_parquet(bronze_dir / "bi.parquet")
        monkeypatch.setattr(mod, "_BRONZE_PATH", bronze_dir)

        proc = GlobalRatesProcessor()
        bronze_df = proc._load_bronze()
        result = proc._transform(bronze_df, run_date=date(2026, 3, 1))
        assert (result["has_structural_break"]).all()
        assert (result["structural_break_id"] == "BI_RATE").all()

    def test_ecb_not_flagged_no_registered_break(self, processor):
        """ECB has no structural break entry — must never be flagged."""
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        ecb = result.filter(pl.col("central_bank") == "ECB")
        assert not ecb["has_structural_break"].any()
        assert ecb["structural_break_id"].null_count() == len(ecb)

    def test_boj_ycc_break_has_bounded_window(self, tmp_path, monkeypatch):
        """BOJ_YCC break has end_date 2024-03-19 — dates after must NOT be flagged."""
        import src.silver.global_rates_processor as mod
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)
        rows = [_bronze_row("BOJ", "JP", date(2024, 1, 1), 0.10)]
        df = pl.DataFrame(rows).with_columns([
            pl.col("obs_date").cast(pl.Date), pl.col("rate_pct").cast(pl.Float64)
        ])
        df.write_parquet(bronze_dir / "boj.parquet")
        monkeypatch.setattr(mod, "_BRONZE_PATH", bronze_dir)

        proc = GlobalRatesProcessor()
        bronze_df = proc._load_bronze()
        result = proc._transform(bronze_df, run_date=date(2026, 6, 30))
        boj = result.filter(pl.col("central_bank") == "BOJ").sort("observation_date")

        within_window = boj.filter(pl.col("observation_date") == date(2024, 3, 1))
        after_window  = boj.filter(pl.col("observation_date") == date(2024, 4, 1))
        assert within_window["has_structural_break"][0] is True
        assert after_window["has_structural_break"][0] is False


class TestSaveAndRun:
    """Test _save() atomic write and run() end-to-end orchestration."""

    def test_save_writes_parquet(self, processor, tmp_path):
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        out_path = processor._save(result, run_date=date(2026, 6, 30))
        assert out_path.exists()
        reloaded = pl.read_parquet(out_path)
        assert len(reloaded) == len(result)

    def test_save_uses_atomic_write_no_tmp_left(self, processor, tmp_path):
        """Supplementary Design G2: atomic write leaves no .tmp file behind."""
        bronze_df = processor._load_bronze()
        result = processor._transform(bronze_df, run_date=date(2026, 6, 30))
        processor._save(result, run_date=date(2026, 6, 30))
        tmp_files = list((tmp_path / "silver" / "global_rates").glob("*.tmp"))
        assert tmp_files == []

    def test_run_end_to_end_returns_path(self, processor):
        result_path = processor.run(run_date=date(2026, 6, 30))
        assert result_path is not None
        assert result_path.exists()

    def test_run_returns_none_when_no_bronze_data(self, tmp_path, monkeypatch):
        import src.silver.global_rates_processor as mod
        monkeypatch.setattr(mod, "_BRONZE_PATH", tmp_path / "nonexistent")
        monkeypatch.setattr(mod, "_OUTPUT_PATH", tmp_path / "silver_out")
        proc = GlobalRatesProcessor()
        result = proc.run(run_date=date(2026, 6, 30))
        assert result is None

    def test_module_run_function(self, processor, tmp_path, monkeypatch):
        import src.silver.global_rates_processor as mod
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(mod, "GlobalRatesProcessor", lambda: processor)
            mod.run(run_date=date(2026, 6, 30))
        out_file = tmp_path / "silver" / "global_rates" / "global_rates_policy.parquet"
        assert out_file.exists()
