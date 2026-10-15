"""
tests/unit/test_hmm_raw_market.py — ADR-051 (GMI-HMM2-001): src/gold/hmm/raw_market.py.

Synthetic Silver (FRED snapshot + per-symbol 1D files) written into tmp_path; build()
takes explicit roots so nothing touches data/ or config/. The real fred_series.yaml
is exercised for the production column contract.

What these pin, in order of how much a silent break would cost:
  * no lookahead — a row's breadth/guards never depend on later dates
  * Layer 1 membership is an EXPLICIT FILE LIST (the stray us_stocks/symbol=CL
    directory measured on the live repo must never enter the breadth universe)
  * the thin-day (min_symbols) and partial-day (coverage ratio) guards null breadth but keep
    us_n_symbols / us_n_eligible / us_coverage_ratio auditable — validity is reproducible from
    one stored row plus the versioned thresholds
  * trading-day rows (holidays absent), local-midnight timestamps keep their date
"""
import statistics
from datetime import date, datetime, timedelta
from math import sqrt

import polars as pl
import pytest
import yaml

from src.gold.hmm import raw_market as rmk
from src.gold.hmm.config import BreadthSettings, market_config, validate_market_config

ST = BreadthSettings(market="us_stocks", timeframe="1D", min_symbols=3, min_coverage_ratio=0.8,
                     rms_window=3, guard_version="t")


# ── builders ─────────────────────────────────────────────────────────────────

def _rets(spec: dict) -> pl.DataFrame:
    """{date: [log_return per symbol, ...]} -> returns frame (symbol, date, log_return)."""
    rows = [(f"S{i}", d, r) for d, rs in spec.items() for i, r in enumerate(rs)]
    return pl.DataFrame(rows, orient="row", schema={"symbol": pl.String, "date": pl.Date, "log_return": pl.Float64})


def _weekdays(start: date, end: date, skip=()) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5 and d not in skip:
            out.append(d)
        d += timedelta(days=1)
    return out


def _write_symbol(root, sym, rows, market="us_stocks", tf="1D", tz="America/New_York"):
    """rows: (date, log_return, is_clean). Stamped at LOCAL midnight like real Silver."""
    df = pl.DataFrame(
        {"timestamp": [datetime(d.year, d.month, d.day) for d, _, _ in rows],
         "log_return": [r for _, r, _ in rows], "is_clean": [c for _, _, c in rows]},
        schema={"timestamp": pl.Datetime("ms"), "log_return": pl.Float64, "is_clean": pl.Boolean},
    )
    if tz:
        df = df.with_columns(pl.col("timestamp").dt.replace_time_zone(tz))
    d = root / market / f"symbol={sym}"
    d.mkdir(parents=True, exist_ok=True)
    df.write_parquet(d / f"{sym}_{tf}_silver.parquet")


def _ret(i, k):
    """Deterministic, mixed-sign synthetic return for symbol i on day k."""
    return 0.001 * (((i * 7 + k * 3) % 11) - 5)


def _populate(root, n_symbols, days, skip_first_clean=True):
    syms = [f"S{i:03d}" for i in range(n_symbols)]
    for i, s in enumerate(syms):
        rows = []
        for k, d in enumerate(days):
            if k == 0 and skip_first_clean:
                rows.append((d, None, None))             # first bar of a real Silver file: NULL return / is_clean
            else:
                rows.append((d, _ret(i, k), True))
        _write_symbol(root, s, rows)
    return syms


def _snapshot(tmp_path, snap_date, series, days):
    rows = []
    for sid in series:
        for k, d in enumerate(days):
            rows.append((sid, d, (d + timedelta(days=1)).isoformat(), 4.0 + 0.01 * k, "t"))
    df = pl.DataFrame(rows, orient="row", schema={
        "series_id": pl.String, "observation_date": pl.Date, "release_date": pl.String,
        "value": pl.Float64, "_ingested_at": pl.String})
    silver = tmp_path / "fred"
    silver.mkdir(exist_ok=True)
    df.write_parquet(silver / f"fred_{snap_date}_silver.parquet")
    return silver


def _mini_cfg(min_symbols=3):
    raw = {
        "series": [{"id": "AAA", "frequency": "daily"}],
        "hmm": {"pit": {"basis": "recorded", "frequency_floor_days": {"daily": 1, "weekly": 7, "monthly": 45,
                                                                       "quarterly": 120},
                        "series_lag_days": {}}},
        "hmm_market": {
            "version": "t",
            "features": [
                {"feature": "AAA_D1", "source": "fred", "series": "AAA", "transform": "diff_1"},
                {"feature": "up", "source": "silver_breadth", "metric": "share_up"},
                {"feature": "med", "source": "silver_breadth", "metric": "median_return"},
                {"feature": "disp", "source": "silver_breadth", "metric": "dispersion"},
                {"feature": "rv", "source": "silver_breadth", "metric": "rms_median_return"},
            ],
            "breadth": {"market": "us_stocks", "timeframe": "1D", "min_symbols": min_symbols,
                        "min_coverage_ratio": 0.8, "rms_window": 3, "guard_version": "t"},
        },
    }
    return validate_market_config(raw)


DAYS = _weekdays(date(2026, 1, 5), date(2026, 3, 31), skip={date(2026, 2, 16)})     # Presidents' Day absent


@pytest.fixture
def env(tmp_path):
    root = tmp_path / "ohlcv"
    syms = _populate(root, 6, DAYS)
    fred = _snapshot(tmp_path, "2026-01-02", ["AAA"], _weekdays(date(2025, 12, 1), date(2026, 3, 31)))
    return {"root": root, "syms": syms, "fred": fred, "cfg": _mini_cfg(), "tmp": tmp_path}


def _build(env, run_date=date(2026, 3, 31), **kw):
    return rmk.build(run_date, silver_dir=env["fred"], ohlcv_root=env["root"], cfg=env["cfg"],
                     symbols=kw.pop("symbols", env["syms"]), **kw)


# ═══ compute_breadth ═════════════════════════════════════════════════════════

class TestComputeBreadth:
    def test_known_values(self):
        r = [0.01, -0.02, 0.03, 0.00, 0.02]
        out = rmk.compute_breadth(_rets({date(2026, 3, 2): r}), ST).row(0, named=True)
        assert out["us_n_symbols"] == 5 and out["us_n_eligible"] == 5 and out["us_breadth_valid"] is True
        assert out["us_coverage_ratio"] == pytest.approx(1.0)
        assert out["_share_up"] == pytest.approx(3 / 5)                  # 0.00 is NOT up
        assert out["_median_return"] == pytest.approx(0.01)
        assert out["_dispersion"] == pytest.approx(statistics.stdev(r))  # sample std (ddof=1)

    def test_dtypes_and_one_row_per_date_sorted(self):
        spec = {date(2026, 3, 3): [0.01] * 4, date(2026, 3, 2): [0.02] * 4}
        out = rmk.compute_breadth(_rets(spec), ST)
        assert out["as_of_date"].to_list() == [date(2026, 3, 2), date(2026, 3, 3)]
        assert out.columns == ["as_of_date", "us_n_symbols", "us_n_eligible", "us_coverage_ratio", "us_breadth_valid",
                               "_share_up", "_median_return", "_dispersion", "_rms_median_return"]
        assert out.schema["us_n_symbols"] == pl.Int32 and out.schema["us_n_eligible"] == pl.Int32
        assert out.schema["us_coverage_ratio"] == pl.Float64 and out.schema["us_breadth_valid"] == pl.Boolean

    def test_thin_day_nulls_breadth_but_keeps_the_counts(self):
        out = rmk.compute_breadth(_rets({date(2026, 3, 2): [0.01, -0.01]}), ST).row(0, named=True)   # 2 < min_symbols 3
        assert out["us_n_symbols"] == 2 and out["us_n_eligible"] == 2 and out["us_breadth_valid"] is False
        assert out["us_coverage_ratio"] == pytest.approx(1.0)            # guard A failed alone, not guard B
        assert all(out[c] is None for c in ("_share_up", "_median_return", "_dispersion", "_rms_median_return"))

    def _history(self, counts):
        """Day k carries the first counts[k] symbols (S0..), so eligibility accumulates from day 0."""
        days = _weekdays(date(2026, 3, 2), date(2026, 4, 30))[: len(counts)]
        return days, _rets({d: [0.001 * (j % 3 - 1) for j in range(c)] for d, c in zip(days, counts)})

    def test_partial_day_is_nulled_by_the_coverage_ratio(self):
        days, r = self._history([10] * 6 + [7])                  # 7 clean of 10 eligible = 0.7 < 0.8
        out = rmk.compute_breadth(r, ST).sort("as_of_date")
        assert out["us_breadth_valid"].to_list() == [True] * 6 + [False]
        last = out.row(6, named=True)
        assert (last["us_n_symbols"], last["us_n_eligible"]) == (7, 10)   # counts survive for audit
        assert last["us_coverage_ratio"] == pytest.approx(0.7) and last["_share_up"] is None

    def test_exactly_at_the_ratio_is_valid(self):
        _, r = self._history([10] * 6 + [8])                     # 8 / 10 == 0.8 -> not partial
        assert rmk.compute_breadth(r, ST).sort("as_of_date")["us_breadth_valid"][-1] is True

    def test_first_date_is_valid_when_all_its_symbols_are_new(self):
        _, r = self._history([4])
        out = rmk.compute_breadth(r, ST).row(0, named=True)
        assert out["us_breadth_valid"] is True and out["us_coverage_ratio"] == pytest.approx(1.0)

    def test_later_listed_names_do_not_count_against_earlier_dates(self):
        """Eligible = symbols whose first clean return is on or before the date. 5 names exist for
        the first 3 days, 5 more list on day 4: early days stay 5/5, day 4 onward is 10/10 — a
        flat 'universe size' denominator would call the early years 50% covered."""
        _, r = self._history([5, 5, 5, 10, 10])
        out = rmk.compute_breadth(r, ST).sort("as_of_date")
        assert out["us_n_eligible"].to_list() == [5, 5, 5, 10, 10]
        assert out["us_coverage_ratio"].to_list() == [pytest.approx(1.0)] * 5
        assert out["us_breadth_valid"].to_list() == [True] * 5

    def test_a_symbol_that_stops_reporting_lowers_coverage_but_stays_eligible(self):
        _, r = self._history([10, 10, 10, 7, 7])                 # three names stop on day 4 (delisted / late ingest)
        out = rmk.compute_breadth(r, ST).sort("as_of_date")
        assert out["us_n_eligible"].to_list() == [10] * 5        # eligibility never shrinks
        assert out["us_breadth_valid"].to_list() == [True, True, True, False, False]

    def test_validity_is_reproducible_from_the_stored_row_alone(self):
        """The reason the trailing-median rule was replaced: every flag must be re-derivable from
        (us_n_symbols, us_n_eligible, us_coverage_ratio) and the versioned thresholds, with no
        neighbouring rows needed."""
        import random
        rnd = random.Random(11)
        counts = [rnd.choice([10, 10, 10, 9, 8, 7, 2]) for _ in range(40)]
        _, r = self._history(counts)
        for row in rmk.compute_breadth(r, ST).iter_rows(named=True):
            assert row["us_coverage_ratio"] == pytest.approx(row["us_n_symbols"] / row["us_n_eligible"])
            assert row["us_breadth_valid"] == (row["us_n_symbols"] >= ST.min_symbols
                                               and row["us_coverage_ratio"] >= ST.min_coverage_ratio)
            assert 0 < row["us_coverage_ratio"] <= 1.0

    def test_eligible_count_never_decreases(self):
        import random
        rnd = random.Random(3)
        _, r = self._history([rnd.choice([10, 6, 8, 10, 4]) for _ in range(30)])
        el = rmk.compute_breadth(r, ST).sort("as_of_date")["us_n_eligible"].to_list()
        assert el == sorted(el)

    def test_no_lookahead_prefix_equals_full(self):
        """Guards, eligibility and the rolling window only ever look BACKWARD: computing on a
        prefix of the data gives the same rows as computing on everything and truncating."""
        import random
        rnd = random.Random(7)
        counts = [rnd.choice([10, 10, 10, 6, 2, 9]) for _ in range(30)]
        days, r = self._history(counts)
        full = rmk.compute_breadth(r, ST).sort("as_of_date")
        for cut in (5, 12, 25):
            pre = rmk.compute_breadth(r.filter(pl.col("date") <= days[cut - 1]), ST).sort("as_of_date")
            assert pre.equals(full.head(cut)), cut

    def test_rms_median_return_is_root_mean_square_over_the_window(self):
        meds = [0.01, 0.02, -0.03, 0.04, 0.05]
        days = _weekdays(date(2026, 3, 2), date(2026, 3, 6))
        out = rmk.compute_breadth(_rets({d: [m, m, m] for d, m in zip(days, meds)}), ST).sort("as_of_date")
        v = out["_rms_median_return"].to_list()
        assert v[0] is None and v[1] is None                      # window of 3 not yet full
        assert v[2] == pytest.approx(sqrt((0.01**2 + 0.02**2 + 0.03**2) / 3))
        assert v[3] == pytest.approx(sqrt((0.02**2 + 0.03**2 + 0.04**2) / 3))
        assert v[4] == pytest.approx(sqrt((0.03**2 + 0.04**2 + 0.05**2) / 3))

    def test_rms_is_zero_mean_by_construction(self):
        """A constant +1% median is NOT 'zero volatility': an RMS (no mean subtracted) reads 0.01,
        which is why the statistic is not called realised volatility."""
        days = _weekdays(date(2026, 3, 2), date(2026, 3, 4))
        out = rmk.compute_breadth(_rets({d: [0.01] * 3 for d in days}), ST).sort("as_of_date")
        assert out["_rms_median_return"][-1] == pytest.approx(0.01)

    def test_invalid_day_neither_contributes_to_nor_blanks_the_rms_window(self):
        meds = {date(2026, 3, 2): 0.01, date(2026, 3, 3): 0.02, date(2026, 3, 4): -0.03,
                date(2026, 3, 6): 0.04}
        spec = {d: [m, m, m] for d, m in meds.items()}
        spec[date(2026, 3, 5)] = [0.50, 0.50]                    # THIN day (2 < 3) with a wild median
        out = rmk.compute_breadth(_rets(spec), ST).sort("as_of_date")
        by = {r["as_of_date"]: r for r in out.iter_rows(named=True)}
        assert by[date(2026, 3, 5)]["_rms_median_return"] is None
        assert by[date(2026, 3, 6)]["_rms_median_return"] == pytest.approx(
            sqrt((0.02**2 + 0.03**2 + 0.04**2) / 3))            # the 0.50 day is skipped, not blanked


# ═══ load_returns / silver_files ═════════════════════════════════════════════

class TestLoadReturns:
    def test_only_clean_finite_non_null_returns_survive(self, tmp_path):
        d = date(2026, 3, 2)
        rows = [(d + timedelta(days=i), r, c) for i, (r, c) in enumerate([
            (None, None),            # first bar of a real Silver file
            (0.01, True),
            (0.02, False),           # not clean
            (0.03, None),            # is_clean NULL
            (None, True),            # null return
            (float("inf"), True),    # non-finite
            (0.04, True),
        ])]
        _write_symbol(tmp_path, "AAA", rows)
        out = rmk.load_returns(rmk.silver_files(tmp_path, "us_stocks", "1D", ["AAA"]))
        assert out["log_return"].to_list() == [0.01, 0.04]

    @pytest.mark.parametrize("tz", ["America/New_York", "UTC", "Asia/Jakarta", None])
    def test_trading_date_is_the_stamps_own_date_whatever_the_timezone(self, tmp_path, tz):
        """Silver 1D bars are stamped at LOCAL midnight. Converting to another zone can
        shift the date (UTC-midnight -> New York is the PREVIOUS day) — never do that."""
        _write_symbol(tmp_path, "AAA", [(date(2026, 3, 2), 0.01, True)], tz=tz)
        out = rmk.load_returns(rmk.silver_files(tmp_path, "us_stocks", "1D", ["AAA"]))
        assert out["date"].to_list() == [date(2026, 3, 2)]

    def test_files_with_different_timezones_concatenate(self, tmp_path):
        _write_symbol(tmp_path, "AAA", [(date(2026, 3, 2), 0.01, True)], tz="America/New_York")
        _write_symbol(tmp_path, "BBB", [(date(2026, 3, 2), 0.02, True)], tz="UTC")
        _write_symbol(tmp_path, "CCC", [(date(2026, 3, 2), 0.03, True)], tz=None)
        out = rmk.load_returns(rmk.silver_files(tmp_path, "us_stocks", "1D", ["AAA", "BBB", "CCC"]))
        assert sorted(out["symbol"].to_list()) == ["AAA", "BBB", "CCC"]

    def test_symbol_comes_from_the_requested_list_not_a_glob(self, tmp_path):
        _write_symbol(tmp_path, "AAA", [(date(2026, 3, 2), 0.01, True)])
        _write_symbol(tmp_path, "STRAY", [(date(2026, 3, 2), 0.99, True)])
        files = rmk.silver_files(tmp_path, "us_stocks", "1D", ["AAA"])
        assert [s for s, _ in files] == ["AAA"]

    def test_symbols_without_a_file_are_skipped_not_errors(self, tmp_path):
        _write_symbol(tmp_path, "AAA", [(date(2026, 3, 2), 0.01, True)])
        assert [s for s, _ in rmk.silver_files(tmp_path, "us_stocks", "1D", ["AAA", "GONE"])] == ["AAA"]

    def test_timeframe_is_part_of_the_path(self, tmp_path):
        _write_symbol(tmp_path, "AAA", [(date(2026, 3, 2), 0.01, True)], tf="1H")
        assert rmk.silver_files(tmp_path, "us_stocks", "1D", ["AAA"]) == []

    def test_empty_file_list_gives_typed_empty_frame(self):
        out = rmk.load_returns([])
        assert out.height == 0 and out.schema == pl.Schema({"symbol": pl.String, "date": pl.Date, "log_return": pl.Float64})


class TestLayer1Symbols:
    def test_real_loader_contract_the_stray_cl_exclusion_depends_on(self):
        """The loader's Layer 1 us_stocks list must not contain CL (the crude ticker
        shares the string with the Colgate stock; live Silver still holds a stale
        us_stocks/symbol=CL) nor the reclassified SPX/VIX/DXY."""
        syms = rmk.layer1_symbols("us_stocks")
        assert len(syms) > 400 and "AAPL" in syms and "BRK-B" in syms
        assert not {"CL", "SPX", "VIX", "DXY"} & set(syms)
        assert syms == sorted(syms)


# ═══ build ═══════════════════════════════════════════════════════════════════

class TestBuild:
    def test_schema_order_dtypes_lineage(self, env):
        df = _build(env)
        assert df.columns == ["as_of_date", "AAA_D1", "up", "med", "disp", "rv",
                              "obs_date__AAA_D1", "available_on__AAA_D1", "avail_basis__AAA_D1",
                              "us_n_symbols", "us_n_eligible", "us_coverage_ratio", "us_breadth_valid",
                              "computation_date", "processing_version", "source_snapshot", "pit_basis",
                              "breadth_guard_version", "breadth_guard_fingerprint", "breadth_symbol_files"]
        assert df.schema["as_of_date"] == pl.Date and df.schema["obs_date__AAA_D1"] == pl.Date
        assert df.schema["available_on__AAA_D1"] == pl.Date and df.schema["avail_basis__AAA_D1"] == pl.String
        assert df.schema["us_n_symbols"] == pl.Int32 and df.schema["us_n_eligible"] == pl.Int32
        assert df.schema["breadth_symbol_files"] == pl.Int32
        assert df["pit_basis"].unique().to_list() == ["recorded"]
        assert df["breadth_guard_version"].unique().to_list() == ["t"]
        assert df["breadth_guard_fingerprint"].unique().to_list() == [env["cfg"].breadth.fingerprint()]
        assert df["computation_date"].unique().to_list() == [date(2026, 3, 31)]
        assert df["processing_version"].unique().to_list() == [rmk.PROCESSING_VERSION]
        assert df["source_snapshot"].unique().to_list() == ["fred_2026-01-02_silver.parquet"]
        assert df["breadth_symbol_files"].unique().to_list() == [6]

    def test_rows_are_trading_days_holiday_absent_weekends_absent(self, env):
        df = _build(env)
        dates = df["as_of_date"].to_list()
        assert date(2026, 2, 16) not in dates                       # market holiday: no bars -> no row
        assert all(d.weekday() < 5 for d in dates)
        # first bar of each file has NULL return -> first trading day has no clean return -> no row
        assert dates == DAYS[1:]

    def test_breadth_features_land_under_their_configured_names(self, env):
        df = _build(env)
        d = DAYS[10]
        k = 10
        expected = [_ret(i, k) for i in range(6)]
        row = df.filter(pl.col("as_of_date") == d).row(0, named=True)
        assert row["up"] == pytest.approx(sum(1 for r in expected if r > 0) / 6)
        assert row["med"] == pytest.approx(statistics.median(expected))
        assert row["disp"] == pytest.approx(statistics.stdev(expected))
        assert row["us_n_symbols"] == 6 and row["us_n_eligible"] == 6 and row["us_breadth_valid"] is True
        assert row["us_coverage_ratio"] == pytest.approx(1.0)

    def test_fred_feature_is_pit_gated_and_carried_to_trading_days(self, env):
        df = _build(env)
        # AAA rises 0.01 per observation -> diff_1 is 0.01 wherever defined
        vals = df["AAA_D1"].drop_nulls().to_list()
        assert vals and all(v == pytest.approx(0.01) for v in vals)
        # a Monday row carries Friday's observation, usable since Saturday (release + 1): 3 days old
        mon = df.filter(pl.col("as_of_date") == date(2026, 3, 2)).row(0, named=True)
        assert mon["obs_date__AAA_D1"] == date(2026, 2, 27) and (mon["as_of_date"] - mon["obs_date__AAA_D1"]).days == 3
        assert mon["available_on__AAA_D1"] == date(2026, 2, 28) and mon["avail_basis__AAA_D1"] == "estimated"
        # the day after the holiday carries across it
        tue = df.filter(pl.col("as_of_date") == date(2026, 2, 17)).row(0, named=True)
        assert tue["AAA_D1"] == pytest.approx(0.01)

    def test_run_date_bounds_the_panel(self, env):
        df = _build(env, run_date=date(2026, 2, 20))
        assert df["as_of_date"].max() == date(2026, 2, 20)

    def test_no_lookahead_short_run_equals_prefix_of_full_run(self, env):
        full = _build(env)
        cut = date(2026, 2, 25)
        short = _build(env, run_date=cut)
        cols = ["as_of_date", "AAA_D1", "up", "med", "disp", "rv", "obs_date__AAA_D1", "available_on__AAA_D1",
                "us_n_symbols", "us_n_eligible", "us_coverage_ratio", "us_breadth_valid"]
        assert short.select(cols).equals(full.filter(pl.col("as_of_date") <= cut).select(cols))

    def test_stray_directory_never_enters_the_universe(self, env):
        """REGRESSION (live repo, 6 Oct 2026): market_ohlcv/us_stocks held 543 dirs vs 542
        registered — the extra `symbol=CL` is stale Colgate data frozen at 2026-09-10.
        A glob-based reader folds it into every breadth number; an explicit file list can't."""
        clean = _build(env)
        _write_symbol(env["root"], "CL", [(d, 0.25, True) for d in DAYS[:50]])     # wild, stale, unregistered
        with_stray = _build(env)
        assert with_stray.equals(clean)
        assert with_stray["breadth_symbol_files"].unique().to_list() == [6]

    def test_context_layer_files_are_not_read(self, env):
        clean = _build(env)
        _write_symbol(env["root"], "SPY", [(d, 0.30, True) for d in DAYS], market="context")
        assert _build(env).equals(clean)

    def test_thin_early_universe_nulls_breadth_without_dropping_the_row(self, env):
        """Only 2 symbols have data on the first usable dates (< min_symbols=3)."""
        root = env["tmp"] / "thin"
        for i in range(2):
            _write_symbol(root, f"T{i}", [(DAYS[1], 0.01, True), (DAYS[2], 0.02, True)])
        for i in range(2, 5):
            _write_symbol(root, f"T{i}", [(DAYS[2], 0.03, True)])
        syms = [f"T{i}" for i in range(5)]
        df = rmk.build(date(2026, 3, 31), silver_dir=env["fred"], ohlcv_root=root, cfg=env["cfg"], symbols=syms)
        thin = df.filter(pl.col("as_of_date") == DAYS[1]).row(0, named=True)
        assert thin["us_n_symbols"] == 2 and thin["us_breadth_valid"] is False and thin["up"] is None
        assert thin["us_n_eligible"] == 2
        ok = df.filter(pl.col("as_of_date") == DAYS[2]).row(0, named=True)
        assert ok["us_n_symbols"] == 5 and ok["us_breadth_valid"] is True and ok["up"] is not None


class TestPartialLatestDay:
    def test_late_ingest_of_the_latest_bar_nulls_that_days_breadth_but_keeps_the_row(self, env):
        """3 of 6 symbols have not ingested the final bar yet: 3 >= min_symbols (3), so only the
        coverage guard (3/6 = 0.5 < 0.8) can catch it — and it must, or half a market would be
        reported as 'the market'."""
        for i, s in enumerate(env["syms"][:3]):
            _write_symbol(env["root"], s, [(d, None, None) if k == 0 else (d, _ret(i, k), True)
                                           for k, d in enumerate(DAYS[:-1])])
        df = _build(env)
        last = df.filter(pl.col("as_of_date") == DAYS[-1]).row(0, named=True)
        assert last["us_n_symbols"] == 3 and last["us_n_eligible"] == 6
        assert last["us_coverage_ratio"] == pytest.approx(0.5) and last["us_breadth_valid"] is False
        assert all(last[c] is None for c in ("up", "med", "disp", "rv"))
        prev = df.filter(pl.col("as_of_date") == DAYS[-2]).row(0, named=True)
        assert prev["us_breadth_valid"] is True and prev["up"] is not None


class TestBuildFailsLoudly:
    def test_no_fred_snapshot(self, env):
        with pytest.raises(rmk.RawMarketError, match="no FRED Silver snapshot"):
            _build(env, run_date=date(2025, 12, 1))

    def test_configured_fred_series_missing(self, env):
        (env["tmp"] / "x").mkdir()
        silver = _snapshot(env["tmp"] / "x", "2026-01-02", ["OTHER"], _weekdays(date(2025, 12, 1), date(2026, 3, 31)))
        with pytest.raises(rmk.RawMarketError, match=r"\['AAA'\]"):
            rmk.build(date(2026, 3, 31), silver_dir=silver, ohlcv_root=env["root"], cfg=env["cfg"], symbols=env["syms"])

    def test_too_few_symbol_files(self, env):
        with pytest.raises(rmk.RawMarketError, match="Layer 1"):
            _build(env, symbols=env["syms"][:2])                    # 2 files < min_symbols 3

    def test_no_returns_on_or_before_run_date(self, env):
        with pytest.raises(rmk.RawMarketError, match="no clean Layer 1"):
            _build(env, run_date=DAYS[0])                           # only the NULL first bar exists

    def test_config_without_breadth_feature(self, env):
        raw = {
            "series": [{"id": "AAA", "frequency": "daily"}],
            "hmm": {"pit": {"basis": "recorded", "frequency_floor_days": {"daily": 1}, "series_lag_days": {}}},
            "hmm_market": {"features": [{"feature": "A", "source": "fred", "series": "AAA", "transform": "level"}]},
        }
        with pytest.raises(rmk.RawMarketError, match="trading-day index"):
            rmk.build(date(2026, 3, 31), silver_dir=env["fred"], ohlcv_root=env["root"],
                      cfg=validate_market_config(raw), symbols=env["syms"])


class TestShrunkUniverseWarning:
    def test_warns_when_fewer_than_95pct_of_the_universe_has_files(self, env):
        from loguru import logger
        msgs = []
        sink = logger.add(lambda m: msgs.append(str(m)), level="WARNING")
        try:
            _build(env, symbols=env["syms"] + [f"MISSING{i}" for i in range(4)])      # 6 / 10 present
        finally:
            logger.remove(sink)
        assert any("breadth universe shrunk" in m and "6/10" in m for m in msgs)


# ═══ run ═════════════════════════════════════════════════════════════════════

class TestRun:
    def _patch(self, monkeypatch, env):
        monkeypatch.setattr(rmk, "RAW_MARKET_PATH", env["tmp"] / "out" / "hmm" / "raw_market.parquet")
        monkeypatch.setattr(rmk, "SILVER_OHLCV_ROOT", env["root"])
        monkeypatch.setattr(rmk.fred_pit, "SILVER_MACRO_PATH", env["fred"])
        monkeypatch.setattr(rmk, "layer1_symbols", lambda market: env["syms"])
        cfg_path = env["tmp"] / "fred_series.yaml"
        cfg_path.write_text(yaml.safe_dump({
            "series": [{"id": "AAA", "frequency": "daily"}],
            "hmm": {"pit": {"basis": "recorded", "frequency_floor_days": {"daily": 1, "weekly": 7, "monthly": 45,
                                                                           "quarterly": 120},
                            "series_lag_days": {}}},
            "hmm_market": {
                "features": [
                    {"feature": "AAA_D1", "source": "fred", "series": "AAA", "transform": "diff_1"},
                    {"feature": "up", "source": "silver_breadth", "metric": "share_up"}],
                "breadth": {"market": "us_stocks", "timeframe": "1D", "min_symbols": 3,
                            "min_coverage_ratio": 0.8, "rms_window": 3, "guard_version": "t"}}}))
        monkeypatch.setattr("src.gold.hmm.config.FRED_SERIES_PATH", cfg_path)

    def test_writes_store_and_is_idempotent(self, env, monkeypatch):
        self._patch(monkeypatch, env)
        rmk.run(date(2026, 3, 31))
        first = pl.read_parquet(rmk.RAW_MARKET_PATH)
        rmk.run(date(2026, 3, 31))
        assert first.height > 0 and first.equals(pl.read_parquet(rmk.RAW_MARKET_PATH))

    def test_atomic_write_leaves_no_tmp_files(self, env, monkeypatch):
        self._patch(monkeypatch, env)
        rmk.run(date(2026, 3, 31))
        assert [p.name for p in rmk.RAW_MARKET_PATH.parent.iterdir()] == ["raw_market.parquet"]

    def test_failure_does_not_clobber_existing_store(self, env, monkeypatch):
        self._patch(monkeypatch, env)
        rmk.run(date(2026, 3, 31))
        before = rmk.RAW_MARKET_PATH.read_bytes()
        monkeypatch.setattr(rmk, "SILVER_OHLCV_ROOT", env["tmp"] / "nothing_here")
        with pytest.raises(rmk.RawMarketError):
            rmk.run(date(2026, 3, 31))
        assert rmk.RAW_MARKET_PATH.read_bytes() == before


class TestRealRegistryContract:
    def test_production_columns_and_order(self, tmp_path):
        cfg = market_config()
        days = _weekdays(date(2026, 1, 5), date(2026, 3, 31))
        root = tmp_path / "ohlcv"
        syms = _populate(root, 120, days)                           # > min_symbols (100)
        fred = _snapshot(tmp_path, "2026-01-02", cfg.required_series(), _weekdays(date(2025, 6, 1), date(2026, 3, 31)))
        df = rmk.build(date(2026, 3, 31), silver_dir=fred, ohlcv_root=root, symbols=syms)
        names = cfg.feature_names()
        fred_names = [f.feature for f in cfg.fred_features()]
        assert df.columns == ["as_of_date"] + names + [f"obs_date__{n}" for n in fred_names] + \
            [f"available_on__{n}" for n in fred_names] + [f"avail_basis__{n}" for n in fred_names] + \
            ["us_n_symbols", "us_n_eligible", "us_coverage_ratio", "us_breadth_valid", "computation_date",
             "processing_version", "source_snapshot", "pit_basis", "breadth_guard_version",
             "breadth_guard_fingerprint", "breadth_symbol_files"]
        assert df["breadth_guard_fingerprint"][0] == cfg.breadth.fingerprint()
        assert len(names) == 10 and df["us_breadth_valid"].all() and df["breadth_symbol_files"][0] == 120
