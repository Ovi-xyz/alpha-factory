"""tests/unit/test_cross_asset_robust.py — XAE-R08 pure helpers (robust.py), validated by simulation."""

import numpy as np
import polars as pl
import pytest

from src.gold.cross_asset.robust import (
    calibrated_p, null_corr_quantile, rank_normal, winsorise_columns, winsorise_mad,
)


class TestWinsorise:

    def test_clips_a_crash_bar_to_k_robust_sigma(self):
        rng = np.random.default_rng(0)
        x = rng.normal(0, 0.01, 200)
        x[50] = -0.16                                   # a -16% bar
        w = winsorise_mad(x, 3.0)
        mad = np.median(np.abs(x - np.median(x))) * 1.4826
        assert w[50] == pytest.approx(np.median(x) - 3 * mad)
        assert np.delete(w, 50) == pytest.approx(np.delete(np.clip(x, np.median(x) - 3 * mad,
                                                                    np.median(x) + 3 * mad), 50))

    def test_ordinary_data_is_barely_touched(self):
        rng = np.random.default_rng(1)
        x = rng.normal(0, 0.01, 5000)
        assert np.mean(winsorise_mad(x) != x) < 0.01    # ~0.27% of a normal lies beyond 3 sigma

    def test_zero_mad_series_is_returned_unchanged(self):
        x = np.zeros(50); x[10] = 0.05
        assert np.array_equal(winsorise_mad(x), x)

    def test_nan_passes_through_and_does_not_poison_the_scale(self):
        x = np.array([0.01, np.nan, -0.01, 0.02, 0.5, 0.0, 0.01, -0.02, 0.0, 0.01])
        w = winsorise_mad(x)
        assert np.isnan(w[1]) and np.isfinite(np.delete(w, 1)).all()
        assert np.nanmax(w) < 0.5

    def test_all_nan_returns_nan(self):
        assert np.isnan(winsorise_mad(np.array([np.nan, np.nan]))).all()

    def test_columns_helper_leaves_other_columns_alone(self):
        df = pl.DataFrame({"date": [1, 2, 3, 4, 5, 6, 7, 8],
                           "A": [0.01, -0.01, 0.02, 0.0, 0.01, -0.02, 0.01, 0.9],
                           "B": [0.01] * 8})
        out = winsorise_columns(df, ["A", "B"])
        assert out["date"].to_list() == df["date"].to_list()
        assert out["A"].max() < 0.9 and out["B"].to_list() == df["B"].to_list()

    def test_nulls_stay_null_so_drop_nulls_still_works(self):
        """A null turned NaN by to_numpy() would survive drop_nulls() and poison the Granger test."""
        df = pl.DataFrame({"A": [0.01, None, -0.01, 0.02, 0.0, 0.01, -0.02, 0.01, 0.0, 0.02]})
        out = winsorise_columns(df, ["A"])
        assert out["A"].null_count() == 1 and out["A"].is_nan().sum() == 0
        assert out.drop_nulls().height == 9

    def test_rank_normal_is_monotone_and_centered(self):
        z = rank_normal(np.array([5.0, 1.0, 3.0, 2.0, 4.0]))
        assert list(np.argsort(z)) == [1, 3, 2, 4, 0] and abs(z.mean()) < 1e-9


def _inflated_null(n, c=2.8, a=0.9, seed=0):
    """p-values from F(p) = min(1, c * p^a): a test that over-rejects ~2.5-3x at small p (guide 1.7:
    size 12.3% at 0.05, 2.8% at 0.01)."""
    u = np.random.default_rng(seed).uniform(size=n)
    return np.minimum(1.0, (u / c) ** (1.0 / a))


def _true_F(p, c=2.8, a=0.9):
    return np.minimum(1.0, c * np.power(p, a))


class TestCalibratedP:

    def test_calibrating_null_draws_against_themselves_is_uniform(self):
        null = _inflated_null(20000, seed=1)
        fresh = _inflated_null(5000, seed=2)
        cal, _ = calibrated_p(fresh, null)
        from scipy import stats
        assert stats.kstest(cal, "uniform").pvalue > 0.01

    def test_size_at_nominal_levels_is_restored(self):
        null = _inflated_null(30000, seed=3)
        fresh = _inflated_null(30000, seed=4)
        raw_size = np.mean(fresh <= 0.05)
        cal, _ = calibrated_p(fresh, null)
        assert raw_size > 0.10                                    # the inflated test over-rejects
        assert np.mean(cal <= 0.05) == pytest.approx(0.05, abs=0.007)
        assert np.mean(cal <= 0.01) == pytest.approx(0.01, abs=0.004)

    def test_p_below_the_null_minimum_is_extrapolated_not_floored(self):
        null = _inflated_null(20000, seed=5)
        tiny = np.array([null.min() / 10, null.min() / 100])
        cal, ext = calibrated_p(tiny, null)
        assert ext.all()
        assert cal[0] < 1.0 / (1 + null.size)                    # below the empirical floor
        assert cal[1] < cal[0]                                   # and still ordered
        truth = _true_F(tiny)
        # within one order of magnitude of the truth for a power-law null...
        assert np.all(cal / truth < 10) and np.all(cal / truth > 0.1)

    def test_extrapolation_is_not_anti_conservative_across_seeds(self):
        """The slope cap and the fitted line (not a noisy anchor) mean the calibrated p is not
        systematically BELOW the truth — across many nulls the median ratio stays >= ~0.5."""
        ratios = []
        for seed in range(20):
            null = _inflated_null(10000, seed=100 + seed)
            tiny = np.array([null.min() / 30])
            cal, _ = calibrated_p(tiny, null)
            ratios.append(float(cal[0] / _true_F(tiny)[0]))
        assert np.median(ratios) > 0.5
        assert np.quantile(ratios, 0.1) > 0.05

    def test_steeper_than_linear_tail_is_capped_at_slope_one(self):
        """A null thinner than linear in the tail (p^2) must not let tiny p's look more significant than
        the linear (valid-test) rate: calibrated p for p = null.min/10 is >= the slope-1 line."""
        rng = np.random.default_rng(11)
        null = rng.uniform(size=20000) ** 2                      # F(p) = sqrt(p): shallower than linear
        null2 = rng.uniform(size=20000) ** 0.5                   # F(p) = p^2: steeper than linear
        for nl in (null, null2):
            cal, ext = calibrated_p(np.array([np.sort(nl)[0] / 10]), nl)
            assert ext.all() and cal[0] > 0
            assert cal[0] >= (1 / (1 + nl.size)) / 10 * 0.2      # not steeper than the linear decay

    def test_extrapolation_is_monotone_below_the_null_minimum(self):
        null = _inflated_null(20000, seed=6)
        ps = np.array([null.min() * 0.999, null.min() * 0.5, null.min() * 0.1, null.min() * 0.001])
        cal, _ = calibrated_p(ps, null)
        assert np.all(np.diff(cal) <= 1e-18) and cal[0] <= 1.0 / (1 + null.size) + 1e-12

    def test_curve_is_monotone_across_the_empirical_boundary(self):
        null = _inflated_null(20000, seed=6)
        ps = np.sort(np.concatenate([[null.min() * 0.5, null.min() * 0.999, null.min()],
                                     np.quantile(null, [0.001, 0.01, 0.1, 0.5])]))
        cal, _ = calibrated_p(ps, null)
        assert np.all(np.diff(cal) >= -1e-18)

    def test_empirical_floor_without_enough_null_to_fit_a_tail(self):
        null = np.linspace(0.01, 1, 50)                          # tail of 5% = 2 points < min_tail_points
        cal, ext = calibrated_p(np.array([1e-9]), null)
        assert not ext.any() and cal[0] == pytest.approx(1 / 51)

    def test_never_returns_zero_or_above_one(self):
        null = _inflated_null(5000, seed=7)
        cal, _ = calibrated_p(np.array([0.0, 1e-12, 0.5, 1.0]), null)
        assert np.all(cal > 0) and np.all(cal <= 1.0)

    def test_empty_null_is_rejected(self):
        with pytest.raises(ValueError):
            calibrated_p(np.array([0.1]), np.array([]))

    def test_bh_can_reject_a_handful_of_real_pairs_only_with_the_tail(self):
        """The resolution argument made concrete: m = 10,920 pairs, q = 0.03, 5 real pairs. An empirical p
        from a 10,920-draw null cannot reach k*q/m for k <= 5; the tail extrapolation can."""
        from statsmodels.stats.multitest import multipletests
        m, n_real = 10920, 5
        null = _inflated_null(m, seed=8)
        rng = np.random.default_rng(9)
        p_obs = np.concatenate([_inflated_null(m - n_real, seed=10), rng.uniform(1e-12, 1e-9, n_real)])
        cal, ext = calibrated_p(p_obs, null)
        rej_tail = multipletests(cal, alpha=0.03, method="fdr_bh")[0]
        emp_floor = (1 + np.searchsorted(np.sort(null), p_obs, side="right")) / (1 + m)
        rej_emp = multipletests(emp_floor, alpha=0.03, method="fdr_bh")[0]
        assert rej_tail[-n_real:].all()
        assert not rej_emp.any(), "the empirical-only p-values cannot clear BH — the reason for the tail rule"
        assert rej_tail.sum() <= n_real + 3, "no flood of false rejections from the extrapolation"


class TestNullCorrQuantile:

    def test_agrees_with_the_analytic_sidak_bound_at_n44(self):
        """Independent check: five independent lags at n-k ~ 41 df. The MC is slightly above (the lags are
        mildly positively dependent through the shared series)."""
        from scipy import stats
        df = 44 - 3
        a = 1 - 0.95 ** (1 / 5)
        t = stats.t.ppf(1 - a / 2, df)
        sidak = t / np.sqrt(df + t * t)
        assert null_corr_quantile(44) == pytest.approx(sidak, abs=0.02)

    def test_matches_the_guides_figure_at_its_typical_pair_length(self):
        """Guide L5: simulated null p95 0.378, median 0.232 — reproduced at n ~ 46-50."""
        assert null_corr_quantile(48) == pytest.approx(0.378, abs=0.015)
        assert null_corr_quantile(48, 5, 0.5) == pytest.approx(0.232, abs=0.01)

    def test_shrinks_with_n(self):
        assert null_corr_quantile(30) > null_corr_quantile(44) > null_corr_quantile(250)

    def test_far_above_the_fixed_0_15_filter_at_production_n(self):
        assert null_corr_quantile(44) > 0.15 * 2

    def test_is_deterministic(self):
        assert null_corr_quantile(40, 5, 0.95, 1000, 7) == null_corr_quantile(40, 5, 0.95, 1000, 7)

    def test_too_short_series_never_qualifies(self):
        assert null_corr_quantile(6) == 1.0
