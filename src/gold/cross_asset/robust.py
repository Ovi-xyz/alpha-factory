"""
robust.py — XAE-R08 (cross_asset_recalibration_guide section 1.7 + Appendix A): pure statistical helpers
shared by the CrossAssetEngine modules. No I/O, no module state, no Silver/Gold reads.

winsorise_mad / winsorise_columns
    Clip a window of returns at median ± k robust sigma (MAD * 1.4826), per series, WITHIN the window.
    The estimator study (118 monthly windows, 516,255 rows) found winsorised ±3 robust sigma keeps the
    out-of-sample persistence of the clean-only production estimator (0.210 vs 0.208) while being safe to
    run on bars the Silver outlier flag would drop (raw all-bars admits crash-day artifacts: raw-only
    selected pairs persist at 0.022, about the 0.019 baseline). HC3 was worse (0.142) and is not offered.
    A series whose MAD is 0 (constant / mostly flat) is returned unchanged — there is no scale to clip to.

calibrated_p
    Placebo-calibrated p-value: the position of an observed p inside a pooled null of placebo p's
    (leader circularly shifted, so any real lead-lag is destroyed while each series keeps its own
    autocorrelation and fat tails). Empirical part: (1 + #{null <= p}) / (1 + N). An empirical p cannot
    go below 1/(N+1), and BH over m tests needs p_(k) <= k*q/m, so with a modest null BH can never reject a
    handful of real pairs. Below the null's smallest value the CDF is therefore EXTRAPOLATED as a power law
    fitted on the lowest tail of the null (log-log regression on the lowest 5%). The slope is CAPPED AT 1:
    a null whose CDF falls faster than linear would let us claim more significance than any valid test can,
    so the cap is the conservative direction. The line is the fit, not an anchor on the null's single
    smallest (noisiest) value. Documented, flagged per row, and a [U] starting rule: it is validated here by
    simulation, not against live data.

null_corr_quantile
    The n-aware replacement for the fixed `max_cross_corr >= 0.15` "actionable" filter (guide L5: 0.15
    passed 89.5% of pairs; the observed median 0.247 sits on the simulated null median 0.232, p95 0.378
    at "n = 44"). Monte Carlo of max_{k=1..max_lag} |corr(a[:-k], b[k:])| for independent normal series of
    length n, cached per (n, max_lag, q). Checked against the analytic Sidak bound for five independent lags
    (r95 = 0.388 at n = 44; this simulation gives 0.400, and 0.384 at n = 48 — the guide's 0.378 sits at
    n of about 46-50, so its "n = 44" was the typical rather than the exact pair length).
"""

from __future__ import annotations

from functools import lru_cache

import numpy as np
import polars as pl

MAD_TO_SIGMA = 1.4826


def winsorise_mad(values: np.ndarray, k: float = 3.0) -> np.ndarray:
    """Clip to [median - k*sigma_r, median + k*sigma_r], sigma_r = 1.4826 * MAD. NaNs pass through."""
    a = np.asarray(values, dtype=float)
    finite = a[np.isfinite(a)]
    if finite.size == 0:
        return a.copy()
    med = np.median(finite)
    mad = np.median(np.abs(finite - med)) * MAD_TO_SIGMA
    if mad <= 0.0:
        return a.copy()
    return np.clip(a, med - k * mad, med + k * mad)


def winsorise_columns(df: pl.DataFrame, columns: list[str], k: float = 3.0) -> pl.DataFrame:
    """winsorise_mad applied to each listed column of a wide return pivot (one column per symbol)."""
    out = df
    for c in columns:
        w = winsorise_mad(df[c].to_numpy(), k)
        # to_numpy() turns a polars null into NaN; a NaN is NOT a null to the callers' drop_nulls(), so
        # it would reach the Granger test and kill the whole pair. nan_to_null restores the null.
        out = out.with_columns(pl.Series(c, w, dtype=pl.Float64, nan_to_null=True))
    return out


def rank_normal(a: np.ndarray) -> np.ndarray:
    """Rank-based inverse-normal transform (kept for the estimator study; not used by production)."""
    from scipy import stats
    a = np.asarray(a, dtype=float)
    return stats.norm.ppf((stats.rankdata(a) - 0.5) / len(a))


def calibrated_p(
    p_obs: np.ndarray,
    p_null: np.ndarray,
    tail_fraction: float = 0.05,
    min_tail_points: int = 20,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Calibrated p-values for `p_obs` against the pooled placebo null `p_null`.
    Returns (p_calibrated, extrapolated) — `extrapolated[i]` is True when p_obs[i] fell below the null's
    smallest value and the power-law tail was used. p_calibrated is in (0, 1]. With fewer than
    `min_tail_points` null values in the fitted tail the extrapolation is skipped and the empirical floor
    1/(N+1) is returned (conservative).
    """
    p_obs = np.asarray(p_obs, dtype=float)
    null = np.sort(np.asarray(p_null, dtype=float))
    n = null.size
    if n == 0:
        raise ValueError("calibrated_p needs a non-empty null")

    emp = (1.0 + np.searchsorted(null, p_obs, side="right")) / (1.0 + n)
    out = emp.copy()
    extrapolated = np.zeros(p_obs.shape, dtype=bool)

    below = p_obs < null[0]
    if below.any():
        m = max(int(round(n * tail_fraction)), 0)
        if m >= min_tail_points:
            tail = null[:m]
            tail = tail[tail > 0]
            if tail.size >= min_tail_points:
                cdf = np.arange(1, tail.size + 1) / (1.0 + n)
                slope, intercept = np.polyfit(np.log(tail), np.log(cdf), 1)
                if slope > 1.0:
                    # A null CDF that falls faster than linear would let us claim more significance
                    # than a valid test ever can (P(p <= x) = x is slope 1). Cap at 1 and refit the
                    # level only — the conservative direction, never a steeper decay than linear.
                    slope = 1.0
                    intercept = float(np.median(np.log(cdf) - np.log(tail)))
                # Use the FITTED line, not an anchor on the null's single smallest value: that value is
                # the noisiest point of the whole null (the minimum of N uniforms varies by orders of
                # magnitude). Capped at the empirical floor so the curve stays monotone with the
                # empirical branch.
                with np.errstate(divide="ignore", over="ignore"):
                    ext = np.exp(intercept) * np.maximum(p_obs[below], 1e-300) ** slope
                out[below] = np.clip(ext, 1e-300, cdf[0])
                extrapolated[below] = True
    return out, extrapolated


@lru_cache(maxsize=256)
def null_corr_quantile(n_obs: int, max_lag: int = 5, q: float = 0.95,
                       n_sims: int = 4000, seed: int = 20260101) -> float:
    """q-quantile of max_{k=1..max_lag} |corr(a[:-k], b[k:])| for independent N(0,1) series of length n_obs."""
    if n_obs < max_lag + 3:
        return 1.0
    rng = np.random.default_rng(seed + n_obs)
    A = rng.standard_normal((n_sims, n_obs))
    B = rng.standard_normal((n_sims, n_obs))
    best = np.zeros(n_sims)
    for k in range(1, max_lag + 1):
        a = A[:, : n_obs - k]
        b = B[:, k:]
        a = a - a.mean(axis=1, keepdims=True)
        b = b - b.mean(axis=1, keepdims=True)
        denom = np.sqrt((a * a).sum(axis=1) * (b * b).sum(axis=1))
        c = np.abs((a * b).sum(axis=1) / denom)
        best = np.maximum(best, c)
    return float(np.quantile(best, q))
