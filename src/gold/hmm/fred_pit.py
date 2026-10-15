"""
src/gold/hmm/fred_pit.py — ADR-051 (GMI-HMM2-001), Amendment 2 §C (+ recorded availability).

ONE shared implementation of point-in-time FRED availability, used by both
the macro branch (raw_macro) and the market branch (raw_market). Two branches,
one rule: if the rule ever needs to change it changes in exactly one place
(GAP-1 precedent: "a second copy of the same logic is a drift risk").

THREE DATES, NEVER CONFLATED (bitemporal vocabulary)
    observation_date   the period a value MEASURES            (valid time)
    available_on       the first date the system could USE it (transaction time)
    as_of_date         the panel row's date: "what was known at the end of this day"
A panel row for as_of_date D contains, per feature, the latest OBSERVATION among
those with available_on <= D. All three dates are written out per feature
(obs_date__ / available_on__ / avail_basis__), so any value can be audited back
to the day it became usable and to HOW that day was determined. Ages are never
stored: they are as_of_date - obs_date__<feature>.

THE RULE: a value is usable on as_of_date D  iff  D >= available_on, where
available_on is determined per row by its BASIS:

  recorded   the UTC date the pipeline FIRST RECORDED the row (Silver
             `_ingested_at`) — an actual timestamp of when this system held the
             value, never an assumption. Used for every row NOT in its series'
             first-ingest batch, when pit.basis = recorded. Clamped to the
             observation date (a forward-dated value, e.g. IORB, is never
             available before the day it describes).
  estimated  max(release_date, observation_date + lag) — an ESTIMATE, used
             (a) for backfilled history, because every row of a series' first
             ingest batch carries the backfill timestamp, not a publication
             time, and (b) for every row when pit.basis = estimated. `lag` is
             the per-series override if configured, else the per-frequency floor
             (daily 1 / weekly 7 / monthly 45 / quarterly 120 days).
  mixed      derived features only: legs with different bases.

WHY TWO BASES. Measured on live Silver (6 Oct 2026): 180,223 of 180,951 rows
(99.6%) are the 2026-08-20 backfill batch, whose `_ingested_at` is a single
load day (median ingest lag 14 years) — useless as availability — so history
cannot be recorded-basis; only the ~0.4% ingested afterwards can. Those live
rows also show the estimates are not safe to treat as truth: NFCI recorded
median lag 15 d vs the estimated 7, CPI 61 vs 45, UNRATE 57.5 vs 45, DEXUSEU/
DEXJPUS up to 17 vs 10 (part of that gap is this pipeline's own run cadence —
which is exactly what "available to this system" means).

WHY `estimated` USES max(release_date, obs + lag) AND NOT release_date ALONE.
Bronze's `release_date` is a fixed SYNTHETIC lag (GDP growth: +30 days for every
row, while FRED's quarterly observation_date is the quarter START and the
advance estimate lands ~+120 days later: ~90 days of lookahead if trusted), and
several `daily` series post in a weekly step (DEXUSEU/DEXJPUS: Fed H.10 batch;
DCOILWTICO: weekly EIA posting). max(release_date, obs + lag) can only be MORE
conservative than release_date alone.

KNOWN SKEW (documented, accepted): history is estimated-basis, the live tail is
recorded-basis, so a series whose real lag exceeds its estimate (NFCI, CPI,
UNRATE) is staler at inference than in the training window. Revisit the
estimates against the recorded lags; pit.basis: estimated reproduces the
all-estimated panel for comparison.

WHAT THIS DOES NOT FIX: Silver FRED holds current-vintage VALUES (is_revision is
False throughout): a revised figure is carried at its revised value. A REVISED
row is re-stamped, so it leaves the backfill batch and becomes recorded at its
revision date — the panel then simply has nothing newer for that observation
until then (conservative; the advance estimate it replaced is not recoverable).

CONVENTIONS
    - Calendar = weekdays (Mon-Fri) unless the caller supplies one (raw_market
      passes US trading days). Availability is compared at UTC-date granularity,
      with end-of-day semantics ("as of the end of D").
    - Forward-fill = as-of join on available_on, taking the latest OBSERVATION
      among those already available (a later-published but older observation
      never overrides a newer one already known).
    - Transforms run on the NATIVE-cadence series BEFORE availability gating
      (a 12-month % change is computed on the monthly observations and is
      available exactly when its latest observation is).
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Callable, Optional, Protocol, Sequence

import polars as pl

SILVER_MACRO_PATH = Path("data/silver/macro_enriched")
_SNAPSHOT_RE = re.compile(r"^fred_(\d{4}-\d{2}-\d{2})_silver\.parquet$")

BASIS_RECORDED  = "recorded"
BASIS_ESTIMATED = "estimated"
BASIS_MIXED     = "mixed"
PIT_BASES = frozenset({BASIS_RECORDED, BASIS_ESTIMATED})      # valid values of hmm.pit.basis

KEY = "as_of_date"            # the panel row's date column


# ── Native-cadence transforms ────────────────────────────────────────────────
# Each takes a single-series frame (observation_date, release_date, value, ...),
# sorted or not, and returns the same shape with `value` transformed (rows that
# cannot be computed are dropped, never zero-filled). Other columns (ingested_on,
# backfilled) ride along unchanged: a transformed value is available exactly when
# the row carrying it is.

def _t_level(obs: pl.DataFrame) -> pl.DataFrame:
    return obs


def _t_pct_change_12m(obs: pl.DataFrame) -> pl.DataFrame:
    """12-month % change (in percent), DATE-based rather than shift(12): a
    missing month yields no value for the two affected dates instead of
    silently pairing the wrong months."""
    prev = obs.select(
        pl.col("observation_date").dt.offset_by("12mo").alias("observation_date"),
        pl.col("value").alias("_prev"),
    )
    return (
        obs.join(prev, on="observation_date", how="left")
        .filter(pl.col("_prev").is_not_null() & (pl.col("_prev") != 0))
        .with_columns(((pl.col("value") / pl.col("_prev") - 1.0) * 100.0).alias("value"))
        .drop("_prev")
    )


def _t_log_change_20(obs: pl.DataFrame) -> pl.DataFrame:
    """ln(v_t / v_{t-20}) over the 20th previous OBSERVATION (ADR-051: "20-observation
    log change" — observation-indexed, unlike pct_change_12m which is date-based).
    Rows without a 20th predecessor, or with a non-positive value/base, are
    dropped (a log of <= 0 is undefined — never clipped or zero-filled)."""
    o = obs.sort("observation_date").with_columns(pl.col("value").shift(20).alias("_prev"))
    return (
        o.filter(pl.col("_prev").is_not_null() & (pl.col("_prev") > 0) & (pl.col("value") > 0))
        .with_columns((pl.col("value") / pl.col("_prev")).log().alias("value"))
        .drop("_prev")
    )


def _t_diff_1(obs: pl.DataFrame) -> pl.DataFrame:
    """Day-over-day change v_t - v_{t-1} over consecutive OBSERVATIONS (a change
    across a holiday is the change vs the previous observation). First row dropped."""
    o = obs.sort("observation_date").with_columns(pl.col("value").shift(1).alias("_prev"))
    return (
        o.filter(pl.col("_prev").is_not_null())
        .with_columns((pl.col("value") - pl.col("_prev")).alias("value"))
        .drop("_prev")
    )


TRANSFORMS: dict[str, Callable[[pl.DataFrame], pl.DataFrame]] = {
    "level": _t_level,
    "pct_change_12m": _t_pct_change_12m,
    "log_change_20": _t_log_change_20,
    "diff_1": _t_diff_1,
}


# ── Feature request protocol (satisfied by config.FeatureSpec / MarketFeature) ─

class _DerivedLike(Protocol):
    op: str
    left: str
    right: str


class FeatureLike(Protocol):
    feature: str
    series: Optional[str]
    transform: str
    derived: Optional[_DerivedLike]


# ── Snapshot + observation loading ───────────────────────────────────────────

def latest_snapshot(run_date: date, silver_dir: Path | str | None = None) -> Optional[Path]:
    """Latest fred_YYYY-MM-DD_silver.parquet whose date is <= run_date. Each
    snapshot carries the FULL history (macro_processor writes the whole
    series every run), so the latest one is sufficient — never a union of
    snapshots. None when no snapshot exists on or before run_date."""
    best: Optional[tuple[date, Path]] = None
    base = SILVER_MACRO_PATH if silver_dir is None else Path(silver_dir)   # call-time default
    for p in base.glob("fred_*_silver.parquet"):
        m = _SNAPSHOT_RE.match(p.name)
        if not m:
            continue
        d = date.fromisoformat(m.group(1))
        if d <= run_date and (best is None or d > best[0]):
            best = (d, p)
    return best[1] if best else None


def _ingested_on_expr(dtype: pl.DataType) -> pl.Expr:
    """UTC calendar date of `_ingested_at` (an ISO string in live Silver; a
    Datetime is also accepted — naive is taken as UTC, aware is converted)."""
    c = pl.col("_ingested_at")
    if dtype == pl.String:
        # explicit format: with strict=False polars still RAISES when it cannot infer
        # a format from the first value; an unparseable stamp must become NULL instead
        return c.str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False)
    if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None:
        return c.dt.convert_time_zone("UTC").dt.date()
    return c.cast(pl.Datetime).dt.date()


def load_observations(snapshot: Path | str, series_ids: Sequence[str]) -> pl.DataFrame:
    """series_id, observation_date (Date), release_date (Date|null), value
    (Float64, non-null), ingested_on (Date|null), backfilled (Boolean).

    ingested_on  UTC date the pipeline first recorded the row (`_ingested_at`).
    backfilled   True when the row belongs to its series' FIRST ingest batch (the
                 backfill: its timestamp is a load day, not an availability time)
                 or has no usable timestamp. Computed here, on the raw series,
                 BEFORE any transform drops rows.

    Lazy scan (M1 8 GB rule). Defensive dedupe on (series_id, observation_date),
    latest _ingested_at wins — Silver is already deduped (SIL-MACRO-DEDUP-01);
    this keeps a future regression from silently double-counting."""
    lf = pl.scan_parquet(snapshot)
    cols = lf.collect_schema().names()
    has_ing = "_ingested_at" in cols
    keep = ["series_id", "observation_date", "release_date", "value"] + (["_ingested_at"] if has_ing else [])
    df = lf.filter(pl.col("series_id").is_in(list(series_ids))).select(keep).collect()
    if df.schema["release_date"] == pl.String:
        df = df.with_columns(pl.col("release_date").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False))
    ing = _ingested_on_expr(df.schema["_ingested_at"]) if has_ing else pl.lit(None, dtype=pl.Date)
    df = df.with_columns(pl.col("observation_date").cast(pl.Date), ing.alias("ingested_on")).drop_nulls("value")
    sort_cols = ["series_id", "observation_date"] + (["_ingested_at"] if has_ing else [])
    df = df.sort(sort_cols).unique(subset=["series_id", "observation_date"], keep="last", maintain_order=True)
    first = pl.col("ingested_on").min().over("series_id")
    df = df.with_columns((pl.col("ingested_on").is_null() | (pl.col("ingested_on") == first)).alias("backfilled"))
    return df.select("series_id", "observation_date", "release_date", "value", "ingested_on", "backfilled")


# ── Availability + as-of ─────────────────────────────────────────────────────

def with_availability(obs: pl.DataFrame, lag_days: int, basis: str = BASIS_RECORDED) -> pl.DataFrame:
    """Add `available_on` and `avail_basis` (see module docstring, THE RULE).

    A frame without ingested_on / backfilled (hand-built, or a Silver snapshot
    with no `_ingested_at`) has no recorded information: every row is estimated.
    max_horizontal skips nulls, so a null release_date degrades to
    observation_date + lag rather than to a null availability."""
    if basis not in PIT_BASES:
        raise ValueError(f"pit basis must be one of {sorted(PIT_BASES)}, got {basis!r}")
    if "ingested_on" not in obs.columns:
        obs = obs.with_columns(pl.lit(None, dtype=pl.Date).alias("ingested_on"))
    if "backfilled" not in obs.columns:
        obs = obs.with_columns(pl.lit(True).alias("backfilled"))
    est = pl.max_horizontal(pl.col("release_date"), pl.col("observation_date") + pl.duration(days=lag_days))
    rec = pl.max_horizontal(pl.col("ingested_on"), pl.col("observation_date"))
    use_rec = pl.lit(basis == BASIS_RECORDED) & ~pl.col("backfilled") & pl.col("ingested_on").is_not_null()
    return obs.with_columns(
        pl.when(use_rec).then(rec).otherwise(est).alias("available_on"),
        pl.when(use_rec).then(pl.lit(BASIS_RECORDED)).otherwise(pl.lit(BASIS_ESTIMATED)).alias("avail_basis"),
    )


def _latest_known(obs_av: pl.DataFrame) -> pl.DataFrame:
    """Reduce to the rows an as-of read can ever return: strictly increasing
    observation_date along availability order, one row per availability date.
    Drops a late-published OLDER observation that would otherwise override a
    newer one already known."""
    o = obs_av.sort(["available_on", "observation_date"])
    o = o.with_columns(pl.col("observation_date").cum_max().shift(1).alias("_prev_max"))
    o = o.filter(pl.col("_prev_max").is_null() | (pl.col("observation_date") > pl.col("_prev_max")))
    return o.drop("_prev_max").unique(subset=["available_on"], keep="last", maintain_order=True)


_EMPTY_ASOF = {"value": pl.Float64, "obs_date": pl.Date, "available_on": pl.Date, "basis": pl.String}


def _asof(calendar: pl.DataFrame, obs_av: pl.DataFrame) -> pl.DataFrame:
    """calendar(as_of_date) -> as_of_date, value, obs_date, available_on, basis."""
    if obs_av.height == 0:
        return calendar.with_columns([pl.lit(None, dtype=t).alias(n) for n, t in _EMPTY_ASOF.items()])
    right = (
        _latest_known(obs_av)
        .select("available_on", pl.col("observation_date").alias("obs_date"), "value",
                pl.col("avail_basis").alias("basis"))
        .sort("available_on")
    )
    joined = calendar.sort(KEY).join_asof(right, left_on=KEY, right_on="available_on", strategy="backward")
    return joined.select(KEY, "value", "obs_date", "available_on", "basis")


def weekday_calendar(start: date, end: date) -> pl.DataFrame:
    """Mon-Fri dates in [start, end] as a one-column frame (`as_of_date`)."""
    if end < start:
        return pl.DataFrame({KEY: pl.Series([], dtype=pl.Date)})
    d = pl.date_range(start, end, interval="1d", eager=True).alias(KEY)
    return pl.DataFrame({KEY: d}).filter(pl.col(KEY).dt.weekday() <= 5)


# ── Panel construction ───────────────────────────────────────────────────────

def build_panel(
    obs: pl.DataFrame,
    features: Sequence[FeatureLike],
    lag_days: Callable[[str], int],
    end: date,
    calendar: Optional[pl.DataFrame] = None,
    basis: str = BASIS_RECORDED,
) -> pl.DataFrame:
    """Daily PIT-gated panel.

    Columns, in `features` order within each group:
        as_of_date
        <feature>                Float64
        obs_date__<feature>      Date     observation date of the carried value
        available_on__<feature>  Date     first date the system could use it
        avail_basis__<feature>   String   recorded | estimated | mixed
    Features whose series has no observations at all come back as all-null
    columns (caller decides whether that is fatal — raw_macro and raw_market
    treat it as a registry defect).

    Default calendar (`calendar=None`, raw_macro): weekdays, starting at the
    first weekday on which ANY feature has a value and ending at the last
    weekday <= `end`.

    Caller-supplied calendar (raw_market): a frame with an `as_of_date` column —
    e.g. the US trading days. Rows are exactly those dates <= `end`; NOTHING is
    trimmed (an early trading day with no FRED value yet is kept, all-null,
    because the caller owns the row index).

    `obs` is load_observations()'s output; `lag_days(series_id)` the shared
    estimated-lag lookup; `basis` is hmm.pit.basis.
    """
    by_series: dict[str, pl.DataFrame] = {}
    if obs.height:
        for (sid,), g in obs.partition_by("series_id", as_dict=True).items():
            by_series[sid] = g.drop("series_id")

    def avail(series_id: str, transform: str) -> pl.DataFrame:
        base = by_series.get(series_id)
        if base is None or base.height == 0:
            return pl.DataFrame(
                schema={"observation_date": pl.Date, "release_date": pl.Date, "value": pl.Float64,
                        "available_on": pl.Date, "avail_basis": pl.String})
        return with_availability(TRANSFORMS[transform](base), lag_days(series_id), basis)

    # Needed (series, transform) availability frames, cached.
    cache: dict[tuple[str, str], pl.DataFrame] = {}

    def get(series_id: str, transform: str) -> pl.DataFrame:
        k = (series_id, transform)
        if k not in cache:
            cache[k] = avail(series_id, transform)
        return cache[k]

    needed: list[tuple[str, str]] = []
    for f in features:
        if f.derived is not None:
            needed += [(f.derived.left, "level"), (f.derived.right, "level")]
        else:
            needed.append((f.series, f.transform))  # type: ignore[arg-type]

    if calendar is None:
        firsts = [get(s, t)["available_on"].min() for s, t in needed if get(s, t).height > 0]
        if not firsts:
            return pl.DataFrame({KEY: pl.Series([], dtype=pl.Date)})
        cal = weekday_calendar(min(firsts), end)
        trim_leading = True
    else:
        cal = calendar.select(pl.col(KEY).cast(pl.Date)).filter(pl.col(KEY) <= end).unique().sort(KEY)
        trim_leading = False
    if cal.height == 0:
        return pl.DataFrame({KEY: pl.Series([], dtype=pl.Date)})
    calendar = cal

    cols: list[pl.DataFrame] = []
    for f in features:
        n = f.feature
        if f.derived is not None:
            if f.derived.op != "subtract":
                raise ValueError(f"unsupported derived op {f.derived.op!r}")
            a = _asof(calendar, get(f.derived.left, "level"))
            b = _asof(calendar, get(f.derived.right, "level")).rename(
                {"value": "_vb", "obs_date": "_ob", "available_on": "_avb", "basis": "_bb"})
            j = a.join(b, on=KEY, how="left")
            both = pl.col("value").is_not_null() & pl.col("_vb").is_not_null()
            col = j.select(
                KEY,
                pl.when(both).then(pl.col("value") - pl.col("_vb")).otherwise(None).alias(n),
                # a derived value is as old as its OLDER leg and usable only once BOTH legs are
                pl.when(both).then(pl.min_horizontal("obs_date", "_ob")).otherwise(None).alias(f"obs_date__{n}"),
                pl.when(both).then(pl.max_horizontal("available_on", "_avb")).otherwise(None)
                  .alias(f"available_on__{n}"),
                pl.when(both).then(
                    pl.when(pl.col("basis") == pl.col("_bb")).then(pl.col("basis")).otherwise(pl.lit(BASIS_MIXED))
                ).otherwise(None).alias(f"avail_basis__{n}"),
            )
        else:
            r = _asof(calendar, get(f.series, f.transform))  # type: ignore[arg-type]
            col = r.rename({"value": n, "obs_date": f"obs_date__{n}",
                            "available_on": f"available_on__{n}", "basis": f"avail_basis__{n}"})
        cols.append(col)

    out = cols[0]
    for c in cols[1:]:
        out = out.join(c, on=KEY, how="left")
    value_cols = [f.feature for f in features]
    if trim_leading:
        # Drop leading rows where nothing is known yet (calendar starts at the first
        # availability of any series, so this only trims transform-induced gaps).
        out = out.filter(pl.any_horizontal([pl.col(c).is_not_null() for c in value_cols]))
    return out.select(
        KEY, *value_cols,
        *[f"obs_date__{c}" for c in value_cols],
        *[f"available_on__{c}" for c in value_cols],
        *[f"avail_basis__{c}" for c in value_cols],
    ).sort(KEY)
