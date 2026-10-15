"""
src/gold/hmm/config.py — ADR-051 (GMI-HMM2-001), Amendment 2 §B/§C.

Loads and VALIDATES the `hmm:` routing block of config/fred_series.yaml.

Why a validated loader rather than a bare yaml.safe_load at each call site:
this repo's recurring failure class is the registry-gate silent drop (see
learnings — EIA, Treasury tenors, ticker collisions): a series that is not
registered, or a feature routed to a domain that does not exist, is silently
dropped on every run and nobody notices. Everything that can be checked
statically is therefore checked HERE, once, and fails loudly with the exact
offending entry — before any model sees a malformed panel.

Checks (all raise HMMConfigError):
  1. feature names unique; domain ∈ hmm.domains
  2. exactly one of `series` / `derived` per feature
  3. every referenced series (feature series, derived legs, lag overrides)
     is in the FRED registry
  4. transform known; pct_change_12m only on a monthly-frequency series
  5. every registry frequency used has a publication-lag floor
  6. collinearity guard: a domain never contains a derived feature together
     with BOTH of its legs (ADR-051 variable table)

No numbers are invented here: floors/overrides/routing are read from the
yaml, whose values are the ones fixed by ADR-051 Amendment 2 §C.

The MARKET branch has its own block (`hmm_market:`, loaded by market_config)
with a stricter contract: every feature names a `source` from a two-entry
whitelist (`fred`, `silver_breadth`), each with an exact key set. Nothing
that reads a Gold store can be expressed — Amendment 2 §A (domain
independence) is enforced here at load time, not just by review. The PIT
availability rule is shared with the macro branch (`hmm.pit`), never repeated.

`hmm.pit.basis` is REQUIRED (no silent default): `recorded` uses the pipeline's
own first-seen timestamp wherever one exists and falls back to the estimated
lag only for backfilled history; `estimated` uses the estimated lag everywhere.
The lag floors/overrides are therefore ESTIMATES (fallback), not the rule.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml

from src.gold.hmm.fred_pit import PIT_BASES, TRANSFORMS

FRED_SERIES_PATH = Path("config/fred_series.yaml")

# Single source of truth: the transforms fred_pit can actually execute. Adding
# a transform there makes it valid here — no second list to forget.
KNOWN_TRANSFORMS = frozenset(TRANSFORMS)
KNOWN_DERIVED_OPS = frozenset({"subtract"})
FREQUENCIES = ("daily", "weekly", "monthly", "quarterly")

# Market branch (ADR-051 Amendment 2 §A): the ONLY feature sources. Neither is a
# Gold store. Per-source exact key sets — an unexpected key (e.g. a smuggled
# `store:` / `path:`) is rejected rather than ignored.
MARKET_SOURCE_KEYS: dict[str, frozenset[str]] = {
    "fred":           frozenset({"feature", "source", "series", "transform", "weight"}),
    "silver_breadth": frozenset({"feature", "source", "metric", "weight"}),
}
BREADTH_METRICS = frozenset({"share_up", "median_return", "dispersion", "rms_median_return"})


class HMMConfigError(ValueError):
    """Raised when the `hmm:` block of fred_series.yaml is malformed."""


@dataclass(frozen=True)
class DerivedSpec:
    op: str
    left: str    # series id of the left leg
    right: str   # series id of the right leg


@dataclass(frozen=True)
class FeatureSpec:
    feature: str                       # column name in raw_macro.parquet
    domain: str
    series: Optional[str] = None       # source series (None when derived)
    transform: str = "level"
    derived: Optional[DerivedSpec] = None
    weight: Optional[float] = None     # signed ordering weight (model config requires it; raw_macro ignores it)


@dataclass(frozen=True)
class MacroConfig:
    version: str
    domains: tuple[str, ...]
    features: tuple[FeatureSpec, ...]
    frequency_floor_days: Mapping[str, int]
    series_lag_days: Mapping[str, int]
    series_frequency: Mapping[str, str]    # registry `frequency:` per series id
    pit_basis: str = "recorded"            # hmm.pit.basis (validated against fred_pit.PIT_BASES)

    def feature_names(self) -> list[str]:
        return [f.feature for f in self.features]

    def features_in(self, domain: str) -> list[FeatureSpec]:
        return [f for f in self.features if f.domain == domain]

    def required_series(self) -> list[str]:
        """Every FRED series the panel needs (feature series + derived legs),
        deterministic order, no duplicates."""
        seen: dict[str, None] = {}
        for f in self.features:
            if f.derived is not None:
                seen[f.derived.left] = None
                seen[f.derived.right] = None
            else:
                seen[f.series] = None   # type: ignore[index]
        return list(seen)

    def lag_days(self, series_id: str) -> int:
        """Publication lag (days) for a series: per-series override if
        configured, else the per-frequency floor (Amendment 2 §C)."""
        return _resolve_lag(series_id, self.series_lag_days,
                            self.frequency_floor_days, self.series_frequency)


def _resolve_lag(series_id: str, overrides: Mapping[str, int],
                 floors: Mapping[str, int], series_frequency: Mapping[str, str]) -> int:
    """Per-series override if configured, else the per-frequency floor. One
    implementation for both branches' configs."""
    if series_id in overrides:
        return int(overrides[series_id])
    return int(floors[series_frequency[series_id]])


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise HMMConfigError(msg)


def _validate_pit(block: Mapping[str, Any], registry: Mapping[str, Any]) -> tuple[dict, dict, str]:
    """Validate hmm.pit (availability basis + estimated-lag floors + per-series
    overrides). Shared by the macro and market validators — the market branch
    reads the SAME block."""
    pit = block.get("pit", {})
    basis = pit.get("basis")
    _require(basis in PIT_BASES,
             f"hmm.pit.basis must be one of {sorted(PIT_BASES)} (explicit, no default), got {basis!r}")
    floors = dict(pit.get("frequency_floor_days", {}))
    overrides = dict(pit.get("series_lag_days", {}))
    for fr, d in floors.items():
        _require(fr in FREQUENCIES, f"hmm.pit.frequency_floor_days: unknown frequency {fr!r}")
        _require(isinstance(d, int) and d >= 0, f"frequency floor for {fr!r} must be an int >= 0, got {d!r}")
    for sid, d in overrides.items():
        _require(sid in registry, f"hmm.pit.series_lag_days: series {sid!r} not in registry")
        _require(isinstance(d, int) and d >= 0, f"lag override for {sid!r} must be an int >= 0, got {d!r}")
    return floors, overrides, basis


def _check_series_transform(where: str, sid: str, tr: str, registry: Mapping[str, Any]) -> None:
    _require(sid in registry, f"{where}: series {sid!r} not in FRED registry")
    _require(tr in KNOWN_TRANSFORMS, f"{where}: unknown transform {tr!r}")
    if tr == "pct_change_12m":
        _require(registry[sid].get("frequency") == "monthly",
                 f"{where}: pct_change_12m is only valid on a monthly series "
                 f"({sid} is {registry[sid].get('frequency')!r})")


def validate_macro_config(raw: Mapping[str, Any]) -> MacroConfig:
    """Validate a parsed fred_series.yaml mapping; return the typed config."""
    registry = {s["id"]: s for s in raw.get("series", [])}
    _require("hmm" in raw, "fred_series.yaml has no `hmm:` block (ADR-051)")
    block = raw["hmm"]

    domains = tuple(block.get("domains", ()))
    _require(len(domains) > 0, "hmm.domains is empty")
    _require(len(set(domains)) == len(domains), f"hmm.domains has duplicates: {domains}")

    floors, overrides, basis = _validate_pit(block, registry)

    features: list[FeatureSpec] = []
    names: set[str] = set()
    for i, e in enumerate(block.get("features", [])):
        where = f"hmm.features[{i}] ({e.get('feature', '?')})"
        name = e.get("feature")
        _require(isinstance(name, str) and name, f"{where}: missing `feature`")
        _require(name not in names, f"{where}: duplicate feature name")
        names.add(name)
        _require(e.get("domain") in domains, f"{where}: domain {e.get('domain')!r} not in hmm.domains")

        has_series, has_derived = "series" in e, "derived" in e
        _require(has_series != has_derived, f"{where}: needs exactly one of `series` / `derived`")
        w = e.get("weight")
        _require(w is None or (isinstance(w, (int, float)) and not isinstance(w, bool) and math.isfinite(w)),
                 f"{where}: weight must be a finite number, got {w!r}")
        weight = None if w is None else float(w)

        if has_derived:
            d = e["derived"]
            _require(d.get("op") in KNOWN_DERIVED_OPS, f"{where}: unknown derived op {d.get('op')!r}")
            for leg in ("left", "right"):
                _require(d.get(leg) in registry, f"{where}: derived leg {d.get(leg)!r} not in registry")
            _require(e.get("transform", "level") == "level", f"{where}: a derived feature takes no transform")
            features.append(FeatureSpec(feature=name, domain=e["domain"],
                                        derived=DerivedSpec(d["op"], d["left"], d["right"]), weight=weight))
        else:
            sid, tr = e["series"], e.get("transform", "level")
            _check_series_transform(where, sid, tr, registry)
            features.append(FeatureSpec(feature=name, domain=e["domain"], series=sid, transform=tr, weight=weight))

    _require(len(features) > 0, "hmm.features is empty")

    # Frequency floors must cover every frequency actually used.
    series_frequency: dict[str, str] = {}
    for f in features:
        for sid in ([f.series] if f.derived is None else [f.derived.left, f.derived.right]):
            fr = registry[sid].get("frequency")
            _require(fr in floors or sid in overrides,
                     f"series {sid!r} has frequency {fr!r} with no publication-lag floor "
                     f"and no per-series override")
            series_frequency[sid] = fr

    # Collinearity guard: derived feature + BOTH legs in one domain.
    for f in features:
        if f.derived is None:
            continue
        plain = {g.series for g in features if g.domain == f.domain and g.derived is None}
        _require(not ({f.derived.left, f.derived.right} <= plain),
                 f"collinearity guard: domain {f.domain!r} holds derived {f.feature!r} together with "
                 f"both legs {f.derived.left!r} and {f.derived.right!r}")

    return MacroConfig(
        version=str(block.get("version", "")),
        domains=domains,
        features=tuple(features),
        frequency_floor_days=floors,
        series_lag_days=overrides,
        series_frequency=series_frequency,
        pit_basis=basis,
    )


def macro_config(path: Path | str | None = None) -> MacroConfig:
    """Load + validate the macro routing block. Raises HMMConfigError.
    `path` defaults to FRED_SERIES_PATH, resolved at CALL time."""
    with open(FRED_SERIES_PATH if path is None else path) as fh:
        raw = yaml.safe_load(fh)
    return validate_macro_config(raw)


# ═══ Market branch (raw_market) ══════════════════════════════════════════════

@dataclass(frozen=True)
class MarketFeature:
    """One market-branch feature. `derived` is always None — present so a
    FRED-sourced MarketFeature satisfies fred_pit.FeatureLike."""
    feature: str
    source: str                          # "fred" | "silver_breadth"
    series: Optional[str] = None         # fred only
    transform: str = "level"             # fred only
    metric: Optional[str] = None         # silver_breadth only
    derived: None = None
    weight: Optional[float] = None       # signed ordering weight (the market MODEL requires it; raw_market ignores it)


@dataclass(frozen=True)
class BreadthSettings:
    """Breadth universe + guard thresholds. VERSIONED: `guard_version` is the
    human-bumped label of the threshold set; `fingerprint()` is an automatic
    digest of EVERY field, so a threshold edited without bumping the label is
    still visible in the data (both are written to each raw_market row)."""
    market: str
    timeframe: str
    min_symbols: int           # guard A: fewer clean returns than this -> breadth nulled
    min_coverage_ratio: float  # guard B: clean returns / eligible symbols below this -> nulled
    rms_window: int            # window (valid trading days) of the median-return RMS
    guard_version: str

    def fingerprint(self) -> str:
        payload = json.dumps(dataclasses.asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()[:12]


@dataclass(frozen=True)
class MarketConfig:
    version: str
    features: tuple[MarketFeature, ...]
    breadth: Optional[BreadthSettings]
    frequency_floor_days: Mapping[str, int]
    series_lag_days: Mapping[str, int]
    series_frequency: Mapping[str, str]
    pit_basis: str = "recorded"

    def feature_names(self) -> list[str]:
        return [f.feature for f in self.features]

    def fred_features(self) -> list[MarketFeature]:
        return [f for f in self.features if f.source == "fred"]

    def breadth_features(self) -> list[MarketFeature]:
        return [f for f in self.features if f.source == "silver_breadth"]

    def required_series(self) -> list[str]:
        seen: dict[str, None] = {}
        for f in self.fred_features():
            seen[f.series] = None       # type: ignore[index]
        return list(seen)

    def lag_days(self, series_id: str) -> int:
        return _resolve_lag(series_id, self.series_lag_days,
                            self.frequency_floor_days, self.series_frequency)


def validate_market_config(raw: Mapping[str, Any]) -> MarketConfig:
    """Validate a parsed fred_series.yaml mapping's `hmm_market:` block (the PIT
    rule comes from the shared `hmm.pit`); return the typed config."""
    registry = {s["id"]: s for s in raw.get("series", [])}
    _require("hmm" in raw, "fred_series.yaml has no `hmm:` block — the shared PIT rule lives in hmm.pit")
    _require("hmm_market" in raw, "fred_series.yaml has no `hmm_market:` block (ADR-051)")
    floors, overrides, basis = _validate_pit(raw["hmm"], registry)
    block = raw["hmm_market"]

    features: list[MarketFeature] = []
    names: set[str] = set()
    metrics: set[str] = set()
    for i, e in enumerate(block.get("features", [])):
        _require(isinstance(e, dict), f"hmm_market.features[{i}] must be a mapping, got {type(e).__name__}")
        where = f"hmm_market.features[{i}] ({e.get('feature', '?')})"
        name = e.get("feature")
        _require(isinstance(name, str) and name, f"{where}: missing `feature`")
        _require(name not in names, f"{where}: duplicate feature name")
        names.add(name)

        src = e.get("source")
        if isinstance(src, str) and (src.lower().startswith("gold") or "data/gold" in src.lower()):
            raise HMMConfigError(
                f"{where}: source {src!r} reads a Gold store — forbidden: the market branch is "
                f"Silver-only (ADR-051 Amendment 2 §A, domain independence)")
        _require(src in MARKET_SOURCE_KEYS,
                 f"{where}: unknown source {src!r} (allowed: {sorted(MARKET_SOURCE_KEYS)})")
        extra = set(e) - MARKET_SOURCE_KEYS[src]
        _require(not extra, f"{where}: unexpected keys {sorted(extra)} for source {src!r}")
        missing = MARKET_SOURCE_KEYS[src] - set(e) - ({"transform", "weight"} if src == "fred" else {"weight"})
        _require(not missing, f"{where}: missing keys {sorted(missing)} for source {src!r}")
        w = e.get("weight")
        _require(w is None or (isinstance(w, (int, float)) and not isinstance(w, bool) and math.isfinite(w)),
                 f"{where}: weight must be a finite number, got {w!r}")
        weight = None if w is None else float(w)

        if src == "fred":
            tr = e.get("transform", "level")
            _check_series_transform(where, e["series"], tr, registry)
            features.append(MarketFeature(feature=name, source="fred", series=e["series"], transform=tr, weight=weight))
        else:
            m = e["metric"]
            _require(m in BREADTH_METRICS, f"{where}: unknown breadth metric {m!r} (allowed: {sorted(BREADTH_METRICS)})")
            _require(m not in metrics, f"{where}: breadth metric {m!r} already used by another feature")
            metrics.add(m)
            features.append(MarketFeature(feature=name, source="silver_breadth", metric=m, weight=weight))

    _require(len(features) > 0, "hmm_market.features is empty")

    series_frequency: dict[str, str] = {}
    for f in features:
        if f.source != "fred":
            continue
        fr = registry[f.series].get("frequency")
        _require(fr in floors or f.series in overrides,
                 f"series {f.series!r} has frequency {fr!r} with no publication-lag floor "
                 f"and no per-series override")
        series_frequency[f.series] = fr

    breadth: Optional[BreadthSettings] = None
    if metrics:
        b = block.get("breadth")
        _require(isinstance(b, dict), "hmm_market.breadth is required when a silver_breadth feature exists")
        allowed = {"market", "timeframe", "min_symbols", "min_coverage_ratio", "rms_window", "guard_version"}
        _require(set(b) == allowed, f"hmm_market.breadth keys must be exactly {sorted(allowed)}, got {sorted(b)}")
        _require(isinstance(b["market"], str) and b["market"], "hmm_market.breadth.market must be a non-empty string")
        _require(isinstance(b["timeframe"], str) and b["timeframe"], "hmm_market.breadth.timeframe must be a non-empty string")
        _require(isinstance(b["guard_version"], str) and b["guard_version"],
                 "hmm_market.breadth.guard_version must be a non-empty string (the threshold set is versioned)")
        for k, lo in (("min_symbols", 1), ("rms_window", 2)):
            _require(isinstance(b[k], int) and not isinstance(b[k], bool) and b[k] >= lo,
                     f"hmm_market.breadth.{k} must be an int >= {lo}, got {b[k]!r}")
        r = b["min_coverage_ratio"]
        _require(isinstance(r, (int, float)) and not isinstance(r, bool) and 0 < r <= 1,
                 f"hmm_market.breadth.min_coverage_ratio must be in (0, 1], got {r!r}")
        breadth = BreadthSettings(
            market=b["market"], timeframe=b["timeframe"], min_symbols=b["min_symbols"],
            min_coverage_ratio=float(r), rms_window=b["rms_window"], guard_version=b["guard_version"],
        )

    return MarketConfig(
        version=str(block.get("version", "")),
        features=tuple(features),
        breadth=breadth,
        frequency_floor_days=floors,
        series_lag_days=overrides,
        series_frequency=series_frequency,
        pit_basis=basis,
    )


def market_config(path: Path | str | None = None) -> MarketConfig:
    """Load + validate the market routing block. Raises HMMConfigError.
    `path` defaults to FRED_SERIES_PATH, resolved at CALL time."""
    with open(FRED_SERIES_PATH if path is None else path) as fh:
        raw = yaml.safe_load(fh)
    return validate_market_config(raw)


# ═══ Macro model (hmm_macro_regime) ══════════════════════════════════════════
# Everything the macro HMM needs beyond the panel routing: state vocabulary, signed
# ordering weights, fit settings and the business-cycle label rule (ADR-051 Amendment 2
# §D/§E/§F). Kept separate from validate_macro_config on purpose: raw_macro neither needs
# nor may depend on any model setting.

FIT_KEYS = frozenset({"model_version", "n_states", "n_random_seeds", "n_iter", "tol", "min_obs_to_fit",
                      "short_history_min_obs", "min_state_share", "var_floor", "scaler_clip"})
LABEL_RULE_KEYS = frozenset({"axes", "required", "activity_threshold", "momentum_threshold", "momentum_window_bd"})


@dataclass(frozen=True)
class FitSettings:
    """HMM fit settings. `model_version` is the human-bumped label of the whole model
    specification; MacroModelConfig.fingerprint() is the automatic digest of everything."""
    model_version: str
    n_states: int
    n_random_seeds: int           # random EM seeds competing with the deterministic composite init
    n_iter: int
    tol: float
    min_obs_to_fit: int           # below this a domain is not fitted at all
    short_history_min_obs: int    # ADR-051 A2 §E: training window below this => short_history
    min_state_share: float        # degenerate-fit guard: every state must own at least this share
    var_floor: float              # variance floor, in robust-scaled units
    scaler_clip: float            # robust-scaled values are clipped to +/- this (ADR-051 A2 §D: 4)


@dataclass(frozen=True)
class MacroLabelRule:
    """Business-cycle label rule (ADR-051 A2 §F)."""
    axes: Mapping[str, int]       # domain -> +1 / -1 orientation of its score in `activity`
    required: str                 # a domain that must be present (and not short_history)
    activity_threshold: float
    momentum_threshold: float
    momentum_window_bd: int       # weekdays


@dataclass(frozen=True)
class MacroModelConfig:
    macro: MacroConfig
    labels: Mapping[str, tuple[str, ...]]            # domain -> state labels, ascending along the oriented axis
    weights: Mapping[str, Mapping[str, float]]       # domain -> feature -> signed ordering weight
    fit: FitSettings
    label_rule: MacroLabelRule

    @property
    def domains(self) -> tuple[str, ...]:
        return self.macro.domains

    def fingerprint(self) -> str:
        """Digest of EVERYTHING that defines the model: feature routing, weights, labels, fit
        settings, label rule. Written to every model file and stored row, so a changed setting
        that kept the old model_version is still visible."""
        payload = {
            "features": [
                [f.feature, f.domain, f.series, f.transform,
                 None if f.derived is None else [f.derived.op, f.derived.left, f.derived.right], f.weight]
                for f in self.macro.features
            ],
            "labels": {d: list(v) for d, v in self.labels.items()},
            "fit": dataclasses.asdict(self.fit),
            "rule": {**dataclasses.asdict(self.label_rule), "axes": dict(self.label_rule.axes)},
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:12]


def _num(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def _validate_fit(fit_raw: Any, key: str, what: str) -> FitSettings:
    """Validate one `fit:` block (shared by the macro and market models; `key` names it in messages)."""
    _require(isinstance(fit_raw, dict), f"{key} is required for the {what} model")
    _require(set(fit_raw) == FIT_KEYS, f"{key} keys must be exactly {sorted(FIT_KEYS)}, got {sorted(fit_raw)}")
    _require(isinstance(fit_raw["model_version"], str) and fit_raw["model_version"],
             f"{key}.model_version must be a non-empty string (the model specification is versioned)")
    for k, lo in (("n_states", 2), ("n_random_seeds", 0), ("n_iter", 1), ("min_obs_to_fit", 1),
                  ("short_history_min_obs", 1)):
        v = fit_raw[k]
        _require(isinstance(v, int) and not isinstance(v, bool) and v >= lo, f"{key}.{k} must be an int >= {lo}, got {v!r}")
    for k in ("tol", "var_floor", "scaler_clip"):
        _require(_num(fit_raw[k]) and fit_raw[k] > 0, f"{key}.{k} must be a number > 0, got {fit_raw[k]!r}")
    k_states = fit_raw["n_states"]
    share = fit_raw["min_state_share"]
    _require(_num(share) and 0 < share <= 1.0 / k_states,
             f"{key}.min_state_share must be in (0, 1/n_states], got {share!r}")
    return FitSettings(
        model_version=fit_raw["model_version"], n_states=k_states, n_random_seeds=fit_raw["n_random_seeds"],
        n_iter=fit_raw["n_iter"], tol=float(fit_raw["tol"]), min_obs_to_fit=fit_raw["min_obs_to_fit"],
        short_history_min_obs=fit_raw["short_history_min_obs"], min_state_share=float(share),
        var_floor=float(fit_raw["var_floor"]), scaler_clip=float(fit_raw["scaler_clip"]),
    )


def validate_macro_model_config(raw: Mapping[str, Any]) -> MacroModelConfig:
    """Validate the macro model settings of a parsed fred_series.yaml (which must also pass
    validate_macro_config); return the typed config."""
    macro = validate_macro_config(raw)
    block = raw["hmm"]

    fit = _validate_fit(block.get("fit"), "hmm.fit", "macro")
    k_states = fit.n_states

    labels_raw = block.get("labels")
    _require(isinstance(labels_raw, dict) and set(labels_raw) == set(macro.domains),
             f"hmm.labels must define exactly the domains {list(macro.domains)}")
    labels: dict[str, tuple[str, ...]] = {}
    for d in macro.domains:
        v = labels_raw[d]
        _require(isinstance(v, list) and len(v) == k_states and all(isinstance(x, str) and x for x in v)
                 and len(set(v)) == len(v),
                 f"hmm.labels.{d} must be {k_states} distinct non-empty strings (ascending along the axis), got {v!r}")
        labels[d] = tuple(v)

    weights: dict[str, dict[str, float]] = {d: {} for d in macro.domains}
    for f in macro.features:
        _require(f.weight is not None, f"feature {f.feature!r}: `weight` is required by the macro model "
                                       f"(signed ordering weight; 0.0 = in the emission model, out of the ordering)")
        weights[f.domain][f.feature] = f.weight
    for d in macro.domains:
        _require(any(w != 0.0 for w in weights[d].values()),
                 f"domain {d!r}: at least one non-zero ordering weight is needed to order its states")

    rule_raw = block.get("macro_label")
    _require(isinstance(rule_raw, dict), "hmm.macro_label is required")
    _require(set(rule_raw) == LABEL_RULE_KEYS,
             f"hmm.macro_label keys must be exactly {sorted(LABEL_RULE_KEYS)}, got {sorted(rule_raw)}")
    axes = rule_raw["axes"]
    _require(isinstance(axes, dict) and len(axes) > 0 and set(axes) <= set(macro.domains)
             and all(v in (1, -1) and not isinstance(v, bool) for v in axes.values()),
             f"hmm.macro_label.axes must map domains in {list(macro.domains)} to +1 / -1, got {axes!r}")
    _require(rule_raw["required"] in axes, f"hmm.macro_label.required must be one of the axes {sorted(axes)}")
    for k in ("activity_threshold", "momentum_threshold"):
        _require(_num(rule_raw[k]) and rule_raw[k] > 0, f"hmm.macro_label.{k} must be a number > 0")
    mw = rule_raw["momentum_window_bd"]
    _require(isinstance(mw, int) and not isinstance(mw, bool) and mw >= 1,
             f"hmm.macro_label.momentum_window_bd must be an int >= 1, got {mw!r}")
    rule = MacroLabelRule(axes=dict(axes), required=rule_raw["required"],
                          activity_threshold=float(rule_raw["activity_threshold"]),
                          momentum_threshold=float(rule_raw["momentum_threshold"]), momentum_window_bd=mw)
    return MacroModelConfig(macro=macro, labels=labels, weights=weights, fit=fit, label_rule=rule)


def macro_model_config(path: Path | str | None = None) -> MacroModelConfig:
    """Load + validate the macro model settings. Raises HMMConfigError.
    `path` defaults to FRED_SERIES_PATH, resolved at CALL time."""
    with open(FRED_SERIES_PATH if path is None else path) as fh:
        raw = yaml.safe_load(fh)
    return validate_macro_model_config(raw)


# ═══ Market model (hmm_market_regime) ═══════════════════════════════════════════════════════════════
# One 3-state HMM over raw_market's panel: state vocabulary, signed ordering weights and fit
# settings, all in the `hmm_market:` block. Kept separate from validate_market_config for the
# same reason as the macro model: raw_market neither needs nor may depend on a model setting.


@dataclass(frozen=True)
class MarketModelConfig:
    market: MarketConfig
    labels: tuple[str, ...]                 # state labels, ascending along the oriented axis (risk_off ... risk_on)
    weights: Mapping[str, float]            # feature -> signed ordering weight (0.0 = in the model, out of the ordering)
    fit: FitSettings

    def feature_names(self) -> list[str]:
        return self.market.feature_names()

    def fingerprint(self) -> str:
        """Digest of EVERYTHING that defines the model: feature routing + weights, labels, fit
        settings, the breadth definition and the PIT basis. Written to every model file and stored
        row, so a changed setting that kept the old model_version is still visible."""
        b = self.market.breadth
        payload = {
            "features": [[f.feature, f.source, f.series, f.transform, f.metric, f.weight] for f in self.market.features],
            "labels": list(self.labels),
            "fit": dataclasses.asdict(self.fit),
            "breadth": None if b is None else dataclasses.asdict(b),
            "pit_basis": self.market.pit_basis,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:12]


def validate_market_model_config(raw: Mapping[str, Any]) -> MarketModelConfig:
    """Validate the market model settings of a parsed fred_series.yaml (which must also pass
    validate_market_config); return the typed config."""
    market = validate_market_config(raw)
    block = raw["hmm_market"]
    fit = _validate_fit(block.get("fit"), "hmm_market.fit", "market")

    v = block.get("labels")
    _require(isinstance(v, list) and len(v) == fit.n_states and all(isinstance(x, str) and x for x in v)
             and len(set(v)) == len(v),
             f"hmm_market.labels must be {fit.n_states} distinct non-empty strings (ascending along the axis), got {v!r}")

    weights: dict[str, float] = {}
    for f in market.features:
        _require(f.weight is not None, f"feature {f.feature!r}: `weight` is required by the market model "
                                       f"(signed ordering weight; 0.0 = in the emission model, out of the ordering)")
        weights[f.feature] = f.weight
    _require(any(w != 0.0 for w in weights.values()),
             "hmm_market: at least one non-zero ordering weight is needed to order the states")
    return MarketModelConfig(market=market, labels=tuple(v), weights=weights, fit=fit)


def market_model_config(path: Path | str | None = None) -> MarketModelConfig:
    """Load + validate the market model settings. Raises HMMConfigError.
    `path` defaults to FRED_SERIES_PATH, resolved at CALL time."""
    with open(FRED_SERIES_PATH if path is None else path) as fh:
        raw = yaml.safe_load(fh)
    return validate_market_model_config(raw)
