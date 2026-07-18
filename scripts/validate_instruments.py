"""
validate_instruments.py — G7 Supplementary Design v1.1
                           + GMI Wave 1 (Architecture Extension v1.0 §8.3,
                             Data Source & Rates Adjustment v1.0 §11.2)
Validasi config/instruments.yaml setelah migrasi.
Jalankan setelah migrate_instruments.py / build_instruments_v14.py.

Usage: python scripts/validate_instruments.py
Exit code 0 = PASSED, 1 = FAILED

Catatan desain: validator ini SENGAJA tidak meng-import InstrumentLoader.
Validasi bekerja langsung di atas raw YAML sebagai ground-truth independen —
jika InstrumentLoader punya bug parsing, validator yang bergantung padanya
akan punya blind spot yang sama. Independence ini adalah lapisan pertahanan
terpisah (CI Gate G-3).

# ADD GMI-VAL-001 — v1.4 dual-layer validation (Architecture Extension v1.0):
  - EXPECTED_TOTAL: 643 -> 692 (Layer 1: 640 + Layer 2 OHLCV: 52, termasuk
    3 deferred — ADR-007: "EXPECTED_TOTAL tetap 692, deferred tetap dihitung
    sebagai target universe")
  - Layer 1 (us_stocks/idx_stocks/commodity/forex/index): validasi field
    wajib, format yfinance_symbol, duplikat — TIDAK BERUBAH dari v1.1
  - NEW: Layer 2 context validation — deferred fields, proxy fields,
    reclassified audit, subcategory coverage (20), include_in_forecast
    consistency untuk etf_broad/sector/factor
  - NEW (Rates Adjustment v1.0 §11.2): dm_cb==9, em_cb==3, no OHLCV di CB
    subcategories, context_rates_policy HARUS absent (renamed -> fed)

# UPD GMI-VAL-002 — GMI_Decision_Document_v1.docx (ADR-013/014/019) +
  GMI_Decision_Document_v2.docx (ADR-023/024), 2026-07-11:
  - EXPECTED_TOTAL: 692 -> 699. Subcategories: 20 -> 22
    (+context_dollar_basket, +context_fx_normalization)
  - NEW: ZERO_WEIGHT_SUBCATEGORIES guard — the two new subcategories must
    keep contributes_to: [] permanently (ADR-014)
  - NEW: domain-score weight-sum validation — every score's
    _meta.contributes_to weights across the whole context tree must sum to
    exactly 1.00 (ADR-019 literal restoration)
  - NEW: context_fx_normalization added to FORECAST_EXCLUDED_SUBCATEGORIES
    (ADR-024 — MYR is a Silver-layer normalization input only, never a
    CrossAssetEngine/ForecastModule feature)

# UPD GMI-VAL-003 — GMI_Decision_Document_v3.docx (Decision B Step 1),
  2026-07-17. Closes the Architecture v2.1 Addendum §7.1/§8 gap:
  - commodity_role/commodity_subcategory now required on ALL 14 commodity
    instruments (3 Layer 1 commodity_trading + 11 Layer 2
    commodity_context, deferred included) — enum-validated against
    VALID_COMMODITY_ROLES / VALID_COMMODITY_SUBCATEGORIES
  - EXPECTED_TOTAL / subcategory count UNCHANGED (699 / 22) — this is a
    field-level taxonomy addition, not a universe-size change
"""

import sys
from pathlib import Path
from collections import Counter

import yaml

# UPD ADR-013/014/024 (GMI Decision Documents v1/v2, 2026-07-11):
#   - EXPECTED_TOTAL: 692 -> 699 (+7: 6 context_dollar_basket currencies +
#     1 context_fx_normalization currency, instruments.yaml v1.5)
#   - EXPECTED_SUBCATEGORIES: 20 -> 22 (+context_dollar_basket,
#     +context_fx_normalization)
#   - ZERO_WEIGHT_SUBCATEGORIES (NEW): both new subcategories must carry
#     contributes_to: [] permanently — ADR-014 rejected folding basket
#     currencies into context_dollar specifically to avoid triple-counting
#     DM/EM dollar strength across DXY + raw pairs + the future Broad Dollar
#     derived feature. A future accidental weight addition here would
#     silently reintroduce that exact risk.
EXPECTED_TOTAL = 699

REQUIRED_FIELDS: dict[str, list[str]] = {
    "us_stocks": ["symbol"],
    "idx_stocks": ["symbol", "yfinance_symbol"],
    # UPD Decision B Step 1: commodity_role/commodity_subcategory required
    # (Architecture v2.1 Addendum §7.1 — "Required For: ALL commodity").
    "commodity":  ["symbol", "yfinance_symbol", "commodity_role", "commodity_subcategory"],
    "forex":      ["symbol", "raw_symbol", "yfinance_symbol"],
    "index":      ["symbol", "yfinance_symbol"],
}

# GMI-VAL-001: canonical 22-subcategory taxonomy — Rates Adjustment v1.0 §5.1
# + GMI_Decision_Document_v1.docx ADR-014 + v2.docx ADR-024
EXPECTED_SUBCATEGORIES: frozenset = frozenset({
    "context_dollar",
    "context_dollar_basket", "context_fx_normalization",
    "context_rates_fed", "context_rates_curve", "context_rates_spread",
    "context_rates_dm_cb", "context_rates_em_cb",
    "context_equity_dm", "context_equity_em", "context_volatility",
    "context_commodity_energy", "context_commodity_metals",
    "context_commodity_agri", "context_commodity_coal",
    "context_etf_broad", "context_etf_sector", "context_etf_factor",
    "context_etf_credit", "context_etf_commodity",
    "context_etf_international", "context_etf_thematic",
})

# ADD ADR-014/024: subcategories whose contributes_to MUST remain empty.
ZERO_WEIGHT_SUBCATEGORIES: frozenset = frozenset({
    "context_dollar_basket", "context_fx_normalization",
})

# ADD ADR-019 (GMI_Decision_Document_v1.docx): every domain score's
# contributor weights must sum to exactly 1.00 — restored to literal
# fidelity with Architecture Extension v1.0 §5.2 / Data Source & Rates
# Adjustment v1.0 §7 after a full audit found 5 of 8 scores drifted.
DOMAIN_SCORE_WEIGHT_TOLERANCE = 1e-9

# GMI-VAL-001: subcategories that must have include_in_forecast=False for
# ALL member instruments — ADR-002 (multicollinearity with Layer 1 holdings)
# + ADR-024 (context_fx_normalization: MYR is a Silver-layer normalization
# input only, never a CrossAssetEngine/ForecastModule feature)
FORECAST_EXCLUDED_SUBCATEGORIES: frozenset = frozenset({
    "context_etf_broad", "context_etf_sector", "context_etf_factor",
    "context_fx_normalization",
})

# GMI-VAL-001: subcategory_id no longer valid post Rates Adjustment v1.0 §5.1
RENAMED_SUBCATEGORY_OLD_ID = "context_rates_policy"

# Layer 1 symbols that MUST NOT remain after ADR-003 reclassification
RECLASSIFIED_SYMBOLS: frozenset = frozenset({"SPX", "VIX", "DXY"})

# Subcategories that are CB-rate macro series only — must carry NO OHLCV
# instruments (Rates Adjustment v1.0 §11.2 "no OHLCV in CB subcategories")
CB_RATE_SUBCATEGORY_IDS: frozenset = frozenset({
    "context_rates_fed", "context_rates_curve", "context_rates_spread",
    "context_rates_dm_cb", "context_rates_em_cb",
})

EXPECTED_DM_CB_COUNT = 9
EXPECTED_EM_CB_COUNT = 3

# ADD Decision B Step 1 (GMI_Decision_Document_v3.docx, Architecture v2.1
# Addendum §7.1/§8.2): canonical enum values for the two new commodity
# taxonomy fields. VALID_COMMODITY_SUBCATEGORY_KEYS is declared
# independently here (not imported from src/gold/sector_rotation.py) —
# same design principle as the rest of this file: validator must not share
# a blind spot with the code it's validating. Keep in sync with
# REGIME_SECTOR_WEIGHTS's key set by hand if either changes.
VALID_COMMODITY_ROLES: frozenset = frozenset({"trading", "context"})
VALID_COMMODITY_SUBCATEGORIES: frozenset = frozenset({
    "energy", "precious_metals", "base_metals", "agricultural", "bulks",
})
# Mechanical f"commodity_{subcategory}" formula (Addendum §8.4), with the
# one documented exception: 'precious_metals' -> 'commodity_precious_metals',
# NOT 'commodity_precious' as Addendum §8.2's key-name table states — that
# table does not match §7.1's own enum value and was resolved in favour of
# the formula (see src/gold/sector_rotation.py's REGIME_SECTOR_WEIGHTS
# docstring for full rationale). This map exists so validate_instruments.py
# can catch the same orphaned-key failure mode independently of
# sector_rotation.py itself.
COMMODITY_SUBCATEGORY_TO_WEIGHT_KEY: dict[str, str] = {
    s: f"commodity_{s}" for s in VALID_COMMODITY_SUBCATEGORIES
}


def _flatten_layer1_items(market: str, content) -> list[dict]:
    """Layer 1 flatten helper — unchanged behaviour from v1.1."""
    if isinstance(content, list):
        return content
    items: list[dict] = []
    for sector_items in content.values():
        items.extend(sector_items)
    return items


def _validate_commodity_taxonomy(
    item: dict, sym: str, scope_label: str, errors: list[str]
) -> None:
    """
    ADD Decision B Step 1 (GMI_Decision_Document_v3.docx): shared
    commodity_role/commodity_subcategory validation, called for both Layer
    1 commodity_trading and Layer 2 commodity_context instruments — a
    single check function so the two call sites cannot silently drift
    apart (the same "one validator, not two" principle this file already
    applies elsewhere).
    """
    role = item.get("commodity_role")
    subcat = item.get("commodity_subcategory")

    if role is not None and role not in VALID_COMMODITY_ROLES:
        errors.append(
            f"[{scope_label}] {sym}: commodity_role={role!r} not in"
            f" {sorted(VALID_COMMODITY_ROLES)}"
        )

    if subcat is not None:
        if subcat not in VALID_COMMODITY_SUBCATEGORIES:
            errors.append(
                f"[{scope_label}] {sym}: commodity_subcategory={subcat!r}"
                f" not in {sorted(VALID_COMMODITY_SUBCATEGORIES)}"
            )
        elif subcat not in COMMODITY_SUBCATEGORY_TO_WEIGHT_KEY:
            # Unreachable given VALID_COMMODITY_SUBCATEGORIES check above,
            # kept as an explicit belt-and-suspenders guard against the two
            # sets ever being edited out of sync with each other.
            errors.append(
                f"[{scope_label}] {sym}: commodity_subcategory={subcat!r}"
                " has no corresponding REGIME_SECTOR_WEIGHTS key mapping"
            )


def _validate_layer1(data: dict, errors: list[str]) -> list[str]:
    """
    Validate Layer 1 markets (us_stocks/idx_stocks/commodity/forex/index).
    Logic identical to v1.1 — preserved verbatim except market loop now
    explicitly excludes 'context' (handled by _validate_layer2).
    Returns the flat list of all Layer 1 symbols collected.
    """
    all_symbols: list[str] = []
    layer1_markets = ("us_stocks", "idx_stocks", "commodity", "forex", "index")

    for market in layer1_markets:
        content = data.get(market)
        if content is None:
            continue
        items = _flatten_layer1_items(market, content)
        req = REQUIRED_FIELDS.get(market, ["symbol"])

        for item in items:
            sym = item.get("symbol", "")
            all_symbols.append(sym)

            for fld in req:
                if fld not in item:
                    errors.append(
                        f"[{market}] {sym}: missing required field '{fld}'"
                    )

            if "." in sym or "/" in sym:
                errors.append(
                    f"[{market}] '{sym}': unsafe character in symbol"
                    " — use normalize_symbol()"
                )

            yf = item.get("yfinance_symbol", "")
            if market == "idx_stocks" and yf and not yf.endswith(".JK"):
                errors.append(
                    f"[{market}] {sym}: yfinance_symbol should end with .JK,"
                    f" got '{yf}'"
                )
            if market == "forex" and yf and not yf.endswith("=X") and yf != "DX-Y.NYB":
                errors.append(
                    f"[{market}] {sym}: yfinance_symbol should end with =X,"
                    f" got '{yf}'"
                )
            if market == "commodity" and yf and not yf.endswith("=F"):
                errors.append(
                    f"[{market}] {sym}: yfinance_symbol should end with =F,"
                    f" got '{yf}'"
                )
            # ADD Decision B Step 1: commodity_role/commodity_subcategory
            # enum validation for Layer 1 commodity_trading instruments.
            if market == "commodity":
                _validate_commodity_taxonomy(item, sym, market, errors)
            if (
                market == "index"
                and yf
                and not yf.startswith("^")
                and yf != "DX-Y.NYB"
            ):
                errors.append(
                    f"[{market}] {sym}: yfinance_symbol should start with ^,"
                    f" got '{yf}'"
                )

        dupes = [s for s, c in Counter(i.get("symbol", "") for i in items).items() if c > 1]
        if dupes:
            errors.append(f"[{market}] Duplicate symbols within market: {dupes}")

    # ADD GMI-VAL-001: reclassified audit — ADR-003. SPX/VIX/DXY must be
    # GONE from Layer 1 (moved to Layer 2 context exclusively).
    found_reclassified = set(all_symbols) & RECLASSIFIED_SYMBOLS
    if found_reclassified:
        errors.append(
            f"[reclassified_audit] {sorted(found_reclassified)} found in Layer 1"
            " — ADR-003 requires SPX/VIX/DXY exclusively in Layer 2 context."
            " Remove from us_stocks/index/forex."
        )

    return all_symbols


def _walk_context_subcategories(ctx: dict):
    """
    ADD GMI-VAL-001: yield (subcategory_id, subcat_block, parent_key) for
    every subcategory block in the context section, regardless of nesting
    shape (direct like 'dollar', or grouped like 'equity.dm').
    """
    direct_keys = ("dollar", "dollar_basket", "fx_normalization")
    grouped_keys = ("rates", "equity", "commodity", "etf")

    for key in direct_keys:
        block = ctx.get(key, {})
        if isinstance(block, dict):
            meta = block.get("_meta", {})
            yield meta.get("subcategory_id"), block, key

    for group_key in grouped_keys:
        group_block = ctx.get(group_key, {})
        if not isinstance(group_block, dict):
            continue
        for subcat_block in group_block.values():
            if not isinstance(subcat_block, dict):
                continue
            meta = subcat_block.get("_meta", {})
            yield meta.get("subcategory_id"), subcat_block, group_key


def _validate_layer2(data: dict, errors: list[str]) -> list[str]:
    """
    ADD GMI-VAL-001 — validate Layer 2 `context` section per Architecture
    Extension v1.0 §8.3 and Data Source & Rates Adjustment v1.0 §11.2.
    Returns the flat list of all Layer 2 OHLCV-bearing symbols.
    """
    ctx = data.get("context", {})
    if not ctx:
        errors.append("[context] Section 'context' missing — Layer 2 required since v1.4")
        return []

    all_symbols: list[str] = []
    seen_subcategory_ids: set[str] = set()

    for subcat_id, subcat_block, parent_key in _walk_context_subcategories(ctx):
        if subcat_id:
            seen_subcategory_ids.add(subcat_id)

        instruments = subcat_block.get("instruments", [])

        # NEW (Rates Adjustment v1.0 §11.2): CB-rate subcategories must carry
        # NO OHLCV instruments — they are FRED/BIS macro series only.
        if subcat_id in CB_RATE_SUBCATEGORY_IDS and instruments:
            errors.append(
                f"[context.{parent_key}] {subcat_id}: CB-rate subcategory must"
                f" have no OHLCV 'instruments' list, found {len(instruments)}"
            )

        for item in instruments:
            sym = item.get("symbol", "")
            all_symbols.append(sym)

            if "symbol" not in item:
                errors.append(f"[context.{parent_key}] missing 'symbol' field: {item}")

            # NEW: deferred validation (Extension v1.0 §8.3)
            if item.get("context_available") is False:
                if not item.get("deferred_reason"):
                    errors.append(
                        f"[context.{parent_key}] {sym}: context_available=false"
                        " requires deferred_reason"
                    )
                if item.get("planned_wave") is None:
                    errors.append(
                        f"[context.{parent_key}] {sym}: context_available=false"
                        " requires planned_wave"
                    )

            # NEW: proxy validation (Extension v1.0 §8.3)
            if item.get("proxy_for"):
                if not item.get("proxy_instrument"):
                    errors.append(
                        f"[context.{parent_key}] {sym}: proxy_for set but"
                        " proxy_instrument missing"
                    )
                if item.get("proxy_correlation_expected") is None:
                    errors.append(
                        f"[context.{parent_key}] {sym}: proxy_for set but"
                        " proxy_correlation_expected missing"
                    )

            # ADD Decision B Step 1 (GMI_Decision_Document_v3.docx):
            # commodity_role/commodity_subcategory required on ALL 11
            # context.commodity.* instruments — deferred (TIN/CPO/RUBBER)
            # included, per Addendum §7.1 "Required For: ALL commodity".
            if parent_key == "commodity":
                if not item.get("commodity_role"):
                    errors.append(
                        f"[context.{parent_key}] {sym}: missing required"
                        " field 'commodity_role'"
                    )
                if not item.get("commodity_subcategory"):
                    errors.append(
                        f"[context.{parent_key}] {sym}: missing required"
                        " field 'commodity_subcategory'"
                    )
                _validate_commodity_taxonomy(item, sym, f"context.{parent_key}", errors)

        # NEW: include_in_forecast consistency (ADR-002 / ADR-024)
        if subcat_id in FORECAST_EXCLUDED_SUBCATEGORIES:
            bad = [
                i.get("symbol") for i in instruments
                if i.get("include_in_forecast", True) is True
            ]
            if bad:
                errors.append(
                    f"[context.{parent_key}] {subcat_id}: instruments"
                    f" {bad} must have include_in_forecast=false (ADR-002)"
                )

        # ADD ADR-014/024: context_dollar_basket / context_fx_normalization
        # must never carry a domain-score weight — see ZERO_WEIGHT_SUBCATEGORIES
        # docstring above for the triple-counting risk this guards against.
        if subcat_id in ZERO_WEIGHT_SUBCATEGORIES:
            contributes = subcat_block.get("_meta", {}).get("contributes_to") or []
            if contributes:
                errors.append(
                    f"[context.{parent_key}] {subcat_id}: contributes_to must"
                    f" be empty (ADR-014/024), found {contributes}"
                )

        dupes = [
            s for s, c in
            Counter(i.get("symbol", "") for i in instruments).items() if c > 1
        ]
        if dupes:
            errors.append(
                f"[context.{parent_key}] {subcat_id}: duplicate symbols {dupes}"
            )

    # NEW: subcategory coverage — Rates Adjustment v1.0 §5.1, extended to 22
    # by ADR-014/024
    missing_subcats = EXPECTED_SUBCATEGORIES - seen_subcategory_ids
    if missing_subcats:
        errors.append(
            f"[context] Missing required subcategories: {sorted(missing_subcats)}"
        )
    unexpected_subcats = seen_subcategory_ids - EXPECTED_SUBCATEGORIES
    if unexpected_subcats:
        errors.append(
            f"[context] Unrecognized subcategory_id (not in canonical 22):"
            f" {sorted(unexpected_subcats)}"
        )
    if len(seen_subcategory_ids) != len(EXPECTED_SUBCATEGORIES):
        errors.append(
            f"[context] Expected {len(EXPECTED_SUBCATEGORIES)} subcategories,"
            f" found {len(seen_subcategory_ids)}"
        )

    # NEW (Rates Adjustment v1.0 §11.2): context_rates_policy must be ABSENT
    if RENAMED_SUBCATEGORY_OLD_ID in seen_subcategory_ids:
        errors.append(
            f"[context.rates] '{RENAMED_SUBCATEGORY_OLD_ID}' must be absent"
            " — renamed to 'context_rates_fed' per Rates Adjustment v1.0 §5.1"
        )

    # NEW (Rates Adjustment v1.0 §11.2): dm_cb==9, em_cb==3 central bank counts
    rates_block = ctx.get("rates", {})
    dm_cb_meta = rates_block.get("dm_cb", {}).get("_meta", {})
    em_cb_meta = rates_block.get("em_cb", {}).get("_meta", {})
    dm_cb_count = len(dm_cb_meta.get("central_banks", []))
    em_cb_count = len(em_cb_meta.get("central_banks", []))
    if dm_cb_count != EXPECTED_DM_CB_COUNT:
        errors.append(
            f"[context.rates.dm_cb] Expected {EXPECTED_DM_CB_COUNT} central banks,"
            f" got {dm_cb_count}"
        )
    if em_cb_count != EXPECTED_EM_CB_COUNT:
        errors.append(
            f"[context.rates.em_cb] Expected {EXPECTED_EM_CB_COUNT} central banks,"
            f" got {em_cb_count}"
        )

    return all_symbols


def _validate_domain_score_weights(data: dict, errors: list[str]) -> None:
    """
    ADD ADR-019 (GMI_Decision_Document_v1.docx): walk every _meta.contributes_to
    block in the whole `context` tree (dollar/dollar_basket/fx_normalization/
    rates/equity/commodity/etf, including nested subcategory groups) and
    assert each domain score's contributor weights sum to exactly 1.00.

    This is the permanent regression guard requested by
    GMI_Decision_Document_v1.docx §9 Definition of Done — a full audit found
    5 of 8 scores (score_dollar_strength, score_yield_curve,
    score_global_growth, score_inflation_pressure, score_risk_appetite) had
    drifted from Architecture Extension v1.0 §5.2 / Data Source & Rates
    Adjustment v1.0 §7's documented tables via undocumented contributor
    weights with no traceable rationale (Section 3.4 of that document).
    """
    ctx = data.get("context", {})
    sums: dict[str, float] = {}

    def _walk(node) -> None:
        if not isinstance(node, dict):
            return
        meta = node.get("_meta")
        if isinstance(meta, dict):
            for c in meta.get("contributes_to") or []:
                score = c.get("score")
                weight = c.get("weight", 0.0)
                if score:
                    sums[score] = sums.get(score, 0.0) + float(weight)
        for key, val in node.items():
            if key == "_meta":
                continue
            _walk(val)

    _walk(ctx)

    for score, total in sorted(sums.items()):
        if abs(total - 1.00) > DOMAIN_SCORE_WEIGHT_TOLERANCE:
            errors.append(
                f"[domain_scores] {score}: contributes_to weights sum to"
                f" {total:.4f}, expected 1.00 (ADR-019)"
            )


def validate(path: str = None) -> bool:
    if path is None:
        path = str(
            Path(__file__).parent.parent / "config" / "instruments.yaml"
        )

    data = yaml.safe_load(Path(path).read_text())
    errors: list[str] = []

    layer1_symbols = _validate_layer1(data, errors)
    layer2_symbols = _validate_layer2(data, errors)
    _validate_domain_score_weights(data, errors)
    all_symbols = layer1_symbols + layer2_symbols

    if len(all_symbols) != EXPECTED_TOTAL:
        errors.append(
            f"Expected {EXPECTED_TOTAL} symbols total"
            f" (Layer 1 + Layer 2 incl. deferred), got {len(all_symbols)}"
            f" (Layer 1={len(layer1_symbols)}, Layer 2={len(layer2_symbols)})"
        )

    # ── Report ────────────────────────────────────────────────────────────
    if errors:
        print(f"\nVALIDATION FAILED — {len(errors)} error(s) found:\n")
        for e in errors:
            print(f"  ERROR: {e}")
        print()
        return False

    print(
        f"\nVALIDATION PASSED — {len(all_symbols)} symbols"
        f" (Layer 1={len(layer1_symbols)}, Layer 2={len(layer2_symbols)}),"
        f" no errors.\n"
    )
    return True


if __name__ == "__main__":
    sys.exit(0 if validate() else 1)
