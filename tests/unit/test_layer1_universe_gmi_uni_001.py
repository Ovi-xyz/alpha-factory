"""
GMI-UNI-001 (chat thread, Ovi, 7 Nov 2026) — Layer 1 universe update.

  * us_stocks:     PARA removed (delisted after the Paramount / Skydance merger).
  * index_futures: NEW Layer 1 market — ES_F, NKD_F, NQ_F, YM_F (Yahoo ES=F, NKD=F, NQ=F, YM=F).
  * always-in markets: (forex, commodity, index_futures); the legacy `index` leaves the set.
  * the resolved active set stays <= 200.

What is pinned here, and why each pin exists:

  1. Registry facts (counts, membership, PARA gone).
  2. The ES / ES_F collision. Bare `ES` is the us_stocks ticker for Eversource. The futures use the
     path-safe symbol `ES_F`; a collision would interleave two instruments under one Hive partition
     (the CL / RISK-30 class of bug).
  3. Bronze fetches futures through `yfinance_symbol`, never by deriving a ticker from the symbol.
  4. Validator rules (unsafe '=', symbol == raw with '=' -> '_', global uniqueness) and the schemas.
  5. MARKETS in the intelligence feature builders mirrors layer1_markets() (a miss is a SILENT null
     market_code) and existing market codes never shift.
  6. The <= 200 ceiling, end-to-end on the REAL registry and on a registry with a 5th always-in future.
"""
from __future__ import annotations

import dataclasses
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import jsonschema
import polars as pl
import pytest
import yaml

from scripts import validate_instruments as vi
from src.bronze.market_ingester import DIRECT_YFINANCE_SYMBOL_MARKETS, MarketOHLCVIngester
from src.config.instrument_loader import get_loader
from src.gold.intelligence import features_technical as fg
from src.gold.intelligence import features_hmm as fh
from src.silver import active_symbols as act
from src.silver.ohlcv_aggregator import NEAR_24H_MARKETS
from src.silver.ohlcv_processor import OHLCVProcessor
from src.utils.silver_scope import layer1_markets, layer1_peer_groups
from src.utils.symbol_utils import normalize_symbol, to_api_symbol

FUTURES = {"ES_F": "ES=F", "NKD_F": "NKD=F", "NQ_F": "NQ=F", "YM_F": "YM=F"}


# ── 1. Registry facts ─────────────────────────────────────────────────────────

class TestRegistry:

    def test_index_futures_market_members(self):
        loader = get_loader()
        members = {i.symbol: i for i in loader.by_market("index_futures")}
        assert set(members) == set(FUTURES)
        for sym, yf in FUTURES.items():
            inst = members[sym]
            assert inst.raw_symbol == yf and inst.yfinance_symbol == yf
            assert inst.market == "index_futures" and inst.is_index_futures
            assert inst.timezone == "America/New_York"

    def test_layer1_counts(self):
        loader = get_loader()
        assert loader.count() == 596
        assert len(loader.by_market("us_stocks")) == 541

    def test_para_is_gone_everywhere_in_the_registry(self):
        loader = get_loader()
        assert "PARA" not in {i.symbol for i in loader.all_symbols()}
        for f in ("identity", "taxonomy"):
            text = Path(f"config/instruments_{f}.yaml").read_text()
            assert "symbol: PARA" not in text

    def test_split_files_stay_in_lockstep(self):
        ident = yaml.safe_load(Path("config/instruments_identity.yaml").read_text())
        tax = yaml.safe_load(Path("config/instruments_taxonomy.yaml").read_text())
        assert [i["symbol"] for g in ident["index_futures"].values() for i in g] == \
               [i["symbol"] for g in tax["index_futures"].values() for i in g]

    def test_real_split_files_validate(self):
        assert vi.validate_split() is True
        assert vi.EXPECTED_TOTAL == 656


# ── 2. ES vs ES_F — the cross-market collision this design avoids ─────────────

class TestEversourceCollision:

    def test_bare_ES_is_still_eversource_in_us_stocks(self):
        loader = get_loader()
        assert loader.get("ES").market == "us_stocks"
        assert loader.market_map()["ES"] == "us_stocks"
        assert loader.market_map()["ES_F"] == "index_futures"

    def test_no_symbol_is_registered_twice_across_layer1_and_layer2(self):
        loader = get_loader()
        syms = [i.symbol for i in loader.all_symbols()] + [i.symbol for i in loader.all_context(include_deferred=True)]
        dupes = sorted({s for s in syms if syms.count(s) > 1})
        assert dupes == [], f"cross-market symbol collision: {dupes}"

    def test_peer_groups_have_no_collision_and_a_four_member_futures_group(self):
        peer_map, sizes = layer1_peer_groups()
        assert sizes["index_futures"] == 4
        assert peer_map["ES_F"] == "index_futures"
        assert peer_map["ES"] == "us_stocks"          # None would mean a detected collision

    def test_no_layer1_symbol_contains_an_unsafe_hive_character(self):
        for inst in get_loader().all_symbols():
            assert not any(c in inst.symbol for c in ".=/"), inst.symbol


# ── 3. Bronze routing ─────────────────────────────────────────────────────────

class TestBronzeRouting:

    @pytest.mark.parametrize("sym,yahoo", sorted(FUTURES.items()))
    def test_futures_fetch_through_yfinance_symbol(self, sym, yahoo):
        inst = get_loader().get(sym)
        assert MarketOHLCVIngester._api_symbol_for(inst, "yfinance") == yahoo

    def test_routing_does_not_depend_on_raw_symbol(self):
        """Even if raw_symbol were mistyped as the bare root 'ES' (a valid Yahoo ticker — Eversource), the
        future is still fetched as ES=F. Deriving from raw/symbol would silently fetch a DIFFERENT instrument."""
        wrong = dataclasses.replace(get_loader().get("ES_F"), raw_symbol="ES")
        assert MarketOHLCVIngester._api_symbol_for(wrong, "yfinance") == "ES=F"

    def test_other_markets_are_unchanged(self):
        loader = get_loader()
        f = MarketOHLCVIngester._api_symbol_for
        assert f(loader.get("AU"), "yfinance") == "GC=F"          # ADR-047 (commodity)
        assert f(loader.get("AAPL"), "yfinance") == "AAPL"
        assert f(loader.get("BBCA"), "yfinance") == "BBCA.JK"
        assert f(loader.get("EUR_USD"), "yfinance") == "EURUSD=X"
        assert f(loader.get("ES"), "yfinance") == "ES"            # the equity keeps its own ticker

    def test_direct_routing_markets_constant(self):
        assert DIRECT_YFINANCE_SYMBOL_MARKETS == frozenset({"commodity", "index_futures"})

    def test_to_api_symbol_and_normalization(self):
        assert to_api_symbol("ES=F", "index_futures", "yfinance") == "ES=F"
        assert normalize_symbol("ES=F", "index_futures") == "ES_F"
        assert normalize_symbol("EUR/USD", "forex") == "EUR_USD"
        assert normalize_symbol("BRK.B", "us_stocks") == "BRK-B"
        for sym, yahoo in FUTURES.items():
            assert normalize_symbol(yahoo, "index_futures") == sym


# ── 4. Validator + schemas ────────────────────────────────────────────────────

def _fut(symbol="ES_F", raw="ES=F", yf="ES=F"):
    return {"index_futures": {"g": [{"symbol": symbol, "raw_symbol": raw, "yfinance_symbol": yf}]}}


class TestValidatorRules:

    def test_valid_future_passes_layer1(self):
        errs: list[str] = []
        assert vi._validate_layer1(_fut(), errs) == ["ES_F"]
        assert errs == []

    def test_bare_root_symbol_is_rejected(self):
        errs: list[str] = []
        vi._validate_layer1(_fut(symbol="ES"), errs)
        assert any("symbol must be raw_symbol with '=' replaced by '_'" in e for e in errs), errs

    def test_equals_sign_in_symbol_is_unsafe(self):
        errs: list[str] = []
        vi._validate_layer1(_fut(symbol="ES=F"), errs)
        assert any("unsafe character" in e for e in errs), errs

    def test_non_futures_yahoo_ticker_is_rejected(self):
        errs: list[str] = []
        vi._validate_layer1(_fut(raw="ES=F", yf="^GSPC"), errs)
        assert any("yfinance_symbol should end with =F" in e for e in errs), errs
        assert any("must equal yfinance_symbol" in e for e in errs), errs

    def test_missing_required_field_is_rejected(self):
        errs: list[str] = []
        vi._validate_layer1({"index_futures": {"g": [{"symbol": "ES_F", "yfinance_symbol": "ES=F"}]}}, errs)
        assert any("missing required field 'raw_symbol'" in e for e in errs), errs

    def test_cross_market_collision_is_a_config_time_error(self, capsys):
        data = {
            "us_stocks": {"Utilities": [{"symbol": "ES"}]},
            "commodity": {"x": [{"symbol": "ES", "yfinance_symbol": "ES=F",
                                 "commodity_role": "trading", "commodity_subcategory": "energy"}]},
            "context": {},
        }
        assert vi.validate_data(data) is False
        assert "[collision]" in capsys.readouterr().out

    def test_index_futures_is_a_layer1_market_and_legacy_index_is_not(self):
        import inspect
        line = inspect.getsource(vi._validate_layer1).split("layer1_markets = ", 1)[1].split("\n", 1)[0]
        assert '"index_futures"' in line and '"index"' not in line


class TestSchemas:

    @pytest.mark.parametrize("name", ["identity.schema.yaml", "taxonomy.schema.yaml"])
    def test_index_futures_is_required(self, name):
        schema = yaml.safe_load(Path("config/schemas/instruments", name).read_text())
        base = {"version": "1", "last_updated": "x", "us_stocks": {}, "idx_stocks": {},
                "commodity": {}, "forex": {}, "context": {}}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(base, schema)
        jsonschema.validate({**base, "index_futures": {}}, schema)

    def test_index_futures_items_are_type_checked(self):
        schema = yaml.safe_load(Path("config/schemas/instruments/identity.schema.yaml").read_text())
        doc = {"version": "1", "last_updated": "x", "us_stocks": {}, "idx_stocks": {}, "commodity": {},
               "forex": {}, "context": {}, "index_futures": {"g": [{"symbol": "ES_F", "yfinance_symbol": 5}]}}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(doc, schema)


# ── 5. Layer 1 consumers with their own market list ───────────────────────────

class TestMarketLists:

    def test_layer1_markets_includes_index_futures(self):
        assert layer1_markets() == ["commodity", "forex", "idx", "index_futures", "us_stocks"]

    @pytest.mark.parametrize("mod", [fg, fh], ids=["features_technical", "features_hmm"])
    def test_feature_builder_markets_mirror_layer1(self, mod):
        assert set(mod.MARKETS) == set(layer1_markets())

    @pytest.mark.parametrize("mod", [fg, fh], ids=["features_technical", "features_hmm"])
    def test_existing_market_codes_never_shift(self, mod):
        """market_code = enumerate(MARKETS) is a numeric model feature: new markets are APPENDED."""
        assert mod.MARKETS[:4] == ("commodity", "forex", "idx", "us_stocks")
        assert mod.MARKETS.index("us_stocks") == 3 and mod.MARKETS.index("index_futures") == 4

    def test_aggregator_treats_futures_as_near_24h(self):
        assert "index_futures" in NEAR_24H_MARKETS

    def test_processor_localises_naive_futures_stamps_to_new_york(self):
        df = pl.DataFrame({"timestamp": [datetime(2026, 10, 15, 0, 0)], "close": [1.0]})
        out = OHLCVProcessor._normalize_timestamps(df, "index_futures", None)
        assert out["timestamp"][0].hour == 4            # NY midnight in October = 04:00 UTC (EDT)
        out_fx = OHLCVProcessor._normalize_timestamps(df, "forex", None)
        assert out_fx["timestamp"][0].hour == 0         # the unchanged forex convention (UTC)


# ── 6. The <= 200 active-symbol ceiling ───────────────────────────────────────

RUN_DATE = date(2025, 2, 28)
BASE = date(2025, 1, 2)


def _write_silver(tmp_path: Path, specs: list[tuple[str, float, int]]) -> str:
    rows = [
        {"symbol": sym, "timestamp": BASE + timedelta(days=i), "close": close + i * 0.01,
         "volume": vol, "is_clean": True}
        for sym, close, vol in specs for i in range(45)
    ]
    out = tmp_path / "silver_1d" / "data.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).with_columns(pl.col("timestamp").cast(pl.Date)).write_parquet(out)
    return str(out.parent / "*.parquet")


def _real_registry_specs() -> list[tuple[str, float, int]]:
    """Every registered Layer 1 symbol with a clean, liquid 45-day history (everything passes screening)."""
    specs = []
    for k, inst in enumerate(get_loader().all_symbols()):
        if inst.market == "idx":
            specs.append((inst.symbol, 8_500.0, 10_000_000 + 1_000 * k))      # ~IDR 85B+/day  > 50B
        elif inst.market == "us_stocks":
            specs.append((inst.symbol, 100.0, 1_000_000 + 1_000 * k))         # ~USD 100M+/day > 10M
        else:
            specs.append((inst.symbol, 100.0, 1_000))                          # always-in: no threshold
    return specs


def _resolve(tmp_path, monkeypatch, silver_path, loader=None) -> pl.DataFrame:
    monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
    resolver = act.ActiveSymbolsResolver()
    if loader is None:
        resolver.resolve(silver_path, RUN_DATE)
    else:
        with patch("src.silver.active_symbols.get_loader", return_value=loader):
            resolver.resolve(silver_path, RUN_DATE)
    return pl.read_parquet(tmp_path / "silver" / "active_symbols" / f"active_ohlcv_{RUN_DATE}.parquet")


def _mock_loader(market_map: dict[str, str]) -> MagicMock:
    m = MagicMock()
    m.market_map.return_value = market_map
    m.symbol_list.return_value = list(market_map)
    return m


class TestActiveCeiling:

    def test_always_in_markets_and_thresholds(self):
        assert act._ALWAYS_IN_MARKETS == ("forex", "commodity", "index_futures")
        for mkt in act._ALWAYS_IN_MARKETS:
            assert act.THRESHOLDS[mkt]["dollar_volume_20d"] == 0
        assert "index" not in act.THRESHOLDS and "index" not in act._ALWAYS_IN_MARKETS

    def test_sql_takes_always_in_markets_as_a_parameter_not_a_literal(self):
        always_in_cte = act._RESOLVE_QUERY.split("always_in AS (", 1)[1].split("screened AS (", 1)[0]
        assert "$always_in_markets" in always_in_cte
        for literal in ("'forex'", "'commodity'", "'index'", "'index_futures'"):
            assert literal not in always_in_cte

    def test_registered_always_in_fit_under_the_ceiling(self):
        n = sum(1 for m in get_loader().market_map().values() if m in act._ALWAYS_IN_MARKETS)
        assert n == 25                                    # 18 forex + 3 commodity + 4 index_futures
        assert act._SCREENED_LIMIT == 175
        assert act._SCREENED_LIMIT + n <= act._MAX_ACTIVE_SYMBOLS == 200
        assert act._effective_screened_limit(n) == 175

    @pytest.mark.parametrize("n,expected", [(0, 175), (25, 175), (26, 174), (60, 140), (199, 1)])
    def test_effective_screened_limit(self, n, expected):
        assert act._effective_screened_limit(n) == expected
        assert act._effective_screened_limit(n) + n <= act._MAX_ACTIVE_SYMBOLS

    def test_no_room_for_screened_symbols_is_a_registry_error(self):
        with pytest.raises(ValueError, match="no room"):
            act._effective_screened_limit(200)

    def test_real_registry_all_liquid_resolves_to_exactly_200(self, tmp_path, monkeypatch):
        """Real registry, every symbol clean and liquid: 175 screened + 25 always-in == 200, never 201."""
        out = _resolve(tmp_path, monkeypatch, _write_silver(tmp_path, _real_registry_specs()))
        assert out.height == 200
        by = {r: out.filter(pl.col("eligibility_reason") == r) for r in ("always_in", "liquidity_screened")}
        assert by["always_in"].height == 25 and by["liquidity_screened"].height == 175
        assert set(by["always_in"]["market"].to_list()) == set(act._ALWAYS_IN_MARKETS)
        assert set(FUTURES) <= set(out["symbol"].to_list())
        assert "PARA" not in out["symbol"].to_list()

    def test_futures_are_always_in_even_with_no_liquidity(self, tmp_path, monkeypatch):
        specs = _real_registry_specs()
        specs = [(s, c, 0 if s in FUTURES else v) for s, c, v in specs]       # zero volume futures
        out = _resolve(tmp_path, monkeypatch, _write_silver(tmp_path, specs))
        assert set(FUTURES) <= set(out["symbol"].to_list())
        assert out.height == 200

    def test_a_fifth_future_shrinks_the_screened_pool_instead_of_breaching_200(self, tmp_path, monkeypatch):
        mk = {f"FX{i:02d}": "forex" for i in range(18)}
        mk |= {f"CM{i}": "commodity" for i in range(3)}
        mk |= {"ES_F": "index_futures", "NKD_F": "index_futures", "NQ_F": "index_futures",
               "YM_F": "index_futures", "RTY_F": "index_futures"}                     # the 26th always-in
        mk |= {f"US{i:03d}": "us_stocks" for i in range(300)}
        specs = [(s, 100.0, 1_000 if m != "us_stocks" else 1_000_000 + 1_000 * k)
                 for k, (s, m) in enumerate(mk.items())]
        out = _resolve(tmp_path, monkeypatch, _write_silver(tmp_path, specs), _mock_loader(mk))
        assert out.height == 200
        always = out.filter(pl.col("eligibility_reason") == "always_in")
        assert always.height == 26                                    # the always-in set is never the one cut
        assert out.filter(pl.col("eligibility_reason") == "liquidity_screened").height == 174

    def test_over_ceiling_set_is_refused_and_never_persisted(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        silver = _write_silver(tmp_path, _real_registry_specs())
        monkeypatch.setattr(act, "_effective_screened_limit", lambda n: 500)       # simulate a broken limit
        with pytest.raises(RuntimeError, match="above the 200-symbol ceiling"):
            act.ActiveSymbolsResolver().resolve(silver, RUN_DATE)
        assert list((tmp_path / "silver" / "active_symbols").glob("active*")) == []

    def test_cold_start_fallback_is_unchanged_and_not_capped(self, tmp_path, monkeypatch):
        """AS-2: Silver not ready -> the full registry (a one-off pre-backfill state, documented as exempt)."""
        monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
        syms = act.ActiveSymbolsResolver().resolve(str(tmp_path / "nothing" / "*.parquet"), RUN_DATE)
        assert len(syms) == get_loader().count() == 596
