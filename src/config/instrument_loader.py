"""
instrument_loader.py — IDD §1 (Implementation Detail Document v1.0)
                        + Architecture Extension v1.0 §8 (GMI Wave 1)
Single gateway untuk akses universe instrumen seluruh pipeline.

Semua modul (Bronze, Silver, Gold, Scheduler) mengakses metadata
instrumen HANYA melalui get_loader() — tidak pernah langsung baca YAML.

# ADD GMI-IL-001 — Dual-Layer Universe (Architecture Extension v1.0 §2-§3, §8):
Mulai instruments.yaml v1.4, universe terbagi dua:
  Layer 1 (trading candidates): us_stocks, idx_stocks, commodity, forex, index
           — diakses TIDAK BERUBAH via all_symbols()/get()/by_market()/count().
           SPX, VIX, DXY DIHAPUS dari Layer 1 (ADR-003 reklasifikasi) —
           640 instrumen (was 643).
  Layer 2 (context anchors, always-on): instruments.yaml `context` section —
           59 OHLCV instruments (56 active + 3 deferred Wave 2: TIN/CPO/RUBBER)
           diakses via API BARU: all_context(), by_context_category(),
           by_context_group(), forecast_context(), correlation_context(),
           deferred_count(), get_context(), subcategory_meta().
  EXPECTED_TOTAL = 640 (Layer 1) + 59 (Layer 2 OHLCV) = 699.
  # UPD ADR-013/014/024 (GMI Decision Documents v1/v2, 2026-07-11): +7 Layer 2
  # instruments (6 context_dollar_basket currencies + MYR context_fx_normalization),
  # all context_available=true from day one — 52->59 total, 49->56 active,
  # 20->22 subcategories, EXPECTED_TOTAL 692->699. See instruments.yaml v1.5.

Layer 1 API (UNCHANGED — backward compatible 100%):
    from src.config.instrument_loader import get_loader
    loader = get_loader()
    symbols = loader.all_symbols()           # list[Instrument], Layer 1 only
    inst    = loader.get("AAPL")             # Instrument
    idx     = loader.by_market("idx")        # list[Instrument]

Layer 2 API (NEW — Architecture Extension v1.0 §8.1):
    ctx       = loader.all_context()                          # 56 active (excl. deferred)
    etfs      = loader.by_context_category("context_etf_sector")
    all_etf   = loader.by_context_group("etf")                # 25 instruments
    fc_inputs = loader.forecast_context()                     # include_in_forecast=True
    cm_inputs = loader.correlation_context()                  # all context_available=True
    n_pending = loader.deferred_count()                       # 3 (TIN, CPO, RUBBER)
    vix       = loader.get_context("VIX")
    meta      = loader.subcategory_meta("context_rates_dm_cb")  # _meta.contributes_to etc.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Optional

import yaml

from src.config.yaml_split_merge import merge_split_trees


# ── Instrument Dataclass ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class Instrument:
    """
    Representasi satu instrumen — immutable, hashable.
    Single source of truth per instrumen di seluruh pipeline.

    Fields baris pertama (symbol..is_active): Layer 1 asli, TIDAK BERUBAH.
    Fields baris kedua (layer..meta): GMI Wave 1 — Architecture Extension
    v1.0 §8.2. Default values membuat Layer 1 instrument construction tetap
    valid tanpa perubahan di _build_us/_build_idx/_build_commodity/_build_forex.
    """

    symbol:          str            # Normalized: no dots/slashes — safe for Hive path & DuckDB
    raw_symbol:      str            # Original dari sumber (e.g. 'BRK.B', 'EUR/USD')
    market:          str            # 'us_stocks' | 'idx' | 'forex' | 'commodity' | 'index' | 'context'
    sector:          Optional[str]  # Hanya us_stocks: 'Technology', 'Health Care', dst.
    yfinance_symbol: str            # Format siap pakai untuk yfinance API call ('' jika belum ada — deferred)
    polygon_symbol:  str            # Format siap pakai untuk Polygon.io API call
    tvfeed_symbol:   Optional[str]  # Format tvdatafeed — hanya idx & index
    eia_series:      Optional[str]  # EIA series ID — hanya commodity CL
    timezone:        str            # 'America/New_York' | 'Asia/Jakarta' | 'UTC' | dst.
    is_active:       bool = True    # False = excluded dari active processing

    # ── ADD GMI-IL-001 — Architecture Extension v1.0 §8.2 ───────────────────────
    layer:                        int            = 1     # 1=trading candidate, 2=context anchor
    context_category:             Optional[str]  = None  # e.g. 'context_etf_sector' (Layer 2 only)
    context_group:                Optional[str]  = None  # e.g. 'etf', 'commodity', 'equity', 'dollar'
    context_available:            bool           = True  # False = deferred (TIN, CPO, RUBBER — Wave 2)
    include_in_forecast:          bool           = True  # False = excluded from ForecastModule VAR input
    proxy_for:                    Optional[str]  = None  # Benchmark being proxied (e.g. 'IRON_ORE_SGX_FE62')
    proxy_instrument:              Optional[str]  = None  # Proxy symbol used (e.g. 'VALE', 'WHC.AX')
    reclassified_from:             Optional[str]  = None  # Audit trail (e.g. 'layer_1_us_stocks_index')
    deferred_reason:               Optional[str]  = None  # Required if context_available=False
    planned_wave:                  Optional[int]  = None  # Wave number for deferred instruments
    reliability_flag:              bool           = False # True = data quality caveat (e.g. SSEC)
    exclude_from_lead_lag_leader:  bool           = False # True = must not be a LeadLagModule leader
    # ADD Decision B Step 1 (GMI_Decision_Document_v3.docx, Architecture v2.1
    # Addendum §7.1/§8.1 — specified in that document down to the code, but
    # never actually implemented in this dataclass until now, confirmed via
    # empirical grep before this change). Populated on ALL 14 commodity
    # instruments (Layer 1 trading + Layer 2 context) — None on every other
    # market. commodity_subcategory drives REGIME_SECTOR_WEIGHTS routing via
    # the 5 disaggregated commodity_* keys (see sector_rotation.py); it is
    # deliberately a coarser, separate taxonomy from context_category/
    # context_group (the 22-subcategory CrossAssetEngine taxonomy) — e.g.
    # COAL_NEWC's context_group stays 'coal' but commodity_subcategory is
    # 'energy', per Addendum §8.2's explicit mapping.
    commodity_role:                Optional[str]  = None  # 'trading' | 'context'
    commodity_subcategory:         Optional[str]  = None  # 'energy'|'precious_metals'|'base_metals'|'agricultural'|'bulks'
    meta:                          dict           = field(default_factory=dict)  # catch-all extras

    # ── Convenience Properties ────────────────────────────────────────────────

    @property
    def is_us_stock(self) -> bool:
        return self.market == "us_stocks"

    @property
    def is_idx(self) -> bool:
        return self.market == "idx"

    @property
    def is_forex(self) -> bool:
        return self.market == "forex"

    @property
    def is_commodity(self) -> bool:
        return self.market == "commodity"

    @property
    def is_index(self) -> bool:
        return self.market == "index"

    @property
    def is_layer1(self) -> bool:
        """ADD GMI-IL-001: True jika instrumen adalah trading candidate (Layer 1)."""
        return self.layer == 1

    @property
    def is_layer2(self) -> bool:
        """ADD GMI-IL-001: True jika instrumen adalah context anchor (Layer 2)."""
        return self.layer == 2

    @property
    def is_deferred(self) -> bool:
        """ADD GMI-IL-001: True jika belum diingest (context_available=False, Wave 2)."""
        return not self.context_available

    @property
    def hive_key(self) -> str:
        """Normalized symbol — safe untuk Hive partition key."""
        return self.symbol

    def __repr__(self) -> str:
        if self.is_layer2:
            return (
                f"Instrument({self.symbol!r}, layer=2,"
                f" category={self.context_category!r}, yf={self.yfinance_symbol!r})"
            )
        return (
            f"Instrument({self.symbol!r}, market={self.market!r},"
            f" yf={self.yfinance_symbol!r})"
        )


# ── InstrumentLoader Class ────────────────────────────────────────────────────

class InstrumentLoader:
    """
    Single entry point untuk akses universe instrumen.
    Thread-safe via lru_cache pada fungsi get_loader().
    Load SATU KALI per process — YAML di-parse sekali, di-cache selamanya.

    Anti-pattern yang DILARANG:
        - Membuat InstrumentLoader() langsung di setiap modul
        - Membaca instruments.yaml langsung (bypass loader)
        - Menggunakan symbol string tanpa lookup ke Instrument

    GMI Wave 1: Layer 1 indexes (_by_symbol, _by_market) HANYA berisi Layer 1
    instruments — Layer 2 context anchors disimpan terpisah di _context_instruments
    dan TIDAK PERNAH bercampur dengan all_symbols()/by_market()/count(), karena
    keduanya merepresentasikan populasi yang berbeda secara arsitektural
    (trading universe vs always-on macro anchors — GD §0.2).
    """

    # ADD GMI Decision Document v5 §2.1 (Decision B Step 2, 2026-07-22):
    # single instruments.yaml (v1.5, 1629 lines, "empat concern satu file")
    # split by concern into two files, joined positionally at load time —
    # lihat src/config/yaml_split_merge.py untuk kontrak join lengkap.
    # YAML_PATH lama DIHAPUS (bukan dipertahankan sebagai alias) — tidak ada
    # caller manapun (src/ maupun tests/) yang pernah pass yaml_path= custom
    # ke __init__ (dikonfirmasi via grep sebelum perubahan ini), jadi tidak
    # ada blast radius dari penghapusan constant lama.
    IDENTITY_YAML_PATH: Path = Path("config/instruments_identity.yaml")
    TAXONOMY_YAML_PATH: Path = Path("config/instruments_taxonomy.yaml")

    # EIA series mapping untuk commodity (Layer 1)
    EIA_SERIES_MAP: dict[str, str | None] = {
        "CL": "PET.RWTC.W",
        "AU": None,
        "AG": None,
    }

    # yfinance index map (juga dipakai oleh symbol_utils.py)
    YFINANCE_INDEX_MAP: dict[str, str] = {
        "SPX": "^GSPC",
        "NDX": "^NDX",
        "DJI": "^DJI",
        "VIX": "^VIX",
        "RUT": "^RUT",
        "DXY": "DX-Y.NYB",
    }

    # GMI Wave 1: groups under `context` that hold nested named subcategories
    # (vs. `dollar`, which is itself a single subcategory with no further nesting)
    _CONTEXT_GROUPED_KEYS: tuple[str, ...] = ("equity", "commodity", "etf")
    # ADD ADR-014/ADR-024 (GMI Decision Documents v1/v2): dollar_basket
    # (Broad Dollar Index basket-completion currencies) and fx_normalization
    # (MYR — single-purpose CPO currency conversion anchor) are single-
    # subcategory groups, structurally identical to `dollar` — no further
    # nesting, contributes_to: [] in both (zero domain-score weight).
    _CONTEXT_DIRECT_KEYS: tuple[str, ...] = ("dollar", "dollar_basket", "fx_normalization")
    # `rates` is grouped like equity/commodity/etf, but its subcategories carry
    # NO "instruments" key (FRED/BIS macro series only) — handled separately
    # for _meta extraction, never contributes Instrument objects.
    _CONTEXT_META_ONLY_GROUP: str = "rates"

    def __init__(
        self,
        identity_path: Path | None = None,
        taxonomy_path: Path | None = None,
    ) -> None:
        self._identity_path = identity_path or self.IDENTITY_YAML_PATH
        self._taxonomy_path = taxonomy_path or self.TAXONOMY_YAML_PATH
        identity = yaml.safe_load(self._identity_path.read_text())
        taxonomy = yaml.safe_load(self._taxonomy_path.read_text())
        # merge_split_trees() raises ValueError (not a silent best-effort)
        # on any structural misalignment between the two files — see
        # src/config/yaml_split_merge.py for the exact join contract.
        raw = merge_split_trees(identity, taxonomy)

        # ── Layer 1 — unchanged semantics ────────────────────────────────────
        self._instruments: list[Instrument] = self._load_layer1(raw)
        self._by_symbol: dict[str, list[Instrument]] = {}
        self._by_market: dict[str, list[Instrument]] = {}
        for inst in self._instruments:
            # Multiple markets can share same symbol string (e.g. CL),
            # so store as list keyed by (symbol, market)
            self._by_symbol.setdefault(inst.symbol, []).append(inst)
            self._by_market.setdefault(inst.market, []).append(inst)

        # ── Layer 2 — GMI Wave 1 (Architecture Extension v1.0 §8) ────────────
        self._context_instruments: list[Instrument] = self._load_layer2(raw)
        self._context_by_symbol: dict[str, Instrument] = {
            i.symbol: i for i in self._context_instruments
        }
        self._subcategory_meta_map: dict[str, dict] = self._load_subcategory_meta(raw)

    # ════════════════════════════════════════════════════════════════════════
    # ── Layer 1 Public API — UNCHANGED from pre-GMI behaviour ────────────────
    # ════════════════════════════════════════════════════════════════════════

    def all_symbols(self) -> list[Instrument]:
        """Return semua instrumen aktif Layer 1. Gunakan untuk Bronze ingestion loop."""
        return [i for i in self._instruments if i.is_active]

    def get(self, symbol: str, market: str | None = None) -> Instrument:
        """
        Lookup by normalized symbol (Layer 1 only — gunakan get_context() untuk Layer 2).
        Jika symbol ada di beberapa market (e.g. CL), wajib specify market.
        Raise KeyError jika tidak ditemukan.
        """
        candidates = self._by_symbol.get(symbol, [])
        if not candidates:
            raise KeyError(f"Symbol tidak ditemukan: {symbol!r}")
        if market is not None:
            filtered = [i for i in candidates if i.market == market]
            if not filtered:
                raise KeyError(
                    f"Symbol {symbol!r} tidak ditemukan di market={market!r}"
                )
            return filtered[0]
        if len(candidates) > 1:
            markets = [i.market for i in candidates]
            raise KeyError(
                f"Symbol {symbol!r} ada di beberapa market {markets}."
                " Specify market= parameter."
            )
        return candidates[0]

    def by_market(self, market: str) -> list[Instrument]:
        """
        Filter by market (Layer 1 only).
        Market values: 'us_stocks' | 'idx' | 'forex' | 'commodity' | 'index'
        """
        return [
            i for i in self._by_market.get(market, []) if i.is_active
        ]

    def by_sector(self, sector: str) -> list[Instrument]:
        """Filter us_stocks by sector. Hanya relevan untuk market='us_stocks'."""
        return [
            i for i in self.by_market("us_stocks") if i.sector == sector
        ]

    def count(self) -> int:
        """Total instrumen aktif Layer 1. Expected: 640 (post GMI Wave 1 reklasifikasi)."""
        return len(self.all_symbols())

    def symbol_list(self, market: str | None = None) -> list[str]:
        """Convenience: return list of normalized symbol strings (Layer 1)."""
        src = self.by_market(market) if market else self.all_symbols()
        return [i.symbol for i in src]

    def sectors(self) -> list[str]:
        """Return daftar sektor unik dari us_stocks, sorted."""
        return sorted(
            {i.sector for i in self.by_market("us_stocks") if i.sector}
        )

    def market_map(self) -> dict[str, str]:
        """
        Return dict {normalized_symbol: market} untuk semua instrumen Layer 1 aktif.
        Digunakan oleh ActiveSymbolsResolver untuk join ke Silver OHLCV.
        Note: jika symbol conflict cross-market (e.g. CL), market terakhir menang.
        """
        return {i.symbol: i.market for i in self.all_symbols()}

    # ════════════════════════════════════════════════════════════════════════
    # ── Layer 2 Public API — ADD GMI-IL-001 (Architecture Extension v1.0 §8.1) ─
    # ════════════════════════════════════════════════════════════════════════

    def all_context(self, include_deferred: bool = False) -> list[Instrument]:
        """
        Return semua Layer 2 OHLCV instruments.
        Default exclude context_available=False (TIN, CPO, RUBBER — Wave 2 deferred).
        Set include_deferred=True untuk audit / health-reporter visibility.
        """
        if include_deferred:
            return list(self._context_instruments)
        return [i for i in self._context_instruments if i.context_available]

    def get_context(self, symbol: str) -> Instrument:
        """Lookup Layer 2 instrument by symbol. Raise KeyError jika tidak ditemukan."""
        if symbol not in self._context_by_symbol:
            raise KeyError(f"Context symbol tidak ditemukan: {symbol!r}")
        return self._context_by_symbol[symbol]

    def by_context_category(self, category: str) -> list[Instrument]:
        """
        Filter by subcategory identifier.
        Contoh: by_context_category('context_etf_sector') -> [XLK, XLF, ...]
        Termasuk deferred — caller filter context_available jika perlu.
        """
        return [
            i for i in self._context_instruments
            if i.context_category == category
        ]

    def by_context_group(self, group: str) -> list[Instrument]:
        """
        Filter by top-level group.
        Contoh: by_context_group('etf') -> semua 25 ETF instruments.
        Groups: 'dollar', 'dollar_basket', 'fx_normalization', 'equity',
        'commodity', 'etf'.
        """
        return [
            i for i in self._context_instruments
            if i.context_group == group
        ]

    def forecast_context(self) -> list[Instrument]:
        """
        Return Layer 2 instruments dengan include_in_forecast=True DAN
        context_available=True. Digunakan ForecastModule sebagai input VAR
        (Architecture Extension v1.0 ADR-002 — sector ETF exclusion).
        """
        return [
            i for i in self._context_instruments
            if i.include_in_forecast and i.context_available
        ]

    def correlation_context(self) -> list[Instrument]:
        """
        Return semua Layer 2 instruments yang context_available=True
        (tanpa filter include_in_forecast). Digunakan CorrelationModule
        dan LeadLagModule — keduanya beroperasi pada variable asli, bukan
        PCA-transformed (Architecture v2.0 §8.2 Design Constraint).
        """
        return [i for i in self._context_instruments if i.context_available]

    def deferred_count(self) -> int:
        """
        Return jumlah deferred instruments (context_available=False).
        Digunakan health reporter daily summary (ADR-007).
        """
        return sum(1 for i in self._context_instruments if not i.context_available)

    def count_context(self, include_deferred: bool = False) -> int:
        """Total Layer 2 OHLCV instruments. Expected: 56 active / 59 with deferred."""
        return len(self.all_context(include_deferred=include_deferred))

    def count_total(self) -> int:
        """Layer 1 + Layer 2 (active) combined. Expected: 640 + 56 = 696 OHLCV-bearing.
        NOTE: EXPECTED_TOTAL=699 in validate_instruments.py counts ALL Layer 2
        slots including deferred (640 + 59 = 699) since deferred instruments
        are declared universe members per ADR-007, just not yet ingested.
        """
        return self.count() + self.count_context(include_deferred=False)

    def subcategory_meta(self, category: str) -> dict:
        """
        Return _meta block untuk subcategory tertentu — termasuk subcategories
        TANPA OHLCV instruments (context_rates_fed, context_rates_curve,
        context_rates_spread, context_rates_dm_cb, context_rates_em_cb).
        CrossAssetEngine menggunakan ini untuk membaca contributes_to weights
        (domain score routing — config-over-code, Architecture Extension v1.0 §4.3).
        Return {} jika subcategory_id tidak ditemukan.
        """
        return self._subcategory_meta_map.get(category, {})

    def all_subcategory_ids(self) -> list[str]:
        """Return semua 22 subcategory_id yang terdaftar (untuk validator coverage check)."""
        return sorted(self._subcategory_meta_map.keys())

    # ════════════════════════════════════════════════════════════════════════
    # ── Internal Loader — Layer 1 (UNCHANGED) ────────────────────────────────
    # ════════════════════════════════════════════════════════════════════════

    def _load_layer1(self, data: dict) -> list[Instrument]:
        instruments: list[Instrument] = []

        # US Stocks (semua sector kecuali 'Index')
        for sector, items in data.get("us_stocks", {}).items():
            for item in items:
                instruments.append(self._build_us(item, sector))

        # Index (legacy section — empty post GMI Wave 1, SPX/VIX moved to context)
        for item in data.get("index", []) or []:
            instruments.append(self._build_index(item))

        # IDX Stocks
        for group_items in data.get("idx_stocks", {}).values():
            for item in group_items:
                instruments.append(self._build_idx(item))

        # Commodity
        for group_items in data.get("commodity", {}).values():
            for item in group_items:
                instruments.append(self._build_commodity(item))

        # Forex (DXY removed post GMI Wave 1 — moved to context.dollar)
        for group_items in data.get("forex", {}).values():
            for item in group_items:
                instruments.append(self._build_forex(item))

        return instruments

    @staticmethod
    def _build_us(item: dict, sector: str) -> Instrument:
        sym = item["symbol"]
        raw = item.get("raw_symbol", sym)
        return Instrument(
            symbol=sym,
            raw_symbol=raw,
            market="us_stocks",
            sector=sector,
            yfinance_symbol=sym,
            polygon_symbol=raw.replace("-", "."),   # BRK-B -> BRK.B for Polygon
            tvfeed_symbol=None,
            eia_series=None,
            timezone="America/New_York",
        )

    @classmethod
    def _build_index(cls, item: dict) -> Instrument:
        sym = item["symbol"]
        yf  = item.get("yfinance_symbol") or cls.YFINANCE_INDEX_MAP.get(
            sym, f"^{sym}"
        )
        return Instrument(
            symbol=sym,
            raw_symbol=sym,
            market="index",
            sector=None,
            yfinance_symbol=yf,
            polygon_symbol=sym,
            tvfeed_symbol=item.get("tvfeed_symbol"),
            eia_series=None,
            timezone="America/New_York",
        )

    @staticmethod
    def _build_idx(item: dict) -> Instrument:
        sym = item["symbol"]
        return Instrument(
            symbol=sym,
            raw_symbol=sym,
            market="idx",
            sector=None,
            yfinance_symbol=item.get("yfinance_symbol", f"{sym}.JK"),
            polygon_symbol=sym,
            tvfeed_symbol=sym,       # IDX: tvdatafeed menggunakan raw ticker
            eia_series=None,
            timezone="Asia/Jakarta",
        )

    @classmethod
    def _build_commodity(cls, item: dict) -> Instrument:
        sym = item["symbol"]
        return Instrument(
            symbol=sym,
            raw_symbol=sym,
            market="commodity",
            sector=None,
            yfinance_symbol=item.get("yfinance_symbol", f"{sym}=F"),
            polygon_symbol=sym,
            tvfeed_symbol=item.get("tvfeed_symbol"),
            eia_series=cls.EIA_SERIES_MAP.get(sym),
            timezone="America/New_York",
            # ADD Decision B Step 1: commodity_role/commodity_subcategory —
            # Layer 1 commodity_trading instruments (AU/AG/CL).
            commodity_role=item.get("commodity_role", "trading"),
            commodity_subcategory=item.get("commodity_subcategory"),
        )

    @staticmethod
    def _build_forex(item: dict) -> Instrument:
        sym = item["symbol"]          # normalized: EUR_USD
        raw = item.get("raw_symbol", sym)   # EUR/USD
        yf  = item.get(
            "yfinance_symbol",
            "DX-Y.NYB" if raw == "DXY" else raw.replace("/", "") + "=X",
        )
        return Instrument(
            symbol=sym,
            raw_symbol=raw,
            market="forex",
            sector=None,
            yfinance_symbol=yf,
            polygon_symbol=f"C:{raw.replace('/', '')}",
            tvfeed_symbol=None,
            eia_series=None,
            timezone="UTC",
        )

    # ════════════════════════════════════════════════════════════════════════
    # ── Internal Loader — Layer 2 — ADD GMI-IL-001 ───────────────────────────
    # ════════════════════════════════════════════════════════════════════════

    def _load_layer2(self, data: dict) -> list[Instrument]:
        """
        Walk instruments.yaml `context` section. Two structural shapes:
          1. Direct subcategory (e.g. `context.dollar`) — has _meta +
             instruments directly, no further nesting.
          2. Grouped subcategories (e.g. `context.equity.dm`,
             `context.equity.em`) — group key holds N named subcategory
             blocks, each with its own _meta + instruments.
        `context.rates.*` is grouped but carries NO "instruments" key
        (FRED/BIS macro series only) — naturally skipped since the
        `.get("instruments", [])` default yields an empty list.
        """
        ctx = data.get("context", {})
        instruments: list[Instrument] = []

        for direct_key in self._CONTEXT_DIRECT_KEYS:
            block = ctx.get(direct_key, {})
            if not isinstance(block, dict):
                continue
            for item in block.get("instruments", []):
                instruments.append(self._build_context_instrument(item, group=direct_key))

        for group_key in self._CONTEXT_GROUPED_KEYS:
            group_block = ctx.get(group_key, {})
            if not isinstance(group_block, dict):
                continue
            for subcat_block in group_block.values():
                if not isinstance(subcat_block, dict):
                    continue
                for item in subcat_block.get("instruments", []):
                    instruments.append(
                        self._build_context_instrument(item, group=group_key)
                    )

        return instruments

    # Keys consumed explicitly into named Instrument fields — everything else
    # in the YAML item dict is preserved in Instrument.meta as a catch-all.
    _CONTEXT_CONSUMED_KEYS: frozenset = frozenset({
        "symbol", "yfinance_symbol", "tvfeed_symbol", "timezone", "layer",
        "context_category", "context_group", "context_available",
        "include_in_forecast", "proxy_for", "proxy_instrument",
        "reclassified_from", "deferred_reason", "planned_wave",
        "reliability_flag", "exclude_from_lead_lag_leader",
        # ADD Decision B Step 1 (GMI_Decision_Document_v3.docx):
        "commodity_role", "commodity_subcategory",
    })

    @classmethod
    def _build_context_instrument(cls, item: dict, group: str) -> Instrument:
        sym = item["symbol"]
        yf  = item.get("yfinance_symbol") or ""   # '' sentinel for deferred (no source yet)
        extra_meta = {
            k: v for k, v in item.items() if k not in cls._CONTEXT_CONSUMED_KEYS
        }
        return Instrument(
            symbol=sym,
            raw_symbol=sym,
            market="context",
            sector=None,
            yfinance_symbol=yf,
            polygon_symbol="",
            tvfeed_symbol=item.get("tvfeed_symbol"),
            eia_series=None,
            timezone=item.get("timezone", "UTC"),
            is_active=True,
            layer=item.get("layer", 2),
            context_category=item.get("context_category"),
            context_group=item.get("context_group", group),
            context_available=item.get("context_available", True),
            include_in_forecast=item.get("include_in_forecast", True),
            proxy_for=item.get("proxy_for"),
            proxy_instrument=item.get("proxy_instrument"),
            reclassified_from=item.get("reclassified_from"),
            deferred_reason=item.get("deferred_reason"),
            planned_wave=item.get("planned_wave"),
            reliability_flag=item.get("reliability_flag", False),
            exclude_from_lead_lag_leader=item.get("exclude_from_lead_lag_leader", False),
            # ADD Decision B Step 1: commodity_role/commodity_subcategory —
            # only present on context.commodity.* items; None on all other
            # Layer 2 groups (dollar/dollar_basket/fx_normalization/rates/
            # equity/etf), matching the dataclass default.
            commodity_role=item.get("commodity_role"),
            commodity_subcategory=item.get("commodity_subcategory"),
            meta=extra_meta,
        )

    def _load_subcategory_meta(self, data: dict) -> dict[str, dict]:
        """
        Walk ALL context subcategories (including rates.* which have no
        OHLCV instruments) and index their _meta block by subcategory_id.
        This is the single source CrossAssetEngine reads for domain-score
        routing weights (_meta.contributes_to) — configuration-over-code.
        """
        ctx = data.get("context", {})
        meta_map: dict[str, dict] = {}

        def _register(block: dict) -> None:
            if not isinstance(block, dict):
                return
            m = block.get("_meta", {})
            sid = m.get("subcategory_id")
            if sid:
                meta_map[sid] = m

        for direct_key in self._CONTEXT_DIRECT_KEYS:
            _register(ctx.get(direct_key, {}))

        all_grouped = list(self._CONTEXT_GROUPED_KEYS) + [self._CONTEXT_META_ONLY_GROUP]
        for group_key in all_grouped:
            group_block = ctx.get(group_key, {})
            if not isinstance(group_block, dict):
                continue
            for subcat_block in group_block.values():
                _register(subcat_block)

        return meta_map


# ── Singleton Cache ───────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_loader() -> InstrumentLoader:
    """
    Return singleton InstrumentLoader.
    Import dan panggil ini di semua modul — JANGAN buat InstrumentLoader() langsung.
    lru_cache memastikan YAML hanya di-parse sekali per process.
    """
    return InstrumentLoader()
