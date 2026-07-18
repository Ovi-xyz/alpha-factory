"""
market_ingester.py — Bronze Market OHLCV Ingester
Primary ingester untuk market OHLCV data: US Stocks, IDX, Forex, Commodity.

Source chain per asset class:
    US Stocks:  yfinance → Polygon.io
    IDX:        tvdatafeed → yfinance (.JK)
    Forex:      yfinance → ForexDayCache (24h) → AlphaVantage (DXY only)
    Commodity:  yfinance → EIA (CL fundamental only)
    Index:      yfinance

Menggunakan:
    IncFetchProtocol  — resolve start_date (G1)
    SchemaValidator   — validate schema sebelum write (GD §3.7)
    BronzeIngester    — write ke Hive partition (GD §3.6)
    ProgressCheckpoint — track per-symbol progress (G6)
"""

from __future__ import annotations

import time
from datetime import date
from pathlib import Path
from typing import Optional

import polars as pl
from loguru import logger

from src.bronze.base_ingester import BronzeIngester
from src.bronze.inc_fetch import FALLBACK_YEARS, IncFetchProtocol
from src.bronze.schema_validator import SchemaValidator
from src.config.instrument_loader import Instrument, get_loader
from src.utils.progress_checkpoint import ProgressCheckpoint
from src.utils.symbol_utils import to_api_symbol


# ── Default Timeframes per Run ────────────────────────────────────────────────
DEFAULT_TIMEFRAMES = ["1D", "1W", "1M"]
# v1.5: 4H dipindah ke Silver layer (GD §4.1 Enrichment, §17.7 Anti-Patterns).
# Bronze hanya fetch raw data dari source. 4H bukan raw source data — synthetic.
INTRADAY_TIMEFRAMES = ["5m", "15m", "1H"]


class MarketOHLCVIngester(BronzeIngester):
    """
    Bronze OHLCV ingester untuk semua asset classes.
    Satu instance, semua symbols.
    """

    BRONZE_OHLCV_PATH: Path = Path("data/bronze/market/ohlcv")

    def __init__(self, timeframes: list[str] = None) -> None:
        self.inc        = IncFetchProtocol()
        self.timeframes = timeframes or DEFAULT_TIMEFRAMES
        self._schema_validators: dict[str, SchemaValidator] = {}
        self._load_schema_validators()

    def _load_schema_validators(self) -> None:
        """Load schema validators dari config/schemas/."""
        schemas_dir = Path("config/schemas")
        validator_map = {
            "yfinance_ohlcv.yaml":  ["yfinance"],
            "polygon_ohlcv.yaml":   ["polygon"],
        }
        for schema_file, sources in validator_map.items():
            schema_path = schemas_dir / schema_file
            if schema_path.exists():
                validator = SchemaValidator(schema_path)
                for source in sources:
                    self._schema_validators[source] = validator

    def run(self, run_date: date) -> None:
        """
        Ingest OHLCV untuk semua 643 instruments × configured timeframes.
        Menggunakan ProgressCheckpoint untuk resume jika crash.
        """
        loader = get_loader()
        all_instruments = loader.all_symbols()
        total = len(all_instruments)

        logger.info(
            f"[MarketIngester] Starting run_date={run_date}"
            f" | {total} instruments | TFs={self.timeframes}"
        )

        for tf in self.timeframes:
            checkpoint = ProgressCheckpoint("bronze_ohlcv_daily", run_date)
            pending = checkpoint.pending_symbols(
                [inst.symbol for inst in all_instruments], timeframe=tf
            )
            logger.info(
                f"[MarketIngester] TF={tf} | {len(pending)}/{total} pending"
            )

            for inst in all_instruments:
                if inst.symbol not in pending:
                    continue
                try:
                    self._run_symbol(inst, tf, run_date)
                    checkpoint.mark_done(inst.symbol, timeframe=tf)
                except Exception as e:
                    checkpoint.mark_failed(inst.symbol, e, timeframe=tf)
                    logger.error(
                        f"[MarketIngester] FAILED {inst.symbol}/{tf}: {e}"
                    )
                finally:
                    # Throttle: ~2000 req/hr yfinance = ~1.8 req/sec
                    time.sleep(0.6)

            summary = checkpoint.summary()
            logger.info(
                f"[MarketIngester] TF={tf} done | {summary}"
            )

    def _run_symbol(
        self,
        inst: Instrument,
        tf: str,
        run_date: date,
    ) -> None:
        """Fetch + validate + write untuk satu symbol × timeframe.

        FIX B-F01: primary_src diambil dari _primary_source_for() — tidak lagi
        memakai 'source' variabel yang tidak terdefinisi.
        FIX B-F02: actual_source dibaca dari kolom _source yang diisi ChainedAdapter
        (GD §3.5) — bukan hardcode 'yfinance'.
        FIX B-F07: ForexDayCache.save() dipanggil setelah primary forex fetch sukses.
        v1.5: 4H DIHAPUS dari Bronze — Bronze hanya fetch raw data (GD §3.1, §17.7).
        """
        # v1.5: fetch_tf == tf untuk semua TF — Bronze tidak lagi aggregate 1H→4H
        # Bronze layer hanya menyimpan data as-is dari source (GD §3.1)
        fetch_tf    = tf
        # FIX B-F01: gunakan primary_src dari helper — source tidak pernah terdefinisi
        primary_src = self._primary_source_for(inst)
        api_symbol  = to_api_symbol(inst.raw_symbol, inst.market, primary_src)

        start_date = self.inc.resolve_start_date(
            bronze_path=self.BRONZE_OHLCV_PATH / inst.market,
            symbol=inst.symbol,
            source=primary_src,  # FIX B-F01: primary_src bukan source
            run_date=run_date,
            fallback_years=FALLBACK_YEARS.get(tf, 10),
        )

        df = self._fetch(api_symbol, inst, fetch_tf, start_date, run_date)
        if df is None or len(df) == 0:
            logger.debug(
                f"[MarketIngester] No data for {inst.symbol}/{tf} — skipping"
            )
            return

        # FIX B-F01 + B-F02: baca actual_source dari kolom _source
        # ChainedAdapter (GD §3.5) mengisi kolom ini dengan adapter.name yang sukses
        actual_source = (
            df["_source"][0] if "_source" in df.columns else primary_src
        )

        # FIX B-F07: ForexDayCache.save() setelah primary fetch sukses
        if inst.market == "forex" and df is not None and len(df) > 0:
            try:
                from src.bronze.forex_cache import ForexDayCache
                ForexDayCache().save(inst.symbol, df, run_date)
            except Exception as cache_err:
                logger.debug(
                    f"[MarketIngester] ForexDayCache save failed (non-critical): {cache_err}"
                )

        # Schema validation pada RAW df (as-is dari source)
        # v1.5: tidak ada agregasi setelah validasi — Bronze menyimpan raw data
        validator_key = actual_source if actual_source in self._schema_validators else primary_src
        if validator_key in self._schema_validators:
            validator = self._schema_validators[validator_key]
            ok, errors = validator.validate(df, inst.symbol)
            if not ok:
                validator.handle_mismatch(
                    df, errors, inst.symbol, on_mismatch="quarantine"
                )
                return

        # Add _tz_hint Bronze extension (G3 FIX)
        tz_hint = {"_tz_hint": inst.timezone}

        self.write(
            df=df,
            source=actual_source,  # FIX B-F01/B-F02: actual source dari ChainedAdapter
            asset_class=f"market/ohlcv/{inst.market}",
            symbol=inst.symbol,
            extra_metadata=tz_hint,
        )

    def _fetch(
        self,
        api_symbol: str,
        inst: Instrument,
        tf: str,
        start: date,
        end: date,
    ) -> Optional[pl.DataFrame]:
        """
        Fetch data using appropriate ChainedAdapter per market.
        GD §3.5: ChainedAdapter provides transparent fallback chain.
        """
        from src.bronze.source_adapter import ChainedAdapter
        from src.bronze.yfinance_adapter import (
            YFinanceAdapter, YFinanceForexAdapter,
            YFinanceJKAdapter, ForexDayCacheAdapter,
        )
        from src.bronze.polygon_adapter import PolygonAdapter
        from src.bronze.alphavantage_adapter import AlphaVantageForexAdapter
        from src.bronze.tvdatafeed_adapter import TvDatafeedAdapter

        # Build market-specific adapter chain (GD §3.3.2 Source Priority Matrix)
        if inst.market == "idx":
            chain = ChainedAdapter([TvDatafeedAdapter(), YFinanceJKAdapter()])
        elif inst.market == "forex":
            chain = ChainedAdapter([
                YFinanceForexAdapter(),
                ForexDayCacheAdapter(),
                AlphaVantageForexAdapter(),   # Last resort — 25/day budget
            ])
        elif inst.market == "us_stocks":
            chain = ChainedAdapter([YFinanceAdapter(), PolygonAdapter()])
        else:
            # index, commodity — yfinance primary, no fallback needed
            chain = ChainedAdapter([YFinanceAdapter()])

        return chain.fetch(api_symbol, tf, start, end)

    @staticmethod
    def _primary_source_for(inst: Instrument) -> str:
        """Return primary source string per asset class (GD §3.3.2).

        FIX B-F01: digunakan oleh _run_symbol() untuk menentukan source
        awal sebelum ChainedAdapter menentukan actual_source.
        """
        if inst.market == "idx":
            return "tvdatafeed"
        if inst.market == "forex":
            return "yfinance"
        return "yfinance"  # us_stocks, commodity, index, context

    # ════════════════════════════════════════════════════════════════════
    # ── Layer 2 Context OHLCV — ADD GMI-BRZ-001 ────────────────────────────
    # Architecture v2.0 §4, Architecture Extension v1.0 §2-3, §8.
    #
    # Gap closed: Bronze sebelumnya HANYA meng-iterate loader.all_symbols()
    # (Layer 1, 640 trading candidates) — 49 Layer 2 context anchors aktif
    # (VIX, DXY, 13 global equity indices, 25 ETF, 8 commodity context)
    # tidak pernah punya OHLCV data di Bronze sama sekali, membuat setiap
    # consumer Gold-layer Layer 2 (CrossAssetEngine, GlobalIndexRegimeModule,
    # gold_domain_scores) tidak punya raw price data untuk beroperasi.
    # Diverifikasi empiris (bukan asumsi) sebelum implementasi ini:
    #   - loader.all_context(include_deferred=False) == 49 instruments,
    #     group breakdown {etf:25, equity:15, commodity:8, dollar:1}
    #   - SEMUA 49 instrumen aktif punya yfinance_symbol non-empty
    #   - to_api_symbol(raw, 'context', 'yfinance') SALAH — tidak ada
    #     cabang untuk market='context', fallback ke YFINANCE_SUFFIX.get()
    #     mengembalikan raw symbol tanpa transformasi (mis. 'DXY' bukan
    #     'DX-Y.NYB', 'VIX' bukan '^VIX') — instruments.yaml v1.4 sudah
    #     menyimpan yfinance_symbol siap-pakai per instrumen; PAKAI LANGSUNG.
    #   - _fetch() TIDAK PERLU diubah — dispatch market di sana sudah punya
    #     cabang else (yfinance-only, no fallback chain) yang otomatis
    #     berlaku untuk market='context' (tidak match idx/forex/us_stocks).
    #   - resolve_start_date(), write(), SchemaValidator — semuanya generic
    #     terhadap 'market' sebagai string path segment; nol modifikasi.
    #
    # Desain checkpoint terpisah ("bronze_ohlcv_context_daily") dari Layer 1
    # ("bronze_ohlcv_daily") — kegagalan salah satu job TIDAK mempengaruhi
    # resume state job lainnya (GD §17.3.1: Bronze ingesters independent).
    #
    # Timeframe: default DEFAULT_TIMEFRAMES (1D/1W/1M) — sama seperti
    # perilaku AKTUAL job bronze_ohlcv_daily hari ini (job_registry.py
    # memanggil MarketOHLCVIngester() tanpa override timeframes= — 5m/15m/1H
    # tidak pernah di-fetch oleh job harian baik untuk Layer 1 maupun Layer 2
    # saat ini). Tidak ada consumer Layer 2 yang terdefinisi (Architecture
    # v2.0 §6 CrossAssetEngine, §6.5 GlobalIndexRegimeModule) yang butuh
    # intraday context data — 1D/1W/1M cukup untuk cycle ini.
    #
    # Deferred instruments (TIN, CPO, RUBBER — context_available=False)
    # otomatis exclude via all_context(include_deferred=False) — konsisten
    # dengan ADR-007, tidak pernah masuk loop ini.
    # ════════════════════════════════════════════════════════════════════

    def run_context(self, run_date: date) -> None:
        """
        Ingest OHLCV untuk 49 Layer 2 context anchors aktif × configured
        timeframes. Layer 2 selalu-on (Architecture v2.0 §4.2 "Filter: None")
        — TIDAK melalui ActiveSymbolsResolver/liquidity filter, langsung dari
        loader.all_context(). Struktur paralel dengan run() (Layer 1), tapi
        checkpoint namespace dan symbol population sepenuhnya independen.
        """
        loader = get_loader()
        context_instruments = loader.all_context(include_deferred=False)
        total = len(context_instruments)

        logger.info(
            f"[MarketIngester] Starting CONTEXT run_date={run_date}"
            f" | {total} Layer 2 instruments | TFs={self.timeframes}"
        )

        for tf in self.timeframes:
            checkpoint = ProgressCheckpoint("bronze_ohlcv_context_daily", run_date)
            pending = checkpoint.pending_symbols(
                [inst.symbol for inst in context_instruments], timeframe=tf
            )
            logger.info(
                f"[MarketIngester] CONTEXT TF={tf} | {len(pending)}/{total} pending"
            )

            for inst in context_instruments:
                if inst.symbol not in pending:
                    continue
                try:
                    self._run_context_symbol(inst, tf, run_date)
                    checkpoint.mark_done(inst.symbol, timeframe=tf)
                except Exception as e:
                    checkpoint.mark_failed(inst.symbol, e, timeframe=tf)
                    logger.error(
                        f"[MarketIngester] CONTEXT FAILED {inst.symbol}/{tf}: {e}"
                    )
                finally:
                    # Throttle sama dengan Layer 1 — shared SourceLimiters.yfinance
                    # budget antara Layer 1 dan Layer 2 (~2000 req/hr conservative)
                    time.sleep(0.6)

            summary = checkpoint.summary()
            logger.info(f"[MarketIngester] CONTEXT TF={tf} done | {summary}")

    def _run_context_symbol(
        self,
        inst: Instrument,
        tf: str,
        run_date: date,
    ) -> None:
        """Fetch + validate + write untuk satu Layer 2 symbol × timeframe.

        Berbeda dari _run_symbol() (Layer 1) dalam SATU hal krusial:
        api_symbol = inst.yfinance_symbol LANGSUNG, bukan
        to_api_symbol(inst.raw_symbol, inst.market, primary_src) — lihat
        blok komentar ADD GMI-BRZ-001 di atas untuk verifikasi empiris
        mengapa to_api_symbol() akan menghasilkan symbol yang salah untuk
        market='context'. Selebihnya (fetch chain, schema validation,
        write path) reuse 100% dari infrastruktur Layer 1 yang sama —
        _fetch() dispatch else-branch (yfinance-only) otomatis berlaku
        karena market='context' tidak match idx/forex/us_stocks.
        """
        fetch_tf    = tf
        primary_src = self._primary_source_for(inst)   # 'yfinance' untuk semua Layer 2
        api_symbol  = inst.yfinance_symbol              # LANGSUNG — bukan to_api_symbol()

        start_date = self.inc.resolve_start_date(
            bronze_path=self.BRONZE_OHLCV_PATH / inst.market,   # 'context' bucket
            symbol=inst.symbol,
            source=primary_src,
            run_date=run_date,
            fallback_years=FALLBACK_YEARS.get(tf, 10),
        )

        df = self._fetch(api_symbol, inst, fetch_tf, start_date, run_date)
        if df is None or len(df) == 0:
            logger.debug(
                f"[MarketIngester] No CONTEXT data for {inst.symbol}/{tf} — skipping"
            )
            return

        actual_source = (
            df["_source"][0] if "_source" in df.columns else primary_src
        )

        validator_key = actual_source if actual_source in self._schema_validators else primary_src
        if validator_key in self._schema_validators:
            validator = self._schema_validators[validator_key]
            ok, errors = validator.validate(df, inst.symbol)
            if not ok:
                validator.handle_mismatch(
                    df, errors, inst.symbol, on_mismatch="quarantine"
                )
                return

        # _tz_hint (G3 Bronze extension) — identik dengan pola _run_symbol().
        # TIDAK menambah kolom context_group/context_category: metadata itu
        # sudah tersedia via InstrumentLoader.get_context(symbol) — single
        # source of truth (module docstring instrument_loader.py). Duplikasi
        # ke setiap row Parquet adalah redundansi yang tidak dibutuhkan
        # consumer manapun saat ini.
        tz_hint = {"_tz_hint": inst.timezone}

        self.write(
            df=df,
            source=actual_source,
            asset_class=f"market/ohlcv/{inst.market}",   # -> market/ohlcv/context
            symbol=inst.symbol,
            extra_metadata=tz_hint,
        )
