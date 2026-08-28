"""
market_ingester.py — Bronze Market OHLCV Ingester
Primary ingester untuk market OHLCV data: US Stocks, IDX, Forex, Commodity.

Source chain per asset class:
    US Stocks:  yfinance → Polygon.io
    IDX:        yfinance (.JK) — SOLE source since ADR-029 (GMI_Decision_
                Document_v7.docx, 30 Jul 2026). tvdatafeed retired
                entirely: signin failing since >=29 Jul 2026 (nologin
                fallback mode; non-IDX exchange fetches time out even on
                a nominally "healthy" session — see
                alpha-factory_preflight_logs___29_July_2026.txt). yfinance
                .JK was already the tested ChainedAdapter fallback — this
                is priority reordering + dependency removal, not new
                integration risk. See KNOWN_RISKS.md RISK-1 (RESOLVED).
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

# FIX ADR-046 Path C (GMI_Decision_Document_v11.docx §2, decided by Ovi):
# 1H wired up for Layer 1 ONLY — not 5m/15m (those remain permanently
# unfetched, Path C is the deliberate middle ground between full intraday
# restoration and abandoning the 7-timeframe design). 1H both contributes
# a real MTF trend value directly and unblocks the existing 1H->4H Silver
# synthesis (ohlcv_aggregator.py), raising real contributors from 3 to 5.
# Scoped to Layer 1 via job_registry.py's _bronze_ohlcv() only — Layer 2
# context (_bronze_ohlcv_context()) keeps DEFAULT_TIMEFRAMES unchanged, per
# run_context()'s own docstring: no Layer 2 consumer needs intraday context
# data in this cycle. Requires ADR-045 (this same file) to have landed
# first — an unpartitioned Bronze bucket would starve 1H via the identical
# same-day idempotency collision ADR-045 exists to close for 1D/1W/1M.
LAYER1_TIMEFRAMES = DEFAULT_TIMEFRAMES + ["1H"]

# ADD (hardcode-avoidance pass, v1.11.2): named constant — sebelumnya literal
# `0.6` diduplikasi persis di dua lokasi (Layer 1 loop + Layer 2/context loop),
# masing-masing dengan komentar terpisah yang menjelaskan alasan yang sama
# ("~2000 req/hr yfinance = ~1.8 req/sec"). Satu named constant menghilangkan
# risiko kedua lokasi diam-diam divergen di masa depan (mis. salah satu diubah
# saat rate limit yfinance berubah, yang lain tidak).
YFINANCE_THROTTLE_SECONDS = 0.6  # ~2000 req/hr yfinance = ~1.8 req/sec — shared budget Layer 1 & Layer 2


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
                    # ADD (hardcode-avoidance v1.11.2): named constant, was literal 0.6
                    time.sleep(YFINANCE_THROTTLE_SECONDS)

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
        # FIX ADR-047 (GMI_Decision_Document_v11.docx §2): commodity market
        # routes through inst.yfinance_symbol directly, mirroring
        # _run_context_symbol()'s already-established Layer 2 pattern.
        # to_api_symbol()'s commodity branch has no override table and falls
        # through to a generic sym + "=F" suffix rule, producing invalid
        # AU=F/AG=F — instruments_identity.yaml already carries the correct
        # GC=F/SI=F on inst.yfinance_symbol via InstrumentLoader. CL=F was
        # already correct by coincidence and is unaffected by this change.
        # to_api_symbol() itself is left unchanged (call-site fix only) and
        # remains the resolution path for every other Layer 1 market.
        if inst.market == "commodity":
            api_symbol = inst.yfinance_symbol
        else:
            api_symbol = to_api_symbol(inst.raw_symbol, inst.market, primary_src)

        # FIX ADR-045 (GMI_Decision_Document_v11.docx §2): timeframe folded
        # into the Bronze scan/write path. Previously bronze_path was
        # symbol+market+source-scoped only, with no timeframe dimension —
        # IncFetchProtocol._scan_last_date()'s glob would see 1D's freshly-
        # written file (DEFAULT_TIMEFRAMES processes 1D first) and report a
        # near-today last_date for 1W/1M, returning a trivial ~7-day fetch
        # window instead of the correct multi-year cold-start backfill.
        start_date = self.inc.resolve_start_date(
            bronze_path=self.BRONZE_OHLCV_PATH / inst.market / f"timeframe={tf}",
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

        # FIX ADR-045: timeframe folded into asset_class — must match the
        # bronze_path passed to resolve_start_date() above exactly, or the
        # write-side idempotency check (BronzeIngester.write(), FIX GD-F08)
        # and the read-side scan (IncFetchProtocol._scan_last_date()) would
        # disagree on where a given (symbol, tf) pair's data lives.
        self.write(
            df=df,
            source=actual_source,  # FIX B-F01/B-F02: actual source dari ChainedAdapter
            asset_class=f"market/ohlcv/{inst.market}/timeframe={tf}",
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

        # Build market-specific adapter chain (GD §3.3.2 Source Priority Matrix)
        # FIX ADR-029 (GMI_Decision_Document_v7.docx, 30 Jul 2026): tvdatafeed
        # retired entirely -- yfinance .JK is now IDX30's SOLE source, not a
        # fallback. TvDatafeedAdapter import removed above; module archived to
        # src/bronze/archive/tvdatafeed_adapter.py (see KNOWN_RISKS.md RISK-1,
        # RESOLVED). Single-adapter ChainedAdapter is intentional -- ChainedAdapter
        # requires >=1 adapter (raises ValueError on empty list) but supports
        # exactly 1 fine; ChainedAdapter.fetch() with 1 adapter is a pure passthrough.
        if inst.market == "idx":
            chain = ChainedAdapter([YFinanceJKAdapter()])
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
        FIX ADR-029 (GMI_Decision_Document_v7.docx, 30 Jul 2026): idx case
        changed 'tvdatafeed' -> 'yfinance'. tvdatafeed retired entirely --
        yfinance .JK is IDX30's sole source now, not a fallback.
        """
        if inst.market == "idx":
            return "yfinance"
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
                    # ADD (hardcode-avoidance v1.11.2): named constant, was literal 0.6.
                    # Shared budget with Layer 1 — same constant, single source of truth.
                    time.sleep(YFINANCE_THROTTLE_SECONDS)

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

        # FIX ADR-045 (consequential — GMI_Decision_Document_v11.docx §2):
        # Layer 2 context ingestion iterates the SAME self.timeframes list
        # per symbol (run_context() loops 1D/1W/1M) via the SAME shared
        # IncFetchProtocol/BronzeIngester.write() base classes as Layer 1 —
        # the identical symbol+market+source-scoped-only path (no tf
        # dimension) starves 1W/1M for Layer 2 anchors exactly as it did
        # for Layer 1 before this fix. Not explicitly named in ADR-045's
        # own Consequences (written against _run_symbol() only), but the
        # same root cause applies verbatim here; left unfixed it would mean
        # Layer 2 context anchors (VIX, DXY, global indices, ETFs) inherit
        # the identical bug ADR-045 exists to close for Layer 1.
        start_date = self.inc.resolve_start_date(
            bronze_path=self.BRONZE_OHLCV_PATH / inst.market / f"timeframe={tf}",   # 'context' bucket
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
            asset_class=f"market/ohlcv/{inst.market}/timeframe={tf}",   # -> market/ohlcv/context/timeframe={tf}
            symbol=inst.symbol,
            extra_metadata=tz_hint,
        )
