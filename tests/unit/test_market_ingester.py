"""
tests/unit/test_market_ingester.py — GMI Wave 1 Cycle 3
Test suite untuk src/bronze/market_ingester.py Layer 2 context extension.

Dokumen referensi: alpha_factory_architecture_v2.docx §4
                   alpha_factory_architecture_extension_v1.docx §2-3, §8

Fokus: run_context() / _run_context_symbol() — ADD GMI-BRZ-001. Layer 1
run()/_run_symbol() TIDAK dimodifikasi di cycle ini, dan sebelumnya tidak
punya file test khusus sama sekali (diverifikasi kosong sebelum menulis
file ini) — di luar scope perubahan ini, tidak disentuh.

Semua test memakai monkeypatch.chdir(tmp_path) + mock _fetch()/get_loader()
untuk menghindari network call nyata ke yfinance (tidak tersedia di
sandbox network allowlist), mengikuti pola TestRunEntryPoint di
tests/unit/test_ohlcv_processor.py.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

import polars as pl
import pytest

from src.bronze.market_ingester import MarketOHLCVIngester
from src.config.instrument_loader import Instrument


def _fake_context_instrument(
    symbol: str = "VIX",
    yfinance_symbol: str = "^VIX",
    context_group: str = "equity",
    context_category: str = "context_volatility",
    timezone: str = "America/New_York",
    context_available: bool = True,
) -> Instrument:
    return Instrument(
        symbol=symbol, raw_symbol=symbol, market="context", sector=None,
        yfinance_symbol=yfinance_symbol, polygon_symbol="", tvfeed_symbol=None,
        eia_series=None, timezone=timezone, is_active=True,
        layer=2, context_category=context_category, context_group=context_group,
        context_available=context_available, include_in_forecast=True,
    )


def _sample_yf_df(n: int = 10) -> pl.DataFrame:
    base = date(2025, 1, 2)
    return pl.DataFrame({
        "timestamp": [base + timedelta(days=i) for i in range(n)],
        "open":   [100.0 + i for i in range(n)],
        "high":   [105.0 + i for i in range(n)],
        "low":    [98.0 + i for i in range(n)],
        "close":  [102.0 + i for i in range(n)],
        "volume": [1_000_000] * n,
    })


class TestContextSymbolResolution:
    """
    ADD GMI-BRZ-001: api_symbol harus inst.yfinance_symbol LANGSUNG.
    Diverifikasi empiris sebelum implementasi:
    to_api_symbol('DXY', 'context', 'yfinance') == 'DXY' (SALAH — tidak ada
    cabang untuk market='context', fallback ke suffix kosong) padahal
    seharusnya 'DX-Y.NYB'. Test ini mengunci kontrak yang benar.
    """

    def test_uses_yfinance_symbol_not_to_api_symbol(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        inst = _fake_context_instrument(
            symbol="DXY", yfinance_symbol="DX-Y.NYB",
            context_group="dollar", context_category="context_dollar",
            timezone="UTC",
        )
        ingester = MarketOHLCVIngester()
        captured = {}

        def fake_fetch(api_symbol, inst_arg, tf, start, end):
            captured["api_symbol"] = api_symbol
            return None

        monkeypatch.setattr(ingester, "_fetch", fake_fetch)
        ingester._run_context_symbol(inst, "1D", date(2026, 7, 1))

        assert captured["api_symbol"] == "DX-Y.NYB"

    def test_never_calls_to_api_symbol_for_context(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        inst = _fake_context_instrument()
        ingester = MarketOHLCVIngester()
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **k: None)

        with patch("src.bronze.market_ingester.to_api_symbol") as mock_to_api:
            ingester._run_context_symbol(inst, "1D", date(2026, 7, 1))
            mock_to_api.assert_not_called()

    def test_vix_symbol_resolution(self, monkeypatch, tmp_path):
        """VIX -> '^VIX' must reach _fetch() unchanged."""
        monkeypatch.chdir(tmp_path)
        inst = _fake_context_instrument(symbol="VIX", yfinance_symbol="^VIX")
        ingester = MarketOHLCVIngester()
        captured = {}

        def fake_fetch(api_symbol, *a, **k):
            captured["api_symbol"] = api_symbol
            return None   # must return None explicitly, not the dict value

        monkeypatch.setattr(ingester, "_fetch", fake_fetch)
        ingester._run_context_symbol(inst, "1D", date(2026, 7, 1))
        assert captured["api_symbol"] == "^VIX"


class TestContextSymbolWrite:
    """ADD GMI-BRZ-001: write path, schema validation, extra_metadata."""

    def test_writes_to_context_bucket(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        inst = _fake_context_instrument()
        ingester = MarketOHLCVIngester()
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **k: _sample_yf_df())

        ingester._run_context_symbol(inst, "1D", date(2026, 7, 1))

        written = list(
            (tmp_path / "data" / "bronze" / "market" / "ohlcv" / "context")
            .rglob("*.parquet")
        )
        assert len(written) == 1
        assert "symbol=VIX" in str(written[0])

    def test_tz_hint_written(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        inst = _fake_context_instrument(timezone="Asia/Tokyo")
        ingester = MarketOHLCVIngester()
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **k: _sample_yf_df())

        ingester._run_context_symbol(inst, "1D", date(2026, 7, 1))

        written = list(
            (tmp_path / "data" / "bronze" / "market" / "ohlcv" / "context")
            .rglob("*.parquet")
        )
        df = pl.read_parquet(written[0])
        assert df["_tz_hint"][0] == "Asia/Tokyo"

    def test_no_duplicated_context_metadata_columns(self, monkeypatch, tmp_path):
        """
        Deliberate design choice (see market_ingester.py ADD GMI-BRZ-001
        comment block): context_group/context_category are NOT duplicated
        into Bronze Parquet rows. They remain available via the single
        source of truth, InstrumentLoader.get_context(symbol) — baking a
        second copy into every row would be redundant and untracked by
        _normalize_columns()'s Bronze-metadata drop list downstream.
        """
        monkeypatch.chdir(tmp_path)
        inst = _fake_context_instrument()
        ingester = MarketOHLCVIngester()
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **k: _sample_yf_df())

        ingester._run_context_symbol(inst, "1D", date(2026, 7, 1))

        written = list(
            (tmp_path / "data" / "bronze" / "market" / "ohlcv" / "context")
            .rglob("*.parquet")
        )
        df = pl.read_parquet(written[0])
        assert "_context_group" not in df.columns
        assert "_context_category" not in df.columns

    def test_none_df_writes_nothing(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        inst = _fake_context_instrument()
        ingester = MarketOHLCVIngester()
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **k: None)

        ingester._run_context_symbol(inst, "1D", date(2026, 7, 1))

        context_dir = tmp_path / "data" / "bronze" / "market" / "ohlcv" / "context"
        assert not context_dir.exists() or not list(context_dir.rglob("*.parquet"))

    def test_schema_mismatch_quarantines(self, monkeypatch, tmp_path):
        """A DataFrame missing required OHLCV columns must be quarantined,
        not written to Bronze — same SchemaValidator/handle_mismatch path
        Layer 1 already uses (reused unmodified for Layer 2)."""
        import shutil
        from pathlib import Path as _P

        # _load_schema_validators() runs at __init__ time and needs the real
        # schema YAML on disk relative to the (already chdir'd) cwd — copy it
        # into tmp_path first, otherwise validation is silently skipped and
        # this test would pass for the wrong reason (no validator loaded).
        real_schema_dir = _P(__file__).resolve().parents[2] / "config" / "schemas"
        tmp_schema_dir  = tmp_path / "config" / "schemas"
        tmp_schema_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(
            real_schema_dir / "yfinance_ohlcv.yaml",
            tmp_schema_dir / "yfinance_ohlcv.yaml",
        )

        monkeypatch.chdir(tmp_path)
        inst = _fake_context_instrument()
        ingester = MarketOHLCVIngester()
        assert "yfinance" in ingester._schema_validators, (
            "Schema validator failed to load — test fixture setup is broken"
        )
        bad_df = pl.DataFrame({"timestamp": [date(2025, 1, 1)], "close": [100.0]})
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **k: bad_df)

        ingester._run_context_symbol(inst, "1D", date(2026, 7, 1))

        context_dir = tmp_path / "data" / "bronze" / "market" / "ohlcv" / "context"
        assert not context_dir.exists() or not list(context_dir.rglob("*.parquet"))
        quarantine_dir = tmp_path / "data" / "quarantine"
        assert quarantine_dir.exists()


class TestRunContextEntryPoint:
    """ADD GMI-BRZ-001: run_context() end-to-end — loop, checkpoint, resume."""

    @staticmethod
    def _fake_loader(instruments):
        class _FakeLoader:
            def all_context(self_inner, include_deferred=False):
                if include_deferred:
                    return instruments
                return [i for i in instruments if i.context_available]
        return _FakeLoader()

    def test_iterates_only_active_context_instruments(self, monkeypatch, tmp_path):
        """Deferred instruments (context_available=False) must never reach
        _run_context_symbol — all_context(include_deferred=False) excludes
        them before the loop body ever runs (ADR-007 contract)."""
        monkeypatch.chdir(tmp_path)
        active   = _fake_context_instrument("VIX", "^VIX")
        deferred = _fake_context_instrument(
            "CPO", "", context_group="commodity",
            context_category="context_commodity_agri", context_available=False,
        )
        monkeypatch.setattr(
            "src.bronze.market_ingester.get_loader",
            lambda: self._fake_loader([active, deferred]),
        )
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        calls = []
        monkeypatch.setattr(
            ingester, "_run_context_symbol",
            lambda inst, tf, rd: calls.append(inst.symbol),
        )
        ingester.run_context(date(2026, 7, 1))
        assert calls == ["VIX"]

    def test_resumable_via_checkpoint(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        inst = _fake_context_instrument("VIX", "^VIX")
        monkeypatch.setattr(
            "src.bronze.market_ingester.get_loader",
            lambda: self._fake_loader([inst]),
        )
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        call_count = {"n": 0}
        monkeypatch.setattr(
            ingester, "_run_context_symbol",
            lambda inst_arg, tf, rd: call_count.__setitem__("n", call_count["n"] + 1),
        )

        run_date = date(2026, 7, 1)
        ingester.run_context(run_date)
        ingester.run_context(run_date)   # second call — already-done symbol must skip

        assert call_count["n"] == 1

    def test_failure_isolated_per_symbol_and_namespace(self, monkeypatch, tmp_path):
        """One symbol's exception must not stop the loop for others, and
        must land in the 'bronze_ohlcv_context_daily' checkpoint namespace
        only — never touching Layer 1's 'bronze_ohlcv_daily' namespace."""
        monkeypatch.chdir(tmp_path)
        a = _fake_context_instrument("VIX", "^VIX")
        b = _fake_context_instrument(
            "DXY", "DX-Y.NYB", context_group="dollar",
            context_category="context_dollar", timezone="UTC",
        )
        monkeypatch.setattr(
            "src.bronze.market_ingester.get_loader",
            lambda: self._fake_loader([a, b]),
        )
        ingester = MarketOHLCVIngester(timeframes=["1D"])

        def flaky(inst_arg, tf, rd):
            if inst_arg.symbol == "VIX":
                raise RuntimeError("simulated fetch failure")

        monkeypatch.setattr(ingester, "_run_context_symbol", flaky)
        ingester.run_context(date(2026, 7, 1))   # must not raise

        from src.utils.progress_checkpoint import ProgressCheckpoint
        ctx_ckpt = ProgressCheckpoint("bronze_ohlcv_context_daily", date(2026, 7, 1))
        assert ctx_ckpt.is_done("DXY", timeframe="1D")
        assert not ctx_ckpt.is_done("VIX", timeframe="1D")

        layer1_ckpt = ProgressCheckpoint("bronze_ohlcv_daily", date(2026, 7, 1))
        assert not layer1_ckpt.is_done("DXY", timeframe="1D")
        assert not layer1_ckpt.is_done("VIX", timeframe="1D")


# ── Layer 1 — coverage tranche (17 Aug 2026)
# run() / _run_symbol() / _fetch() / _primary_source_for()'s idx+forex branches
# had zero test coverage from the start (per this file's own original module
# docstring, verified empty before Layer 2 work began and left untouched by
# GMI Wave 1 Cycle 3). Same isolation pattern as TestRunContextEntryPoint
# above: monkeypatch.chdir(tmp_path) + mock _fetch()/get_loader() to avoid
# real network calls.

def _fake_layer1_instrument(
    symbol: str = "AAPL",
    raw_symbol: str = None,
    market: str = "us_stocks",
    timezone: str = "America/New_York",
) -> Instrument:
    return Instrument(
        symbol=symbol, raw_symbol=raw_symbol or symbol, market=market, sector=None,
        yfinance_symbol=symbol, polygon_symbol=symbol, tvfeed_symbol=None,
        eia_series=None, timezone=timezone, is_active=True, layer=1,
    )


class TestRunEntryPoint:
    """Layer 1 equivalent of TestRunContextEntryPoint above."""

    @staticmethod
    def _fake_loader(instruments):
        class _FakeLoader:
            def all_symbols(self_inner):
                return instruments
        return _FakeLoader()

    def test_iterates_all_active_instruments(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        a = _fake_layer1_instrument("AAPL")
        b = _fake_layer1_instrument("MSFT")
        monkeypatch.setattr(
            "src.bronze.market_ingester.get_loader",
            lambda: self._fake_loader([a, b]),
        )
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        calls = []
        monkeypatch.setattr(
            ingester, "_run_symbol", lambda inst, tf, rd: calls.append(inst.symbol)
        )
        ingester.run(date(2026, 7, 1))
        assert calls == ["AAPL", "MSFT"]

    def test_resumable_via_checkpoint(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        inst = _fake_layer1_instrument("AAPL")
        monkeypatch.setattr(
            "src.bronze.market_ingester.get_loader",
            lambda: self._fake_loader([inst]),
        )
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        call_count = {"n": 0}
        monkeypatch.setattr(
            ingester, "_run_symbol",
            lambda inst_arg, tf, rd: call_count.__setitem__("n", call_count["n"] + 1),
        )
        run_date = date(2026, 7, 1)
        ingester.run(run_date)
        ingester.run(run_date)   # second call — already-done symbol must skip
        assert call_count["n"] == 1

    def test_failure_isolated_per_symbol_and_namespace(self, monkeypatch, tmp_path):
        """Uses the 'bronze_ohlcv_daily' namespace — never the Layer 2
        'bronze_ohlcv_context_daily' namespace (mirror of the Layer 2 test's
        cross-namespace-isolation assertion, in the opposite direction)."""
        monkeypatch.chdir(tmp_path)
        a = _fake_layer1_instrument("AAPL")
        b = _fake_layer1_instrument("MSFT")
        monkeypatch.setattr(
            "src.bronze.market_ingester.get_loader",
            lambda: self._fake_loader([a, b]),
        )
        ingester = MarketOHLCVIngester(timeframes=["1D"])

        def flaky(inst_arg, tf, rd):
            if inst_arg.symbol == "AAPL":
                raise RuntimeError("simulated fetch failure")

        monkeypatch.setattr(ingester, "_run_symbol", flaky)
        ingester.run(date(2026, 7, 1))   # must not raise

        from src.utils.progress_checkpoint import ProgressCheckpoint
        layer1_ckpt = ProgressCheckpoint("bronze_ohlcv_daily", date(2026, 7, 1))
        assert layer1_ckpt.is_done("MSFT", timeframe="1D")
        assert not layer1_ckpt.is_done("AAPL", timeframe="1D")

        ctx_ckpt = ProgressCheckpoint("bronze_ohlcv_context_daily", date(2026, 7, 1))
        assert not ctx_ckpt.is_done("MSFT", timeframe="1D")


class TestRunSymbol:
    """_run_symbol() body — lines 148-199."""

    def test_no_data_skips_write(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: None)
        write_calls = []
        monkeypatch.setattr(ingester, "write", lambda **kw: write_calls.append(kw))
        inst = _fake_layer1_instrument("AAPL")
        ingester._run_symbol(inst, "1D", date(2026, 7, 1))
        assert write_calls == []

    def test_successful_fetch_writes_with_actual_source_from_df(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        df = _sample_yf_df().with_columns(pl.lit("polygon").alias("_source"))
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: df)
        ingester._schema_validators = {}   # skip schema gate for this test
        write_calls = []
        monkeypatch.setattr(ingester, "write", lambda **kw: write_calls.append(kw))
        inst = _fake_layer1_instrument("AAPL")
        ingester._run_symbol(inst, "1D", date(2026, 7, 1))
        assert len(write_calls) == 1
        assert write_calls[0]["source"] == "polygon"   # from df["_source"], not primary_src
        assert write_calls[0]["asset_class"] == "market/ohlcv/us_stocks/timeframe=1D"
        assert write_calls[0]["extra_metadata"] == {"_tz_hint": "America/New_York"}

    def test_missing_source_column_falls_back_to_primary_src(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        df = _sample_yf_df()   # no _source column
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: df)
        ingester._schema_validators = {}
        write_calls = []
        monkeypatch.setattr(ingester, "write", lambda **kw: write_calls.append(kw))
        inst = _fake_layer1_instrument("AAPL")
        ingester._run_symbol(inst, "1D", date(2026, 7, 1))
        assert write_calls[0]["source"] == "yfinance"   # _primary_source_for fallback

    def test_schema_mismatch_quarantines_and_skips_write(self, monkeypatch, tmp_path):
        from unittest.mock import MagicMock
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        df = _sample_yf_df().with_columns(pl.lit("yfinance").alias("_source"))
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: df)
        mock_validator = MagicMock()
        mock_validator.validate.return_value = (False, ["bad schema"])
        ingester._schema_validators = {"yfinance": mock_validator}
        write_calls = []
        monkeypatch.setattr(ingester, "write", lambda **kw: write_calls.append(kw))
        inst = _fake_layer1_instrument("AAPL")
        ingester._run_symbol(inst, "1D", date(2026, 7, 1))
        assert write_calls == []


class TestRunSymbolADR045TimeframePartition:
    """FIX ADR-045 (GMI_Decision_Document_v11.docx §2): _run_symbol() must
    fold the timeframe into both the IncFetchProtocol.resolve_start_date()
    scan path and the BronzeIngester.write() write path — previously both
    were symbol+market+source-scoped only, causing 1D's freshly-written
    file to starve 1W/1M via the same-day idempotency check (FIX GD-F08)
    on every run, every day (not just cold start)."""

    def test_resolve_start_date_receives_timeframe_scoped_bronze_path(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1W"])
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: None)
        calls = []
        monkeypatch.setattr(
            ingester.inc, "resolve_start_date",
            lambda **kw: (calls.append(kw), date(2020, 1, 1))[1],
        )
        inst = _fake_layer1_instrument("AAPL", market="us_stocks")
        ingester._run_symbol(inst, "1W", date(2026, 7, 1))
        assert len(calls) == 1
        assert calls[0]["bronze_path"] == (
            ingester.BRONZE_OHLCV_PATH / "us_stocks" / "timeframe=1W"
        )

    def test_write_receives_timeframe_scoped_asset_class(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1M"])
        df = _sample_yf_df().with_columns(pl.lit("yfinance").alias("_source"))
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: df)
        ingester._schema_validators = {}
        write_calls = []
        monkeypatch.setattr(ingester, "write", lambda **kw: write_calls.append(kw))
        inst = _fake_layer1_instrument("AAPL", market="us_stocks")
        ingester._run_symbol(inst, "1M", date(2026, 7, 1))
        assert write_calls[0]["asset_class"] == "market/ohlcv/us_stocks/timeframe=1M"

    def test_different_timeframes_scope_to_different_bronze_paths(self, monkeypatch, tmp_path):
        """Regression guard for the exact starvation bug: 1D and 1W must
        never resolve to the same bronze_path for the same symbol."""
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: None)
        paths = []
        monkeypatch.setattr(
            ingester.inc, "resolve_start_date",
            lambda **kw: (paths.append(kw["bronze_path"]), date(2020, 1, 1))[1],
        )
        inst = _fake_layer1_instrument("AAPL", market="us_stocks")
        for tf in ["1D", "1W", "1M"]:
            ingester._run_symbol(inst, tf, date(2026, 7, 1))
        assert len(set(paths)) == 3, f"Expected 3 distinct bronze_paths, got {paths}"


class TestRunContextSymbolADR045TimeframePartition:
    """FIX ADR-045 companion (Layer 2 — same root cause as Layer 1 above,
    not named explicitly in ADR-045's own Consequences but structurally
    identical: run_context() iterates self.timeframes per instrument via
    the SAME shared IncFetchProtocol/BronzeIngester.write() base classes)."""

    def test_resolve_start_date_receives_timeframe_scoped_bronze_path(self, monkeypatch, tmp_path):
        from src.config.instrument_loader import Instrument
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1W"])
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: None)
        calls = []
        monkeypatch.setattr(
            ingester.inc, "resolve_start_date",
            lambda **kw: (calls.append(kw), date(2020, 1, 1))[1],
        )
        inst = Instrument(
            symbol="VIX", raw_symbol="VIX", market="context", sector=None,
            yfinance_symbol="^VIX", polygon_symbol="", tvfeed_symbol=None,
            eia_series=None, timezone="America/New_York", is_active=True,
            layer=2, context_category="context_volatility", context_group="equity",
            context_available=True, include_in_forecast=True,
        )
        ingester._run_context_symbol(inst, "1W", date(2026, 7, 1))
        assert len(calls) == 1
        assert calls[0]["bronze_path"] == (
            ingester.BRONZE_OHLCV_PATH / "context" / "timeframe=1W"
        )

    def test_write_receives_timeframe_scoped_asset_class(self, monkeypatch, tmp_path):
        from src.config.instrument_loader import Instrument
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        df = _sample_yf_df().with_columns(pl.lit("yfinance").alias("_source"))
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: df)
        ingester._schema_validators = {}
        write_calls = []
        monkeypatch.setattr(ingester, "write", lambda **kw: write_calls.append(kw))
        inst = Instrument(
            symbol="VIX", raw_symbol="VIX", market="context", sector=None,
            yfinance_symbol="^VIX", polygon_symbol="", tvfeed_symbol=None,
            eia_series=None, timezone="America/New_York", is_active=True,
            layer=2, context_category="context_volatility", context_group="equity",
            context_available=True, include_in_forecast=True,
        )
        ingester._run_context_symbol(inst, "1D", date(2026, 7, 1))
        assert write_calls[0]["asset_class"] == "market/ohlcv/context/timeframe=1D"


class TestRunSymbolADR047CommodityTicker:
    """FIX ADR-047 (GMI_Decision_Document_v11.docx §2): _run_symbol()'s
    commodity-market ticker resolution routes through inst.yfinance_symbol
    directly, mirroring _run_context_symbol()'s Layer 2 pattern —
    to_api_symbol()'s commodity branch has no override table and falls
    through to a generic sym + '=F' rule, producing invalid AU=F/AG=F
    (instruments_identity.yaml already carries the correct GC=F/SI=F on
    inst.yfinance_symbol). to_api_symbol() itself is left unchanged."""

    @staticmethod
    def _fake_commodity_instrument(symbol, yfinance_symbol):
        return Instrument(
            symbol=symbol, raw_symbol=symbol, market="commodity", sector=None,
            yfinance_symbol=yfinance_symbol, polygon_symbol=symbol, tvfeed_symbol=None,
            eia_series=None, timezone="America/New_York", is_active=True, layer=1,
        )

    def test_gold_au_uses_yfinance_symbol_not_to_api_symbol(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        captured = {}
        monkeypatch.setattr(
            ingester, "_fetch",
            lambda api_symbol, inst, tf, start, end: captured.update(api_symbol=api_symbol) or None,
        )
        inst = self._fake_commodity_instrument("AU", "GC=F")
        ingester._run_symbol(inst, "1D", date(2026, 7, 1))
        assert captured["api_symbol"] == "GC=F"   # NOT 'AU=F' (to_api_symbol's wrong generic output)

    def test_silver_ag_uses_yfinance_symbol_not_to_api_symbol(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        captured = {}
        monkeypatch.setattr(
            ingester, "_fetch",
            lambda api_symbol, inst, tf, start, end: captured.update(api_symbol=api_symbol) or None,
        )
        inst = self._fake_commodity_instrument("AG", "SI=F")
        ingester._run_symbol(inst, "1D", date(2026, 7, 1))
        assert captured["api_symbol"] == "SI=F"   # NOT 'AG=F' (to_api_symbol's wrong generic output)

    def test_wti_crude_unaffected_still_resolves_cl_f(self, monkeypatch, tmp_path):
        """CL was already correct via to_api_symbol()'s generic suffix rule
        by coincidence — must remain correct after routing via yfinance_symbol."""
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        captured = {}
        monkeypatch.setattr(
            ingester, "_fetch",
            lambda api_symbol, inst, tf, start, end: captured.update(api_symbol=api_symbol) or None,
        )
        inst = self._fake_commodity_instrument("CL", "CL=F")
        ingester._run_symbol(inst, "1D", date(2026, 7, 1))
        assert captured["api_symbol"] == "CL=F"

    def test_non_commodity_market_still_uses_to_api_symbol(self, monkeypatch, tmp_path):
        """Regression guard: the ADR-047 fix must be scoped to market ==
        'commodity' only — every other Layer 1 market keeps resolving via
        to_api_symbol(), unchanged."""
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        captured = {}
        monkeypatch.setattr(
            ingester, "_fetch",
            lambda api_symbol, inst, tf, start, end: captured.update(api_symbol=api_symbol) or None,
        )
        # us_stocks instrument whose yfinance_symbol deliberately differs from
        # what to_api_symbol() would compute — proves the us_stocks path did
        # NOT switch to inst.yfinance_symbol.
        inst = Instrument(
            symbol="AAPL", raw_symbol="AAPL", market="us_stocks", sector=None,
            yfinance_symbol="WRONG_ON_PURPOSE", polygon_symbol="AAPL", tvfeed_symbol=None,
            eia_series=None, timezone="America/New_York", is_active=True, layer=1,
        )
        ingester._run_symbol(inst, "1D", date(2026, 7, 1))
        assert captured["api_symbol"] == "AAPL"   # to_api_symbol('AAPL','us_stocks','yfinance')

    def test_forex_market_saves_to_day_cache(self, monkeypatch, tmp_path):
        from src.bronze.forex_cache import ForexDayCache
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        df = _sample_yf_df().with_columns(pl.lit("yfinance_forex").alias("_source"))
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: df)
        ingester._schema_validators = {}
        monkeypatch.setattr(ingester, "write", lambda **kw: None)
        inst = _fake_layer1_instrument("EUR_USD", raw_symbol="EUR/USD", market="forex", timezone="UTC")
        with patch.object(ForexDayCache, "save") as mock_save:
            ingester._run_symbol(inst, "1D", date(2026, 7, 1))
        mock_save.assert_called_once_with("EUR_USD", df, date(2026, 7, 1))

    def test_forex_cache_save_failure_is_non_critical(self, monkeypatch, tmp_path):
        """FIX B-F07: a ForexDayCache.save() exception must not propagate —
        it's a best-effort cache write, not a required step."""
        from src.bronze.forex_cache import ForexDayCache
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        df = _sample_yf_df().with_columns(pl.lit("yfinance_forex").alias("_source"))
        monkeypatch.setattr(ingester, "_fetch", lambda *a, **kw: df)
        ingester._schema_validators = {}
        write_calls = []
        monkeypatch.setattr(ingester, "write", lambda **kw: write_calls.append(kw))
        inst = _fake_layer1_instrument("EUR_USD", raw_symbol="EUR/USD", market="forex", timezone="UTC")
        with patch.object(ForexDayCache, "save", side_effect=OSError("disk full")):
            ingester._run_symbol(inst, "1D", date(2026, 7, 1))   # must not raise
        assert len(write_calls) == 1   # write still proceeds despite cache failure


class TestFetchChainConstruction:
    """_fetch() — lines 219-249: per-market ChainedAdapter composition."""

    def test_idx_market_single_adapter_chain(self, monkeypatch, tmp_path):
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        inst = _fake_layer1_instrument("BBCA", market="idx", timezone="Asia/Jakarta")
        with patch("src.bronze.yfinance_adapter.YFinanceJKAdapter.fetch", return_value=_sample_yf_df()) as mock_fetch:
            result = ingester._fetch("BBCA.JK", inst, "1D", date(2026, 6, 1), date(2026, 7, 1))
        assert result is not None
        assert result["_source"][0] == "yfinance_jk"
        mock_fetch.assert_called_once()

    def test_forex_market_three_adapter_chain(self, monkeypatch, tmp_path):
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        inst = _fake_layer1_instrument("EUR_USD", market="forex", timezone="UTC")
        # First adapter in chain succeeds — confirms chain is wired, not which one wins
        with patch("src.bronze.yfinance_adapter.YFinanceForexAdapter.fetch", return_value=_sample_yf_df()) as mock_fetch:
            result = ingester._fetch("EURUSD=X", inst, "1D", date(2026, 6, 1), date(2026, 7, 1))
        assert result is not None
        assert result["_source"][0] == "yfinance_forex"
        mock_fetch.assert_called_once()

    def test_us_stocks_market_yfinance_polygon_chain(self, monkeypatch, tmp_path):
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        inst = _fake_layer1_instrument("AAPL", market="us_stocks")
        with patch("src.bronze.yfinance_adapter.YFinanceAdapter.fetch", return_value=_sample_yf_df()) as mock_fetch:
            result = ingester._fetch("AAPL", inst, "1D", date(2026, 6, 1), date(2026, 7, 1))
        assert result is not None
        assert result["_source"][0] == "yfinance"
        mock_fetch.assert_called_once()

    def test_other_markets_yfinance_only_chain(self, monkeypatch, tmp_path):
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        ingester = MarketOHLCVIngester(timeframes=["1D"])
        inst = _fake_layer1_instrument("CL", market="commodity")
        with patch("src.bronze.yfinance_adapter.YFinanceAdapter.fetch", return_value=_sample_yf_df()) as mock_fetch:
            result = ingester._fetch("CL=F", inst, "1D", date(2026, 6, 1), date(2026, 7, 1))
        assert result is not None
        assert result["_source"][0] == "yfinance"
        mock_fetch.assert_called_once()


class TestPrimarySourceForRemainingBranches:
    """_primary_source_for() — lines 262 (idx), 264 (forex). The default
    fallback branch (line 265) was already exercised elsewhere; the idx and
    forex branches specifically (which return the same value but via a
    distinct code path) were not."""

    def test_idx_returns_yfinance(self):
        inst = _fake_layer1_instrument("BBCA", market="idx")
        assert MarketOHLCVIngester._primary_source_for(inst) == "yfinance"

    def test_forex_returns_yfinance(self):
        inst = _fake_layer1_instrument("EUR_USD", market="forex")
        assert MarketOHLCVIngester._primary_source_for(inst) == "yfinance"
