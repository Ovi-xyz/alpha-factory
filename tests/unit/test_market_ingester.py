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
