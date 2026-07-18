"""
test_finnhub_sentiment_ingester.py — Unit Tests: FinnhubSentimentIngester

Tests validate:
    1. Layer separation invariants — no Silver imports in Bronze module (GD §17.7)
    2. depends_on=[] contract — scope from InstrumentLoader, not Silver output
    3. _fetch_one() — per-symbol API call success/failure paths
    4. run() — early exits, write() invocation, schema validation gate
    5. Schema validation integration (SchemaValidator gate, GD §3.7)

Coverage target: ≥80% line coverage (IDD §10.3 / refactor plan §7.4)

NOTE on AST-based checks:
    Tests that inspect source code use ast.parse() + node walking to distinguish
    actual code from docstrings/comments. Docstrings mention removed patterns for
    historical context — a raw string-search would produce false positives.
"""

from __future__ import annotations

import ast
import sys
from datetime import date
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.bronze.finnhub_sentiment_ingester import (
    FinnhubSentimentIngester,
    BRONZE_SENTIMENT_PATH,
    THROTTLE_SECONDS,
    _ASSET_CLASS,
    _SCHEMA_PATH,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def run_date() -> date:
    return date(2025, 1, 22)


@pytest.fixture
def valid_finnhub_response() -> dict:
    return {
        "companyNewsScore": 0.75,
        "buzz": {"buzz": 1.2, "articlesInLastWeek": 15},
        "symbol": "AAPL",
    }


@pytest.fixture
def ingester_with_mock_client(valid_finnhub_response) -> FinnhubSentimentIngester:
    ingester = FinnhubSentimentIngester()
    ingester._api_key = "test_key"
    mock_client = MagicMock()
    mock_client.news_sentiment.return_value = valid_finnhub_response
    ingester._client = mock_client
    return ingester


def _get_code_only(source: str) -> str:
    """
    Return source with docstring string-literals removed.
    AST walk: collect line ranges of module/class/function docstrings,
    then exclude those lines. Prevents false positives from historical
    context strings in docstrings.
    """
    tree = ast.parse(source)
    docstring_lines: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef,
                              ast.FunctionDef, ast.AsyncFunctionDef)):
            if (node.body
                    and isinstance(node.body[0], ast.Expr)
                    and isinstance(node.body[0].value, ast.Constant)
                    and isinstance(node.body[0].value.value, str)):
                ds = node.body[0]
                docstring_lines.update(range(ds.lineno, ds.end_lineno + 1))
    lines = source.splitlines()
    return "\n".join(
        line for i, line in enumerate(lines, 1)
        if i not in docstring_lines
    )


# ── Layer Separation Invariants ───────────────────────────────────────────────

class TestLayerSeparationInvariants:
    """Architectural invariants per GD §17.7 and GD §17.3."""

    def test_no_silver_layer_imports(self):
        """Bronze ingester must NOT import from Silver layer (GD §17.7)."""
        import src.bronze.finnhub_sentiment_ingester as module
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module and "silver" in node.module), (
                    f"Bronze ingester imports from Silver layer: {node.module}. "
                    "Violates GD §17.7 — Bronze must be independent of all Silver modules."
                )

    def test_no_active_symbols_resolver_import(self):
        """Bronze ingester must NOT import ActiveSymbolsResolver."""
        import src.bronze.finnhub_sentiment_ingester as module
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module:
                    for alias in node.names:
                        assert "ActiveSymbols" not in alias.name, (
                            "Bronze imports ActiveSymbolsResolver — violates depends_on=[] contract."
                        )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert "active_symbols" not in alias.name

    def test_finnhub_imported_lazily_not_at_module_level(self):
        """
        finnhub must be imported lazily (inside run()) — NOT at module top level.
        Top-level import would fail on environments without finnhub-python installed.
        """
        import src.bronze.finnhub_sentiment_ingester as module
        tree = ast.parse(Path(module.__file__).read_text())
        # Module.body contains ONLY top-level statements
        for node in tree.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert "finnhub" not in alias.name, (
                        f"finnhub imported at module top level — must be lazy import "
                        f"inside run() to support optional dependency."
                    )
            elif isinstance(node, ast.ImportFrom):
                assert not (node.module and "finnhub" in node.module), (
                    "finnhub imported at module top level — must be lazy."
                )

    def test_uses_instrument_loader_for_scope(self):
        """Bronze ingester must use InstrumentLoader for 643 universe scope."""
        import src.bronze.finnhub_sentiment_ingester as module
        code = _get_code_only(Path(module.__file__).read_text())
        assert "get_loader" in code or "InstrumentLoader" in code, (
            "Bronze ingester does not reference InstrumentLoader. "
            "Scope must be 643 universe, not Silver active_symbols output."
        )

    def test_syntax_valid(self):
        """Module must be syntactically valid Python."""
        import src.bronze.finnhub_sentiment_ingester as module
        ast.parse(Path(module.__file__).read_text())

    def test_schema_yaml_exists(self):
        """finnhub_sentiment.yaml must exist (GD §3.7)."""
        assert Path(_SCHEMA_PATH).exists(), (
            f"Schema registry YAML missing: {_SCHEMA_PATH}"
        )

    def test_asset_class_in_bronze_fundamental_path(self):
        """_ASSET_CLASS must route to Bronze market/fundamental path."""
        assert _ASSET_CLASS.startswith("market/fundamental"), (
            f"Unexpected asset_class: {_ASSET_CLASS}"
        )

    def test_throttle_respects_rate_limit(self):
        """THROTTLE_SECONDS must be ≥1.0 (Finnhub: 60 req/min = 1 req/sec min)."""
        assert THROTTLE_SECONDS >= 1.0, (
            f"THROTTLE_SECONDS={THROTTLE_SECONDS} too low — Finnhub limit 60 req/min."
        )


# ── run() Early-Exit Paths ────────────────────────────────────────────────────

class TestRunEarlyExits:

    def test_no_api_key_returns_early_no_write(self, run_date):
        """run() must skip write() when FINNHUB_API_KEY is not set."""
        ingester = FinnhubSentimentIngester()
        ingester._api_key = None
        with patch.object(ingester, "write") as mock_write:
            ingester.run(run_date)
        mock_write.assert_not_called()

    @patch("time.sleep")
    def test_all_symbols_fail_skips_write(self, mock_sleep, run_date):
        """run() skips write() when every _fetch_one() returns None."""
        # Inject fake finnhub into sys.modules so the import inside run() succeeds
        fake_finnhub = ModuleType("finnhub")
        fake_client  = MagicMock()
        fake_client.news_sentiment.side_effect = Exception("rate limited")
        fake_finnhub.Client = MagicMock(return_value=fake_client)

        ingester = FinnhubSentimentIngester()
        ingester._api_key = "test_key"

        mock_inst = MagicMock()
        mock_inst.symbol = "BBCA"

        with patch.dict(sys.modules, {"finnhub": fake_finnhub}):
            with patch("src.bronze.finnhub_sentiment_ingester.get_loader") as mock_loader:
                mock_loader.return_value.all_symbols.return_value = [mock_inst]
                with patch.object(ingester, "write") as mock_write:
                    ingester.run(run_date)
                    mock_write.assert_not_called()

    @patch("time.sleep")
    def test_run_calls_write_with_correct_asset_class(self, mock_sleep, run_date,
                                                       ingester_with_mock_client):
        """run() calls write(asset_class=_ASSET_CLASS, source='finnhub') on success."""
        fake_finnhub = ModuleType("finnhub")
        fake_finnhub.Client = MagicMock(return_value=ingester_with_mock_client._client)

        mock_inst = MagicMock()
        mock_inst.symbol = "AAPL"

        with patch.dict(sys.modules, {"finnhub": fake_finnhub}):
            with patch("src.bronze.finnhub_sentiment_ingester.get_loader") as mock_loader:
                mock_loader.return_value.all_symbols.return_value = [mock_inst]
                with patch.object(ingester_with_mock_client, "_validator") as mock_val:
                    mock_val.validate.return_value = (True, [])
                    with patch.object(ingester_with_mock_client, "write",
                                      return_value=MagicMock()) as mock_write:
                        ingester_with_mock_client.run(run_date)
                        mock_write.assert_called_once()
                        kwargs = mock_write.call_args.kwargs
                        assert kwargs.get("asset_class") == _ASSET_CLASS
                        assert kwargs.get("source") == "finnhub"

    @patch("time.sleep")
    def test_run_symbol_key_includes_run_date(self, mock_sleep, run_date,
                                               ingester_with_mock_client):
        """write() symbol key must embed run_date for BronzeIngester idempotency."""
        fake_finnhub = ModuleType("finnhub")
        fake_finnhub.Client = MagicMock(return_value=ingester_with_mock_client._client)

        mock_inst = MagicMock()
        mock_inst.symbol = "AAPL"

        with patch.dict(sys.modules, {"finnhub": fake_finnhub}):
            with patch("src.bronze.finnhub_sentiment_ingester.get_loader") as mock_loader:
                mock_loader.return_value.all_symbols.return_value = [mock_inst]
                with patch.object(ingester_with_mock_client, "_validator") as mock_val:
                    mock_val.validate.return_value = (True, [])
                    with patch.object(ingester_with_mock_client, "write",
                                      return_value=None) as mock_write:
                        ingester_with_mock_client.run(run_date)
                        symbol_arg = mock_write.call_args.kwargs.get("symbol", "")
                        assert run_date.isoformat() in symbol_arg, (
                            f"Symbol key '{symbol_arg}' must contain '{run_date.isoformat()}'"
                        )

    @patch("time.sleep")
    def test_schema_mismatch_skips_write_calls_quarantine(self, mock_sleep, run_date,
                                                           ingester_with_mock_client):
        """Schema validation failure must quarantine data and skip write()."""
        fake_finnhub = ModuleType("finnhub")
        fake_finnhub.Client = MagicMock(return_value=ingester_with_mock_client._client)

        mock_inst = MagicMock()
        mock_inst.symbol = "AAPL"

        with patch.dict(sys.modules, {"finnhub": fake_finnhub}):
            with patch("src.bronze.finnhub_sentiment_ingester.get_loader") as mock_loader:
                mock_loader.return_value.all_symbols.return_value = [mock_inst]
                with patch.object(ingester_with_mock_client, "_validator") as mock_val:
                    mock_val.validate.return_value = (False, ["Missing column: 'symbol'"])
                    with patch.object(ingester_with_mock_client, "write") as mock_write:
                        ingester_with_mock_client.run(run_date)
                        mock_write.assert_not_called()
                        mock_val.handle_mismatch.assert_called_once()


# ── _fetch_one() Unit Tests ───────────────────────────────────────────────────

class TestFetchOne:

    def test_success_returns_complete_dict(self, run_date, ingester_with_mock_client):
        result = ingester_with_mock_client._fetch_one("AAPL", run_date)
        assert result is not None
        assert set(result.keys()) == {
            "symbol", "sentiment_score", "buzz_score",
            "news_volume_7d", "source", "fetched_date"
        }

    def test_success_correct_values(self, run_date, ingester_with_mock_client):
        result = ingester_with_mock_client._fetch_one("AAPL", run_date)
        assert result["symbol"] == "AAPL"
        assert result["sentiment_score"] == pytest.approx(0.75)
        assert result["buzz_score"] == pytest.approx(1.2)
        assert result["news_volume_7d"] == 15
        assert result["source"] == "finnhub"
        assert result["fetched_date"] == str(run_date)

    def test_success_python_types(self, run_date, ingester_with_mock_client):
        result = ingester_with_mock_client._fetch_one("AAPL", run_date)
        assert isinstance(result["sentiment_score"], float)
        assert isinstance(result["buzz_score"], float)
        assert isinstance(result["news_volume_7d"], int)

    def test_api_exception_returns_none(self, run_date):
        ingester = FinnhubSentimentIngester()
        ingester._api_key = "test_key"
        ingester._client = MagicMock()
        ingester._client.news_sentiment.side_effect = Exception("timeout")
        assert ingester._fetch_one("BBCA", run_date) is None

    def test_none_response_returns_none(self, run_date):
        ingester = FinnhubSentimentIngester()
        ingester._api_key = "test_key"
        ingester._client = MagicMock()
        ingester._client.news_sentiment.return_value = None
        assert ingester._fetch_one("EUR_USD", run_date) is None

    def test_list_response_returns_none(self, run_date):
        ingester = FinnhubSentimentIngester()
        ingester._api_key = "test_key"
        ingester._client = MagicMock()
        ingester._client.news_sentiment.return_value = []
        assert ingester._fetch_one("EUR_USD", run_date) is None

    def test_missing_buzz_defaults_to_zero(self, run_date):
        ingester = FinnhubSentimentIngester()
        ingester._api_key = "test_key"
        ingester._client = MagicMock()
        ingester._client.news_sentiment.return_value = {"companyNewsScore": 0.5}
        result = ingester._fetch_one("MSFT", run_date)
        assert result is not None
        assert result["buzz_score"] == pytest.approx(0.0)
        assert result["news_volume_7d"] == 0

    def test_none_values_default_to_zero(self, run_date):
        ingester = FinnhubSentimentIngester()
        ingester._api_key = "test_key"
        ingester._client = MagicMock()
        ingester._client.news_sentiment.return_value = {
            "companyNewsScore": None,
            "buzz": {"buzz": None, "articlesInLastWeek": None},
        }
        result = ingester._fetch_one("AAPL", run_date)
        assert result is not None
        assert result["sentiment_score"] == pytest.approx(0.0)
        assert result["buzz_score"] == pytest.approx(0.0)
        assert result["news_volume_7d"] == 0

    def test_source_always_finnhub(self, run_date, ingester_with_mock_client):
        result = ingester_with_mock_client._fetch_one("AAPL", run_date)
        assert result["source"] == "finnhub"


# ── Schema Validation Integration ─────────────────────────────────────────────

class TestSchemaValidationIntegration:

    def test_valid_df_passes_schema(self, run_date):
        if not Path(_SCHEMA_PATH).exists():
            pytest.skip(f"Schema not found: {_SCHEMA_PATH}")
        from src.bronze.schema_validator import SchemaValidator
        validator = SchemaValidator(Path(_SCHEMA_PATH))
        df = pl.DataFrame({
            "symbol":          ["AAPL", "MSFT"],
            "sentiment_score": [0.75, 0.3],
            "buzz_score":      [1.2, 0.8],
            "news_volume_7d":  [15, 5],
            "source":          ["finnhub", "finnhub"],
            "fetched_date":    [str(run_date), str(run_date)],
        })
        ok, errors = validator.validate(df, "test")
        assert ok, f"Valid DataFrame failed schema: {errors}"

    def test_wrong_type_fails_schema(self, run_date):
        if not Path(_SCHEMA_PATH).exists():
            pytest.skip(f"Schema not found: {_SCHEMA_PATH}")
        from src.bronze.schema_validator import SchemaValidator
        validator = SchemaValidator(Path(_SCHEMA_PATH))
        df = pl.DataFrame({
            "symbol":          ["AAPL"],
            "sentiment_score": [0.75],
            "buzz_score":      [1.2],
            "news_volume_7d":  [15.0],   # Float64, expected Int64
            "source":          ["finnhub"],
            "fetched_date":    [str(run_date)],
        })
        ok, errors = validator.validate(df, "test")
        assert not ok

    def test_missing_column_fails_schema(self, run_date):
        if not Path(_SCHEMA_PATH).exists():
            pytest.skip(f"Schema not found: {_SCHEMA_PATH}")
        from src.bronze.schema_validator import SchemaValidator
        validator = SchemaValidator(Path(_SCHEMA_PATH))
        df = pl.DataFrame({"symbol": ["AAPL"]})
        ok, errors = validator.validate(df, "test")
        assert not ok
        assert len(errors) > 0
