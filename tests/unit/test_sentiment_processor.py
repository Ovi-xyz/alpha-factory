"""
test_silver_sentiment_processor.py — Unit Tests: SentimentProcessor (post-refactor)

Tests validate:
    1. Layer separation invariants — zero API calls in Silver module (GD §17.7)
    2. No lateral Silver coupling — no ActiveSymbolsResolver reference
    3. _transform() — schema normalization, null filtering, Bronze metadata drop
    4. run() — Bronze→Silver flow, missing Bronze handling, idempotency
    5. Replay guarantee — Silver produces correct output with NO network access
    6. job_registry integration — correct depends_on and DAILY_SEQUENCE placement

Coverage target: ≥80% line coverage (IDD §10.3 / refactor plan §7.4)

NOTE on AST-based checks:
    Docstrings in sentiment_processor.py mention removed patterns for historical
    context. All layer-separation tests use ast.parse() to inspect only actual
    executable code, not string literals (docstrings/comments).
"""

from __future__ import annotations

import ast
import socket
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import src.silver.sentiment_processor as sentiment_module
from src.silver.sentiment_processor import SentimentProcessor


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def run_date() -> date:
    return date(2025, 1, 22)


@pytest.fixture
def bronze_sentiment_dir(tmp_path: Path, run_date: date) -> Path:
    """
    Bronze sentiment fixture in BronzeIngester.write() path structure:
        {root}/source=finnhub/symbol=sentiment_{date}/year={Y}/month={M}/*.parquet

    3 records:
        AAPL  — sentiment_score=0.75  (valid)
        MSFT  — sentiment_score=0.3   (valid)
        GOOGL — sentiment_score=None  (filtered by _transform)
    """
    bronze_root = tmp_path / "data" / "bronze" / "market" / "fundamental" / "sentiment"
    partition = (
        bronze_root
        / "source=finnhub"
        / f"symbol=sentiment_{run_date.isoformat()}"
        / f"year={run_date.year}"
        / f"month={run_date.month:02d}"
    )
    partition.mkdir(parents=True)

    df = pl.DataFrame({
        "symbol":          ["AAPL",    "MSFT",    "GOOGL"],
        "sentiment_score": [0.75,      0.3,       None],
        "buzz_score":      [1.2,       0.8,       0.5],
        "news_volume_7d":  [15,        5,         3],
        "source":          ["finnhub", "finnhub", "finnhub"],
        "fetched_date":    [str(run_date)] * 3,
        # Bronze metadata — must be dropped by _transform()
        "_source":         ["finnhub"] * 3,
        "_ingested_at":    ["2025-01-22T03:00:00"] * 3,
        "_symbol":         [f"sentiment_{run_date.isoformat()}"] * 3,
    })
    df.write_parquet(partition / "sentiment_2025-01-22_raw_030000.parquet")
    return bronze_root


@pytest.fixture(autouse=True)
def patch_paths(tmp_path: Path, bronze_sentiment_dir: Path):
    """Redirect module-level path constants to tmp_path for every test."""
    orig_bronze = sentiment_module.BRONZE_SENTIMENT_PATH
    orig_silver = sentiment_module.SILVER_SENTIMENT_PATH

    sentiment_module.BRONZE_SENTIMENT_PATH = bronze_sentiment_dir
    sentiment_module.SILVER_SENTIMENT_PATH = tmp_path / "data" / "silver" / "sentiment"

    yield

    sentiment_module.BRONZE_SENTIMENT_PATH = orig_bronze
    sentiment_module.SILVER_SENTIMENT_PATH  = orig_silver


def _code_only(source: str) -> str:
    """
    Strip docstring string-literals from source for accurate code inspection.
    Walks the AST to collect line ranges of module/class/function docstrings,
    then returns only the non-docstring lines.
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
    return "\n".join(
        line for i, line in enumerate(source.splitlines(), 1)
        if i not in docstring_lines
    )


# ── Layer Separation Invariants ───────────────────────────────────────────────

class TestLayerSeparationInvariants:
    """
    Architectural invariants per GD §17.7.
    Silver processor must have zero external API calls after refactor.
    All checks use AST-based inspection to avoid docstring false-positives.
    """

    def test_no_finnhub_import_in_actual_code(self):
        """Silver processor must NOT import finnhub in actual code (GD §17.7)."""
        import src.silver.sentiment_processor as module
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert "finnhub" not in alias.name, (
                        "Silver processor imports 'finnhub' — violates GD §17.7."
                    )
            elif isinstance(node, ast.ImportFrom):
                assert not (node.module and "finnhub" in node.module), (
                    f"Silver processor imports from 'finnhub' module: {node.module} — GD §17.7."
                )

    def test_no_finnhub_api_key_reference_in_code(self):
        """Silver processor must NOT reference FINNHUB_API_KEY in executable code."""
        import src.silver.sentiment_processor as module
        code = _code_only(Path(module.__file__).read_text())
        assert "FINNHUB_API_KEY" not in code, (
            "Silver processor references FINNHUB_API_KEY in executable code. "
            "Silver must not call external APIs."
        )

    def test_no_news_sentiment_call_in_code(self):
        """Silver processor must NOT call news_sentiment() in actual code."""
        import src.silver.sentiment_processor as module
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "news_sentiment":
                pytest.fail(
                    "Silver processor calls .news_sentiment() in actual code — "
                    "violates GD §17.7. Silver must not call external APIs."
                )

    def test_no_active_symbols_resolver_import(self):
        """Silver processor must NOT import ActiveSymbolsResolver — lateral coupling."""
        import src.silver.sentiment_processor as module
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    assert "ActiveSymbols" not in alias.name, (
                        "Silver processor imports ActiveSymbolsResolver — "
                        "lateral Silver coupling is an anti-pattern per GD §17.7."
                    )

    def test_no_active_symbols_module_import(self):
        """Silver processor must NOT import silver.active_symbols module."""
        import src.silver.sentiment_processor as module
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module and "active_symbols" in node.module), (
                    f"Silver imports from active_symbols module: {node.module}. "
                    "Lateral Silver coupling — violates GD §17.7."
                )

    def test_no_finnhub_client_instantiation_in_code(self):
        """Silver processor must NOT instantiate finnhub.Client() in code."""
        import src.silver.sentiment_processor as module
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            # Check for Call(func=Attribute(attr='Client')) with 'finnhub' context
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Attribute) and func.attr == "Client":
                    pytest.fail(
                        "Silver processor instantiates .Client() — possible Finnhub call. "
                        "Silver must not instantiate API clients."
                    )

    def test_reads_from_bronze_path_in_code(self):
        """Silver processor must reference BRONZE_SENTIMENT_PATH in executable code."""
        import src.silver.sentiment_processor as module
        code = _code_only(Path(module.__file__).read_text())
        assert "BRONZE_SENTIMENT_PATH" in code, (
            "Silver processor does not reference BRONZE_SENTIMENT_PATH in code. "
            "It must read from Bronze Parquet, not call any external API."
        )

    def test_syntax_valid(self):
        """Module must be syntactically valid Python."""
        import src.silver.sentiment_processor as module
        ast.parse(Path(module.__file__).read_text())


# ── _transform() Unit Tests ───────────────────────────────────────────────────

class TestTransform:

    def test_filters_null_sentiment_score(self, run_date):
        proc = SentimentProcessor()
        df = pl.DataFrame({
            "symbol":          ["AAPL", "MSFT", "GOOGL"],
            "sentiment_score": [0.75, None, 0.3],
            "buzz_score":      [1.2, 0.8, 0.5],
            "news_volume_7d":  [15, 5, 3],
            "source":          ["finnhub"] * 3,
            "fetched_date":    [str(run_date)] * 3,
        })
        result = proc._transform(df, run_date)
        assert len(result) == 2
        assert "MSFT" not in result["symbol"].to_list()

    def test_drops_bronze_metadata_columns(self, run_date):
        proc = SentimentProcessor()
        df = pl.DataFrame({
            "symbol":          ["AAPL"],
            "sentiment_score": [0.75],
            "buzz_score":      [1.2],
            "news_volume_7d":  [15],
            "source":          ["finnhub"],
            "fetched_date":    [str(run_date)],
            "_source":         ["finnhub"],
            "_ingested_at":    ["2025-01-22T03:00:00"],
            "_symbol":         ["sentiment_2025-01-22"],
        })
        result = proc._transform(df, run_date)
        for col in ("_source", "_ingested_at", "_symbol"):
            assert col not in result.columns, f"Bronze metadata column '{col}' not dropped."

    def test_exact_silver_schema_columns(self, run_date):
        proc = SentimentProcessor()
        df = pl.DataFrame({
            "symbol":          ["AAPL"],
            "sentiment_score": [0.75],
            "buzz_score":      [1.2],
            "news_volume_7d":  [15],
            "source":          ["finnhub"],
            "fetched_date":    [str(run_date)],
        })
        result = proc._transform(df, run_date)
        expected = {"symbol", "date", "sentiment_score", "buzz_score",
                    "news_volume_7d", "source"}
        assert set(result.columns) == expected, (
            f"Silver schema mismatch. Expected: {expected}, got: {set(result.columns)}"
        )

    def test_date_column_set_to_run_date(self, run_date):
        proc = SentimentProcessor()
        df = pl.DataFrame({
            "symbol":          ["AAPL"],
            "sentiment_score": [0.75],
            "buzz_score":      [1.2],
            "news_volume_7d":  [15],
            "source":          ["finnhub"],
            "fetched_date":    ["2025-01-01"],   # different from run_date — must be ignored
        })
        result = proc._transform(df, run_date)
        assert result["date"][0] == str(run_date)

    def test_exact_column_dtypes(self, run_date):
        proc = SentimentProcessor()
        df = pl.DataFrame({
            "symbol":          ["AAPL"],
            "sentiment_score": [0.75],
            "buzz_score":      [1.2],
            "news_volume_7d":  [15],
            "source":          ["finnhub"],
            "fetched_date":    [str(run_date)],
        })
        result = proc._transform(df, run_date)
        assert result["sentiment_score"].dtype == pl.Float64
        assert result["buzz_score"].dtype == pl.Float64
        assert result["news_volume_7d"].dtype == pl.Int64
        assert result["symbol"].dtype == pl.String
        assert result["source"].dtype == pl.String
        assert result["date"].dtype == pl.String

    def test_missing_required_columns_returns_empty_schema(self, run_date):
        proc = SentimentProcessor()
        df = pl.DataFrame({"symbol": ["AAPL"]})
        result = proc._transform(df, run_date)
        assert result.is_empty()
        assert set(result.columns) == {
            "symbol", "date", "sentiment_score", "buzz_score", "news_volume_7d", "source"
        }

    def test_all_null_sentiment_returns_empty(self, run_date):
        proc = SentimentProcessor()
        df = pl.DataFrame({
            "symbol":          ["AAPL", "MSFT"],
            "sentiment_score": [None, None],
            "buzz_score":      [1.2, 0.8],
            "news_volume_7d":  [15, 5],
            "source":          ["finnhub", "finnhub"],
            "fetched_date":    [str(run_date), str(run_date)],
        })
        result = proc._transform(df, run_date)
        assert len(result) == 0


# ── run() Integration Tests ───────────────────────────────────────────────────

class TestRun:

    def test_writes_silver_parquet(self, run_date):
        SentimentProcessor().run(run_date)
        silver_file = (
            sentiment_module.SILVER_SENTIMENT_PATH
            / f"date={run_date.isoformat()}"
            / "sentiment_silver.parquet"
        )
        assert silver_file.exists(), f"Silver Parquet not written: {silver_file}"

    def test_filters_null_sentiment_in_output(self, run_date):
        SentimentProcessor().run(run_date)
        silver_file = (
            sentiment_module.SILVER_SENTIMENT_PATH
            / f"date={run_date.isoformat()}"
            / "sentiment_silver.parquet"
        )
        df = pl.read_parquet(silver_file)
        assert len(df) == 2              # GOOGL (null) filtered out
        assert "GOOGL" not in df["symbol"].to_list()

    def test_correct_silver_schema_columns(self, run_date):
        SentimentProcessor().run(run_date)
        silver_file = (
            sentiment_module.SILVER_SENTIMENT_PATH
            / f"date={run_date.isoformat()}"
            / "sentiment_silver.parquet"
        )
        df = pl.read_parquet(silver_file)
        expected = {"symbol", "date", "sentiment_score", "buzz_score",
                    "news_volume_7d", "source"}
        assert set(df.columns) == expected

    def test_date_column_correct_value(self, run_date):
        SentimentProcessor().run(run_date)
        silver_file = (
            sentiment_module.SILVER_SENTIMENT_PATH
            / f"date={run_date.isoformat()}"
            / "sentiment_silver.parquet"
        )
        df = pl.read_parquet(silver_file)
        assert all(d == str(run_date) for d in df["date"].to_list())

    def test_idempotent_no_row_duplication(self, run_date):
        """Running twice must not duplicate rows — _write() must overwrite."""
        proc = SentimentProcessor()
        proc.run(run_date)
        proc.run(run_date)
        silver_file = (
            sentiment_module.SILVER_SENTIMENT_PATH
            / f"date={run_date.isoformat()}"
            / "sentiment_silver.parquet"
        )
        df = pl.read_parquet(silver_file)
        assert len(df) == 2, (
            f"Expected 2 rows, got {len(df)} — possible duplication from re-run."
        )

    def test_missing_bronze_path_returns_early(self, run_date, tmp_path):
        sentiment_module.BRONZE_SENTIMENT_PATH = tmp_path / "nonexistent"
        SentimentProcessor().run(run_date)   # Must NOT raise
        silver_dir = (
            sentiment_module.SILVER_SENTIMENT_PATH
            / f"date={run_date.isoformat()}"
        )
        assert not silver_dir.exists()

    def test_no_bronze_data_for_date_returns_early(self, run_date):
        different_date = date(2025, 12, 31)
        SentimentProcessor().run(different_date)  # Must NOT raise
        silver_dir = (
            sentiment_module.SILVER_SENTIMENT_PATH
            / f"date={different_date.isoformat()}"
        )
        assert not silver_dir.exists()

    def test_replay_guarantee_zero_network_access(self, run_date):
        """
        REPLAY GUARANTEE (core architectural invariant):
        Silver must produce correct output from Bronze snapshot with NO network access.

        If this test fails, Silver is still making network calls —
        the GD §17.7 debt has not been fully eliminated.
        """
        original_socket_init = socket.socket.__init__

        class BlockedSocket:
            def __init__(self, *args, **kwargs):
                raise OSError(
                    "NETWORK BLOCKED: Silver sentiment attempted network access. "
                    "Silver must read only from Bronze Parquet — GD §17.7 debt present."
                )

        original_class = socket.socket
        socket.socket  = BlockedSocket      # type: ignore[misc]
        try:
            SentimentProcessor().run(run_date)
            silver_file = (
                sentiment_module.SILVER_SENTIMENT_PATH
                / f"date={run_date.isoformat()}"
                / "sentiment_silver.parquet"
            )
            assert silver_file.exists(), (
                "Silver Parquet must be produced from Bronze snapshot alone (no network)."
            )
        finally:
            socket.socket = original_class


# ── job_registry Integration Tests ───────────────────────────────────────────

class TestJobRegistryIntegration:

    def test_bronze_finnhub_sentiment_registered(self):
        from src.scheduler.job_registry import JOB_REGISTRY
        assert "bronze_finnhub_sentiment" in JOB_REGISTRY

    def test_bronze_finnhub_sentiment_depends_on_empty(self):
        """depends_on=[] is KRITIS — Bronze must not read Silver output."""
        from src.scheduler.job_registry import JOB_REGISTRY
        job = JOB_REGISTRY["bronze_finnhub_sentiment"]
        assert job["depends_on"] == [], (
            f"bronze_finnhub_sentiment.depends_on must be [], got {job['depends_on']}. "
            "KRITIS: Bronze must not depend on Silver output (refactor plan §3.2)."
        )

    def test_bronze_finnhub_sentiment_is_bronze_layer(self):
        from src.scheduler.job_registry import JOB_REGISTRY
        assert JOB_REGISTRY["bronze_finnhub_sentiment"]["layer"] == "bronze"

    def test_silver_sentiment_depends_on_bronze_not_silver(self):
        from src.scheduler.job_registry import JOB_REGISTRY
        deps = JOB_REGISTRY["silver_sentiment"]["depends_on"]
        assert "bronze_finnhub_sentiment" in deps, (
            f"silver_sentiment must depend on bronze_finnhub_sentiment, got {deps}."
        )
        assert "silver_active_symbols" not in deps, (
            f"silver_sentiment must NOT depend on silver_active_symbols. Got {deps}."
        )

    def test_silver_active_symbols_still_present(self):
        """silver_active_symbols must NOT be removed — still needed by Gold layer."""
        from src.scheduler.job_registry import JOB_REGISTRY
        assert "silver_active_symbols" in JOB_REGISTRY

    def test_daily_sequence_has_bronze_sentiment(self):
        from src.scheduler.job_registry import DAILY_SEQUENCE
        assert "bronze_finnhub_sentiment" in DAILY_SEQUENCE

    def test_daily_sequence_has_silver_sentiment_re_enabled(self):
        """silver_sentiment must be re-enabled — was disabled in v1.5 due to debt."""
        from src.scheduler.job_registry import DAILY_SEQUENCE
        assert "silver_sentiment" in DAILY_SEQUENCE, (
            "silver_sentiment still commented out. After refactor it depends on "
            "bronze_finnhub_sentiment (implemented) — re-enable in DAILY_SEQUENCE."
        )

    def test_daily_sequence_bronze_precedes_silver_sentiment(self):
        from src.scheduler.job_registry import DAILY_SEQUENCE
        if "silver_sentiment" not in DAILY_SEQUENCE:
            pytest.skip("silver_sentiment not in DAILY_SEQUENCE")
        bi = DAILY_SEQUENCE.index("bronze_finnhub_sentiment")
        si = DAILY_SEQUENCE.index("silver_sentiment")
        assert bi < si, (
            f"bronze_finnhub_sentiment (idx={bi}) must precede silver_sentiment (idx={si})."
        )
