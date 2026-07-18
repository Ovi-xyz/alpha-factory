"""
active_symbols.py — Silver Layer — ActiveSymbolsResolver
Precision Audit v1.7.1 — 12 findings resolved (AS-1..AS-12)
GMI Wave 1 — Architecture v2.0 §4: Dual-Layer Active Universe

Dokumen referensi: precision_audit_active_symbols.docx (Juni 2026)
                   alpha_factory_architecture_v2.docx §4 (Juni 2026)
Hierarki: Grand Design v1.2 > Supplementary Design v1.1 > IDD v1.0 >
          Architecture v2.0 > Architecture Extension v1.0 > Audit

Fixes applied (AS-1..AS-12, unchanged from v1.7.x — preserved verbatim):
  AS-1  [P0]: DuckDB $name parameter (was :name — silent NULL risk)
  AS-2  [P0]: Fail-fast on query error; fallback only when Silver not ready
  AS-3  [P1]: ROW_NUMBER 20 trading day exact window (was 45-calendar AVG)
  AS-4  [P1]: is_clean filter at ohlcv CTE source (dirty rows excluded everywhere)
  AS-5  [P1]: UNION policy — always-in never truncated by LIMIT
  AS-6  [P2]: Rich output schema with metrics and eligibility_reason
  AS-7  [P2]: DuckDB context manager — no resource leak
  AS-8  [P2]: Paths / memory / threads from get_config() and env var
  AS-9  [P2]: Atomic write via temp file + rename
  AS-10 [NEW]: Unknown market NULL logged and filtered explicitly
  AS-11 [NEW]: hive_partitioning=False (Supp. Design G2 convention)
  AS-12 [NEW]: Query from audit Section 8 (sketch Section 7 discarded)

# ADD GMI-AS-001 — Dual-Layer Architecture (Architecture v2.0 §4.1-§4.2):
  Grand Design's single active_symbols list conflated two incompatible
  consumers: gold_signals (needs tradeable OHLCV only) vs CrossAssetEngine
  (needs always-on macro context anchors with no dollar_volume concept).
  Resolution: split into two independent artifacts. Originally (GMI-AS-001)
  both were resolved by this same class/module; MOVED GMI-CTX-001 (GMI
  Wave 1 Bronze/Silver Solidification) extracted Layer 2 entirely into
  src/silver/context_anchors.py (ContextAnchorsResolver) — see that
  module's docstring for the full separation-of-concerns rationale. This
  module is now Layer 1 only.

  IMPORTANT — _SCREENED_LIMIT=175 and _RESOLVE_QUERY are preserved
  EXACTLY as audited (AS-1..AS-12), UNCHANGED by the GMI-CTX-001 extraction
  above (verified: the only lines removed were resolve_context()/
  load_context()/load_context_full() and one call site in run() — zero
  lines inside resolve()/_run_query()/_RESOLVE_QUERY were touched).
  No per-market cap split was introduced here: Architecture v2.0 §4.3's
  "~165 US / 12 IDX" figures are achieved naturally once instruments.yaml
  v1.4 removes DXY from forex (-1) and SPX/VIX from index (-2) — the
  existing combined screened-pool LIMIT=175 plus now-19-pair forex
  always-in plus 3-commodity always-in lands at ≤197 total, within the
  documented "~190" tolerance band, with ZERO changes to already-tested
  query logic. Splitting into independent per-market LIMITs was considered
  and REJECTED for this cycle: it would invalidate test_screened_limit_is_175
  / test_screened_limited_to_175 without a correctness defect driving the
  change (audit principle: don't touch tested logic without a concrete
  bug). Tracked as a KNOWN_RISKS.md follow-up if real liquidity data later
  shows IDX/US cross-currency dollar_volume comparison inside one ORDER BY
  is material.

Output:
  data/silver/active_symbols/active_{date}.parquet          (legacy, kept)
  data/silver/active_symbols/active_ohlcv_{date}.parquet     (Layer 1 canonical)

  Layer 2 output (data/silver/context_anchors/context_anchors_{date}.parquet)
  moved to src/silver/context_anchors.py — see that module, not this one.

Dikonsumsi oleh: gold_signals, silver_sentiment, gold_correlation,
                 signal_aggregation, gold_global_regime
"""

from __future__ import annotations

import os
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from loguru import logger

from src.config.instrument_loader import get_loader
from src.config.pipeline_config import get_config


# ── Resolver version — bump when logic changes (AS-6) ────────────────────────
RESOLVER_VERSION: str = "1.1"


# ── Screening Thresholds per Market ──────────────────────────────────────────
# All values used via DuckDB $name param binding — never hardcoded in SQL.
# Consistent with Supplementary Design v1.1 G2.

THRESHOLDS: dict[str, dict[str, float | int]] = {
    "us_stocks": {
        "dollar_volume_20d": 10_000_000,     # USD 10M/day minimum liquidity
        "price_floor":       1.0,            # Exclude penny stocks
        "min_days":          20,             # AS-3: 20 trading days (not 30)
    },
    "idx": {
        "dollar_volume_20d": 5_000_000_000,  # IDR 5B/day — IDX liquidity unit
        "price_floor":       50.0,
        "min_days":          20,             # AS-3: 20 trading days
    },
    # AS-5: always-in markets — thresholds here for documentation only;
    # enforced via UNION policy, not WHERE clause.
    "forex":     {"dollar_volume_20d": 0, "price_floor": 0.0, "min_days": 0},
    "commodity": {"dollar_volume_20d": 0, "price_floor": 0.0, "min_days": 0},
    "index":     {"dollar_volume_20d": 0, "price_floor": 0.0, "min_days": 0},
}

# ── Screened market LIMIT (headroom for 25 always-in instruments) ─────────────
# 175 screened + ≤25 always-in ≈ 200 total (AS-5)
_SCREENED_LIMIT: int = 175
_ALWAYS_IN_MARKETS: tuple[str, ...] = ("forex", "commodity", "index")

# ── Final SQL query — from audit Section 8 (Section 7 discarded as DRAFT) ────
# AS-1: all params use $name format (DuckDB official)
# AS-3: ROW_NUMBER for exact 20 trading days
# AS-4: is_clean filter at ohlcv CTE (affects ALL downstream CTEs)
# AS-5: UNION policy — always_in never hits LIMIT
# AS-10: m.market IS NOT NULL guard (unknown market filtered before eligible check)
# AS-11: hive_partitioning=false (Supp. Design G2 convention)
_RESOLVE_QUERY = """
WITH ohlcv AS (
    -- AS-1: $name placeholders  |  AS-11: hive_partitioning=false
    -- AS-4: is_clean=TRUE at source — affects ALL downstream CTEs
    SELECT
        s.symbol,
        m.market,
        s.close * s.volume          AS dollar_volume,
        s.close                     AS last_close,
        CAST(s.timestamp AS DATE)   AS ts_date
    FROM read_parquet($path, hive_partitioning=false) s
    LEFT JOIN market_lookup m ON s.symbol = m.symbol
    -- AS-10: market NULL guard applied before aggregation
    WHERE CAST(s.timestamp AS DATE) BETWEEN $run_date - INTERVAL 45 DAYS AND $run_date
      AND m.market IS NOT NULL
      AND s.is_clean = TRUE
),

ranked_clean AS (
    -- AS-3: ROW_NUMBER for exact 20 most-recent trading days per symbol
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts_date DESC) AS rn
    FROM ohlcv
),

agg AS (
    SELECT
        symbol,
        market,
        AVG(dollar_volume) FILTER (WHERE rn <= 20)   AS dollar_volume_20d,
        COUNT(*)                                      AS clean_days,
        FIRST(last_close ORDER BY ts_date DESC)       AS last_close
    FROM ranked_clean
    GROUP BY symbol, market
),

-- AS-5: always-in markets — no threshold, no LIMIT
always_in AS (
    SELECT
        symbol, market, dollar_volume_20d, clean_days, last_close,
        'always_in' AS eligibility_reason
    FROM agg
    WHERE market IN ('forex', 'commodity', 'index')
),

-- AS-5: screened — threshold per market, ORDER BY, LIMIT only on this CTE
screened AS (
    SELECT
        symbol, market, dollar_volume_20d, clean_days, last_close,
        'liquidity_screened' AS eligibility_reason
    FROM agg
    WHERE (market = 'us_stocks'
           AND dollar_volume_20d > $us_dvol
           AND last_close        >= $us_price
           AND clean_days        >= $us_days)
       OR (market = 'idx'
           AND dollar_volume_20d > $idx_dvol
           AND last_close        >= $idx_price
           AND clean_days        >= $idx_days)
    ORDER BY dollar_volume_20d DESC
    LIMIT $screened_limit
)

-- Final UNION: always-in first, then screened
SELECT symbol, market, dollar_volume_20d, clean_days, last_close, eligibility_reason
FROM always_in
UNION ALL
SELECT symbol, market, dollar_volume_20d, clean_days, last_close, eligibility_reason
FROM screened
"""


class ActiveSymbolsResolver:
    """
    Resolve active trading symbols dari Silver OHLCV via dollar_volume_20d.

    Fixes circular dependency: screener memerlukan active_symbols, tapi
    active_symbols ditentukan dari Silver OHLCV data — bukan screener output.

    All 12 audit findings (AS-1..AS-12) resolved in this implementation.
    Always-in guarantee: forex/commodity/index NEVER truncated by LIMIT (AS-5).
    Fail-fast guarantee: query errors propagate to runner — no silent failures (AS-2).

    Output schema (AS-6):
        symbol, market, dollar_volume_20d, clean_days, last_close,
        eligibility_reason, resolved_date, resolver_version, unknown_market_count
    """

    # AS-8: output path derived from config (not hardcoded)
    @property
    def OUTPUT_PATH(self) -> Path:
        cfg = get_config()
        data_root = Path(os.getenv("PIPELINE_DATA_ROOT", str(cfg.silver_path.parent)))
        return data_root / "silver" / "active_symbols"

    def resolve(
        self,
        silver_1d_path: str,
        run_date: date,
    ) -> list[str]:
        """
        Resolve active symbols untuk run_date.

        AS-2: Two semantically distinct failure modes are handled separately:
          (1) Silver data not ready   → legitimate fallback, returns full universe
          (2) Query/runtime error     → fail-fast, exception propagates to runner

        Args:
            silver_1d_path: Glob pattern to Silver 1D Parquet files
            run_date:       Pipeline run date (reproducibility — no CURRENT_DATE)

        Returns:
            List of normalized symbol strings in the active universe.

        Raises:
            RuntimeError: if query fails after Silver data is confirmed available
        """
        # Step 1: build market lookup table from InstrumentLoader (AS-10 ready)
        loader  = get_loader()
        mkt_map = loader.market_map()
        mkt_df  = pl.DataFrame({
            "symbol": list(mkt_map.keys()),
            "market": list(mkt_map.values()),
        })

        # Step 2: AS-2 — check Silver availability BEFORE opening connection
        data_available = self._check_silver_available(silver_1d_path)
        if not data_available:
            logger.warning(
                "[ActiveSymbols] Silver 1D not ready — fallback to full universe"
            )
            all_syms = loader.symbol_list()
            self._save_fallback(all_syms, run_date)
            return all_syms

        # Step 3: run query — errors propagate (no except catch-all)
        result_df = self._run_query(mkt_df, silver_1d_path, run_date)

        # Step 4: AS-10 — detect and log symbols with unknown market
        unknown_count = self._audit_unknown_markets(
            silver_1d_path, run_date, mkt_map
        )

        # Step 5: persist rich output schema (AS-6)
        self._save(result_df, run_date, unknown_count)

        symbols = result_df["symbol"].to_list()
        always_in_count  = result_df.filter(
            pl.col("eligibility_reason") == "always_in"
        ).height
        screened_count = result_df.filter(
            pl.col("eligibility_reason") == "liquidity_screened"
        ).height
        logger.info(
            f"[ActiveSymbols] {len(symbols)} symbols resolved for {run_date} "
            f"(always_in={always_in_count}, screened={screened_count}, "
            f"unknown_market={unknown_count})"
        )
        return symbols

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _check_silver_available(self, silver_1d_path: str) -> bool:
        """AS-2: probe Silver availability without masking real query errors."""
        try:
            probe = pl.scan_parquet(silver_1d_path).limit(1).collect()
            return len(probe) > 0
        except Exception:
            return False

    def _run_query(
        self,
        mkt_df: pl.DataFrame,
        silver_1d_path: str,
        run_date: date,
    ) -> pl.DataFrame:
        """
        Execute the audit Section 8 query.
        AS-1: all params use $name (DuckDB official format).
        AS-7: DuckDB context manager — connection closed on exit.
        AS-8: memory_limit and threads from get_config().
        """
        cfg = get_config()

        # AS-1: smoke test — verify $name substitution is active
        with duckdb.connect() as probe_con:
            row = probe_con.execute("SELECT $v AS x", {"v": 42}).fetchone()
            if row is None or row[0] != 42:
                raise RuntimeError(
                    f"[ActiveSymbols] DuckDB $name param broken — got {row}. "
                    "Upgrade duckdb or check Python DB API version."
                )

        # AS-7: context manager ensures connection is closed
        with duckdb.connect() as con:
            # AS-8: config-driven memory and threads
            con.execute(f"SET memory_limit='{cfg.duckdb_memory_limit_gb}GB'")
            con.execute(f"SET threads={cfg.duckdb_threads}")
            con.register("market_lookup", mkt_df.to_arrow())

            # AS-2: no except — errors propagate to caller
            result = con.execute(
                _RESOLVE_QUERY,
                {
                    "path":          silver_1d_path,
                    "run_date":      run_date,
                    "us_dvol":       THRESHOLDS["us_stocks"]["dollar_volume_20d"],
                    "us_price":      THRESHOLDS["us_stocks"]["price_floor"],
                    "us_days":       THRESHOLDS["us_stocks"]["min_days"],
                    "idx_dvol":      THRESHOLDS["idx"]["dollar_volume_20d"],
                    "idx_price":     THRESHOLDS["idx"]["price_floor"],
                    "idx_days":      THRESHOLDS["idx"]["min_days"],
                    "screened_limit": _SCREENED_LIMIT,
                },
            ).pl()

        return result

    def _audit_unknown_markets(
        self,
        silver_1d_path: str,
        run_date: date,
        mkt_map: dict[str, str],
    ) -> int:
        """
        AS-10: detect Silver symbols with no InstrumentLoader mapping.
        Logs WARNING with list of orphan symbols.
        Returns count for inclusion in output metadata.

        FIX F-AS-01 [P2]: replaced eager pl.read_parquet() with lazy
        pl.scan_parquet(...).select('symbol').unique().collect().
        BEFORE: pl.read_parquet(silver_1d_path).select('symbol').unique()
                loads ENTIRE Silver 1D dataset into RAM — O(full_data) memory.
                For 643 symbols × 10Y = potentially several GB on M1 8GB.
        AFTER:  scan_parquet reads only the 'symbol' column (columnar pushdown),
                .unique() deduplicates in streaming fashion, .collect() materialises
                only the small symbol set — O(n_distinct_symbols) RAM usage.
        hive_partitioning=False: consistent with AS-11 and _run_query() convention.
        """
        try:
            # FIX F-AS-01 [P2]: lazy scan — only 'symbol' column loaded into RAM
            silver_syms_df = (
                pl.scan_parquet(silver_1d_path, hive_partitioning=False)
                .select("symbol")
                .unique()
                .collect()
            )
            silver_syms = set(silver_syms_df["symbol"].to_list())
            known_syms  = set(mkt_map.keys())
            orphans     = sorted(silver_syms - known_syms)
            if orphans:
                logger.warning(
                    f"[ActiveSymbols] {len(orphans)} unknown-market symbols excluded "
                    f"(not in instruments.yaml): {orphans[:10]}"
                    + (" ..." if len(orphans) > 10 else "")
                )
            return len(orphans)
        except Exception as exc:
            logger.debug(f"[ActiveSymbols] Unknown market audit skipped: {exc}")
            return 0

    def _save(
        self,
        result_df: pl.DataFrame,
        run_date: date,
        unknown_market_count: int,
    ) -> None:
        """
        AS-6: persist rich output schema.
        AS-9: atomic write via temp file + rename (no partial Parquet risk).

        # ADD GMI-AS-001: also persist to the Architecture v2.0 §4.2 canonical
        # filename active_ohlcv_{date}.parquet (Layer 1). Legacy active_{date}.parquet
        # is preserved byte-for-byte for backward compatibility with load()/load_full()
        # and any external consumer still reading the pre-GMI path.
        """
        output_path = self.OUTPUT_PATH
        output_path.mkdir(parents=True, exist_ok=True)

        out_df = result_df.with_columns([
            pl.lit(str(run_date)).alias("resolved_date"),
            pl.lit(RESOLVER_VERSION).alias("resolver_version"),
            pl.lit(unknown_market_count).cast(pl.Int32).alias("unknown_market_count"),
            pl.lit(False).alias("is_fallback"),
        ])

        for fname in (
            f"active_{run_date.isoformat()}.parquet",          # legacy
            f"active_ohlcv_{run_date.isoformat()}.parquet",    # GMI-AS-001 canonical
        ):
            final = output_path / fname
            with tempfile.NamedTemporaryFile(
                dir=output_path, suffix=".parquet.tmp", delete=False
            ) as tmp:
                tmp_path = Path(tmp.name)
            try:
                out_df.write_parquet(
                    tmp_path, compression="zstd", compression_level=3
                )
                os.replace(tmp_path, final)  # FIX SIL-AIO-003: os.replace is POSIX-atomic
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise

    def _save_fallback(self, symbols: list[str], run_date: date) -> None:
        """
        AS-2: fallback save — Silver not ready, full universe, flagged is_fallback=True.
        AS-9: also uses atomic write.
        # ADD GMI-AS-001: mirrors to active_ohlcv_{date}.parquet (canonical path).
        """
        output_path = self.OUTPUT_PATH
        output_path.mkdir(parents=True, exist_ok=True)

        out_df = pl.DataFrame({
            "symbol":               symbols,
            "market":               [None] * len(symbols),
            "dollar_volume_20d":    [None] * len(symbols),
            "clean_days":           [None] * len(symbols),
            "last_close":           [None] * len(symbols),
            "eligibility_reason":   ["fallback_full_universe"] * len(symbols),
            "resolved_date":        [str(run_date)] * len(symbols),
            "resolver_version":     [RESOLVER_VERSION] * len(symbols),
            "unknown_market_count": [0] * len(symbols),
            "is_fallback":          [True] * len(symbols),
        })

        for fname in (
            f"active_{run_date.isoformat()}.parquet",
            f"active_ohlcv_{run_date.isoformat()}.parquet",
        ):
            final = output_path / fname
            with tempfile.NamedTemporaryFile(
                dir=output_path, suffix=".parquet.tmp", delete=False
            ) as tmp:
                tmp_path = Path(tmp.name)
            try:
                out_df.write_parquet(tmp_path, compression="zstd", compression_level=3)
                os.replace(tmp_path, final)
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise

    def load(self, run_date: date) -> list[str]:
        """
        Load previously resolved symbols for run_date.
        Raises FileNotFoundError if not yet resolved.
        """
        path = self.OUTPUT_PATH / f"active_{run_date.isoformat()}.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"Active symbols not resolved for {run_date}. "
                "Run silver_active_symbols job first."
            )
        # FIX SIL-RPQ-001: scan_parquet for lazy API consistency (single small file)
        return pl.scan_parquet(str(path)).collect()["symbol"].to_list()

    def load_full(self, run_date: date) -> pl.DataFrame:
        """
        Load full resolved DataFrame including all audit columns (AS-6).
        Useful for diagnostic queries and debugging universe changes.
        """
        path = self.OUTPUT_PATH / f"active_{run_date.isoformat()}.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"Active symbols not resolved for {run_date}. "
                "Run silver_active_symbols job first."
            )
        # FIX SIL-RPQ-001: scan_parquet for lazy API consistency
        return pl.scan_parquet(str(path)).collect()

    # ── ADD GMI-AS-001 — Layer 1 canonical loader (Architecture v2.0 §5.2) ──────

    def load_ohlcv(self, run_date: date) -> list[str]:
        """
        Load Layer 1 active_ohlcv symbols — Architecture v2.0 §5.2 canonical API:
            active = resolver.load_ohlcv(run_date)
        Reads active_ohlcv_{date}.parquet (identical content to load(), new name).
        """
        path = self.OUTPUT_PATH / f"active_ohlcv_{run_date.isoformat()}.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"active_ohlcv not resolved for {run_date}. "
                "Run silver_active_symbols job first."
            )
        return pl.scan_parquet(str(path)).collect()["symbol"].to_list()


def run(run_date: date) -> None:
    """
    Job entry point — called by job_registry.py.
    Silver 1D glob path is pipeline-standard pattern.

    MOVED GMI-CTX-001: this function previously also called
    resolver.resolve_context(run_date) (Layer 2) after Layer 1's resolve().
    Layer 2 resolution now lives entirely in src/silver/context_anchors.py
    (ContextAnchorsResolver.resolve(), module-level run()), wired as its own
    independent 'silver_context_anchors' job in job_registry.py — see that
    module's docstring for the full rationale (Separation of Concerns: Layer
    2 has zero Silver dependency and was never architecturally coupled to
    Layer 1's resolve() beyond having been bundled in the same Python
    function). This function is now Layer 1 only, matching what this
    module's name and class actually describe.
    """
    cfg = get_config()
    data_root = Path(os.getenv("PIPELINE_DATA_ROOT", str(cfg.silver_path.parent)))
    silver_1d_path = str(
        data_root / "silver" / "market_ohlcv" / "**" / "*_1D_silver.parquet"
    )
    resolver = ActiveSymbolsResolver()
    resolver.resolve(silver_1d_path=silver_1d_path, run_date=run_date)
