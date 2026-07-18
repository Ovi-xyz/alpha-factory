"""
context_anchors.py — Silver Layer — ContextAnchorsResolver
GMI Wave 1 — Bronze/Silver Solidification (pre-Cycle 4)

MOVED GMI-CTX-001: extracted from active_symbols.py's resolve_context() /
load_context() / load_context_full() (previously GMI-AS-001, Architecture
v2.0 §4.4). ActiveSymbolsResolver was doing two structurally unrelated
jobs in one class: Layer 1's liquidity-screened DuckDB query (the audited
AS-1..AS-12 logic, protected by KNOWN_RISKS.md / checkpoint invariant —
"do not touch without a concrete correctness defect") and Layer 2's
config-driven, zero-Silver-query metadata passthrough. Bundling them meant
every edit to one layer's logic carried non-zero risk of an accidental
diff bleeding into the other's protected code, and the class/file name
("ActiveSymbolsResolver" / "active_symbols.py") only accurately describes
what Layer 1 does — Layer 2 is not "active" in the liquidity sense at all
(Architecture v2.0 Table §4.2: "Filter: None").

This module is the single, independent home for Layer 2 (always-on
context anchor) resolution. It has NO Silver dependency — composition is
entirely config-driven from instruments.yaml via InstrumentLoader — and
therefore cannot fail, block, or be blocked by anything in the Layer 1
OHLCV pipeline (GD §17.2 Layer Independence, applied here at the
within-layer module level, not just across Bronze/Silver/Gold).

Blast-radius check performed before this extraction (empirical, not
assumed): resolve_context() / load_context() / load_context_full() had
ZERO callers anywhere in src/ outside active_symbols.py itself. The only
other references were active_symbols.py's own test file. This made the
split a clean break rather than a compatibility-shimmed migration.

Output:
    data/silver/context_anchors/context_anchors_{date}.parquet
    (NEW path — previously data/silver/active_symbols/active_context_{date}.parquet
    under the Layer 1 resolver's directory. No external consumer of the old
    path was found (see blast-radius note above), so the path was corrected
    rather than kept as legacy alongside the new one — unlike Layer 1's
    active_{date}.parquet -> active_ohlcv_{date}.parquet rename, which DID
    keep the legacy file because it has real consumers.)

Consumed by: CrossAssetEngine (Layer 2, Cycle 4 — not yet built).
Job registry: 'silver_context_anchors', depends_on=[] (see docstring on
run() below for why zero dependency is the technically honest choice,
not a shortcut).
"""

from __future__ import annotations

import os
import tempfile
from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from src.config.instrument_loader import get_loader
from src.config.pipeline_config import get_config

# ── Resolver version — bump when output schema/logic changes ─────────────────
# Independent of active_symbols.py's RESOLVER_VERSION — the two modules now
# version independently, matching their independent release/change cadence.
RESOLVER_VERSION: str = "1.0"


class ContextAnchorsResolver:
    """
    Resolve Layer 2 active_context — always-on macro/cross-asset anchors,
    NO liquidity filter (Architecture v2.0 Table §4.2: "Filter: None").

    Composition is config-driven from instruments.yaml `context.*` section
    via InstrumentLoader.all_context(include_deferred=False), NOT a Silver
    DuckDB query — there is no dollar_volume concept for treasury/CB-rate
    anchors, and global indices/ETFs are intentionally exempt from the
    trading liquidity gate (GD §0.2: pipeline produces context, not trade
    decisions).

    Deferred instruments (context_available=False: TIN, CPO, RUBBER) are
    excluded from resolve()/load() — Architecture Extension v1.0 ADR-007.
    Use loader.all_context(include_deferred=True) directly (bypassing this
    resolver) if deferred visibility is needed for audit/health-reporter
    purposes.

    Output schema:
        symbol, context_category, context_group, layer,
        include_in_forecast, reliability_flag, proxy_for,
        resolved_date, resolver_version
    """

    # Mirrors ActiveSymbolsResolver.OUTPUT_PATH's AS-8 convention: derived
    # from get_config() + PIPELINE_DATA_ROOT override, never hardcoded.
    @property
    def OUTPUT_PATH(self) -> Path:
        cfg = get_config()
        data_root = Path(os.getenv("PIPELINE_DATA_ROOT", str(cfg.silver_path.parent)))
        return data_root / "silver" / "context_anchors"

    def resolve(self, run_date: date) -> list[str]:
        """
        Resolve Layer 2 context anchors for run_date and persist to Parquet.

        No silver_1d_path argument (unlike ActiveSymbolsResolver.resolve()):
        this method never touches Silver OHLCV. It is pure InstrumentLoader
        enumeration, so it cannot raise on missing/stale Silver data and has
        no fallback-vs-fail-fast distinction to make (AS-2 does not apply
        here — there is nothing to fall back FROM).

        Returns:
            List of normalized Layer 2 symbol strings (49 active, deferred
            excluded).
        """
        loader = get_loader()
        instruments = loader.all_context(include_deferred=False)

        out_df = pl.DataFrame({
            "symbol":              [i.symbol for i in instruments],
            "context_category":    [i.context_category or "" for i in instruments],
            "context_group":       [i.context_group or "" for i in instruments],
            "layer":               [i.layer for i in instruments],
            "include_in_forecast": [i.include_in_forecast for i in instruments],
            "reliability_flag":    [i.reliability_flag for i in instruments],
            "proxy_for":           [i.proxy_for or "" for i in instruments],
            "resolved_date":       [str(run_date)] * len(instruments),
            "resolver_version":    [RESOLVER_VERSION] * len(instruments),
        })

        output_path = self.OUTPUT_PATH
        output_path.mkdir(parents=True, exist_ok=True)
        final = output_path / f"context_anchors_{run_date.isoformat()}.parquet"

        # Atomic write via temp file + os.replace — same convention as
        # ActiveSymbolsResolver (AS-9) and every other Silver/Gold writer.
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

        symbols = out_df["symbol"].to_list()
        logger.info(
            f"[ContextAnchors] {len(symbols)} context anchors resolved for {run_date} "
            f"(deferred excluded: {loader.deferred_count()})"
        )
        return symbols

    def load(self, run_date: date) -> list[str]:
        """Load Layer 2 active_context symbols for run_date.

        Raises FileNotFoundError if not yet resolved for this run_date.
        """
        path = self.OUTPUT_PATH / f"context_anchors_{run_date.isoformat()}.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"context_anchors not resolved for {run_date}. "
                "Run silver_context_anchors job first."
            )
        return pl.scan_parquet(str(path)).collect()["symbol"].to_list()

    def load_full(self, run_date: date) -> pl.DataFrame:
        """Load full Layer 2 active_context DataFrame including all metadata
        columns (context_category, context_group, layer, include_in_forecast,
        reliability_flag, proxy_for). Useful for CrossAssetEngine (Cycle 4)
        and diagnostic queries.

        Raises FileNotFoundError if not yet resolved for this run_date.
        """
        path = self.OUTPUT_PATH / f"context_anchors_{run_date.isoformat()}.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"context_anchors not resolved for {run_date}. "
                "Run silver_context_anchors job first."
            )
        return pl.scan_parquet(str(path)).collect()


def run(run_date: date) -> None:
    """
    Job entry point — called by job_registry.py's 'silver_context_anchors'.

    depends_on=[] is the technically honest declaration, not a shortcut:
    resolve() reads only config/instruments.yaml (via InstrumentLoader) and
    never touches data/bronze/ or data/silver/market_ohlcv/. Declaring a
    dependency on silver_ohlcv or silver_ohlcv_context here would be
    cosmetic (matching the reader's mental model of "Layer 2 stuff runs
    together") rather than a real data dependency — and a fake dependency
    would let an unrelated Layer 1 Silver failure needlessly block this job
    (DependencyGuard, GD §14.3.3), which is the opposite of what
    Separation of Concerns is for.
    """
    ContextAnchorsResolver().resolve(run_date)
