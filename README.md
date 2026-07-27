# Alpha Factory — Data Platform

**Bronze → Silver → Gold** | Medallion Architecture | **699 Instruments** (dual-layer) | 12 Data Sources

```
Grand Design v1.2 · Supplementary Design v1.1 · Implementation Detail v1.0
Architecture v2.0 · Architecture Extension v1.0 · Data Source & Rates Adjustment v1.0
CI/CD Ops Guide v1.7.4 · GMI Decision Documents v1 & v2
v1.10.0 · 1188 Tests Passing · July 2026
```

---

## Overview

Alpha Factory is a production-grade Data Platform built on
**Medallion Architecture** (Bronze → Silver → Gold), evolving toward a **Global Macro
Intelligence Platform (GMI)**. It is a data-and-signal-generation layer only —
position sizing, order execution, and trading decisions are explicitly out of
scope (Separation of Concerns, GD §0). Designed to run efficiently on a single
MacBook Air M1 (8GB RAM / 512GB SSD), using Polars (lazy evaluation), DuckDB
(parameterized queries), and Parquet with Hive-style partitioning.

```
External APIs (12 sources)
        │
        ▼
┌─────────────────┐
│  BRONZE LAYER   │  Raw ingestion — immutable, append-only, Snappy
│  data/bronze/   │  Schema validation → quarantine on mismatch
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SILVER LAYER   │  Clean + enrich — UTC normalized, PIT-integrity
│  data/silver/   │  VWAP (H+L+C)/3, log_return, dollar_volume, is_clean
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   GOLD LAYER    │  Signals + Analytics — regime, MTF, screener
│   data/gold/    │  Output interface for Trading Engine (external, out of scope)
└─────────────────┘
```

### Dual-Layer Instrument Universe (GMI Architecture v2.0 / Extension v1.0)

The instrument universe is split into two functionally distinct layers:

| | **Layer 1 — `active_ohlcv`** | **Layer 2 — `active_context`** |
|---|---|---|
| **Role** | Tradeable candidates — liquidity-screened | Always-on macro context anchors |
| **Count** | 640 | 59 (56 active + 3 deferred, Wave 2) |
| **Filter** | `dollar_volume_20d` threshold + price floor | None — always-on regardless of liquidity |
| **Consumers** | `gold_signals`, `signal_aggregation`, `gold_screener` | Future `CrossAssetEngine` (GMI Cycle 4, not yet built) |
| **Examples** | AAPL, BBCA, EUR/USD, gold (AU) futures | VIX, DXY, 13 global equity indices, 25 ETFs, 11 commodity context, BIS central bank rates |

VIX, DXY, and SPX were formally reclassified from Layer 1 to Layer 2
(ADR-003) — they are macro/regime anchors in this system, not tradeable
candidates, and computing RSI/MACD on VIX is not meaningful.

---

## Quick Start

### 1. Setup Environment

> **Prerequisite — Poetry itself:** everything below assumes `poetry` is
> already on PATH. If `poetry --version` fails, install it first:
> `pip install poetry` (conda envs aren't externally-managed, so plain pip
> works fine here — no `--user`/`--break-system-packages` needed once the
> env below is active), or `pipx install poetry` for an isolated global
> install. `make setup` / `make install` / `make doctor` now check this
> automatically and print a clear message instead of a bare
> `command not found` (FIX ADR-028).

```bash
cd alpha-factory

# conda (ARM64-native, recommended for M1) — both steps required
conda env create -f environment.yml
conda activate alpha-factory
poetry install --with dev

# — or — poetry only (no conda; creates its own virtualenv)
poetry install --with dev
poetry env activate   # prints the activation command — eval/source it

cp .env.example .env
# Edit .env: FRED_API_KEY, FINNHUB_API_KEY, POLYGON_API_KEY, TV_USERNAME, etc.
```

> **Dependency note (v1.10.0):** `pandas-ta` was migrated to
> `pandas-ta-classic` — PyPI removed pandas-ta's entire stable (0.3.x)
> release line; pandas-ta-classic is a maintained continuation of the
> same lineage. Python floor remains **3.11+** (CI tests both 3.11 and
> 3.12). `scipy` and `statsmodels` are now hard dependencies (required by
> GMI Wave 1 Cycle 4's CorrelationModule/LeadLagModule/ForecastModule).
> See `KNOWN_RISKS.md` RISK-7 for the full migration history.

### 2. Initialize & Validate Instruments

```bash
# instruments.yaml split into 2 files at v1.6 (v1.12.0, Decision B) —
# validate reads and merges both, same invocation as before:
python scripts/validate_instruments.py
# Expect: VALIDATION PASSED — 699 symbols (Layer 1=640, Layer 2=59), no errors.
```

### 3. Run the Pipeline

```bash
python src/runner.py --list                        # all 27 registered jobs
python src/runner.py --status                       # today's DONE/PENDING
python src/runner.py --job all                      # full DAILY_SEQUENCE (16 jobs)
python src/runner.py --job bronze_ohlcv_daily        # single job
python src/runner.py --job gold_regime --force       # bypass dependency check
python src/runner.py --job all --date 2026-07-13     # backfill a specific date
```

### 4. Run Tests

```bash
python -m pytest tests/ -q                          # full suite (1188 tests)
python -m pytest tests/unit/ -v                     # unit only (57 files)
python -m pytest tests/integration/ -v              # integration only (11 files)
python scripts/check_glob_scope.py                  # CI Gate G-8 (Layer1/Layer2 scoping)
python scripts/validate_instruments.py              # CI Gate G-3 (instrument universe)
```

---

## Architecture

### Layer Independence Guarantee (GD §17.2)

| Layer | Allowed | Forbidden |
|-------|---------|-----------|
| **Bronze** | Fetch external APIs, write Parquet, validate schema, quarantine invalid data | Read Silver/Gold; business transformation (joins, derived columns) |
| **Silver** | Read Bronze via `scan_parquet()`; one sanctioned supplement API call (Finnhub sentiment) | Fetch external APIs for primary enrichment; read Gold |
| **Gold** | Read Silver via DuckDB views; write Gold | Read Bronze directly; call external APIs; modify Silver; make trading decisions |

Enforced by CI Gate G-2 (no f-string SQL — DuckDB `$name` parameterized
binding required everywhere) and Gate G-8 (no unfiltered Layer 1/Layer 2
glob scanning — see below).

### GMI Wave 1 Progress

| Cycle | Scope | Status |
|-------|-------|--------|
| Cycle 1 | Dual-layer 692-instrument universe foundation (`instruments.yaml`, `InstrumentLoader`, `validate_instruments.py`) | ✅ Complete |
| Cycle 2 | BIS Central Bank Rates infrastructure (12 non-FED central banks via `WS_CBPOL_D`) | ✅ Complete |
| Cycle 3 | Layer 2 context OHLCV pipeline (Bronze + Silver ingestion for VIX/DXY/ETFs/global indices/commodity context) | ✅ Complete |
| Solidification | Bronze/Silver/Gold hardening pre-Cycle-4: `context_anchors.py` extraction, Layer1/Layer2 glob-scope fixes, `active_ohlcv` filter, ADX crash fix, Finnhub schema validation | ✅ Complete (v1.9.0) |
| **This release** | GMI Decision Documents v1 & v2: dependency remediation, universe expansion to 699, domain-score correction, CI Gate G-8, 6 additional glob-scope fixes | ✅ Complete (v1.10.0) |
| **Cycle 4** | **CrossAssetEngine** — CorrelationModule, LeadLagModule, ForecastModule, GlobalIndexRegimeModule | ⬜ **Not started** |

Cycle 4 is the next planned milestone. `scipy`/`statsmodels` are now
declared dependencies in anticipation of it; no CrossAssetEngine code
exists in this repository yet.

### Instrument Universe (699 total)

**Layer 1 — 640 (tradeable candidates):**

| Market | Count | Primary Source |
|--------|-------|-----------------|
| US Stocks | 588 | yfinance → Polygon |
| IDX (IDX30) | 30 | tvdatafeed → yfinance `.JK` |
| Forex | 19 | yfinance → ForexDayCache |
| Commodity (trading) | 3 | yfinance — AU (Gold), AG (Silver), CL (WTI) |

**Layer 2 — 59 (context anchors, 56 active + 3 deferred), 22 subcategories across 5 groups:**

| Group | Subcategories | OHLCV Instruments | Notes |
|-------|---------------|--------------------|-------|
| Dollar & Rates | `context_dollar`, `context_dollar_basket` *(new)*, `context_rates_fed`, `context_rates_curve`, `context_rates_spread`, `context_rates_dm_cb`, `context_rates_em_cb`, `context_fx_normalization` *(new)* | 8 (DXY + 6 basket currencies + MYR) | Rate series (fed/curve/spread/dm_cb/em_cb) are FRED/BIS macro series, not OHLCV |
| Global Equity | `context_equity_dm`, `context_equity_em`, `context_volatility` | 15 | SPX, VIX reclassified here from Layer 1 |
| Commodity | `context_commodity_energy`, `_metals`, `_agri`, `_coal` | 11 (8 active + 3 deferred: TIN, CPO, RUBBER) | Only **CPO** is MYR-dependent (v1.10.0 correction — TIN/RUBBER are USD-native) |
| ETF | `context_etf_broad`, `_sector`, `_factor`, `_credit`, `_commodity`, `_international`, `_thematic` | 25 | Broad/sector/factor excluded from ForecastModule (multicollinearity with Layer 1) |

`context_dollar_basket` (CNH, KRW, SGD, HKD, TWD, NOK) and
`context_fx_normalization` (MYR) are new in v1.10.0 — data foundation for
a future `compute_broad_dollar()` and CPO currency normalization
respectively (both `contributes_to: []`, zero direct domain-score weight,
CrossAssetEngine computation itself not yet built).

### Data Sources (12)

| # | Source | Domain | Rate Limit | Role |
|---|--------|--------|-----------|------|
| 1 | FRED | Macro | 120 req/min | Primary macro — 60 series |
| 2 | BLS | Macro | 500 req/day | CPI, PPI, NFP |
| 3 | BEA | Macro | 100 req/min | GDP, PCE |
| 4 | IMF | Macro | Unlimited | Global macro |
| 5 | **BIS** | Central bank rates | Unlimited | 12 non-FED central banks, `WS_CBPOL_D` |
| 6 | tvdatafeed | OHLCV | Session-based | IDX primary |
| 7 | yfinance | OHLCV | ~2k/hr | US stocks + FX + Commodity + Layer 2 context |
| 8 | Finnhub | Market + Sentiment | 60 req/min | Real-time, earnings, sentiment (`bronze_finnhub` gated — not yet live) |
| 9 | AlphaVantage | OHLCV | **25 req/DAY** | FX supplemental only |
| 10 | Polygon.io | OHLCV | 5 req/min | US stocks fallback |
| 11 | EIA | Energy | Unlimited | Oil/gas inventories (Wednesday only) |
| 12 | US Treasury | Bond | Unlimited | Yield curve 1M–30Y |

---

## Pipeline Jobs (27 registered, `JOB_REGISTRY`)

### DAILY_SEQUENCE (16 jobs — `python src/runner.py --job all`)

```
bronze_ohlcv_daily          bronze_ohlcv_context_daily     bronze_treasury
bronze_finnhub_sentiment    silver_ohlcv                   silver_ohlcv_context
silver_validate             silver_active_symbols          silver_context_anchors
silver_sentiment            gold_signals                   gold_mtf
gold_regime                 gold_sector                    gold_screener
health_report
```

### WEEKLY_SEQUENCE (6 weekly-only jobs, prepended to DAILY_SEQUENCE — run Sunday)

```bash
python src/runner.py --job bronze_macro_weekly   # FRED, BLS, BEA, IMF
python src/runner.py --job bronze_bis_rates      # 12 non-FED central banks
python src/runner.py --job bronze_eia            # Wednesday-only, schedule-gated
python src/runner.py --job silver_macro          # PIT integrity, revisions
python src/runner.py --job silver_global_rates   # Dedicated PIT table (not merged into silver_macro)
python src/runner.py --job gold_correlation      # Rolling 60D, active symbols only
```

### Maintenance

```bash
python src/runner.py --status                      # today's job status
python src/runner.py --reset gold_regime            # clear one sentinel
python src/runner.py --reset-all                    # full pipeline re-run
python -m src.utils.delta_reprocessor --dry-run     # preview stale Silver symbols
python -m src.utils.pipeline_dashboard              # rich terminal health dashboard
```

---

## CI/CD Gate Hierarchy (8 gates, all blocking except G-6/G-7)

| Gate | Checks | Added |
|------|--------|-------|
| G-1 | `ast.parse()` syntax validation, every modified `.py` | v1.7.x |
| G-2 | No f-string SQL anywhere in `src/` (AST-based whole-tree scan) | v1.7.x, rewritten v1.8.1 |
| G-3 | `validate_instruments.py` exits 0 | v1.7.x |
| G-4 | Test count ≥ `tests/COUNT_BASELINE.txt` (currently 1188) | v1.7.4 |
| G-5 | 0 failed, 0 collection error | v1.7.x |
| G-6 | Coverage ≥ 70% (PR only) | v1.7.x |
| G-7 | CHANGELOG.md updated (PR only, warn) | v1.7.x |
| **G-8** | **Layer 1/Layer 2 glob-scope enforcement** — no double-`**` globs, no unfiltered `market_ohlcv/**` scans outside `silver_scope.py`'s helpers | **v1.10.0** |

CI runs on a Python **3.11 + 3.12** matrix (`.github/workflows/ci.yml`).

---

## Key Design Decisions

### VWAP Formula (typical price, not close)

```python
typical_price = (high + low + close) / 3
vwap = cumsum(typical_price × volume) / cumsum(volume)   # resets per trading session
```

### Point-in-Time (PIT) Integrity

All macro/rate data carries `vintage_date`/`release_date` (or, for BIS
central bank rates, `effective_date` — a dedicated `silver_global_rates`
table, deliberately **not** merged into `silver_macro_enriched`, since the
two semantics are incompatible). Backtest queries always guard:
`WHERE vintage_date <= trade_date`.

### Domain Score Weight-Sum Invariant (v1.10.0)

Every domain score's `_meta.contributes_to` weights in
`config/instruments_taxonomy.yaml` (v1.12.0, split from `instruments.yaml`
— see CHANGELOG.md ADR-027) must sum to **exactly 1.00** — enforced by
`scripts/validate_instruments.py::_validate_domain_score_weights()`. A
full audit found 5 of 8 scores had drifted (1.05–1.30) due to
undocumented contributors; all restored to literal fidelity with the
governing design documents.

### Layer 1/Layer 2 Glob Scoping (v1.9.0 + v1.10.0)

Never construct a raw `data/silver/market_ohlcv/**/...` glob directly —
use `src/utils/silver_scope.py`:

```python
from src.utils.silver_scope import layer1_globs, context_glob

layer1_globs(Path("data/silver/market_ohlcv"), "*_1D_silver.parquet")
# -> list[str], one glob per existing Layer 1 market, safe to bind as a
#    DuckDB list parameter or embed as a SQL list literal

context_glob(Path("data/silver/market_ohlcv"), "*_1D_silver.parquet")
# -> str | None, Layer 2 (context/) only
```

An unfiltered glob at this root silently scans BOTH layers together —
this exact defect class (RISK-6, `KNOWN_RISKS.md`) has been found and
fixed in 8 modules across two releases (`quality_validator.py`,
`technical_signals.py`, `screener.py`, `correlation_matrix.py`,
`pit_data.py`, `views.py`, `delta_reprocessor.py`,
`pipeline_dashboard.py`) and is now a permanent CI gate (G-8).

---

## Project Structure

```
alpha-factory/
├── config/
│   ├── instruments_identity.yaml   # v1.6 — sourcing fields (Decision B split)
│   ├── instruments_taxonomy.yaml   # v1.6 — routing/scoring + _meta blocks
│   ├── regime_sector_weights.yaml  # externalized from sector_rotation.py
│   ├── schemas/instruments/        # jsonschema Draft-7, one per file above
│   ├── bis_cb_rates.yaml           # 12 REF_AREA map + structural break registry
│   ├── fred_series.yaml           # 60 FRED series registry
│   ├── pipeline.yaml
│   └── schemas/                    # Bronze schema registry (12 YAML files)
│
├── scripts/
│   ├── validate_instruments.py     # Gate G-3 + domain-score weight-sum guard
│   ├── check_glob_scope.py         # Gate G-8 (NEW v1.10.0)
│   ├── check_poetry_env.py         # ADR-026 — poetry/conda env-reuse guard
│   ├── archive/                    # v1.11.2 — disabled, historical reference only
│   │   ├── migrate_instruments.py  # superseded pre-v1.4 schema; see archive/README.md
│   │   ├── build_instruments_v14.py# SRC==DST one-time transform; see archive/README.md
│   │   ├── instruments_raw.py      # relocated from src/config/ — orphaned data, sole consumer archived
│   │   └── README.md
│   └── preflight/                  # NEW v1.10.0 — authored, network-execution deferred
│       ├── check_yfinance_tickers.py
│       ├── check_bis_cbpol_d.py
│       └── check_finnhub_shape.py
│
├── src/
│   ├── config/
│   │   ├── instrument_loader.py    # Dual-layer InstrumentLoader (Layer 1 + Layer 2 API)
│   │   └── instruments_raw.py
│   │
│   ├── bronze/                     # 18 ingesters/adapters — append-only, immutable
│   │   ├── base_ingester.py · schema_validator.py · source_adapter.py
│   │   ├── market_ingester.py      # Layer 1 + Layer 2 context OHLCV
│   │   ├── bis_rates_ingester.py   # 12 non-FED central banks
│   │   ├── finnhub_ingester.py · finnhub_sentiment_ingester.py
│   │   ├── fred_ingester.py · bls_ingester.py · bea_ingester.py · imf_ingester.py
│   │   ├── eia_ingester.py · treasury_ingester.py
│   │   ├── forex_cache.py · inc_fetch.py
│   │   └── tvdatafeed_session.py · tvdatafeed_adapter.py · yfinance_adapter.py · polygon_adapter.py · alphavantage_adapter.py
│   │
│   ├── silver/                     # 10 processors — clean, enrich, UTC-normalize
│   │   ├── ohlcv_processor.py · ohlcv_aggregator.py (4H synthesis)
│   │   ├── active_symbols.py       # Layer 1 ONLY (post-GMI extraction)
│   │   ├── context_anchors.py      # Layer 2 ONLY — separated from active_symbols.py
│   │   ├── global_rates_processor.py  # BIS rates, dedicated PIT table
│   │   ├── macro_processor.py · fundamental_processor.py · sentiment_processor.py
│   │   └── quality_validator.py    # Layer 1 CRITICAL + Layer 2 WARNING checks
│   │
│   ├── gold/
│   │   ├── indicators/
│   │   │   ├── core_indicators.py     # Pure Polars EMA/RSI/MACD/ATR
│   │   │   └── pandas_indicators.py   # pandas-ta-classic BBands + ADX (v1.10.0)
│   │   ├── technical_signals.py    # active_ohlcv-filtered (Layer 1 only)
│   │   ├── mtf_alignment.py · macro_regime.py · sector_rotation.py
│   │   ├── screener.py · correlation_matrix.py · views.py
│   │   └── hmm_regime.py           # Phase 2 regime detection (dependency present, not wired to a job yet)
│   │
│   ├── scheduler/
│   │   ├── job_registry.py         # 27 jobs, DAILY_SEQUENCE (16) + WEEKLY_SEQUENCE (22)
│   │   ├── dependency_guard.py     # file-sentinel based, with staleness-window support
│   │   └── pipeline_scheduler.py   # APScheduler upgrade path (not yet activated)
│   │
│   ├── utils/
│   │   ├── symbol_utils.py · atomic_io.py · progress_checkpoint.py
│   │   ├── silver_scope.py         # Layer1/Layer2 glob-scoping helpers (v1.9.0+)
│   │   ├── pipeline_logger.py · pipeline_dashboard.py · health_reporter.py
│   │   ├── delta_reprocessor.py · rate_limiter.py
│   │
│   ├── backtest/
│   │   ├── pit_data.py · engine.py · slippage.py · metrics.py
│   │
│   └── runner.py                   # CLI entry point
│
├── tests/
│   ├── conftest.py
│   ├── unit/                       # 57 test files
│   └── integration/                # 11 test files
│   └── COUNT_BASELINE.txt          # 1188 (Gate G-4 floor)
│
├── data/                            # Runtime data (gitignored)
│   ├── bronze/ · silver/ · gold/ · health/ · quarantine/ · .sentinels/
│
├── .github/workflows/ci.yml         # 8 gates, Python 3.11+3.12 matrix
├── pyproject.toml                   # poetry — pandas-ta-classic, scipy, statsmodels
├── poetry.lock                      # NEW v1.10.0 — 113 packages resolved
├── environment.yml                  # conda (M1 ARM64)
├── KNOWN_RISKS.md                   # 8 risk entries (RISK-1..8), accepted-by-design + resolved
├── CHANGELOG.md
└── README.md                        # this file
```

---

## Gold Output Schema (Trading Engine Interface Contract, GD §0.4)

All outputs written to `data/gold/` as Parquet, queryable via DuckDB views
(`src/gold/views.py::get_pipeline_connection()`). Views are Layer-1-scoped
only (`v_ohlcv_1D`, `v_ohlcv_1H`, `v_ohlcv_all`) — a Trading Engine querying
these will never see a Layer 2 context instrument (VIX, DXY, an ETF)
disguised as a tradeable candidate.

```python
from src.gold.views import get_pipeline_connection
con = get_pipeline_connection()
df  = con.execute("SELECT * FROM v_ohlcv_1D WHERE symbol = 'AAPL'").pl()
```

| Output | Path | Refresh |
|--------|------|---------|
| Regime Store | `data/gold/macro/regime_store.parquet` | Daily |
| MTF Alignment | `data/gold/mtf/mtf_alignment_{date}.parquet` | Daily |
| Watchlist | `data/gold/screener/watchlist_{date}.parquet` | Daily |
| Sector Weights | `data/gold/sector/sector_regime_weights.parquet` | Daily |
| Correlation Clusters | `data/gold/correlation/correlation_clusters.parquet` | Weekly |
| Global Rates (BIS) | `data/silver/global_rates/global_rates_policy.parquet` | Weekly |

*`gold_domain_scores.parquet` (8 domain scores computed from Layer 2
`_meta.contributes_to` weights) and the full CrossAssetEngine output
catalog (`cross_asset_corr.parquet`, `lead_lag_matrix.parquet`,
`cross_asset_forecast.parquet`, `global_regime.parquet`) are specified in
Architecture Extension v1.0 §5 / Architecture v2.0 §6 but **not yet
implemented** — GMI Wave 1 Cycle 4.*

---

## Separation of Concerns

This pipeline is **data platform only**.

| Pipeline Provides (IN SCOPE) | Trading Engine Decides (OUT OF SCOPE) |
|------------------------------|----------------------------------------|
| MTF score + signal quality grade | Entry/exit execution |
| Regime classification + transition flag | Position sizing |
| Sector weight multipliers | Stop loss / take profit management |
| `days_to_earnings` (data field) | Whether to trade around earnings |
| `sentiment_score` (numeric) | Sentiment-based filtering |
| Domain scores (planned, Cycle 4) | Portfolio rebalancing |

---

## Environment Variables

```bash
# Macro data (all free tier)
FRED_API_KEY=
BLS_API_KEY=
BEA_API_KEY=
EIA_API_KEY=
# BIS: no API key required

# Market data
FINNHUB_API_KEY=
ALPHAVANTAGE_API_KEY=      # 25 req/day free tier
POLYGON_API_KEY=
TV_USERNAME=                # TradingView account (tvdatafeed)
TV_PASSWORD=

# Optional
TELEGRAM_BOT_TOKEN=         # health_report alerts
TELEGRAM_CHAT_ID=
```

---

## Document Hierarchy

Highest authority wins any conflict:

```
Grand Design v1.2
  └── Supplementary Design v1.1
        └── Implementation Detail Document v1.0
              └── Architecture v2.0
                    └── Architecture Extension v1.0
                          └── Data Source & Rates Adjustment v1.0
                                └── Alpha Factory CI/CD Ops Guide v1.7.4
                                      └── GMI Implementation Checkpoints (v1 → v2 → v3)
                                            └── GMI Decision Documents (v1, v2)
                                                  └── This codebase (v1.10.0)
```

See `CHANGELOG.md` for the full per-version fix history and
`KNOWN_RISKS.md` for accepted risks and resolved defects.

---

*Alpha Factory · Bronze/Silver/Gold Medallion Architecture · Global Macro Intelligence Platform (GMI) · v1.10.0 · July 2026*
