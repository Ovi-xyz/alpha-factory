"""
job_registry.py — GD §14.3.2 + G5 Supplementary Design v1.1
Daftar terpusat semua pipeline job.

Tambahkan job baru di sini — runner.py tidak perlu diubah.
Semua job functions adalah lazy imports (bukan top-level) untuk
menghindari circular imports dan startup overhead.

G5 Extension: schedule framework untuk EIA (weekly), BLS (monthly),
BEA (quarterly) dengan _passes_schedule() guard.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Callable


# ── Schedule Guard (G5) ───────────────────────────────────────────────────────

def _passes_schedule(job: dict, run_date: date) -> bool:
    """
    Return True jika run_date memenuhi semua schedule constraints job.

    Constraint types:
        run_on_weekdays:    [0..6] — 0=Monday, 4=Friday
        run_on_day_of_month: [1..31]
        run_on_months:      [1..12]
        run_on_nth_weekday: {'n': int, 'weekday': int, 'tolerance_days': int}

    Jika tidak ada constraint → selalu True (daily job).
    """
    # run_on_weekdays: e.g. [2] = Wednesday only (EIA)
    if "run_on_weekdays" in job:
        if run_date.weekday() not in job["run_on_weekdays"]:
            return False

    # run_on_day_of_month: e.g. list(range(10, 16)) untuk BLS CPI
    if "run_on_day_of_month" in job:
        if run_date.day not in job["run_on_day_of_month"]:
            return False

    # run_on_months: e.g. [1, 4, 7, 10] untuk BEA GDP quarterly
    if "run_on_months" in job:
        if run_date.month not in job["run_on_months"]:
            return False

    # run_on_nth_weekday: e.g. {'n': 1, 'weekday': 4} = first Friday (BLS NFP)
    if "run_on_nth_weekday" in job:
        spec = job["run_on_nth_weekday"]
        tolerance = spec.get("tolerance_days", 0)

        # Check if run_date is the nth occurrence of weekday in month
        def is_nth_weekday(d: date) -> bool:
            if d.weekday() != spec["weekday"]:
                return False
            return (d.day - 1) // 7 + 1 == spec["n"]

        # Check run_date AND tolerance window (G5×GD Cross-gap)
        import datetime as dt
        matches = any(
            is_nth_weekday(run_date - dt.timedelta(days=i))
            for i in range(tolerance + 1)
        )
        if not matches:
            return False

    return True


# ── Lazy Job Function Loaders ─────────────────────────────────────────────────

def _bronze_ohlcv(run_date: date) -> None:
    from src.bronze.market_ingester import MarketOHLCVIngester
    MarketOHLCVIngester().run(run_date)


def _bronze_ohlcv_context(run_date: date) -> None:
    """
    ADD GMI-JR-002 — Architecture v2.0 §4, Architecture Extension v1.0 §2-3.
    Bronze OHLCV untuk 49 Layer 2 context anchors aktif (VIX, DXY, 13 global
    equity indices, 25 ETF, 8 commodity context). depends_on=[] — independen
    dari bronze_ohlcv_daily (GD §17.3.1: Bronze ingesters tidak saling
    bergantung), berjalan paralel secara logis di DAILY_SEQUENCE.
    """
    from src.bronze.market_ingester import MarketOHLCVIngester
    MarketOHLCVIngester().run_context(run_date)


def _bronze_macro_weekly(run_date: date) -> None:
    """FRED, BLS, BEA, IMF — macro economics weekly batch."""
    from src.bronze.fred_ingester import FREDIngester
    from src.bronze.bls_ingester import BLSIngester
    from src.bronze.bea_ingester import BEAIngester
    from src.bronze.imf_ingester import IMFIngester
    FREDIngester().run(run_date)
    BLSIngester().run(run_date)
    BEAIngester().run(run_date)
    IMFIngester().run(run_date)


def _bronze_bis_rates(run_date: date) -> None:
    """
    ADD GMI-JR-001 — Data Source & Rates Adjustment v1.0 §8.1.
    BIS CBPOL_D central bank policy rates — 12 non-FED CBs (ADR-010, ADR-011).
    Independent from bronze_macro_weekly (GD §17.3.1: Bronze ingesters
    tidak saling bergantung) — runs alongside it in WEEKLY_SEQUENCE.
    """
    from src.bronze.bis_rates_ingester import run as bis_rates_run
    bis_rates_run(run_date)


def _bronze_treasury(run_date: date) -> None:
    from src.bronze.treasury_ingester import TreasuryIngester
    TreasuryIngester().run(run_date)


def _bronze_eia(run_date: date) -> None:
    from src.bronze.eia_ingester import EIAIngester
    EIAIngester().run(run_date)


def _bronze_bls_cpi(run_date: date) -> None:
    """BLS CPI — fetch via FRED mirror (CPIAUCSL series)."""
    from src.bronze.bls_ingester import BLSIngester
    BLSIngester().run(run_date)


def _bronze_bls_nfp(run_date: date) -> None:
    """BLS NFP — fetch via FRED mirror (PAYEMS series)."""
    from src.bronze.bls_ingester import BLSIngester
    BLSIngester().run(run_date)


def _bronze_bea_gdp(run_date: date) -> None:
    """BEA GDP — fetch via BEAIngester (or FRED mirror)."""
    from src.bronze.bea_ingester import BEAIngester
    BEAIngester().run(run_date)


def _silver_ohlcv(run_date: date) -> None:
    """
    Process Bronze OHLCV → Silver (VWAP, adj_factor, is_clean).

    FIX GAP-6 [P1] (Production Readiness Assessment v1.7.2, GD §14.3.2):
    delegates to src.silver.ohlcv_processor.run(), which now exposes the
    module-level run(run_date) entry point GD §14.3.2 requires every
    Silver/Gold module to have. This wrapper previously contained its own
    inline copy of the full 2-pass Bronze->Silver->4H logic instead of
    delegating — functionally correct, but a second copy of the same logic
    is a drift risk: a fix applied to one copy and not the other silently
    diverges (the same "half-fix" failure pattern GAP-1 was caused by).
    Matches the delegate-only pattern every other wrapper in this file uses
    (_silver_macro, _silver_active_symbols, etc.).

    2-pass design (unchanged, now owned by ohlcv_processor.run()):
      PASS 1: Bronze raw TFs (5m,15m,1H,1D,1W,1M) → Silver.
      PASS 2: Silver 1H → Silver 4H synthesis (v1.5: 4H moved to Silver,
              GD §4.1/§17.7 — 4H is not raw Bronze source data).
    """
    from src.silver.ohlcv_processor import run as ohlcv_run
    ohlcv_run(run_date)


def _silver_ohlcv_context(run_date: date) -> None:
    """
    ADD GMI-JR-002 — Silver counterpart to bronze_ohlcv_context_daily.
    Delegates to ohlcv_processor.run_context() (matches the delegate-only
    pattern every other wrapper in this file uses — see _silver_ohlcv
    comment on why inline duplication is a drift risk, GAP-1 precedent).
    """
    from src.silver.ohlcv_processor import run_context as ohlcv_context_run
    ohlcv_context_run(run_date)


def _silver_macro(run_date: date) -> None:
    from src.silver.macro_processor import run as macro_run
    macro_run(run_date)


def _silver_global_rates(run_date: date) -> None:
    """
    ADD GMI-JR-001 — Data Source & Rates Adjustment v1.0 §9.
    Transform Bronze BIS CB rates -> silver_global_rates/global_rates_policy.parquet
    (forward-fill, structural break flags, PIT vintage_date). Dedicated table —
    NOT merged into silver_macro (different PIT semantics, §9.1).
    """
    from src.silver.global_rates_processor import run as global_rates_run
    global_rates_run(run_date)


def _silver_active_symbols(run_date: date) -> None:
    """
    FIX GMI-JR-001: delegate to module-level run() (matches every other
    wrapper's pattern in this file — see _silver_ohlcv comment) instead of
    calling resolver.resolve() inline.

    UPDATED MOVED GMI-CTX-001: this wrapper (and active_symbols.py's own
    run()) previously ALSO resolved Layer 2 context anchors in the same
    call (GMI-JR-001's own fix, at the time, was to stop SKIPPING that
    Layer 2 call). Layer 2 has since been extracted into its own module
    and its own job — see _silver_context_anchors below. This wrapper is
    Layer 1 only now, matching what active_symbols.py itself does.
    """
    from src.silver.active_symbols import run as active_symbols_run
    active_symbols_run(run_date)


def _silver_context_anchors(run_date: date) -> None:
    """
    MOVED GMI-CTX-001 — Layer 2 context anchor resolution, extracted from
    active_symbols.py (Architecture v2.0 §4.4) into its own module and job
    for Separation of Concerns: Layer 2 has zero Silver dependency (pure
    InstrumentLoader enumeration) and was never architecturally coupled to
    Layer 1's dollar-volume-screened resolve() beyond having been bundled
    in the same Python function. See src/silver/context_anchors.py
    docstring for the full rationale.
    """
    from src.silver.context_anchors import run as context_anchors_run
    context_anchors_run(run_date)


def _silver_validate(run_date: date) -> None:
    from src.silver.quality_validator import run as validate_run
    validate_run(run_date)


def _gold_signals(run_date: date) -> None:
    from src.gold.technical_signals import run as gold_signals_run
    gold_signals_run(run_date)


def _gold_mtf(run_date: date) -> None:
    from src.gold.mtf_alignment import run as mtf_run
    mtf_run(run_date)


def _gold_regime(run_date: date) -> None:
    from src.gold.macro_regime import run as regime_run
    regime_run(run_date)


def _gold_sector(run_date: date) -> None:
    from src.gold.sector_rotation import run as sector_run
    sector_run(run_date)


def _gold_screener(run_date: date) -> None:
    from src.gold.screener import run as screener_run
    screener_run(run_date)


def _gold_correlation(run_date: date) -> None:
    from src.gold.correlation_matrix import run as corr_run
    corr_run(run_date)


def _health_report(run_date: date) -> None:
    from src.utils.health_reporter import run as health_run
    health_run(run_date)


# ── Job Registry ──────────────────────────────────────────────────────────────

JOB_REGISTRY: dict[str, dict[str, Any]] = {

    # ── BRONZE LAYER ──────────────────────────────────────────────────────────

    "bronze_ohlcv_daily": {
        "description": "Ingest yfinance + Polygon — 643 symbols OHLCV daily bars",
        "fn":          _bronze_ohlcv,
        "depends_on":  [],
        "layer":       "bronze",
        # v1.5: -10m vs v1.4 — tidak ada aggregate_ohlcv() call untuk 4H
        "est_minutes": 35,
    },

    "bronze_ohlcv_context_daily": {
        # ADD GMI-JR-002 — Architecture v2.0 §4, Architecture Extension v1.0 §2-3.
        "description": (
            "Ingest yfinance — 49 Layer 2 context anchors aktif "
            "(VIX, DXY, 13 global equity indices, 25 ETF, 8 commodity context)"
        ),
        "fn":          _bronze_ohlcv_context,
        "depends_on":  [],   # GD §17.3.1: Bronze ingesters independent
        "layer":       "bronze",
        # 49 symbols × up to 3 TF (1D/1W/1M) × 0.6s throttle ≈ 88s + overhead
        "est_minutes": 4,
    },

    "bronze_macro_weekly": {
        "description": "Ingest FRED, BLS, BEA, IMF — macro economics (weekly, Sundays)",
        "fn":          _bronze_macro_weekly,
        "depends_on":  [],
        "layer":       "bronze",
        "est_minutes": 15,
        # No run_on_weekdays — called explicitly on weekly SOP
    },

    "bronze_bis_rates": {
        # ADD GMI-JR-001 — Data Source & Rates Adjustment v1.0 §8.1, §8.2.
        "description": (
            "Ingest BIS CBPOL_D — 12 non-FED central bank policy rates "
            "(ADR-010, ADR-011). No API key required."
        ),
        "fn":          _bronze_bis_rates,
        "depends_on":  [],   # GD §17.3.1: Bronze ingesters independent
        "layer":       "bronze",
        "est_minutes": 3,
        # No run_on_weekdays — called explicitly on weekly SOP (after bronze_macro_weekly)
    },

    "bronze_treasury": {
        "description": "Ingest US Treasury — daily yield curve 1M–30Y",
        "fn":          _bronze_treasury,
        "depends_on":  [],
        "layer":       "bronze",
        "est_minutes": 2,
    },

    "bronze_eia": {
        "description": "Ingest EIA — crude oil inventory & production (weekly Wednesday)",
        "fn":          _bronze_eia,
        "depends_on":  [],
        "layer":       "bronze",
        "est_minutes": 2,
        "run_on_weekdays": [2],   # 0=Mon .. 4=Fri; 2=Wednesday
    },

    "bronze_bls_cpi": {
        "description": "BLS Consumer Price Index — monthly release (day 10-15)",
        "fn":          _bronze_bls_cpi,
        "depends_on":  [],
        "layer":       "bronze",
        "est_minutes": 2,
        "run_on_day_of_month": list(range(10, 16)),   # hari 10-15
    },

    "bronze_bls_nfp": {
        "description": "BLS Non-Farm Payroll — first Friday of each month",
        "fn":          _bronze_bls_nfp,
        "depends_on":  [],
        "layer":       "bronze",
        "est_minutes": 2,
        "run_on_nth_weekday": {"n": 1, "weekday": 4, "tolerance_days": 1},
    },

    "bronze_bea_gdp": {
        "description": "BEA GDP Advance Estimate — quarterly (last week of Jan/Apr/Jul/Oct)",
        "fn":          _bronze_bea_gdp,
        "depends_on":  [],
        "layer":       "bronze",
        "est_minutes": 2,
        "run_on_months":       [1, 4, 7, 10],
        "run_on_day_of_month": list(range(25, 32)),
    },

    # ── SILVER LAYER ──────────────────────────────────────────────────────────

    "silver_ohlcv": {
        "description": "Clean + enrich OHLCV — VWAP, log_return, adj_factor, is_clean; 2-pass incl. 4H synthesis",
        "fn":          _silver_ohlcv,
        "depends_on":  ["bronze_ohlcv_daily"],
        "layer":       "silver",
        # v1.5: +15m vs v1.4 — Pass 2 sintesis Silver 4H untuk 643 symbols
        "est_minutes": 75,
    },

    "silver_ohlcv_context": {
        # ADD GMI-JR-002 — Silver counterpart to bronze_ohlcv_context_daily.
        "description": (
            "Clean + enrich Layer 2 context OHLCV — 49 instruments, "
            "1-pass (1D/1W/1M, no 4H synthesis — no defined consumer yet)"
        ),
        "fn":          _silver_ohlcv_context,
        "depends_on":  ["bronze_ohlcv_context_daily"],
        "layer":       "silver",
        "est_minutes": 3,
    },

    "silver_macro": {
        "description": "Clean macro series — PIT integrity, vintage_date, revisions",
        "fn":          _silver_macro,
        "depends_on":  ["bronze_macro_weekly"],
        "layer":       "silver",
        "est_minutes": 10,
    },

    "silver_global_rates": {
        # ADD GMI-JR-001 — Data Source & Rates Adjustment v1.0 §9.
        "description": (
            "Transform Bronze BIS CB rates — forward-fill, structural break "
            "flags, PIT vintage_date. Dedicated table (not silver_macro, §9.1)."
        ),
        "fn":          _silver_global_rates,
        "depends_on":  ["bronze_bis_rates"],
        "layer":       "silver",
        "est_minutes": 3,
    },

    "silver_validate": {
        "description": "Cross-layer quality check — null, outlier, PIT integrity",
        "fn":          _silver_validate,
        "depends_on":  ["silver_ohlcv", "silver_macro"],
        # FIX NEW-1 [BLOCKING] (audit_v1_7_3_uncovered_findings.docx §2, Opsi A):
        # silver_macro cadence mingguan (GD §3.3.1) — TIDAK ada di DAILY_SEQUENCE.
        # stale_tolerance mengizinkan sentinel silver_macro hingga 7 hari lalu
        # (mis. dari run Minggu) dianggap valid untuk hari Senin-Sabtu, sehingga
        # `--job all` bisa selesai pada run_date APAPUN (GATE-N1), bukan hanya
        # pada hari yang sama persis dengan terakhir kali silver_macro dijalankan.
        # 7 hari = 1 siklus mingguan penuh + 1 hari buffer (siklus normal hanya
        # butuh maksimum 6 hari mundur untuk mencapai Minggu sebelumnya).
        "stale_tolerance": {"silver_macro": 7},
        "layer":       "silver",
        "est_minutes": 10,
    },

    # IDD §7: silver_active_symbols HARUS ada setelah silver_validate
    "silver_active_symbols": {
        "description": "Resolve Layer 1 active symbols via dollar_volume_20d — ~190 symbols",
        "fn":          _silver_active_symbols,
        "depends_on":  ["silver_ohlcv", "silver_validate"],
        "layer":       "silver",
        "est_minutes": 5,
    },

    # ADD GMI-CTX-001 — Layer 2 context anchors (Architecture v2.0 §4.4).
    # depends_on=[] is deliberate, not a placeholder: resolve() is pure
    # InstrumentLoader enumeration with zero Silver read — see
    # src/silver/context_anchors.py::run() docstring for why a fake
    # dependency on silver_ohlcv would be dishonest and only add needless
    # blocking risk. Positioned next to silver_active_symbols for SOP
    # readability (both are "active universe resolution" in the operator's
    # mental model) even though the two jobs share no real dependency.
    "silver_context_anchors": {
        "description": "Resolve Layer 2 context anchors — 49 always-on, config-driven (no Silver query)",
        "fn":          _silver_context_anchors,
        "depends_on":  [],
        "layer":       "silver",
        "est_minutes": 1,
    },

    # ── GOLD LAYER ────────────────────────────────────────────────────────────

    "gold_signals": {
        "description": "Technical indicators — 643 symbols x 7 timeframes",
        "fn":          _gold_signals,
        "depends_on":  ["silver_ohlcv", "silver_validate", "silver_active_symbols"],
        "layer":       "gold",
        "est_minutes": 90,
    },

    "gold_mtf": {
        "description": "MTF alignment — score -7..+7, signal quality A/B/C/D",
        "fn":          _gold_mtf,
        "depends_on":  ["gold_signals"],
        "layer":       "gold",
        "est_minutes": 30,
    },

    "gold_regime": {
        "description": "Macro regime detection — RISK_ON/OFF + transition flag",
        "fn":          _gold_regime,
        "depends_on":  ["silver_macro", "silver_validate"],
        # FIX NEW-1 [BLOCKING]: sama dengan silver_validate di atas — silver_macro
        # cadence mingguan, butuh staleness window agar --job all tidak crash
        # di hari Senin-Sabtu. Lihat komentar lengkap di entry silver_validate.
        "stale_tolerance": {"silver_macro": 7},
        "layer":       "gold",
        "est_minutes": 5,
    },

    "gold_sector": {
        "description": "Sector rotation weights berdasarkan aktif regime",
        "fn":          _gold_sector,
        "depends_on":  ["gold_regime"],
        "layer":       "gold",
        "est_minutes": 2,
    },

    "gold_screener": {
        "description": "Watchlist top-20 — MTF + Regime + Sector",
        "fn":          _gold_screener,
        # FIX ADR-043 (GMI_Decision_Document_v10.docx): "silver_sentiment" DIHAPUS
        # dari depends_on. Finnhub diretired penuh (sentiment + earnings/quotes) —
        # sentiment 403 setiap simbol (plan-tier gate, bukan defect) dan
        # earnings/quotes tidak pernah live (stub NotImplementedError, FIX R-F04).
        # Supersedes FIX NEW-2 (audit_v1_7_3_uncovered_findings.docx §3, Opsi A),
        # yang sebelumnya sudah menghapus "silver_fundamental" dari sini dengan
        # alasan yang sama (hard-dependency ke job yang tidak pernah bisa selesai).
        # "silver_fundamental" dan "silver_sentiment" kini tidak ada sama sekali di
        # JOB_REGISTRY (dihapus, bukan sekadar di-skip) — lihat ADR-043 Consequences.
        # gold/screener.py::_enrich_earnings() dan _enrich_sentiment() juga dihapus
        # (FIX ADR-044) — kolom days_to_earnings/near_earnings_flag/sentiment_score/
        # buzz_score tetap ada di watchlist output, permanently NULL (Interface
        # Contract GD §0.4/§17.6 tidak berubah — hanya nilainya, bukan skema).
        "depends_on":  ["gold_mtf", "gold_regime", "gold_sector"],
        "layer":       "gold",
        "est_minutes": 5,
    },

    "gold_correlation": {
        "description": "Rolling 60D correlation matrix — active symbols only (~200)",
        "fn":          _gold_correlation,
        "depends_on":  ["silver_ohlcv", "silver_active_symbols"],
        "layer":       "gold",
        "est_minutes": 10,
    },

    # ── UTILITIES ─────────────────────────────────────────────────────────────

    "health_report": {
        "description": "Daily summary — run stats, storage check, optional Telegram alert",
        "fn":          _health_report,
        "depends_on":  ["gold_screener"],
        "layer":       "util",
        "est_minutes": 2,
    },
}


# ── Pipeline Sequences ────────────────────────────────────────────────────────
# FIX R-F03: Pisahkan DAILY_SEQUENCE dan WEEKLY_SEQUENCE secara eksplisit.
# PIPELINE_SEQUENCE (alias DAILY_SEQUENCE) digunakan oleh runner.py --job all.
#
# MASALAH sebelumnya:
#   - PIPELINE_SEQUENCE tunggal mencampur daily dan weekly jobs
#   - silver_macro (dep: bronze_macro_weekly) masuk daily sequence —
#     akan selalu gagal dependency check pada hari selain Minggu
#   - bronze_finnhub (stub) masuk daily sequence — selalu fake success
#
# FIX GD-F04: silver_active_symbols dependencies sudah benar di JOB_REGISTRY.
# Pastikan urutannya di sequence: silver_validate → silver_active_symbols → ...

DAILY_SEQUENCE: list[str] = [
    # Bronze — no inter-dependency, berjalan berurutan
    "bronze_ohlcv_daily",
    "bronze_ohlcv_context_daily",  # ADD GMI-JR-002: Layer 2, independen dari Layer 1
    "bronze_treasury",
    # bronze_finnhub, bronze_finnhub_sentiment DIHAPUS — ADR-043 (Finnhub full
    # retirement): sentiment 403 plan-tier gate, earnings/quotes never live.
    # bronze_macro_weekly DIHAPUS dari daily — weekly-only (FIX R-F03)
    # bronze_eia DIHAPUS dari daily — Rabu-only via schedule guard

    # Silver Phase 1 — processing (FIX R-F03: silver_macro tidak di sini)
    "silver_ohlcv",
    "silver_ohlcv_context",  # ADD GMI-JR-002: dep bronze_ohlcv_context_daily
    "silver_validate",

    # Silver Phase 2 — active symbols (IDD §7 + GD-F04: setelah silver_validate)
    # silver_active_symbols tetap ada: dibutuhkan gold_signals, gold_correlation, gold_screener
    "silver_active_symbols",
    # ADD GMI-CTX-001: Layer 2, depends_on=[] — position here is for SOP
    # readability only (grouped with the other "active universe" job), not
    # a real ordering requirement.
    "silver_context_anchors",

    # silver_sentiment DIHAPUS — ADR-043 (Finnhub full retirement)

    # Gold — urutan KRITIS: regime -> sector -> screener
    "gold_signals",
    "gold_mtf",
    "gold_regime",        # dep: silver_macro — akan skip jika silver_macro tidak ada
    "gold_sector",
    "gold_screener",

    # Util
    "health_report",

    # gold_correlation: Minggu — ada di WEEKLY_SEQUENCE, tidak di sini
]

WEEKLY_SEQUENCE: list[str] = [
    # Bronze macro — Minggu
    "bronze_macro_weekly",
    "bronze_bis_rates",     # ADD GMI-JR-001: independent of bronze_macro_weekly, same SOP slot
    "bronze_eia",           # Rabu via schedule guard; tetap masuk weekly SOP

    # Silver macro
    "silver_macro",         # FIX R-F03: hanya di weekly, bukan daily
    "silver_global_rates",  # ADD GMI-JR-001: dep bronze_bis_rates, dedicated PIT table (§9.1)

    # silver_fundamental line DIHAPUS — ADR-043 (Finnhub full retirement);
    # bronze_finnhub, silver_fundamental no longer exist in JOB_REGISTRY.

    # Gold correlation — rolling 60D, weekly refresh
    "gold_correlation",

    # Lanjutkan dengan DAILY_SEQUENCE setelah ini (per SOP §14.4.2)
] + DAILY_SEQUENCE

# PIPELINE_SEQUENCE dipertahankan sebagai alias DAILY_SEQUENCE untuk backward-compat
# runner.py --job all menggunakan DAILY_SEQUENCE
PIPELINE_SEQUENCE = DAILY_SEQUENCE


# ── Layer-Scoped Job Names ────────────────────────────────────────────────────
# ADD GMI-JR-003 — Ovi, this thread: `python src/runner.py --job bronze|silver|gold`
# for live testing one layer at a time without a full `--job all` run.
#
# Derived from WEEKLY_SEQUENCE (the superset — weekly-only jobs + all of
# DAILY_SEQUENCE, see above) rather than a separately hand-maintained list,
# so these three names can never drift from DAILY_SEQUENCE/WEEKLY_SEQUENCE
# the way a hand-copied list would (the same staleness class this project's
# own preflight/coverage work exists to catch).
#
# This means the deliberate exclusions already baked into the two sequences
# carry over automatically — NOT reproduced as a second list to maintain:
#   - bronze_bls_cpi/nfp,
#     bronze_bea_gdp          — registered but never sequenced; bronze_macro_weekly
#                                already covers BLS/BEA via FRED mirror (see
#                                _bronze_macro_weekly above) — manual-only jobs.
#
# NOTE (ADR-043, GMI_Decision_Document_v10.docx): bronze_finnhub,
# bronze_finnhub_sentiment, silver_fundamental, and silver_sentiment are NOT
# listed above because they no longer exist in JOB_REGISTRY at all — Finnhub
# was retired in full, not merely excluded from sequencing. This is a change
# from the prior state (bronze_finnhub / silver_fundamental existed but were
# deliberately unsequenced, per the note this replaces).
#
# health_report keeps layer="util", not "gold" — `--job gold` intentionally
# excludes it, matching the literal per-layer scope this was asked for.

def layer_sequence(layer: str) -> list[str]:
    """
    Return job names tagged `layer` in JOB_REGISTRY, ordered per their first
    position in WEEKLY_SEQUENCE (the ordered superset of every sequenced job
    — weekly-only jobs followed by DAILY_SEQUENCE), deduplicated. Jobs that
    exist in JOB_REGISTRY but are absent from both DAILY_SEQUENCE and
    WEEKLY_SEQUENCE (deliberately unsequenced — see module comment above)
    are excluded, matching `--job all`'s own scope.
    """
    seen: set[str] = set()
    ordered: list[str] = []
    for job_name in WEEKLY_SEQUENCE:
        if job_name in seen:
            continue
        seen.add(job_name)
        if JOB_REGISTRY[job_name]["layer"] == layer:
            ordered.append(job_name)
    return ordered


LAYER_JOB_NAMES: dict[str, list[str]] = {
    "bronze": layer_sequence("bronze"),
    "silver": layer_sequence("silver"),
    "gold":   layer_sequence("gold"),
}
