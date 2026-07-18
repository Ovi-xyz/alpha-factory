# Makefile — Data Platform
# Usage: make <target>

.PHONY: help setup migrate validate test test-unit test-int coverage \
        lint run-daily run-weekly status reset-all dashboard delta-check \
        clean install docs

PYTHON   := python
PYTEST   := python -m pytest
SRC      := src
TESTS    := tests

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "Data Platform — Available Commands"
	@echo "=========================================================="
	@echo ""
	@echo "SETUP"
	@echo "  make setup          Create conda env (interpreter only) + poetry install + copy .env.example"
	@echo "  make install        Install dependencies via Poetry (poetry install --with dev)"
	@echo "  make migrate        Run instruments migration (once)"
	@echo "  make validate       Validate instruments.yaml (699 symbols)"
	@echo ""
	@echo "TESTING"
	@echo "  make test           Run all tests (unit + integration)"
	@echo "  make test-unit      Run unit tests only"
	@echo "  make test-int       Run integration tests only"
	@echo "  make coverage       Run tests with coverage report"
	@echo ""
	@echo "PIPELINE"
	@echo "  make run-daily      Run full daily pipeline (--job all)"
	@echo "  make run-weekly     Run weekly jobs (macro + correlation)"
	@echo "  make status         Show today's job completion status"
	@echo "  make reset-all      Reset all sentinels (force full re-run)"
	@echo "  make dashboard      Show pipeline health dashboard"
	@echo ""
	@echo "MAINTENANCE"
	@echo "  make delta-check    Find stale Silver files (dry-run)"
	@echo "  make delta-fix      Reprocess stale Silver files"
	@echo "  make lint           Run ruff linter"
	@echo "  make clean          Remove Python cache files"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────
# UPD Decision A (GMI_Decision_Document_v3.docx): environment.yml now only
# provisions the interpreter (python=3.11) + pip itself — it no longer
# installs any package directly, so `poetry install --with dev` is now a
# required second step, not optional. See environment.yml header for why
# (tvdatafeed>=2.0 doesn't exist on PyPI and was poisoning the whole
# conda pip: block atomically).
setup:
	@echo "Creating conda environment (interpreter + isolated shell only)..."
	conda env create -f environment.yml || conda env update -f environment.yml
	@echo ""
	@echo "Installing dependencies via Poetry..."
	poetry install --with dev
	@echo ""
	@echo "Copying .env template..."
	@test -f .env || cp .env.example .env
	@echo "Edit .env and add your API keys, then run: make migrate"

# UPD Decision A: repointed at the same Poetry command CI now uses, instead
# of a separately hand-maintained pip list. The old list had already
# drifted from pyproject.toml/environment.yml in its own direction: it
# still said `pandas-ta` (not `-classic`, ADR-020) and was missing scipy /
# statsmodels (ADR-021) and tvdatafeed entirely.
install:
	poetry install --with dev

migrate:
	@echo "Migrating instruments_raw.py → config/instruments.yaml..."
	$(PYTHON) scripts/migrate_instruments.py

validate:
	@echo "Validating instruments.yaml..."
	$(PYTHON) scripts/validate_instruments.py

# ── Testing ───────────────────────────────────────────────────────────────────
test:
	$(PYTEST) $(TESTS)/ -v

test-unit:
	$(PYTEST) $(TESTS)/unit/ -v

test-int:
	$(PYTEST) $(TESTS)/integration/ -v

test-smoke:
	$(PYTEST) $(TESTS)/integration/test_end_to_end_smoke.py -v

coverage:
	$(PYTEST) $(TESTS)/ \
	    --cov=$(SRC) \
	    --cov-report=term-missing \
	    --cov-report=html:htmlcov \
	    -q
	@echo ""
	@echo "HTML coverage report: htmlcov/index.html"

# ── Pipeline Operations ───────────────────────────────────────────────────────
run-daily:
	$(PYTHON) $(SRC)/runner.py --job all

run-weekly:
	$(PYTHON) $(SRC)/runner.py --job bronze_macro_weekly
	$(PYTHON) $(SRC)/runner.py --job silver_macro
	$(PYTHON) $(SRC)/runner.py --job gold_correlation

status:
	$(PYTHON) $(SRC)/runner.py --status

reset-all:
	@echo "Resetting all today's sentinels..."
	$(PYTHON) $(SRC)/runner.py --reset-all

dashboard:
	$(PYTHON) -m src.utils.pipeline_dashboard

# ── Individual Jobs ───────────────────────────────────────────────────────────
bronze:
	$(PYTHON) $(SRC)/runner.py --job bronze_ohlcv_daily
	$(PYTHON) $(SRC)/runner.py --job bronze_finnhub
	$(PYTHON) $(SRC)/runner.py --job bronze_treasury

silver:
	$(PYTHON) $(SRC)/runner.py --job silver_ohlcv
	$(PYTHON) $(SRC)/runner.py --job silver_validate
	$(PYTHON) $(SRC)/runner.py --job silver_active_symbols
	$(PYTHON) $(SRC)/runner.py --job silver_sentiment

gold:
	$(PYTHON) $(SRC)/runner.py --job gold_signals
	$(PYTHON) $(SRC)/runner.py --job gold_mtf
	$(PYTHON) $(SRC)/runner.py --job gold_regime
	$(PYTHON) $(SRC)/runner.py --job gold_sector
	$(PYTHON) $(SRC)/runner.py --job gold_screener

# ── Maintenance ───────────────────────────────────────────────────────────────
delta-check:
	$(PYTHON) -m src.utils.delta_reprocessor --dry-run

delta-fix:
	$(PYTHON) -m src.utils.delta_reprocessor

lint:
	@which ruff > /dev/null 2>&1 && ruff check $(SRC)/ || \
	    echo "ruff not installed. Run: pip install ruff"

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cache cleaned"

# ── Data Management ───────────────────────────────────────────────────────────
list-jobs:
	$(PYTHON) $(SRC)/runner.py --list

backfill:
	@read -p "Enter date to backfill (YYYY-MM-DD): " d; \
	$(PYTHON) $(SRC)/runner.py --job all --date $$d

health:
	$(PYTHON) $(SRC)/runner.py --job health_report --force
