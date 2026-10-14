PY ?= python
ARGS ?= --max-pages 1
MATCH_ARGS ?= --limit 1000
PRICING_ARGS ?= --as-of-date $(shell date +%F)

.PHONY: install fmt lint test db-migrate run-api ingest-catalog ingest-prices match-cards compute-prices detect-anomalies test-integration

install:
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[dev]"

fmt:
	ruff format .
	ruff check --fix .

lint:
	ruff check .
	ruff format --check .

test:
	$(PY) -m pytest -q

db-migrate:
	$(PY) -m src.db.migrate

run-api:
	$(PY) -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

ingest-catalog:
	$(PY) -m src.ingestion.catalog $(ARGS)

ingest-prices:
	$(PY) -m src.ingestion.prices $(ARGS)

match-cards:
	$(PY) -m src.matching.resolve $(MATCH_ARGS)

compute-prices:
	$(PY) -m src.pricing.compute $(PRICING_ARGS)

detect-anomalies:
	$(PY) -m src.pricing.anomalies $(PRICING_ARGS)

test-integration:
	@test -n "$$TEST_DATABASE_URL" || { echo "Set TEST_DATABASE_URL first"; exit 1; }
	$(PY) -m pytest -q -rs tests/test_catalog_integration.py
