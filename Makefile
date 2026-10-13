PY ?= python
ARGS ?= --max-pages 1

.PHONY: test-integration install fmt lint test db-migrate run-api ingest-catalog ingest-prices

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
	@echo "ingest-prices is not implemented yet (planned for a later phase)."
	@exit 1

test-integration:
	@test -n "$$TEST_DATABASE_URL" || { echo "Set TEST_DATABASE_URL first"; exit 1; }
	$(PY) -m pytest -q -rs tests/test_catalog_integration.py
