# CardPulse

Pokemon trading-card pricing and data-quality platform.

## Goal

Ingest the Pokemon card catalog and third-party price observations, match
messy price listings to canonical cards, publish clean daily prices, and
surface data-quality problems (bad matches, price anomalies, pipeline failures).

## Architecture

    Pokemon TCG API ──┐
                      ├─> raw_*  ->  stg_*  ->  dim_* / fct_*  ->  FastAPI
    Price API ────────┘      (Postgres, layered)       ^
                                                       |
                       Prefect scheduler ──────────────┘
                       data_pipeline_runs / data_quality_alerts

- raw_*: append-only source payloads (JSONB)
- stg_*: typed, cleaned, still source-shaped
- dim_* / fct_*: canonical cards, grades, matches, observations, daily prices, anomalies
- Services: postgres, api (FastAPI), scheduler (Prefect placeholder)

## Local setup (WSL)

    python3 -m venv .venv && source .venv/bin/activate
    cp .env.example .env
    make install
    docker compose up -d db
    make db-migrate
    make run-api            # http://localhost:8000/health
    make test
    make ingest-catalog     # ARGS="--max-pages 0" to fetch everything

Full stack in containers: docker compose up --build

The Pokemon TCG API works without a key; POKEMON_TCG_API_KEY only raises the rate limit.

## Implementation phases

1. Scaffold, schema, catalog client (this commit)
2. Persist catalog: raw_card_catalog -> stg_cards -> dim_cards, with pipeline run tracking
3. Price ingestion into raw_price_observation / stg_price_observations
4. Entity matching (rapidfuzz) into fct_card_matches
5. Fact loading, daily aggregates, anomaly detection, quality alerts
6. Prefect flows and schedules, API endpoints, Streamlit dashboard, Alembic migrations

## Catalog persistence (Phase 2)

### Data flow

    Pokemon TCG API (one page at a time)
        -> raw_card_catalog   original card JSON, append-only, one row per distinct version
        -> stg_cards          typed/cleaned, one row per (source, source_card_id), linked via raw_id
        -> dim_cards          canonical card design, one row per card_id

dim_cards describes card designs only. No language, edition, finish, condition or
grading-variant fields are populated.

### Idempotency rules

- raw: payload_hash = SHA-256 of json.dumps(payload, sort_keys=True,
  separators=(",", ":"), ensure_ascii=False). Unique index on (source, source_card_id,
  payload_hash). Identical payload -> existing row reused. Changed payload -> new raw
  row. Raw rows are never updated or deleted.
- staging: upsert on (source, source_card_id); a row is only written when a value
  (including raw_id) actually changes.
- dimension: upsert on card_id from staging; updated_at only moves when a value changes.
- Reruns are safe. A rerun of unchanged data writes nothing.

### Transactions

Each page is fetched first, then raw + staging + dimension rows for that page are
written in one transaction and committed. If a later page fails, earlier pages stay
committed; just rerun.

### Pipeline run tracking

Each run inserts a data_pipeline_runs row (status running) before fetching, then
finishes it as succeeded or failed in a separate transaction. Failures store a
sanitized error_message (no credentials) and re-raise. rows_read = cards fetched;
rows_written = rows inserted or updated across raw, staging and dimension. Detailed
counts are in metadata (raw_inserted, raw_reused, staging_inserted, staging_updated,
dimension_inserted, dimension_updated, ...).

### Commands

    make db-migrate
    make ingest-catalog ARGS="--max-pages 1 --query set.id:base1"
    make test                      # unit tests; DB integration tests skip without TEST_DATABASE_URL
    export TEST_DATABASE_URL=postgresql+psycopg://cardpulse:cardpulse@localhost:5432/cardpulse_test
    make test-integration          # each test uses a throwaway schema, dropped afterwards

### Verification SQL

    SELECT run_id, status, rows_read, rows_written, metadata FROM data_pipeline_runs ORDER BY run_id DESC LIMIT 5;
    SELECT count(*) AS raw_rows, count(DISTINCT source_card_id) AS cards FROM raw_card_catalog;
    SELECT count(*) FROM stg_cards;
    SELECT count(*) FROM dim_cards;
    SELECT source_card_id, count(*) FROM raw_card_catalog GROUP BY 1 HAVING count(*) > 1;
