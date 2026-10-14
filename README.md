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

## Price ingestion (Phase 3)

### Data flow

    price source adapter (e.g. Pokemon TCG API tcgplayer / cardmarket blocks)
        -> validate every observation (pure rules, no I/O)
        -> raw_price_observation   verbatim JSON, append-only, one row per observation version
        -> stg_price_observations  typed row per raw row (unknown values stay NULL)
        -> data_quality_alerts     one alert per rule violation; rejected rows are NOT stored

No matching to canonical cards happens in this phase: source_card_ref is only the
source's own card reference.

### Observation types

completed_sale, asking_price, provider_aggregate, auction_result, unknown.
The pokemontcg adapter emits only provider_aggregate. The type is preserved exactly as
the adapter reports it, and a completed_sale is only accepted when the source explicitly
marks it sold (is_sold = true).

### Why sold prices and asking prices differ

An asking price is what a seller hopes for; a completed sale is what a buyer actually
paid. Asking prices drift above sale prices, especially for high-demand cards, so mixing
them biases any price series. Provider aggregates (market, trend, average, low) are
statistics computed by the provider and say nothing about individual sales. CardPulse
keeps the type on every row so later phases never have to guess.

### Raw immutability and idempotency

- raw_price_observation.payload is the original JSON object from the response (for the
  pokemontcg adapter: the tcgplayer or cardmarket block, unmodified).
- payload_hash = SHA-256 of canonical JSON (sorted keys, compact separators).
- Unique index: (source, source_card_ref, observation_key, observation_type, observed_at,
  price, currency, payload_hash). observation_key (e.g. tcgplayer/normal/market) is part
  of it so two variants of one card with equal prices are not collapsed.
- Same observation fetched again -> the existing row is reused. Changed observation ->
  a new raw row. Raw rows are never updated or deleted.
- stg_price_observations has one row per raw row (unique raw_id); reruns insert nothing.

### Validation rules (rejected observations are recorded in data_quality_alerts)

- missing_source_card_ref, missing_observation_key
- missing_observed_at (also: timestamps without a timezone; they are never guessed;
  date-only values are stored as 00:00 UTC, so precision is one day)
- missing_price, invalid_price, non_positive_price
- invalid_currency (must be 3 uppercase letters)
- invalid_observation_type, unsupported_sale_claim
- impossible_grade (1-10 in half-point steps), only checked when the source gives a grade
- duplicate_listing_id (same source listing id twice within one run)

Grading company, grade, condition, language and edition are only stored when the source
provides them. The pokemontcg adapter provides none of them.

### Pipeline run tracking

Each non-dry run inserts a data_pipeline_runs row (pipeline_name = price_ingest) before
fetching. rows_read = observations extracted; rows_written = newly inserted raw + staging
rows. metadata holds raw_inserted, raw_reused, staging_inserted, staging_unchanged,
rejected_count, alert_count, alert_checks (counts per rule), pages_fetched and
items_without_price_data (cards with no price block). Failures are recorded in their own
transaction with a sanitized error message.

### Configuration (.env)

    PRICE_SOURCE_NAME=pokemontcg         # adapter name
    PRICE_API_BASE_URL=                  # blank = adapter default
    PRICE_API_KEY=                       # blank = pokemontcg adapter uses POKEMON_TCG_API_KEY
    PRICE_REQUEST_TIMEOUT_SECONDS=30
    PRICE_MAX_PAGES=1                    # 0 = all pages; one page = up to 250 cards

To add a provider, implement PriceSourceClient (src/ingestion/prices.py) and register it
in ADAPTERS.

### Commands

    make ingest-prices ARGS="--dry-run --max-pages 1"       # fetch + validate, write nothing
    make ingest-prices ARGS="--max-pages 1 --query set.id:base1"
    python -m src.ingestion.prices --max-pages 1
    make test
    export TEST_DATABASE_URL=postgresql+psycopg://cardpulse:cardpulse@localhost:5432/cardpulse_test
    make test-integration

### Verification SQL

    SELECT run_id, status, rows_read, rows_written, metadata FROM data_pipeline_runs
      WHERE pipeline_name = 'price_ingest' ORDER BY run_id DESC LIMIT 5;
    SELECT observation_type, currency, count(*) FROM raw_price_observation GROUP BY 1, 2;
    SELECT check_name, count(*) FROM data_quality_alerts GROUP BY 1 ORDER BY 2 DESC;
    SELECT count(*) FROM stg_price_observations s LEFT JOIN raw_price_observation r
      ON r.id = s.raw_id WHERE r.id IS NULL;

## Entity resolution (Phase 4)

### Matching flow

    stg_price_observations (no match row yet)
        -> stage 1  deterministic: source_card_ref == dim_cards.card_id (authoritative sources)
        -> stage 2  attribute matching: normalize -> candidates -> score -> threshold
        -> fct_card_matches  exactly one row per observation, with an explanation (JSONB)

Run: make match-cards MATCH_ARGS="--limit 1000" or python -m src.matching.resolve --limit 1000
Flags: --limit (0 = all), --source, --dry-run (read-only transaction, writes nothing),
--reprocess (also reconsider review rows), --batch-size.

### Deterministic versus fuzzy

Deterministic matching is preferred: when a source's own reference IS the canonical card
id (pokemontcg), a lookup is exact, instant and cannot be wrong because of spelling,
reordering or ambiguous set names. Fuzzy matching exists for sources without such ids
(marketplace titles). match_method = deterministic only for the source-reference stage.
Every other accepted match is recorded as fuzzy, including exact name + set + number;
the rule in explanation says which it was. If a pokemontcg reference is not in dim_cards,
attributes are NOT allowed to override it: the observation goes to review.

### Hard constraints

If the observation states a set, only cards of that set are candidates. If it states a
number, only cards with that number are candidates. A similar name never overrides a
conflicting set or number; such cards are listed as excluded (with the reason) in the
explanation. "Base Set" and "Base" are the same set; "Base Set 2" is a different one.

### Confidence (0-100) and thresholds

    source reference                          100
    set AND number exact                      50 + 0.45 * name_similarity   (exact name = 95)
    only one of set / number known            30 + 0.40 * name_similarity   (max 70)
    name only                                 0.60 * name_similarity        (max 60)

name_similarity is RapidFuzz token_sort_ratio on normalized names (0-100).
MATCH_AUTO_THRESHOLD (default 90): at or above this a candidate is accepted (status auto).
MATCH_REVIEW_MIN_SCORE (default 50): below this a candidate is not even suggested.
Two candidates within 2 points of each other are never auto-accepted (rule: ambiguous).
Only set + number exact matches can reach the auto threshold.

### Review queue

Observations that are not accepted get status review, match_method none and card_key NULL.
The best candidates (score, rule, name similarity) and the near-misses excluded by
conflicts are in explanation. match_score holds the best candidate's score (NULL when there
was no candidate). Humans resolve a row by setting status confirmed (with a card_key) or
rejected. confirmed and rejected rows are never changed by the pipeline; --reprocess only
revisits review rows, and the write SQL itself only updates rows whose status is review.

### Normalization

Language, edition and grade labels are normalized and recorded in the explanation
(en / 1st_edition / PSA 10 ...) but never used to choose the card, because dim_cards holds
card designs only. Unknown or unparseable values stay unknown; nothing is guessed.

### Limitations

- Marketplace titles must already be split into name, set and number fields; free-text
  title parsing is not implemented.
- Set matching is exact on normalized names; there is no alias table (e.g. "BS" for Base).
- Name similarity ignores language; non-English card names are not translated.
- The printed total in "4/102" is dropped, not used as a set signal.
- Only card designs are matched; variants, finishes and gradings are not part of the match.

### Verification SQL

    SELECT status, match_method, count(*) FROM fct_card_matches GROUP BY 1, 2 ORDER BY 1, 2;
    SELECT count(*) AS unmatched_observations FROM stg_price_observations s
      LEFT JOIN fct_card_matches m ON m.stg_observation_id = s.id WHERE m.match_id IS NULL;
    SELECT stg_observation_id, count(*) FROM fct_card_matches GROUP BY 1 HAVING count(*) > 1;
    SELECT m.stg_observation_id, s.raw_card_name, s.raw_set_name, s.raw_card_number,
           m.match_score, m.explanation->>'rule' AS rule
      FROM fct_card_matches m JOIN stg_price_observations s ON s.id = m.stg_observation_id
      WHERE m.status = 'review' ORDER BY m.match_score DESC NULLS LAST LIMIT 20;
    SELECT run_id, status, rows_read, rows_written, metadata FROM data_pipeline_runs
      WHERE pipeline_name = 'entity_resolution' ORDER BY run_id DESC LIMIT 3;

## Internal API and dashboard (Phase 6)

### Run the full local stack (WSL)

    sudo service postgresql start                      # PostgreSQL at localhost:5432
    source .venv/bin/activate
    make db-migrate
    make ingest-catalog ARGS="--max-pages 1 --query set.id:base1"
    make ingest-prices ARGS="--max-pages 1 --query set.id:base1"
    make match-cards
    make compute-prices PRICING_ARGS="--as-of-date $(date -u +%F)"
    make detect-anomalies PRICING_ARGS="--as-of-date $(date -u +%F)"
    make run-api            # terminal 1: http://localhost:8000  (docs at /docs)
    make run-dashboard      # terminal 2: http://localhost:8501

The dashboard reads the API at DASHBOARD_API_URL (default http://localhost:8000), so start the
API first. The API has no authentication and binds 0.0.0.0 (see run-api); keep it on your own
machine or network, or run uvicorn with --host 127.0.0.1.

### API

All endpoints are read-only and use a short-lived read-only database connection per request.
List endpoints return {"total", "limit", "offset", "items"}. Errors: 404 unknown card (or no
market value for the selection), 422 invalid parameters, 503 database unavailable.

    curl -s localhost:8000/health
    curl -s "localhost:8000/cards/search?q=charizard&set_name=base&limit=5&offset=0"
    curl -s localhost:8000/cards/1
    curl -s "localhost:8000/cards/1/market-value?currency=USD&observation_type=provider_aggregate"
    curl -s "localhost:8000/cards/1/market-value?as_of_date=2026-10-14&currency=EUR"
    curl -s "localhost:8000/cards/1/price-history?days=90&currency=USD"
    curl -s "localhost:8000/cards/1/observations?limit=50&offset=0"
    curl -s "localhost:8000/anomalies?severity=warning&anomaly_type=stale_data&limit=50"
    curl -s localhost:8000/data-quality/summary

    GET /health                         status, service, version, database, timestamp (503 if the DB is down)
    GET /cards/search                   q, set_name, limit (default 20, max 100), offset
    GET /cards/{card_key}               card details + latest_market_values
    GET /cards/{card_key}/market-value  as_of_date (default: latest), observation_type, currency (default USD)
    GET /cards/{card_key}/price-history days (default 90, max 365), observation_type, currency
    GET /cards/{card_key}/observations  limit (default 50, max 200), offset (matched observations only)
    GET /anomalies                      limit (default 50, max 200), offset, severity, anomaly_type, card_key
    GET /data-quality/summary           counts, by source/type, latest successful runs, freshness, open anomalies

A card has several price series (for example tcgplayer/normal/market in USD and
cardmarket/trendPrice in EUR), so market-value returns a values list with one entry per series.
Each entry carries observation_type, observation_type_note, confidence_score,
confidence_components, methodology_version and data_freshness. Without as_of_date the latest
available date for the card is used; an explicit date returns exactly that date or 404. Price
history ends at the card's latest price_date and contains only stored days.
unmatched_observations = observations that are neither matched nor in review (no match row, or
rejected).

### Dashboard

make run-dashboard starts Streamlit on port 8501. Use the sidebar to search for a card and pick
one; the tabs show the market value (7- and 30-day medians, changes, buyback/buy/retail
estimates, confidence with each component and its explanation), the stored price history,
recent matched observations, anomalies and data-quality counts. Pricing logic is not repeated
in the dashboard; it only displays what the API returns.

### Data limitations and disclaimer

- Every current price is a provider_aggregate: a statistic published by TCGplayer or Cardmarket
  through the Pokémon TCG API. It is NOT a completed sale and carries no transaction volume.
  The API and the dashboard never call these prices sold prices.
- History is only what the pipelines have stored. Dates without data are not filled in, and no
  history is back-filled; with one ingestion run there is one day of history.
- Buy, retail and buyback prices are policy-based estimates, not offers.
- Confidence reflects data quality (count, recency, dispersion, agreement, completeness), not
  the probability that a price is correct.
- With a single source, source agreement cannot be assessed.
- The API is internal: no authentication, no rate limiting, no caching.

### Window semantics (pricing)

    age = as-of date - observation date, in whole UTC days (the as-of day has age 0)
    windows are half-open on age: start_age <= age < end_age
    7d window    0 <= age < 7        30d window     0 <= age < 30
    previous 7d  7 <= age < 14       previous 30d  30 <= age < 60
    the same windows feed the medians, the counts, the change percentages and the
    confidence recency/coverage inputs
