-- CardPulse initial schema. Idempotent: safe to re-run.

-- ---------- Operational ----------
CREATE TABLE IF NOT EXISTS data_pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    pipeline_name   TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    rows_read       INTEGER,
    rows_written    INTEGER,
    error_message   TEXT,
    metadata        JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS data_quality_alerts (
    alert_id        BIGSERIAL PRIMARY KEY,
    run_id          BIGINT REFERENCES data_pipeline_runs (run_id),
    check_name      TEXT        NOT NULL,
    severity        TEXT        NOT NULL DEFAULT 'warning'
                    CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    table_name      TEXT,
    message         TEXT        NOT NULL,
    details         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMPTZ
);

-- ---------- Raw layer (append-only, untouched source payloads) ----------
CREATE TABLE IF NOT EXISTS raw_card_catalog (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT        NOT NULL DEFAULT 'pokemontcg',
    source_card_id  TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    payload_hash    TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id          BIGINT REFERENCES data_pipeline_runs (run_id)
);
CREATE INDEX IF NOT EXISTS idx_raw_card_catalog_card ON raw_card_catalog (source, source_card_id);

CREATE TABLE IF NOT EXISTS raw_price_observation (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT        NOT NULL,
    source_card_ref TEXT,
    payload         JSONB       NOT NULL,
    observed_at     TIMESTAMPTZ,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id          BIGINT REFERENCES data_pipeline_runs (run_id)
);
CREATE INDEX IF NOT EXISTS idx_raw_price_observation_ref ON raw_price_observation (source, source_card_ref);

-- ---------- Staging layer (typed, cleaned, still source-shaped) ----------
CREATE TABLE IF NOT EXISTS stg_cards (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT        NOT NULL DEFAULT 'pokemontcg',
    source_card_id  TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    supertype       TEXT,
    subtypes        TEXT[]      NOT NULL DEFAULT '{}',
    set_id          TEXT,
    set_name        TEXT,
    series          TEXT,
    card_number     TEXT,
    rarity          TEXT,
    artist          TEXT,
    release_date    DATE,
    image_small_url TEXT,
    image_large_url TEXT,
    raw_id          BIGINT REFERENCES raw_card_catalog (id),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_card_id)
);

CREATE TABLE IF NOT EXISTS stg_price_observations (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT        NOT NULL,
    source_card_ref TEXT,
    raw_card_name   TEXT,
    raw_set_name    TEXT,
    raw_card_number TEXT,
    raw_grade_label TEXT,
    price           NUMERIC(12, 2),
    currency        CHAR(3)     NOT NULL DEFAULT 'USD',
    observed_at     TIMESTAMPTZ NOT NULL,
    raw_id          BIGINT REFERENCES raw_price_observation (id),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- Dimensions ----------
CREATE TABLE IF NOT EXISTS dim_cards (
    card_key        BIGSERIAL PRIMARY KEY,
    card_id         TEXT        NOT NULL UNIQUE,   -- Pokemon TCG API id, e.g. base1-4
    name            TEXT        NOT NULL,
    supertype       TEXT,
    subtypes        TEXT[]      NOT NULL DEFAULT '{}',
    set_id          TEXT,
    set_name        TEXT,
    series          TEXT,
    card_number     TEXT,
    rarity          TEXT,
    artist          TEXT,
    release_date    DATE,
    image_small_url TEXT,
    image_large_url TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dim_cards_name ON dim_cards (name);
CREATE INDEX IF NOT EXISTS idx_dim_cards_set ON dim_cards (set_id, card_number);

CREATE TABLE IF NOT EXISTS dim_grades (
    grade_id        SERIAL PRIMARY KEY,
    grading_company TEXT        NOT NULL,          -- RAW, PSA, BGS, CGC, ...
    grade           NUMERIC(3, 1),                 -- NULL for ungraded
    label           TEXT        NOT NULL UNIQUE,   -- e.g. 'RAW', 'PSA 10'
    is_graded       BOOLEAN     NOT NULL DEFAULT TRUE
);
INSERT INTO dim_grades (grading_company, grade, label, is_graded)
VALUES ('RAW', NULL, 'RAW', FALSE)
ON CONFLICT (label) DO NOTHING;

-- ---------- Facts ----------
CREATE TABLE IF NOT EXISTS fct_card_matches (
    match_id        BIGSERIAL PRIMARY KEY,
    stg_observation_id BIGINT NOT NULL REFERENCES stg_price_observations (id),
    card_key        BIGINT REFERENCES dim_cards (card_key),
    match_method    TEXT        NOT NULL,          -- exact, fuzzy, manual
    match_score     NUMERIC(5, 2),
    status          TEXT        NOT NULL DEFAULT 'auto'
                    CHECK (status IN ('auto', 'review', 'confirmed', 'rejected')),
    matched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fct_card_matches_obs ON fct_card_matches (stg_observation_id);

CREATE TABLE IF NOT EXISTS fct_price_observations (
    observation_id  BIGSERIAL PRIMARY KEY,
    card_key        BIGINT      NOT NULL REFERENCES dim_cards (card_key),
    grade_id        INTEGER     NOT NULL REFERENCES dim_grades (grade_id),
    source          TEXT        NOT NULL,
    price           NUMERIC(12, 2) NOT NULL,
    currency        CHAR(3)     NOT NULL DEFAULT 'USD',
    observed_at     TIMESTAMPTZ NOT NULL,
    stg_observation_id BIGINT REFERENCES stg_price_observations (id),
    run_id          BIGINT REFERENCES data_pipeline_runs (run_id)
);
CREATE INDEX IF NOT EXISTS idx_fct_price_obs_card_time
    ON fct_price_observations (card_key, grade_id, observed_at);

CREATE TABLE IF NOT EXISTS fct_daily_card_prices (
    card_key          BIGINT      NOT NULL REFERENCES dim_cards (card_key),
    grade_id          INTEGER     NOT NULL REFERENCES dim_grades (grade_id),
    price_date        DATE        NOT NULL,
    source            TEXT        NOT NULL,
    currency          CHAR(3)     NOT NULL DEFAULT 'USD',
    min_price         NUMERIC(12, 2),
    max_price         NUMERIC(12, 2),
    avg_price         NUMERIC(12, 2),
    median_price      NUMERIC(12, 2),
    observation_count INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (card_key, grade_id, price_date, source, currency)
);

CREATE TABLE IF NOT EXISTS fct_price_anomalies (
    anomaly_id      BIGSERIAL PRIMARY KEY,
    observation_id  BIGINT REFERENCES fct_price_observations (observation_id),
    card_key        BIGINT REFERENCES dim_cards (card_key),
    anomaly_type    TEXT        NOT NULL,
    severity        TEXT        NOT NULL DEFAULT 'warning',
    expected_price  NUMERIC(12, 2),
    observed_price  NUMERIC(12, 2),
    details         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at     TIMESTAMPTZ
);

-- ---------- Phase 2: catalog idempotency ----------
-- Raw rows are append-only versions: one row per distinct (card, payload hash).
ALTER TABLE raw_card_catalog ALTER COLUMN payload_hash SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_card_catalog_version
    ON raw_card_catalog (source, source_card_id, payload_hash);

-- ---------- Phase 3: price observations ----------
-- raw_price_observation: one append-only row per observation version.
ALTER TABLE raw_price_observation
    ADD COLUMN IF NOT EXISTS observation_key  TEXT,
    ADD COLUMN IF NOT EXISTS observation_type TEXT,
    ADD COLUMN IF NOT EXISTS price            NUMERIC(12, 4),
    ADD COLUMN IF NOT EXISTS currency         TEXT,
    ADD COLUMN IF NOT EXISTS payload_hash     TEXT;

ALTER TABLE raw_price_observation
    ALTER COLUMN source_card_ref  SET NOT NULL,
    ALTER COLUMN observed_at      SET NOT NULL,
    ALTER COLUMN observation_key  SET NOT NULL,
    ALTER COLUMN observation_type SET NOT NULL,
    ALTER COLUMN price            SET NOT NULL,
    ALTER COLUMN currency         SET NOT NULL,
    ALTER COLUMN payload_hash     SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_price_observation_version
    ON raw_price_observation (
        source, source_card_ref, observation_key, observation_type,
        observed_at, price, currency, payload_hash
    );

-- stg_price_observations: typed observation, one row per raw row. Unknown values stay NULL.
ALTER TABLE stg_price_observations
    ADD COLUMN IF NOT EXISTS observation_type TEXT,
    ADD COLUMN IF NOT EXISTS observation_key  TEXT,
    ADD COLUMN IF NOT EXISTS source_url       TEXT,
    ADD COLUMN IF NOT EXISTS condition_label  TEXT,
    ADD COLUMN IF NOT EXISTS language_label   TEXT,
    ADD COLUMN IF NOT EXISTS edition_label    TEXT,
    ADD COLUMN IF NOT EXISTS variant_label    TEXT,
    ADD COLUMN IF NOT EXISTS grading_company  TEXT,
    ADD COLUMN IF NOT EXISTS grade_value     NUMERIC(3, 1),
    ADD COLUMN IF NOT EXISTS is_sold          BOOLEAN,
    ADD COLUMN IF NOT EXISTS listing_id       TEXT;

ALTER TABLE stg_price_observations
    ALTER COLUMN price TYPE NUMERIC(12, 4),
    ALTER COLUMN currency DROP DEFAULT,  -- never silently assume USD
    ALTER COLUMN observation_type SET NOT NULL,
    ALTER COLUMN observation_key  SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_stg_price_observations_raw
    ON stg_price_observations (raw_id);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_raw_price_observation_type'
                     AND conrelid = 'raw_price_observation'::regclass) THEN
        ALTER TABLE raw_price_observation ADD CONSTRAINT ck_raw_price_observation_type
            CHECK (observation_type IN ('completed_sale', 'asking_price',
                   'provider_aggregate', 'auction_result', 'unknown'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_raw_price_observation_price'
                     AND conrelid = 'raw_price_observation'::regclass) THEN
        ALTER TABLE raw_price_observation ADD CONSTRAINT ck_raw_price_observation_price
            CHECK (price > 0);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_stg_price_observations_type'
                     AND conrelid = 'stg_price_observations'::regclass) THEN
        ALTER TABLE stg_price_observations ADD CONSTRAINT ck_stg_price_observations_type
            CHECK (observation_type IN ('completed_sale', 'asking_price',
                   'provider_aggregate', 'auction_result', 'unknown'));
    END IF;
END
$$;

-- ---------- Phase 4: entity resolution ----------
-- fct_card_matches: exactly one row per staged observation.
ALTER TABLE fct_card_matches
    ADD COLUMN IF NOT EXISTS explanation JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS run_id      BIGINT REFERENCES data_pipeline_runs (run_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fct_card_matches_observation
    ON fct_card_matches (stg_observation_id);
CREATE INDEX IF NOT EXISTS idx_fct_card_matches_status ON fct_card_matches (status);
CREATE INDEX IF NOT EXISTS idx_fct_card_matches_card ON fct_card_matches (card_key);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_fct_card_matches_method'
                     AND conrelid = 'fct_card_matches'::regclass) THEN
        ALTER TABLE fct_card_matches ADD CONSTRAINT ck_fct_card_matches_method
            CHECK (match_method IN ('deterministic', 'fuzzy', 'manual', 'none'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_fct_card_matches_score'
                     AND conrelid = 'fct_card_matches'::regclass) THEN
        ALTER TABLE fct_card_matches ADD CONSTRAINT ck_fct_card_matches_score
            CHECK (match_score IS NULL OR (match_score >= 0 AND match_score <= 100));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_fct_card_matches_card'
                     AND conrelid = 'fct_card_matches'::regclass) THEN
        ALTER TABLE fct_card_matches ADD CONSTRAINT ck_fct_card_matches_card
            CHECK (status NOT IN ('auto', 'confirmed') OR card_key IS NOT NULL);
    END IF;
END
$$;

-- ---------- Phase 5: pricing ----------
-- fct_daily_card_prices: one row per series per day. observation_type and observation_key
-- join the grain so different measures/types are never blended.
ALTER TABLE fct_daily_card_prices
    ADD COLUMN IF NOT EXISTS observation_type   TEXT,
    ADD COLUMN IF NOT EXISTS observation_key    TEXT,
    ADD COLUMN IF NOT EXISTS price_dispersion   NUMERIC(10, 6),
    ADD COLUMN IF NOT EXISTS latest_price       NUMERIC(12, 4),
    ADD COLUMN IF NOT EXISTS latest_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS freshness_days     INTEGER,
    ADD COLUMN IF NOT EXISTS as_of_date         DATE,
    ADD COLUMN IF NOT EXISTS computed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS run_id             BIGINT REFERENCES data_pipeline_runs (run_id);

ALTER TABLE fct_daily_card_prices
    ALTER COLUMN min_price    TYPE NUMERIC(12, 4),
    ALTER COLUMN max_price    TYPE NUMERIC(12, 4),
    ALTER COLUMN avg_price    TYPE NUMERIC(12, 4),
    ALTER COLUMN median_price TYPE NUMERIC(12, 4),
    ALTER COLUMN currency DROP DEFAULT,  -- never silently assume USD
    ALTER COLUMN observation_type SET NOT NULL,
    ALTER COLUMN observation_key  SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
        WHERE c.conrelid = 'fct_daily_card_prices'::regclass
          AND c.contype = 'p' AND a.attname = 'observation_key'
    ) THEN
        ALTER TABLE fct_daily_card_prices DROP CONSTRAINT fct_daily_card_prices_pkey;
        ALTER TABLE fct_daily_card_prices ADD PRIMARY KEY (
            card_key, grade_id, price_date, source, currency, observation_type, observation_key
        );
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS fct_card_market_values (
    card_key                  BIGINT   NOT NULL REFERENCES dim_cards (card_key),
    grade_id                  INTEGER  NOT NULL REFERENCES dim_grades (grade_id),
    as_of_date                DATE     NOT NULL,
    observation_type          TEXT     NOT NULL
        CHECK (observation_type IN ('completed_sale', 'asking_price', 'provider_aggregate',
                                    'auction_result', 'unknown')),
    currency                  CHAR(3)  NOT NULL,
    observation_key           TEXT     NOT NULL,
    methodology_version       TEXT     NOT NULL,
    price_7d_median           NUMERIC(12, 4),
    price_30d_median          NUMERIC(12, 4),
    price_7d_change_pct       NUMERIC(12, 4),
    price_30d_change_pct      NUMERIC(12, 4),
    observation_count_7d      INTEGER  NOT NULL DEFAULT 0,
    observation_count_30d     INTEGER  NOT NULL DEFAULT 0,
    price_dispersion          NUMERIC(10, 6),
    source_agreement_score    NUMERIC(5, 4),
    confidence_score          NUMERIC(5, 2) CHECK (confidence_score BETWEEN 0 AND 100),
    confidence_components     JSONB    NOT NULL DEFAULT '{}'::jsonb,
    liquidity_tier            TEXT,
    buyback_tier              TEXT,
    buyback_discount          NUMERIC(5, 4),
    recommended_market_value  NUMERIC(12, 2) CHECK (recommended_market_value >= 0),
    recommended_buy_price     NUMERIC(12, 2) CHECK (recommended_buy_price >= 0),
    recommended_retail_price  NUMERIC(12, 2) CHECK (recommended_retail_price >= 0),
    recommended_buyback_price NUMERIC(12, 2) CHECK (recommended_buyback_price >= 0),
    policy                    JSONB    NOT NULL DEFAULT '{}'::jsonb,
    latest_observed_at        TIMESTAMPTZ,
    computed_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id                    BIGINT REFERENCES data_pipeline_runs (run_id),
    PRIMARY KEY (card_key, grade_id, as_of_date, observation_type, currency,
                 observation_key, methodology_version)
);
CREATE INDEX IF NOT EXISTS idx_fct_card_market_values_asof ON fct_card_market_values (as_of_date);

-- fct_price_anomalies: rerunnable via dedupe_key (one row per finding per as-of date).
ALTER TABLE fct_price_anomalies
    ADD COLUMN IF NOT EXISTS as_of_date         DATE,
    ADD COLUMN IF NOT EXISTS stg_observation_id BIGINT REFERENCES stg_price_observations (id),
    ADD COLUMN IF NOT EXISTS source             TEXT,
    ADD COLUMN IF NOT EXISTS observation_type   TEXT,
    ADD COLUMN IF NOT EXISTS observation_key    TEXT,
    ADD COLUMN IF NOT EXISTS currency           TEXT,
    ADD COLUMN IF NOT EXISTS dedupe_key         TEXT,
    ADD COLUMN IF NOT EXISTS run_id             BIGINT REFERENCES data_pipeline_runs (run_id);

ALTER TABLE fct_price_anomalies
    ALTER COLUMN expected_price TYPE NUMERIC(12, 4),
    ALTER COLUMN observed_price TYPE NUMERIC(12, 4),
    ALTER COLUMN as_of_date SET NOT NULL,
    ALTER COLUMN dedupe_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_fct_price_anomalies_dedupe
    ON fct_price_anomalies (dedupe_key);
CREATE INDEX IF NOT EXISTS idx_fct_price_anomalies_card
    ON fct_price_anomalies (card_key, as_of_date);
CREATE INDEX IF NOT EXISTS idx_fct_price_anomalies_type
    ON fct_price_anomalies (anomaly_type, severity);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_fct_price_anomalies_type'
                     AND conrelid = 'fct_price_anomalies'::regclass) THEN
        ALTER TABLE fct_price_anomalies ADD CONSTRAINT ck_fct_price_anomalies_type
            CHECK (anomaly_type IN ('duplicate_observation', 'missing_price',
                   'non_positive_price', 'statistical_outlier', 'price_spike', 'price_drop',
                   'stale_data', 'conflicting_sources', 'missing_match',
                   'insufficient_observations'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_fct_price_anomalies_severity'
                     AND conrelid = 'fct_price_anomalies'::regclass) THEN
        ALTER TABLE fct_price_anomalies ADD CONSTRAINT ck_fct_price_anomalies_severity
            CHECK (severity IN ('info', 'warning', 'error', 'critical'));
    END IF;
END
$$;
