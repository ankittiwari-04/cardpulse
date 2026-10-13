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
