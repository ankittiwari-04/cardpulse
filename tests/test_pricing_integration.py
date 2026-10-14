"""Database integration tests for market values and anomaly detection.

Run with TEST_DATABASE_URL set. Each test works inside its own throwaway schema
(cp_test_<random>) that is dropped afterwards; no existing table or data is touched.
"""

import os
import uuid
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from src.config import Settings
from src.db.connection import normalize_dsn
from src.db.migrate import apply_schema
from src.pricing import anomalies as anomalies_module
from src.pricing import market_value as market_value_module
from src.pricing.anomalies import detect_anomalies
from src.pricing.market_value import compute_market_values

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL is not set; skipping database integration tests",
)

D = date(2026, 10, 14)


@pytest.fixture
def dsn():
    base = normalize_dsn(os.environ["TEST_DATABASE_URL"])
    schema = f"cp_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    scoped = make_conninfo(base, options=f"-c search_path={schema}")
    apply_schema(scoped)
    yield scoped
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def rows(dsn, query, params=()):
    with psycopg.connect(dsn) as conn:
        return conn.execute(query, params).fetchall()


def count(dsn, table):
    return rows(dsn, f"SELECT count(*) FROM {table}")[0][0]


_card_counter = iter(range(1, 1000))


def add_card(dsn):
    n = next(_card_counter)
    return rows(
        dsn,
        "INSERT INTO dim_cards (card_id, name, set_id, set_name, card_number) "
        "VALUES (%s, %s, 'base1', 'Base', %s) RETURNING card_key",
        (f"base1-{n}", f"Card {n}", str(n)),
    )[0][0]


def add_obs(
    dsn,
    card_key,
    *,
    days_ago=0,
    price=10.0,
    hour=0,
    key="tcgplayer/normal/market",
    obs_type="provider_aggregate",
    currency="USD",
    source="pokemontcg",
    match="auto",
    grading_company=None,
    grade_value=None,
):
    """match: 'auto' | 'confirmed' | 'review' | None (no match row at all)."""
    observed = datetime.combine(D - timedelta(days=days_ago), time(hour), tzinfo=UTC)
    obs_id = rows(
        dsn,
        "INSERT INTO stg_price_observations (source, source_card_ref, price, currency, "
        "observed_at, observation_type, observation_key, grading_company, grade_value) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
        (source, f"ref-{card_key}", price, currency, observed, obs_type, key,
         grading_company, grade_value),
    )[0][0]  # fmt: skip
    if match is not None:
        method = "none" if match == "review" else "deterministic"
        matched_card = None if match == "review" else card_key
        rows(
            dsn,
            "INSERT INTO fct_card_matches (stg_observation_id, card_key, match_method, "
            "match_score, status) VALUES (%s, %s, %s, 100, %s) RETURNING 1",
            (obs_id, matched_card, method, match),
        )
    return obs_id


def compute(dsn, **kwargs):
    return compute_market_values(Settings(_env_file=None), as_of_date=D, dsn=dsn, **kwargs)


def detect(dsn, **kwargs):
    return detect_anomalies(Settings(_env_file=None), as_of_date=D, dsn=dsn, **kwargs)


def run_row(dsn, run_id):
    return rows(
        dsn,
        "SELECT status, rows_read, rows_written, finished_at, error_message, metadata "
        "FROM data_pipeline_runs WHERE run_id = %s",
        (run_id,),
    )[0]


# ---------- daily aggregation ----------
def test_daily_prices_are_aggregated_per_series(dsn):
    card = add_card(dsn)
    for hour, price in ((1, 10), (2, 20), (3, 30)):
        add_obs(dsn, card, price=price, hour=hour)
    add_obs(dsn, card, price=5.5, key="tcgplayer/normal/low")
    add_obs(dsn, card, price=8, key="cardmarket/trendPrice", currency="EUR")

    result = compute(dsn)

    daily = rows(
        dsn,
        "SELECT observation_key, observation_type, currency, observation_count, min_price, "
        "max_price, avg_price, median_price, latest_price, freshness_days, grade_id "
        "FROM fct_daily_card_prices ORDER BY observation_key",
    )
    assert len(daily) == 3
    by_key = {r[0]: r for r in daily}
    market = by_key["tcgplayer/normal/market"]
    assert market[1:4] == ("provider_aggregate", "USD", 3)
    assert market[4:9] == (Decimal(10), Decimal(30), Decimal(20), Decimal(20), Decimal(30))
    assert market[9] == 0
    assert by_key["cardmarket/trendPrice"][2] == "EUR"
    raw_id = rows(dsn, "SELECT grade_id FROM dim_grades WHERE label = 'RAW'")[0][0]
    assert {r[10] for r in daily} == {raw_id}

    _, rows_read, rows_written, finished_at, error, meta = run_row(dsn, result.run_id)
    assert finished_at is not None and error is None
    assert meta["cards_processed"] == 1 and meta["observations_processed"] == 5
    assert meta["daily_price_rows_written"] == 3
    assert meta["market_value_rows_written"] == 2  # only the two headline series
    assert meta["anomalies_detected"] == 0 and meta["alerts_created"] == 0
    assert (rows_read, rows_written) == (5, 5)


# ---------- market values ----------
def test_market_value_row_confidence_and_policy(dsn):
    card = add_card(dsn)
    for days_ago in range(0, 12):
        add_obs(dsn, card, days_ago=days_ago, price=100 if days_ago >= 7 else 120)
        add_obs(dsn, card, days_ago=days_ago, price=3, key="tcgplayer/normal/low")

    compute(dsn)

    assert count(dsn, "fct_card_market_values") == 1  # the low-listing series gets no mart row
    (row,) = rows(
        dsn,
        "SELECT observation_type, currency, observation_key, methodology_version, "
        "price_7d_median, price_30d_median, price_7d_change_pct, price_30d_change_pct, "
        "observation_count_7d, observation_count_30d, source_agreement_score, "
        "confidence_score, confidence_components, liquidity_tier, buyback_tier, "
        "recommended_market_value, recommended_buy_price, recommended_retail_price, "
        "recommended_buyback_price FROM fct_card_market_values",
    )
    assert row[:4] == ("provider_aggregate", "USD", "tcgplayer/normal/market", "mv1")
    assert row[4:8] == (Decimal(120), Decimal(120), Decimal(20), None)
    assert row[8:10] == (7, 12)
    assert row[10] is None  # one source: agreement is not assessable
    assert float(row[11]) == pytest.approx(88.97, abs=0.02)
    components = row[12]
    assert set(components) >= {"observation_count", "recency", "dispersion",
                               "source_agreement", "completeness", "total"}  # fmt: skip
    assert components["source_agreement"]["score"] == 0.5
    assert row[13:15] == ("unknown", "low")  # provider data carries no volume evidence
    assert row[15:] == (Decimal("120.00"), Decimal("84.00"), Decimal("132.00"), Decimal("60.00"))


def test_only_matched_observations_and_each_observation_once(dsn):
    card = add_card(dsn)
    add_obs(dsn, card, price=10, hour=1)
    add_obs(dsn, card, price=20, hour=2, match="confirmed")
    add_obs(dsn, card, price=999, hour=3, match=None)
    add_obs(dsn, card, price=888, hour=4, match="review")
    add_obs(dsn, card, price=10, hour=1)  # exact duplicate of the first observation

    compute(dsn)

    (daily,) = rows(
        dsn, "SELECT observation_count, min_price, max_price FROM fct_daily_card_prices"
    )
    assert daily == (2, Decimal(10), Decimal(20))


def test_graded_observations_use_dim_grades_or_are_skipped(dsn):
    card = add_card(dsn)
    add_obs(dsn, card, price=10)
    add_obs(dsn, card, price=500, hour=1, grading_company="PSA", grade_value=10)
    first = compute(dsn)
    assert first.stats.observations_skipped == 1  # PSA 10 is not in dim_grades yet
    assert count(dsn, "fct_daily_card_prices") == 1

    grade_id = rows(
        dsn,
        "INSERT INTO dim_grades (grading_company, grade, label, is_graded) "
        "VALUES ('PSA', 10, 'PSA 10', TRUE) RETURNING grade_id",
    )[0][0]
    second = compute(dsn)
    assert second.stats.observations_skipped == 0
    graded = rows(dsn, "SELECT price_dispersion, median_price FROM fct_daily_card_prices "
                       "WHERE grade_id = %s", (grade_id,))  # fmt: skip
    assert graded == [(None, Decimal(500))]


def test_filters_limit_the_scope(dsn):
    a, b = add_card(dsn), add_card(dsn)
    add_obs(dsn, a)
    add_obs(dsn, b)
    add_obs(dsn, b, obs_type="asking_price", key="shop/listing")

    compute(dsn, card_key=a)
    assert {r[0] for r in rows(dsn, "SELECT card_key FROM fct_daily_card_prices")} == {a}

    compute(dsn, observation_type="asking_price")
    types = {r[0] for r in rows(dsn, "SELECT observation_type FROM fct_daily_card_prices")}
    assert types == {"provider_aggregate", "asking_price"}  # first run's row is still there
    assert count(dsn, "fct_daily_card_prices") == 2


# ---------- idempotency and dry-run ----------
def test_rerun_is_idempotent(dsn):
    card = add_card(dsn)
    for days_ago in range(5):
        add_obs(dsn, card, days_ago=days_ago, price=10 + days_ago)
    compute(dsn)
    before_daily = rows(dsn, "SELECT price_date, computed_at FROM fct_daily_card_prices ORDER BY 1")
    before_mv = rows(dsn, "SELECT computed_at FROM fct_card_market_values")

    second = compute(dsn)

    assert second.stats.daily_price_rows_written == 0
    assert second.stats.market_value_rows_written == 0
    assert second.stats.rows_unchanged == 5 + 1
    assert count(dsn, "fct_daily_card_prices") == 5
    assert count(dsn, "fct_card_market_values") == 1
    assert rows(dsn, "SELECT price_date, computed_at FROM fct_daily_card_prices ORDER BY 1") == (
        before_daily
    )
    assert rows(dsn, "SELECT computed_at FROM fct_card_market_values") == before_mv
    assert count(dsn, "data_pipeline_runs") == 2


def test_new_data_updates_only_what_changed(dsn):
    card = add_card(dsn)
    add_obs(dsn, card, days_ago=0, price=10)
    compute(dsn)
    add_obs(dsn, card, days_ago=1, price=12)
    third = compute(dsn)
    assert third.stats.daily_price_rows_written == 1  # the new day only
    assert third.stats.market_value_rows_written == 1  # the rolling value changed


def test_dry_run_writes_nothing(dsn):
    card = add_card(dsn)
    for days_ago in range(4):
        add_obs(dsn, card, days_ago=days_ago)
    add_obs(dsn, card, price=0, key="tcgplayer/normal/low")

    result = compute(dsn, dry_run=True)
    assert result.run_id is None
    assert result.stats.daily_rows_computed > 0 and result.stats.market_value_rows_computed == 1
    assert result.stats.daily_price_rows_written == 0

    findings = detect(dsn, dry_run=True)
    assert findings.run_id is None and findings.stats.anomalies_detected > 0
    for table in (
        "fct_daily_card_prices",
        "fct_card_market_values",
        "fct_price_anomalies",
        "data_quality_alerts",
        "data_pipeline_runs",
    ):
        assert count(dsn, table) == 0, table


def test_failure_is_tracked_with_a_sanitized_message(dsn, monkeypatch):
    add_obs(dsn, add_card(dsn))

    def boom(*_args, **_kwargs):
        raise RuntimeError("keys unavailable password=hunter2")

    monkeypatch.setattr(market_value_module, "list_card_keys", boom)
    with pytest.raises(RuntimeError):
        compute(dsn)
    status, rows_read, rows_written, finished_at, error, _ = rows(
        dsn,
        "SELECT status, rows_read, rows_written, finished_at, error_message, metadata "
        "FROM data_pipeline_runs",
    )[0]
    assert status == "failed" and finished_at is not None
    assert "keys unavailable" in error and "hunter2" not in error

    monkeypatch.setattr(anomalies_module, "list_card_keys", boom)
    with pytest.raises(RuntimeError):
        detect(dsn)
    assert [r[0] for r in rows(dsn, "SELECT status FROM data_pipeline_runs ORDER BY run_id")] == [
        "failed",
        "failed",
    ]


# ---------- anomalies ----------
def seed_anomaly_scenario(dsn):
    spike_card, stale_card, thin_card, bad_card = (add_card(dsn) for _ in range(4))
    for days_ago in range(5, 0, -1):
        add_obs(dsn, spike_card, days_ago=days_ago, price=10)
    add_obs(dsn, spike_card, days_ago=0, price=16)  # +60%: spike
    add_obs(dsn, stale_card, days_ago=12, price=10)  # 12 days old: stale (and thin)
    add_obs(dsn, thin_card, days_ago=0, price=10)
    add_obs(dsn, thin_card, days_ago=1, price=10)  # 2 observations: insufficient
    add_obs(dsn, thin_card, days_ago=0, price=10)  # identical stg row: duplicate
    add_obs(dsn, bad_card, days_ago=0, price=None)  # missing price
    add_obs(dsn, bad_card, days_ago=1, price=0, key="tcgplayer/normal/low")  # non-positive
    add_obs(dsn, bad_card, days_ago=2, price=10, match=None)  # no match row
    add_obs(dsn, bad_card, days_ago=3, price=10, match="review")  # review-only match
    return spike_card


def anomaly_counts(dsn):
    return dict(
        ((t, s), n)
        for t, s, n in rows(
            dsn,
            "SELECT anomaly_type, severity, count(*) FROM fct_price_anomalies GROUP BY 1, 2",
        )
    )


def test_anomaly_detection_end_to_end_and_alerts(dsn):
    spike_card = seed_anomaly_scenario(dsn)

    result = detect(dsn)

    assert anomaly_counts(dsn) == {
        ("price_spike", "warning"): 1,
        ("stale_data", "warning"): 1,
        ("insufficient_observations", "info"): 2,
        ("duplicate_observation", "info"): 1,
        ("missing_price", "error"): 1,
        ("non_positive_price", "error"): 1,
        ("missing_match", "warning"): 1,
        ("missing_match", "info"): 1,
    }
    spike = rows(
        dsn,
        "SELECT card_key, expected_price, observed_price, detected_at, as_of_date "
        "FROM fct_price_anomalies WHERE anomaly_type = 'price_spike'",
    )[0]
    assert spike[:3] == (spike_card, Decimal(10), Decimal(16))
    assert spike[3] is not None and spike[4] == D

    _, _, rows_written, _, _, meta = run_row(dsn, result.run_id)
    assert meta["anomalies_detected"] == 9 and meta["alerts_created"] == 2
    assert rows_written == 9 and meta["anomalies_by_type"]["missing_match"] == 2
    alerts = rows(dsn, "SELECT check_name, severity, run_id FROM data_quality_alerts ORDER BY 1")
    assert alerts == [
        ("missing_price", "error", result.run_id),
        ("non_positive_price", "error", result.run_id),
    ]


def test_anomaly_rerun_is_idempotent_and_does_not_duplicate_alerts(dsn):
    seed_anomaly_scenario(dsn)
    detect(dsn)
    detected_at = rows(dsn, "SELECT anomaly_id, detected_at FROM fct_price_anomalies ORDER BY 1")

    second = detect(dsn)

    assert second.stats.anomalies_detected == 9
    assert second.stats.anomaly_rows_written == 0 and second.stats.alerts_created == 0
    assert count(dsn, "fct_price_anomalies") == 9
    assert count(dsn, "data_quality_alerts") == 2
    assert rows(dsn, "SELECT anomaly_id, detected_at FROM fct_price_anomalies ORDER BY 1") == (
        detected_at
    )


def test_anomaly_table_constraints(dsn):
    card = add_card(dsn)
    insert = (
        "INSERT INTO fct_price_anomalies (card_key, anomaly_type, severity, as_of_date, "
        "dedupe_key) VALUES (%s, %s, %s, %s, %s) RETURNING 1"
    )
    rows(dsn, insert, (card, "price_spike", "warning", D, "k1"))
    with pytest.raises(psycopg.errors.UniqueViolation):
        rows(dsn, insert, (card, "price_spike", "warning", D, "k1"))
    with pytest.raises(psycopg.errors.CheckViolation):
        rows(dsn, insert, (card, "not_a_real_type", "warning", D, "k2"))
    with pytest.raises(psycopg.errors.CheckViolation):
        rows(dsn, insert, (card, "price_spike", "catastrophic", D, "k3"))
