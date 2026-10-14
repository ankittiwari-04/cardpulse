"""API tests against a real (throwaway-schema) database.

Run with TEST_DATABASE_URL set. Each test works inside its own throwaway schema
(cp_test_<random>) that is dropped afterwards; no existing table or data is touched.
"""

import os
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.api.deps import check_database, get_db
from src.db.connection import normalize_dsn, transaction
from src.db.migrate import apply_schema
from src.main import app

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL is not set; skipping database integration tests",
)

D = date(2026, 10, 14)


def at(day: date, hour: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


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


@pytest.fixture
def api(dsn):
    def db():
        with transaction(dsn, read_only=True, row_factory=dict_row) as conn:
            yield conn

    def healthy():
        with transaction(dsn, read_only=True) as conn:
            conn.execute("SELECT 1")
        return True

    app.dependency_overrides[get_db] = db
    app.dependency_overrides[check_database] = healthy
    yield TestClient(app)
    app.dependency_overrides.clear()


def rows(dsn, query, params=()):
    with psycopg.connect(dsn) as conn:
        return conn.execute(query, params).fetchall()


def add_card(dsn, card_id, name, set_name="Base", number="1", rarity="Rare Holo", set_id="base1"):
    return rows(
        dsn,
        "INSERT INTO dim_cards (card_id, name, set_id, set_name, card_number, rarity, "
        "image_small_url) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING card_key",
        (card_id, name, set_id, set_name, number, rarity, f"https://img.test/{card_id}.png"),
    )[0][0]


def raw_grade(dsn):
    return rows(dsn, "SELECT grade_id FROM dim_grades WHERE label = 'RAW'")[0][0]


def add_market_value(
    dsn, card_key, as_of, *, key="tcgplayer/normal/market", currency="USD",
    obs_type="provider_aggregate", m7="120", m30="110", c7="20", n7=7, n30=12,
):  # fmt: skip
    components = {
        "observation_count": {"score": 1.0, "weight": 0.25, "weighted": 0.25, "explanation": "x"},
        "total": {"score": 88.97},
    }
    rows(
        dsn,
        "INSERT INTO fct_card_market_values (card_key, grade_id, as_of_date, observation_type, "
        "currency, observation_key, methodology_version, price_7d_median, price_30d_median, "
        "price_7d_change_pct, observation_count_7d, observation_count_30d, confidence_score, "
        "confidence_components, liquidity_tier, buyback_tier, buyback_discount, "
        "recommended_market_value, recommended_buy_price, recommended_retail_price, "
        "recommended_buyback_price, policy, latest_observed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, 'mv1', %s, %s, %s, %s, %s, 88.97, %s, 'unknown', "
        "'low', 0.5, %s, 84, 132, 60, %s, %s) RETURNING 1",
        (card_key, raw_grade(dsn), as_of, obs_type, currency, key, Decimal(m7), Decimal(m30),
         Decimal(c7), n7, n30, Jsonb(components), Decimal(m7), Jsonb({"basis_window": "7d"}),
         at(as_of)),
    )  # fmt: skip


def add_daily(dsn, card_key, day, *, median="10", key="tcgplayer/normal/market",
              currency="USD", obs_type="provider_aggregate"):  # fmt: skip
    rows(
        dsn,
        "INSERT INTO fct_daily_card_prices (card_key, grade_id, price_date, source, currency, "
        "observation_type, observation_key, min_price, max_price, avg_price, median_price, "
        "observation_count, latest_price, latest_observed_at, as_of_date) "
        "VALUES (%s, %s, %s, 'pokemontcg', %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s) "
        "RETURNING 1",
        (card_key, raw_grade(dsn), day, currency, obs_type, key, Decimal(median),
         Decimal(median), Decimal(median), Decimal(median), Decimal(median), at(day), D),
    )  # fmt: skip


def add_obs(dsn, card_key, *, observed, price="10.5", source="pokemontcg",
            obs_type="provider_aggregate", key="tcgplayer/normal/market", status="auto",
            method="deterministic", score=100):  # fmt: skip
    obs_id = rows(
        dsn,
        "INSERT INTO stg_price_observations (source, source_card_ref, price, currency, "
        "observed_at, observation_type, observation_key) "
        "VALUES (%s, %s, %s, 'USD', %s, %s, %s) RETURNING id",
        (source, f"ref-{card_key}", Decimal(price), observed, obs_type, key),
    )[0][0]
    if status is not None:
        matched = card_key if status in ("auto", "confirmed") else None
        rows(
            dsn,
            "INSERT INTO fct_card_matches (stg_observation_id, card_key, match_method, "
            "match_score, status) VALUES (%s, %s, %s, %s, %s) RETURNING 1",
            (obs_id, matched, method, score, status),
        )
    return obs_id


def add_anomaly(dsn, card_key, kind, severity, *, detected, key, resolved=False):
    rows(
        dsn,
        "INSERT INTO fct_price_anomalies (card_key, anomaly_type, severity, expected_price, "
        "observed_price, details, as_of_date, dedupe_key, detected_at, resolved_at, "
        "observation_key) VALUES (%s, %s, %s, 10, 16, %s, %s, %s, %s, %s, "
        "'tcgplayer/normal/market') "
        "RETURNING 1",
        (card_key, kind, severity, Jsonb({"change_pct": 60}), D, key, detected,
         detected if resolved else None),
    )  # fmt: skip


# ---------- health ----------
def test_health_checks_the_database(api):
    body = api.get("/health").json()
    assert body["status"] == "ok" and body["database"] == "ok"


# ---------- search ----------
def test_card_search_filters_ranks_and_paginates(api, dsn):
    add_card(dsn, "base1-4", "Charizard", number="4")
    add_card(dsn, "base1-46", "Charmander", number="46", rarity="Common")
    add_card(dsn, "base4-4", "Charizard", set_name="Base Set 2", number="4", set_id="base4")
    add_card(dsn, "base1-100", "Rare Candy_100%", number="100")

    everything = api.get("/cards/search").json()
    assert everything["total"] == 4

    zard = api.get("/cards/search?q=charizard").json()
    assert zard["total"] == 2 and {i["card_id"] for i in zard["items"]} == {"base1-4", "base4-4"}
    assert zard["items"][0].keys() >= {
        "card_key", "card_id", "name", "set_name", "card_number", "rarity", "image_small_url"
    }  # fmt: skip

    char = api.get("/cards/search?q=char&limit=1&offset=1").json()
    assert char["total"] == 3 and len(char["items"]) == 1
    assert (char["limit"], char["offset"]) == (1, 1)

    in_set = api.get("/cards/search?q=charizard&set_name=base set 2").json()
    assert [i["card_id"] for i in in_set["items"]] == ["base4-4"]

    # user text is literal: a bare % or _ is not a wildcard
    assert api.get("/cards/search?q=%25").json()["total"] == 1  # only the card with a literal %
    assert api.get("/cards/search?q=zzz").json() == {
        "total": 0,
        "limit": 20,
        "offset": 0,
        "items": [],
    }


# ---------- card detail ----------
def test_card_detail_includes_latest_market_values(api, dsn):
    card = add_card(dsn, "base1-4", "Charizard", number="4")
    bare = add_card(dsn, "base1-5", "Clefairy", number="5")
    add_market_value(dsn, card, D - timedelta(days=1), m7="100")
    add_market_value(dsn, card, D)
    add_market_value(dsn, card, D, key="cardmarket/trendPrice", currency="EUR")

    detail = api.get(f"/cards/{card}").json()
    assert detail["card_id"] == "base1-4" and detail["release_date"] is None
    values = detail["latest_market_values"]
    assert {v["as_of_date"] for v in values} == {D.isoformat()}  # latest date only
    assert {(v["currency"], v["observation_key"]) for v in values} == {
        ("USD", "tcgplayer/normal/market"),
        ("EUR", "cardmarket/trendPrice"),
    }
    assert api.get(f"/cards/{bare}").json()["latest_market_values"] == []
    assert api.get("/cards/987654").status_code == 404


# ---------- market value ----------
def test_market_value_endpoint(api, dsn):
    card = add_card(dsn, "base1-4", "Charizard", number="4")
    add_market_value(dsn, card, D - timedelta(days=1), m7="100", m30="90")
    add_market_value(dsn, card, D)
    add_market_value(dsn, card, D, key="cardmarket/trendPrice", currency="EUR", m7="80")
    add_market_value(dsn, card, D, key="tcgplayer/normal/market", obs_type="asking_price", m7="500")

    body = api.get(f"/cards/{card}/market-value").json()
    assert body["as_of_date"] == D.isoformat() and body["currency"] == "USD"
    assert body["card"]["card_id"] == "base1-4"
    values = {v["observation_type"]: v for v in body["values"]}
    assert set(values) == {"provider_aggregate", "asking_price"}  # USD only; types stay separate
    value = values["provider_aggregate"]
    assert value["price_7d_median"] == 120.0 and value["price_30d_median"] == 110.0
    assert value["price_7d_change_pct"] == 20.0 and value["price_30d_change_pct"] is None
    assert (value["observation_count_7d"], value["observation_count_30d"]) == (7, 12)
    assert value["confidence_score"] == 88.97
    assert value["confidence_components"]["total"]["score"] == 88.97
    assert (value["liquidity_tier"], value["buyback_tier"]) == ("unknown", "low")
    assert value["recommended_market_value"] == 120.0
    assert value["recommended_buyback_price"] == 60.0
    assert value["methodology_version"] == "mv1" and value["grade_label"] == "RAW"
    assert "not a completed sale" in value["observation_type_note"].lower()
    assert value["data_freshness"]["age_days_at_as_of"] == 0
    assert value["data_freshness"]["latest_observed_at"] and value["data_freshness"]["computed_at"]

    older = api.get(f"/cards/{card}/market-value?as_of_date={D - timedelta(days=1)}").json()
    assert older["values"][0]["price_7d_median"] == 100.0

    eur = api.get(f"/cards/{card}/market-value?currency=eur").json()
    assert (
        eur["currency"] == "EUR" and eur["values"][0]["observation_key"] == "cardmarket/trendPrice"
    )

    only = api.get(f"/cards/{card}/market-value?observation_type=asking_price").json()
    assert [v["observation_type"] for v in only["values"]] == ["asking_price"]


def test_market_value_404s(api, dsn):
    card = add_card(dsn, "base1-4", "Charizard", number="4")
    assert api.get(f"/cards/{card}/market-value").status_code == 404  # nothing computed
    add_market_value(dsn, card, D)
    assert api.get(f"/cards/{card}/market-value?as_of_date=2026-01-01").status_code == 404
    assert api.get(f"/cards/{card}/market-value?observation_type=completed_sale").status_code == 404
    assert api.get("/cards/987654/market-value").status_code == 404


# ---------- price history ----------
def test_price_history_returns_only_existing_days(api, dsn):
    card = add_card(dsn, "base1-4", "Charizard", number="4")
    for offset, price in ((4, "10"), (2, "11"), (0, "12")):
        add_daily(dsn, card, D - timedelta(days=offset), median=price)
    add_daily(dsn, card, D, median="8", key="cardmarket/trendPrice", currency="EUR")
    add_daily(dsn, card, D - timedelta(days=1), median="99", obs_type="asking_price", key="shop/x")

    body = api.get(f"/cards/{card}/price-history?days=90&currency=USD").json()
    usd = [p for p in body["points"] if p["observation_type"] == "provider_aggregate"]
    assert [p["price_date"] for p in usd] == [
        (D - timedelta(days=4)).isoformat(),
        (D - timedelta(days=2)).isoformat(),
        D.isoformat(),
    ]  # no invented days in between
    assert [p["median_price"] for p in usd] == [10.0, 11.0, 12.0]
    assert body["to_date"] == D.isoformat() and "not filled in" in body["notice"]

    short = api.get(
        f"/cards/{card}/price-history?days=2&observation_type=provider_aggregate"
    ).json()
    assert {p["price_date"] for p in short["points"]} == {
        D.isoformat(),
        (D - timedelta(days=0)).isoformat(),
    }
    assert len(short["points"]) == 2  # USD and EUR rows of the latest day only
    assert short["from_date"] == (D - timedelta(days=1)).isoformat()

    eur = api.get(f"/cards/{card}/price-history?currency=EUR").json()
    assert [p["observation_key"] for p in eur["points"]] == ["cardmarket/trendPrice"]

    empty = add_card(dsn, "base1-5", "Clefairy", number="5")
    nothing = api.get(f"/cards/{empty}/price-history").json()
    assert nothing["points"] == [] and nothing["from_date"] is None


# ---------- observations ----------
def test_observations_are_matched_only_ordered_and_paginated(api, dsn):
    card = add_card(dsn, "base1-4", "Charizard", number="4")
    other = add_card(dsn, "base1-5", "Clefairy", number="5")
    first = add_obs(dsn, card, observed=at(D, 1), price="10")
    second = add_obs(dsn, card, observed=at(D, 3), price="12", source="shop",
                     obs_type="asking_price", status="confirmed", method="manual")  # fmt: skip
    third = add_obs(dsn, card, observed=at(D, 2), price="11")
    add_obs(dsn, card, observed=at(D, 4), status="review", method="none")  # not matched
    add_obs(dsn, other, observed=at(D, 5))  # another card

    body = api.get(f"/cards/{card}/observations").json()
    assert body["total"] == 3
    assert [i["observation_id"] for i in body["items"]] == [second, third, first]  # newest first
    top = body["items"][0]
    assert top["source"] == "shop" and top["observation_type"] == "asking_price"
    assert (top["match_method"], top["match_status"]) == ("manual", "confirmed")
    assert top["price"] == 12.0 and top["currency"] == "USD" and top["match_score"] == 100.0
    assert body["items"][1]["source_card_ref"] == f"ref-{card}"

    page = api.get(f"/cards/{card}/observations?limit=1&offset=2").json()
    assert page["total"] == 3 and [i["observation_id"] for i in page["items"]] == [first]


# ---------- anomalies ----------
def test_anomalies_endpoint_filters_and_embeds_cards(api, dsn):
    card = add_card(dsn, "base1-4", "Charizard", number="4")
    add_anomaly(dsn, card, "price_spike", "warning", detected=at(D, 3), key="a1")
    add_anomaly(dsn, card, "stale_data", "error", detected=at(D, 2), key="a2")
    add_anomaly(dsn, None, "missing_match", "info", detected=at(D, 1), key="a3", resolved=True)

    everything = api.get("/anomalies").json()
    assert everything["total"] == 3
    assert [a["anomaly_type"] for a in everything["items"]] == [
        "price_spike", "stale_data", "missing_match"
    ]  # fmt: skip
    spike = everything["items"][0]
    assert spike["card"]["card_id"] == "base1-4" and spike["card"]["name"] == "Charizard"
    assert (spike["expected_price"], spike["observed_price"]) == (10.0, 16.0)
    assert spike["details"] == {"change_pct": 60} and spike["resolved_at"] is None
    assert everything["items"][2]["card"] is None and everything["items"][2]["resolved_at"]

    assert api.get("/anomalies?severity=error").json()["total"] == 1
    assert (
        api.get("/anomalies?anomaly_type=price_spike").json()["items"][0]["severity"] == "warning"
    )
    assert api.get(f"/anomalies?card_key={card}").json()["total"] == 2
    assert len(api.get("/anomalies?limit=1&offset=1").json()["items"]) == 1


# ---------- data quality ----------
def test_data_quality_summary(api, dsn):
    card = add_card(dsn, "base1-4", "Charizard", number="4")
    for hour in (1, 2, 3):
        add_obs(dsn, card, observed=at(D, hour))
    add_obs(dsn, card, observed=at(D, 4), source="shop", obs_type="asking_price",
            status="confirmed", method="manual")  # fmt: skip
    add_obs(dsn, card, observed=at(D, 5), status="review", method="none")
    add_obs(dsn, card, observed=at(D, 6), source="shop", obs_type="asking_price", status=None)
    add_obs(dsn, card, observed=at(D, 7), source="shop", obs_type="asking_price",
            status="rejected", method="none")  # fmt: skip

    for name, status, finished in (
        ("entity_resolution", "succeeded", at(D, 1)),
        ("entity_resolution", "succeeded", at(D, 9)),
        ("price_ingest", "failed", at(D, 10)),
        ("market_value_computation", "succeeded", at(D, 8)),
    ):
        rows(
            dsn,
            "INSERT INTO data_pipeline_runs (pipeline_name, status, finished_at, rows_read, "
            "rows_written) VALUES (%s, %s, %s, 5, 4) RETURNING 1",
            (name, status, finished),
        )
    add_anomaly(dsn, card, "price_spike", "warning", detected=at(D), key="q1")
    add_anomaly(dsn, card, "stale_data", "warning", detected=at(D), key="q2", resolved=True)

    body = api.get("/data-quality/summary").json()
    assert body["total_observations"] == 7
    assert body["matched_observations"] == 4 and body["review_observations"] == 1
    assert body["unmatched_observations"] == 2  # no match row + rejected
    assert {s["source"]: s["observations"] for s in body["observations_by_source"]} == {
        "pokemontcg": 4,
        "shop": 3,
    }
    assert {t["observation_type"]: t["observations"] for t in body["observations_by_type"]} == {
        "provider_aggregate": 4,
        "asking_price": 3,
    }
    runs = {r["pipeline_name"]: r for r in body["latest_successful_runs"]}
    assert set(runs) == {"entity_resolution", "market_value_computation"}  # failed run excluded
    assert runs["entity_resolution"]["finished_at"].startswith(f"{D}T09")
    fresh = {f["source"]: f for f in body["data_freshness_by_source"]}
    assert set(fresh) == {"pokemontcg", "shop"} and fresh["shop"]["age_days"] > 0
    assert body["open_anomalies"] == 1
    assert any("not completed sales" in note for note in body["notes"])
