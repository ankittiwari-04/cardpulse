"""Database integration tests for price ingestion.

Run with TEST_DATABASE_URL set. Each test works inside its own throwaway schema
(cp_test_<random>) that is dropped afterwards; no existing table or data is touched.
"""

import os
import uuid
from decimal import Decimal

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from tenacity import wait_none

from src.config import Settings
from src.db.connection import normalize_dsn
from src.db.migrate import apply_schema
from src.ingestion.prices import PokemonTCGPriceClient, ingest_prices

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL is not set; skipping database integration tests",
)


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


def card(card_id, *, tcg_market=1.0, tcg_low=0.5, tcg_updated="2026/10/12", cm_trend=2.0):
    return {
        "id": card_id,
        "name": "Alakazam",
        "number": card_id.split("-")[1],
        "set": {"id": "base1", "name": "Base"},
        "tcgplayer": {
            "url": f"https://prices.pokemontcg.io/tcgplayer/{card_id}",
            "updatedAt": tcg_updated,
            "prices": {
                "normal": {
                    "low": tcg_low,
                    "mid": 0.8,
                    "high": 3.0,
                    "market": tcg_market,
                    "directLow": None,
                }
            },
        },
        "cardmarket": {
            "url": f"https://prices.pokemontcg.io/cardmarket/{card_id}",
            "updatedAt": "2026/10/12",
            "prices": {"averageSellPrice": 1.5, "lowPrice": 0.9, "trendPrice": cm_trend},
        },
    }


def make_client(pages, total, fail_on_page=None):
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        if page == fail_on_page:
            return httpx.Response(400, json={"error": "bad request"})
        data = pages[page]
        body = {"data": data, "page": page, "pageSize": 2, "count": len(data), "totalCount": total}
        return httpx.Response(200, json=body)

    return PokemonTCGPriceClient(
        Settings(_env_file=None),
        page_size=2,
        wait=wait_none(),
        transport=httpx.MockTransport(handler),
    )


def run(dsn, client, **kwargs):
    return ingest_prices(Settings(_env_file=None), client=client, dsn=dsn, **kwargs)


def run_row(dsn, run_id):
    return rows(
        dsn,
        "SELECT status, rows_read, rows_written, finished_at, error_message, metadata "
        "FROM data_pipeline_runs WHERE run_id = %s",
        (run_id,),
    )[0]


def test_first_ingestion_stores_raw_and_staging(dsn):
    c1, c2 = card("base1-1"), card("base1-2")
    result = run(dsn, make_client({1: [c1, c2]}, total=2))

    status, rows_read, rows_written, finished_at, error, meta = run_row(dsn, result.run_id)
    assert status == "succeeded" and finished_at is not None and error is None
    assert (rows_read, rows_written) == (10, 20)
    assert meta["raw_inserted"] == 10 and meta["raw_reused"] == 0
    assert meta["staging_inserted"] == 10 and meta["staging_unchanged"] == 0
    assert meta["rejected_count"] == 0 and meta["alert_count"] == 0

    raw = rows(
        dsn,
        "SELECT payload, price, currency, observation_type, payload_hash FROM "
        "raw_price_observation WHERE source_card_ref = 'base1-1' "
        "AND observation_key = 'tcgplayer/normal/market'",
    )[0]
    assert raw[0] == c1["tcgplayer"]  # original vendor block, byte-for-byte equal as JSON
    assert raw[1] == Decimal("1.0") and raw[2] == "USD"
    assert raw[3] == "provider_aggregate" and len(raw[4]) == 64

    stg = rows(
        dsn,
        "SELECT s.observation_type, s.variant_label, s.is_sold, s.condition_label, "
        "s.language_label, s.edition_label, s.grading_company, s.grade_value, s.listing_id, "
        "s.raw_grade_label, s.raw_card_name, s.raw_set_name, s.raw_card_number, s.currency, "
        "s.raw_id = r.id FROM stg_price_observations s "
        "JOIN raw_price_observation r ON r.id = s.raw_id "
        "WHERE s.source_card_ref = 'base1-1' AND s.observation_key = 'tcgplayer/normal/market'",
    )[0]
    assert stg == (
        "provider_aggregate", "normal", None, None, None, None, None, None, None,
        None, "Alakazam", "Base", "1", "USD", True,
    )  # fmt: skip


def test_identical_rerun_reuses_raw_observations(dsn):
    pages = {1: [card("base1-1"), card("base1-2")]}
    run(dsn, make_client(pages, total=2))
    second = run(dsn, make_client(pages, total=2))

    _, _, rows_written, _, _, meta = run_row(dsn, second.run_id)
    assert rows_written == 0
    assert meta["raw_inserted"] == 0 and meta["raw_reused"] == 10
    assert meta["staging_inserted"] == 0 and meta["staging_unchanged"] == 10
    assert count(dsn, "raw_price_observation") == 10
    assert count(dsn, "stg_price_observations") == 10
    assert count(dsn, "data_pipeline_runs") == 2


def test_changed_observation_creates_a_new_raw_version(dsn):
    run(dsn, make_client({1: [card("base1-1")]}, total=1))
    changed = card("base1-1", tcg_market=1.25, tcg_updated="2026/10/13")
    second = run(dsn, make_client({1: [changed]}, total=1))

    meta = run_row(dsn, second.run_id)[5]
    assert meta["raw_inserted"] == 2  # tcgplayer market + low (new payload and observed_at)
    assert meta["raw_reused"] == 3  # cardmarket block was unchanged
    assert count(dsn, "raw_price_observation") == 7
    prices = rows(
        dsn,
        "SELECT price FROM raw_price_observation "
        "WHERE observation_key = 'tcgplayer/normal/market' ORDER BY id",
    )
    assert [p[0] for p in prices] == [Decimal("1.0"), Decimal("1.25")]
    first_payload = rows(
        dsn,
        "SELECT payload->>'updatedAt' FROM raw_price_observation "
        "WHERE observation_key = 'tcgplayer/normal/market' ORDER BY id LIMIT 1",
    )
    assert first_payload == [("2026/10/12",)]  # the old version is untouched


def test_invalid_prices_are_rejected_and_recorded_as_alerts(dsn):
    cards = [card("base1-1", tcg_market=0), card("base1-2", tcg_low=None)]
    result = run(dsn, make_client({1: cards}, total=2))

    _, rows_read, _, _, _, meta = run_row(dsn, result.run_id)
    assert rows_read == 10
    assert meta["rejected_count"] == 2 and meta["alert_count"] == 2
    assert meta["raw_inserted"] == 8
    assert count(dsn, "raw_price_observation") == 8  # rejects are not stored
    alerts = rows(
        dsn,
        "SELECT check_name, severity, run_id, details->>'observation_key' "
        "FROM data_quality_alerts ORDER BY alert_id",
    )
    assert sorted(a[0] for a in alerts) == ["missing_price", "non_positive_price"]
    assert {a[1] for a in alerts} == {"error"}
    assert {a[2] for a in alerts} == {result.run_id}
    assert {a[3] for a in alerts} == {"tcgplayer/normal/market", "tcgplayer/normal/low"}


def test_missing_observed_at_is_rejected_and_recorded(dsn):
    result = run(dsn, make_client({1: [card("base1-1", tcg_updated=None)]}, total=1))

    meta = run_row(dsn, result.run_id)[5]
    assert meta["rejected_count"] == 2 and meta["raw_inserted"] == 3
    alerts = rows(dsn, "SELECT check_name FROM data_quality_alerts")
    assert alerts == [("missing_observed_at",), ("missing_observed_at",)]
    assert count(dsn, "raw_price_observation") == 3  # only the cardmarket observations


def test_failure_on_first_page_is_tracked(dsn):
    with pytest.raises(httpx.HTTPStatusError):
        run(dsn, make_client({}, total=2, fail_on_page=1))

    status, rows_read, rows_written, finished_at, error, _ = rows(
        dsn,
        "SELECT status, rows_read, rows_written, finished_at, error_message, metadata "
        "FROM data_pipeline_runs",
    )[0]
    assert status == "failed" and finished_at is not None
    assert (rows_read, rows_written) == (0, 0)
    assert "400" in error
    assert count(dsn, "raw_price_observation") == 0


def test_failure_on_later_page_keeps_committed_pages(dsn):
    pages = {1: [card("base1-1"), card("base1-2")]}
    with pytest.raises(httpx.HTTPStatusError):
        run(dsn, make_client(pages, total=4, fail_on_page=2))

    status, rows_read, rows_written, _, _, meta = rows(
        dsn,
        "SELECT status, rows_read, rows_written, finished_at, error_message, metadata "
        "FROM data_pipeline_runs",
    )[0]
    assert status == "failed"
    assert (rows_read, rows_written) == (10, 20)
    assert meta["pages_fetched"] == 1
    assert count(dsn, "raw_price_observation") == 10


def test_dry_run_writes_nothing(dsn):
    cards = [card("base1-1", tcg_market=0), card("base1-2")]
    result = run(dsn, make_client({1: cards}, total=2), dry_run=True)

    assert result.run_id is None
    assert result.stats.accepted == 9 and result.stats.rejected_count == 1
    assert result.stats.raw_inserted == 0
    for table in (
        "raw_price_observation",
        "stg_price_observations",
        "data_pipeline_runs",
        "data_quality_alerts",
    ):
        assert count(dsn, table) == 0, table
