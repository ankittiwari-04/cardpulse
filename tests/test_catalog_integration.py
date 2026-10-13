"""Database integration tests.

Run with TEST_DATABASE_URL set (a database you are happy to create tables in).
Each test works inside its own throwaway schema (cp_test_<random>) which is dropped
afterwards, so no existing table or data -- including the dev database -- is touched.
"""

import os
import uuid
from datetime import date

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from tenacity import wait_none

from src.config import Settings
from src.db.connection import normalize_dsn
from src.db.migrate import apply_schema
from src.ingestion.catalog import ingest_catalog
from src.ingestion.client import PokemonTCGClient

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


def card(card_id, name, hp="40"):
    return {
        "id": card_id,
        "name": name,
        "supertype": "Pokémon",
        "subtypes": ["Basic"],
        "hp": hp,
        "set": {
            "id": "base1",
            "name": "Base",
            "series": "Base",
            "printedTotal": 102,
            "total": 102,
            "releaseDate": "1999/01/09",
            "updatedAt": "2022/10/10 15:12:00",
        },
        "number": card_id.split("-")[1],
        "artist": "Ken Sugimori",
        "rarity": "Rare",
        "images": {"small": f"https://img/{card_id}-s.png", "large": f"https://img/{card_id}.png"},
        "flavorText": "kept as is",
    }


def make_client(pages, total, fail_on_page=None):
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        if page == fail_on_page:
            return httpx.Response(400, json={"error": "bad request"})
        data = pages[page]
        body = {"data": data, "page": page, "pageSize": 2, "count": len(data), "totalCount": total}
        return httpx.Response(200, json=body)

    return PokemonTCGClient(
        Settings(_env_file=None),
        page_size=2,
        wait=wait_none(),
        transport=httpx.MockTransport(handler),
    )


def run(dsn, client):
    return ingest_catalog(Settings(_env_file=None), client=client, dsn=dsn)


def run_row(dsn, run_id):
    return rows(
        dsn,
        "SELECT status, rows_read, rows_written, finished_at, error_message, metadata "
        "FROM data_pipeline_runs WHERE run_id = %s",
        (run_id,),
    )[0]


def test_first_ingestion_populates_all_layers(dsn):
    c1, c2 = card("base1-1", "Alakazam"), card("base1-2", "Blastoise")
    result = run(dsn, make_client({1: [c1, c2]}, total=2))

    status, rows_read, rows_written, finished_at, error, meta = run_row(dsn, result.run_id)
    assert status == "succeeded"
    assert (rows_read, rows_written) == (2, 6)
    assert finished_at is not None and error is None
    assert meta["raw_inserted"] == 2 and meta["raw_reused"] == 0
    assert meta["staging_inserted"] == 2 and meta["staging_updated"] == 0
    assert meta["dimension_inserted"] == 2 and meta["dimension_updated"] == 0

    raw = rows(dsn, "SELECT id, payload, payload_hash FROM raw_card_catalog ORDER BY id")
    assert [r[1] for r in raw] == [c1, c2]  # original payload preserved exactly
    assert all(len(r[2]) == 64 for r in raw)

    stg = rows(dsn, "SELECT source_card_id, release_date, subtypes, raw_id FROM stg_cards")
    assert {r[0]: r[1] for r in stg} == {"base1-1": date(1999, 1, 9), "base1-2": date(1999, 1, 9)}
    assert {r[0]: r[3] for r in stg} == {"base1-1": raw[0][0], "base1-2": raw[1][0]}

    dim = rows(dsn, "SELECT card_id, name, set_id, rarity FROM dim_cards ORDER BY card_id")
    assert dim == [
        ("base1-1", "Alakazam", "base1", "Rare"),
        ("base1-2", "Blastoise", "base1", "Rare"),
    ]


def test_identical_rerun_is_a_noop(dsn):
    pages = {1: [card("base1-1", "Alakazam"), card("base1-2", "Blastoise")]}
    run(dsn, make_client(pages, total=2))
    dim_before = rows(dsn, "SELECT card_id, updated_at FROM dim_cards ORDER BY card_id")
    stg_before = rows(dsn, "SELECT source_card_id, loaded_at FROM stg_cards ORDER BY 1")

    second = run(dsn, make_client(pages, total=2))

    meta = run_row(dsn, second.run_id)[5]
    assert meta["raw_inserted"] == 0 and meta["raw_reused"] == 2
    assert meta["staging_unchanged"] == 2 and meta["dimension_unchanged"] == 2
    assert run_row(dsn, second.run_id)[2] == 0  # rows_written
    assert rows(dsn, "SELECT count(*) FROM raw_card_catalog")[0][0] == 2
    assert rows(dsn, "SELECT card_id, updated_at FROM dim_cards ORDER BY card_id") == dim_before
    assert rows(dsn, "SELECT source_card_id, loaded_at FROM stg_cards ORDER BY 1") == stg_before
    assert rows(dsn, "SELECT count(*) FROM data_pipeline_runs")[0][0] == 2


def test_changed_payload_adds_raw_version_and_updates_downstream(dsn):
    a, b = card("base1-1", "Alakazam"), card("base1-2", "Blastoise")
    run(dsn, make_client({1: [a, b]}, total=2))
    dim_before = dict(rows(dsn, "SELECT card_id, updated_at FROM dim_cards"))

    changed = card("base1-1", "Alakazam EX", hp="80")
    second = run(dsn, make_client({1: [changed, b]}, total=2))

    meta = run_row(dsn, second.run_id)[5]
    assert meta["raw_inserted"] == 1 and meta["raw_reused"] == 1
    assert meta["staging_updated"] == 1 and meta["staging_unchanged"] == 1
    assert meta["dimension_updated"] == 1 and meta["dimension_unchanged"] == 1

    versions = rows(
        dsn,
        "SELECT payload->>'name' FROM raw_card_catalog "
        "WHERE source_card_id = 'base1-1' ORDER BY id",
    )
    assert versions == [("Alakazam",), ("Alakazam EX",)]  # old version is preserved

    latest_raw = rows(dsn, "SELECT max(id) FROM raw_card_catalog WHERE source_card_id = 'base1-1'")[
        0
    ][0]
    assert rows(dsn, "SELECT raw_id, name FROM stg_cards WHERE source_card_id = 'base1-1'") == [
        (latest_raw, "Alakazam EX")
    ]

    dim_after = dict(rows(dsn, "SELECT card_id, updated_at FROM dim_cards"))
    assert dim_after["base1-1"] > dim_before["base1-1"]
    assert dim_after["base1-2"] == dim_before["base1-2"]


def test_payload_only_change_does_not_touch_dimension(dsn):
    b = card("base1-2", "Blastoise")
    run(dsn, make_client({1: [b]}, total=1))
    dim_before = rows(dsn, "SELECT updated_at FROM dim_cards")

    second = run(dsn, make_client({1: [card("base1-2", "Blastoise", hp="999")]}, total=1))

    meta = run_row(dsn, second.run_id)[5]
    assert meta["raw_inserted"] == 1  # new raw version (hp is not a catalog column)
    assert meta["staging_updated"] == 1  # staging now points at the new raw row
    assert meta["dimension_updated"] == 0 and meta["dimension_unchanged"] == 1
    assert rows(dsn, "SELECT updated_at FROM dim_cards") == dim_before


def test_failure_on_first_page_is_tracked(dsn):
    client = make_client({}, total=2, fail_on_page=1)
    with pytest.raises(httpx.HTTPStatusError):
        run(dsn, client)

    status, rows_read, rows_written, finished_at, error, _ = rows(
        dsn,
        "SELECT status, rows_read, rows_written, finished_at, error_message, metadata "
        "FROM data_pipeline_runs",
    )[0]
    assert status == "failed"
    assert (rows_read, rows_written) == (0, 0)
    assert finished_at is not None
    assert "400" in error
    assert rows(dsn, "SELECT count(*) FROM raw_card_catalog")[0][0] == 0


def test_failure_on_later_page_keeps_earlier_pages(dsn):
    pages = {1: [card("base1-1", "Alakazam"), card("base1-2", "Blastoise")]}
    client = make_client(pages, total=4, fail_on_page=2)
    with pytest.raises(httpx.HTTPStatusError):
        run(dsn, client)

    status, rows_read, rows_written, _, error, meta = rows(
        dsn,
        "SELECT status, rows_read, rows_written, finished_at, error_message, metadata "
        "FROM data_pipeline_runs",
    )[0][:6]
    assert status == "failed"
    assert (rows_read, rows_written) == (2, 6)  # page 1 was committed and counted
    assert meta["pages_fetched"] == 1
    assert rows(dsn, "SELECT count(*) FROM raw_card_catalog")[0][0] == 2
    assert rows(dsn, "SELECT count(*) FROM dim_cards")[0][0] == 2
