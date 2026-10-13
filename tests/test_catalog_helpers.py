from datetime import date

import httpx
from tenacity import wait_none

from src.config import Settings
from src.ingestion.catalog_store import (
    CatalogStats,
    canonical_json,
    compute_payload_hash,
    parse_release_date,
)
from src.ingestion.client import PokemonTCGClient
from src.ingestion.runs import sanitize_error


def test_canonical_json_is_sorted_compact_and_unescaped():
    assert canonical_json({"b": 1, "a": "Pokémon"}) == '{"a":"Pokémon","b":1}'


def test_hash_ignores_key_order_but_detects_changes():
    a = {"id": "x-1", "name": "A", "hp": "40"}
    b = {"hp": "40", "name": "A", "id": "x-1"}
    assert compute_payload_hash(a) == compute_payload_hash(b)
    assert compute_payload_hash(a) != compute_payload_hash({**a, "hp": "50"})
    assert len(compute_payload_hash(a)) == 64


def test_parse_release_date():
    assert parse_release_date("1999/01/09") == date(1999, 1, 9)
    assert parse_release_date("1999-01-09") == date(1999, 1, 9)
    assert parse_release_date("not a date") is None
    assert parse_release_date("") is None
    assert parse_release_date(None) is None


def test_sanitize_error_removes_secrets():
    exc = RuntimeError(
        "connect postgresql://cardpulse:s3cret@db:5432/x failed password=hunter2 key=abc123"
    )
    text = sanitize_error(exc, secrets=["abc123"])
    assert "s3cret" not in text
    assert "hunter2" not in text
    assert "abc123" not in text
    assert text.startswith("RuntimeError:")


def test_stats_rows_written_and_metadata():
    stats = CatalogStats(rows_read=3, raw_inserted=2, staging_inserted=1, staging_updated=1)
    stats.add(CatalogStats(rows_read=1, dimension_inserted=2, raw_reused=5))
    assert stats.rows_read == 4
    assert stats.rows_written == 2 + 1 + 1 + 2
    assert "rows_read" not in stats.as_metadata()
    assert stats.as_metadata()["raw_reused"] == 5


def test_client_preserves_original_payload_exactly():
    original = {
        "id": "base1-1",
        "name": "Alakazam",
        "set": {"id": "base1", "name": "Base", "releaseDate": "1999/01/09"},
        "flavorText": "unknown field must survive",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body = {"data": [original], "page": 1, "pageSize": 250, "count": 1, "totalCount": 1}
        return httpx.Response(200, json=body)

    client = PokemonTCGClient(
        Settings(_env_file=None), wait=wait_none(), transport=httpx.MockTransport(handler)
    )
    with client:
        pages = list(client.iter_catalog_pages())

    assert len(pages) == 1
    raw = pages[0].raw_cards[0]
    assert raw == original  # nothing added, nothing lost
    assert "subtypes" not in raw  # model defaults are NOT injected into the raw payload
    assert raw["flavorText"] == "unknown field must survive"
    assert pages[0].cards[0].subtypes == []  # while the validated model still has defaults
