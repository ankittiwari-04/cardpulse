import httpx
from tenacity import wait_none

from src.config import Settings
from src.ingestion.client import PokemonTCGClient


def _client(handler, page_size=2):
    return PokemonTCGClient(
        Settings(_env_file=None),
        page_size=page_size,
        wait=wait_none(),
        transport=httpx.MockTransport(handler),
    )


def test_pagination_collects_all_pages():
    pages = {
        1: {
            "data": [{"id": "a-1", "name": "A"}, {"id": "a-2", "name": "B"}],
            "page": 1,
            "pageSize": 2,
            "count": 2,
            "totalCount": 3,
        },
        2: {
            "data": [{"id": "a-3", "name": "C"}],
            "page": 2,
            "pageSize": 2,
            "count": 1,
            "totalCount": 3,
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=pages[int(request.url.params["page"])])

    with _client(handler) as client:
        ids = [card.id for card in client.iter_cards()]

    assert ids == ["a-1", "a-2", "a-3"]


def test_retries_on_server_error():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={
                "data": [{"id": "a-1", "name": "A"}],
                "page": 1,
                "pageSize": 2,
                "count": 1,
                "totalCount": 1,
            },
        )

    with _client(handler) as client:
        cards = list(client.iter_cards())

    assert len(cards) == 1
    assert calls["n"] == 2
