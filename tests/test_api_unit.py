"""API tests that need no database: validation, 404 behavior, error handling."""

import psycopg
import pytest
from fastapi.testclient import TestClient

from src.api.deps import get_db
from src.main import app


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class FakeConn:
    """Returns the queued row lists, one per execute() call; empty afterwards."""

    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return FakeCursor(self.results.pop(0) if self.results else [])


@pytest.fixture
def api():
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


def use(conn):
    app.dependency_overrides[get_db] = lambda: conn


INVALID_URLS = [
    "/cards/search?limit=101",
    "/cards/search?limit=0",
    "/cards/search?offset=-1",
    "/cards/search?q=",
    "/cards/0",
    "/cards/abc",
    "/cards/1/market-value?currency=US",
    "/cards/1/market-value?observation_type=bogus",
    "/cards/1/market-value?as_of_date=not-a-date",
    "/cards/1/price-history?days=366",
    "/cards/1/price-history?days=0",
    "/cards/1/price-history?currency=EURO",
    "/cards/1/observations?limit=201",
    "/cards/1/observations?limit=0",
    "/cards/1/observations?offset=-5",
    "/anomalies?limit=201",
    "/anomalies?limit=0",
    "/anomalies?severity=bogus",
    "/anomalies?anomaly_type=bogus",
]


@pytest.mark.parametrize("url", INVALID_URLS)
def test_invalid_parameters_are_rejected(api, url):
    use(FakeConn())
    assert api.get(url).status_code == 422


def test_limit_and_days_maximums_are_accepted(api):
    use(FakeConn([{"total": 0}], []))
    assert api.get("/cards/search?limit=100").status_code == 200
    use(FakeConn())
    assert api.get("/cards/1/price-history?days=365").status_code != 422
    assert api.get("/cards/1/observations?limit=200").status_code != 422
    use(FakeConn([{"total": 0}], []))
    assert api.get("/anomalies?limit=200").status_code == 200


@pytest.mark.parametrize("path", ["", "/market-value", "/price-history", "/observations"])
def test_unknown_card_returns_404(api, path):
    use(FakeConn())
    response = api.get(f"/cards/999{path}")
    assert response.status_code == 404
    assert response.json()["detail"] == "Card 999 not found"


def test_search_returns_a_page_of_cards(api):
    row = {
        "card_key": 7,
        "card_id": "base1-4",
        "name": "Charizard",
        "set_name": "Base",
        "card_number": "4",
        "rarity": "Rare Holo",
        "image_small_url": "https://img.test/s.png",
        "supertype": "Pokémon",
    }
    conn = FakeConn([{"total": 1}], [row])
    use(conn)
    response = api.get("/cards/search?q=chariz&limit=5")
    assert response.status_code == 200
    body = response.json()
    assert (body["total"], body["limit"], body["offset"]) == (1, 5, 0)
    assert body["items"][0]["card_id"] == "base1-4"
    # user text only ever travels as a bound parameter
    assert all("chariz" not in sql for sql, _ in conn.calls)
    assert conn.calls[0][1]["pattern"] == "%chariz%"


def test_database_outage_returns_503_without_details(api):
    def broken():
        raise psycopg.OperationalError("connection to host secret-host failed password=hunter2")
        yield  # pragma: no cover

    app.dependency_overrides[get_db] = broken
    response = api.get("/cards/search")
    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    assert "hunter2" not in response.text and "secret-host" not in response.text
