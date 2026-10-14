from fastapi.testclient import TestClient

from src.api.deps import check_database
from src.main import app


def _client(database_ok: bool) -> TestClient:
    app.dependency_overrides[check_database] = lambda: database_ok
    return TestClient(app)


def teardown_function():
    app.dependency_overrides.clear()


def test_health_ok():
    response = _client(True).get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "cardpulse"
    assert body["database"] == "ok"
    assert body["version"] and body["timestamp"]


def test_health_degraded_when_database_is_down():
    response = _client(False).get("/health")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "degraded" and body["database"] == "unavailable"
    assert "postgresql" not in response.text and "password" not in response.text
