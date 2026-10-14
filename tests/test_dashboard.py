import httpx
import pytest

from src.dashboard import helpers
from src.dashboard.client import ApiError, CardPulseClient, NotFoundError


def make_client(handler):
    return CardPulseClient("http://api.test/", transport=httpx.MockTransport(handler))


# ---------- client ----------
def test_client_sends_only_non_null_parameters():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = request.url
        return httpx.Response(200, json={"total": 0, "items": []})

    make_client(handler).search_cards(q="char", set_name=None, limit=5)
    assert seen["url"].path == "/cards/search"
    assert dict(seen["url"].params) == {"q": "char", "limit": "5", "offset": "0"}


def test_client_maps_404_and_errors():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/1"):
            return httpx.Response(404, json={"detail": "Card 1 not found"})
        return httpx.Response(500, json={"detail": "boom"})

    client = make_client(handler)
    with pytest.raises(NotFoundError, match="Card 1 not found"):
        client.card(1)
    with pytest.raises(ApiError, match="500"):
        client.card(2)


def test_client_reports_an_unreachable_api():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    client = make_client(handler)
    with pytest.raises(ApiError, match="unreachable"):
        client.anomalies()
    with pytest.raises(ApiError, match="unreachable"):
        client.health()


def test_client_health_returns_the_body_even_when_degraded():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"status": "degraded", "database": "unavailable"})

    assert make_client(handler).health()["status"] == "degraded"


def test_market_value_defaults_to_usd():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json={"values": []})

    make_client(handler).market_value(5)
    assert seen["params"] == {"currency": "USD"}


# ---------- helpers ----------
def test_disclaimer_states_that_aggregates_are_not_sales():
    assert "not completed sales" in helpers.DISCLAIMER
    assert "provider_aggregate" in helpers.DISCLAIMER


def test_formatting():
    assert helpers.format_money(None, "USD") == "—"
    assert helpers.format_money(1234.5, "USD") == "USD 1,234.50"
    assert helpers.format_pct(None) == "—"
    assert helpers.format_pct(20) == "+20.0%"
    assert helpers.format_pct(-3.25) == "-3.2%" or helpers.format_pct(-3.25) == "-3.3%"
    assert [helpers.confidence_band(s) for s in (90, 60, 10, None)] == [
        "high",
        "medium",
        "low",
        "unknown",
    ]


def test_confidence_rows_follow_a_fixed_order_and_carry_explanations():
    components = {
        "total": {"score": 50},
        "recency": {"score": 1.0, "weight": 0.25, "weighted": 0.25, "explanation": "fresh"},
        "observation_count": {
            "score": 0.1,
            "weight": 0.25,
            "weighted": 0.025,
            "explanation": "few",
        },
    }
    rows = helpers.confidence_rows(components)
    assert [r["component"] for r in rows] == ["Number of observations", "Recency"]
    assert rows[0]["explanation"] == "few"


def test_freshness_label():
    assert helpers.freshness_label(None) == "no observation time available"
    label = helpers.freshness_label(
        {"latest_observed_at": "x", "age_days_at_as_of": 2, "age_days_now": 2.5}
    )
    assert "2 day(s) old at the as-of date" in label and "2.5 day(s) old now" in label


def test_history_frame_keeps_only_the_returned_dates():
    points = [
        {"price_date": "2026-10-14", "observation_key": "a", "median_price": 12.0,
         "min_price": 12.0, "max_price": 12.0, "avg_price": 12.0, "latest_price": 12.0},
        {"price_date": "2026-10-10", "observation_key": "a", "median_price": 10.0,
         "min_price": 10.0, "max_price": 10.0, "avg_price": 10.0, "latest_price": 10.0},
    ]  # fmt: skip
    frame = helpers.history_frame(points)
    assert len(frame) == 2  # the days in between are not invented
    assert list(frame["median_price"]) == [10.0, 12.0]
    assert helpers.history_frame([]).empty
