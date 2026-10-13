from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from pydantic import ValidationError
from tenacity import wait_none

from src.config import Settings
from src.ingestion.price_models import PriceCandidate, validate_candidate
from src.ingestion.prices import (
    PokemonTCGPriceClient,
    PriceCardPage,
    ingest_prices,
    main,
)


def sample_card(card_id="base1-1", *, tcg_market=1.0, tcg_low=0.5, tcg_updated="2026/10/12"):
    return {
        "id": card_id,
        "name": "Alakazam",
        "number": card_id.split("-")[1] if card_id else "1",
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
            "prices": {
                "averageSellPrice": 1.5,
                "lowPrice": 0.9,
                "trendPrice": 2.0,
                "avg1": 1.4,
            },
        },
    }


def make_client(pages, total, page_size=2):
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        data = pages[page]
        body = {
            "data": data,
            "page": page,
            "pageSize": page_size,
            "count": len(data),
            "totalCount": total,
        }
        return httpx.Response(200, json=body)

    return PokemonTCGPriceClient(
        Settings(_env_file=None),
        page_size=page_size,
        wait=wait_none(),
        transport=httpx.MockTransport(handler),
    )


def candidate(**overrides) -> PriceCandidate:
    base = {
        "source": "pokemontcg",
        "source_card_ref": "base1-1",
        "observation_key": "tcgplayer/normal/market",
        "observation_type": "provider_aggregate",
        "price": 1.25,
        "currency": "USD",
        "observed_at": datetime(2026, 10, 12, tzinfo=UTC),
        "payload": {"updatedAt": "2026/10/12"},
        "observed_at_raw": "2026/10/12",
    }
    base.update(overrides)
    return PriceCandidate(**base)


# ---------- settings ----------
def test_price_settings_defaults_and_env(monkeypatch):
    for key in (
        "PRICE_SOURCE_NAME",
        "PRICE_API_BASE_URL",
        "PRICE_API_KEY",
        "PRICE_REQUEST_TIMEOUT_SECONDS",
        "PRICE_MAX_PAGES",
    ):
        monkeypatch.delenv(key, raising=False)
    defaults = Settings(_env_file=None)
    assert defaults.price_source_name == "pokemontcg"
    assert defaults.price_api_base_url == ""
    assert defaults.price_request_timeout_seconds == 30.0
    assert defaults.price_max_pages == 1

    monkeypatch.setenv("PRICE_MAX_PAGES", "3")
    monkeypatch.setenv("PRICE_REQUEST_TIMEOUT_SECONDS", "12.5")
    monkeypatch.setenv("PRICE_API_BASE_URL", "https://example.test/v1")
    configured = Settings(_env_file=None)
    assert configured.price_max_pages == 3
    assert configured.price_request_timeout_seconds == 12.5
    assert configured.price_api_base_url == "https://example.test/v1"


def test_blank_env_values_fall_back_to_defaults(monkeypatch):
    monkeypatch.setenv("PRICE_MAX_PAGES", "")
    assert Settings(_env_file=None).price_max_pages == 1


# ---------- response validation ----------
def test_response_validation_accepts_valid_page():
    page = PriceCardPage.model_validate(
        {"data": [sample_card()], "page": 1, "pageSize": 250, "count": 1, "totalCount": 1}
    )
    assert page.data[0].tcgplayer.updated_at == "2026/10/12"


def test_response_validation_rejects_malformed_page():
    with pytest.raises(ValidationError):
        PriceCardPage.model_validate({"data": [], "page": 1, "pageSize": 250, "count": 0})
    with pytest.raises(ValidationError):
        PriceCardPage.model_validate(
            {"data": "nope", "page": 1, "pageSize": 1, "count": 0, "totalCount": 0}
        )


# ---------- adapter ----------
def test_adapter_extracts_observations_and_preserves_raw_payload():
    original = sample_card()
    with make_client({1: [original]}, total=1) as client:
        pages = list(client.iter_pages())

    candidates = pages[0].candidates
    assert [c.observation_key for c in candidates] == [
        "tcgplayer/normal/market",
        "tcgplayer/normal/low",
        "cardmarket/trendPrice",
        "cardmarket/averageSellPrice",
        "cardmarket/lowPrice",
    ]
    tcg = [c for c in candidates if c.observation_key.startswith("tcgplayer")]
    assert all(c.payload == original["tcgplayer"] for c in tcg)  # verbatim vendor block
    assert "directLow" in tcg[0].payload["prices"]["normal"]  # null fields are kept too
    assert {c.currency for c in tcg} == {"USD"}
    assert {c.currency for c in candidates if c.observation_key.startswith("cardmarket")} == {"EUR"}
    assert {c.observation_type for c in candidates} == {"provider_aggregate"}
    assert all(c.is_sold is None for c in candidates)  # never claimed as sold
    assert all(c.grading_company is None and c.grade_value is None for c in candidates)
    assert all(c.condition_label is None and c.language_label is None for c in candidates)
    assert candidates[0].observed_at == datetime(2026, 10, 12, tzinfo=UTC)
    assert candidates[0].variant_label == "normal"


def test_adapter_counts_cards_without_price_data():
    bare = {"id": "x-1", "name": "No Prices", "set": {"name": "X"}}
    with make_client({1: [bare, sample_card("base1-2")]}, total=2) as client:
        page = next(client.iter_pages())
    assert page.items_without_price_data == 1
    assert len(page.candidates) == 5


def test_missing_card_id_is_rejected_per_observation_not_per_page():
    card = sample_card()
    del card["id"]
    with make_client({1: [card]}, total=1) as client:
        page = next(client.iter_pages())
    obs, issues = validate_candidate(page.candidates[0], set())
    assert obs is None
    assert "missing_source_card_ref" in {i.check_name for i in issues}


def test_pagination_and_retry_on_429():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429)
        page = int(request.url.params["page"])
        data = {1: [sample_card("a-1")], 2: [sample_card("a-2")]}[page]
        body = {"data": data, "page": page, "pageSize": 1, "count": 1, "totalCount": 2}
        return httpx.Response(200, json=body)

    client = PokemonTCGPriceClient(
        Settings(_env_file=None),
        page_size=1,
        wait=wait_none(),
        transport=httpx.MockTransport(handler),
    )
    with client:
        pages = list(client.iter_pages())
    assert [p.page for p in pages] == [1, 2]
    assert calls["n"] == 3  # one retried 429 + two pages


# ---------- validation rules ----------
def test_valid_candidate_is_normalized_without_guessing():
    obs, issues = validate_candidate(candidate(price=0.1), set())
    assert issues == []
    assert obs.price == Decimal("0.1000")
    assert obs.observation_type == "provider_aggregate"
    assert obs.grading_company is None and obs.grade_value is None and obs.is_sold is None


@pytest.mark.parametrize(
    ("overrides", "check"),
    [
        ({"price": 0}, "non_positive_price"),
        ({"price": -3.5}, "non_positive_price"),
        ({"price": 0.00001}, "non_positive_price"),
        ({"price": None}, "missing_price"),
        ({"price": "abc"}, "invalid_price"),
        ({"price": True}, "invalid_price"),
        ({"price": float("nan")}, "invalid_price"),
        ({"observed_at": None}, "missing_observed_at"),
        ({"observed_at": datetime(2026, 10, 12)}, "missing_observed_at"),
        ({"currency": "usd"}, "invalid_currency"),
        ({"currency": "US"}, "invalid_currency"),
        ({"currency": None}, "invalid_currency"),
        ({"source_card_ref": None}, "missing_source_card_ref"),
        ({"source_card_ref": "  "}, "missing_source_card_ref"),
        ({"observation_key": None}, "missing_observation_key"),
        ({"observation_type": "bogus"}, "invalid_observation_type"),
        ({"observation_type": None}, "invalid_observation_type"),
        ({"observation_type": "completed_sale"}, "unsupported_sale_claim"),
        ({"observation_type": "completed_sale", "is_sold": False}, "unsupported_sale_claim"),
        ({"grade_value": 11}, "impossible_grade"),
        ({"grade_value": 0}, "impossible_grade"),
        ({"grade_value": 9.3}, "impossible_grade"),
        ({"grade_value": "high"}, "impossible_grade"),
    ],
)
def test_invalid_candidates_are_rejected_with_a_reason(overrides, check):
    obs, issues = validate_candidate(candidate(**overrides), set())
    assert obs is None
    assert check in {i.check_name for i in issues}
    assert all(
        i.details["source_card_ref"] == overrides.get("source_card_ref", "base1-1") for i in issues
    )


def test_explicit_sale_and_valid_grade_are_accepted():
    obs, issues = validate_candidate(
        candidate(
            observation_type="completed_sale",
            is_sold=True,
            grading_company="PSA",
            grade_value=9.5,
        ),
        set(),
    )
    assert issues == []
    assert obs.grade_value == Decimal("9.5")
    assert obs.is_sold is True


def test_all_problems_are_collected():
    _, issues = validate_candidate(candidate(price=None, observed_at=None), set())
    assert {"missing_price", "missing_observed_at"} <= {i.check_name for i in issues}


def test_duplicate_listing_ids_are_rejected():
    seen: set[tuple[str, str]] = set()
    first, _ = validate_candidate(candidate(listing_id="L-1"), seen)
    second, issues = validate_candidate(replace(candidate(listing_id="L-1")), seen)
    assert first is not None
    assert second is None
    assert [i.check_name for i in issues] == ["duplicate_listing_id"]


# ---------- dry run / CLI ----------
def test_dry_run_never_touches_the_database():
    unreachable = "postgresql://nobody:nopass@127.0.0.1:1/none"
    cards = [sample_card("base1-1", tcg_market=0), sample_card("base1-2")]
    with make_client({1: cards}, total=2) as client:
        result = ingest_prices(
            Settings(_env_file=None), client=client, dry_run=True, dsn=unreachable
        )
    assert result.run_id is None and result.dry_run
    s = result.stats
    assert (s.rows_read, s.accepted, s.rejected_count, s.alert_count) == (10, 9, 1, 1)
    assert s.raw_inserted == 0 and s.staging_inserted == 0 and s.rows_written == 0
    assert result.alert_checks == {"non_positive_price": 1}


def test_cli_rejects_bad_arguments():
    for argv in (["--max-pages", "-1"], ["--source", "nope"], ["--query", "   "]):
        with pytest.raises(SystemExit) as exc:
            main(argv)
        assert exc.value.code == 2
