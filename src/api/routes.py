"""Internal pricing API routes (read-only)."""

from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Any

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Path, Query

from src.api import queries
from src.api.deps import get_db
from src.api.schemas import (
    OBSERVATION_TYPE_NOTES,
    AnomalyOut,
    AnomalyType,
    CardDetail,
    CardSummary,
    DataFreshness,
    DataQualitySummary,
    MarketValue,
    MarketValueResponse,
    ObservationOut,
    ObservationType,
    Page,
    PriceHistoryPoint,
    PriceHistoryResponse,
    Severity,
)

router = APIRouter()

Db = Annotated[psycopg.Connection, Depends(get_db)]
CardKey = Annotated[int, Path(ge=1, description="dim_cards.card_key")]
_CURRENCY_PATTERN = r"^[A-Za-z]{3}$"


def _require_card(db: psycopg.Connection, card_key: int) -> dict[str, Any]:
    card = queries.get_card(db, card_key)
    if card is None:
        raise HTTPException(status_code=404, detail=f"Card {card_key} not found")
    return card


def _freshness(
    latest_observed_at: datetime | None, as_of_date: date, computed_at: datetime | None
) -> DataFreshness:
    if latest_observed_at is None:
        return DataFreshness(computed_at=computed_at)
    latest = latest_observed_at.astimezone(UTC)
    return DataFreshness(
        latest_observed_at=latest_observed_at,
        age_days_at_as_of=(as_of_date - latest.date()).days,
        age_days_now=round((datetime.now(UTC) - latest).total_seconds() / 86400, 2),
        computed_at=computed_at,
    )


def _market_value(row: dict[str, Any]) -> MarketValue:
    data = dict(row)
    data["observation_type_note"] = OBSERVATION_TYPE_NOTES.get(data["observation_type"], "")
    data["data_freshness"] = _freshness(
        data.pop("latest_observed_at"), data["as_of_date"], data.pop("computed_at")
    )
    return MarketValue.model_validate(data)


# NOTE: /cards/search must be declared before /cards/{card_key}.
@router.get("/cards/search", response_model=Page[CardSummary])
def search_cards(
    db: Db,
    q: Annotated[str | None, Query(min_length=1, max_length=100, description="name search")] = None,
    set_name: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> Page[CardSummary]:
    total, rows = queries.search_cards(db, q, set_name, limit, offset)
    items = [CardSummary.model_validate(r) for r in rows]
    return Page[CardSummary](total=total, limit=limit, offset=offset, items=items)


@router.get("/cards/{card_key}", response_model=CardDetail)
def card_detail(db: Db, card_key: CardKey) -> CardDetail:
    card = _require_card(db, card_key)
    as_of = queries.latest_as_of(db, card_key, None, None)
    values: list[MarketValue] = []
    if as_of is not None:
        values = [
            _market_value(r) for r in queries.fetch_market_values(db, card_key, as_of, None, None)
        ]
    return CardDetail.model_validate({**card, "latest_market_values": values})


@router.get("/cards/{card_key}/market-value", response_model=MarketValueResponse)
def card_market_value(
    db: Db,
    card_key: CardKey,
    as_of_date: Annotated[
        date | None, Query(description="defaults to the latest available")
    ] = None,
    observation_type: ObservationType | None = None,
    currency: Annotated[str, Query(pattern=_CURRENCY_PATTERN)] = "USD",
) -> MarketValueResponse:
    card = _require_card(db, card_key)
    currency = currency.upper()
    effective = as_of_date or queries.latest_as_of(db, card_key, currency, observation_type)
    if effective is None:
        raise HTTPException(
            status_code=404, detail=f"No market value found for card {card_key} in {currency}"
        )
    rows = queries.fetch_market_values(db, card_key, effective, currency, observation_type)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No market value for card {card_key} in {currency} as of {effective}",
        )
    return MarketValueResponse(
        card=CardSummary.model_validate(card),
        as_of_date=effective,
        currency=currency,
        observation_type=observation_type,
        values=[_market_value(r) for r in rows],
    )


@router.get("/cards/{card_key}/price-history", response_model=PriceHistoryResponse)
def card_price_history(
    db: Db,
    card_key: CardKey,
    days: Annotated[int, Query(ge=1, le=365)] = 90,
    observation_type: ObservationType | None = None,
    currency: Annotated[str | None, Query(pattern=_CURRENCY_PATTERN)] = None,
) -> PriceHistoryResponse:
    card = _require_card(db, card_key)
    wanted_currency = currency.upper() if currency else None
    end = queries.history_window_end(db, card_key, wanted_currency, observation_type)
    start = None if end is None else end - timedelta(days=days - 1)
    rows = (
        []
        if end is None
        else queries.fetch_price_history(
            db, card_key, start, end, wanted_currency, observation_type
        )
    )
    return PriceHistoryResponse(
        card=CardSummary.model_validate(card),
        days=days,
        from_date=start,
        to_date=end,
        observation_type=observation_type,
        currency=wanted_currency,
        points=[PriceHistoryPoint.model_validate(r) for r in rows],
    )


@router.get("/cards/{card_key}/observations", response_model=Page[ObservationOut])
def card_observations(
    db: Db,
    card_key: CardKey,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> Page[ObservationOut]:
    _require_card(db, card_key)
    total, rows = queries.fetch_observations(db, card_key, limit, offset)
    items = [
        ObservationOut.model_validate(
            {**r, "observation_type_note": OBSERVATION_TYPE_NOTES.get(r["observation_type"], "")}
        )
        for r in rows
    ]
    return Page[ObservationOut](total=total, limit=limit, offset=offset, items=items)


def _anomaly(row: dict[str, Any]) -> AnomalyOut:
    card = None
    if row["card_key"] is not None and row["card_id"] is not None:
        card = {
            "card_key": row["card_key"],
            "card_id": row["card_id"],
            "name": row["card_name"],
            "set_name": row["set_name"],
            "card_number": row["card_number"],
            "rarity": row["rarity"],
            "image_small_url": row["image_small_url"],
        }
    return AnomalyOut.model_validate({**row, "card": card})


@router.get("/anomalies", response_model=Page[AnomalyOut])
def list_anomalies(
    db: Db,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    severity: Severity | None = None,
    anomaly_type: AnomalyType | None = None,
    card_key: Annotated[int | None, Query(ge=1)] = None,
) -> Page[AnomalyOut]:
    total, rows = queries.fetch_anomalies(db, limit, offset, severity, anomaly_type, card_key)
    return Page[AnomalyOut](
        total=total, limit=limit, offset=offset, items=[_anomaly(r) for r in rows]
    )


@router.get("/data-quality/summary", response_model=DataQualitySummary)
def data_quality(db: Db) -> DataQualitySummary:
    summary = queries.data_quality_summary(db)
    return DataQualitySummary.model_validate(
        {
            **summary,
            "generated_at": datetime.now(UTC),
            "notes": [
                "matched = auto or confirmed match to a card; review = awaiting human review; "
                "unmatched = no match row or rejected.",
                "All provider_aggregate observations are provider statistics, not completed sales.",
            ],
        }
    )
