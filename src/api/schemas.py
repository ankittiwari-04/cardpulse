"""Pydantic response models for the internal pricing API."""

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field, PlainSerializer

# Prices are NUMERIC in Postgres (Decimal in Python) and JSON numbers on the wire.
Number = Annotated[Decimal, PlainSerializer(float, return_type=float, when_used="json")]

ObservationType = Literal[
    "completed_sale", "asking_price", "provider_aggregate", "auction_result", "unknown"
]
Severity = Literal["info", "warning", "error", "critical"]
AnomalyType = Literal[
    "duplicate_observation",
    "missing_price",
    "non_positive_price",
    "statistical_outlier",
    "price_spike",
    "price_drop",
    "stale_data",
    "conflicting_sources",
    "missing_match",
    "insufficient_observations",
]

OBSERVATION_TYPE_NOTES = {
    "provider_aggregate": (
        "Provider aggregate statistic (market, trend, average or low). "
        "Not a completed sale; no transaction volume is implied."
    ),
    "asking_price": "Seller asking price. Not a realized sale price.",
    "completed_sale": "Completed sale, as explicitly reported by the source.",
    "auction_result": "Auction result, as reported by the source.",
    "unknown": "The source did not say what kind of price this is.",
}

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    total: int
    limit: int
    offset: int
    items: list[T]


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    service: str
    version: str
    database: Literal["ok", "unavailable"]
    timestamp: datetime


class CardSummary(BaseModel):
    card_key: int
    card_id: str
    name: str
    set_name: str | None = None
    card_number: str | None = None
    rarity: str | None = None
    image_small_url: str | None = None


class DataFreshness(BaseModel):
    latest_observed_at: datetime | None = None
    age_days_at_as_of: int | None = None
    age_days_now: float | None = None
    computed_at: datetime | None = None


class MarketValue(BaseModel):
    observation_type: ObservationType
    observation_type_note: str
    observation_key: str
    grade_id: int
    grade_label: str
    currency: str
    as_of_date: date
    price_7d_median: Number | None = None
    price_30d_median: Number | None = None
    price_7d_change_pct: Number | None = None
    price_30d_change_pct: Number | None = None
    observation_count_7d: int = 0
    observation_count_30d: int = 0
    price_dispersion: Number | None = None
    source_agreement_score: Number | None = None
    confidence_score: Number | None = None
    confidence_components: dict[str, Any] = Field(default_factory=dict)
    liquidity_tier: str | None = None
    buyback_tier: str | None = None
    buyback_discount: Number | None = None
    recommended_market_value: Number | None = None
    recommended_buy_price: Number | None = None
    recommended_retail_price: Number | None = None
    recommended_buyback_price: Number | None = None
    methodology_version: str
    policy: dict[str, Any] = Field(default_factory=dict)
    data_freshness: DataFreshness


class CardDetail(CardSummary):
    supertype: str | None = None
    subtypes: list[str] = Field(default_factory=list)
    set_id: str | None = None
    series: str | None = None
    artist: str | None = None
    release_date: date | None = None
    image_large_url: str | None = None
    latest_market_values: list[MarketValue] = Field(default_factory=list)


class MarketValueResponse(BaseModel):
    card: CardSummary
    as_of_date: date
    currency: str
    observation_type: ObservationType | None = None
    values: list[MarketValue]


class PriceHistoryPoint(BaseModel):
    price_date: date
    source: str
    observation_type: ObservationType
    observation_key: str
    currency: str
    grade_id: int
    grade_label: str
    observation_count: int
    min_price: Number | None = None
    max_price: Number | None = None
    avg_price: Number | None = None
    median_price: Number | None = None
    price_dispersion: Number | None = None
    latest_price: Number | None = None
    latest_observed_at: datetime | None = None


class PriceHistoryResponse(BaseModel):
    card: CardSummary
    days: int
    from_date: date | None = None
    to_date: date | None = None
    observation_type: ObservationType | None = None
    currency: str | None = None
    points: list[PriceHistoryPoint]
    notice: str = (
        "Only dates that have data are returned; missing dates are not filled in or estimated."
    )


class ObservationOut(BaseModel):
    observation_id: int
    source: str
    source_card_ref: str | None = None
    observation_type: ObservationType
    observation_type_note: str
    observation_key: str
    price: Number | None = None
    currency: str
    observed_at: datetime
    match_method: str
    match_score: Number | None = None
    match_status: str


class AnomalyOut(BaseModel):
    anomaly_id: int
    anomaly_type: AnomalyType
    severity: Severity
    as_of_date: date
    detected_at: datetime
    resolved_at: datetime | None = None
    card: CardSummary | None = None
    stg_observation_id: int | None = None
    source: str | None = None
    observation_type: str | None = None
    observation_key: str | None = None
    currency: str | None = None
    expected_price: Number | None = None
    observed_price: Number | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class SourceCount(BaseModel):
    source: str
    observations: int


class TypeCount(BaseModel):
    observation_type: str
    observations: int


class PipelineRunSummary(BaseModel):
    run_id: int
    pipeline_name: str
    status: str
    finished_at: datetime | None = None
    rows_read: int | None = None
    rows_written: int | None = None


class SourceFreshness(BaseModel):
    source: str
    latest_observed_at: datetime | None = None
    latest_loaded_at: datetime | None = None
    age_days: Number | None = None


class DataQualitySummary(BaseModel):
    generated_at: datetime
    total_observations: int
    matched_observations: int
    unmatched_observations: int
    review_observations: int
    observations_by_source: list[SourceCount]
    observations_by_type: list[TypeCount]
    latest_successful_runs: list[PipelineRunSummary]
    data_freshness_by_source: list[SourceFreshness]
    open_anomalies: int
    notes: list[str]
