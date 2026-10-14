"""Shared pricing building blocks: configuration, observation loading, run bookkeeping."""

import argparse
import fnmatch
import logging
from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, TypeVar

import psycopg

from src.config import Settings
from src.ingestion.catalog import _non_empty, _secrets
from src.ingestion.price_models import OBSERVATION_TYPES
from src.ingestion.runs import finish_run, sanitize_error

logger = logging.getLogger(__name__)

METHODOLOGY_VERSION = "mv1"
T = TypeVar("T")


@dataclass(frozen=True)
class PricingConfig:
    """Every tunable of the methodology. Defaults match the README."""

    headline_patterns: tuple[str, ...] = ("tcgplayer/*/market", "cardmarket/trendPrice")
    stale_days: int = 7
    stale_lookback_days: int = 90
    min_observations: int = 3
    outlier_z: float = 3.5
    outlier_min_baseline: int = 5
    spike_pct: float = 50.0
    conflict_pct: float = 25.0
    buyback_discount_high: Decimal = Decimal("0.15")
    buyback_discount_medium: Decimal = Decimal("0.30")
    buyback_discount_low: Decimal = Decimal("0.50")
    buy_discount: Decimal = Decimal("0.30")
    retail_markup: Decimal = Decimal("0.10")
    unknown_liquidity: str = "low"
    confidence_high: float = 80.0
    confidence_medium: float = 50.0
    liquid_min_observations: int = 10
    medium_liquid_min_observations: int = 3
    count_target: int = 10
    max_dispersion: float = 0.5
    max_source_spread: float = 0.25
    weight_count: float = 0.25
    weight_recency: float = 0.25
    weight_dispersion: float = 0.20
    weight_agreement: float = 0.15
    weight_completeness: float = 0.15

    @classmethod
    def from_settings(cls, s: Settings) -> "PricingConfig":
        patterns = tuple(p.strip() for p in s.pricing_headline_patterns.split(",") if p.strip())
        return cls(
            headline_patterns=patterns,
            stale_days=s.pricing_stale_days,
            min_observations=s.pricing_min_observations,
            outlier_z=s.pricing_outlier_z,
            spike_pct=s.pricing_spike_pct,
            conflict_pct=s.pricing_conflict_pct,
            buyback_discount_high=Decimal(str(s.pricing_buyback_discount_high)),
            buyback_discount_medium=Decimal(str(s.pricing_buyback_discount_medium)),
            buyback_discount_low=Decimal(str(s.pricing_buyback_discount_low)),
            buy_discount=Decimal(str(s.pricing_buy_discount)),
            retail_markup=Decimal(str(s.pricing_retail_markup)),
            unknown_liquidity=s.pricing_unknown_liquidity,
        )


def is_headline(config: PricingConfig, observation_key: str) -> bool:
    return any(fnmatch.fnmatchcase(observation_key, p) for p in config.headline_patterns)


def window_bounds(as_of_date: date, days_back: int) -> tuple[datetime, datetime]:
    """[start, end) in UTC covering as_of_date - days_back .. as_of_date inclusive."""
    start = datetime.combine(as_of_date - timedelta(days=days_back), time.min, tzinfo=UTC)
    end = datetime.combine(as_of_date + timedelta(days=1), time.min, tzinfo=UTC)
    return start, end


def chunked(items: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


@dataclass(frozen=True)
class PriceObservation:
    """A matched, valid-price observation. price_date is the UTC calendar date."""

    stg_id: int
    card_key: int
    grade_id: int
    source: str
    observation_type: str
    currency: str
    observation_key: str
    price: Decimal
    observed_at: datetime

    @property
    def price_date(self) -> date:
        return self.observed_at.astimezone(UTC).date()

    @property
    def series(self) -> tuple[int, int, str, str, str]:
        return (
            self.card_key,
            self.grade_id,
            self.observation_type,
            self.currency,
            self.observation_key,
        )

    @property
    def daily_key(self) -> tuple[int, int, date, str, str, str, str]:
        return (
            self.card_key,
            self.grade_id,
            self.price_date,
            self.source,
            self.currency,
            self.observation_type,
            self.observation_key,
        )


@dataclass
class PricingStats:
    cards_processed: int = 0
    observations_processed: int = 0
    observations_skipped: int = 0
    daily_rows_computed: int = 0
    market_value_rows_computed: int = 0
    daily_price_rows_written: int = 0
    market_value_rows_written: int = 0
    anomalies_detected: int = 0
    anomaly_rows_written: int = 0
    alerts_created: int = 0
    rows_unchanged: int = 0

    @property
    def rows_written(self) -> int:
        return (
            self.daily_price_rows_written
            + self.market_value_rows_written
            + self.anomaly_rows_written
        )

    def as_metadata(self) -> dict[str, int]:
        data = asdict(self)
        data["rows_written"] = self.rows_written
        return data


# ---------- grades ----------
@dataclass(frozen=True)
class GradeMap:
    raw_id: int
    graded: dict[tuple[str, Decimal], int]

    def resolve(self, grading_company: str | None, grade_value: Decimal | None) -> int | None:
        """No grading info at all -> the RAW bucket. A partial or unknown grade -> None."""
        if grading_company is None and grade_value is None:
            return self.raw_id
        if grading_company is None or grade_value is None:
            return None
        return self.graded.get((grading_company.upper(), Decimal(str(grade_value))))


def load_grade_map(conn: psycopg.Connection) -> GradeMap:
    rows = conn.execute(
        "SELECT grade_id, grading_company, grade, label, is_graded FROM dim_grades"
    ).fetchall()
    raw_id = None
    graded: dict[tuple[str, Decimal], int] = {}
    for grade_id, company, grade, label, is_graded in rows:
        if label == "RAW" and not is_graded:
            raw_id = grade_id
        elif is_graded and grade is not None:
            graded[(company.upper(), Decimal(str(grade)))] = grade_id
    if raw_id is None:
        raise RuntimeError("dim_grades has no RAW row; run make db-migrate")
    return GradeMap(raw_id=raw_id, graded=graded)


# ---------- loading ----------
_MATCHED_JOIN = """
FROM stg_price_observations s
JOIN fct_card_matches m ON m.stg_observation_id = s.id
     AND m.status IN ('auto', 'confirmed') AND m.card_key IS NOT NULL
JOIN dim_cards c ON c.card_key = m.card_key
"""

_FILTERS = """
  AND (%(source)s::text IS NULL OR s.source = %(source)s)
  AND (%(observation_type)s::text IS NULL OR s.observation_type = %(observation_type)s)
"""

_CARD_KEYS_SQL = f"""
SELECT DISTINCT m.card_key
{_MATCHED_JOIN}
WHERE s.observed_at >= %(start)s AND s.observed_at < %(end)s
  AND s.price IS NOT NULL AND s.price > 0
  AND (%(card_key)s::bigint IS NULL OR m.card_key = %(card_key)s)
  {_FILTERS}
ORDER BY 1
"""

# Identical observations (same card, source, series, timestamp, price, grading) count once.
_OBSERVATIONS_SQL = f"""
SELECT DISTINCT ON (m.card_key, s.source, s.observation_type, s.currency, s.observation_key,
                    s.observed_at, s.price, s.grading_company, s.grade_value)
       s.id, m.card_key, s.source, s.observation_type, s.currency, s.observation_key,
       s.price, s.observed_at, s.grading_company, s.grade_value
{_MATCHED_JOIN}
WHERE s.observed_at >= %(start)s AND s.observed_at < %(end)s
  AND s.price IS NOT NULL AND s.price > 0
  AND m.card_key = ANY(%(card_keys)s::bigint[])
  {_FILTERS}
ORDER BY m.card_key, s.source, s.observation_type, s.currency, s.observation_key,
         s.observed_at, s.price, s.grading_company, s.grade_value, s.id
"""


def list_card_keys(
    conn: psycopg.Connection,
    start: datetime,
    end: datetime,
    *,
    card_key: int | None,
    source: str | None,
    observation_type: str | None,
) -> list[int]:
    rows = conn.execute(
        _CARD_KEYS_SQL,
        {
            "start": start,
            "end": end,
            "card_key": card_key,
            "source": source,
            "observation_type": observation_type,
        },
    ).fetchall()
    return [r[0] for r in rows]


def load_observations(
    conn: psycopg.Connection,
    card_keys: Sequence[int],
    start: datetime,
    end: datetime,
    *,
    source: str | None,
    observation_type: str | None,
    grade_map: GradeMap,
) -> tuple[list[PriceObservation], int]:
    """Matched, positive-price observations. Returns (observations, skipped_unmapped_grade)."""
    rows = conn.execute(
        _OBSERVATIONS_SQL,
        {
            "start": start,
            "end": end,
            "card_keys": list(card_keys),
            "source": source,
            "observation_type": observation_type,
        },
    ).fetchall()
    observations: list[PriceObservation] = []
    skipped = 0
    for stg_id, card_key, src, obs_type, currency, key, price, observed_at, company, grade in rows:
        grade_id = grade_map.resolve(company, grade)
        if grade_id is None:
            skipped += 1
            continue
        observations.append(
            PriceObservation(
                stg_id=stg_id,
                card_key=card_key,
                grade_id=grade_id,
                source=src,
                observation_type=obs_type,
                currency=currency,
                observation_key=key,
                price=price,
                observed_at=observed_at,
            )
        )
    return observations, skipped


# ---------- run bookkeeping ----------
def record_failure(
    run_id: int, stats: PricingStats, exc: BaseException, settings: Settings, dsn: str
) -> None:
    """Failure bookkeeping in its own transaction so it survives any batch rollback."""
    try:
        finish_run(
            run_id,
            "failed",
            stats.observations_processed,
            stats.rows_written,
            stats.as_metadata(),
            error_message=sanitize_error(exc, _secrets(settings)),
            dsn=dsn,
        )
    except Exception as tracking_exc:  # never mask the original error
        logger.error(
            "Could not record failure for run %s: %s",
            run_id,
            sanitize_error(tracking_exc, _secrets(settings)),
        )


# ---------- CLI helpers ----------
def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not an integer: {value!r}") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return number


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--as-of-date", type=_iso_date, default=None, help="YYYY-MM-DD (default: today, UTC)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="compute but write nothing (read-only transaction)"
    )
    parser.add_argument("--card-key", type=_positive_int, default=None, help="only this card")
    parser.add_argument("--source", type=_non_empty, default=None, help="only this source")
    parser.add_argument(
        "--observation-type", choices=OBSERVATION_TYPES, default=None, help="only this type"
    )
    parser.add_argument("--batch-size", type=_positive_int, default=200, help="cards per commit")


def resolve_as_of(value: date | None) -> date:
    return value or datetime.now(UTC).date()


def jsonable(value: Any) -> Any:
    """Decimals and dates to JSON-safe values (used for explanation payloads)."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value
