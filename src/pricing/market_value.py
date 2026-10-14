"""Daily price aggregation and rolling market values.

Only MATCHED observations are used (stg_price_observations -> fct_card_matches status
auto/confirmed -> dim_cards). Series are never blended: the grain always includes
observation_type and observation_key. Currencies are never converted.
provider_aggregate values are provider statistics, NOT completed sales, and no
transaction volume is ever inferred.
"""

import logging
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from src.config import Settings, get_settings
from src.db.connection import transaction
from src.ingestion.runs import finish_run, start_run
from src.pricing.data import (
    METHODOLOGY_VERSION,
    PriceObservation,
    PricingConfig,
    PricingStats,
    age_in_days,
    chunked,
    in_age_window,
    is_headline,
    list_card_keys,
    load_grade_map,
    load_observations,
    record_failure,
    window_bounds,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "market_value_computation"

_Q2 = Decimal("0.01")
_Q4 = Decimal("0.0001")
_Q6 = Decimal("0.000001")


def _q(value: Decimal | None, quantum: Decimal) -> Decimal | None:
    return None if value is None else value.quantize(quantum, rounding=ROUND_HALF_UP)


# ---------- statistics ----------
def median(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def coefficient_of_variation(values: Sequence[Decimal]) -> Decimal | None:
    """Population standard deviation / mean. None with fewer than 2 values or a zero mean."""
    if len(values) < 2:
        return None
    mean = sum(values, Decimal(0)) / len(values)
    if mean == 0:
        return None
    variance = sum(((v - mean) ** 2 for v in values), Decimal(0)) / len(values)
    return variance.sqrt() / mean


def pct_change(current: Decimal | None, previous: Decimal | None) -> Decimal | None:
    """(current - previous) / previous * 100. None when either side is missing or previous is 0."""
    if current is None or previous is None or previous == 0:
        return None
    return (current - previous) / previous * 100


# ---------- daily aggregation ----------
@dataclass(frozen=True)
class DailyRow:
    card_key: int
    grade_id: int
    price_date: date
    source: str
    currency: str
    observation_type: str
    observation_key: str
    min_price: Decimal
    max_price: Decimal
    avg_price: Decimal
    median_price: Decimal
    observation_count: int
    price_dispersion: Decimal | None
    latest_price: Decimal
    latest_observed_at: datetime
    freshness_days: int
    as_of_date: date


def aggregate_daily(observations: Sequence[PriceObservation], as_of_date: date) -> list[DailyRow]:
    groups: dict[tuple, list[PriceObservation]] = defaultdict(list)
    for obs in observations:
        if obs.price_date <= as_of_date:
            groups[obs.daily_key].append(obs)
    rows: list[DailyRow] = []
    for key in sorted(groups):
        members = groups[key]
        prices = [o.price for o in members]
        latest = max(members, key=lambda o: (o.observed_at, o.stg_id))
        cv = coefficient_of_variation(prices)
        card_key, grade_id, price_date, source, currency, obs_type, obs_key = key
        rows.append(
            DailyRow(
                card_key=card_key,
                grade_id=grade_id,
                price_date=price_date,
                source=source,
                currency=currency,
                observation_type=obs_type,
                observation_key=obs_key,
                min_price=_q(min(prices), _Q4),
                max_price=_q(max(prices), _Q4),
                avg_price=_q(sum(prices, Decimal(0)) / len(prices), _Q4),
                median_price=_q(median(prices), _Q4),
                observation_count=len(prices),
                price_dispersion=_q(cv, _Q6),
                latest_price=_q(latest.price, _Q4),
                latest_observed_at=latest.observed_at,
                freshness_days=age_in_days(latest.price_date, as_of_date),
                as_of_date=as_of_date,
            )
        )
    return rows


# ---------- confidence ----------
@dataclass(frozen=True)
class Confidence:
    score: float  # 0-100
    components: dict[str, Any]


def _part(score: float, weight: float, value: dict[str, Any], explanation: str) -> dict[str, Any]:
    return {
        "score": round(score, 4),
        "weight": weight,
        "weighted": round(score * weight, 4),
        "value": value,
        "explanation": explanation,
    }


def compute_confidence(
    *,
    observation_count: int,
    age_days: int,
    dispersion: float | None,
    source_agreement: float | None,
    coverage_days: int,
    config: PricingConfig,
) -> Confidence:
    """Weighted sum of five 0-1 components, scaled to 0-100. Every component is explained.

    A component that cannot be assessed (dispersion with one observation, agreement with
    one source) scores a neutral 0.5 instead of being skipped, so thin evidence can never
    reach full confidence.
    """
    c = config
    count_score = min(1.0, observation_count / c.count_target)
    recency_score = max(0.0, 1.0 - age_days / c.stale_days)
    if dispersion is None:
        dispersion_score = 0.5
        dispersion_text = "not assessable (needs at least 2 observations): neutral 0.5"
    else:
        dispersion_score = max(0.0, 1.0 - dispersion / c.max_dispersion)
        dispersion_text = (
            f"coefficient of variation {dispersion:.4f}; 0 at {c.max_dispersion} or above"
        )
    if source_agreement is None:
        agreement_score = 0.5
        agreement_text = "not assessable (needs 2 sources reporting the same series): neutral 0.5"
    else:
        agreement_score = source_agreement
        agreement_text = f"sources agree to {source_agreement:.4f} (1 = identical medians)"
    completeness_score = min(1.0, coverage_days / 7)

    components = {
        "observation_count": _part(
            count_score,
            c.weight_count,
            {"observations_30d": observation_count, "target": c.count_target},
            f"{observation_count} observations in 30 days; full score at {c.count_target}",
        ),
        "recency": _part(
            recency_score,
            c.weight_recency,
            {"age_days": age_days, "stale_after_days": c.stale_days},
            f"latest observation {age_days} day(s) old; score reaches 0 at {c.stale_days} days",
        ),
        "dispersion": _part(
            dispersion_score,
            c.weight_dispersion,
            {"coefficient_of_variation": dispersion},
            dispersion_text,
        ),
        "source_agreement": _part(
            agreement_score,
            c.weight_agreement,
            {"agreement": source_agreement},
            agreement_text,
        ),
        "completeness": _part(
            completeness_score,
            c.weight_completeness,
            {"days_with_data_last_7": coverage_days},
            f"observations on {coverage_days} of the last 7 days",
        ),
    }
    total = 100.0 * (
        c.weight_count * count_score
        + c.weight_recency * recency_score
        + c.weight_dispersion * dispersion_score
        + c.weight_agreement * agreement_score
        + c.weight_completeness * completeness_score
    )
    components["total"] = {"score": round(total, 2), "scale": "0-100", "method": "weighted_sum"}
    return Confidence(score=round(total, 2), components=components)


# ---------- liquidity and recommendations ----------
def liquidity_tier(observation_type: str, observation_count: int, config: PricingConfig) -> str:
    """Liquidity can only come from real transaction evidence.

    completed_sale / auction_result observations are individual transactions, so their count
    is a volume signal. Anything else (provider aggregates, asking prices, unknown) carries no
    volume information and is 'unknown'; volume is never inferred.
    """
    if observation_type in ("completed_sale", "auction_result"):
        if observation_count >= config.liquid_min_observations:
            return "high"
        if observation_count >= config.medium_liquid_min_observations:
            return "medium"
        return "low"
    return "unknown"


def buyback_tier(confidence: float, effective_liquidity: str, config: PricingConfig) -> str:
    if confidence >= config.confidence_high and effective_liquidity == "high":
        return "high"
    if confidence >= config.confidence_medium and effective_liquidity in ("high", "medium"):
        return "medium"
    return "low"


@dataclass(frozen=True)
class Recommendation:
    market_value: Decimal
    buy_price: Decimal
    retail_price: Decimal
    buyback_price: Decimal
    liquidity_tier: str
    effective_liquidity: str
    buyback_tier: str
    buyback_discount: Decimal


def _money(value: Decimal) -> Decimal:
    return max(Decimal(0), value).quantize(_Q2, rounding=ROUND_HALF_UP)


def pricing_recommendations(
    value: Decimal,
    confidence: float,
    observation_type: str,
    observation_count: int,
    config: PricingConfig,
) -> Recommendation:
    """Policy-based estimates from the market value. Not guaranteed offers."""
    tier_liquidity = liquidity_tier(observation_type, observation_count, config)
    effective = config.unknown_liquidity if tier_liquidity == "unknown" else tier_liquidity
    tier = buyback_tier(confidence, effective, config)
    discount = {
        "high": config.buyback_discount_high,
        "medium": config.buyback_discount_medium,
        "low": config.buyback_discount_low,
    }[tier]
    return Recommendation(
        market_value=_money(value),
        buy_price=_money(value * (1 - config.buy_discount)),
        retail_price=_money(value * (1 + config.retail_markup)),
        buyback_price=_money(value * (1 - discount)),  # never negative
        liquidity_tier=tier_liquidity,
        effective_liquidity=effective,
        buyback_tier=tier,
        buyback_discount=discount,
    )


# ---------- rolling market values ----------
@dataclass(frozen=True)
class MarketValueRow:
    card_key: int
    grade_id: int
    as_of_date: date
    observation_type: str
    currency: str
    observation_key: str
    methodology_version: str
    price_7d_median: Decimal | None
    price_30d_median: Decimal | None
    price_7d_change_pct: Decimal | None
    price_30d_change_pct: Decimal | None
    observation_count_7d: int
    observation_count_30d: int
    price_dispersion: Decimal | None
    source_agreement_score: Decimal | None
    confidence_score: Decimal
    confidence_components: dict[str, Any]
    liquidity_tier: str
    buyback_tier: str
    buyback_discount: Decimal
    recommended_market_value: Decimal
    recommended_buy_price: Decimal
    recommended_retail_price: Decimal
    recommended_buyback_price: Decimal
    policy: dict[str, Any]
    latest_observed_at: datetime


def _window(
    observations: Sequence[PriceObservation], as_of: date, start_age: int, end_age: int
) -> list[PriceObservation]:
    """Observations whose age in days satisfies start_age <= age < end_age.

    age = as-of date - observation date (UTC calendar days), so the as-of day has age 0.
    Windows are half-open: they include start_age and exclude end_age.

        7d window        0 <= age < 7       30d window       0 <= age < 30
        previous 7d      7 <= age < 14      previous 30d    30 <= age < 60

    Future observations (age < 0) are never included. Every count, median, change
    percentage and coverage figure in this module is built from these windows.
    """
    return [o for o in observations if in_age_window(o.price_date, as_of, start_age, end_age)]


def source_agreement(
    observations: Sequence[PriceObservation], config: PricingConfig
) -> float | None:
    """1 - relative spread of per-source medians. None unless 2+ sources report the series."""
    by_source: dict[str, list[Decimal]] = defaultdict(list)
    for obs in observations:
        by_source[obs.source].append(obs.price)
    if len(by_source) < 2:
        return None
    medians = [median(prices) for prices in by_source.values()]
    mean = sum(medians, Decimal(0)) / len(medians)
    if mean <= 0:
        return None
    spread = float((max(medians) - min(medians)) / mean)
    return max(0.0, 1.0 - spread / config.max_source_spread)


def _build_row(
    series: tuple[int, int, str, str, str],
    observations: list[PriceObservation],
    as_of: date,
    config: PricingConfig,
) -> MarketValueRow | None:
    card_key, grade_id, obs_type, currency, obs_key = series
    w7 = _window(observations, as_of, 0, 7)
    w30 = _window(observations, as_of, 0, 30)
    if not w30:
        return None
    prev7 = _window(observations, as_of, 7, 14)
    prev30 = _window(observations, as_of, 30, 60)

    med7 = median([o.price for o in w7])
    med30 = median([o.price for o in w30])
    change7 = pct_change(med7, median([o.price for o in prev7]))
    change30 = pct_change(med30, median([o.price for o in prev30]))

    basis, basis_label = (w7, "7d") if w7 else (w30, "30d")
    value = median([o.price for o in basis])
    latest = max(w30, key=lambda o: (o.observed_at, o.stg_id))
    age_days = age_in_days(latest.price_date, as_of)
    cv = coefficient_of_variation([o.price for o in w30])
    agreement = source_agreement(basis, config)
    confidence = compute_confidence(
        observation_count=len(w30),
        age_days=age_days,
        dispersion=None if cv is None else float(cv),
        source_agreement=agreement,
        coverage_days=len({o.price_date for o in w7}),
        config=config,
    )
    rec = pricing_recommendations(value, confidence.score, obs_type, len(w30), config)
    policy = {
        "basis_window": basis_label,
        "liquidity_tier": rec.liquidity_tier,
        "liquidity_treated_as": rec.effective_liquidity,
        "buyback_tier": rec.buyback_tier,
        "buyback_discount": str(rec.buyback_discount),
        "buy_discount": str(config.buy_discount),
        "retail_markup": str(config.retail_markup),
        "note": (
            "Policy-based estimates, not a guaranteed offer. provider_aggregate values are "
            "provider statistics, not completed sales; no transaction volume is inferred."
        ),
    }
    return MarketValueRow(
        card_key=card_key,
        grade_id=grade_id,
        as_of_date=as_of,
        observation_type=obs_type,
        currency=currency,
        observation_key=obs_key,
        methodology_version=METHODOLOGY_VERSION,
        price_7d_median=_q(med7, _Q4),
        price_30d_median=_q(med30, _Q4),
        price_7d_change_pct=_q(change7, _Q4),
        price_30d_change_pct=_q(change30, _Q4),
        observation_count_7d=len(w7),
        observation_count_30d=len(w30),
        price_dispersion=_q(cv, _Q6),
        source_agreement_score=None if agreement is None else _q(Decimal(str(agreement)), _Q4),
        confidence_score=Decimal(str(confidence.score)).quantize(_Q2),
        confidence_components=confidence.components,
        liquidity_tier=rec.liquidity_tier,
        buyback_tier=rec.buyback_tier,
        buyback_discount=rec.buyback_discount,
        recommended_market_value=rec.market_value,
        recommended_buy_price=rec.buy_price,
        recommended_retail_price=rec.retail_price,
        recommended_buyback_price=rec.buyback_price,
        policy=policy,
        latest_observed_at=latest.observed_at,
    )


def build_market_values(
    observations: Sequence[PriceObservation], as_of_date: date, config: PricingConfig
) -> list[MarketValueRow]:
    """One row per headline series that has at least one observation in the last 30 days."""
    series: dict[tuple, list[PriceObservation]] = defaultdict(list)
    for obs in observations:
        if obs.price_date <= as_of_date and is_headline(config, obs.observation_key):
            series[obs.series].append(obs)
    rows = []
    for key in sorted(series):
        row = _build_row(key, series[key], as_of_date, config)
        if row is not None:
            rows.append(row)
    return rows


# ---------- persistence ----------
_DAILY_KEY = (
    "card_key",
    "grade_id",
    "price_date",
    "source",
    "currency",
    "observation_type",
    "observation_key",
)
_DAILY_VALUES = (
    "min_price",
    "max_price",
    "avg_price",
    "median_price",
    "observation_count",
    "price_dispersion",
    "latest_price",
    "latest_observed_at",
    "freshness_days",
    "as_of_date",
)
_daily_all = ", ".join((*_DAILY_KEY, *_DAILY_VALUES))
_daily_params = ", ".join(f"%({c})s" for c in (*_DAILY_KEY, *_DAILY_VALUES))
_daily_sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in _DAILY_VALUES)
_daily_old = ", ".join(f"fct_daily_card_prices.{c}" for c in _DAILY_VALUES)
_daily_new = ", ".join(f"EXCLUDED.{c}" for c in _DAILY_VALUES)

_DAILY_SQL = f"""
INSERT INTO fct_daily_card_prices ({_daily_all}, run_id)
VALUES ({_daily_params}, %(run_id)s)
ON CONFLICT ({", ".join(_DAILY_KEY)}) DO UPDATE SET
    {_daily_sets}, run_id = EXCLUDED.run_id, computed_at = now()
WHERE ({_daily_old}) IS DISTINCT FROM ({_daily_new})
RETURNING (xmax = 0) AS inserted
"""

_MV_KEY = (
    "card_key",
    "grade_id",
    "as_of_date",
    "observation_type",
    "currency",
    "observation_key",
    "methodology_version",
)
_MV_VALUES = (
    "price_7d_median",
    "price_30d_median",
    "price_7d_change_pct",
    "price_30d_change_pct",
    "observation_count_7d",
    "observation_count_30d",
    "price_dispersion",
    "source_agreement_score",
    "confidence_score",
    "confidence_components",
    "liquidity_tier",
    "buyback_tier",
    "buyback_discount",
    "recommended_market_value",
    "recommended_buy_price",
    "recommended_retail_price",
    "recommended_buyback_price",
    "policy",
    "latest_observed_at",
)
_mv_all = ", ".join((*_MV_KEY, *_MV_VALUES))
_mv_params = ", ".join(f"%({c})s" for c in (*_MV_KEY, *_MV_VALUES))
_mv_sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in _MV_VALUES)
_mv_old = ", ".join(f"fct_card_market_values.{c}" for c in _MV_VALUES)
_mv_new = ", ".join(f"EXCLUDED.{c}" for c in _MV_VALUES)

_MV_SQL = f"""
INSERT INTO fct_card_market_values ({_mv_all}, run_id)
VALUES ({_mv_params}, %(run_id)s)
ON CONFLICT ({", ".join(_MV_KEY)}) DO UPDATE SET
    {_mv_sets}, run_id = EXCLUDED.run_id, computed_at = now()
WHERE ({_mv_old}) IS DISTINCT FROM ({_mv_new})
RETURNING (xmax = 0) AS inserted
"""


def _count(rows_returned: list[tuple[bool]], attempted: int) -> tuple[int, int, int]:
    inserted = sum(1 for r in rows_returned if r[0])
    updated = len(rows_returned) - inserted
    return inserted, updated, attempted - len(rows_returned)


def upsert_daily(
    conn: psycopg.Connection, rows: Sequence[DailyRow], run_id: int | None
) -> tuple[int, int, int]:
    """Returns (inserted, updated, unchanged). Unchanged rows are not rewritten."""
    returned = []
    for row in rows:
        result = conn.execute(_DAILY_SQL, {**asdict(row), "run_id": run_id}).fetchone()
        if result is not None:
            returned.append(result)
    return _count(returned, len(rows))


def upsert_market_values(
    conn: psycopg.Connection, rows: Sequence[MarketValueRow], run_id: int | None
) -> tuple[int, int, int]:
    returned = []
    for row in rows:
        params = asdict(row)
        params["confidence_components"] = Jsonb(row.confidence_components)
        params["policy"] = Jsonb(row.policy)
        params["run_id"] = run_id
        result = conn.execute(_MV_SQL, params).fetchone()
        if result is not None:
            returned.append(result)
    return _count(returned, len(rows))


# ---------- run ----------
@dataclass
class ComputeResult:
    run_id: int | None
    stats: PricingStats
    dry_run: bool
    as_of_date: date


def compute_market_values(
    settings: Settings | None = None,
    *,
    as_of_date: date,
    dry_run: bool = False,
    card_key: int | None = None,
    source: str | None = None,
    observation_type: str | None = None,
    batch_size: int = 200,
    dsn: str | None = None,
    config: PricingConfig | None = None,
) -> ComputeResult:
    """Daily rows and market values for the 60-day window ending at as_of_date.

    Idempotent: rows are upserted on their primary keys and only rewritten when a value
    actually changes. One transaction per batch of cards.
    """
    settings = settings or get_settings()
    dsn = dsn or settings.psycopg_dsn
    config = config or PricingConfig.from_settings(settings)

    run_id: int | None = None
    if not dry_run:
        run_id = start_run(
            PIPELINE_NAME,
            {
                "as_of_date": as_of_date.isoformat(),
                "card_key": card_key,
                "source": source,
                "observation_type": observation_type,
            },
            dsn,
        )
        logger.info("Started run %s (%s)", run_id, PIPELINE_NAME)

    stats = PricingStats()
    start, end = window_bounds(as_of_date, 59)
    try:
        with transaction(dsn, read_only=True) as conn:
            grade_map = load_grade_map(conn)
            keys = list_card_keys(
                conn,
                start,
                end,
                card_key=card_key,
                source=source,
                observation_type=observation_type,
            )
        for chunk in chunked(keys, batch_size):
            with transaction(dsn, read_only=dry_run) as conn:
                observations, skipped = load_observations(
                    conn,
                    chunk,
                    start,
                    end,
                    source=source,
                    observation_type=observation_type,
                    grade_map=grade_map,
                )
                daily = aggregate_daily(observations, as_of_date)
                values = build_market_values(observations, as_of_date, config)
                daily_counts = (0, 0, 0) if dry_run else upsert_daily(conn, daily, run_id)
                value_counts = (0, 0, 0) if dry_run else upsert_market_values(conn, values, run_id)
            stats.cards_processed += len(chunk)  # counted only after the batch committed
            stats.observations_processed += len(observations)
            stats.observations_skipped += skipped
            stats.daily_rows_computed += len(daily)
            stats.market_value_rows_computed += len(values)
            stats.daily_price_rows_written += daily_counts[0] + daily_counts[1]
            stats.market_value_rows_written += value_counts[0] + value_counts[1]
            stats.rows_unchanged += daily_counts[2] + value_counts[2]
            logger.info("Processed batch of %s cards", len(chunk))
    except BaseException as exc:
        if run_id is not None:
            record_failure(run_id, stats, exc, settings, dsn)
        raise

    if run_id is not None:
        finish_run(
            run_id,
            "succeeded",
            stats.observations_processed,
            stats.rows_written,
            {**stats.as_metadata(), "methodology_version": METHODOLOGY_VERSION},
            dsn=dsn,
        )
    return ComputeResult(run_id=run_id, stats=stats, dry_run=dry_run, as_of_date=as_of_date)


__all__ = ["UTC"]  # keep the UTC import available for callers building windows
