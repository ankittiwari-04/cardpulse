"""Price observation model and validation rules (pure functions, no I/O)."""

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

OBSERVATION_TYPES = (
    "completed_sale",
    "asking_price",
    "provider_aggregate",
    "auction_result",
    "unknown",
)
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_PRICE_QUANTUM = Decimal("0.0001")
_MAX_PRICE = Decimal("100000000")  # NUMERIC(12, 4) holds values below 1e8

_MESSAGES = {
    "missing_price": "price is missing",
    "invalid_price": "price is not a finite number in the supported range",
    "non_positive_price": "price must be greater than zero",
}


@dataclass(frozen=True)
class PriceCandidate:
    """An observation as extracted by an adapter. Unvalidated; unknown values are None."""

    source: str
    source_card_ref: str | None
    observation_key: str | None
    observation_type: str | None
    price: Any
    currency: str | None
    observed_at: datetime | None
    payload: dict[str, Any]
    observed_at_raw: str | None = None
    raw_card_name: str | None = None
    raw_set_name: str | None = None
    raw_card_number: str | None = None
    raw_grade_label: str | None = None
    source_url: str | None = None
    condition_label: str | None = None
    language_label: str | None = None
    edition_label: str | None = None
    variant_label: str | None = None
    grading_company: str | None = None
    grade_value: Any = None
    is_sold: bool | None = None
    listing_id: str | None = None


@dataclass(frozen=True)
class NormalizedObservation:
    """A validated observation, ready to store."""

    source: str
    source_card_ref: str
    observation_key: str
    observation_type: str
    price: Decimal
    currency: str
    observed_at: datetime
    payload: dict[str, Any]
    raw_card_name: str | None = None
    raw_set_name: str | None = None
    raw_card_number: str | None = None
    raw_grade_label: str | None = None
    source_url: str | None = None
    condition_label: str | None = None
    language_label: str | None = None
    edition_label: str | None = None
    variant_label: str | None = None
    grading_company: str | None = None
    grade_value: Decimal | None = None
    is_sold: bool | None = None
    listing_id: str | None = None


@dataclass(frozen=True)
class QualityIssue:
    check_name: str
    message: str
    details: dict[str, Any]


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float) and value == value and value not in (float("inf"), float("-inf")):
        return value
    return repr(value)


def _context(candidate: PriceCandidate) -> dict[str, Any]:
    return {
        "source": candidate.source,
        "source_card_ref": candidate.source_card_ref,
        "observation_key": candidate.observation_key,
        "observed_at_raw": _jsonable(candidate.observed_at_raw),
        "raw_price": _jsonable(candidate.price),
        "raw_currency": _jsonable(candidate.currency),
    }


def _to_price(value: Any) -> tuple[Decimal | None, str | None]:
    """Returns (price, problem check name)."""
    if value is None:
        return None, "missing_price"
    if isinstance(value, bool):
        return None, "invalid_price"
    try:
        price = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None, "invalid_price"
    if not price.is_finite() or price >= _MAX_PRICE:
        return None, "invalid_price"
    if price <= 0:
        return None, "non_positive_price"
    price = price.quantize(_PRICE_QUANTUM, rounding=ROUND_HALF_UP)
    if price <= 0:
        return None, "non_positive_price"
    return price, None


def _to_grade(value: Any) -> tuple[Decimal | None, bool]:
    """Returns (grade, valid). Grades must be 1-10 in half-point steps."""
    if value is None:
        return None, True
    if isinstance(value, bool):
        return None, False
    try:
        grade = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None, False
    if not grade.is_finite() or grade < 1 or grade > 10 or (grade * 2) % 1 != 0:
        return None, False
    return grade, True


def validate_candidate(
    candidate: PriceCandidate, seen_listing_ids: set[tuple[str, str]]
) -> tuple[NormalizedObservation | None, list[QualityIssue]]:
    """Apply every rule and collect all problems. Returns (observation, issues).

    The observation is None when any issue was found (the candidate is rejected).
    seen_listing_ids is shared across a run to detect duplicate source listing ids.
    """
    issues: list[QualityIssue] = []

    def issue(check_name: str, message: str, **details: Any) -> None:
        issues.append(QualityIssue(check_name, message, {**_context(candidate), **details}))

    ref = candidate.source_card_ref
    if not isinstance(ref, str) or not ref.strip():
        issue("missing_source_card_ref", "source card reference is missing")

    if not candidate.observation_key:
        issue("missing_observation_key", "observation key is missing")

    observed_at = candidate.observed_at
    if observed_at is None:
        issue(
            "missing_observed_at",
            "observed_at is missing or unparseable (timezone-less timestamps are not guessed)",
        )
    elif observed_at.tzinfo is None:
        issue("missing_observed_at", "observed_at has no timezone; refusing to guess one")

    price, price_problem = _to_price(candidate.price)
    if price_problem:
        issue(price_problem, _MESSAGES[price_problem])

    currency = candidate.currency
    if not isinstance(currency, str) or not _CURRENCY_RE.match(currency):
        issue("invalid_currency", "currency must be a 3-letter uppercase code")

    obs_type = candidate.observation_type
    if obs_type not in OBSERVATION_TYPES:
        issue("invalid_observation_type", f"observation_type must be one of {OBSERVATION_TYPES}")
    elif obs_type == "completed_sale" and candidate.is_sold is not True:
        issue(
            "unsupported_sale_claim",
            "completed_sale requires the source to explicitly mark the price as sold",
        )

    grade, grade_ok = _to_grade(candidate.grade_value)
    if not grade_ok:
        issue("impossible_grade", "grade must be between 1 and 10 in half-point steps")

    listing_key = None
    if candidate.listing_id:
        listing_key = (candidate.source, candidate.listing_id)
        if listing_key in seen_listing_ids:
            issue("duplicate_listing_id", "source listing id was already seen in this run")

    if issues:
        return None, issues

    if listing_key is not None:
        seen_listing_ids.add(listing_key)
    return (
        NormalizedObservation(
            source=candidate.source,
            source_card_ref=ref.strip(),  # type: ignore[union-attr]
            observation_key=candidate.observation_key,  # type: ignore[arg-type]
            observation_type=obs_type,  # type: ignore[arg-type]
            price=price,  # type: ignore[arg-type]
            currency=currency,  # type: ignore[arg-type]
            observed_at=observed_at,  # type: ignore[arg-type]
            payload=candidate.payload,
            raw_card_name=candidate.raw_card_name,
            raw_set_name=candidate.raw_set_name,
            raw_card_number=candidate.raw_card_number,
            raw_grade_label=candidate.raw_grade_label,
            source_url=candidate.source_url,
            condition_label=candidate.condition_label,
            language_label=candidate.language_label,
            edition_label=candidate.edition_label,
            variant_label=candidate.variant_label,
            grading_company=candidate.grading_company,
            grade_value=grade,
            is_sold=candidate.is_sold,
            listing_id=candidate.listing_id,
        ),
        [],
    )
