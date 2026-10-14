import itertools
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pytest

from src.pricing.data import PriceObservation, PricingConfig, in_age_window, is_headline
from src.pricing.market_value import (
    aggregate_daily,
    build_market_values,
    coefficient_of_variation,
    compute_confidence,
    liquidity_tier,
    median,
    pct_change,
    pricing_recommendations,
)

D = date(2026, 10, 14)
CFG = PricingConfig()
_ids = itertools.count(1)


def make_obs(
    days_ago,
    price,
    *,
    hour=0,
    card_key=1,
    grade_id=1,
    source="pokemontcg",
    obs_type="provider_aggregate",
    currency="USD",
    key="tcgplayer/normal/market",
):
    observed = datetime.combine(D - timedelta(days=days_ago), time(hour), tzinfo=UTC)
    return PriceObservation(
        next(_ids),
        card_key,
        grade_id,
        source,
        obs_type,
        currency,
        key,
        Decimal(str(price)),
        observed,
    )


# ---------- statistics ----------
def test_median():
    assert median([Decimal(3), Decimal(1), Decimal(2)]) == Decimal(2)
    assert median([Decimal(1), Decimal(2), Decimal(3), Decimal(4)]) == Decimal("2.5")
    assert median([Decimal(7)]) == Decimal(7)
    assert median([]) is None


def test_coefficient_of_variation():
    assert coefficient_of_variation([Decimal(10)] * 3) == 0
    assert coefficient_of_variation([Decimal(10)]) is None
    assert coefficient_of_variation([Decimal(0), Decimal(0)]) is None
    cv = coefficient_of_variation([Decimal(10), Decimal(20), Decimal(30)])
    assert float(cv) == pytest.approx(0.408248, rel=1e-4)


def test_pct_change():
    assert pct_change(Decimal(110), Decimal(100)) == Decimal(10)
    assert pct_change(Decimal(80), Decimal(100)) == Decimal(-20)
    assert pct_change(Decimal(5), Decimal(0)) is None
    assert pct_change(None, Decimal(5)) is None
    assert pct_change(Decimal(5), None) is None


def test_headline_patterns():
    assert is_headline(CFG, "tcgplayer/normal/market")
    assert is_headline(CFG, "tcgplayer/holofoil/market")
    assert is_headline(CFG, "cardmarket/trendPrice")
    assert not is_headline(CFG, "tcgplayer/normal/low")
    assert not is_headline(CFG, "cardmarket/lowPrice")


# ---------- daily aggregation ----------
def test_daily_aggregation_groups_by_series_and_keeps_observation_type():
    obs = [
        make_obs(0, 10, hour=1),
        make_obs(0, 20, hour=2),
        make_obs(0, 30, hour=3),
        make_obs(0, 5, obs_type="asking_price"),
        make_obs(1, 8),
    ]
    rows = aggregate_daily(obs, D)
    assert len(rows) == 3
    today = next(
        r for r in rows if r.price_date == D and r.observation_type == "provider_aggregate"
    )
    assert today.observation_count == 3
    assert (today.min_price, today.max_price) == (Decimal(10), Decimal(30))
    assert today.avg_price == Decimal(20) and today.median_price == Decimal(20)
    assert today.latest_price == Decimal(30) and today.freshness_days == 0
    assert float(today.price_dispersion) == pytest.approx(0.408248, rel=1e-4)
    asking = next(r for r in rows if r.observation_type == "asking_price")
    assert asking.observation_count == 1 and asking.price_dispersion is None
    yesterday = next(r for r in rows if r.price_date == D - timedelta(days=1))
    assert yesterday.freshness_days == 1


def test_daily_aggregation_ignores_future_observations():
    assert aggregate_daily([make_obs(-1, 10)], D) == []


# ---------- rolling windows ----------
def test_rolling_7_and_30_day_values_and_changes():
    # offsets 59..7 cost 100, offsets 6..0 cost 120
    obs = [make_obs(k, 100 if k >= 7 else 120) for k in range(0, 60)]
    (row,) = build_market_values(obs, D, CFG)
    assert (row.observation_count_7d, row.observation_count_30d) == (7, 30)
    assert row.price_7d_median == Decimal(120)
    assert row.price_30d_median == Decimal(100)  # 23 of 30 observations cost 100
    assert row.price_7d_change_pct == Decimal(20)  # vs offsets 13..7 (all 100)
    assert row.price_30d_change_pct == Decimal(0)  # vs offsets 59..30 (all 100)
    assert row.recommended_market_value == Decimal("120.00")
    assert row.observation_type == "provider_aggregate"


def test_30_day_change_percentage():
    obs = [make_obs(k, 100 if k >= 30 else 130) for k in range(0, 60)]
    (row,) = build_market_values(obs, D, CFG)
    assert row.price_30d_median == Decimal(130)
    assert row.price_30d_change_pct == Decimal(30)
    assert row.price_7d_change_pct == Decimal(0)


def test_window_boundaries():
    """Windows are half-open on age: 7d is age 0..6 and 30d is age 0..29.

    The age-7 observation is outside the 7-day window but inside the 30-day window.
    The age-30 observation is outside the 30-day window; it starts the previous 30 days.
    """
    obs = [make_obs(6, 10), make_obs(7, 99), make_obs(29, 20), make_obs(30, 99)]
    (row,) = build_market_values(obs, D, CFG)

    assert row.observation_count_7d == 1  # ages 0-6: only the age-6 observation
    assert row.price_7d_median == Decimal(10)
    assert row.observation_count_30d == 3  # ages 0-29: the observations aged 6, 7 and 29
    assert row.price_30d_median == Decimal(20)  # median of 10, 99, 20

    # the previous windows start exactly where the current ones end
    assert row.price_7d_change_pct == Decimal("-89.8990")  # 10 vs the age-7 value 99
    assert row.price_30d_change_pct == Decimal("-79.7980")  # 20 vs the age-30 value 99
    assert row.recommended_market_value == Decimal("10.00")  # the 7d median is the basis


def test_age_window_helper_is_half_open():
    assert in_age_window(D, D, 0, 7)
    assert in_age_window(D - timedelta(days=6), D, 0, 7)
    assert not in_age_window(D - timedelta(days=7), D, 0, 7)
    assert in_age_window(D - timedelta(days=7), D, 7, 14)
    assert not in_age_window(D - timedelta(days=14), D, 7, 14)
    assert not in_age_window(D + timedelta(days=1), D, 0, 7)  # future observations are excluded


def test_missing_prior_window_gives_no_change():
    (row,) = build_market_values([make_obs(k, 50) for k in range(0, 5)], D, CFG)
    assert row.price_7d_change_pct is None and row.price_30d_change_pct is None


def test_no_recent_data_gives_no_row_and_non_headline_series_are_skipped():
    assert build_market_values([make_obs(45, 10)], D, CFG) == []
    assert build_market_values([make_obs(0, 10, key="tcgplayer/normal/low")], D, CFG) == []
    assert build_market_values([make_obs(-1, 10)], D, CFG) == []


def test_series_and_observation_types_are_never_blended():
    obs = [
        make_obs(0, 10),
        make_obs(0, 500, obs_type="asking_price"),
        make_obs(0, 20, currency="EUR", key="cardmarket/trendPrice"),
    ]
    rows = build_market_values(obs, D, CFG)
    assert {(r.observation_type, r.currency, r.recommended_market_value) for r in rows} == {
        ("provider_aggregate", "USD", Decimal("10.00")),
        ("asking_price", "USD", Decimal("500.00")),
        ("provider_aggregate", "EUR", Decimal("20.00")),
    }


def test_value_falls_back_to_30_day_median_without_recent_data():
    (row,) = build_market_values([make_obs(20, 40), make_obs(25, 60)], D, CFG)
    assert row.observation_count_7d == 0 and row.price_7d_median is None
    assert row.recommended_market_value == Decimal("50.00")
    assert row.policy["basis_window"] == "30d"


# ---------- confidence ----------
def test_confidence_perfect_and_thin_evidence():
    perfect = compute_confidence(
        observation_count=10,
        age_days=0,
        dispersion=0.0,
        source_agreement=1.0,
        coverage_days=7,
        config=CFG,
    )
    assert perfect.score == 100.0

    thin = compute_confidence(
        observation_count=1,
        age_days=0,
        dispersion=None,
        source_agreement=None,
        coverage_days=1,
        config=CFG,
    )
    assert thin.score == pytest.approx(47.14, abs=0.01)
    parts = thin.components
    assert (
        parts["dispersion"]["score"] == 0.5
        and "not assessable" in parts["dispersion"]["explanation"]
    )
    assert parts["source_agreement"]["score"] == 0.5
    assert sum(p["weighted"] for k, p in parts.items() if k != "total") * 100 == pytest.approx(
        thin.score, abs=0.05
    )
    assert all(p["explanation"] for k, p in parts.items() if k != "total")


def test_confidence_weights_sum_to_one():
    total = (
        CFG.weight_count
        + CFG.weight_recency
        + CFG.weight_dispersion
        + CFG.weight_agreement
        + CFG.weight_completeness
    )
    assert total == pytest.approx(1.0)


def test_confidence_recency_and_dispersion_components():
    stale = compute_confidence(
        observation_count=10, age_days=7, dispersion=0.0, source_agreement=1.0, coverage_days=7,
        config=CFG,
    )  # fmt: skip
    assert stale.components["recency"]["score"] == 0.0
    assert stale.score == pytest.approx(75.0)
    noisy = compute_confidence(
        observation_count=10, age_days=0, dispersion=0.25, source_agreement=1.0, coverage_days=7,
        config=CFG,
    )  # fmt: skip
    assert noisy.components["dispersion"]["score"] == 0.5


# ---------- buyback policy ----------
def test_buyback_tiers_follow_confidence_and_liquidity():
    high = pricing_recommendations(Decimal(100), 90.0, "completed_sale", 12, CFG)
    assert (high.liquidity_tier, high.buyback_tier) == ("high", "high")
    assert high.buyback_price == Decimal("85.00")
    assert high.retail_price == Decimal("110.00") and high.buy_price == Decimal("70.00")

    medium = pricing_recommendations(Decimal(100), 60.0, "completed_sale", 5, CFG)
    assert (medium.liquidity_tier, medium.buyback_tier) == ("medium", "medium")
    assert medium.buyback_price == Decimal("70.00")

    low_confidence = pricing_recommendations(Decimal(100), 40.0, "completed_sale", 12, CFG)
    assert low_confidence.buyback_tier == "low" and low_confidence.buyback_price == Decimal("50.00")


def test_unknown_liquidity_is_conservative_and_configurable():
    rec = pricing_recommendations(Decimal(100), 95.0, "provider_aggregate", 30, CFG)
    assert rec.liquidity_tier == "unknown" and rec.effective_liquidity == "low"
    assert rec.buyback_tier == "low" and rec.buyback_price == Decimal("50.00")

    relaxed = replace(CFG, unknown_liquidity="medium")
    rec = pricing_recommendations(Decimal(100), 95.0, "provider_aggregate", 30, relaxed)
    assert rec.liquidity_tier == "unknown" and rec.buyback_tier == "medium"
    assert rec.buyback_price == Decimal("70.00")


def test_buyback_is_never_negative_and_rounds_half_up():
    extreme = replace(CFG, buyback_discount_low=Decimal("1.5"))
    assert pricing_recommendations(Decimal(100), 10.0, "unknown", 1, extreme).buyback_price == 0
    assert pricing_recommendations(Decimal("33.333"), 10.0, "unknown", 1, CFG).buyback_price == (
        Decimal("16.67")
    )


def test_liquidity_comes_only_from_transaction_evidence():
    assert liquidity_tier("completed_sale", 10, CFG) == "high"
    assert liquidity_tier("auction_result", 3, CFG) == "medium"
    assert liquidity_tier("completed_sale", 1, CFG) == "low"
    for obs_type in ("provider_aggregate", "asking_price", "unknown"):
        assert liquidity_tier(obs_type, 500, CFG) == "unknown"
