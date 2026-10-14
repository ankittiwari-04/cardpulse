import itertools
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

from src.pricing.anomalies import (
    DuplicateGroup,
    PriceProblemRow,
    UnmatchedRow,
    detect_series_anomalies,
    duplicate_findings,
    missing_match_findings,
    price_problem_findings,
)
from src.pricing.data import PriceObservation, PricingConfig

D = date(2026, 10, 14)
CFG = PricingConfig()
_ids = itertools.count(1000)


def make_obs(days_ago, price, *, source="pokemontcg", key="tcgplayer/normal/market", card_key=1):
    observed = datetime.combine(D - timedelta(days=days_ago), time.min, tzinfo=UTC)
    return PriceObservation(
        next(_ids), card_key, 1, source, "provider_aggregate", "USD", key,
        Decimal(str(price)), observed,
    )  # fmt: skip


def types(findings):
    return sorted(f.anomaly_type for f in findings)


def detect(observations):
    return detect_series_anomalies(observations, D, CFG)


# ---------- stale data ----------
def test_stale_data_threshold_severity_and_lookback():
    recent = detect([make_obs(3, 10), make_obs(4, 10), make_obs(5, 10)])
    assert "stale_data" not in types(recent)

    stale = detect([make_obs(10, 10), make_obs(11, 10), make_obs(12, 10)])
    (finding,) = [f for f in stale if f.anomaly_type == "stale_data"]
    assert finding.severity == "warning" and finding.details["age_days"] == 10
    assert finding.observed_price == Decimal(10)

    very_stale = detect([make_obs(25, 10), make_obs(26, 10), make_obs(27, 10)])
    assert [f.severity for f in very_stale if f.anomaly_type == "stale_data"] == ["error"]

    dormant = detect([make_obs(120, 10)])
    assert "stale_data" not in types(dormant)  # beyond the 90-day lookback


# ---------- price movement ----------
def test_spike_drop_and_normal_moves():
    base = [make_obs(k, 10) for k in range(5, 0, -1)]
    spike = detect([*base, make_obs(0, 16)])
    assert types(spike) == ["price_spike"]
    assert spike[0].severity == "warning"
    assert (spike[0].expected_price, spike[0].observed_price) == (Decimal(10), Decimal(16))

    huge = detect([*base, make_obs(0, 25)])
    assert [f.severity for f in huge] == ["error"]

    drop = detect([*base, make_obs(0, 4)])
    assert types(drop) == ["price_drop"]

    assert detect([*base, make_obs(0, 10.5)]) == []


def test_no_movement_check_without_a_baseline():
    assert "price_spike" not in types(detect([make_obs(0, 500)]))


def test_statistical_outlier_below_spike_threshold():
    prices = [10, 10.5, 9.5, 10, 10.5, 9.5, 10, 10]  # offsets 8..1
    base = [make_obs(8 - i, p) for i, p in enumerate(prices)]
    findings = detect([*base, make_obs(0, 14)])  # +40%: not a spike, but far outside the MAD
    assert types(findings) == ["statistical_outlier"]
    assert findings[0].details["modified_z"] > CFG.outlier_z


def test_a_spike_is_not_reported_twice_as_an_outlier():
    prices = [10, 10.5, 9.5, 10, 10.5, 9.5, 10, 10]
    base = [make_obs(8 - i, p) for i, p in enumerate(prices)]
    assert types(detect([*base, make_obs(0, 18)])) == ["price_spike"]


# ---------- conflicting sources ----------
def test_conflicting_sources():
    obs = [make_obs(k, 10, source="a") for k in range(3)]
    far = [make_obs(k, 15, source="b") for k in range(3)]
    (finding,) = detect([*obs, *far])
    assert finding.anomaly_type == "conflicting_sources"
    assert finding.details["spread_pct"] == 50.0
    assert finding.details["source_medians"] == {"a": "10", "b": "15"}

    close = [make_obs(k, 11, source="b") for k in range(3)]
    assert detect([*obs, *close]) == []


# ---------- insufficient observations ----------
def test_insufficient_observations_only_for_headline_series():
    assert types(detect([make_obs(0, 10), make_obs(1, 10)])) == ["insufficient_observations"]
    assert detect([make_obs(0, 10), make_obs(1, 10), make_obs(2, 10)]) == []
    assert detect([make_obs(0, 10, key="tcgplayer/normal/low")]) == []


# ---------- identity of findings ----------
def test_dedupe_keys_are_stable_and_distinct():
    obs = [make_obs(k, 10) for k in range(5, 0, -1)]
    first = detect([*obs, make_obs(0, 16)])
    second = detect([*obs, make_obs(0, 16)])
    assert [f.dedupe_key for f in first] == [f.dedupe_key for f in second]
    other_series = detect([*[make_obs(k, 10, card_key=2) for k in range(5, 0, -1)],
                           make_obs(0, 16, card_key=2)])  # fmt: skip
    assert first[0].dedupe_key != other_series[0].dedupe_key


# ---------- row-level rules ----------
def test_missing_and_non_positive_prices():
    at = datetime(2026, 10, 14, tzinfo=UTC)
    rows = [
        PriceProblemRow(1, 5, "pokemontcg", "provider_aggregate", "USD", "k", None, at),
        PriceProblemRow(2, 5, "pokemontcg", "provider_aggregate", "USD", "k", Decimal(0), at),
        PriceProblemRow(3, 5, "pokemontcg", "provider_aggregate", "USD", "k", Decimal(-1), at),
    ]
    findings = price_problem_findings(rows, D)
    assert types(findings) == ["missing_price", "non_positive_price", "non_positive_price"]
    assert {f.severity for f in findings} == {"error"}
    assert len({f.dedupe_key for f in findings}) == 3


def test_duplicate_observations():
    at = datetime(2026, 10, 14, tzinfo=UTC)
    group = DuplicateGroup(
        7, [7, 9], 5, "pokemontcg", "provider_aggregate", "USD", "k", Decimal(10), at
    )
    (finding,) = duplicate_findings([group], D)
    assert finding.anomaly_type == "duplicate_observation" and finding.severity == "info"
    assert finding.stg_observation_id == 7
    assert finding.details["copies"] == 2 and finding.details["observation_ids"] == [7, 9]


def test_missing_matches():
    at = datetime(2026, 10, 14, tzinfo=UTC)
    rows = [
        UnmatchedRow(1, "shop", "asking_price", "USD", "k", at, None),
        UnmatchedRow(2, "shop", "asking_price", "USD", "k", at, "review"),
    ]
    findings = missing_match_findings(rows, D)
    assert [(f.anomaly_type, f.severity) for f in findings] == [
        ("missing_match", "warning"),
        ("missing_match", "info"),
    ]


def test_dedupe_key_ignores_loader_ids_but_separates_distinct_observations():
    base = [make_obs(k, 10) for k in range(5, 0, -1)]
    first = detect([*base, make_obs(0, 16)])[0]
    second = detect([*base, make_obs(0, 16)])[0]
    assert first.stg_observation_id != second.stg_observation_id  # different synthetic row ids
    assert first.dedupe_key == second.dedupe_key  # same logical anomaly, same key
    assert first.dedupe_key.split("|")[-2:] == ["2026-10-14T00:00:00+00:00", "16"]
    other_price = detect([*base, make_obs(0, 17)])[0]
    assert other_price.dedupe_key != first.dedupe_key  # a genuinely different observation


def test_numeric_details_are_json_numbers_not_strings():
    import json

    base = [make_obs(k, 10) for k in range(5, 0, -1)]
    (spike,) = detect([*base, make_obs(0, 16)])
    assert spike.details["change_pct"] == 60.0
    assert isinstance(spike.details["change_pct"], float)
    json.dumps(spike.details)  # serializable exactly as stored in JSONB
