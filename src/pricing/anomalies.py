"""Anomaly detection for price observations. Writes fct_price_anomalies (+ alerts).

Rules: duplicate_observation, missing_price, non_positive_price, statistical_outlier,
price_spike / price_drop, stale_data, conflicting_sources, missing_match,
insufficient_observations.

Reruns are safe: every finding has a deterministic dedupe_key (type, as-of date and the
identity of what was flagged) and is upserted. Alerts are created only for NEWLY inserted
error/critical findings, so a rerun never duplicates them. A finding that is no longer
detected is NOT auto-resolved (set resolved_at manually).
"""

import argparse
import logging
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from src.config import Settings, get_settings
from src.db.connection import describe_dsn, transaction
from src.ingestion.catalog import _secrets
from src.ingestion.runs import finish_run, sanitize_error, start_run
from src.log import configure_logging
from src.pricing.data import (
    PriceObservation,
    PricingConfig,
    PricingStats,
    add_common_arguments,
    chunked,
    is_headline,
    jsonable,
    list_card_keys,
    load_grade_map,
    load_observations,
    record_failure,
    resolve_as_of,
    window_bounds,
)
from src.pricing.market_value import median

logger = logging.getLogger(__name__)

PIPELINE_NAME = "anomaly_detection"
ALERT_SEVERITIES = frozenset({"error", "critical"})
PROBLEM_WINDOW_DAYS = 29  # row-level rules look at observations of the last 30 days


@dataclass(frozen=True)
class AnomalyFinding:
    anomaly_type: str
    severity: str
    as_of_date: date
    card_key: int | None
    stg_observation_id: int | None
    source: str | None
    observation_type: str | None
    observation_key: str | None
    currency: str | None
    expected_price: Decimal | None
    observed_price: Decimal | None
    details: dict[str, Any]
    dedupe_key: str


def make_finding(
    anomaly_type: str,
    severity: str,
    as_of_date: date,
    *,
    card_key: int | None = None,
    grade_id: int | None = None,
    stg_observation_id: int | None = None,
    source: str | None = None,
    observation_type: str | None = None,
    observation_key: str | None = None,
    currency: str | None = None,
    expected_price: Decimal | None = None,
    observed_price: Decimal | None = None,
    details: dict[str, Any] | None = None,
) -> AnomalyFinding:
    parts = [
        anomaly_type,
        as_of_date.isoformat(),
        card_key,
        grade_id,
        source,
        observation_type,
        currency,
        observation_key,
        stg_observation_id,
    ]
    dedupe_key = "|".join("-" if p is None else str(p) for p in parts)
    merged = {
        "as_of_date": as_of_date.isoformat(),
        "grade_id": grade_id,
        **{k: jsonable(v) for k, v in (details or {}).items()},
    }
    return AnomalyFinding(
        anomaly_type=anomaly_type,
        severity=severity,
        as_of_date=as_of_date,
        card_key=card_key,
        stg_observation_id=stg_observation_id,
        source=source,
        observation_type=observation_type,
        observation_key=observation_key,
        currency=currency,
        expected_price=expected_price,
        observed_price=observed_price,
        details=merged,
        dedupe_key=dedupe_key,
    )


# ---------- series-level rules (matched observations) ----------
def _series_fields(series: tuple[int, int, str, str, str]) -> dict[str, Any]:
    card_key, grade_id, obs_type, currency, obs_key = series
    return {
        "card_key": card_key,
        "grade_id": grade_id,
        "observation_type": obs_type,
        "currency": currency,
        "observation_key": obs_key,
    }


def _prices_between(
    obs: Sequence[PriceObservation], as_of: date, start_back: int, end_back: int
) -> list[Decimal]:
    low = as_of - timedelta(days=start_back)
    high = as_of - timedelta(days=end_back)
    return [o.price for o in obs if low <= o.price_date <= high]


def _stale(series, obs, as_of, config) -> list[AnomalyFinding]:
    latest = max(obs, key=lambda o: (o.observed_at, o.stg_id))
    age = (as_of - latest.price_date).days
    if not config.stale_days < age <= config.stale_lookback_days:
        return []
    severity = "error" if age >= 3 * config.stale_days else "warning"
    return [
        make_finding(
            "stale_data",
            severity,
            as_of,
            source=latest.source,
            observed_price=latest.price,
            details={
                "latest_observed_at": latest.observed_at,
                "age_days": age,
                "stale_after_days": config.stale_days,
                "latest_stg_observation_id": latest.stg_id,
            },
            **_series_fields(series),
        )
    ]


def _insufficient(series, obs, as_of, config) -> list[AnomalyFinding]:
    if not is_headline(config, series[4]):
        return []
    count = len(_prices_between(obs, as_of, 29, 0))
    if not 0 < count < config.min_observations:
        return []
    return [
        make_finding(
            "insufficient_observations",
            "info",
            as_of,
            details={"observations_30d": count, "minimum": config.min_observations},
            **_series_fields(series),
        )
    ]


def _movement(series, obs, as_of, config) -> list[AnomalyFinding]:
    findings: list[AnomalyFinding] = []
    threshold = Decimal(str(config.spike_pct))
    for today in (o for o in obs if o.price_date == as_of):
        base7 = _prices_between(obs, as_of, 7, 1)
        base30 = _prices_between(obs, as_of, 30, 1)
        med7 = median(base7)
        flagged = False
        if med7 is not None and med7 > 0:
            pct = (today.price - med7) / med7 * 100
            if abs(pct) >= threshold:
                flagged = True
                kind = "price_spike" if pct > 0 else "price_drop"
                severity = "error" if abs(pct) >= 2 * threshold else "warning"
                findings.append(
                    make_finding(
                        kind,
                        severity,
                        as_of,
                        source=today.source,
                        stg_observation_id=today.stg_id,
                        expected_price=med7,
                        observed_price=today.price,
                        details={
                            "change_pct": round(pct, 2),
                            "baseline": "median of the previous 7 days",
                            "baseline_observations": len(base7),
                            "threshold_pct": config.spike_pct,
                        },
                        **_series_fields(series),
                    )
                )
        if flagged or len(base30) < config.outlier_min_baseline:
            continue
        med = median(base30)
        mad = median([abs(b - med) for b in base30])
        if mad is None or mad <= 0:
            continue
        z = 0.6745 * float(today.price - med) / float(mad)
        if abs(z) > config.outlier_z:
            severity = "error" if abs(z) >= 2 * config.outlier_z else "warning"
            findings.append(
                make_finding(
                    "statistical_outlier",
                    severity,
                    as_of,
                    source=today.source,
                    stg_observation_id=today.stg_id,
                    expected_price=med,
                    observed_price=today.price,
                    details={
                        "modified_z": round(z, 2),
                        "mad": mad,
                        "baseline_observations": len(base30),
                        "threshold_z": config.outlier_z,
                    },
                    **_series_fields(series),
                )
            )
    return findings


def _conflict(series, obs, as_of, config) -> list[AnomalyFinding]:
    low = as_of - timedelta(days=6)
    by_source: dict[str, list[Decimal]] = defaultdict(list)
    for o in obs:
        if low <= o.price_date <= as_of:
            by_source[o.source].append(o.price)
    if len(by_source) < 2:
        return []
    medians = {src: median(prices) for src, prices in by_source.items()}
    lowest, highest = min(medians.values()), max(medians.values())
    if lowest <= 0:
        return []
    spread = (highest - lowest) / lowest * 100
    if spread < Decimal(str(config.conflict_pct)):
        return []
    return [
        make_finding(
            "conflicting_sources",
            "warning",
            as_of,
            expected_price=median(list(medians.values())),
            observed_price=highest,
            details={
                "spread_pct": round(spread, 2),
                "source_medians": {k: str(v) for k, v in sorted(medians.items())},
                "threshold_pct": config.conflict_pct,
            },
            **_series_fields(series),
        )
    ]


def detect_series_anomalies(
    observations: Sequence[PriceObservation], as_of_date: date, config: PricingConfig
) -> list[AnomalyFinding]:
    series_map: dict[tuple, list[PriceObservation]] = defaultdict(list)
    for obs in observations:
        if obs.price_date <= as_of_date:
            series_map[obs.series].append(obs)
    findings: list[AnomalyFinding] = []
    for series in sorted(series_map):
        obs = sorted(series_map[series], key=lambda o: (o.observed_at, o.stg_id))
        findings += _stale(series, obs, as_of_date, config)
        findings += _insufficient(series, obs, as_of_date, config)
        findings += _movement(series, obs, as_of_date, config)
        findings += _conflict(series, obs, as_of_date, config)
    return findings


# ---------- row-level rules (any observation, matched or not) ----------
@dataclass(frozen=True)
class PriceProblemRow:
    stg_id: int
    card_key: int | None
    source: str
    observation_type: str
    currency: str
    observation_key: str
    price: Decimal | None
    observed_at: datetime


@dataclass(frozen=True)
class DuplicateGroup:
    first_id: int
    ids: list[int]
    card_key: int | None
    source: str
    observation_type: str
    currency: str
    observation_key: str
    price: Decimal
    observed_at: datetime


@dataclass(frozen=True)
class UnmatchedRow:
    stg_id: int
    source: str
    observation_type: str
    currency: str
    observation_key: str
    observed_at: datetime
    match_status: str | None  # None = no match row at all


def price_problem_findings(rows: Sequence[PriceProblemRow], as_of: date) -> list[AnomalyFinding]:
    findings = []
    for row in rows:
        kind = "missing_price" if row.price is None else "non_positive_price"
        findings.append(
            make_finding(
                kind,
                "error",
                as_of,
                card_key=row.card_key,
                stg_observation_id=row.stg_id,
                source=row.source,
                observation_type=row.observation_type,
                observation_key=row.observation_key,
                currency=row.currency,
                observed_price=row.price,
                details={"observed_at": row.observed_at},
            )
        )
    return findings


def duplicate_findings(groups: Sequence[DuplicateGroup], as_of: date) -> list[AnomalyFinding]:
    return [
        make_finding(
            "duplicate_observation",
            "info",
            as_of,
            card_key=g.card_key,
            stg_observation_id=g.first_id,
            source=g.source,
            observation_type=g.observation_type,
            observation_key=g.observation_key,
            currency=g.currency,
            observed_price=g.price,
            details={"observation_ids": g.ids, "copies": len(g.ids), "observed_at": g.observed_at},
        )
        for g in groups
    ]


def missing_match_findings(rows: Sequence[UnmatchedRow], as_of: date) -> list[AnomalyFinding]:
    return [
        make_finding(
            "missing_match",
            "warning" if row.match_status is None else "info",
            as_of,
            stg_observation_id=row.stg_id,
            source=row.source,
            observation_type=row.observation_type,
            observation_key=row.observation_key,
            currency=row.currency,
            details={"match_status": row.match_status, "observed_at": row.observed_at},
        )
        for row in rows
    ]


_ROW_FILTERS = """
  AND (%(source)s::text IS NULL OR s.source = %(source)s)
  AND (%(observation_type)s::text IS NULL OR s.observation_type = %(observation_type)s)
"""

_PROBLEMS_SQL = f"""
SELECT s.id, m.card_key, s.source, s.observation_type, s.currency, s.observation_key,
       s.price, s.observed_at
FROM stg_price_observations s
LEFT JOIN fct_card_matches m ON m.stg_observation_id = s.id
WHERE s.observed_at >= %(start)s AND s.observed_at < %(end)s
  AND (s.price IS NULL OR s.price <= 0)
  AND (%(card_key)s::bigint IS NULL OR m.card_key = %(card_key)s)
  {_ROW_FILTERS}
ORDER BY s.id
"""

_DUPLICATES_SQL = f"""
SELECT min(s.id), array_agg(s.id ORDER BY s.id), (array_agg(m.card_key ORDER BY s.id))[1],
       s.source, s.observation_type, s.currency, s.observation_key, s.price, s.observed_at
FROM stg_price_observations s
LEFT JOIN fct_card_matches m ON m.stg_observation_id = s.id
WHERE s.observed_at >= %(start)s AND s.observed_at < %(end)s
  AND s.price IS NOT NULL
  AND (%(card_key)s::bigint IS NULL OR m.card_key = %(card_key)s)
  {_ROW_FILTERS}
GROUP BY s.source, s.source_card_ref, s.observation_type, s.currency, s.observation_key,
         s.price, s.observed_at
HAVING count(*) > 1
ORDER BY min(s.id)
"""

_UNMATCHED_SQL = f"""
SELECT s.id, s.source, s.observation_type, s.currency, s.observation_key, s.observed_at,
       m.status
FROM stg_price_observations s
LEFT JOIN fct_card_matches m ON m.stg_observation_id = s.id
WHERE s.observed_at >= %(start)s AND s.observed_at < %(end)s
  AND (m.stg_observation_id IS NULL OR m.card_key IS NULL
       OR m.status NOT IN ('auto', 'confirmed'))
  {_ROW_FILTERS}
ORDER BY s.id
"""


def _row_params(start, end, card_key, source, observation_type) -> dict[str, Any]:
    return {
        "start": start,
        "end": end,
        "card_key": card_key,
        "source": source,
        "observation_type": observation_type,
    }


def detect_row_anomalies(
    conn: psycopg.Connection,
    as_of: date,
    *,
    card_key: int | None,
    source: str | None,
    observation_type: str | None,
) -> tuple[list[AnomalyFinding], int]:
    """Returns (findings, rows examined). missing_match is skipped when filtering by card."""
    start, end = window_bounds(as_of, PROBLEM_WINDOW_DAYS)
    params = _row_params(start, end, card_key, source, observation_type)
    problems = [PriceProblemRow(*r) for r in conn.execute(_PROBLEMS_SQL, params).fetchall()]
    groups = [DuplicateGroup(*r) for r in conn.execute(_DUPLICATES_SQL, params).fetchall()]
    findings = price_problem_findings(problems, as_of) + duplicate_findings(groups, as_of)
    examined = len(problems) + sum(len(g.ids) for g in groups)
    if card_key is None:
        unmatched_params = {k: v for k, v in params.items() if k != "card_key"}
        unmatched = [
            UnmatchedRow(*r) for r in conn.execute(_UNMATCHED_SQL, unmatched_params).fetchall()
        ]
        findings += missing_match_findings(unmatched, as_of)
        examined += len(unmatched)
    return findings, examined


# ---------- persistence ----------
_ANOMALY_SQL = """
INSERT INTO fct_price_anomalies (
    card_key, anomaly_type, severity, expected_price, observed_price, details, as_of_date,
    stg_observation_id, source, observation_type, observation_key, currency, dedupe_key, run_id
)
VALUES (
    %(card_key)s, %(anomaly_type)s, %(severity)s, %(expected_price)s, %(observed_price)s,
    %(details)s, %(as_of_date)s, %(stg_observation_id)s, %(source)s, %(observation_type)s,
    %(observation_key)s, %(currency)s, %(dedupe_key)s, %(run_id)s
)
ON CONFLICT (dedupe_key) DO UPDATE SET
    severity = EXCLUDED.severity,
    expected_price = EXCLUDED.expected_price,
    observed_price = EXCLUDED.observed_price,
    details = EXCLUDED.details,
    run_id = EXCLUDED.run_id
WHERE (fct_price_anomalies.severity, fct_price_anomalies.expected_price,
       fct_price_anomalies.observed_price, fct_price_anomalies.details)
      IS DISTINCT FROM
      (EXCLUDED.severity, EXCLUDED.expected_price, EXCLUDED.observed_price, EXCLUDED.details)
RETURNING anomaly_id, (xmax = 0) AS inserted
"""

_ALERT_SQL = """
INSERT INTO data_quality_alerts (run_id, check_name, severity, table_name, message, details)
VALUES (%(run_id)s, %(check_name)s, %(severity)s, 'fct_price_anomalies', %(message)s, %(details)s)
"""


def write_findings(
    conn: psycopg.Connection, findings: Sequence[AnomalyFinding], run_id: int | None
) -> tuple[int, int, int, int]:
    """Upsert findings. Returns (inserted, updated, unchanged, alerts_created)."""
    inserted = updated = unchanged = alerts = 0
    for f in findings:
        row = conn.execute(
            _ANOMALY_SQL,
            {
                "card_key": f.card_key,
                "anomaly_type": f.anomaly_type,
                "severity": f.severity,
                "expected_price": f.expected_price,
                "observed_price": f.observed_price,
                "details": Jsonb(f.details),
                "as_of_date": f.as_of_date,
                "stg_observation_id": f.stg_observation_id,
                "source": f.source,
                "observation_type": f.observation_type,
                "observation_key": f.observation_key,
                "currency": f.currency,
                "dedupe_key": f.dedupe_key,
                "run_id": run_id,
            },
        ).fetchone()
        if row is None:
            unchanged += 1
            continue
        anomaly_id, was_inserted = row
        if not was_inserted:
            updated += 1
            continue
        inserted += 1
        if f.severity in ALERT_SEVERITIES:
            conn.execute(
                _ALERT_SQL,
                {
                    "run_id": run_id,
                    "check_name": f.anomaly_type,
                    "severity": f.severity,
                    "message": f"{f.anomaly_type} detected for as-of {f.as_of_date}",
                    "details": Jsonb(
                        {
                            "anomaly_id": anomaly_id,
                            "dedupe_key": f.dedupe_key,
                            "card_key": f.card_key,
                            "observation_key": f.observation_key,
                        }
                    ),
                },
            )
            alerts += 1
    return inserted, updated, unchanged, alerts


# ---------- run ----------
@dataclass
class AnomalyResult:
    run_id: int | None
    stats: PricingStats
    dry_run: bool
    as_of_date: date
    by_type: dict[str, int]


def detect_anomalies(
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
) -> AnomalyResult:
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
    by_type: dict[str, int] = defaultdict(int)

    def absorb(findings: Sequence[AnomalyFinding], counts: tuple[int, int, int, int]) -> None:
        stats.anomalies_detected += len(findings)
        stats.anomaly_rows_written += counts[0] + counts[1]
        stats.rows_unchanged += counts[2]
        stats.alerts_created += counts[3]
        for f in findings:
            by_type[f.anomaly_type] += 1

    start, end = window_bounds(as_of_date, config.stale_lookback_days)
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
                findings = detect_series_anomalies(observations, as_of_date, config)
                counts = (0, 0, 0, 0) if dry_run else write_findings(conn, findings, run_id)
            stats.cards_processed += len(chunk)
            stats.observations_processed += len(observations)
            stats.observations_skipped += skipped
            absorb(findings, counts)

        with transaction(dsn, read_only=dry_run) as conn:
            findings, examined = detect_row_anomalies(
                conn,
                as_of_date,
                card_key=card_key,
                source=source,
                observation_type=observation_type,
            )
            counts = (0, 0, 0, 0) if dry_run else write_findings(conn, findings, run_id)
        stats.observations_processed += examined
        absorb(findings, counts)
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
            {**stats.as_metadata(), "anomalies_by_type": dict(by_type)},
            dsn=dsn,
        )
    return AnomalyResult(run_id, stats, dry_run, as_of_date, dict(by_type))


# ---------- CLI ----------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Detect price anomalies for an as-of date")
    add_common_arguments(parser)
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("Database: %s", describe_dsn(settings.psycopg_dsn))
    as_of = resolve_as_of(args.as_of_date)

    try:
        result = detect_anomalies(
            settings,
            as_of_date=as_of,
            dry_run=args.dry_run,
            card_key=args.card_key,
            source=args.source,
            observation_type=args.observation_type,
            batch_size=args.batch_size,
        )
    except Exception as exc:
        logger.error("Anomaly detection failed: %s", sanitize_error(exc, _secrets(settings)))
        return 1

    s = result.stats
    logger.info(
        "%s as_of=%s run=%s: cards=%s observations=%s detected=%s written=%s alerts=%s | %s",
        "DRY RUN" if result.dry_run else "Detection succeeded",
        result.as_of_date,
        result.run_id,
        s.cards_processed,
        s.observations_processed,
        s.anomalies_detected,
        s.anomaly_rows_written,
        s.alerts_created,
        result.by_type,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
