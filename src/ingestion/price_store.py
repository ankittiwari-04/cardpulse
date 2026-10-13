"""SQL for persisting price observations: raw (append-only) -> staging, plus quality alerts."""

from dataclasses import asdict, dataclass, fields
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from src.ingestion.catalog_store import compute_payload_hash
from src.ingestion.price_models import NormalizedObservation, QualityIssue


@dataclass
class PriceStats:
    rows_read: int = 0
    accepted: int = 0
    raw_inserted: int = 0
    raw_reused: int = 0
    staging_inserted: int = 0
    staging_unchanged: int = 0
    rejected_count: int = 0
    alert_count: int = 0
    items_without_price_data: int = 0

    def add(self, other: "PriceStats") -> None:
        for field in fields(self):
            setattr(self, field.name, getattr(self, field.name) + getattr(other, field.name))

    @property
    def rows_written(self) -> int:
        """Newly inserted rows only (raw + staging). Alerts are not counted."""
        return self.raw_inserted + self.staging_inserted

    def as_metadata(self) -> dict[str, int]:
        data = asdict(self)
        data.pop("rows_read")
        return data


_RAW_KEY_FILTER = """
    r.source = %(source)s AND r.source_card_ref = %(source_card_ref)s
    AND r.observation_key = %(observation_key)s AND r.observation_type = %(observation_type)s
    AND r.observed_at = %(observed_at)s AND r.price = %(price)s
    AND r.currency = %(currency)s AND r.payload_hash = %(payload_hash)s
"""

_RAW_SQL = f"""
WITH ins AS (
    INSERT INTO raw_price_observation (
        source, source_card_ref, observation_key, observation_type, observed_at,
        price, currency, payload, payload_hash, run_id
    )
    VALUES (
        %(source)s, %(source_card_ref)s, %(observation_key)s, %(observation_type)s,
        %(observed_at)s, %(price)s, %(currency)s, %(payload)s, %(payload_hash)s, %(run_id)s
    )
    ON CONFLICT (
        source, source_card_ref, observation_key, observation_type,
        observed_at, price, currency, payload_hash
    ) DO NOTHING
    RETURNING id
)
SELECT id, TRUE AS inserted FROM ins
UNION ALL
SELECT r.id, FALSE FROM raw_price_observation r
WHERE {_RAW_KEY_FILTER} AND NOT EXISTS (SELECT 1 FROM ins)
"""

_RAW_LOOKUP_SQL = f"SELECT r.id FROM raw_price_observation r WHERE {_RAW_KEY_FILTER}"

_STG_SQL = """
INSERT INTO stg_price_observations (
    source, source_card_ref, raw_card_name, raw_set_name, raw_card_number, raw_grade_label,
    price, currency, observed_at, raw_id, observation_type, observation_key, source_url,
    condition_label, language_label, edition_label, variant_label, grading_company,
    grade_value, is_sold, listing_id
)
VALUES (
    %(source)s, %(source_card_ref)s, %(raw_card_name)s, %(raw_set_name)s, %(raw_card_number)s,
    %(raw_grade_label)s, %(price)s, %(currency)s, %(observed_at)s, %(raw_id)s,
    %(observation_type)s, %(observation_key)s, %(source_url)s, %(condition_label)s,
    %(language_label)s, %(edition_label)s, %(variant_label)s, %(grading_company)s,
    %(grade_value)s, %(is_sold)s, %(listing_id)s
)
ON CONFLICT (raw_id) DO NOTHING
RETURNING id
"""

_ALERT_SQL = """
INSERT INTO data_quality_alerts (run_id, check_name, severity, table_name, message, details)
VALUES (%(run_id)s, %(check_name)s, 'error', 'raw_price_observation', %(message)s, %(details)s)
"""


def insert_raw(
    conn: psycopg.Connection, obs: NormalizedObservation, run_id: int
) -> tuple[int, bool]:
    """Insert one raw observation version. Existing rows are never touched."""
    key = {
        "source": obs.source,
        "source_card_ref": obs.source_card_ref,
        "observation_key": obs.observation_key,
        "observation_type": obs.observation_type,
        "observed_at": obs.observed_at,
        "price": obs.price,
        "currency": obs.currency,
        "payload_hash": compute_payload_hash(obs.payload),
    }
    row = conn.execute(
        _RAW_SQL, {**key, "payload": Jsonb(obs.payload), "run_id": run_id}
    ).fetchone()
    if row is None:  # a concurrent writer committed the same version after our snapshot
        existing = conn.execute(_RAW_LOOKUP_SQL, key).fetchone()
        return existing[0], False
    return row[0], bool(row[1])


def insert_staging(conn: psycopg.Connection, obs: NormalizedObservation, raw_id: int) -> bool:
    """Insert the staging row for a raw row. Returns False if it already existed."""
    params: dict[str, Any] = {
        "source": obs.source,
        "source_card_ref": obs.source_card_ref,
        "raw_card_name": obs.raw_card_name,
        "raw_set_name": obs.raw_set_name,
        "raw_card_number": obs.raw_card_number,
        "raw_grade_label": obs.raw_grade_label,
        "price": obs.price,
        "currency": obs.currency,
        "observed_at": obs.observed_at,
        "raw_id": raw_id,
        "observation_type": obs.observation_type,
        "observation_key": obs.observation_key,
        "source_url": obs.source_url,
        "condition_label": obs.condition_label,
        "language_label": obs.language_label,
        "edition_label": obs.edition_label,
        "variant_label": obs.variant_label,
        "grading_company": obs.grading_company,
        "grade_value": obs.grade_value,
        "is_sold": obs.is_sold,
        "listing_id": obs.listing_id,
    }
    return conn.execute(_STG_SQL, params).fetchone() is not None


def insert_alert(conn: psycopg.Connection, issue: QualityIssue, run_id: int) -> None:
    conn.execute(
        _ALERT_SQL,
        {
            "run_id": run_id,
            "check_name": issue.check_name,
            "message": issue.message,
            "details": Jsonb(issue.details),
        },
    )


def persist_price_page(
    conn: psycopg.Connection,
    observations: list[NormalizedObservation],
    issues: list[QualityIssue],
    run_id: int,
) -> PriceStats:
    """Write one page (raw -> staging -> alerts). The caller owns the transaction."""
    stats = PriceStats()
    for obs in observations:
        raw_id, inserted = insert_raw(conn, obs, run_id)
        if inserted:
            stats.raw_inserted += 1
        else:
            stats.raw_reused += 1
        if insert_staging(conn, obs, raw_id):
            stats.staging_inserted += 1
        else:
            stats.staging_unchanged += 1
    for issue in issues:
        insert_alert(conn, issue, run_id)
        stats.alert_count += 1
    return stats
