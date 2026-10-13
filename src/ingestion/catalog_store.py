"""SQL for persisting the Pokemon card catalog: raw (append-only) -> staging -> dimension."""

import hashlib
import json
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime
from typing import Any, Literal

import psycopg
from psycopg.types.json import Jsonb

from src.ingestion.client import CatalogPage
from src.ingestion.models import Card

SOURCE = "pokemontcg"


# ---------- hashing / parsing ----------
def canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_payload_hash(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def parse_release_date(value: str | None) -> date | None:
    """The API uses 'YYYY/MM/DD'. Anything unparseable becomes NULL instead of failing."""
    if not value:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


# ---------- stats ----------
@dataclass
class CatalogStats:
    rows_read: int = 0
    raw_inserted: int = 0
    raw_reused: int = 0
    staging_inserted: int = 0
    staging_updated: int = 0
    staging_unchanged: int = 0
    dimension_inserted: int = 0
    dimension_updated: int = 0
    dimension_unchanged: int = 0

    def add(self, other: "CatalogStats") -> None:
        for field in fields(self):
            setattr(self, field.name, getattr(self, field.name) + getattr(other, field.name))

    @property
    def rows_written(self) -> int:
        """Rows actually inserted or updated across raw, staging and dimension."""
        return (
            self.raw_inserted
            + self.staging_inserted
            + self.staging_updated
            + self.dimension_inserted
            + self.dimension_updated
        )

    def as_metadata(self) -> dict[str, int]:
        data = asdict(self)
        data.pop("rows_read")
        return data


# ---------- SQL ----------
_CATALOG_COLUMNS = (
    "name",
    "supertype",
    "subtypes",
    "set_id",
    "set_name",
    "series",
    "card_number",
    "rarity",
    "artist",
    "release_date",
    "image_small_url",
    "image_large_url",
)
_STG_COLUMNS = (*_CATALOG_COLUMNS, "raw_id")


def _placeholder(column: str) -> str:
    return f"%({column})s::text[]" if column == "subtypes" else f"%({column})s"


_stg_cols = ", ".join(_STG_COLUMNS)
_stg_values = ", ".join(_placeholder(c) for c in _STG_COLUMNS)
_stg_sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in _STG_COLUMNS)
_stg_old = ", ".join(f"stg_cards.{c}" for c in _STG_COLUMNS)
_stg_new = ", ".join(f"EXCLUDED.{c}" for c in _STG_COLUMNS)

_dim_cols = ", ".join(_CATALOG_COLUMNS)
_dim_sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in _CATALOG_COLUMNS)
_dim_old = ", ".join(f"dim_cards.{c}" for c in _CATALOG_COLUMNS)
_dim_new = ", ".join(f"EXCLUDED.{c}" for c in _CATALOG_COLUMNS)

_RAW_SQL = """
WITH ins AS (
    INSERT INTO raw_card_catalog (source, source_card_id, payload, payload_hash, run_id)
    VALUES (%(source)s, %(card_id)s, %(payload)s, %(hash)s, %(run_id)s)
    ON CONFLICT (source, source_card_id, payload_hash) DO NOTHING
    RETURNING id
)
SELECT id, TRUE AS inserted FROM ins
UNION ALL
SELECT r.id, FALSE FROM raw_card_catalog r
WHERE r.source = %(source)s AND r.source_card_id = %(card_id)s
  AND r.payload_hash = %(hash)s AND NOT EXISTS (SELECT 1 FROM ins)
"""

_RAW_LOOKUP_SQL = """
SELECT id FROM raw_card_catalog
WHERE source = %(source)s AND source_card_id = %(card_id)s AND payload_hash = %(hash)s
"""

_STG_SQL = f"""
INSERT INTO stg_cards (source, source_card_id, {_stg_cols})
VALUES (%(source)s, %(source_card_id)s, {_stg_values})
ON CONFLICT (source, source_card_id) DO UPDATE SET
    {_stg_sets},
    loaded_at = now()
WHERE ({_stg_old}) IS DISTINCT FROM ({_stg_new})
RETURNING (xmax = 0) AS inserted
"""

_DIM_SQL = f"""
INSERT INTO dim_cards (card_id, {_dim_cols})
SELECT source_card_id, {_dim_cols}
FROM stg_cards
WHERE source = %(source)s AND source_card_id = ANY(%(ids)s::text[])
ORDER BY source_card_id
ON CONFLICT (card_id) DO UPDATE SET
    {_dim_sets},
    updated_at = now()
WHERE ({_dim_old}) IS DISTINCT FROM ({_dim_new})
RETURNING (xmax = 0) AS inserted
"""


# ---------- operations ----------
def insert_raw(
    conn: psycopg.Connection, card_id: str, raw_card: dict[str, Any], run_id: int
) -> tuple[int, bool]:
    """Insert one raw version. Returns (raw id, inserted?). Existing rows are never touched."""
    params = {
        "source": SOURCE,
        "card_id": card_id,
        "payload": Jsonb(raw_card),
        "hash": compute_payload_hash(raw_card),
        "run_id": run_id,
    }
    row = conn.execute(_RAW_SQL, params).fetchone()
    if row is None:  # a concurrent writer committed the same version after our snapshot
        lookup = {k: params[k] for k in ("source", "card_id", "hash")}
        existing = conn.execute(_RAW_LOOKUP_SQL, lookup).fetchone()
        return existing[0], False
    return row[0], bool(row[1])


def _staging_params(card: Card, raw_id: int) -> dict[str, Any]:
    card_set = card.card_set
    images = card.images
    return {
        "source": SOURCE,
        "source_card_id": card.id,
        "name": card.name,
        "supertype": card.supertype,
        "subtypes": list(card.subtypes),
        "set_id": card_set.id if card_set else None,
        "set_name": card_set.name if card_set else None,
        "series": card_set.series if card_set else None,
        "card_number": card.number,
        "rarity": card.rarity,
        "artist": card.artist,
        "release_date": parse_release_date(card_set.release_date if card_set else None),
        "image_small_url": images.small if images else None,
        "image_large_url": images.large if images else None,
        "raw_id": raw_id,
    }


def upsert_staging(
    conn: psycopg.Connection, card: Card, raw_id: int
) -> Literal["inserted", "updated", "unchanged"]:
    row = conn.execute(_STG_SQL, _staging_params(card, raw_id)).fetchone()
    if row is None:
        return "unchanged"
    return "inserted" if row[0] else "updated"


def upsert_dimension(conn: psycopg.Connection, card_ids: list[str]) -> tuple[int, int, int]:
    """Upsert dim_cards from staging. Returns (inserted, updated, unchanged)."""
    if not card_ids:
        return 0, 0, 0
    rows = conn.execute(_DIM_SQL, {"source": SOURCE, "ids": card_ids}).fetchall()
    inserted = sum(1 for r in rows if r[0])
    updated = len(rows) - inserted
    return inserted, updated, len(set(card_ids)) - len(rows)


def persist_page(conn: psycopg.Connection, page: CatalogPage, run_id: int) -> CatalogStats:
    """Write one fetched page (raw -> staging -> dimension). Caller owns the transaction."""
    stats = CatalogStats(rows_read=len(page.cards))
    card_ids: list[str] = []
    for card, raw_card in zip(page.cards, page.raw_cards, strict=True):
        raw_id, inserted = insert_raw(conn, card.id, raw_card, run_id)
        if inserted:
            stats.raw_inserted += 1
        else:
            stats.raw_reused += 1
        outcome = upsert_staging(conn, card, raw_id)
        setattr(stats, f"staging_{outcome}", getattr(stats, f"staging_{outcome}") + 1)
        card_ids.append(card.id)

    dim_inserted, dim_updated, dim_unchanged = upsert_dimension(conn, card_ids)
    stats.dimension_inserted = dim_inserted
    stats.dimension_updated = dim_updated
    stats.dimension_unchanged = dim_unchanged
    return stats
