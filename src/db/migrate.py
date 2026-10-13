"""Apply src/db/schema.sql to the configured database (idempotent bootstrap)."""

import logging
from pathlib import Path

import psycopg

from src.config import get_settings
from src.log import configure_logging

logger = logging.getLogger(__name__)
SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def apply_schema(dsn: str) -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with psycopg.connect(dsn) as conn:  # commits on clean exit
        conn.execute(sql)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("Applying schema from %s", SCHEMA_PATH)
    apply_schema(settings.psycopg_dsn)
    logger.info("Schema applied")


if __name__ == "__main__":
    main()
