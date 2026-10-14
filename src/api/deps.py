"""FastAPI dependencies: one short-lived read-only database connection per request."""

import logging
from collections.abc import Iterator

import psycopg
from psycopg.rows import dict_row

from src.db.connection import transaction

logger = logging.getLogger(__name__)


def get_db() -> Iterator[psycopg.Connection]:
    """Read-only connection returning dict rows. Tests override this dependency."""
    with transaction(read_only=True, connect_timeout=5, row_factory=dict_row) as conn:
        yield conn


def check_database() -> bool:
    """True when the database answers SELECT 1. Never leaks connection details."""
    try:
        with transaction(read_only=True, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
    except Exception:
        logger.warning("Database health check failed")
        return False
    return True
