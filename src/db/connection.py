"""Postgres connection helpers (psycopg 3). Never logs credentials."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict

from src.config import get_settings


def normalize_dsn(url: str) -> str:
    """Strip the SQLAlchemy driver suffix so psycopg accepts the URL."""
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def describe_dsn(dsn: str) -> str:
    """Password-free description, safe to log: user@host:port/dbname."""
    info = conninfo_to_dict(normalize_dsn(dsn))
    return (
        f"{info.get('user', '?')}@{info.get('host', 'localhost')}"
        f":{info.get('port', '5432')}/{info.get('dbname', '?')}"
    )


@contextmanager
def transaction(
    dsn: str | None = None,
    *,
    read_only: bool = False,
    connect_timeout: int | None = None,
    row_factory: Any = None,
) -> Iterator[psycopg.Connection]:
    """One connection = one transaction.

    Commits on clean exit, rolls back on exception, always closes the connection.
    Defaults to the DATABASE_URL from Settings. read_only=True makes the database
    itself reject any write on this connection.
    """
    resolved = normalize_dsn(dsn) if dsn else get_settings().psycopg_dsn
    kwargs: dict[str, Any] = {}
    if connect_timeout is not None:
        kwargs["connect_timeout"] = connect_timeout
    if row_factory is not None:
        kwargs["row_factory"] = row_factory
    with psycopg.connect(resolved, **kwargs) as conn:
        if read_only:
            conn.read_only = True
        yield conn
