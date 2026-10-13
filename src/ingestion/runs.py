"""Pipeline run tracking (data_pipeline_runs). Each call uses its own transaction."""

import re
from collections.abc import Iterable
from typing import Any

from psycopg.types.json import Jsonb

from src.db.connection import transaction

_URL_CREDENTIALS = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s@]+@")
_KEY_VALUE_SECRET = re.compile(
    r"(?i)\b(password|passwd|pwd|api[_-]?key|token|secret)\b(\s*[=:]\s*)\S+"
)


def sanitize_error(
    exc: BaseException, secrets: Iterable[str | None] = (), max_len: int = 1000
) -> str:
    """Error text that is safe to store/log: no URL credentials, passwords or API keys."""
    text = f"{type(exc).__name__}: {exc}"
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    text = _URL_CREDENTIALS.sub(r"\1***@", text)
    text = _KEY_VALUE_SECRET.sub(r"\1\2***", text)
    text = " ".join(text.split())
    return text[:max_len]


def start_run(pipeline_name: str, metadata: dict[str, Any], dsn: str | None = None) -> int:
    with transaction(dsn) as conn:
        row = conn.execute(
            "INSERT INTO data_pipeline_runs (pipeline_name, status, metadata) "
            "VALUES (%s, 'running', %s) RETURNING run_id",
            (pipeline_name, Jsonb(metadata)),
        ).fetchone()
    return row[0]


def finish_run(
    run_id: int,
    status: str,
    rows_read: int,
    rows_written: int,
    metadata: dict[str, Any],
    error_message: str | None = None,
    dsn: str | None = None,
) -> None:
    with transaction(dsn) as conn:
        conn.execute(
            "UPDATE data_pipeline_runs "
            "SET status = %s, finished_at = now(), rows_read = %s, rows_written = %s, "
            "    error_message = %s, metadata = metadata || %s "
            "WHERE run_id = %s",
            (status, rows_read, rows_written, error_message, Jsonb(metadata), run_id),
        )
