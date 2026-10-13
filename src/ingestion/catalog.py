"""Catalog ingestion: Pokemon TCG API -> raw -> staging -> dimension, tracked per run.

Usage: python -m src.ingestion.catalog --max-pages 1 --query 'set.id:base1'

Each page is fetched first, then written in ONE transaction (commit per page).
Reruns are safe: identical payloads are reused, changed payloads add a new raw version.
"""

import argparse
import logging
from dataclasses import dataclass

from src.config import Settings, get_settings
from src.db.connection import describe_dsn, transaction
from src.ingestion.catalog_store import CatalogStats, persist_page
from src.ingestion.client import PokemonTCGClient
from src.ingestion.runs import finish_run, sanitize_error, start_run
from src.log import configure_logging

logger = logging.getLogger(__name__)

PIPELINE_NAME = "catalog_ingest"


@dataclass
class IngestResult:
    run_id: int
    pages: int
    stats: CatalogStats


def _secrets(settings: Settings) -> list[str | None]:
    return [settings.pokemon_tcg_api_key, settings.price_api_key]


def _record_failure(
    run_id: int,
    stats: CatalogStats,
    pages: int,
    exc: BaseException,
    settings: Settings,
    dsn: str,
) -> None:
    """Failure bookkeeping in its own transaction so it survives the page rollback."""
    try:
        finish_run(
            run_id,
            "failed",
            stats.rows_read,
            stats.rows_written,
            {**stats.as_metadata(), "pages_fetched": pages},
            error_message=sanitize_error(exc, _secrets(settings)),
            dsn=dsn,
        )
    except Exception as tracking_exc:  # never mask the original error
        logger.error(
            "Could not record failure for run %s: %s",
            run_id,
            sanitize_error(tracking_exc, _secrets(settings)),
        )


def ingest_catalog(
    settings: Settings | None = None,
    *,
    query: str | None = None,
    max_pages: int | None = None,
    client: PokemonTCGClient | None = None,
    dsn: str | None = None,
) -> IngestResult:
    settings = settings or get_settings()
    dsn = dsn or settings.psycopg_dsn

    run_id = start_run(PIPELINE_NAME, {"query": query, "max_pages": max_pages}, dsn)
    logger.info("Started run %s (%s)", run_id, PIPELINE_NAME)

    stats = CatalogStats()
    pages = 0
    owns_client = client is None
    try:
        if client is None:
            client = PokemonTCGClient(settings)
        for page in client.iter_catalog_pages(query=query, max_pages=max_pages):
            with transaction(dsn) as conn:
                page_stats = persist_page(conn, page, run_id)
            # only counted once the page transaction has committed
            stats.add(page_stats)
            pages += 1
            logger.info("Committed page %s (%s cards)", page.page, len(page.cards))
    except BaseException as exc:
        _record_failure(run_id, stats, pages, exc, settings, dsn)
        raise
    finally:
        if owns_client and client is not None:
            client.close()

    finish_run(
        run_id,
        "succeeded",
        stats.rows_read,
        stats.rows_written,
        {**stats.as_metadata(), "pages_fetched": pages},
        dsn=dsn,
    )
    return IngestResult(run_id=run_id, pages=pages, stats=stats)


def _non_negative_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not an integer: {value!r}") from exc
    if number < 0:
        raise argparse.ArgumentTypeError("must be >= 0 (0 = fetch everything)")
    return number


def _non_empty(value: str) -> str:
    value = value.strip()
    if not value:
        raise argparse.ArgumentTypeError("must not be empty")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and persist the Pokemon TCG card catalog")
    parser.add_argument(
        "--max-pages", type=_non_negative_int, default=1, help="0 = fetch everything (default: 1)"
    )
    parser.add_argument(
        "--query",
        type=_non_empty,
        default=None,
        help="Pokemon TCG API 'q' filter, e.g. set.id:base1",
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("Database: %s", describe_dsn(settings.psycopg_dsn))

    try:
        result = ingest_catalog(settings, query=args.query, max_pages=args.max_pages or None)
    except Exception as exc:
        logger.error("Catalog ingestion failed: %s", sanitize_error(exc, _secrets(settings)))
        return 1

    s = result.stats
    logger.info(
        "Run %s succeeded: pages=%s read=%s written=%s | raw +%s/=%s | "
        "staging +%s/~%s/=%s | dim +%s/~%s/=%s",
        result.run_id,
        result.pages,
        s.rows_read,
        s.rows_written,
        s.raw_inserted,
        s.raw_reused,
        s.staging_inserted,
        s.staging_updated,
        s.staging_unchanged,
        s.dimension_inserted,
        s.dimension_updated,
        s.dimension_unchanged,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
