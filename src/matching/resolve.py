"""CLI: python -m src.matching.resolve --limit 1000 [--source S] [--dry-run] [--reprocess]"""

import argparse
import logging

from src.config import get_settings
from src.db.connection import describe_dsn
from src.ingestion.catalog import _non_empty, _non_negative_int, _secrets
from src.ingestion.runs import sanitize_error
from src.log import configure_logging
from src.matching.entity_resolution import resolve_observations

logger = logging.getLogger(__name__)


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        import argparse as _argparse

        raise _argparse.ArgumentTypeError(f"not an integer: {value!r}") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return number


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Match staged price observations to dim_cards")
    parser.add_argument(
        "--limit", type=_non_negative_int, default=1000, help="max observations; 0 = all"
    )
    parser.add_argument("--source", type=_non_empty, default=None, help="only this source")
    parser.add_argument(
        "--dry-run", action="store_true", help="decide but write nothing (read-only transaction)"
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help="also reconsider observations currently in review status (never auto/confirmed)",
    )
    parser.add_argument("--batch-size", type=_positive_int, default=500, help="rows per commit")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("Database: %s", describe_dsn(settings.psycopg_dsn))

    try:
        result = resolve_observations(
            settings,
            limit=args.limit or None,
            source=args.source,
            dry_run=args.dry_run,
            reprocess=args.reprocess,
            batch_size=args.batch_size,
        )
    except Exception as exc:
        logger.error("Entity resolution failed: %s", sanitize_error(exc, _secrets(settings)))
        return 1

    s = result.stats
    logger.info(
        "%s: run=%s processed=%s deterministic=%s fuzzy=%s review=%s unmatched=%s | "
        "rows +%s/~%s/=%s",
        "DRY RUN" if result.dry_run else "Resolution succeeded",
        result.run_id,
        s.observations_processed,
        s.deterministic_matches,
        s.fuzzy_matches,
        s.review_count,
        s.unmatched_count,
        s.rows_inserted,
        s.rows_updated,
        s.rows_unchanged,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
