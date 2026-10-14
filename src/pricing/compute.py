"""CLI: python -m src.pricing.compute --as-of-date YYYY-MM-DD [--dry-run] [filters]"""

import argparse
import logging

from src.config import get_settings
from src.db.connection import describe_dsn
from src.ingestion.catalog import _secrets
from src.ingestion.runs import sanitize_error
from src.log import configure_logging
from src.pricing.data import add_common_arguments, resolve_as_of
from src.pricing.market_value import compute_market_values

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compute daily card prices and rolling market values from matched observations"
    )
    add_common_arguments(parser)
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("Database: %s", describe_dsn(settings.psycopg_dsn))
    as_of = resolve_as_of(args.as_of_date)

    try:
        result = compute_market_values(
            settings,
            as_of_date=as_of,
            dry_run=args.dry_run,
            card_key=args.card_key,
            source=args.source,
            observation_type=args.observation_type,
            batch_size=args.batch_size,
        )
    except Exception as exc:
        logger.error("Market value computation failed: %s", sanitize_error(exc, _secrets(settings)))
        return 1

    s = result.stats
    logger.info(
        "%s as_of=%s run=%s: cards=%s observations=%s skipped=%s | daily computed=%s written=%s | "
        "market_values computed=%s written=%s | unchanged=%s",
        "DRY RUN" if result.dry_run else "Computation succeeded",
        result.as_of_date,
        result.run_id,
        s.cards_processed,
        s.observations_processed,
        s.observations_skipped,
        s.daily_rows_computed,
        s.daily_price_rows_written,
        s.market_value_rows_computed,
        s.market_value_rows_written,
        s.rows_unchanged,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
