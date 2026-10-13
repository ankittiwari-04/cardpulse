"""Price ingestion: provider adapters, orchestration and CLI.

Usage:
    python -m src.ingestion.prices --max-pages 1 [--source pokemontcg] [--query Q] [--dry-run]

Per page: fetch -> validate (pure) -> ONE transaction (raw, staging, alerts) -> commit.
Raw rows are append-only. Rejected observations are never stored; each rejection is
recorded in data_quality_alerts. No matching to canonical cards happens here.
"""

import argparse
import logging
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx
from pydantic import BaseModel, ConfigDict, Field
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential_jitter
from tenacity.wait import wait_base

from src.config import Settings, get_settings
from src.db.connection import describe_dsn, transaction
from src.ingestion.catalog import _non_empty, _non_negative_int, _secrets
from src.ingestion.client import MAX_PAGE_SIZE, _is_retryable
from src.ingestion.price_models import (
    NormalizedObservation,
    PriceCandidate,
    QualityIssue,
    validate_candidate,
)
from src.ingestion.price_store import PriceStats, persist_price_page
from src.ingestion.runs import finish_run, sanitize_error, start_run
from src.log import configure_logging

logger = logging.getLogger(__name__)

PIPELINE_NAME = "price_ingest"


# ---------- provider-agnostic interface ----------
@dataclass(frozen=True)
class PricePage:
    page: int
    total_count: int | None
    candidates: list[PriceCandidate]
    items_without_price_data: int = 0


class PriceSourceClient(Protocol):
    """What a price provider adapter must offer."""

    source_name: str

    def iter_pages(
        self, max_pages: int | None = None, query: str | None = None
    ) -> Iterator[PricePage]: ...

    def close(self) -> None: ...


# ---------- Pokemon TCG API adapter ----------
class _Api(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class _CardSet(_Api):
    id: str | None = None
    name: str | None = None


class _VendorBlock(_Api):
    url: str | None = None
    updated_at: str | None = Field(default=None, alias="updatedAt")
    prices: dict[str, Any] | None = None


class _PriceCard(_Api):
    id: str | None = None  # a missing id is rejected per observation, not by failing the page
    name: str | None = None
    number: str | None = None
    card_set: _CardSet | None = Field(default=None, alias="set")
    tcgplayer: _VendorBlock | None = None
    cardmarket: _VendorBlock | None = None


class PriceCardPage(_Api):
    data: list[_PriceCard] = Field(default_factory=list)
    page: int = 1
    page_size: int = Field(alias="pageSize")
    count: int = 0
    total_count: int = Field(alias="totalCount")


# Documented price fields we turn into observations (all are provider aggregates).
TCGPLAYER_FIELDS = ("market", "low")  # per variant: normal, holofoil, reverseHolofoil, ...
CARDMARKET_FIELDS = ("trendPrice", "averageSellPrice", "lowPrice")
_VENDOR_CURRENCY = {"tcgplayer": "USD", "cardmarket": "EUR"}  # as documented by the provider


def parse_provider_datetime(value: Any) -> datetime | None:
    """Date-only values become 00:00 UTC (one-day precision). Zone-less times are rejected."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


class PokemonTCGPriceClient:
    """Reads the tcgplayer / cardmarket price blocks embedded in api.pokemontcg.io cards."""

    source_name = "pokemontcg"
    DEFAULT_BASE_URL = "https://api.pokemontcg.io/v2"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        page_size: int = MAX_PAGE_SIZE,
        max_attempts: int = 5,
        wait: wait_base | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        settings = settings or get_settings()
        base_url = (settings.price_api_base_url or self.DEFAULT_BASE_URL).rstrip("/") + "/"
        api_key = settings.price_api_key or settings.pokemon_tcg_api_key
        headers = {"Accept": "application/json"}
        if api_key:
            headers["X-Api-Key"] = api_key
        timeout = settings.price_request_timeout_seconds
        self._http = httpx.Client(
            base_url=base_url,
            headers=headers,
            timeout=httpx.Timeout(timeout, connect=min(10.0, timeout)),
            transport=transport,
        )
        self._page_size = min(page_size, MAX_PAGE_SIZE)
        self._retryer = Retrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait or wait_exponential_jitter(initial=1, max=30),
            retry=retry_if_exception(_is_retryable),  # transport errors, 429 and 5xx
            reraise=True,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "PokemonTCGPriceClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _request(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        response = self._http.get(path, params=params)
        response.raise_for_status()
        return response.json()

    def iter_pages(
        self, max_pages: int | None = None, query: str | None = None
    ) -> Iterator[PricePage]:
        page = 1
        while True:
            params: dict[str, Any] = {"page": page, "pageSize": self._page_size}
            if query:
                params["q"] = query
            raw = self._retryer(self._request, "cards", params)
            parsed = PriceCardPage.model_validate(raw)
            candidates, without_prices = self._extract(parsed, raw)
            logger.info(
                "Fetched price page %s (%s cards, %s observations, %s total cards)",
                page,
                len(parsed.data),
                len(candidates),
                parsed.total_count,
            )
            yield PricePage(page, parsed.total_count, candidates, without_prices)
            if not parsed.data or page * parsed.page_size >= parsed.total_count:
                return
            if max_pages is not None and page >= max_pages:
                return
            page += 1

    def _extract(
        self, parsed: PriceCardPage, raw: dict[str, Any]
    ) -> tuple[list[PriceCandidate], int]:
        candidates: list[PriceCandidate] = []
        without_prices = 0
        for card, raw_card in zip(parsed.data, raw.get("data", []), strict=True):
            before = len(candidates)
            for vendor in ("tcgplayer", "cardmarket"):
                block: _VendorBlock | None = getattr(card, vendor)
                raw_block = raw_card.get(vendor)
                if block is None or not isinstance(raw_block, dict):
                    continue
                candidates.extend(self._vendor_candidates(vendor, card, block, raw_block))
            if len(candidates) == before:
                without_prices += 1
        return candidates, without_prices

    def _vendor_candidates(
        self, vendor: str, card: _PriceCard, block: _VendorBlock, raw_block: dict[str, Any]
    ) -> list[PriceCandidate]:
        prices = block.prices or {}

        def build(key: str, price: Any, variant: str | None = None) -> PriceCandidate:
            return PriceCandidate(
                source=self.source_name,
                source_card_ref=card.id,
                observation_key=key,
                observation_type="provider_aggregate",  # never a sale: the source says nothing
                price=price,
                currency=_VENDOR_CURRENCY[vendor],
                observed_at=parse_provider_datetime(block.updated_at),
                observed_at_raw=block.updated_at,
                payload=raw_block,  # verbatim vendor block from the response
                raw_card_name=card.name,
                raw_set_name=card.card_set.name if card.card_set else None,
                raw_card_number=card.number,
                source_url=block.url,
                variant_label=variant,
            )

        found: list[PriceCandidate] = []
        if vendor == "tcgplayer":
            for variant, stats in prices.items():
                if not isinstance(stats, dict):
                    continue
                for price_field in TCGPLAYER_FIELDS:
                    if price_field in stats:
                        key = f"tcgplayer/{variant}/{price_field}"
                        found.append(build(key, stats[price_field], variant))
        else:
            for price_field in CARDMARKET_FIELDS:
                if price_field in prices:
                    found.append(build(f"cardmarket/{price_field}", prices[price_field]))
        return found


# ---------- registry ----------
ADAPTERS: dict[str, Callable[[Settings], PriceSourceClient]] = {
    PokemonTCGPriceClient.source_name: PokemonTCGPriceClient,
}


class UnknownPriceSourceError(ValueError):
    pass


def build_client(source: str, settings: Settings) -> PriceSourceClient:
    factory = ADAPTERS.get(source)
    if factory is None:
        raise UnknownPriceSourceError(
            f"Unknown price source {source!r}. Available: {', '.join(sorted(ADAPTERS))}"
        )
    return factory(settings)


# ---------- pipeline ----------
@dataclass
class PriceIngestResult:
    run_id: int | None
    pages: int
    stats: PriceStats
    dry_run: bool
    alert_checks: dict[str, int]


def _validate_page(
    page: PricePage, seen_listing_ids: set[tuple[str, str]]
) -> tuple[list[NormalizedObservation], list[QualityIssue], int]:
    accepted: list[NormalizedObservation] = []
    issues: list[QualityIssue] = []
    rejected = 0
    for candidate in page.candidates:
        observation, found = validate_candidate(candidate, seen_listing_ids)
        if observation is None:
            rejected += 1
            issues.extend(found)
        else:
            accepted.append(observation)
    return accepted, issues, rejected


def _record_failure(
    run_id: int,
    stats: PriceStats,
    pages: int,
    checks: Counter[str],
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
            {**stats.as_metadata(), "pages_fetched": pages, "alert_checks": dict(checks)},
            error_message=sanitize_error(exc, _secrets(settings)),
            dsn=dsn,
        )
    except Exception as tracking_exc:  # never mask the original error
        logger.error(
            "Could not record failure for run %s: %s",
            run_id,
            sanitize_error(tracking_exc, _secrets(settings)),
        )


def ingest_prices(
    settings: Settings | None = None,
    *,
    source_name: str | None = None,
    max_pages: int | None = None,
    query: str | None = None,
    dry_run: bool = False,
    client: PriceSourceClient | None = None,
    dsn: str | None = None,
) -> PriceIngestResult:
    """max_pages=None means all pages. dry_run fetches and validates but never touches the DB."""
    settings = settings or get_settings()
    dsn = dsn or settings.psycopg_dsn
    owns_client = client is None
    if client is None:
        client = build_client(source_name or settings.price_source_name, settings)

    run_id: int | None = None
    if not dry_run:
        run_id = start_run(
            PIPELINE_NAME,
            {"source": client.source_name, "max_pages": max_pages, "query": query},
            dsn,
        )
        logger.info("Started run %s (%s, source=%s)", run_id, PIPELINE_NAME, client.source_name)

    stats = PriceStats()
    checks: Counter[str] = Counter()
    seen_listing_ids: set[tuple[str, str]] = set()
    pages = 0
    try:
        for page in client.iter_pages(max_pages=max_pages, query=query):
            accepted, issues, rejected = _validate_page(page, seen_listing_ids)
            checks.update(i.check_name for i in issues)
            if dry_run:
                page_stats = PriceStats(alert_count=len(issues))  # alerts that WOULD be written
            else:
                with transaction(dsn) as conn:
                    page_stats = persist_price_page(conn, accepted, issues, run_id)  # type: ignore[arg-type]
            page_stats.rows_read = len(page.candidates)
            page_stats.accepted = len(accepted)
            page_stats.rejected_count = rejected
            page_stats.items_without_price_data = page.items_without_price_data
            stats.add(page_stats)  # counted only after the page committed
            pages += 1
            for found in issues[:5]:
                logger.warning("Rejected %s: %s", found.check_name, found.details)
    except BaseException as exc:
        if run_id is not None:
            _record_failure(run_id, stats, pages, checks, exc, settings, dsn)
        raise
    finally:
        if owns_client:
            client.close()

    if run_id is not None:
        finish_run(
            run_id,
            "succeeded",
            stats.rows_read,
            stats.rows_written,
            {
                **stats.as_metadata(),
                "pages_fetched": pages,
                "alert_checks": dict(checks),
                "source": client.source_name,
            },
            dsn=dsn,
        )
    return PriceIngestResult(run_id, pages, stats, dry_run, dict(checks))


# ---------- CLI ----------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest price observations from a price source")
    parser.add_argument(
        "--source",
        choices=sorted(ADAPTERS),
        default=None,
        help="price source adapter (default: PRICE_SOURCE_NAME)",
    )
    parser.add_argument(
        "--max-pages",
        type=_non_negative_int,
        default=None,
        help="pages to fetch; 0 = all (default: PRICE_MAX_PAGES)",
    )
    parser.add_argument("--query", type=_non_empty, default=None, help="source-specific filter")
    parser.add_argument(
        "--dry-run", action="store_true", help="fetch and validate only; write nothing"
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)
    max_pages = args.max_pages if args.max_pages is not None else settings.price_max_pages
    if not args.dry_run:
        logger.info("Database: %s", describe_dsn(settings.psycopg_dsn))

    try:
        result = ingest_prices(
            settings,
            source_name=args.source,
            max_pages=max_pages or None,
            query=args.query,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        logger.error("Price ingestion failed: %s", sanitize_error(exc, _secrets(settings)))
        return 1

    s = result.stats
    logger.info(
        "%s: run=%s pages=%s read=%s valid=%s rejected=%s alerts=%s | raw +%s/=%s | "
        "staging +%s/=%s | no_price_data=%s | rejects_by_check=%s",
        "DRY RUN" if result.dry_run else "Price run succeeded",
        result.run_id,
        result.pages,
        s.rows_read,
        s.accepted,
        s.rejected_count,
        s.alert_count,
        s.raw_inserted,
        s.raw_reused,
        s.staging_inserted,
        s.staging_unchanged,
        s.items_without_price_data,
        result.alert_checks,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
