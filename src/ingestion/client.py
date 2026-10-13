import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)
from tenacity.wait import wait_base

from src.config import Settings, get_settings
from src.ingestion.models import Card, CardPage, CardSet, SetPage

logger = logging.getLogger(__name__)

MAX_PAGE_SIZE = 250  # Pokemon TCG API maximum


@dataclass(frozen=True)
class CatalogPage:
    """One fetched page: validated cards plus the untouched card objects from the API."""

    page: int
    total_count: int
    cards: list[Card]
    raw_cards: list[dict[str, Any]]


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):  # timeouts, connection errors
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False


class PokemonTCGClient:
    """Minimal client for https://api.pokemontcg.io/v2 (cards and sets)."""

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
        headers = {"Accept": "application/json"}
        if settings.pokemon_tcg_api_key:  # optional: only raises the rate limit
            headers["X-Api-Key"] = settings.pokemon_tcg_api_key

        self._http = httpx.Client(
            base_url=settings.pokemon_tcg_base_url.rstrip("/") + "/",
            headers=headers,
            timeout=httpx.Timeout(30.0, connect=10.0),
            transport=transport,
        )
        self._page_size = min(page_size, MAX_PAGE_SIZE)
        self._retryer = Retrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait or wait_exponential_jitter(initial=1, max=30),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        )

    # -- lifecycle --
    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "PokemonTCGClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- http --
    def _request(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        response = self._http.get(path, params=params)
        response.raise_for_status()
        return response.json()

    def _paginate(
        self,
        path: str,
        page_model: type[Any],
        params: dict[str, Any] | None = None,
        max_pages: int | None = None,
    ) -> Iterator[tuple[Any, dict[str, Any]]]:
        """Yield (validated page, original response JSON) for each page."""
        params = dict(params or {})
        page = 1
        while True:
            raw = self._retryer(
                self._request, path, {**params, "page": page, "pageSize": self._page_size}
            )
            parsed = page_model.model_validate(raw)
            logger.info(
                "Fetched %s page %s (%s items, %s total)",
                path,
                page,
                len(parsed.data),
                parsed.total_count,
            )
            yield parsed, raw
            if not parsed.data or page * parsed.page_size >= parsed.total_count:
                return
            if max_pages is not None and page >= max_pages:
                return
            page += 1

    # -- public API --
    def iter_catalog_pages(
        self, query: str | None = None, max_pages: int | None = None
    ) -> Iterator[CatalogPage]:
        params = {"q": query} if query else None
        for parsed, raw in self._paginate("cards", CardPage, params, max_pages):
            yield CatalogPage(
                page=parsed.page,
                total_count=parsed.total_count,
                cards=parsed.data,
                raw_cards=raw.get("data", []),
            )

    def iter_card_pages(
        self, query: str | None = None, max_pages: int | None = None
    ) -> Iterator[CardPage]:
        params = {"q": query} if query else None
        for parsed, _raw in self._paginate("cards", CardPage, params, max_pages):
            yield parsed

    def iter_cards(self, query: str | None = None, max_pages: int | None = None) -> Iterator[Card]:
        for page in self.iter_card_pages(query, max_pages):
            yield from page.data

    def iter_sets(self, max_pages: int | None = None) -> Iterator[CardSet]:
        for parsed, _raw in self._paginate("sets", SetPage, None, max_pages):
            yield from parsed.data
