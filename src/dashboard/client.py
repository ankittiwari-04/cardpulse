"""Small HTTP client for the CardPulse API (used by the Streamlit dashboard)."""

from typing import Any

import httpx


class ApiError(RuntimeError):
    """The API was unreachable or returned an error."""


class NotFoundError(ApiError):
    """The API answered 404 (unknown card, or no data for the selection)."""


def _detail(response: httpx.Response) -> str:
    try:
        detail = response.json().get("detail")
    except (ValueError, AttributeError):
        detail = None
    return str(detail) if detail else response.reason_phrase


class CardPulseClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(base_url=self.base_url, timeout=timeout, transport=transport)

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        try:
            response = self._http.get(path, params=clean)
        except httpx.HTTPError as exc:
            raise ApiError(
                f"API unreachable at {self.base_url} ({type(exc).__name__}). "
                "Is `make run-api` running?"
            ) from exc
        if response.status_code == 404:
            raise NotFoundError(_detail(response))
        if response.status_code >= 400:
            raise ApiError(f"API returned {response.status_code}: {_detail(response)}")
        return response.json()

    def health(self) -> dict[str, Any]:
        """Returns the health body even when the API reports 503 (degraded)."""
        try:
            return self._http.get("/health").json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ApiError(f"API unreachable at {self.base_url} ({type(exc).__name__})") from exc

    def search_cards(
        self,
        q: str | None = None,
        set_name: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> dict[str, Any]:
        params = {"q": q, "set_name": set_name, "limit": limit, "offset": offset}
        return self._get("/cards/search", params)

    def card(self, card_key: int) -> dict[str, Any]:
        return self._get(f"/cards/{card_key}")

    def market_value(
        self,
        card_key: int,
        currency: str = "USD",
        observation_type: str | None = None,
        as_of_date: str | None = None,
    ) -> dict[str, Any]:
        params = {
            "currency": currency,
            "observation_type": observation_type,
            "as_of_date": as_of_date,
        }
        return self._get(f"/cards/{card_key}/market-value", params)

    def price_history(
        self,
        card_key: int,
        days: int = 90,
        observation_type: str | None = None,
        currency: str | None = None,
    ) -> dict[str, Any]:
        params = {"days": days, "observation_type": observation_type, "currency": currency}
        return self._get(f"/cards/{card_key}/price-history", params)

    def observations(self, card_key: int, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        return self._get(f"/cards/{card_key}/observations", {"limit": limit, "offset": offset})

    def anomalies(
        self,
        limit: int = 50,
        offset: int = 0,
        severity: str | None = None,
        anomaly_type: str | None = None,
        card_key: int | None = None,
    ) -> dict[str, Any]:
        params = {
            "limit": limit,
            "offset": offset,
            "severity": severity,
            "anomaly_type": anomaly_type,
            "card_key": card_key,
        }
        return self._get("/anomalies", params)

    def data_quality_summary(self) -> dict[str, Any]:
        return self._get("/data-quality/summary")
