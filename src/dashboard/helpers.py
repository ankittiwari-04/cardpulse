"""Pure helpers for the dashboard (no Streamlit import, so they are unit-testable)."""

from typing import Any

import pandas as pd

DISCLAIMER = (
    "**Provider aggregates are not completed sales.** Every price shown here is currently a "
    "`provider_aggregate`: a statistic (market, trend, average or low) published by TCGplayer "
    "or Cardmarket through the Pokémon TCG API. It does not describe individual sales and "
    "carries no transaction volume. Buy, retail and buyback figures are policy-based "
    "estimates, not offers."
)

COMPONENT_ORDER = (
    "observation_count",
    "recency",
    "dispersion",
    "source_agreement",
    "completeness",
)
COMPONENT_LABELS = {
    "observation_count": "Number of observations",
    "recency": "Recency",
    "dispersion": "Price dispersion",
    "source_agreement": "Source agreement",
    "completeness": "Data completeness",
}


def format_money(value: Any, currency: str) -> str:
    if value is None:
        return "—"
    return f"{currency} {float(value):,.2f}"


def format_pct(value: Any) -> str:
    if value is None:
        return "—"
    return f"{float(value):+.1f}%"


def confidence_band(score: Any) -> str:
    if score is None:
        return "unknown"
    score = float(score)
    if score >= 80:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def confidence_rows(components: dict[str, Any]) -> list[dict[str, Any]]:
    """One row per confidence component, in a fixed order, with its explanation."""
    rows = []
    for name in COMPONENT_ORDER:
        part = components.get(name)
        if not isinstance(part, dict):
            continue
        rows.append(
            {
                "component": COMPONENT_LABELS[name],
                "score (0-1)": part.get("score"),
                "weight": part.get("weight"),
                "weighted": part.get("weighted"),
                "explanation": part.get("explanation"),
            }
        )
    return rows


def freshness_label(freshness: dict[str, Any] | None) -> str:
    if not freshness or freshness.get("latest_observed_at") is None:
        return "no observation time available"
    at_as_of = freshness.get("age_days_at_as_of")
    now = freshness.get("age_days_now")
    parts = []
    if at_as_of is not None:
        parts.append(f"{at_as_of} day(s) old at the as-of date")
    if now is not None:
        parts.append(f"{now:.1f} day(s) old now")
    return ", ".join(parts)


def history_frame(points: list[dict[str, Any]]) -> pd.DataFrame:
    """Daily points exactly as returned by the API: no resampling, no filling of gaps."""
    frame = pd.DataFrame(points)
    if frame.empty:
        return frame
    frame["price_date"] = pd.to_datetime(frame["price_date"])
    for column in ("min_price", "max_price", "avg_price", "median_price", "latest_price"):
        frame[column] = pd.to_numeric(frame[column])
    return frame.sort_values(["observation_key", "price_date"]).reset_index(drop=True)
