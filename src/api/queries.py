"""Parameterized, read-only SQL for the API. Rows are dicts (psycopg dict_row)."""

from datetime import date
from typing import Any

import psycopg

CARD_COLUMNS = (
    "c.card_key, c.card_id, c.name, c.supertype, c.subtypes, c.set_id, c.set_name, c.series, "
    "c.card_number, c.rarity, c.artist, c.release_date, c.image_small_url, c.image_large_url"
)

Row = dict[str, Any]


def escape_like(value: str) -> str:
    """Make user text literal inside a LIKE pattern (used with ESCAPE '\\')."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# ---------- cards ----------
def search_cards(
    conn: psycopg.Connection, q: str | None, set_name: str | None, limit: int, offset: int
) -> tuple[int, list[Row]]:
    where: list[str] = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    ranking = ""
    if q:
        params["q"] = q
        params["pattern"] = f"%{escape_like(q)}%"
        params["prefix"] = f"{escape_like(q)}%"
        where.append("c.name ILIKE %(pattern)s ESCAPE '\\'")
        ranking = (
            "(lower(c.name) = lower(%(q)s)) DESC, (c.name ILIKE %(prefix)s ESCAPE '\\') DESC, "
        )
    if set_name:
        params["set_pattern"] = f"%{escape_like(set_name)}%"
        where.append("c.set_name ILIKE %(set_pattern)s ESCAPE '\\'")
    clause = f" WHERE {' AND '.join(where)}" if where else ""

    total = conn.execute(f"SELECT count(*) AS total FROM dim_cards c{clause}", params).fetchone()
    rows = conn.execute(
        f"SELECT {CARD_COLUMNS} FROM dim_cards c{clause} "
        f"ORDER BY {ranking}c.name, c.set_name, c.card_key "
        "LIMIT %(limit)s OFFSET %(offset)s",
        params,
    ).fetchall()
    return total["total"], rows


def get_card(conn: psycopg.Connection, card_key: int) -> Row | None:
    return conn.execute(
        f"SELECT {CARD_COLUMNS} FROM dim_cards c WHERE c.card_key = %(card_key)s",
        {"card_key": card_key},
    ).fetchone()


# ---------- market values ----------
_MARKET_VALUE_COLUMNS = """
    v.card_key, v.grade_id, g.label AS grade_label, v.as_of_date, v.observation_type,
    v.currency, v.observation_key, v.methodology_version, v.price_7d_median,
    v.price_30d_median, v.price_7d_change_pct, v.price_30d_change_pct,
    v.observation_count_7d, v.observation_count_30d, v.price_dispersion,
    v.source_agreement_score, v.confidence_score, v.confidence_components,
    v.liquidity_tier, v.buyback_tier, v.buyback_discount, v.recommended_market_value,
    v.recommended_buy_price, v.recommended_retail_price, v.recommended_buyback_price,
    v.policy, v.latest_observed_at, v.computed_at
"""


def latest_as_of(
    conn: psycopg.Connection, card_key: int, currency: str | None, observation_type: str | None
) -> date | None:
    where = ["card_key = %(card_key)s"]
    params: dict[str, Any] = {"card_key": card_key}
    if currency:
        where.append("currency = %(currency)s")
        params["currency"] = currency
    if observation_type:
        where.append("observation_type = %(observation_type)s")
        params["observation_type"] = observation_type
    clause = " AND ".join(where)
    row = conn.execute(
        f"SELECT max(as_of_date) AS as_of_date FROM fct_card_market_values WHERE {clause}", params
    ).fetchone()
    return row["as_of_date"]


def fetch_market_values(
    conn: psycopg.Connection,
    card_key: int,
    as_of_date: date,
    currency: str | None,
    observation_type: str | None,
) -> list[Row]:
    where = ["v.card_key = %(card_key)s", "v.as_of_date = %(as_of_date)s"]
    params: dict[str, Any] = {"card_key": card_key, "as_of_date": as_of_date}
    if currency:
        where.append("v.currency = %(currency)s")
        params["currency"] = currency
    if observation_type:
        where.append("v.observation_type = %(observation_type)s")
        params["observation_type"] = observation_type
    clause = " AND ".join(where)
    rows = conn.execute(
        f"SELECT DISTINCT ON (v.grade_id, v.observation_type, v.currency, v.observation_key) "
        f"{_MARKET_VALUE_COLUMNS} "
        "FROM fct_card_market_values v JOIN dim_grades g ON g.grade_id = v.grade_id "
        f"WHERE {clause} "
        "ORDER BY v.grade_id, v.observation_type, v.currency, v.observation_key, "
        "v.methodology_version DESC",
        params,
    ).fetchall()
    return sorted(rows, key=lambda r: (r["currency"], r["observation_key"], r["grade_id"]))


# ---------- price history ----------
def _history_filters(
    card_key: int, currency: str | None, observation_type: str | None
) -> tuple[str, dict[str, Any]]:
    where = ["d.card_key = %(card_key)s"]
    params: dict[str, Any] = {"card_key": card_key}
    if currency:
        where.append("d.currency = %(currency)s")
        params["currency"] = currency
    if observation_type:
        where.append("d.observation_type = %(observation_type)s")
        params["observation_type"] = observation_type
    return " AND ".join(where), params


def history_window_end(
    conn: psycopg.Connection, card_key: int, currency: str | None, observation_type: str | None
) -> date | None:
    clause, params = _history_filters(card_key, currency, observation_type)
    row = conn.execute(
        f"SELECT max(d.price_date) AS last_date FROM fct_daily_card_prices d WHERE {clause}",
        params,
    ).fetchone()
    return row["last_date"]


def fetch_price_history(
    conn: psycopg.Connection,
    card_key: int,
    start: date,
    end: date,
    currency: str | None,
    observation_type: str | None,
) -> list[Row]:
    clause, params = _history_filters(card_key, currency, observation_type)
    params.update({"start": start, "end": end})
    return conn.execute(
        "SELECT d.price_date, d.source, d.observation_type, d.observation_key, d.currency, "
        "d.grade_id, g.label AS grade_label, d.observation_count, d.min_price, d.max_price, "
        "d.avg_price, d.median_price, d.price_dispersion, d.latest_price, d.latest_observed_at "
        "FROM fct_daily_card_prices d JOIN dim_grades g ON g.grade_id = d.grade_id "
        f"WHERE {clause} AND d.price_date >= %(start)s AND d.price_date <= %(end)s "
        "ORDER BY d.price_date, d.observation_key, d.currency, d.source, d.grade_id",
        params,
    ).fetchall()


# ---------- observations ----------
_OBSERVATION_FROM = """
FROM fct_card_matches m
JOIN stg_price_observations s ON s.id = m.stg_observation_id
WHERE m.card_key = %(card_key)s AND m.status IN ('auto', 'confirmed')
"""


def fetch_observations(
    conn: psycopg.Connection, card_key: int, limit: int, offset: int
) -> tuple[int, list[Row]]:
    params = {"card_key": card_key, "limit": limit, "offset": offset}
    total = conn.execute(f"SELECT count(*) AS total {_OBSERVATION_FROM}", params).fetchone()
    rows = conn.execute(
        "SELECT s.id AS observation_id, s.source, s.source_card_ref, s.observation_type, "
        "s.observation_key, s.price, s.currency, s.observed_at, m.match_method, "
        "m.match_score, m.status AS match_status "
        f"{_OBSERVATION_FROM} "
        "ORDER BY s.observed_at DESC, s.id DESC LIMIT %(limit)s OFFSET %(offset)s",
        params,
    ).fetchall()
    return total["total"], rows


# ---------- anomalies ----------
def fetch_anomalies(
    conn: psycopg.Connection,
    limit: int,
    offset: int,
    severity: str | None,
    anomaly_type: str | None,
    card_key: int | None,
) -> tuple[int, list[Row]]:
    where: list[str] = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if severity:
        where.append("a.severity = %(severity)s")
        params["severity"] = severity
    if anomaly_type:
        where.append("a.anomaly_type = %(anomaly_type)s")
        params["anomaly_type"] = anomaly_type
    if card_key is not None:
        where.append("a.card_key = %(card_key)s")
        params["card_key"] = card_key
    clause = f" WHERE {' AND '.join(where)}" if where else ""
    total = conn.execute(
        f"SELECT count(*) AS total FROM fct_price_anomalies a{clause}", params
    ).fetchone()
    rows = conn.execute(
        "SELECT a.anomaly_id, a.anomaly_type, a.severity, a.as_of_date, a.detected_at, "
        "a.resolved_at, a.card_key, a.stg_observation_id, a.source, a.observation_type, "
        "a.observation_key, a.currency, a.expected_price, a.observed_price, a.details, "
        "c.card_id, c.name AS card_name, c.set_name, c.card_number, c.rarity, "
        "c.image_small_url "
        "FROM fct_price_anomalies a LEFT JOIN dim_cards c ON c.card_key = a.card_key"
        f"{clause} ORDER BY a.detected_at DESC, a.anomaly_id DESC "
        "LIMIT %(limit)s OFFSET %(offset)s",
        params,
    ).fetchall()
    return total["total"], rows


# ---------- data quality ----------
def data_quality_summary(conn: psycopg.Connection) -> dict[str, Any]:
    counts = conn.execute(
        "SELECT count(*) AS total, "
        "count(*) FILTER (WHERE m.status IN ('auto', 'confirmed') AND m.card_key IS NOT NULL) "
        "AS matched, "
        "count(*) FILTER (WHERE m.status = 'review') AS review "
        "FROM stg_price_observations s "
        "LEFT JOIN fct_card_matches m ON m.stg_observation_id = s.id"
    ).fetchone()
    by_source = conn.execute(
        "SELECT source, count(*) AS observations FROM stg_price_observations "
        "GROUP BY source ORDER BY count(*) DESC, source"
    ).fetchall()
    by_type = conn.execute(
        "SELECT observation_type, count(*) AS observations FROM stg_price_observations "
        "GROUP BY observation_type ORDER BY count(*) DESC, observation_type"
    ).fetchall()
    runs = conn.execute(
        "SELECT DISTINCT ON (pipeline_name) run_id, pipeline_name, status, finished_at, "
        "rows_read, rows_written FROM data_pipeline_runs WHERE status = 'succeeded' "
        "ORDER BY pipeline_name, finished_at DESC NULLS LAST, run_id DESC"
    ).fetchall()
    freshness = conn.execute(
        "SELECT source, max(observed_at) AS latest_observed_at, max(loaded_at) AS "
        "latest_loaded_at, extract(epoch FROM (now() - max(observed_at))) / 86400.0 AS age_days "
        "FROM stg_price_observations GROUP BY source ORDER BY source"
    ).fetchall()
    open_anomalies = conn.execute(
        "SELECT count(*) AS open_anomalies FROM fct_price_anomalies WHERE resolved_at IS NULL"
    ).fetchone()
    return {
        "total_observations": counts["total"],
        "matched_observations": counts["matched"],
        "review_observations": counts["review"],
        "unmatched_observations": counts["total"] - counts["matched"] - counts["review"],
        "observations_by_source": by_source,
        "observations_by_type": by_type,
        "latest_successful_runs": runs,
        "data_freshness_by_source": freshness,
        "open_anomalies": open_anomalies["open_anomalies"],
    }
