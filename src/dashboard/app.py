"""CardPulse analyst dashboard. Run with: make run-dashboard (needs `make run-api`)."""

import altair as alt
import pandas as pd
import streamlit as st

from src.api.schemas import OBSERVATION_TYPE_NOTES
from src.config import get_settings
from src.dashboard import helpers
from src.dashboard.client import ApiError, CardPulseClient, NotFoundError
from src.ingestion.price_models import OBSERVATION_TYPES

st.set_page_config(page_title="CardPulse analyst dashboard", page_icon="📈", layout="wide")


@st.cache_resource
def get_client() -> CardPulseClient:
    return CardPulseClient(get_settings().dashboard_api_url)


@st.cache_data(ttl=60, show_spinner=False)
def cached(method: str, **kwargs):
    return getattr(get_client(), method)(**kwargs)


st.title("CardPulse analyst dashboard")
st.warning(helpers.DISCLAIMER)

# ---------- sidebar: API status and card selection ----------
with st.sidebar:
    st.header("API")
    try:
        health = get_client().health()
        st.caption(
            f"{get_client().base_url} · status **{health.get('status')}** · "
            f"database **{health.get('database')}** · v{health.get('version')}"
        )
    except ApiError as exc:
        st.error(str(exc))
        st.stop()

    st.header("Find a card")
    query = st.text_input("Card name", value="charizard")
    set_filter = st.text_input("Set name (optional)", value="")
    try:
        page = cached("search_cards", q=query or None, set_name=set_filter or None, limit=50)
    except ApiError as exc:
        st.error(str(exc))
        st.stop()
    items = page["items"]
    st.caption(f"{page['total']} card(s) match" + (" (showing 50)" if page["total"] > 50 else ""))
    if not items:
        st.info("No cards match this search.")
        st.stop()
    labels = {
        i["card_key"]: f"{i['name']} · {i['set_name'] or '?'} #{i['card_number'] or '?'}"
        for i in items
    }
    card_key = st.selectbox("Card", options=list(labels), format_func=labels.get)

selected = next(i for i in items if i["card_key"] == card_key)
head_left, head_right = st.columns([1, 5])
if selected.get("image_small_url"):
    head_left.image(selected["image_small_url"], width=140)
head_right.subheader(selected["name"])
head_right.caption(
    f"{selected['set_name'] or '?'} · #{selected['card_number'] or '?'} · "
    f"{selected['rarity'] or 'rarity unknown'} · card_id `{selected['card_id']}` · "
    f"card_key {selected['card_key']}"
)

tab_value, tab_history, tab_obs, tab_anomalies, tab_quality = st.tabs(
    ["Market value", "Price history", "Observations", "Anomalies", "Data quality"]
)

# ---------- market value ----------
with tab_value:
    left, right = st.columns(2)
    currency = left.selectbox("Currency", ["USD", "EUR"], key="mv_currency")
    type_choice = right.selectbox("Observation type", ["(all)", *OBSERVATION_TYPES], key="mv_type")
    try:
        payload = cached(
            "market_value",
            card_key=card_key,
            currency=currency,
            observation_type=None if type_choice == "(all)" else type_choice,
        )
    except NotFoundError as exc:
        st.info(f"{exc}. Nothing has been computed for this selection yet.")
        payload = None
    except ApiError as exc:
        st.error(str(exc))
        payload = None

    if payload:
        values = payload["values"]
        index = 0
        if len(values) > 1:
            index = st.selectbox(
                "Price series",
                range(len(values)),
                format_func=lambda i: (
                    f"{values[i]['observation_key']} · {values[i]['grade_label']}"
                ),
            )
        v = values[index]
        cur = v["currency"]
        st.markdown(
            f"`{v['observation_type']}` · methodology `{v['methodology_version']}` · "
            f"as of **{v['as_of_date']}** · latest observation: "
            f"{helpers.freshness_label(v['data_freshness'])}"
        )
        st.caption(v["observation_type_note"])

        m1, m2, m3, m4 = st.columns(4)
        m1.metric(
            "Recommended market value", helpers.format_money(v["recommended_market_value"], cur)
        )
        m2.metric("7-day median", helpers.format_money(v["price_7d_median"], cur))
        m3.metric("30-day median", helpers.format_money(v["price_30d_median"], cur))
        m4.metric(
            "Buyback price (policy estimate)",
            helpers.format_money(v["recommended_buyback_price"], cur),
        )
        n1, n2, n3, n4 = st.columns(4)
        n1.metric("7-day change", helpers.format_pct(v["price_7d_change_pct"]))
        n2.metric("30-day change", helpers.format_pct(v["price_30d_change_pct"]))
        n3.metric(
            "Buy price (policy estimate)", helpers.format_money(v["recommended_buy_price"], cur)
        )
        n4.metric(
            "Retail price (policy estimate)",
            helpers.format_money(v["recommended_retail_price"], cur),
        )
        st.caption(
            f"Observations: {v['observation_count_7d']} in 7 days, "
            f"{v['observation_count_30d']} in 30 days · liquidity `{v['liquidity_tier']}` · "
            f"buyback tier `{v['buyback_tier']}`. Change percentages are empty when there is "
            "no earlier window to compare with."
        )

        st.markdown("#### Confidence")
        score = float(v["confidence_score"] or 0)
        st.progress(
            min(max(score / 100, 0.0), 1.0),
            text=f"Confidence {score:.1f} / 100 ({helpers.confidence_band(score)})",
        )
        rows = helpers.confidence_rows(v["confidence_components"])
        if rows:
            st.dataframe(pd.DataFrame(rows), hide_index=True)
        with st.expander("Pricing policy and methodology"):
            st.json(v["policy"])

# ---------- price history ----------
with tab_history:
    days = st.slider("Days of history", min_value=7, max_value=365, value=90, key="hist_days")
    try:
        history = cached("price_history", card_key=card_key, days=days)
    except ApiError as exc:
        st.error(str(exc))
        history = None
    if history is not None:
        frame = helpers.history_frame(history["points"])
        if frame.empty:
            st.info("No price history is stored for this card.")
        else:
            currencies = sorted(frame["currency"].unique())
            chosen = st.selectbox("Currency", currencies, key="hist_currency")
            view = frame[frame["currency"] == chosen]
            distinct_days = view["price_date"].nunique()
            st.caption(
                f"{distinct_days} day(s) with data between {history['from_date']} and "
                f"{history['to_date']}. Only stored daily rows are plotted; gaps are not filled "
                "or interpolated. Prices are provider aggregates, not completed sales."
            )
            chart = (
                alt.Chart(view)
                .mark_line(point=True)
                .encode(
                    x=alt.X("price_date:T", title="Date"),
                    y=alt.Y("median_price:Q", title=f"Median price ({chosen})"),
                    color=alt.Color("observation_key:N", title="Series"),
                    tooltip=[
                        "price_date:T",
                        "observation_key:N",
                        "observation_type:N",
                        "median_price:Q",
                        "observation_count:Q",
                        "source:N",
                    ],
                )
            )
            st.altair_chart(chart)
            with st.expander("Daily rows"):
                st.dataframe(view, hide_index=True)

# ---------- observations ----------
with tab_obs:
    page_size = st.selectbox("Rows per page", [25, 50, 100], index=1, key="obs_size")
    page_number = st.number_input("Page", min_value=1, value=1, step=1, key="obs_page")
    try:
        obs = cached(
            "observations",
            card_key=card_key,
            limit=page_size,
            offset=(int(page_number) - 1) * page_size,
        )
    except ApiError as exc:
        st.error(str(exc))
        obs = None
    if obs is not None:
        st.caption(
            f"{obs['total']} matched observation(s). `observation_type` says what each price "
            "is; provider_aggregate rows are not sales."
        )
        if obs["items"]:
            columns = [
                "observed_at", "observation_type", "observation_key", "price", "currency",
                "source", "source_card_ref", "match_method", "match_score", "match_status",
            ]  # fmt: skip
            st.dataframe(pd.DataFrame(obs["items"])[columns], hide_index=True)
        else:
            st.info("No observations on this page.")

# ---------- anomalies ----------
with tab_anomalies:
    a1, a2, a3 = st.columns(3)
    severity = a1.selectbox("Severity", ["(all)", "info", "warning", "error", "critical"])
    only_card = a3.checkbox("Only this card", value=False)
    try:
        found = cached(
            "anomalies",
            limit=100,
            severity=None if severity == "(all)" else severity,
            card_key=card_key if only_card else None,
        )
    except ApiError as exc:
        st.error(str(exc))
        found = None
    if found is not None:
        st.caption(f"{found['total']} anomaly(ies) found.")
        if found["items"]:
            table = pd.DataFrame(
                [
                    {
                        "detected_at": a["detected_at"],
                        "as_of_date": a["as_of_date"],
                        "type": a["anomaly_type"],
                        "severity": a["severity"],
                        "card": (a["card"] or {}).get("name"),
                        "series": a["observation_key"],
                        "expected": a["expected_price"],
                        "observed": a["observed_price"],
                        "resolved": a["resolved_at"] is not None,
                    }
                    for a in found["items"]
                ]
            )
            st.dataframe(table, hide_index=True)
        else:
            st.success("No anomalies recorded.")

# ---------- data quality ----------
with tab_quality:
    try:
        quality = cached("data_quality_summary")
    except ApiError as exc:
        st.error(str(exc))
        quality = None
    if quality is not None:
        q1, q2, q3, q4, q5 = st.columns(5)
        q1.metric("Observations", quality["total_observations"])
        q2.metric("Matched", quality["matched_observations"])
        q3.metric("In review", quality["review_observations"])
        q4.metric("Unmatched", quality["unmatched_observations"])
        q5.metric("Open anomalies", quality["open_anomalies"])
        for note in quality["notes"]:
            st.caption(note)
        c1, c2 = st.columns(2)
        c1.markdown("**Observations by source**")
        c1.dataframe(pd.DataFrame(quality["observations_by_source"]), hide_index=True)
        c2.markdown("**Observations by observation type**")
        c2.dataframe(pd.DataFrame(quality["observations_by_type"]), hide_index=True)
        st.markdown("**Data freshness by source**")
        st.dataframe(pd.DataFrame(quality["data_freshness_by_source"]), hide_index=True)
        st.markdown("**Latest successful pipeline runs**")
        st.dataframe(pd.DataFrame(quality["latest_successful_runs"]), hide_index=True)
        st.caption(
            "Type notes: " + " | ".join(f"{k}: {v}" for k, v in OBSERVATION_TYPE_NOTES.items())
        )
