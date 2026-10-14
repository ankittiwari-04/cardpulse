"""Database integration tests for entity resolution.

Run with TEST_DATABASE_URL set. Each test works inside its own throwaway schema
(cp_test_<random>) that is dropped afterwards; no existing table or data is touched.
"""

import os
import uuid

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from src.config import Settings
from src.db.connection import normalize_dsn, transaction
from src.db.migrate import apply_schema
from src.matching import entity_resolution as er
from src.matching.entity_resolution import MatchDecision, resolve_observations

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL is not set; skipping database integration tests",
)


@pytest.fixture
def dsn():
    base = normalize_dsn(os.environ["TEST_DATABASE_URL"])
    schema = f"cp_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    scoped = make_conninfo(base, options=f"-c search_path={schema}")
    apply_schema(scoped)
    yield scoped
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def rows(dsn, query, params=()):
    with psycopg.connect(dsn) as conn:
        return conn.execute(query, params).fetchall()


def count(dsn, table):
    return rows(dsn, f"SELECT count(*) FROM {table}")[0][0]


def add_card(dsn, card_id, name, set_id, set_name, number):
    return rows(
        dsn,
        "INSERT INTO dim_cards (card_id, name, set_id, set_name, card_number) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING card_key",
        (card_id, name, set_id, set_name, number),
    )[0][0]


def add_obs(
    dsn,
    *,
    source="pokemontcg",
    ref="base1-4",
    name="Charizard",
    set_name="Base",
    number="4",
    key="tcgplayer/normal/market",
):
    return rows(
        dsn,
        "INSERT INTO stg_price_observations (source, source_card_ref, raw_card_name, "
        "raw_set_name, raw_card_number, price, currency, observed_at, observation_type, "
        "observation_key) VALUES (%s, %s, %s, %s, %s, 1.5, 'USD', now(), "
        "'provider_aggregate', %s) RETURNING id",
        (source, ref, name, set_name, number, key),
    )[0][0]


def resolve(dsn, **kwargs):
    return resolve_observations(Settings(_env_file=None), dsn=dsn, **kwargs)


def matches(dsn):
    return rows(
        dsn,
        "SELECT stg_observation_id, card_key, match_method, match_score, status, "
        "explanation->>'rule' FROM fct_card_matches ORDER BY stg_observation_id",
    )


def run_row(dsn, run_id):
    return rows(
        dsn,
        "SELECT status, rows_read, rows_written, finished_at, error_message, metadata "
        "FROM data_pipeline_runs WHERE run_id = %s",
        (run_id,),
    )[0]


def test_deterministic_pokemontcg_matching(dsn):
    k4 = add_card(dsn, "base1-4", "Charizard", "base1", "Base", "4")
    k5 = add_card(dsn, "base1-5", "Clefairy", "base1", "Base", "5")
    o1 = add_obs(dsn, ref="base1-4", key="tcgplayer/normal/market")
    o2 = add_obs(dsn, ref="base1-4", key="tcgplayer/normal/low")
    o3 = add_obs(dsn, ref="base1-5", name="Clefairy", number="5")

    result = resolve(dsn)

    assert matches(dsn) == [
        (o1, k4, "deterministic", 100, "auto", "source_reference"),
        (o2, k4, "deterministic", 100, "auto", "source_reference"),
        (o3, k5, "deterministic", 100, "auto", "source_reference"),
    ]
    status, rows_read, rows_written, finished_at, error, meta = run_row(dsn, result.run_id)
    assert status == "succeeded" and finished_at is not None and error is None
    assert (rows_read, rows_written) == (3, 3)
    assert meta["observations_processed"] == 3 and meta["deterministic_matches"] == 3
    assert meta["fuzzy_matches"] == 0 and meta["review_count"] == 0
    assert meta["unmatched_count"] == 0 and meta["rows_written"] == 3


def test_fuzzy_conflicts_and_review_routing(dsn):
    k4 = add_card(dsn, "base1-4", "Charizard", "base1", "Base", "4")
    add_card(dsn, "base1-5", "Clefairy", "base1", "Base", "5")
    exact = add_obs(dsn, source="shop", ref="m-1", set_name="Base Set", number="4/102")
    typo = add_obs(dsn, source="shop", ref="m-2", name="Charizrd", number="#4")
    conflict = add_obs(dsn, source="shop", ref="m-3", number="5")
    unknown = add_obs(dsn, source="shop", ref="m-4", name="Zubat Deluxe Collector Edition",
                      set_name=None, number=None)  # fmt: skip

    result = resolve(dsn)

    by_obs = {m[0]: m for m in matches(dsn)}
    assert by_obs[exact][1:5] == (k4, "fuzzy", 95, "auto")
    assert by_obs[typo][1] == k4 and by_obs[typo][4] == "auto" and by_obs[typo][2] == "fuzzy"
    assert by_obs[conflict][1] is None and by_obs[conflict][2:5][0] == "none"
    assert by_obs[conflict][4] == "review"
    assert by_obs[unknown][1] is None and by_obs[unknown][3] is None
    assert by_obs[unknown][4] == "review"

    explanation = rows(
        dsn, "SELECT explanation FROM fct_card_matches WHERE stg_observation_id = %s", (conflict,)
    )[0][0]
    assert {e["card_id"]: e["reason"] for e in explanation["excluded"]}["base1-4"] == (
        "number_conflict"
    )

    meta = run_row(dsn, result.run_id)[5]
    assert meta["fuzzy_matches"] == 2 and meta["review_count"] == 1
    assert meta["unmatched_count"] == 1 and meta["deterministic_matches"] == 0
    assert meta["rows_written"] == 4


def test_idempotent_rerun(dsn):
    add_card(dsn, "base1-4", "Charizard", "base1", "Base", "4")
    add_obs(dsn)
    add_obs(dsn, source="shop", ref="m-1", name="Unknowable", set_name=None, number=None)
    resolve(dsn)
    before = rows(dsn, "SELECT stg_observation_id, matched_at FROM fct_card_matches ORDER BY 1")

    second = resolve(dsn)

    assert second.stats.observations_processed == 0 and second.stats.rows_written == 0
    assert (
        rows(dsn, "SELECT stg_observation_id, matched_at FROM fct_card_matches ORDER BY 1")
        == before
    )
    assert count(dsn, "fct_card_matches") == 2
    assert count(dsn, "data_pipeline_runs") == 2


def test_reprocess_only_reconsiders_review_rows(dsn):
    add_card(dsn, "base1-4", "Charizard", "base1", "Base", "4")
    auto_obs = add_obs(dsn)
    late = add_obs(dsn, source="shop", ref="m-1", name="Machamp", number="8")
    stuck = add_obs(dsn, source="shop", ref="m-2", name="Zubat Deluxe Collector Edition",
                    set_name=None, number=None)  # fmt: skip
    resolve(dsn)
    assert {m[0]: m[4] for m in matches(dsn)} == {auto_obs: "auto", late: "review", stuck: "review"}
    stamps = {
        r[0]: r[1] for r in rows(dsn, "SELECT stg_observation_id, matched_at FROM fct_card_matches")
    }

    k8 = add_card(dsn, "base1-8", "Machamp", "base1", "Base", "8")  # the catalog catches up

    plain = resolve(dsn)  # without --reprocess nothing is reconsidered
    assert plain.stats.observations_processed == 0

    again = resolve(dsn, reprocess=True)
    assert again.stats.observations_processed == 2  # only the two review rows
    assert again.stats.fuzzy_matches == 1 and again.stats.unmatched_count == 1
    assert (again.stats.rows_updated, again.stats.rows_unchanged) == (1, 1)
    assert again.stats.rows_written == 1

    final = {m[0]: m for m in matches(dsn)}
    assert count(dsn, "fct_card_matches") == 3  # still one row per observation
    assert final[late][1] == k8 and final[late][2] == "fuzzy" and final[late][4] == "auto"
    after = {
        r[0]: r[1] for r in rows(dsn, "SELECT stg_observation_id, matched_at FROM fct_card_matches")
    }
    assert after[auto_obs] == stamps[auto_obs]  # auto row untouched
    assert after[stuck] == stamps[stuck]  # unchanged review row untouched
    assert after[late] > stamps[late]

    assert resolve(dsn, reprocess=True).stats.observations_processed == 1  # only 'stuck' remains


def test_dry_run_writes_nothing(dsn):
    add_card(dsn, "base1-4", "Charizard", "base1", "Base", "4")
    add_obs(dsn)
    add_obs(dsn, key="tcgplayer/normal/low")
    add_obs(dsn, source="shop", ref="m-1", name="Unknowable", set_name=None, number=None)

    result = resolve(dsn, dry_run=True)

    assert result.run_id is None and result.dry_run
    assert result.stats.observations_processed == 3
    assert result.stats.deterministic_matches == 2 and result.stats.unmatched_count == 1
    assert result.stats.rows_written == 0
    assert count(dsn, "fct_card_matches") == 0
    assert count(dsn, "data_pipeline_runs") == 0


def test_dry_run_connection_is_read_only(dsn):
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        with transaction(dsn, read_only=True) as conn:
            conn.execute("INSERT INTO dim_grades (grading_company, label) VALUES ('X', 'X 1')")


def test_confirmed_and_rejected_matches_are_never_overwritten(dsn):
    k4 = add_card(dsn, "base1-4", "Charizard", "base1", "Base", "4")
    k5 = add_card(dsn, "base1-5", "Clefairy", "base1", "Base", "5")
    confirmed = add_obs(dsn, ref="base1-4")
    rejected = add_obs(dsn, source="shop", ref="m-1", name="Charizard", number="4")
    for obs_id, card_key, status in ((confirmed, k5, "confirmed"), (rejected, None, "rejected")):
        rows(
            dsn,
            "INSERT INTO fct_card_matches (stg_observation_id, card_key, match_method, "
            "match_score, status) VALUES (%s, %s, 'manual', 100, %s) RETURNING 1",
            (obs_id, card_key, status),
        )

    result = resolve(dsn, reprocess=True)
    assert result.stats.observations_processed == 0

    # Even if a decision is forced at the SQL level, only 'review' rows can be updated.
    forced = MatchDecision(confirmed, k4, "base1-4", "deterministic", "auto", 100.0,
                           "source_reference", "deterministic", {})  # fmt: skip
    with transaction(dsn) as conn:
        assert er.write_decisions(conn, [forced], None) == (0, 0, 1)
    assert [(m[0], m[1], m[4]) for m in matches(dsn)] == [
        (confirmed, k5, "confirmed"),
        (rejected, None, "rejected"),
    ]


def test_one_match_row_per_observation_is_enforced(dsn):
    k4 = add_card(dsn, "base1-4", "Charizard", "base1", "Base", "4")
    obs_id = add_obs(dsn)
    insert = (
        "INSERT INTO fct_card_matches (stg_observation_id, card_key, match_method, status) "
        "VALUES (%s, %s, 'deterministic', 'auto') RETURNING 1"
    )
    rows(dsn, insert, (obs_id, k4))
    with pytest.raises(psycopg.errors.UniqueViolation):
        rows(dsn, insert, (obs_id, k4))
    with pytest.raises(psycopg.errors.CheckViolation):  # an accepted match needs a card
        rows(
            dsn,
            "INSERT INTO fct_card_matches (stg_observation_id, card_key, match_method, status) "
            "VALUES (%s, NULL, 'fuzzy', 'auto') RETURNING 1",
            (add_obs(dsn, ref="base1-9"),),
        )


def test_failure_is_tracked_with_a_sanitized_message(dsn, monkeypatch):
    add_obs(dsn)

    def boom(_dsn=None):
        raise RuntimeError("catalog unavailable password=hunter2")

    monkeypatch.setattr(er, "load_card_index", boom)
    with pytest.raises(RuntimeError):
        resolve(dsn)

    status, rows_read, rows_written, finished_at, error, _ = rows(
        dsn,
        "SELECT status, rows_read, rows_written, finished_at, error_message, metadata "
        "FROM data_pipeline_runs",
    )[0]
    assert status == "failed" and finished_at is not None
    assert (rows_read, rows_written) == (0, 0)
    assert "catalog unavailable" in error and "hunter2" not in error
    assert count(dsn, "fct_card_matches") == 0
