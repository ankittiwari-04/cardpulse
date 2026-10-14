import pytest

from src.matching.entity_resolution import (
    CardIndex,
    MatchConfig,
    Observation,
    build_card_ref,
    resolve_observation,
)


def make_index(extra=()):
    rows = [
        (1, "base1-4", "Charizard", "base1", "Base", "4"),
        (2, "base1-5", "Clefairy", "base1", "Base", "5"),
        (3, "base1-2", "Blastoise", "base1", "Base", "2"),
        (4, "base4-4", "Charizard", "base4", "Base Set 2", "4"),
        (5, "base2-1", "Clefable", "base2", "Jungle", "1"),
        *extra,
    ]
    return CardIndex(build_card_ref(*row) for row in rows)


def obs(**overrides) -> Observation:
    base = {
        "id": 1,
        "source": "marketplace_x",
        "source_card_ref": "listing-1",
        "raw_card_name": None,
        "raw_set_name": None,
        "raw_card_number": None,
        "raw_grade_label": None,
        "language_label": None,
        "edition_label": None,
        "grading_company": None,
    }
    base.update(overrides)
    return Observation(**base)


def test_deterministic_match_on_source_reference():
    d = resolve_observation(obs(source="pokemontcg", source_card_ref="base1-4"), make_index())
    assert (d.method, d.status, d.score, d.card_id) == ("deterministic", "auto", 100.0, "base1-4")
    assert d.category == "deterministic" and d.card_key == 1
    assert d.explanation["rule"] == "source_reference"


def test_unknown_reference_for_authoritative_source_goes_to_review():
    d = resolve_observation(
        obs(
            source="pokemontcg",
            source_card_ref="base1-999",
            raw_card_name="Charizard",
            raw_set_name="Base",
            raw_card_number="4",
        ),
        make_index(),
    )
    assert d.status == "review" and d.card_key is None and d.method == "none"
    assert d.rule == "source_reference_not_in_catalog"
    assert d.explanation["candidates"][0]["card_id"] == "base1-4"


def test_exact_name_set_number_is_high_confidence():
    d = resolve_observation(
        obs(raw_card_name="Charizard", raw_set_name="Base Set", raw_card_number="4/102"),
        make_index(),
    )
    assert (d.method, d.status, d.card_id, d.score) == ("fuzzy", "auto", "base1-4", 95.0)
    assert d.rule == "exact_name_set_number" and d.category == "fuzzy"
    assert d.explanation["components"]["formula"] == "50 + 0.45 * name_similarity"


def test_fuzzy_name_with_exact_set_and_number_is_medium_confidence():
    d = resolve_observation(
        obs(raw_card_name="Charizrd", raw_set_name="Base", raw_card_number="#4"), make_index()
    )
    assert d.status == "auto" and d.card_id == "base1-4"
    assert d.rule == "fuzzy_name_exact_set_number"
    assert 90.0 <= d.score < 95.0


def test_threshold_is_configurable():
    strict = MatchConfig(auto_threshold=99.0)
    d = resolve_observation(
        obs(raw_card_name="Charizrd", raw_set_name="Base", raw_card_number="#4"),
        make_index(),
        strict,
    )
    assert d.status == "review" and d.card_key is None
    assert d.explanation["candidates"][0]["card_id"] == "base1-4"  # still shown for review


def test_conflicting_card_number_is_never_matched_on_name_alone():
    d = resolve_observation(
        obs(raw_card_name="Charizard", raw_set_name="Base", raw_card_number="5"), make_index()
    )
    assert d.status == "review" and d.card_key is None
    assert d.score is not None and d.score < 90.0
    excluded = {e["card_id"]: e["reason"] for e in d.explanation["excluded"]}
    assert excluded["base1-4"] == "number_conflict"


def test_conflicting_set_is_never_matched_on_name_alone():
    d = resolve_observation(
        obs(raw_card_name="Charizard", raw_set_name="Jungle", raw_card_number="4"), make_index()
    )
    assert d.status == "review" and d.card_key is None
    assert d.rule == "no_candidates" and d.category == "unmatched"
    excluded = {e["card_id"]: e["reason"] for e in d.explanation["excluded"]}
    assert excluded["base1-4"] == "set_conflict"


def test_base_set_2_is_not_confused_with_base_set():
    d = resolve_observation(
        obs(raw_card_name="Charizard", raw_set_name="Base Set 2", raw_card_number="4"),
        make_index(),
    )
    assert d.status == "auto" and d.card_id == "base4-4"


def test_name_only_is_low_confidence_review():
    d = resolve_observation(obs(raw_card_name="Charizard"), make_index())
    assert d.status == "review" and d.method == "none" and d.card_key is None
    assert d.rule == "name_only" and d.score == 60.0 and d.category == "review"


def test_unknown_card_is_unmatched_review():
    d = resolve_observation(obs(raw_card_name="Zubat Deluxe Collector Edition"), make_index())
    assert d.status == "review" and d.category == "unmatched" and d.score is None
    assert resolve_observation(obs(), make_index()).rule == "no_candidates"


def test_ambiguous_candidates_go_to_review():
    index = make_index(
        extra=[
            (6, "dup-a", "Alakazam", "base1", "Base", "1"),
            (7, "dup-b", "Alakazam", "base1", "Base", "1"),
        ]
    )
    d = resolve_observation(
        obs(raw_card_name="Alakazam", raw_set_name="Base", raw_card_number="1"), index
    )
    assert d.status == "review" and d.rule == "ambiguous" and d.card_key is None


def test_normalized_attributes_are_recorded_and_unknowns_stay_unknown():
    d = resolve_observation(
        obs(
            raw_card_name="Charizard",
            raw_set_name="Base",
            raw_card_number="4",
            raw_grade_label="PSA10",
            language_label="ENG",
            edition_label="1st Ed",
            grading_company="psa",
        ),
        make_index(),
    )
    seen = d.explanation["observation"]
    assert seen["language"] == "en" and seen["edition"] == "1st_edition"
    assert seen["grading_company"] == "PSA"
    assert seen["grade"] == {"company": "PSA", "grade": "10"}

    unknown = resolve_observation(
        obs(
            raw_card_name="Charizard",
            raw_set_name="Base",
            raw_card_number="4",
            language_label="Klingon",
            edition_label="mystery",
            raw_grade_label="Gem Mint",
        ),
        make_index(),
    ).explanation["observation"]
    assert unknown["language"] is None and unknown["edition"] is None
    assert unknown["grade"] is None and unknown["grading_company"] is None


def test_config_validation_in_settings(monkeypatch):
    from pydantic import ValidationError

    from src.config import Settings

    monkeypatch.setenv("MATCH_AUTO_THRESHOLD", "40")
    monkeypatch.setenv("MATCH_REVIEW_MIN_SCORE", "60")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
