from decimal import Decimal

import pytest

from src.matching.normalize import (
    normalize_card_name,
    normalize_card_number,
    normalize_edition,
    normalize_grade_label,
    normalize_grading_company,
    normalize_language,
    normalize_match_name,
    normalize_set_name,
)


def test_card_name_variants_collapse():
    assert {normalize_card_name(v) for v in ("Pokémon", "Pokemon", "POKEMON")} == {"pokemon"}
    assert normalize_card_name("Farfetch'd") == "farfetchd"
    assert normalize_card_name("Mr. Mime") == "mr mime"
    assert normalize_card_name("Nidoran♀") == "nidoran female"
    assert normalize_card_name("  ") is None
    assert normalize_card_name(None) is None


def test_match_name_drops_only_marketplace_filler():
    assert normalize_match_name("Charizard Holo") == "charizard"
    assert normalize_match_name("Rare Candy") == "rare candy"
    assert normalize_match_name("Holo") is None


def test_set_names():
    assert normalize_set_name("Base Set") == "base"
    assert normalize_set_name("Base") == "base"
    assert normalize_set_name("BASE SET") == "base"
    assert normalize_set_name("Base Set 2") == "base set 2"
    assert normalize_set_name("Base Set 2") != normalize_set_name("Base Set")
    assert normalize_set_name("Sun & Moon") == "sun and moon"
    assert normalize_set_name(None) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("4/102", "4"),
        ("No. 4", "4"),
        ("#4", "4"),
        ("004", "4"),
        (" 004 / 102 ", "4"),
        (4, "4"),
        ("TG01/TG30", "TG1"),
        ("SWSH001", "SWSH1"),
        ("10", "10"),
        ("", None),
        ("abc", None),
        (None, None),
    ],
)
def test_card_numbers(raw, expected):
    assert normalize_card_number(raw) == expected


@pytest.mark.parametrize("raw", ["English", "ENG", "EN", "en"])
def test_language_english(raw):
    assert normalize_language(raw) == "en"


def test_language_unknown_stays_unknown():
    assert normalize_language("Japanese") == "ja"
    assert normalize_language("Klingon") is None
    assert normalize_language(None) is None


@pytest.mark.parametrize("raw", ["1st Edition", "1st Ed", "1ED", "1st ed.", "First Edition"])
def test_first_edition_variants(raw):
    assert normalize_edition(raw) == "1st_edition"


def test_other_editions_and_unknown():
    assert normalize_edition("Unlimited") == "unlimited"
    assert normalize_edition("Shadowless") == "shadowless"
    assert normalize_edition("Limited") is None
    assert normalize_edition("whatever") is None
    assert normalize_edition(None) is None


def test_grading_company():
    assert normalize_grading_company("psa") == "PSA"
    assert normalize_grading_company("Beckett") == "BGS"
    assert normalize_grading_company("CGC") == "CGC"
    assert normalize_grading_company("Acme Grading") is None


@pytest.mark.parametrize(
    ("raw", "company", "grade"),
    [
        ("PSA 10", "PSA", "10"),
        ("PSA10", "PSA", "10"),
        ("psa-10", "PSA", "10"),
        ("BGS 9.5", "BGS", "9.5"),
        ("BGS 9.5 GEM MINT", "BGS", "9.5"),
        ("Beckett 9", "BGS", "9"),
    ],
)
def test_grade_labels(raw, company, grade):
    result = normalize_grade_label(raw)
    assert result.company == company
    assert result.grade == Decimal(grade)
    assert result.as_dict() == {"company": company, "grade": grade}


@pytest.mark.parametrize(
    "raw", ["PSA 11", "PSA 9.3", "PSA 9.55", "RAW", "Gem Mint 10", "XYZ 10", "", None]
)
def test_grade_labels_never_guess(raw):
    assert normalize_grade_label(raw) is None
