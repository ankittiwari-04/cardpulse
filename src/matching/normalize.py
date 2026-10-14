"""Reusable normalizers for entity resolution.

Rule: unknown or unparseable input stays unknown (None). Nothing is guessed.
"""

import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

_PUNCT = re.compile(r"[\W_]+")
_SPACES = re.compile(r"\s+")


def _fold(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).casefold()


def _normalize_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = _fold(value)
    for apostrophe in ("'", "\u2019", "`"):
        text = text.replace(apostrophe, "")
    text = text.replace("\u2640", " female ").replace("\u2642", " male ").replace("&", " and ")
    text = _SPACES.sub(" ", _PUNCT.sub(" ", text)).strip()
    return text or None


# ---------- names ----------
def normalize_card_name(value: Any) -> str | None:
    """'Pokémon' / 'Pokemon' / 'POKEMON' -> 'pokemon'. Accents, case, punctuation removed."""
    return _normalize_text(value)


_TITLE_NOISE = frozenset({"tcg", "ccg", "card", "holo", "holofoil", "foil"})


def normalize_match_name(value: Any) -> str | None:
    """Name used for similarity: normalized, minus a few marketplace filler words."""
    text = _normalize_text(value)
    if text is None:
        return None
    tokens = [t for t in text.split() if t not in _TITLE_NOISE]
    return " ".join(tokens) or None


# ---------- sets ----------
def normalize_set_name(value: Any) -> str | None:
    """'Base Set' / 'Base' -> 'base'. 'Base Set 2' stays 'base set 2' (a different set)."""
    text = _normalize_text(value)
    if text is None:
        return None
    tokens = text.split()
    if len(tokens) > 1 and tokens[-1] == "set":
        tokens = tokens[:-1]
    return " ".join(tokens)


# ---------- card numbers ----------
_NUMBER_PREFIX = re.compile(r"^(?:no\.?|num\.?|number|card|#)\s*", re.IGNORECASE)
_NUMBER_PARTS = re.compile(r"^([A-Z]*)0*(\d+)([A-Z]*)$")


def normalize_card_number(value: Any) -> str | None:
    """'4/102' / 'No. 4' / '#4' / '004' -> '4'. 'TG01/TG30' -> 'TG1'. No digits -> None."""
    if value is None:
        return None
    text = str(value).strip().split("/", 1)[0].strip()
    for _ in range(2):
        text = _NUMBER_PREFIX.sub("", text).strip()
    text = re.sub(r"[\s\-_.#]+", "", text).upper()
    match = _NUMBER_PARTS.match(text)
    if not match:
        return None
    prefix, digits, suffix = match.groups()
    return f"{prefix}{int(digits)}{suffix}"


# ---------- language ----------
_LANGUAGES = {
    "en": "en", "eng": "en", "english": "en",
    "ja": "ja", "jp": "ja", "jpn": "ja", "japanese": "ja",
    "de": "de", "ger": "de", "deu": "de", "german": "de",
    "fr": "fr", "fra": "fr", "fre": "fr", "french": "fr",
    "it": "it", "ita": "it", "italian": "it",
    "es": "es", "spa": "es", "spanish": "es",
    "pt": "pt", "por": "pt", "portuguese": "pt",
    "ko": "ko", "kor": "ko", "korean": "ko",
    "zh": "zh", "chi": "zh", "chinese": "zh",
}  # fmt: skip


def normalize_language(value: Any) -> str | None:
    """'English' / 'ENG' / 'EN' -> 'en'. Anything not in the table stays unknown (None)."""
    text = _normalize_text(value)
    return _LANGUAGES.get(text) if text else None


# ---------- edition ----------
_FIRST_EDITION = re.compile(r"\b(?:1st|first)\s*(?:ed|edition)\b|\b1ed\b|\b1e\b")
_SHADOWLESS = re.compile(r"\bshadowless\b")
_UNLIMITED = re.compile(r"\bunlimited\b|\bunlim\b")


def normalize_edition(value: Any) -> str | None:
    """'1st Edition' / '1st Ed' / '1ED' -> '1st_edition'; also 'shadowless', 'unlimited'.

    Precedence when a label mentions several: 1st_edition, shadowless, unlimited.
    """
    text = _normalize_text(value)
    if text is None:
        return None
    if _FIRST_EDITION.search(text):
        return "1st_edition"
    if _SHADOWLESS.search(text):
        return "shadowless"
    if _UNLIMITED.search(text):
        return "unlimited"
    return None


# ---------- grading ----------
_COMPANIES = {
    "psa": "PSA",
    "bgs": "BGS",
    "beckett": "BGS",
    "beckett grading": "BGS",
    "beckett grading services": "BGS",
    "cgc": "CGC",
    "sgc": "SGC",
    "ace": "ACE",
    "tag": "TAG",
    "ags": "AGS",
}


def normalize_grading_company(value: Any) -> str | None:
    """'psa' -> 'PSA', 'Beckett' -> 'BGS'. Unknown companies stay None."""
    text = _normalize_text(value)
    return _COMPANIES.get(text) if text else None


@dataclass(frozen=True)
class GradeLabel:
    company: str
    grade: Decimal

    @property
    def grade_text(self) -> str:
        return f"{self.grade.normalize():f}"

    @property
    def label(self) -> str:
        return f"{self.company} {self.grade_text}"

    def as_dict(self) -> dict[str, str]:
        return {"company": self.company, "grade": self.grade_text}


_GRADE_RE = re.compile(
    r"^\s*(?P<company>[A-Za-z][A-Za-z \-_]*?)[\s\-_]*(?P<grade>\d{1,2}(?:\.\d)?)(?!\d|\.\d)"
)


def normalize_grade_label(value: Any) -> GradeLabel | None:
    """'PSA 10' / 'PSA10' / 'psa-10' / 'BGS 9.5 GEM MINT' -> GradeLabel.

    Returns None unless BOTH a known company and a valid grade (1-10, half steps)
    are present in the label. A grade is never attached to a guessed company.
    """
    if not isinstance(value, str):
        return None
    match = _GRADE_RE.match(value)
    if not match:
        return None
    company = normalize_grading_company(match.group("company"))
    if company is None:
        return None
    try:
        grade = Decimal(match.group("grade"))
    except InvalidOperation:
        return None
    if grade < 1 or grade > 10 or (grade * 2) % 1 != 0:
        return None
    return GradeLabel(company=company, grade=grade)
