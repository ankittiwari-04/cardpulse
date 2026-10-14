"""Entity resolution: link each staged price observation to a canonical card (dim_cards).

Stage 1 (deterministic): for sources whose source_card_ref IS the canonical card id
    (DETERMINISTIC_SOURCES), look the reference up directly. Score 100.
Stage 2 (attribute matching): normalize name/set/number, generate candidates that
    satisfy every known hard constraint (same set, same number), score them with a
    transparent formula, and accept automatically only above the threshold.

Anything not accepted becomes a review row (card_key NULL); the best candidates and the
near-misses excluded by set/number conflicts are kept in the explanation JSON.
One fct_card_matches row exists per observation; confirmed/rejected rows are never touched.
"""

import logging
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.types.json import Jsonb
from rapidfuzz import fuzz, process

from src.config import Settings, get_settings
from src.db.connection import transaction
from src.ingestion.catalog import _secrets
from src.ingestion.runs import finish_run, sanitize_error, start_run
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

logger = logging.getLogger(__name__)

PIPELINE_NAME = "entity_resolution"

# Sources whose source_card_ref equals dim_cards.card_id (authoritative references).
DETERMINISTIC_SOURCES = frozenset({"pokemontcg"})

FORMULAS = {
    "set_and_number": "50 + 0.45 * name_similarity",
    "partial_constraints": "30 + 0.40 * name_similarity",
    "name_only": "0.60 * name_similarity",
}


@dataclass(frozen=True)
class MatchConfig:
    auto_threshold: float = 90.0
    review_min_score: float = 50.0
    ambiguity_margin: float = 2.0

    @classmethod
    def from_settings(cls, settings: Settings) -> "MatchConfig":
        return cls(
            auto_threshold=settings.match_auto_threshold,
            review_min_score=settings.match_review_min_score,
        )


# ---------- catalog index ----------
@dataclass(frozen=True)
class CardRef:
    card_key: int
    card_id: str
    name: str | None
    set_name: str | None
    card_number: str | None
    name_cmp: str | None
    set_norm: str | None
    number_norm: str | None


def build_card_ref(
    card_key: int,
    card_id: str,
    name: str | None,
    set_id: str | None,
    set_name: str | None,
    card_number: str | None,
) -> CardRef:
    return CardRef(
        card_key=card_key,
        card_id=card_id,
        name=name,
        set_name=set_name,
        card_number=card_number,
        name_cmp=normalize_match_name(name),
        set_norm=normalize_set_name(set_name),
        number_norm=normalize_card_number(card_number),
    )


class CardIndex:
    """In-memory view of dim_cards with the lookups candidate generation needs."""

    def __init__(self, cards: Iterable[CardRef]) -> None:
        self.cards = list(cards)
        self.by_card_id = {c.card_id: c for c in self.cards}
        self.by_set_number: dict[tuple[str, str], list[CardRef]] = defaultdict(list)
        self.by_set: dict[str, list[CardRef]] = defaultdict(list)
        self.by_number: dict[str, list[CardRef]] = defaultdict(list)
        for card in self.cards:
            if card.set_norm and card.number_norm:
                self.by_set_number[(card.set_norm, card.number_norm)].append(card)
            if card.set_norm:
                self.by_set[card.set_norm].append(card)
            if card.number_norm:
                self.by_number[card.number_norm].append(card)
        self._named = [c for c in self.cards if c.name_cmp]
        self._names = [c.name_cmp for c in self._named]

    def pool(self, set_norm: str | None, number_norm: str | None) -> list[CardRef]:
        """Cards satisfying every KNOWN hard constraint (empty if no constraint is known)."""
        if set_norm is not None and number_norm is not None:
            return self.by_set_number.get((set_norm, number_norm), [])
        if set_norm is not None:
            return self.by_set.get(set_norm, [])
        if number_norm is not None:
            return self.by_number.get(number_norm, [])
        return []

    def nearest_by_name(
        self, name_cmp: str, *, limit: int, cutoff: float
    ) -> list[tuple[CardRef, float]]:
        if not self._names:
            return []
        found = process.extract(
            name_cmp, self._names, scorer=fuzz.token_sort_ratio, limit=limit, score_cutoff=cutoff
        )
        return [(self._named[index], float(score)) for _choice, score, index in found]


def load_card_index(dsn: str | None = None) -> CardIndex:
    with transaction(dsn, read_only=True) as conn:
        rows = conn.execute(
            "SELECT card_key, card_id, name, set_id, set_name, card_number FROM dim_cards"
        ).fetchall()
    return CardIndex(build_card_ref(*row) for row in rows)


# ---------- observations and decisions ----------
@dataclass(frozen=True)
class Observation:
    id: int
    source: str
    source_card_ref: str | None
    raw_card_name: str | None
    raw_set_name: str | None
    raw_card_number: str | None
    raw_grade_label: str | None
    language_label: str | None
    edition_label: str | None
    grading_company: str | None


@dataclass(frozen=True)
class MatchDecision:
    observation_id: int
    card_key: int | None
    card_id: str | None
    method: str  # deterministic | fuzzy | none
    status: str  # auto | review
    score: float | None
    rule: str
    category: str  # deterministic | fuzzy | review | unmatched
    explanation: dict[str, Any]


@dataclass(frozen=True)
class _Attrs:
    name_cmp: str | None
    set: str | None
    number: str | None
    public: dict[str, Any]


@dataclass(frozen=True)
class _Candidate:
    card: CardRef
    score: float
    rule: str
    name_similarity: float | None
    exact_name: bool
    set_state: str
    number_state: str


def _attrs(obs: Observation) -> _Attrs:
    grade = normalize_grade_label(obs.raw_grade_label)
    name_cmp = normalize_match_name(obs.raw_card_name)
    set_norm = normalize_set_name(obs.raw_set_name)
    number = normalize_card_number(obs.raw_card_number)
    # Language, edition and grade describe the physical item, not the card design, so
    # they are normalized and recorded but never used to pick the card.
    public = {
        "name": normalize_card_name(obs.raw_card_name),
        "set": set_norm,
        "number": number,
        "language": normalize_language(obs.language_label),
        "edition": normalize_edition(obs.edition_label),
        "grading_company": normalize_grading_company(obs.grading_company),
        "grade": grade.as_dict() if grade else None,
    }
    return _Attrs(name_cmp=name_cmp, set=set_norm, number=number, public=public)


def _score_candidate(card: CardRef, attrs: _Attrs) -> _Candidate:
    similarity: float | None = None
    if attrs.name_cmp and card.name_cmp:
        similarity = float(fuzz.token_sort_ratio(attrs.name_cmp, card.name_cmp))
    n = similarity or 0.0
    set_known = attrs.set is not None
    number_known = attrs.number is not None
    exact_name = attrs.name_cmp is not None and attrs.name_cmp == card.name_cmp
    if set_known and number_known:
        score = 50.0 + 0.45 * n
        if exact_name:
            rule = "exact_name_set_number"
        elif similarity is None:
            rule = "set_number_without_name"
        else:
            rule = "fuzzy_name_exact_set_number"
    elif set_known or number_known:
        score = 30.0 + 0.40 * n
        rule = "partial_constraints"
    else:
        score = 0.60 * n
        rule = "name_only"
    return _Candidate(
        card=card,
        score=round(score, 2),
        rule=rule,
        name_similarity=None if similarity is None else round(similarity, 2),
        exact_name=exact_name,
        set_state="exact" if set_known else "unknown",
        number_state="exact" if number_known else "unknown",
    )


def _generate_candidates(attrs: _Attrs, index: CardIndex, config: MatchConfig) -> list[_Candidate]:
    if attrs.set is None and attrs.number is None:
        if attrs.name_cmp is None:
            return []
        cutoff = config.review_min_score / 0.60  # below this a name-only match cannot qualify
        near = index.nearest_by_name(attrs.name_cmp, limit=5, cutoff=cutoff)
        pool = [card for card, _ in near]
    else:
        pool = index.pool(attrs.set, attrs.number)
    scored = [_score_candidate(card, attrs) for card in pool]
    return sorted(scored, key=lambda c: (-c.score, c.card.card_id))


def _near_misses(attrs: _Attrs, index: CardIndex, pool_ids: set[str]) -> list[dict[str, Any]]:
    """Similarly named cards that were EXCLUDED because their set or number conflicts."""
    if attrs.name_cmp is None:
        return []
    out: list[dict[str, Any]] = []
    for card, similarity in index.nearest_by_name(attrs.name_cmp, limit=15, cutoff=70.0):
        if card.card_id in pool_ids:
            continue
        set_conflict = attrs.set is not None and card.set_norm != attrs.set
        number_conflict = attrs.number is not None and card.number_norm != attrs.number
        if set_conflict and number_conflict:
            reason = "set_and_number_conflict"
        elif set_conflict:
            reason = "set_conflict"
        elif number_conflict:
            reason = "number_conflict"
        else:
            continue
        out.append(
            {
                "card_id": card.card_id,
                "name_similarity": round(similarity, 2),
                "reason": reason,
            }
        )
        if len(out) >= 3:
            break
    return out


def _explanation(
    rule: str,
    attrs: _Attrs,
    config: MatchConfig,
    viable: list[_Candidate],
    *,
    excluded: list[dict[str, Any]] | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        "rule": rule,
        "observation": attrs.public,
        "thresholds": {
            "auto": config.auto_threshold,
            "review_min": config.review_min_score,
        },
    }
    if viable:
        best = viable[0]
        if best.set_state == "exact" and best.number_state == "exact":
            formula = FORMULAS["set_and_number"]
        elif best.set_state == "exact" or best.number_state == "exact":
            formula = FORMULAS["partial_constraints"]
        else:
            formula = FORMULAS["name_only"]
        data["components"] = {
            "name_similarity": best.name_similarity,
            "name_exact": best.exact_name,
            "set": best.set_state,
            "number": best.number_state,
            "formula": formula,
        }
        data["candidates"] = [
            {
                "card_id": c.card.card_id,
                "score": c.score,
                "rule": c.rule,
                "name_similarity": c.name_similarity,
            }
            for c in viable[:3]
        ]
    if excluded:
        data["excluded"] = excluded
    if note:
        data["note"] = note
    return data


def _review(
    obs: Observation,
    attrs: _Attrs,
    index: CardIndex,
    config: MatchConfig,
    candidates: list[_Candidate],
    viable: list[_Candidate],
    rule: str,
    note: str | None = None,
) -> MatchDecision:
    pool_ids = {c.card.card_id for c in candidates}
    excluded = _near_misses(attrs, index, pool_ids)
    best = viable[0] if viable else None
    return MatchDecision(
        observation_id=obs.id,
        card_key=None,
        card_id=None,
        method="none",
        status="review",
        score=best.score if best else None,
        rule=rule,
        category="review" if best else "unmatched",
        explanation=_explanation(rule, attrs, config, viable, excluded=excluded, note=note),
    )


def resolve_observation(
    obs: Observation, index: CardIndex, config: MatchConfig | None = None
) -> MatchDecision:
    config = config or MatchConfig()
    attrs = _attrs(obs)

    # Stage 1: the source's own reference is the canonical card id.
    if obs.source in DETERMINISTIC_SOURCES and obs.source_card_ref:
        card = index.by_card_id.get(obs.source_card_ref)
        if card is not None:
            return MatchDecision(
                observation_id=obs.id,
                card_key=card.card_key,
                card_id=card.card_id,
                method="deterministic",
                status="auto",
                score=100.0,
                rule="source_reference",
                category="deterministic",
                explanation={
                    "rule": "source_reference",
                    "reference": obs.source_card_ref,
                    "basis": "source_card_ref equals dim_cards.card_id for this source",
                    "observation": attrs.public,
                },
            )

    # Stage 2: attribute matching under hard set/number constraints.
    candidates = _generate_candidates(attrs, index, config)
    viable = [c for c in candidates if c.score >= config.review_min_score]
    if not viable:
        return _review(obs, attrs, index, config, candidates, viable, "no_candidates")

    best = viable[0]
    if best.score < config.auto_threshold:
        return _review(obs, attrs, index, config, candidates, viable, best.rule)
    if len(viable) > 1 and best.score - viable[1].score < config.ambiguity_margin:
        note = "two or more candidates scored within the ambiguity margin"
        return _review(obs, attrs, index, config, candidates, viable, "ambiguous", note)
    if obs.source in DETERMINISTIC_SOURCES:
        note = "source reference is not in dim_cards; refusing to override it by attributes"
        return _review(
            obs, attrs, index, config, candidates, viable, "source_reference_not_in_catalog", note
        )

    return MatchDecision(
        observation_id=obs.id,
        card_key=best.card.card_key,
        card_id=best.card.card_id,
        method="fuzzy",
        status="auto",
        score=best.score,
        rule=best.rule,
        category="fuzzy",
        explanation=_explanation(best.rule, attrs, config, viable),
    )


# ---------- persistence ----------
_FETCH_SQL = """
SELECT s.id, s.source, s.source_card_ref, s.raw_card_name, s.raw_set_name,
       s.raw_card_number, s.raw_grade_label, s.language_label, s.edition_label,
       s.grading_company
FROM stg_price_observations s
LEFT JOIN fct_card_matches m ON m.stg_observation_id = s.id
WHERE s.id > %(after_id)s
  AND (m.stg_observation_id IS NULL OR (%(reprocess)s AND m.status = 'review'))
  AND (%(source)s::text IS NULL OR s.source = %(source)s)
ORDER BY s.id
LIMIT %(n)s
"""

# Only 'review' rows may ever be updated; auto, confirmed and rejected rows are immutable here.
_WRITE_SQL = """
INSERT INTO fct_card_matches (
    stg_observation_id, card_key, match_method, match_score, status, explanation, run_id
)
VALUES (
    %(observation_id)s, %(card_key)s, %(method)s, %(score)s, %(status)s,
    %(explanation)s, %(run_id)s
)
ON CONFLICT (stg_observation_id) DO UPDATE SET
    card_key = EXCLUDED.card_key,
    match_method = EXCLUDED.match_method,
    match_score = EXCLUDED.match_score,
    status = EXCLUDED.status,
    explanation = EXCLUDED.explanation,
    run_id = EXCLUDED.run_id,
    matched_at = now()
WHERE fct_card_matches.status = 'review'
  AND (fct_card_matches.card_key, fct_card_matches.match_method,
       fct_card_matches.match_score, fct_card_matches.status, fct_card_matches.explanation)
      IS DISTINCT FROM
      (EXCLUDED.card_key, EXCLUDED.match_method,
       EXCLUDED.match_score, EXCLUDED.status, EXCLUDED.explanation)
RETURNING (xmax = 0) AS inserted
"""


def fetch_observations(
    conn: psycopg.Connection, after_id: int, limit: int, source: str | None, reprocess: bool
) -> list[Observation]:
    rows = conn.execute(
        _FETCH_SQL,
        {"after_id": after_id, "n": limit, "source": source, "reprocess": reprocess},
    ).fetchall()
    return [Observation(*row) for row in rows]


def write_decisions(
    conn: psycopg.Connection, decisions: list[MatchDecision], run_id: int | None
) -> tuple[int, int, int]:
    """Upsert decisions. Returns (inserted, updated, unchanged)."""
    inserted = updated = unchanged = 0
    for d in decisions:
        score = None if d.score is None else Decimal(str(d.score)).quantize(Decimal("0.01"))
        row = conn.execute(
            _WRITE_SQL,
            {
                "observation_id": d.observation_id,
                "card_key": d.card_key,
                "method": d.method,
                "score": score,
                "status": d.status,
                "explanation": Jsonb(d.explanation),
                "run_id": run_id,
            },
        ).fetchone()
        if row is None:
            unchanged += 1  # existing row left alone (same result, or not a review row)
        elif row[0]:
            inserted += 1
        else:
            updated += 1
    return inserted, updated, unchanged


# ---------- run ----------
@dataclass
class ResolveStats:
    observations_processed: int = 0
    deterministic_matches: int = 0
    fuzzy_matches: int = 0
    review_count: int = 0
    unmatched_count: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0

    @property
    def rows_written(self) -> int:
        return self.rows_inserted + self.rows_updated

    def add_batch(self, decisions: list[MatchDecision], counts: tuple[int, int, int]) -> None:
        self.observations_processed += len(decisions)
        for d in decisions:
            if d.category == "deterministic":
                self.deterministic_matches += 1
            elif d.category == "fuzzy":
                self.fuzzy_matches += 1
            elif d.category == "review":
                self.review_count += 1
            else:
                self.unmatched_count += 1
        self.rows_inserted += counts[0]
        self.rows_updated += counts[1]
        self.rows_unchanged += counts[2]

    def as_metadata(self) -> dict[str, int]:
        return {
            "observations_processed": self.observations_processed,
            "deterministic_matches": self.deterministic_matches,
            "fuzzy_matches": self.fuzzy_matches,
            "review_count": self.review_count,
            "unmatched_count": self.unmatched_count,
            "rows_written": self.rows_written,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_unchanged": self.rows_unchanged,
        }


@dataclass
class ResolveResult:
    run_id: int | None
    stats: ResolveStats
    dry_run: bool


def _record_failure(
    run_id: int, stats: ResolveStats, exc: BaseException, settings: Settings, dsn: str
) -> None:
    """Failure bookkeeping in its own transaction so it survives any batch rollback."""
    try:
        finish_run(
            run_id,
            "failed",
            stats.observations_processed,
            stats.rows_written,
            stats.as_metadata(),
            error_message=sanitize_error(exc, _secrets(settings)),
            dsn=dsn,
        )
    except Exception as tracking_exc:  # never mask the original error
        logger.error(
            "Could not record failure for run %s: %s",
            run_id,
            sanitize_error(tracking_exc, _secrets(settings)),
        )


def resolve_observations(
    settings: Settings | None = None,
    *,
    limit: int | None = 1000,
    source: str | None = None,
    dry_run: bool = False,
    reprocess: bool = False,
    batch_size: int = 500,
    dsn: str | None = None,
    config: MatchConfig | None = None,
) -> ResolveResult:
    """limit=None means all. One transaction (fetch + write) per batch, committed per batch."""
    settings = settings or get_settings()
    dsn = dsn or settings.psycopg_dsn
    config = config or MatchConfig.from_settings(settings)

    run_id: int | None = None
    if not dry_run:
        run_id = start_run(
            PIPELINE_NAME,
            {"source": source, "limit": limit, "reprocess": reprocess},
            dsn,
        )
        logger.info("Started run %s (%s)", run_id, PIPELINE_NAME)

    stats = ResolveStats()
    try:
        index = load_card_index(dsn)
        after_id = 0
        remaining = limit
        while remaining is None or remaining > 0:
            size = batch_size if remaining is None else min(batch_size, remaining)
            with transaction(dsn, read_only=dry_run) as conn:
                observations = fetch_observations(conn, after_id, size, source, reprocess)
                if not observations:
                    break
                decisions = [resolve_observation(o, index, config) for o in observations]
                counts = (0, 0, 0) if dry_run else write_decisions(conn, decisions, run_id)
            stats.add_batch(decisions, counts)  # counted only after the batch committed
            after_id = observations[-1].id
            if remaining is not None:
                remaining -= len(observations)
            logger.info("Processed batch of %s observations", len(observations))
    except BaseException as exc:
        if run_id is not None:
            _record_failure(run_id, stats, exc, settings, dsn)
        raise

    if run_id is not None:
        finish_run(
            run_id,
            "succeeded",
            stats.observations_processed,
            stats.rows_written,
            {
                **stats.as_metadata(),
                "auto_threshold": config.auto_threshold,
                "review_min_score": config.review_min_score,
            },
            dsn=dsn,
        )
    return ResolveResult(run_id=run_id, stats=stats, dry_run=dry_run)
