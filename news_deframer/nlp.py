"""Language-specific helpers for extracting lexical stems."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Optional, Sequence
from bs4 import BeautifulSoup

from news_deframer.config import Config
from news_deframer.spacy_models import (
    get_spacy_model,
    get_spacy_model_name as _registry_spacy_model_name,
)

try:  # pragma: no cover - optional dependency
    import spacy
except Exception:  # pragma: no cover - optional dependency
    spacy = None  # type: ignore[assignment]

if TYPE_CHECKING:  # pragma: no cover - typing only
    from spacy.language import Language as SpacyLanguage
else:  # pragma: no cover - runtime fallback
    SpacyLanguage = Any  # type: ignore[misc,assignment]


def sanitize_text(value: Optional[str]) -> Optional[str]:
    """Strip HTML tags from text using BeautifulSoup."""

    if value is None:
        return None

    text = BeautifulSoup(value, "html.parser").get_text()
    if "<" in text and ">" in text:
        text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)

    return text.strip()


def stem_category(
    text: Optional[str],
    language: str,
    stop_words: Optional[Iterable[str]] = None,
    config: Config | None = None,
) -> Optional[str]:
    """Return the lemmatized version of a category string."""
    if not text:
        return None

    normalized = text.strip()
    if not normalized:
        return None

    custom_stops = set(w.lower() for w in (stop_words or []))
    nlp = _get_spacy_model(language, config=config, with_ner=False)
    try:
        doc = nlp(normalized)
    except Exception as exc:
        raise RuntimeError("Failed to process text with spaCy model") from exc

    lemmas = [
        token.lemma_.lower()
        for token in doc
        if token.is_alpha
        and not _is_stop_word(token.lemma_, language)
        and token.lemma_.lower() not in custom_stops
    ]

    return " ".join(lemmas) if lemmas else None


def stem_noun(
    language: str, word: Optional[str], config: Config | None = None
) -> Optional[str]:
    """Return noun/proper-noun lemmas for a single word or phrase."""
    if not word:
        return None

    normalized = word.strip()
    if not normalized:
        return None

    nlp = _get_spacy_model(language, config=config, with_ner=False)
    try:
        doc = nlp(normalized)
    except Exception as exc:
        raise RuntimeError("Failed to process text with spaCy model") from exc

    lemmas = []
    seen: set[str] = set()
    for token in doc:
        if token.pos_ not in {"NOUN", "PROPN"}:
            continue
        if not token.lemma_ or not token.is_alpha:
            continue

        lemma = token.lemma_.lower()
        if lemma in seen:
            continue

        seen.add(lemma)
        lemmas.append(lemma)

    return " ".join(lemmas) if lemmas else None


_STOPWORD_CACHE: dict[str, frozenset[str]] = {}


def _get_language_code(language: str) -> str:
    """Extract the base language code (e.g., 'en-US' -> 'en')."""
    return (language or "").split("-")[0].lower()


def _get_spacy_model_name(language: str) -> str:
    """Retrieve the spaCy model name for a given language code."""
    if spacy is None:
        raise RuntimeError("spaCy is required but not installed")
    return _registry_spacy_model_name(language)


def _get_spacy_model(
    language: str, config: Config | None = None, with_ner: bool = True
) -> SpacyLanguage:
    _get_spacy_model_name(language)
    return get_spacy_model(language, config=config, with_ner=with_ner)


def _get_stopwords(language: str) -> frozenset[str]:
    if spacy is None:
        raise RuntimeError("spaCy is required but not installed")

    lang_code = _get_language_code(language)
    if not lang_code:
        raise RuntimeError("Language code is required for stopword handling")

    cached = _STOPWORD_CACHE.get(lang_code)
    if cached is not None:
        return cached

    util = getattr(spacy, "util", None)
    if util is None:
        raise RuntimeError("spaCy util module unavailable")

    try:
        lang_class = util.get_lang_class(lang_code)
    except (KeyError, AttributeError) as exc:
        raise RuntimeError(
            f"No spaCy stopword list available for language '{language}'"
        ) from exc

    stopwords = frozenset(word.lower() for word in lang_class.Defaults.stop_words)
    _STOPWORD_CACHE[lang_code] = stopwords
    return stopwords


def _is_stop_word(value: str, language: str) -> bool:
    return value.lower() in _get_stopwords(language)


# Thesis: Focus on Persons, Organizations, and Locations (Chapter 12.3.2)
# Mapping spaCy entity labels to Thesis concepts:
# PERSON -> Person (P)
# ORG -> Organization (Group/Company)
# GPE, LOC -> Location (L)
# EVENT -> Event (E)
RELEVANT_ENTITY_LABELS = {"PERSON", "ORG", "GPE", "LOC", "EVENT", "FAC"}


def extract_stems(
    content: str,
    language: str,
    stop_words: Optional[Iterable[str]] = None,
    with_ner: bool = True,
    config: Config | None = None,
) -> tuple[Sequence[str], Sequence[str], Sequence[str]]:
    """
    Return noun, verb, and adjective lemmas using spaCy with NER integration.

    Thesis Alignment:
    1. noun_stems -> Triggers (Entities + Topics) [Def 8.1.2]
    2. verb_stems -> Context (Actions) [Def 8.1.3]
    3. adj_stems  -> Diversificators (Qualities) [Section 7.2.3]
    """
    normalized = content.strip()
    if not normalized:
        return [], [], []

    custom_stops = set(w.lower() for w in (stop_words or []))
    nlp = _get_spacy_model(language, config=config, with_ner=with_ner)

    try:
        doc = nlp(normalized)
    except Exception as exc:
        raise RuntimeError("Failed to process text with spaCy model") from exc

    # --- 1. The Trigger ($T$) ---
    # Thesis Chapter 12.3.2: Use Named Entity Recognition (NER)
    # We prioritize detecting entities (Persons, Orgs, Locs) to capture multi-word
    # triggers like "Lehman Brothers" or "Wall Street" as single units.
    trigger_stems = set()

    # A. Extract Named Entities
    if with_ner:
        if not hasattr(doc, "ents"):
            model_name = _get_spacy_model_name(language)
            raise AttributeError(f"Model '{model_name}' output has no 'ents' attribute")
        for ent in doc.ents:
            if ent.label_ in RELEVANT_ENTITY_LABELS:
                # We use the lemma of the entity (e.g., "Donald Trumps" -> "donald trump")
                if ent.lemma_ and not ent.lemma_.isspace():
                    lemma = ent.lemma_.lower()
                    if lemma not in custom_stops:
                        trigger_stems.add(lemma)

    # B. Extract Common Topics (Nouns)
    # Thesis: Topics ($To$) are also part of Triggers (Def 8.1.2)
    # We collect generic nouns that fall outside of named entities (e.g., "price", "crisis")
    noun_tokens = _collect_sorted_unique_stems(
        doc, {"NOUN", "PROPN"}, language, custom_stops
    )
    trigger_stems.update(noun_tokens)

    # Sort the combined set of Entities and Topics
    noun_stems = sorted(trigger_stems)

    # --- 2. The Context ($C$) ---
    # Thesis: Verbs define the action or relation (Def 8.1.3)
    verb_stems = _collect_sorted_unique_stems(doc, {"VERB"}, language, custom_stops)

    # --- 3. The Diversificator ---
    # Thesis: Adjectives act as satisfiers/disatisfiers (Section 7.2.3)
    adj_stems = _collect_sorted_unique_stems(doc, {"ADJ"}, language, custom_stops)

    return noun_stems, verb_stems, adj_stems


def extract_stems_simple(
    content: str,
    language: str,
    stop_words: Optional[Iterable[str]] = None,
    config: Config | None = None,
) -> tuple[Sequence[str], Sequence[str], Sequence[str]]:
    # TODO: you must implement Named Entity Recognition (NER) as described in Chapter 12.3.2 of the thesis
    # Filter: Only keep meaningful entities (Person, Org, GPE/Location)
    """
    Return noun, verb, and adjective lemmas for ``content`` using spaCy.

    Returns:
        (noun_stems, verb_stems, adj_stems)
    """
    normalized = content.strip()
    if not normalized:
        return [], [], []

    custom_stops = set(w.lower() for w in (stop_words or []))
    nlp = _get_spacy_model(language, config=config, with_ner=False)

    try:
        doc = nlp(normalized)
    except Exception as exc:
        raise RuntimeError("Failed to process text with spaCy model") from exc

    # Thesis: Nouns (Triggers) include common nouns and Proper Nouns (Entities)
    # This doesn't handle NER (Named Entity Recognition e.g. Person Names or Locations)
    noun_stems = _collect_sorted_unique_stems(
        doc, {"NOUN", "PROPN"}, language, custom_stops
    )

    # Thesis: Verbs are 'Diversificators' indicating action
    verb_stems = _collect_sorted_unique_stems(doc, {"VERB"}, language, custom_stops)

    # Thesis: Adjectives are 'Diversificators' indicating sentiment/direction
    adj_stems = _collect_sorted_unique_stems(doc, {"ADJ"}, language, custom_stops)

    return noun_stems, verb_stems, adj_stems


def _collect_sorted_unique_stems(
    tokens: Iterable[Any],
    allowed_pos: set[str],
    language: str,
    custom_stops: Optional[set[str]] = None,
) -> list[str]:
    stems = {
        token.lemma_.lower()
        for token in tokens
        if token.pos_ in allowed_pos  # Strict POS filtering
        and token.lemma_
        and token.is_alpha  # Remove punctuation/numbers
        and not token.is_stop  # Keep generic stop word removal
        and (not custom_stops or token.lemma_.lower() not in custom_stops)
    }
    return sorted(stems)
