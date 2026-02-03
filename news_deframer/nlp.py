"""Language-specific helpers for extracting lexical stems."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Optional, Sequence
from bs4 import BeautifulSoup

from news_deframer.spacy_models import SPACY_LANGUAGE_MODELS

try:  # pragma: no cover - optional dependency
    import spacy
except Exception:  # pragma: no cover - optional dependency
    spacy = None  # type: ignore[assignment]

if TYPE_CHECKING:  # pragma: no cover - typing only
    from spacy.language import Language as SpacyLanguage
else:  # pragma: no cover - runtime fallback
    SpacyLanguage = Any  # type: ignore[misc,assignment]


def extract_stems(
    content: str,
    language: str
) -> tuple[Sequence[str], Sequence[str], Sequence[str]]:
    """
    Return noun, verb, and adjective lemmas for ``content`` using spaCy.

    Returns:
        (noun_stems, verb_stems, adj_stems)
    """
    normalized = content.strip()
    if not normalized:
        return [], [], []

    nlp = _get_spacy_model(language)

    try:
        doc = nlp(normalized)
    except Exception as exc:
        raise RuntimeError("Failed to process text with spaCy model") from exc

    # Thesis: Nouns (Triggers) include common nouns and Proper Nouns (Entities)
    noun_stems = _collect_sorted_unique_stems(doc, {"NOUN", "PROPN"}, language)

    # Thesis: Verbs are 'Diversificators' indicating action
    verb_stems = _collect_sorted_unique_stems(doc, {"VERB"}, language)

    # Thesis: Adjectives are 'Diversificators' indicating sentiment/direction
    adj_stems = _collect_sorted_unique_stems(doc, {"ADJ"}, language)

    return noun_stems, verb_stems, adj_stems

def sanitize_text(value: Optional[str]) -> Optional[str]:
    """Strip HTML tags from text using BeautifulSoup."""

    if value is None:
        return None
    return BeautifulSoup(value, "html.parser").get_text()


def stem_category(text: Optional[str], language: str) -> Optional[str]:
    """Return the lemmatized version of a category string."""
    if not text:
        return None

    normalized = text.strip()
    if not normalized:
        return None

    nlp = _get_spacy_model(language)
    try:
        doc = nlp(normalized)
    except Exception as exc:
        raise RuntimeError("Failed to process text with spaCy model") from exc

    lemmas = [
        token.lemma_.lower()
        for token in doc
        if token.is_alpha and not _is_stop_word(token.lemma_, language)
    ]

    return " ".join(lemmas) if lemmas else None


_NLP_CACHE: dict[str, SpacyLanguage] = {}
_STOPWORD_CACHE: dict[str, frozenset[str]] = {}


def _get_spacy_model(language: str) -> SpacyLanguage:
    if spacy is None:
        raise RuntimeError("spaCy is required but not installed")

    lang_code = (language or "").split("-")[0].lower()

    model_name = SPACY_LANGUAGE_MODELS.get(lang_code)
    if not model_name:
        raise RuntimeError(f"No spaCy model available for language '{language}'")

    if model_name in _NLP_CACHE:
        return _NLP_CACHE[model_name]

    try:
        model = spacy.load(model_name, disable=("ner",))
    except Exception as exc:  # pragma: no cover - propagate failure gracefully
        raise RuntimeError(f"Failed to load spaCy model '{model_name}'") from exc

    _NLP_CACHE[model_name] = model
    return model


def _get_stopwords(language: str) -> frozenset[str]:
    if spacy is None:
        raise RuntimeError("spaCy is required but not installed")

    lang_code = (language or "").split("-")[0].lower()
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


def _collect_sorted_unique_stems(
    tokens: Iterable[Any],
    allowed_pos: set[str],
    language: str,
) -> list[str]:
    stems = {
        token.lemma_.lower()
        for token in tokens
        if token.pos_ in allowed_pos  # Strict POS filtering
        and token.lemma_
        and token.is_alpha  # Remove punctuation/numbers
        and not token.is_stop  # Keep generic stop word removal
    }
    return sorted(stems)
