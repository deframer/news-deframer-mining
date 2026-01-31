"""Language-specific helpers for extracting lexical stems."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence
from bs4 import BeautifulSoup

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
    language: str,
    *,
    title: Optional[str] = None,
    description: Optional[str] = None,
) -> tuple[Sequence[str], Sequence[str]]:
    """Return noun and verb lemmas for ``content`` using spaCy models."""

    normalized = content.strip()
    if not normalized:
        return [], []

    nlp = _get_spacy_model(language)
    try:
        doc = nlp(normalized)
    except Exception as exc:  # pragma: no cover - spaCy runtime failure
        raise RuntimeError("Failed to process text with spaCy model") from exc

    noun_stems = [
        token.lemma_.lower()
        for token in doc
        if token.pos_ in {"NOUN", "PROPN"}
        and token.lemma_
        and token.is_alpha
        and not _is_stop_word(token.lemma_, language)
    ]
    verb_stems = [
        token.lemma_.lower()
        for token in doc
        if token.pos_ == "VERB"
        and token.lemma_
        and token.is_alpha
        and not _is_stop_word(token.lemma_, language)
    ]

    return noun_stems, verb_stems


def sanitize_text(value: Optional[str]) -> Optional[str]:
    """Strip HTML tags from text using BeautifulSoup."""

    if value is None:
        return None
    return BeautifulSoup(value, "html.parser").get_text()


_SPACY_LANGUAGE_MODELS = {
    "en": "en_core_web_sm",
    "de": "de_core_news_sm",
    "es": "es_core_news_sm",
    "fr": "fr_core_news_sm",
    "it": "it_core_news_sm",
    "pt": "pt_core_news_sm",
    "nl": "nl_core_news_sm",
    "pl": "pl_core_news_sm",
    "ru": "ru_core_news_sm",
}
_FALLBACK_SPACY_MODEL = "xx_ent_wiki_sm"
_NLP_CACHE: dict[str, SpacyLanguage] = {}
_STOPWORD_CACHE: dict[str, frozenset[str]] = {}


def _get_spacy_model(language: str) -> SpacyLanguage:
    if spacy is None:
        raise RuntimeError("spaCy is required but not installed")

    lang_code = (language or "").split("-")[0].lower()
    candidates: list[str] = []

    model_name = _SPACY_LANGUAGE_MODELS.get(lang_code)
    if model_name:
        candidates.append(model_name)

    if _FALLBACK_SPACY_MODEL not in candidates:
        candidates.append(_FALLBACK_SPACY_MODEL)

    for name in candidates:
        if name in _NLP_CACHE:
            return _NLP_CACHE[name]

        try:
            model = spacy.load(name, disable=("ner",))
        except Exception as exc:  # pragma: no cover - propagate failure gracefully
            raise RuntimeError(f"Failed to load spaCy model '{name}'") from exc

        _NLP_CACHE[name] = model
        return model

    raise RuntimeError(f"No spaCy model available for language '{language}'")


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
