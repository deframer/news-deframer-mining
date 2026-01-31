"""Language-specific helpers for extracting lexical stems."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence
import re
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
    """Return noun and verb stems for ``content``.

    Attempts to use spaCy when available; otherwise falls back to a simple
    regex-based tokenizer that preserves the previous behavior used in tests.
    """

    normalized = content.strip()
    if normalized:
        nlp = _get_spacy_model(language)
        if nlp is not None:
            try:
                doc = nlp(normalized)
            except Exception:  # pragma: no cover - spaCy runtime failure
                doc = None
            if doc is not None:
                noun_stems = [
                    token.lemma_.lower()
                    for token in doc
                    if token.pos_ in {"NOUN", "PROPN"} and token.lemma_
                ]
                verb_stems = [
                    token.lemma_.lower()
                    for token in doc
                    if token.pos_ == "VERB" and token.lemma_
                ]
                if noun_stems or verb_stems:
                    return noun_stems, verb_stems

    return _fallback_tokens(title, description)


def _fallback_tokens(
    title: Optional[str], description: Optional[str]
) -> tuple[Sequence[str], Sequence[str]]:
    return _tokenize_words(title), _tokenize_words(description)


_WORD_RE = re.compile(r"[A-Za-z]+")


def _tokenize_words(value: Optional[str]) -> Sequence[str]:
    if not value:
        return []
    return [match.group(0).lower() for match in _WORD_RE.finditer(value)]


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
_NLP_CACHE: dict[str, SpacyLanguage | None] = {}


def _get_spacy_model(language: str) -> SpacyLanguage | None:
    if spacy is None:
        return None

    lang_code = (language or "").split("-")[0].lower()
    candidates: list[str] = []

    model_name = _SPACY_LANGUAGE_MODELS.get(lang_code)
    if model_name:
        candidates.append(model_name)

    if _FALLBACK_SPACY_MODEL not in candidates:
        candidates.append(_FALLBACK_SPACY_MODEL)

    for name in candidates:
        if name in _NLP_CACHE:
            cached = _NLP_CACHE[name]
            if cached is not None:
                return cached
            continue

        try:
            nlp = spacy.load(name, disable=("ner",))
        except Exception:  # pragma: no cover - propagate failure gracefully
            _NLP_CACHE[name] = None
            continue

        _NLP_CACHE[name] = nlp
        return nlp

    return None
