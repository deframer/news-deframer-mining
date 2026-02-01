from __future__ import annotations

import pytest

from news_deframer import nlp


def test_sanitize_text_strips_html() -> None:
    html_text = "<p>Hello <strong>World</strong>&nbsp;!</p>"
    assert nlp.sanitize_text(html_text) == "Hello World\xa0!"


def test_extract_stems_errors_without_spacy(monkeypatch) -> None:
    monkeypatch.setattr(nlp, "spacy", None)
    monkeypatch.setattr(nlp, "_NLP_CACHE", {})

    with pytest.raises(RuntimeError):
        nlp.extract_stems("text", "en")


def test_extract_stems_uses_spacy_when_available(monkeypatch) -> None:
    class DummyToken:
        def __init__(self, lemma: str, pos: str):
            self.lemma_ = lemma
            self.pos_ = pos
            self.is_alpha = True

    class DummyModel:
        def __call__(self, _: str):
            return [
                DummyToken("City", "NOUN"),
                DummyToken("Walk", "VERB"),
                DummyToken("People", "PROPN"),
                DummyToken("Ignored", "ADJ"),
            ]

    monkeypatch.setattr(nlp, "_get_spacy_model", lambda _: DummyModel())
    monkeypatch.setattr(nlp, "_get_stopwords", lambda _lang: frozenset())

    nouns, verbs = nlp.extract_stems("Cities walk", "en")

    assert nouns == ["city", "people"]
    assert verbs == ["walk"]


def test_extract_stems_returns_unique_sorted_lemmas(monkeypatch) -> None:
    class DummyToken:
        def __init__(self, lemma: str, pos: str):
            self.lemma_ = lemma
            self.pos_ = pos
            self.is_alpha = True

    class DummyModel:
        def __call__(self, _: str):
            return [
                DummyToken("Banana", "NOUN"),
                DummyToken("apple", "PROPN"),
                DummyToken("banana", "NOUN"),
                DummyToken("carrot", "NOUN"),
                DummyToken("Run", "VERB"),
                DummyToken("run", "VERB"),
                DummyToken("Jog", "VERB"),
            ]

    monkeypatch.setattr(nlp, "_get_spacy_model", lambda _: DummyModel())
    monkeypatch.setattr(nlp, "_get_stopwords", lambda _lang: frozenset())

    nouns, verbs = nlp.extract_stems("content", "en")

    assert nouns == ["apple", "banana", "carrot"]
    assert verbs == ["jog", "run"]


def test_extract_stems_with_real_english_model() -> None:
    try:
        nlp._get_spacy_model("en")
    except RuntimeError:
        pytest.skip("spaCy English model unavailable")

    nouns, verbs = nlp.extract_stems(
        "The quick brown fox jumps over the lazy dog.",
        "en",
    )

    assert nouns == ["dog", "fox"]
    assert verbs == ["jump"]


def test_extract_stems_with_real_german_model() -> None:
    try:
        nlp._get_spacy_model("de")
    except RuntimeError:
        pytest.skip("spaCy German model unavailable")

    nouns, verbs = nlp.extract_stems(
        "Der schnelle braune Fuchs springt ueber den faulen Hund.",
        "de",
    )

    assert nouns == ["fuchs", "hund"]
    assert verbs == ["springen"]


def test_extract_stems_with_real_french_model() -> None:
    try:
        nlp._get_spacy_model("fr")
    except RuntimeError:
        pytest.skip("spaCy French model unavailable")

    nouns, verbs = nlp.extract_stems(
        "Le renard brun rapide saute par-dessus le chien paresseux.",
        "fr",
    )

    assert nouns == ["chien", "renard"]
    assert verbs == ["saute"]


@pytest.mark.parametrize(
    "language,text,expected",
    [
        ("en", "The fox and the dog", ["dog", "fox"]),
        ("de", "Der Fuchs und der Hund", ["fuchs", "hund"]),
        ("fr", "Le renard et le chien", ["chien", "renard"]),
    ],
)
def test_stopword_removal_real_models(
    language: str, text: str, expected: list[str]
) -> None:
    try:
        nlp._get_spacy_model(language)
    except RuntimeError:
        pytest.skip(f"spaCy model for {language} unavailable")

    nouns, verbs = nlp.extract_stems(text, language)

    assert nouns == expected
    assert verbs == []
