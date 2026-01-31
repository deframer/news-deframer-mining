from __future__ import annotations

from news_deframer import nlp


def test_sanitize_text_strips_html() -> None:
    html_text = "<p>Hello <strong>World</strong>&nbsp;!</p>"
    assert nlp.sanitize_text(html_text) == "Hello World\xa0!"


def test_extract_stems_fallback_without_spacy(monkeypatch) -> None:
    monkeypatch.setattr(nlp, "_get_spacy_model", lambda _: None)

    nouns, verbs = nlp.extract_stems(
        "",
        "en",
        title="Cats & Dogs",
        description="Run fast now",
    )

    assert nouns == ["cats", "dogs"]
    assert verbs == ["run", "fast", "now"]


def test_extract_stems_uses_spacy_when_available(monkeypatch) -> None:
    class DummyToken:
        def __init__(self, lemma: str, pos: str):
            self.lemma_ = lemma
            self.pos_ = pos

    class DummyModel:
        def __call__(self, _: str):
            return [
                DummyToken("City", "NOUN"),
                DummyToken("Walk", "VERB"),
                DummyToken("People", "PROPN"),
                DummyToken("Ignored", "ADJ"),
            ]

    monkeypatch.setattr(nlp, "_get_spacy_model", lambda _: DummyModel())

    nouns, verbs = nlp.extract_stems("Cities walk", "en")

    assert nouns == ["city", "people"]
    assert verbs == ["walk"]
