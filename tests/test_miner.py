from datetime import datetime
from typing import cast
from uuid import uuid4

import pytest

from news_deframer import nlp
import news_deframer.miner as miner_module
from news_deframer.config import Config
from news_deframer.miner import Miner, MiningTask
from news_deframer.postgres import Postgres, Trend


class RepositoryStub:
    def __init__(self):
        self.upserted = []

    def upsert_trends(self, trends: list[Trend]):
        self.upserted.extend(trends)


def make_config() -> Config:
    config = Config.load()
    config.dsn = ""
    config.log_level = "INFO"
    config.log_database = False
    return config


def test_mine_item_upserts_trend(monkeypatch):
    try:
        nlp._get_spacy_model("en")
    except RuntimeError:
        pytest.skip("spaCy English model unavailable")

    repo = RepositoryStub()
    miner = Miner(make_config(), repository=cast(Postgres, repo))
    monkeypatch.setattr(
        miner_module,
        "extract_sentiments_array",
        lambda stems, language, **_kwargs: {"v": 6.19},
    )
    task = MiningTask(
        feed_id=uuid4(),
        feed_url="https://feed",
        item_id=uuid4(),
        language="en",
        categories=["a"],
        title_deframed=None,
        description_deframed=None,
        title_original="Title of Nouns",
        description_original="The verbs run now",
        pub_date=datetime(2024, 1, 1, 12, 0, 0),
        root_domain="example.com",
    )

    miner.mine_item(task)

    assert len(repo.upserted) == 1
    stored_trend = repo.upserted[0]
    assert stored_trend.item_id == task.item_id
    assert stored_trend.feed_id == task.feed_id
    assert stored_trend.language == task.language
    assert stored_trend.noun_stems == ["nouns", "title", "verb"]
    assert stored_trend.verb_stems == ["run"]
    assert stored_trend.sentiments == {"v": 6.19}


@pytest.mark.parametrize(
    "language,title,description,expected_nouns,expected_verbs",
    [
        (
            "en",
            "The quick brown fox",
            "jumps over the lazy dog",
            ["dog", "fox"],
            ["jump"],
        ),
        (
            "de",
            "Der schnelle braune Fuchs",
            "springt ueber den faulen Hund",
            ["fuchs", "hund"],
            ["springen"],
        ),
        (
            "fr",
            "Le renard brun rapide",
            "saute par-dessus le chien paresseux",
            ["chien", "renard"],
            ["saute"],
        ),
    ],
)
def test_miner_stem_extraction_real_models(
    language: str,
    title: str,
    description: str,
    expected_nouns: list[str],
    expected_verbs: list[str],
    monkeypatch,
):
    try:
        nlp._get_spacy_model(language)
    except RuntimeError:
        pytest.skip(f"spaCy model for {language} unavailable")

    repo = RepositoryStub()
    miner = Miner(make_config(), repository=cast(Postgres, repo))
    monkeypatch.setattr(
        miner_module,
        "extract_sentiments_array",
        lambda stems, sentiment_language, **_kwargs: {},
    )
    task = MiningTask(
        feed_id=uuid4(),
        feed_url="https://feed",
        item_id=uuid4(),
        language=language,
        categories=["a"],
        title_deframed=None,
        description_deframed=None,
        title_original=title,
        description_original=description,
        pub_date=datetime(2024, 1, 1, 12, 0, 0),
        root_domain="example.com",
    )

    miner.mine_item(task)

    assert len(repo.upserted) == 1
    stored_trend = repo.upserted[0]
    assert stored_trend.noun_stems == expected_nouns
    assert stored_trend.verb_stems == expected_verbs


def test_mine_item_filters_stop_words(monkeypatch):
    try:
        nlp._get_spacy_model("en")
    except RuntimeError:
        pytest.skip("spaCy English model unavailable")

    repo = RepositoryStub()
    miner = Miner(make_config(), repository=cast(Postgres, repo))
    monkeypatch.setattr(
        miner_module,
        "extract_sentiments_array",
        lambda stems, sentiment_language, **_kwargs: {},
    )
    task = MiningTask(
        feed_id=uuid4(),
        feed_url="https://feed",
        item_id=uuid4(),
        language="en",
        categories=[],
        title_deframed=None,
        description_deframed=None,
        title_original="The Fox",
        description_original="The Dog",
        pub_date=datetime(2024, 1, 1, 12, 0, 0),
        root_domain="example.com",
        stop_words=["fox"],
    )

    miner.mine_item(task)

    assert len(repo.upserted) == 1
    stored_trend = repo.upserted[0]
    assert "fox" not in stored_trend.noun_stems
    assert "dog" in stored_trend.noun_stems
