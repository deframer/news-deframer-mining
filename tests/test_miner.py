from datetime import datetime
from typing import cast
from uuid import uuid4

import pytest

from news_deframer import nlp
from news_deframer.config import Config
from news_deframer.miner import Miner, MiningTask
from news_deframer.postgres import Postgres, Trend


class RepositoryStub:
    def __init__(self):
        self.upserted = []

    def upsert_trends(self, trends: list[Trend]):
        self.upserted.extend(trends)


def make_config() -> Config:
    return Config(dsn="", log_level="INFO", log_database=False)


def test_miner_logs_task(caplog):
    try:
        nlp._get_spacy_model("en")
    except RuntimeError:
        pytest.skip("spaCy English model unavailable")

    repo = RepositoryStub()
    miner = Miner(make_config(), repository=cast(Postgres, repo))
    task = MiningTask(
        feed_id=uuid4(),
        feed_url="https://feed",
        item_id=uuid4(),
        language="en",
        categories=["a"],
        title="Title of Nouns",
        description="Running verbs now",
        pub_date=datetime(2024, 1, 1, 12, 0, 0),
        root_domain="example.com",
    )

    with caplog.at_level("INFO"):
        miner.mine_item(task)

    assert len(repo.upserted) == 1
    stored_trend = repo.upserted[0]
    assert stored_trend.item_id == task.item_id
    assert stored_trend.feed_id == task.feed_id
    assert stored_trend.language == task.language
    assert stored_trend.noun_stems == ["nouns", "title", "verb"]
    assert stored_trend.verb_stems == ["run"]


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
):
    try:
        nlp._get_spacy_model(language)
    except RuntimeError:
        pytest.skip(f"spaCy model for {language} unavailable")

    repo = RepositoryStub()
    miner = Miner(make_config(), repository=cast(Postgres, repo))
    task = MiningTask(
        feed_id=uuid4(),
        feed_url="https://feed",
        item_id=uuid4(),
        language=language,
        categories=["a"],
        title=title,
        description=description,
        pub_date=datetime(2024, 1, 1, 12, 0, 0),
        root_domain="example.com",
    )

    miner.mine_item(task)

    assert len(repo.upserted) == 1
    stored_trend = repo.upserted[0]
    assert stored_trend.noun_stems == expected_nouns
    assert stored_trend.verb_stems == expected_verbs
