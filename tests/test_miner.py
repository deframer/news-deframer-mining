from datetime import datetime
from typing import cast
from uuid import uuid4

from news_deframer.config import Config
from news_deframer.duckdb_store import DuckDBStore
from news_deframer.miner import Miner, MiningTask


class StoreStub:
    def __init__(self):
        self.inserted = []

    def insert_trend_docs(self, docs):
        self.inserted.extend(docs)


def make_config() -> Config:
    return Config(dsn="", log_level="INFO", log_database=False, duck_db_file=":memory:")


def test_miner_logs_task(caplog):
    store = StoreStub()
    miner = Miner(make_config(), store=cast(DuckDBStore, store))
    task = MiningTask(
        feed_id=str(uuid4()),
        feed_url="https://feed",
        item_id=str(uuid4()),
        language="en",
        categories=["a"],
        title="Title of Nouns",
        description="Running verbs now",
        pub_date=datetime(2024, 1, 1, 12, 0, 0),
    )

    with caplog.at_level("INFO"):
        miner.mine_item(task)

    assert any("Processed feed item" in record.message for record in caplog.records)
    assert len(store.inserted) == 1
    stored_doc = store.inserted[0]
    assert stored_doc.item_id == task.item_id
    assert stored_doc.feed_id == task.feed_id
    assert stored_doc.language == task.language
    assert stored_doc.noun_stems == ["title", "of", "nouns"]
    assert stored_doc.verb_stems == ["running", "verbs", "now"]
