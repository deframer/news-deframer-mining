from uuid import uuid4

from news_deframer.config import Config
from news_deframer.miner import Miner, MiningTask


def make_config() -> Config:
    return Config(dsn="", log_level="INFO", log_database=False)


def test_miner_logs_task(caplog):
    miner = Miner(make_config())
    task = MiningTask(
        feed_id=str(uuid4()),
        feed_url="https://feed",
        item_id=str(uuid4()),
        language="en",
        categories=["a"],
        title="Title",
        description="Desc",
    )

    with caplog.at_level("INFO"):
        miner.mine_item(task)

    assert any("Processed feed item" in record.message for record in caplog.records)
