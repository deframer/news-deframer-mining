from uuid import uuid4

from news_deframer.config import Config, DEFAULT_LOCK_DURATION, POLLING_INTERVAL
from news_deframer.database.postgres import Feed, Item
from news_deframer.miner.miner import MiningTask
from news_deframer.miner.poller import poll_feed, poll_next_feed


class DummyRepo:
    def __init__(
        self,
        feed: Feed | None = None,
        fail_begin: bool = False,
        pending_items: list[Item] | None = None,
    ):
        self.feed = feed
        self.fail_begin = fail_begin
        self.pending_items = pending_items or []
        self.end_calls: list[tuple[str, Exception | None, int]] = []
        self.lock_duration: int | None = None
        self.fetched_for: list[str] = []

    def begin_mine_update(self, lock_duration: int):
        self.lock_duration = lock_duration
        if self.fail_begin:
            raise RuntimeError("boom")
        return self.feed

    def end_mine_update(self, feed_id, error: Exception | None, retry_interval: int):
        self.end_calls.append((str(feed_id), error, retry_interval))

    def fetch_pending_items(self, feed_id, feed_url=None):
        self.fetched_for.append(str(feed_id))
        return self.pending_items

    def mark_items_mined(self, item_ids):
        pass


class DummyMiner:
    def __init__(self):
        self.tasks = []

    def mine_item(self, task):
        self.tasks.append(task)


def make_config() -> Config:
    return Config(dsn="", log_level="INFO", log_database=False)


def test_poll_next_feed_returns_false_when_no_feed():
    repo = DummyRepo(feed=None)
    miner = DummyMiner()
    assert poll_next_feed(make_config(), miner, repo) is False
    assert repo.lock_duration == DEFAULT_LOCK_DURATION
    assert repo.end_calls == []


def test_poll_next_feed_success_calls_end_update(monkeypatch):
    feed_id = uuid4()
    repo = DummyRepo(feed=Feed(id=feed_id))
    miner = DummyMiner()

    def fake_poll_feed(feed, miner_obj, repo_obj):
        miner_obj.tasks.append(
            MiningTask(
                feed_id=str(feed.id),
                feed_url=feed.url,
                item_id=str(feed.id),
                language="en",
                categories=[],
                title=None,
                description=None,
            )
        )

    monkeypatch.setattr("news_deframer.miner.poller.poll_feed", fake_poll_feed)

    assert poll_next_feed(make_config(), miner, repo) is True
    assert repo.end_calls == [(str(feed_id), None, POLLING_INTERVAL)]


def test_poll_next_feed_passes_errors(monkeypatch):
    feed_id = uuid4()
    repo = DummyRepo(feed=Feed(id=feed_id))
    miner = DummyMiner()

    def boom(feed, miner_obj, repo_obj):  # noqa: ARG001
        raise ValueError("fail")

    monkeypatch.setattr("news_deframer.miner.poller.poll_feed", boom)

    assert poll_next_feed(make_config(), miner, repo) is True
    assert len(repo.end_calls) == 1
    feed_id_value, err, retry = repo.end_calls[0]
    assert feed_id_value == str(feed_id)
    assert isinstance(err, ValueError)
    assert retry == POLLING_INTERVAL


def test_poll_next_feed_handles_begin_failure(caplog):
    repo = DummyRepo(feed=None, fail_begin=True)
    miner = DummyMiner()
    assert poll_next_feed(make_config(), miner, repo) is False
    assert repo.end_calls == []
    assert any("Failed to query next feed" in msg for msg in caplog.text.splitlines())


def test_poll_feed_fetches_items():
    feed_id = uuid4()
    pending_items = [
        Item(id=uuid4(), feed_id=feed_id, language="es", title="foo", description="bar")
    ]
    repo = DummyRepo(pending_items=pending_items)
    miner = DummyMiner()

    poll_feed(Feed(id=feed_id), miner, repo)

    assert repo.fetched_for == [str(feed_id)]
    assert len(miner.tasks) == 1


def test_poll_feed_returns_error(monkeypatch, caplog):
    feed = Feed(id=uuid4(), url="https://feed")
    item = Item(id=uuid4(), feed_id=feed.id)
    repo = DummyRepo(pending_items=[item])

    class ExplodingMiner:
        def mine_item(self, task):
            raise RuntimeError("boom")

    miner = ExplodingMiner()

    with caplog.at_level("ERROR"):
        error = poll_feed(feed, miner, repo)

    assert isinstance(error, RuntimeError)
    assert any("Failed to process item" in record.message for record in caplog.records)
