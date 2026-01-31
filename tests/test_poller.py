from uuid import uuid4

from news_deframer.config import Config, DEFAULT_LOCK_DURATION, POLLING_INTERVAL
from news_deframer.miner.poller import poll_feed, poll_item, poll_next_feed
from news_deframer.database.postgres import Feed, Item


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
        self.marked: list[list] = []

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
        self.marked.append(list(item_ids))


def make_config() -> Config:
    return Config(dsn="", log_level="INFO", log_database=False)


def test_poll_next_feed_returns_false_when_no_feed():
    repo = DummyRepo(feed=None)
    assert poll_next_feed(make_config(), repo) is False
    assert repo.lock_duration == DEFAULT_LOCK_DURATION
    assert repo.end_calls == []


def test_poll_next_feed_success_calls_end_update(monkeypatch):
    feed_id = uuid4()
    repo = DummyRepo(feed=Feed(id=feed_id))

    called = {}

    def fake_mine(feed, repository):
        called["feed"] = feed

    monkeypatch.setattr("news_deframer.miner.poller.poll_feed", fake_mine)

    assert poll_next_feed(make_config(), repo) is True
    assert called["feed"].id == feed_id
    assert repo.end_calls == [(str(feed_id), None, POLLING_INTERVAL)]


def test_poll_next_feed_passes_errors(monkeypatch):
    feed_id = uuid4()
    repo = DummyRepo(feed=Feed(id=feed_id))

    def boom(feed, repository):  # noqa: ARG001 - required by signature
        raise ValueError("fail")

    monkeypatch.setattr("news_deframer.miner.poller.poll_feed", boom)

    assert poll_next_feed(make_config(), repo) is True
    assert len(repo.end_calls) == 1
    feed_id_value, err, retry = repo.end_calls[0]
    assert feed_id_value == str(feed_id)
    assert isinstance(err, ValueError)
    assert retry == POLLING_INTERVAL


def test_poll_next_feed_handles_begin_failure(caplog):
    repo = DummyRepo(feed=None, fail_begin=True)
    assert poll_next_feed(make_config(), repo) is False
    assert repo.end_calls == []
    assert any("Failed to query next feed" in msg for msg in caplog.text.splitlines())


def test_poll_feed_fetches_items(monkeypatch):
    feed_id = uuid4()
    pending_items = [
        Item(id=uuid4(), feed_id=feed_id, language="es", title="foo", description="bar")
    ]
    repo = DummyRepo(pending_items=pending_items)
    calls = []

    def fake_mine_item(feed, item):
        calls.append((feed.id, item.id))

    monkeypatch.setattr("news_deframer.miner.poller.poll_item", fake_mine_item)

    poll_feed(Feed(id=feed_id), repo)
    assert repo.fetched_for == [str(feed_id)]
    assert len(calls) == 1
    assert repo.marked == []


def test_poll_feed_returns_error(monkeypatch, caplog):
    feed = Feed(id=uuid4(), url="https://feed")
    item = Item(id=uuid4(), feed_id=feed.id)
    repo = DummyRepo(pending_items=[item])

    def boom(feed, item):  # noqa: ARG001
        raise RuntimeError("boom")

    monkeypatch.setattr("news_deframer.miner.poller.poll_item", boom)

    with caplog.at_level("ERROR"):
        error = poll_feed(feed, repo)

    assert isinstance(error, RuntimeError)
    assert any("Failed to process item" in record.message for record in caplog.records)
    assert repo.marked == []


def test_poll_item_logs(caplog):
    feed = Feed(id=uuid4(), categories=["feed-cat"], language="en", url="https://feed")
    item = Item(
        id=uuid4(),
        feed_id=feed.id,
        categories=["item-cat"],
        language="es",
        title="Sample",
        description="Body",
    )
    with caplog.at_level("INFO"):
        poll_item(feed, item)
    assert any(record.message == "Processed feed item" for record in caplog.records)
    assert any(
        "categories" in record.__dict__
        and record.categories == ["feed-cat", "item-cat"]
        for record in caplog.records
    )
    assert any(getattr(record, "language", None) == "es" for record in caplog.records)


def test_poll_item_language_fallback_warns(caplog):
    feed = Feed(id=uuid4(), url="https://feed")
    item = Item(id=uuid4(), feed_id=feed.id)
    with caplog.at_level("WARNING"):
        poll_item(feed, item)
    assert any("falling back" in record.message.lower() for record in caplog.records)
    assert any(
        getattr(record, "feed_url", None) == "https://feed" for record in caplog.records
    )
