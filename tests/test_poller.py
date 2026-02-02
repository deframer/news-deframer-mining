from datetime import datetime
from typing import cast
from uuid import UUID, uuid4

from news_deframer.config import Config, DEFAULT_LOCK_DURATION, POLLING_INTERVAL
from news_deframer.postgres import Feed, Item, Postgres
from news_deframer.miner import Miner, MiningTask
from news_deframer.poller import (
    _extract_title_and_description,
    poll_feed,
    poll_next_feed,
)


class DummyRepo:
    def __init__(
        self,
        feed: Feed | None = None,
        fail_begin: bool = False,
        pending_items: list[Item] | None = None,
    ) -> None:
        self.feed = feed
        self.fail_begin = fail_begin
        self.pending_items = list(pending_items) if pending_items is not None else []
        self.end_calls: list[tuple[str, int]] = []
        self.lock_duration: int | None = None
        self.fetched_for: list[str] = []

    def begin_mine_update(self, lock_duration: int) -> Feed | None:
        self.lock_duration = lock_duration
        if self.fail_begin:
            raise RuntimeError("boom")
        return self.feed

    def end_mine_update(self, feed_id: UUID, polling_interval: int) -> None:
        self.end_calls.append((str(feed_id), polling_interval))

    def fetch_pending_items(
        self, feed_id: UUID, feed_url: str | None = None
    ) -> list[Item]:
        self.fetched_for.append(str(feed_id))
        return list(self.pending_items)


class DummyMiner(Miner):
    def __init__(self) -> None:
        super().__init__(make_config(), repository=cast(Postgres, DummyRepo()))
        self.tasks: list[MiningTask] = []

    def mine_item(self, task: MiningTask) -> None:
        self.tasks.append(task)


def make_config() -> Config:
    return Config(dsn="", log_level="INFO", log_database=False)


def test_poll_next_feed_returns_false_when_no_feed() -> None:
    repo = DummyRepo(feed=None)
    miner = DummyMiner()
    assert poll_next_feed(make_config(), miner, repo) is False
    assert repo.lock_duration == DEFAULT_LOCK_DURATION
    assert repo.end_calls == []


def test_poll_next_feed_success_calls_end_update(monkeypatch) -> None:
    feed_id = uuid4()
    repo = DummyRepo(feed=Feed(id=feed_id, url="https://feed"))
    miner = DummyMiner()

    def fake_poll_feed(feed: Feed, miner_obj: DummyMiner, repo_obj: DummyRepo) -> None:
        miner_obj.tasks.append(
            MiningTask(
                feed_id=feed.id,
                feed_url=feed.url,
                item_id=feed.id,
                language="en",
                categories=[],
                title=None,
                description=None,
                pub_date=datetime(2024, 1, 1, 0, 0, 0),
                root_domain="example.com",
            )
        )

    monkeypatch.setattr("news_deframer.poller.poll_feed", fake_poll_feed)

    assert poll_next_feed(make_config(), miner, repo) is True
    assert repo.end_calls == [(str(feed_id), POLLING_INTERVAL)]


def test_poll_next_feed_passes_errors(monkeypatch) -> None:
    feed_id = uuid4()
    repo = DummyRepo(feed=Feed(id=feed_id, url="https://feed"))
    miner = DummyMiner()

    def boom(feed: Feed, miner_obj: DummyMiner, repo_obj: DummyRepo) -> None:  # noqa: ARG001
        raise ValueError("fail")

    monkeypatch.setattr("news_deframer.poller.poll_feed", boom)

    assert poll_next_feed(make_config(), miner, repo) is True
    assert len(repo.end_calls) == 1
    feed_id_value, retry = repo.end_calls[0]
    assert feed_id_value == str(feed_id)
    assert retry == POLLING_INTERVAL


def test_poll_next_feed_handles_begin_failure(caplog) -> None:
    repo = DummyRepo(feed=None, fail_begin=True)
    miner = DummyMiner()
    assert poll_next_feed(make_config(), miner, repo) is False
    assert repo.end_calls == []
    assert any("Failed to query next feed" in msg for msg in caplog.text.splitlines())


def test_poll_feed_fetches_items() -> None:
    feed_id = uuid4()
    pending_items = [
        Item(
            id=uuid4(),
            feed_id=feed_id,
            language="es",
            content="<item><deframer:title_original>foo</deframer:title_original></item>",
            pub_date=datetime(2024, 1, 1, 0, 0, 0),
        )
    ]
    repo = DummyRepo(pending_items=pending_items)
    miner = DummyMiner()

    poll_feed(Feed(id=feed_id, url="https://feed"), miner, repo)

    assert repo.fetched_for == [str(feed_id)]
    assert len(miner.tasks) == 1
    assert miner.tasks[0].pub_date == datetime(2024, 1, 1, 0, 0, 0)
    assert miner.tasks[0].categories == []
    assert miner.tasks[0].root_domain == "feed"


def test_poll_feed_uses_feed_root_domain() -> None:
    feed_id = uuid4()
    pending_items = [
        Item(
            id=uuid4(),
            feed_id=feed_id,
            content="<item/>",
            pub_date=datetime(2024, 1, 1, 0, 0, 0),
        )
    ]
    repo = DummyRepo(pending_items=pending_items)
    miner = DummyMiner()
    feed = Feed(id=feed_id, url="https://feed", root_domain="feed.example")

    poll_feed(feed, miner, repo)

    assert len(miner.tasks) == 1
    assert miner.tasks[0].categories == []
    assert miner.tasks[0].root_domain == "feed.example"


def test_poll_feed_returns_error(monkeypatch, caplog) -> None:
    feed = Feed(id=uuid4(), url="https://feed")
    item = Item(
        id=uuid4(),
        feed_id=feed.id,
        content="<item/>",
        pub_date=datetime(2024, 1, 1, 0, 0, 0),
    )
    repo = DummyRepo(pending_items=[item])

    class ExplodingMiner(Miner):
        def __init__(self) -> None:
            super().__init__(make_config(), repository=cast(Postgres, DummyRepo()))

        def mine_item(self, task: MiningTask) -> None:
            raise RuntimeError("boom")

    miner = ExplodingMiner()

    with caplog.at_level("ERROR"):
        error = poll_feed(feed, miner, repo)

    assert isinstance(error, RuntimeError)
    assert any("Failed to process item" in record.message for record in caplog.records)


def test_extract_title_and_description_success() -> None:
    content = """
    <item>
      <deframer:title_original>
        Boost Your Productivity
      </deframer:title_original>
      <deframer:description_original>
        Simple Tips
      </deframer:description_original>
      <title>Ignored Standard Title</title>
    </item>
    """
    title, description = _extract_title_and_description(content)
    assert title == "Boost Your Productivity"
    assert description == "Simple Tips"


def test_extract_title_and_description_ignores_standard_tags() -> None:
    content = """
    <item>
      <title>Standard Title</title>
      <description>Standard Description</description>
    </item>
    """
    title, description = _extract_title_and_description(content)
    assert title is None
    assert description is None


def test_extract_title_and_description_handles_malformed_xml() -> None:
    content = "<item><title>Unclosed"
    title, description = _extract_title_and_description(content)
    assert title is None
    assert description is None


def test_extract_title_and_description_with_unknown_namespaces() -> None:
    content = """<item>
  <deframer:title_original>Extracted Title</deframer:title_original>
  <deframer:description_original>Extracted Description</deframer:description_original>
  <wfw:commentRss>http://example.com/feed</wfw:commentRss>
  <slash:comments>10</slash:comments>
</item>"""
    title, description = _extract_title_and_description(content)
    assert title == "Extracted Title"
    assert description == "Extracted Description"
