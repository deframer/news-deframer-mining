from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Tuple
from uuid import uuid4

from news_deframer.config import Config
import news_deframer.postgres as postgres_module


def make_config() -> Config:
    return Config(
        dsn="postgres://local",
        log_level="INFO",
        log_database=False,
    )


@dataclass
class CursorStub:
    fetchone_queue: List[Tuple] = field(default_factory=list)
    fetchall_results: List[List[Tuple]] = field(default_factory=list)
    fetchall_index: int = 0
    execute_calls: list[tuple[str, tuple | None]] = field(default_factory=list)

    # Context manager methods
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))

    def fetchone(self):
        if self.fetchone_queue:
            return self.fetchone_queue.pop(0)
        return None

    def fetchall(self):
        if self.fetchall_index < len(self.fetchall_results):
            result = self.fetchall_results[self.fetchall_index]
            self.fetchall_index += 1
            return result
        return []


@dataclass
class ConnectionStub:
    cursor_stub: CursorStub

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_stub


def patch_connect(monkeypatch, cursor_stub):
    conn = ConnectionStub(cursor_stub)

    def fake_connect(*args, **kwargs):
        return conn

    monkeypatch.setattr(
        postgres_module, "psycopg2", type("P", (), {"connect": fake_connect})
    )
    return conn


def test_begin_mine_update_returns_none(monkeypatch):
    cursor = CursorStub()
    patch_connect(monkeypatch, cursor)
    repo = postgres_module.Postgres(make_config())

    feed = repo.begin_mine_update(lock_duration=60)

    assert feed is None
    assert any("SELECT" in call[0].upper() for call in cursor.execute_calls)


def test_begin_mine_update_returns_feed(monkeypatch):
    feed_id = uuid4()
    cursor = CursorStub(
        fetchone_queue=[
            (feed_id, ["cat1", "cat2"], "EN", "https://feed.example")
        ]
    )
    patch_connect(monkeypatch, cursor)
    repo = postgres_module.Postgres(make_config())

    feed = repo.begin_mine_update(lock_duration=30)

    assert feed is not None
    assert feed.id == feed_id
    assert feed.categories == ["cat1", "cat2"]
    assert feed.language == "en"
    assert feed.url == "https://feed.example"
    assert feed.root_domain is None
    assert len(cursor.execute_calls) >= 2  # select + update


def test_fetch_pending_items(monkeypatch):
    item_id = uuid4()
    feed_id = uuid4()
    cursor = CursorStub()
    cursor.fetchall_results = [
        [
            (
                item_id,
                ["x"],
                "es",
                datetime(2024, 6, 1, 12, 0, 0),
                "Raw title",
                "Corrected title",
                "Raw description",
                "Corrected description",
            ),
        ],
    ]
    patch_connect(monkeypatch, cursor)
    repo = postgres_module.Postgres(make_config())

    items = repo.fetch_pending_items(feed_id=feed_id)

    assert len(items) == 1
    assert items[0].id == item_id
    assert items[0].feed_id is None
    assert items[0].categories == ["x"]
    assert items[0].language == "es"
    assert items[0].pub_date == datetime(2024, 6, 1, 12, 0, 0)
    assert items[0].think_result is not None
    assert items[0].think_result.title_original == "Raw title"
    assert items[0].think_result.description_corrected == "Corrected description"
    sql = cursor.execute_calls[0][0]
    assert "i.think_result->>'title_original'" in sql
    assert "i.think_result->>'description_corrected'" in sql


def test_upsert_trends(monkeypatch):
    cursor = CursorStub()
    patch_connect(monkeypatch, cursor)
    repo = postgres_module.Postgres(make_config())

    executed_values = []
    monkeypatch.setattr(
        postgres_module,
        "execute_values",
        lambda cur, sql, args, **kwargs: executed_values.append((sql, args)),
    )

    trend = postgres_module.Trend(
        item_id=uuid4(),
        feed_id=uuid4(),
        language="en",
        pub_date=datetime(2024, 1, 1, 12, 0, 0),
        root_domain="example.com",
        category_stems=["cat1"],
        noun_stems=["noun1"],
        verb_stems=["verb1"],
    )

    repo.upsert_trends([trend])

    assert len(executed_values) == 1
    sql, args_list = executed_values[0]
    assert "INSERT INTO trends" in sql
    assert "VALUES %s" in sql
    assert len(args_list) == 1
    tup = args_list[0]
    assert tup[0] == trend.item_id
    assert tup[1] == trend.feed_id
    assert tup[2] == "en"
    assert tup[4] == ["cat1"]


def test_fetch_trends_without_sentiments(monkeypatch):
    item_id = uuid4()
    feed_id = uuid4()
    cursor = CursorStub()
    # Set up multiple result sets for different queries
    cursor.fetchall_results = [
        # First call: trends query
        [
            (
                item_id,
                "de",
                ["käse"],
                ["essen"],
                ["gut"],
                *([None] * 16),
            )
        ],
        # Second call: items query (_fetch_items_by_ids)
        [
            (
                item_id,
                feed_id,
                "Original title",
                "Corrected title",
                "Original description",
                "Corrected description",
            )
        ],
        # Third call: feeds query (_fetch_feeds_by_ids)
        [(feed_id, "https://feed.example")],
    ]
    patch_connect(monkeypatch, cursor)
    repo = postgres_module.Postgres(make_config())

    trends, items_and_feeds = repo.fetch_trends_without_sentiments()

    assert len(trends) == 1
    trend = trends[0]
    assert trend.item_id == item_id
    assert trend.language == "de"
    assert trend.category_stems == []
    assert trend.noun_stems == ["käse"]
    assert trend.verb_stems == ["essen"]
    assert trend.adjective_stems == ["gut"]
    assert trend.feed_id is None
    assert trend.pub_date is None
    assert trend.root_domain is None
    assert trend.sentiments == {}
    assert trend.sentiments_deframed == {}
    # Check that we got the item and feed for this trend
    assert item_id in items_and_feeds
    item, feed = items_and_feeds[item_id]
    assert item.id == item_id
    assert item.feed_id == feed_id
    assert item.think_result is not None
    assert item.think_result.title_corrected == "Corrected title"
    items_sql = cursor.execute_calls[1][0]
    assert "i.think_result->>'title_original'" in items_sql
    assert "i.think_error_count = 0" in items_sql
    assert feed.id == feed_id
    assert feed.url == "https://feed.example"
    assert feed.categories == []
    assert feed.language is None
    assert feed.root_domain is None
    # Check that we made 3 calls: trends, items, feeds
    assert len(cursor.execute_calls) >= 3
    trends_sql = cursor.execute_calls[0][0]
    assert "t.sentiments->>'v'" in trends_sql
    assert "t.sentiments_deframed->>'d_g'" in trends_sql


def test_update_trend_sentiments(monkeypatch):
    cursor = CursorStub()
    patch_connect(monkeypatch, cursor)
    repo = postgres_module.Postgres(make_config())

    executed_values = []
    monkeypatch.setattr(
        postgres_module,
        "execute_values",
        lambda cur, sql, args, **kwargs: executed_values.append((sql, args)),
    )

    item_id = uuid4()
    repo.update_trend_sentiments({item_id: {"v": 6.19, "j": 3.28}})

    assert len(executed_values) == 1
    sql, args_list = executed_values[0]
    assert "UPDATE trends AS t" in sql
    assert "t.sentiments = '{}'::jsonb" in sql
    assert len(args_list) == 1
    assert args_list[0][0] == item_id
    assert isinstance(args_list[0][1], postgres_module.Json)


def test_update_trend_deframed_sentiments(monkeypatch):
    cursor = CursorStub()
    patch_connect(monkeypatch, cursor)
    repo = postgres_module.Postgres(make_config())

    executed_values = []
    monkeypatch.setattr(
        postgres_module,
        "execute_values",
        lambda cur, sql, args, **kwargs: executed_values.append((sql, args)),
    )

    item_id = uuid4()
    repo.update_trend_deframed_sentiments({item_id: {"v": 6.19, "j": 3.28}})

    assert len(executed_values) == 1
    sql, args_list = executed_values[0]
    assert "UPDATE trends AS t" in sql
    assert "sentiments_deframed =" in sql
    assert "t.sentiments_deframed = '{}'::jsonb" in sql
    assert len(args_list) == 1
    assert args_list[0][0] == item_id
    assert isinstance(args_list[0][1], postgres_module.Json)
