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
    fetchall_result: List[Tuple] = field(default_factory=list)
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
        return list(self.fetchall_result)


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
            (feed_id, ["cat1", "cat2"], "EN", "https://feed.example", "feed.example")
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
    assert feed.root_domain == "feed.example"
    assert len(cursor.execute_calls) >= 2  # select + update


def test_fetch_pending_items(monkeypatch):
    item_id = uuid4()
    feed_id = uuid4()
    cursor = CursorStub(
        fetchall_result=[
            (
                item_id,
                feed_id,
                ["x"],
                "es",
                datetime(2024, 6, 1, 12, 0, 0),
                "raw content",
            ),
        ]
    )
    patch_connect(monkeypatch, cursor)
    repo = postgres_module.Postgres(make_config())

    items = repo.fetch_pending_items(feed_id=feed_id)

    assert len(items) == 1
    assert items[0].id == item_id
    assert items[0].feed_id == feed_id
    assert items[0].categories == ["x"]
    assert items[0].language == "es"
    assert items[0].pub_date == datetime(2024, 6, 1, 12, 0, 0)
    assert items[0].content == "raw content"


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


def test_mark_items_mined(monkeypatch):
    cursor = CursorStub()
    patch_connect(monkeypatch, cursor)
    repo = postgres_module.Postgres(make_config())

    executed_values = []
    monkeypatch.setattr(
        postgres_module,
        "execute_values",
        lambda cur, sql, args, **kwargs: executed_values.append((sql, args)),
    )

    item_ids = [uuid4(), uuid4()]
    repo.mark_items_mined(item_ids)

    assert len(executed_values) == 1
    sql, args_list = executed_values[0]
    assert "UPDATE items AS t" in sql
    assert len(args_list) == 2
    assert args_list[0] == (item_ids[0],)
