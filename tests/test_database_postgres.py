from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple
from uuid import uuid4

from news_deframer.config import Config
from news_deframer.database import postgres


def make_config() -> Config:
    return Config(dsn="postgres://local", log_level="INFO", log_database=False)


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

    monkeypatch.setattr(postgres, "psycopg2", type("P", (), {"connect": fake_connect}))
    return conn


def test_begin_mine_update_returns_none(monkeypatch):
    cursor = CursorStub()
    patch_connect(monkeypatch, cursor)
    repo = postgres.Postgres(make_config())

    feed = repo.begin_mine_update(lock_duration=60)

    assert feed is None
    assert any("SELECT" in call[0].upper() for call in cursor.execute_calls)


def test_begin_mine_update_returns_feed(monkeypatch):
    feed_id = uuid4()
    cursor = CursorStub(
        fetchone_queue=[(feed_id, ["cat1", "cat2"], "EN", "https://feed.example")]
    )
    patch_connect(monkeypatch, cursor)
    repo = postgres.Postgres(make_config())

    feed = repo.begin_mine_update(lock_duration=30)

    assert feed is not None
    assert feed.id == feed_id
    assert feed.categories == ["cat1", "cat2"]
    assert feed.language == "en"
    assert feed.url == "https://feed.example"
    assert len(cursor.execute_calls) >= 2  # select + update


def test_fetch_pending_items(monkeypatch):
    item_id = uuid4()
    feed_id = uuid4()
    cursor = CursorStub(
        fetchall_result=[
            (item_id, feed_id, ["x"], "es", "title", "desc"),
        ]
    )
    patch_connect(monkeypatch, cursor)
    repo = postgres.Postgres(make_config())

    items = repo.fetch_pending_items(feed_id=feed_id)

    assert len(items) == 1
    assert items[0].id == item_id
    assert items[0].feed_id == feed_id
    assert items[0].categories == ["x"]
    assert items[0].language == "es"
    assert items[0].title == "title"
    assert items[0].description == "desc"
