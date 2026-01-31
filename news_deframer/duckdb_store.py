"""DuckDB storage helpers for news deframer trend mining."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence

import duckdb

from news_deframer.config import TREND_DOC_BUFFER_SIZE

_MEMORY_ALIASES = frozenset({":memory", ":memory:"})


@dataclass(slots=True)
class TrendDoc:
    """Represents a single record stored in DuckDB."""

    item_id: str
    feed_id: str
    language: str
    pub_date: datetime | None = None
    categories: Sequence[str] | None = None
    noun_stems: Sequence[str] | None = None
    verb_stems: Sequence[str] | None = None


class DuckDBStore:
    """Wraps DuckDB access and ensures the ``trend_docs`` table exists."""

    def __init__(self, db_path: str):
        self._db_path: Path | None = None
        self._buffer: list[TrendDoc] = []
        self._buffer_limit = max(1, TREND_DOC_BUFFER_SIZE)
        normalized_target = db_path.strip()
        database = _normalize_database_target(normalized_target)

        if not _is_memory_target(normalized_target):
            path = Path(normalized_target)
            if not path.suffix:
                path = path.with_suffix(".duckdb")
            path.parent.mkdir(parents=True, exist_ok=True)
            self._db_path = path
            database = str(path)

        self._conn = duckdb.connect(database=database, read_only=False)
        self._ensure_schema()

    def __enter__(self) -> "DuckDBStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: D401 (context manager cleanup)
        self.close()

    def _ensure_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trend_docs (
                item_id VARCHAR PRIMARY KEY,
                feed_id VARCHAR NOT NULL,
                language VARCHAR NOT NULL,
                pub_date TIMESTAMP,
                categories VARCHAR[],
                noun_stems VARCHAR[],
                verb_stems VARCHAR[]
            )
            """
        )

    def insert_trend_docs(self, docs: Iterable[TrendDoc]) -> None:
        """Bulk insert or replace trend documents."""

        pending = False
        for doc in docs:
            pending = True
            self._buffer.append(doc)
            if len(self._buffer) >= self._buffer_limit:
                self._flush_buffer()

        if not pending:
            return

    def close(self) -> None:
        self.flush()
        self._conn.close()

    def flush(self) -> bool:
        """Force flushing any buffered trend documents to DuckDB.

        Returns True if data was persisted, False otherwise.
        """

        return self._flush_buffer()

    def _flush_buffer(self) -> bool:
        if not self._buffer:
            return False

        prepared = [
            (
                doc.item_id,
                doc.feed_id,
                doc.language,
                doc.pub_date,
                list(doc.categories) if doc.categories else None,
                list(doc.noun_stems) if doc.noun_stems else None,
                list(doc.verb_stems) if doc.verb_stems else None,
            )
            for doc in self._buffer
        ]
        self._buffer.clear()

        self._conn.executemany(
            """
            INSERT OR REPLACE INTO trend_docs
            (item_id, feed_id, language, pub_date, categories, noun_stems, verb_stems)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            prepared,
        )
        return True


__all__ = ["DuckDBStore", "TrendDoc"]


def _normalize_database_target(target: str) -> str:
    if _is_memory_target(target):
        return ":memory:"
    return target


def _is_memory_target(target: str) -> bool:
    return target.strip().lower() in _MEMORY_ALIASES
