"""Postgres-backed repository helpers."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import UUID

import psycopg2
from psycopg2.extras import register_uuid

from news_deframer.config import Config
from news_deframer.logger import SilentLogger


@dataclass
class Feed:
    id: UUID
    url: str
    categories: list[str] = field(default_factory=list)
    language: Optional[str] = None
    root_domain: Optional[str] = None


@dataclass
class Item:
    id: UUID
    feed_id: UUID
    content: str
    pub_date: datetime
    categories: list[str] = field(default_factory=list)
    language: Optional[str] = None


@dataclass
class Trend:
    item_id: UUID
    feed_id: UUID
    language: str
    pub_date: datetime
    root_domain: str
    category_stems: list[str] = field(default_factory=list)
    noun_stems: list[str] = field(default_factory=list)
    verb_stems: list[str] = field(default_factory=list)


register_uuid()

logger = logging.getLogger(__name__)


class Postgres:
    """Implements the mining repository against Postgres."""

    def __init__(self, config: Config):
        self.config = config
        if config.log_database:
            self._logger: logging.Logger | SilentLogger = logger.getChild("Postgres")
        else:
            self._logger = SilentLogger()

    def begin_mine_update(self, lock_duration: int) -> Optional[Feed]:
        """Attempt to lock the next feed ready for mining."""
        lock_seconds = max(int(lock_duration), 0)
        select_sql = """
            SELECT fs.id, f.categories, f.language, f.url, f.root_domain
            FROM feed_schedules AS fs
            JOIN feeds AS f ON f.id = fs.id
            WHERE fs.next_mining_at IS NOT NULL
              AND fs.next_mining_at <= NOW()
              AND (fs.mining_locked_until IS NULL OR fs.mining_locked_until < NOW())
              AND f.enabled = TRUE
              AND f.mining = TRUE
              AND (f.deleted_at IS NULL)
            ORDER BY fs.next_mining_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """
        update_sql = """
            UPDATE feed_schedules
            SET mining_locked_until = NOW() + (%s * INTERVAL '1 second'),
                updated_at = NOW()
            WHERE id = %s
        """

        with psycopg2.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(select_sql)
                row = cur.fetchone()
                if not row:
                    self._logger.debug("No feeds eligible for mining")
                    return None

                feed_id = row[0]
                categories = row[1] or []
                language = row[2]
                url = row[3]
                cur.execute(update_sql, (lock_seconds, feed_id))
                if url is None:
                    raise RuntimeError("Feed record missing URL")
                feed_url = str(url)
                root_domain = str(row[4]) if row[4] is not None else None
                feed_label = feed_url or str(feed_id)
                self._logger.debug("Locked feed %s for mining", feed_label)
                return Feed(
                    id=feed_id,
                    url=feed_url,
                    categories=list(categories),
                    language=_normalize_language_value(language),
                    root_domain=root_domain,
                )

    def end_mine_update(
        self, feed_id: UUID, error: Exception | None, retry_interval: int
    ) -> None:
        """Release the lock and update scheduling metadata."""
        error_text = str(error) if error else None
        retry_seconds = max(int(retry_interval), 0)

        with psycopg2.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT enabled, mining, url FROM feeds WHERE id = %s",
                    (feed_id,),
                )
                row = cur.fetchone()
                enabled = bool(row[0]) if row else False
                mining = bool(row[1]) if row else False
                feed_url = str(row[2]) if row and row[2] is not None else None
                feed_label = feed_url or str(feed_id)

                if error_text is not None:
                    update_sql = """
                        UPDATE feed_schedules
                        SET mining_locked_until = NULL,
                            updated_at = NOW(),
                            mining_error = %s,
                            next_mining_at = NULL
                        WHERE id = %s
                    """
                    cur.execute(update_sql, (error_text, feed_id))
                    self._logger.debug("Marked feed %s mining error", feed_label)
                    return

                if enabled and mining:
                    update_sql = """
                        UPDATE feed_schedules
                        SET mining_locked_until = NULL,
                            updated_at = NOW(),
                            mining_error = NULL,
                            next_mining_at = NOW() + (%s * INTERVAL '1 second')
                        WHERE id = %s
                    """
                    cur.execute(update_sql, (retry_seconds, feed_id))
                    self._logger.debug(
                        "Feed %s mining complete; scheduled next run", feed_label
                    )
                else:
                    update_sql = """
                        UPDATE feed_schedules
                        SET mining_locked_until = NULL,
                            updated_at = NOW(),
                            mining_error = NULL,
                            next_mining_at = NULL
                        WHERE id = %s
                    """
                    cur.execute(update_sql, (feed_id,))
                    self._logger.debug(
                        "Feed %s mining complete; no further schedule", feed_label
                    )

    def fetch_pending_items(
        self, feed_id: UUID, feed_url: Optional[str] = None
    ) -> list[Item]:
        """Fetch items for the feed that still need mining."""
        sql = """
            SELECT
                id,
                feed_id,
                categories,
                language,
                pub_date,
                content
            FROM items
            WHERE feed_id = %s
              AND mining_done_at IS NULL
        """

        with psycopg2.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (feed_id,))
                rows = cur.fetchall()
                items = [
                    Item(
                        id=row[0],
                        feed_id=row[1],
                        categories=list(row[2] or []),
                        language=_normalize_language_value(row[3]),
                        pub_date=row[4],
                        content=row[5],
                    )
                    for row in rows
                ]
                label = feed_url or str(feed_id)
                self._logger.debug(
                    "Fetched %s pending items for feed %s", len(items), label
                )
                return items

    def mark_items_mined(self, item_ids: list[UUID]) -> None:
        if not item_ids:
            return

        chunk_size = 100
        with psycopg2.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                for idx in range(0, len(item_ids), chunk_size):
                    chunk = item_ids[idx : idx + chunk_size]
                    cur.execute(
                        "UPDATE items SET mining_done_at = NOW() WHERE id = ANY(%s)",
                        (chunk,),
                    )
        self._logger.debug("Marked %s items as mined", len(item_ids))

    def upsert_trend(self, trend: Trend) -> None:
        """Insert or update a trend record."""
        sql = """
            INSERT INTO trends (
                item_id,
                feed_id,
                language,
                pub_date,
                categories,
                noun_stems,
                verb_stems,
                root_domain
            ) VALUES (
                %(item_id)s,
                %(feed_id)s,
                %(language)s,
                %(pub_date)s,
                %(category_stems)s,
                %(noun_stems)s,
                %(verb_stems)s,
                %(root_domain)s
            )
            ON CONFLICT (item_id) DO UPDATE SET
                feed_id = EXCLUDED.feed_id,
                language = EXCLUDED.language,
                pub_date = EXCLUDED.pub_date,
                categories = EXCLUDED.categories,
                noun_stems = EXCLUDED.noun_stems,
                verb_stems = EXCLUDED.verb_stems,
                root_domain = EXCLUDED.root_domain
        """

        with psycopg2.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "item_id": trend.item_id,
                        "feed_id": trend.feed_id,
                        "language": trend.language,
                        "pub_date": trend.pub_date,
                        "category_stems": trend.category_stems,
                        "noun_stems": trend.noun_stems,
                        "verb_stems": trend.verb_stems,
                        "root_domain": trend.root_domain,
                    },
                )
        self._logger.debug("Upserted trend for item %s", trend.item_id)


def _normalize_language_value(value: Optional[str]) -> Optional[str]:
    if not isinstance(value, str):
        return None
    stripped = value.strip().lower()
    filtered = "".join(ch for ch in stripped if ch.isalpha())
    if len(filtered) >= 2:
        return filtered[:2]
    if len(stripped) >= 2:
        return stripped[:2]
    return None
