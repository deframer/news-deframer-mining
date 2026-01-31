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


@dataclass
class Feed:
    id: UUID
    categories: list[str] = field(default_factory=list)
    language: Optional[str] = None
    url: Optional[str] = None


@dataclass
class Item:
    id: UUID
    feed_id: UUID
    categories: list[str] = field(default_factory=list)
    language: Optional[str] = None
    pub_date: datetime | None = None
    title: Optional[str] = None
    description: Optional[str] = None


register_uuid()

logger = logging.getLogger(__name__)


class Postgres:
    """Implements the mining repository against Postgres."""

    def __init__(self, config: Config):
        self.config = config

    def begin_mine_update(self, lock_duration: int) -> Optional[Feed]:
        """Attempt to lock the next feed ready for mining."""
        lock_seconds = max(int(lock_duration), 0)
        select_sql = """
            SELECT fs.id, f.categories, f.language, f.url
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
                    logger.debug("No feeds eligible for mining")
                    return None

                feed_id = row[0]
                categories = row[1] or []
                language = row[2]
                url = row[3]
                cur.execute(update_sql, (lock_seconds, feed_id))
                feed_url = str(url) if url is not None else None
                feed_label = feed_url or str(feed_id)
                logger.debug("Locked feed %s for mining", feed_label)
                return Feed(
                    id=feed_id,
                    categories=list(categories),
                    language=_normalize_language_value(language),
                    url=feed_url,
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
                    "SELECT enabled, mining FROM feeds WHERE id = %s",
                    (feed_id,),
                )
                row = cur.fetchone()
                enabled = bool(row[0]) if row else False
                mining = bool(row[1]) if row else False

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
                    logger.debug("Marked feed %s mining error", feed_id)
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
                    logger.debug("Feed %s mining complete; scheduled next run", feed_id)
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
                    logger.debug(
                        "Feed %s mining complete; no further schedule", feed_id
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
                think_result ->> 'title_original' AS title,
                think_result ->> 'description_original' AS description
            FROM items
            WHERE feed_id = %s
              AND mining_done_at IS NULL
              AND think_result IS NOT NULL
              AND think_error IS NULL
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
                        title=row[5],
                        description=row[6],
                    )
                    for row in rows
                ]
                label = feed_url or str(feed_id)
                logger.debug("Fetched %s pending items for feed %s", len(items), label)
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
        logger.debug("Marked %s items as mined", len(item_ids))


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
