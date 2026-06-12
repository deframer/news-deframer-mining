"""Postgres-backed repository helpers."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

import psycopg2
from psycopg2.extras import Json, execute_values, register_uuid

from news_deframer.config import Config
from news_deframer.logger import SilentLogger
from news_deframer.sentiments import Sentiment


@dataclass
class Feed:
    id: UUID
    url: str
    categories: list[str] = field(default_factory=list)
    language: Optional[str] = None


@dataclass
class ThinkResult:
    title_original: Optional[str] = None
    title_corrected: Optional[str] = None
    description_original: Optional[str] = None
    description_corrected: Optional[str] = None


@dataclass
class Item:
    id: UUID
    feed_id: Optional[UUID] = None
    pub_date: Optional[datetime] = None
    categories: list[str] = field(default_factory=list)
    language: Optional[str] = None
    think_result: Optional[ThinkResult] = None


@dataclass
class Trend:
    item_id: UUID
    language: str
    feed_id: Optional[UUID] = None
    pub_date: Optional[datetime] = None
    root_domain: Optional[str] = None
    category_stems: list[str] = field(default_factory=list)
    noun_stems: list[str] = field(default_factory=list)
    verb_stems: list[str] = field(default_factory=list)
    adjective_stems: list[str] = field(default_factory=list)
    sentiments: Sentiment = field(default_factory=lambda: _empty_sentiment())
    sentiments_deframed: Sentiment = field(default_factory=lambda: _empty_sentiment())


register_uuid()

logger = logging.getLogger(__name__)


class Postgres:
    """Implements the mining repository against Postgres."""

    def __init__(self, config: Config):
        self.config = config
        self._conn = None
        if config.log_database:
            self._logger: logging.Logger | SilentLogger = logger.getChild("Postgres")
        else:
            self._logger = SilentLogger()

    def _get_connection(self):
        if self._conn is None or (hasattr(self._conn, "closed") and self._conn.closed):
            self._conn = psycopg2.connect(self.config.dsn)
        return self._conn

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

        conn = self._get_connection()
        with conn:
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
                feed_label = feed_url or str(feed_id)
                self._logger.debug("Locked feed %s for mining", feed_label)
                return Feed(
                    id=feed_id,
                    url=feed_url,
                    categories=list(categories),
                    language=_normalize_language_value(language),
                )

    def end_mine_update(self, feed_id: UUID, polling_interval: int) -> None:
        """Release the lock and update scheduling metadata."""
        polling_seconds = max(int(polling_interval), 0)

        conn = self._get_connection()
        with conn:
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

                if enabled and mining:
                    update_sql = """
                        UPDATE feed_schedules
                        SET mining_locked_until = NULL,
                            updated_at = NOW(),
                            next_mining_at = NOW() + (%s * INTERVAL '1 second')
                        WHERE id = %s
                    """
                    cur.execute(update_sql, (polling_seconds, feed_id))
                    self._logger.debug(
                        "Feed %s mining complete; scheduled next run", feed_label
                    )
                else:
                    update_sql = """
                        UPDATE feed_schedules
                        SET mining_locked_until = NULL,
                            updated_at = NOW(),
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
                i.id,
                i.categories,
                i.language,
                i.pub_date,
                i.think_result->>'title_original' AS title_original,
                i.think_result->>'title_corrected' AS title_corrected,
                i.think_result->>'description_original' AS description_original,
                i.think_result->>'description_corrected' AS description_corrected
            FROM items i
            LEFT JOIN trends t ON t.item_id = i.id
            WHERE i.feed_id = %s
              AND i.think_result IS NOT NULL
              AND i.think_error_count = 0
              AND t.item_id IS NULL
        """

        conn = self._get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (feed_id,))
                rows = cur.fetchall()
                items = [
                    Item(
                        id=row[0],
                        categories=list(row[1] or []),
                        language=_normalize_language_value(row[2]),
                        pub_date=row[3],
                        think_result=ThinkResult(
                            title_original=row[4],
                            title_corrected=row[5],
                            description_original=row[6],
                            description_corrected=row[7],
                        ),
                    )
                    for row in rows
                ]
                label = feed_url or str(feed_id)
                self._logger.debug(
                    "Fetched %s pending items for feed %s", len(items), label
                )
                return items

    def _fetch_items_by_ids(self, item_ids: list[UUID]) -> dict[UUID, Item]:
        """Fetch items by their IDs."""
        if not item_ids:
            return {}

        # Use execute_values approach for safe parameter substitution
        placeholders = ",".join(["%s"] * len(item_ids))
        sql = f"""
            SELECT
                i.id,
                i.feed_id,
                i.think_result->>'title_original' AS title_original,
                i.think_result->>'title_corrected' AS title_corrected,
                i.think_result->>'description_original' AS description_original,
                i.think_result->>'description_corrected' AS description_corrected
            FROM items i
            WHERE i.id IN ({placeholders})
              AND i.think_result IS NOT NULL
              AND i.think_error_count = 0
        """

        conn = self._get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, item_ids)
                rows = cur.fetchall()
                items = {}
                for row in rows:
                    item = Item(
                        id=row[0],
                        feed_id=row[1],
                        think_result=ThinkResult(
                            title_original=row[2],
                            title_corrected=row[3],
                            description_original=row[4],
                            description_corrected=row[5],
                        ),
                    )
                    items[row[0]] = item
                return items

    def _fetch_feeds_by_ids(self, feed_ids: list[UUID]) -> dict[UUID, Feed]:
        """Fetch feeds by their IDs."""
        if not feed_ids:
            return {}

        # Use execute_values approach for safe parameter substitution
        placeholders = ",".join(["%s"] * len(feed_ids))
        sql = f"""
            SELECT
                f.id,
                f.url
            FROM feeds f
            WHERE f.id IN ({placeholders})
        """

        conn = self._get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, feed_ids)
                rows = cur.fetchall()
                feeds = {}
                for row in rows:
                    feed = Feed(id=row[0], url=row[1])
                    feeds[row[0]] = feed
                return feeds

    def upsert_trends(self, trends: list[Trend]) -> None:
        """Insert or update multiple trend records in batch."""
        if not trends:
            return

        sql = """
            INSERT INTO trends (
                item_id,
                feed_id,
                language,
                pub_date,
                category_stems,
                noun_stems,
                verb_stems,
                adjective_stems,
                root_domain,
                sentiments,
                sentiments_deframed
            ) VALUES %s
            ON CONFLICT (item_id) DO UPDATE SET
                feed_id = EXCLUDED.feed_id,
                language = EXCLUDED.language,
                pub_date = EXCLUDED.pub_date,
                category_stems = EXCLUDED.category_stems,
                noun_stems = EXCLUDED.noun_stems,
                verb_stems = EXCLUDED.verb_stems,
                adjective_stems = EXCLUDED.adjective_stems,
                root_domain = EXCLUDED.root_domain,
                sentiments = EXCLUDED.sentiments,
                sentiments_deframed = EXCLUDED.sentiments_deframed
        """

        values = [
            (
                t.item_id,
                t.feed_id,
                t.language,
                t.pub_date,
                t.category_stems,
                t.noun_stems,
                t.verb_stems,
                t.adjective_stems,
                t.root_domain,
                Json(t.sentiments),
                Json(t.sentiments_deframed),
            )
            for t in trends
        ]

        conn = self._get_connection()
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, values)
        self._logger.debug("Upserted %s trends", len(trends))

    def fetch_trends_without_sentiments(
        self, limit: int = 100
    ) -> tuple[list[Trend], dict[UUID, tuple[Item, Feed]]]:
        """Fetch trends whose sentiment payload is still empty."""
        # First, get the trends that need processing (without item and feed data to avoid transferring large content unnecessarily)
        sql_trends = """
            SELECT
                t.item_id,
                t.language,
                t.noun_stems,
                t.verb_stems,
                t.adjective_stems,
                t.sentiments->>'v' AS sentiments_v,
                t.sentiments->>'a' AS sentiments_a,
                t.sentiments->>'d' AS sentiments_d,
                t.sentiments->>'j' AS sentiments_j,
                t.sentiments->>'a_n' AS sentiments_a_n,
                t.sentiments->>'s' AS sentiments_s,
                t.sentiments->>'f' AS sentiments_f,
                t.sentiments->>'d_g' AS sentiments_d_g,
                t.sentiments_deframed->>'v' AS sentiments_deframed_v,
                t.sentiments_deframed->>'a' AS sentiments_deframed_a,
                t.sentiments_deframed->>'d' AS sentiments_deframed_d,
                t.sentiments_deframed->>'j' AS sentiments_deframed_j,
                t.sentiments_deframed->>'a_n' AS sentiments_deframed_a_n,
                t.sentiments_deframed->>'s' AS sentiments_deframed_s,
                t.sentiments_deframed->>'f' AS sentiments_deframed_f,
                t.sentiments_deframed->>'d_g' AS sentiments_deframed_d_g
            FROM trends t
            WHERE t.sentiments = '{}'::jsonb OR t.sentiments_deframed = '{}'::jsonb
            ORDER BY t.pub_date ASC
            LIMIT %s
        """

        conn = self._get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql_trends, (max(int(limit), 0),))
                rows = cur.fetchall()

                # Build Trend objects and identify which ones need item/feed data
                trends = []
                item_ids_needed = (
                    set()
                )  # item_ids where we need to load full item/feed data
                for row in rows:
                    sentiments = _sentiment_from_row(row, 5)
                    sentiments_deframed = _sentiment_from_row(row, 13)
                    trend = Trend(
                        item_id=row[0],
                        language=row[1],
                        noun_stems=list(row[2] or []),
                        verb_stems=list(row[3] or []),
                        adjective_stems=list(row[4] or []),
                        sentiments=sentiments,
                        sentiments_deframed=sentiments_deframed,
                    )
                    trends.append(trend)

                    # Only load full item and feed when sentiments_deframed is empty
                    if sentiments_deframed == {}:
                        item_ids_needed.add(row[0])

        # If we need item/feed data, fetch it in batches
        item_feed_map: dict[UUID, tuple[Item, Feed]] = {}
        if item_ids_needed:
            # Fetch all needed items in one query
            items_by_id = self._fetch_items_by_ids(list(item_ids_needed))

            # Get unique feed_ids from the items we fetched
            feed_ids_needed = {item.feed_id for item in items_by_id.values()}

            # Fetch all needed feeds in one query
            feeds_by_id = self._fetch_feeds_by_ids(list(feed_ids_needed))

            # Combine items and feeds into the map
            for item_id in item_ids_needed:
                if item_id in items_by_id:
                    item = items_by_id[item_id]
                    if item.feed_id in feeds_by_id:
                        item_feed_map[item_id] = (item, feeds_by_id[item.feed_id])

        self._logger.debug("Fetched %s trends without sentiments", len(trends))
        return trends, item_feed_map

    def update_trend_sentiments(
        self, sentiments_by_item_id: dict[UUID, Sentiment]
    ) -> None:
        """Batch update trend sentiments when the target rows are still empty."""
        if not sentiments_by_item_id:
            return

        sql = """
            UPDATE trends AS t
            SET sentiments = data.sentiments::jsonb
            FROM (VALUES %s) AS data(item_id, sentiments)
            WHERE t.item_id = data.item_id
              AND t.sentiments = '{}'::jsonb
        """
        values = [
            (item_id, Json(sentiment))
            for item_id, sentiment in sentiments_by_item_id.items()
        ]

        conn = self._get_connection()
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, values)
        self._logger.debug("Updated sentiments for %s trends", len(values))

    def update_trend_deframed_sentiments(
        self, sentiments_by_item_id: dict[UUID, Sentiment]
    ) -> None:
        """Batch update trend deframed sentiments when the target rows are still empty."""
        if not sentiments_by_item_id:
            return

        sql = """
            UPDATE trends AS t
            SET sentiments_deframed = data.sentiments::jsonb
            FROM (VALUES %s) AS data(item_id, sentiments)
            WHERE t.item_id = data.item_id
              AND t.sentiments_deframed = '{}'::jsonb
        """
        values = [
            (item_id, Json(sentiment))
            for item_id, sentiment in sentiments_by_item_id.items()
        ]

        conn = self._get_connection()
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, values)
        self._logger.debug("Updated deframed sentiments for %s trends", len(values))


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


def _empty_sentiment() -> Sentiment:
    return {}


def _sentiment_from_row(row: tuple[Any, ...], offset: int) -> Sentiment:
    keys = ("v", "a", "d", "j", "a_n", "s", "f", "d_g")
    sentiment: Sentiment = {}
    for index, key in enumerate(keys):
        raw = row[offset + index]
        if raw is None:
            continue
        value = float(raw)
        if key == "v":
            sentiment["v"] = value
        elif key == "a":
            sentiment["a"] = value
        elif key == "d":
            sentiment["d"] = value
        elif key == "j":
            sentiment["j"] = value
        elif key == "a_n":
            sentiment["a_n"] = value
        elif key == "s":
            sentiment["s"] = value
        elif key == "f":
            sentiment["f"] = value
        elif key == "d_g":
            sentiment["d_g"] = value
    return sentiment
