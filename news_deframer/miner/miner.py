"""Miner worker routines."""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from news_deframer.config import (
    DEFAULT_LOCK_DURATION,
    IDLE_SLEEP_TIME,
    POLLING_INTERVAL,
    Config,
)
from news_deframer.database.postgres import Feed, Item, Postgres

logger = logging.getLogger(__name__)


def poll(config: Config) -> None:
    """Run the polling loop until interrupted."""
    logger.info("Miner poll started. Press Ctrl+C to exit.")
    logger.debug("Loaded configuration: log level=%s", config.log_level)

    repository = Postgres(config)

    try:
        while True:
            if mine_next_feed(config, repository):
                logger.info("A feed was mined")
                continue

            logger.info("Sleeping... duration=%s", IDLE_SLEEP_TIME)
            time.sleep(IDLE_SLEEP_TIME)
    except KeyboardInterrupt:
        logger.info("Poll interrupted. Exiting.")


def mine_next_feed(config: Config, repository: Optional[Any] = None) -> bool:
    """Attempt to mine the next feed; returns True when work was done."""
    repo = repository or Postgres(config)
    logger.info("mine_next_feed")

    try:
        feed = repo.begin_mine_update(DEFAULT_LOCK_DURATION)
    except Exception as exc:  # pragma: no cover - db failure path
        logger.error("Failed to query next feed to mine", exc_info=exc)
        return False

    if feed is None:
        return False

    mining_error: Exception | None = None
    try:
        mine_feed(feed, repo)
    except Exception as exc:  # pragma: no cover - mining failure path
        mining_error = exc
        logger.error(
            "Feed mining failed", extra={"feed_id": str(feed.id)}, exc_info=exc
        )

    try:
        repo.end_mine_update(feed.id, mining_error, POLLING_INTERVAL)
    except Exception as exc:  # pragma: no cover - db failure path
        logger.error(
            "Failed to end feed update",
            extra={"feed_id": str(feed.id)},
            exc_info=exc,
        )

    return True


def mine_feed(feed: Feed, repository: Any) -> None:
    """Fetch pending items for the feed and process each one."""
    items = repository.fetch_pending_items(feed.id, feed.url)
    items = [item for item in items if item.feed_id == feed.id]
    feed_label = feed.url or str(feed.id)
    if not items:
        logger.info("No pending items to mine for feed %s", feed_label)
        return

    logger.info("Fetched %s pending items for feed %s", len(items), feed_label)
    for item in items:
        mine_item(feed, item)


def mine_item(feed: Feed, item: Item) -> None:
    """Placeholder implementation that logs the processed item."""
    language = item.language or feed.language or "en"
    if language == "en" and not (item.language or feed.language):
        logger.warning(
            "Missing language metadata; falling back to 'en'",
            extra={
                "feed_url": feed.url,
                "item_id": str(item.id),
            },
        )

    categories = sorted({*feed.categories, *item.categories})
    logger.info(
        "Processed feed item",
        extra={
            "feed_url": feed.url,
            "item_id": str(item.id),
            "language": language,
            "categories": categories,
            "title": item.title,
            "description": item.description,
        },
    )
