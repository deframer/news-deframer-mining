"""Poller worker routines."""

from __future__ import annotations

import logging
import signal
import time
from types import FrameType
from typing import Any, Optional, cast
from uuid import UUID

from news_deframer.config import (
    DEFAULT_LOCK_DURATION,
    IDLE_SLEEP_TIME,
    POLLING_INTERVAL,
    Config,
)
from news_deframer.postgres import Feed, Item, Postgres
from news_deframer.miner import Miner, MiningTask
from news_deframer.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


def poll(config: Config, store: DuckDBStore | None = None) -> None:
    logger.info("Miner poll started. Press Ctrl+C to exit.")
    logger.debug("Loaded configuration: log level=%s", config.log_level)

    repository = Postgres(config)
    owns_store = False
    duck_store = store
    if duck_store is None:
        duck_store = DuckDBStore(config.duck_db_file)
        owns_store = True
    miner = Miner(config, store=duck_store)

    previous_sigterm = _install_sigterm_handler()
    try:
        while True:
            if poll_next_feed(config, miner, repository):
                logger.info("A feed was mined")
                continue

            logger.info("Sleeping... duration=%s", IDLE_SLEEP_TIME)
            time.sleep(IDLE_SLEEP_TIME)
    except KeyboardInterrupt:
        logger.info("Poll interrupted. Exiting.")
    finally:
        _restore_sigterm_handler(previous_sigterm)
        if duck_store is not None:
            duck_store.flush()
        if owns_store and duck_store is not None:
            duck_store.close()


def poll_next_feed(
    config: Config, miner: Miner, repository: Optional[Any] = None
) -> bool:
    repo = repository or Postgres(config)
    logger.info("poll_next_feed")

    try:
        feed = repo.begin_mine_update(DEFAULT_LOCK_DURATION)
    except Exception as exc:  # pragma: no cover - db failure path
        logger.error("Failed to query next feed to mine", exc_info=exc)
        return False

    if feed is None:
        return False

    poll_error: Exception | None = None
    try:
        poll_feed(feed, miner, repo)
    except Exception as exc:  # pragma: no cover - mining failure path
        poll_error = exc
        logger.error(
            "Feed mining failed", extra={"feed_id": str(feed.id)}, exc_info=exc
        )

    try:
        repo.end_mine_update(feed.id, poll_error, POLLING_INTERVAL)
    except Exception as exc:  # pragma: no cover - db failure path
        logger.error(
            "Failed to end feed update",
            extra={"feed_id": str(feed.id)},
            exc_info=exc,
        )

    return True


def poll_feed(feed: Feed, miner: Miner, repository: Any) -> Optional[Exception]:
    items = repository.fetch_pending_items(feed.id, feed.url)
    items = [item for item in items if item.feed_id == feed.id]
    feed_label = feed.url or str(feed.id)
    if not items:
        logger.info("No pending items to mine for feed %s", feed_label)
        return None

    logger.info("Fetched %s pending items for feed %s", len(items), feed_label)
    processed_ids: list[UUID] = []
    for item in items:
        try:
            task = _build_task(feed, item)
            miner.mine_item(task)
        except Exception as exc:  # pragma: no cover - per-item failure
            logger.error(
                "Failed to process item",
                extra={
                    "feed_url": feed.url,
                    "item_id": str(item.id),
                },
                exc_info=exc,
            )
            return exc
        else:
            processed_ids.append(item.id)

    # repository.mark_items_mined(processed_ids)
    logger.warning(
        "Skipping persistence of mined items; database not updated",
        extra={"feed_url": feed.url, "processed_count": len(processed_ids)},
    )

    return None


def _install_sigterm_handler() -> signal.Handlers | None:
    if not hasattr(signal, "SIGTERM"):
        return None

    previous = signal.getsignal(signal.SIGTERM)

    def _handle_sigterm(signum: int, _: Optional[FrameType]) -> None:
        try:
            signal_name = signal.Signals(signum).name
        except ValueError:  # pragma: no cover - unexpected signal value
            signal_name = str(signum)
        logger.info("Received %s; initiating graceful shutdown", signal_name)
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return cast(signal.Handlers, previous)


def _restore_sigterm_handler(previous: signal.Handlers | None) -> None:
    if previous is None or not hasattr(signal, "SIGTERM"):
        return
    signal.signal(signal.SIGTERM, previous)


def _build_task(feed: Feed, item: Item) -> MiningTask:
    language = item.language or feed.language or "en"
    if language == "en" and not (item.language or feed.language):
        logger.warning(
            "Missing language metadata; falling back to 'en'",
            extra={"feed_url": feed.url, "item_id": str(item.id)},
        )

    categories = sorted({*feed.categories, *item.categories})
    return MiningTask(
        feed_id=str(feed.id),
        feed_url=feed.url,
        item_id=str(item.id),
        language=language,
        categories=categories,
        title=item.title,
        description=item.description,
        pub_date=item.pub_date,
    )
