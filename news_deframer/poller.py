"""Poller worker routines."""

from __future__ import annotations

import logging
import signal
import time
from types import FrameType
from typing import Any, Optional, cast

from news_deframer.config import (
    DEFAULT_LOCK_DURATION,
    IDLE_SLEEP_TIME,
    POLLING_INTERVAL,
    Config,
)
from news_deframer.netutil import (
    flush_domain_cache,
    get_base_domain_name,
    get_root_domain,
)
from news_deframer.postgres import Feed, Item, Postgres
from news_deframer.miner import Miner, MiningTask

logger = logging.getLogger(__name__)


def poll(config: Config) -> None:
    logger.info("Miner poll started. Press Ctrl+C to exit.")
    logger.debug("Loaded configuration: log level=%s", config.log_level)

    repository = Postgres(config)
    miner = Miner(config, repository=repository)

    previous_sigterm = _install_sigterm_handler()
    try:
        while True:
            items_mined = poll_next_feed(config, miner, repository)
            if items_mined:
                logger.info("Mined %s items", items_mined)
                continue

            logger.info("Sleeping... duration=%s", IDLE_SLEEP_TIME)
            time.sleep(IDLE_SLEEP_TIME)
    except KeyboardInterrupt:
        logger.info("Poll interrupted. Exiting.")
    finally:
        _restore_sigterm_handler(previous_sigterm)


def poll_next_feed(
    config: Config, miner: Miner, repository: Optional[Any] = None
) -> int:
    repo = repository or Postgres(config)
    logger.info("poll_next_feed")

    try:
        feed = repo.begin_mine_update(DEFAULT_LOCK_DURATION)
    except Exception as exc:  # pragma: no cover - db failure path
        logger.error("Failed to query next feed to mine", exc_info=exc)
        return 0

    if feed is None:
        return 0

    items_mined = 0
    try:
        items_mined = poll_feed(feed, miner, repo)
    except Exception as exc:  # pragma: no cover - mining failure path
        logger.error(
            "Feed mining failed", extra={"feed_id": str(feed.id)}, exc_info=exc
        )
    finally:
        try:
            repo.end_mine_update(feed.id, POLLING_INTERVAL)
        except Exception as exc:  # pragma: no cover - db failure path
            logger.error(
                "Failed to end feed update",
                extra={"feed_id": str(feed.id)},
                exc_info=exc,
            )

    return items_mined


def poll_feed(feed: Feed, miner: Miner, repository: Any) -> int:
    mined_count = 0
    items = repository.fetch_pending_items(feed.id, feed.url)
    feed_label = feed.url or str(feed.id)
    if not items:
        logger.info("No pending items to mine for feed %s", feed_label)
        return 0

    # flush cache
    flush_domain_cache()

    logger.info("Fetched %s pending items for feed %s", len(items), feed_label)
    for item in items:
        try:
            task = _build_task(feed, item)
            # logger.debug(
            #     "Mining item %s | Title: %s | Description: %s",
            #     item.id,
            #     task.title,
            #     task.description,
            # )
            miner.mine_item(task)
            mined_count += 1
        except Exception as exc:  # pragma: no cover - per-item failure
            logger.error(
                "Failed to process item",
                extra={
                    "feed_url": feed.url,
                    "item_id": str(item.id),
                },
                exc_info=exc,
            )
            return mined_count

    return mined_count


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
    domain = get_root_domain(feed.url)
    base_domain = get_base_domain_name(feed.url)

    # we split here for apollo-news also at the -
    stop_words = (
        [w for w in base_domain.split("-") if len(w) >= 3] if base_domain else []
    )

    think_result = item.think_result
    if item.pub_date is None:
        raise RuntimeError(f"Missing pub_date for item {item.id}")

    return MiningTask(
        feed_id=feed.id,
        feed_url=feed.url,
        root_domain=domain,
        item_id=item.id,
        language=language,
        categories=categories,
        title_deframed=think_result.title_corrected if think_result else None,
        description_deframed=think_result.description_corrected
        if think_result
        else None,
        title_original=think_result.title_original if think_result else None,
        description_original=think_result.description_original
        if think_result
        else None,
        pub_date=item.pub_date,
        stop_words=stop_words,
    )
