"""Poller worker routines."""

from __future__ import annotations

import logging
import signal
import time
from types import FrameType
from typing import Any, Optional, cast
from uuid import UUID

import xmltodict  # type: ignore

from news_deframer.config import (
    DEFAULT_LOCK_DURATION,
    IDLE_SLEEP_TIME,
    POLLING_INTERVAL,
    Config,
)
from news_deframer.netutil import get_root_domain
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
            if poll_next_feed(config, miner, repository):
                logger.info("A feed was mined")
                continue

            logger.info("Sleeping... duration=%s", IDLE_SLEEP_TIME)
            time.sleep(IDLE_SLEEP_TIME)
    except KeyboardInterrupt:
        logger.info("Poll interrupted. Exiting.")
    finally:
        _restore_sigterm_handler(previous_sigterm)


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
            # logger.debug(
            #     "Mining item %s | Title: %s | Description: %s",
            #     item.id,
            #     task.title,
            #     task.description,
            # )
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

    repository.mark_items_mined(processed_ids)

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
    domain = feed.root_domain or get_root_domain(feed.url)
    title, description = _extract_title_and_description(item.content)
    return MiningTask(
        feed_id=feed.id,
        feed_url=feed.url,
        root_domain=domain,
        item_id=item.id,
        language=language,
        categories=categories,
        title=title,
        description=description,
        pub_date=item.pub_date,
    )


def _extract_title_and_description(content: str) -> tuple[Optional[str], Optional[str]]:
    # Wrap content to handle XML fragments (multiple root elements)
    wrapped = f"<root>{content}</root>"

    try:
        # process_namespaces=False ensures we don't fail on unbound prefixes
        doc = xmltodict.parse(wrapped, process_namespaces=False)
    except Exception:
        return None, None

    def find_text(obj: Any, key: str) -> Optional[str]:
        if isinstance(obj, dict):
            if key in obj:
                val = obj[key]
                if isinstance(val, str):
                    return val.strip()
                if isinstance(val, dict):
                    return val.get("#text", "").strip() or None
                return None
            for v in obj.values():
                if found := find_text(v, key):
                    return found
        elif isinstance(obj, list):
            for item in obj:
                if found := find_text(item, key):
                    return found
        return None

    title = find_text(doc, "deframer:title_original")
    desc = find_text(doc, "deframer:description_original")
    return title, desc
