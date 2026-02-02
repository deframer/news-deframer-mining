"""Poller worker routines."""

from __future__ import annotations

from html.parser import HTMLParser
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

    try:
        poll_feed(feed, miner, repo)
    except Exception as exc:  # pragma: no cover - mining failure path
        logger.error(
            "Feed mining failed", extra={"feed_id": str(feed.id)}, exc_info=exc
        )

    try:
        repo.end_mine_update(feed.id, POLLING_INTERVAL)
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
    title, description = _extract_title_and_description(item.content, item_id=item.id)
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


class _DeframerParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.data: dict[str, Optional[str]] = {
            "deframer:title_original": None,
            "deframer:description_original": None,
        }
        self._current: Optional[str] = None
        self._buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag in self.data:
            self._current = tag
            self._buffer = []

    def handle_endtag(self, tag: str) -> None:
        if tag == self._current:
            self.data[tag] = "".join(self._buffer).strip() or None
            self._current = None

    def handle_data(self, data: str) -> None:
        if self._current:
            self._buffer.append(data)


def _extract_title_and_description(
    content: str, item_id: Optional[UUID] = None
) -> tuple[Optional[str], Optional[str]]:
    parser = _DeframerParser()
    try:
        parser.feed(content)
        parser.close()
    except Exception as exc:
        if item_id:
            logger.error(
                "Failed to parse content", extra={"item_id": str(item_id)}, exc_info=exc
            )
        return None, None
    return parser.data["deframer:title_original"], parser.data[
        "deframer:description_original"
    ]
