"""Miner service responsible for handling processed items."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Optional

from news_deframer.config import Config


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MiningTask:
    feed_id: str
    feed_url: Optional[str]
    item_id: str
    language: str
    categories: list[str]
    title: Optional[str]
    description: Optional[str]


class Miner:
    """Encapsulates business logic for handling mined items."""

    def __init__(self, config: Config):
        self.config = config
        self._logger = logger.getChild("Miner")

    def mine_item(self, task: MiningTask) -> None:
        """Persist or otherwise process a single mined item.

        Currently this is a placeholder that simply logs the provided task.
        """

        self._logger.info(
            "Processed feed item",
            extra={
                "feed_id": task.feed_id,
                "feed_url": task.feed_url,
                "item_id": task.item_id,
                "language": task.language,
                "categories": task.categories,
                "title": task.title,
                "description": task.description,
            },
        )
