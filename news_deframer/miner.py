"""Miner service responsible for handling processed items."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Optional
from uuid import UUID

from news_deframer.config import Config
from news_deframer.postgres import Postgres, Trend
from news_deframer.nlp import extract_stems, sanitize_text, stem_category


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MiningTask:
    feed_id: UUID
    item_id: UUID
    language: str
    categories: list[str]
    title: Optional[str]
    description: Optional[str]
    pub_date: datetime | None = None
    feed_url: Optional[str] = None
    root_domain: Optional[str] = None


class Miner:
    """Encapsulates business logic for handling mined items."""

    def __init__(self, config: Config, repository: Postgres):
        self.config = config
        self._logger = logger.getChild("Miner")
        self._repository = repository

    def mine_item(self, task: MiningTask) -> None:
        """Persist or otherwise process a single mined item.

        Currently this is a placeholder that simply logs the provided task.
        """

        task.title = sanitize_text(task.title)
        task.description = sanitize_text(task.description)

        title_text = task.title or ""
        description_text = task.description or ""
        content = f"{title_text}{' ' if title_text else ''}{description_text}"

        category_stems = []
        for c in task.categories:
            if stemmed := stem_category(sanitize_text(c), task.language):
                category_stems.append(stemmed)

        noun_stems, verb_stems = extract_stems(
            content,
            task.language,
            title=task.title,
            description=task.description,
        )

        trend = Trend(
            item_id=task.item_id,
            feed_id=task.feed_id,
            language=task.language,
            pub_date=task.pub_date or datetime.now(),
            root_domain=task.root_domain or "",
            category_stems=category_stems,
            noun_stems=list(noun_stems),
            verb_stems=list(verb_stems),
        )
        self._repository.upsert_trend(trend)
