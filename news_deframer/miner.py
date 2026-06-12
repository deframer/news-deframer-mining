"""Miner service responsible for handling processed items."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import logging
from typing import Optional
from uuid import UUID

from news_deframer.config import Config
from news_deframer.postgres import Postgres, Trend
from news_deframer.nlp import extract_stems, sanitize_text, stem_category
from news_deframer.sentiments import extract_sentiments_array


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MiningTask:
    feed_id: UUID
    item_id: UUID
    language: str
    categories: list[str]
    title_deframed: Optional[str]
    description_deframed: Optional[str]
    title_original: Optional[str]
    description_original: Optional[str]
    pub_date: datetime
    root_domain: str
    feed_url: Optional[str] = None
    stop_words: list[str] = field(default_factory=list)


class Miner:
    """Encapsulates business logic for handling mined items."""

    def __init__(self, config: Config, repository: Postgres):
        self.config = config
        self._logger = logger.getChild("Miner")
        self._repository = repository

    def mine_item(self, task: MiningTask) -> None:
        """Process a single mined item."""

        # turn off ner (spicy is buggy)
        with_ner = False

        task.title_original = sanitize_text(task.title_original)
        task.description_original = sanitize_text(task.description_original)
        content = f"{task.title_original}{' ' if task.title_original else ''}{task.description_original}"

        noun_stems, verb_stems, adj_stems = extract_stems(
            content,
            task.language,
            stop_words=task.stop_words,
            with_ner=with_ner,
            config=self.config,
        )
        sentiments = (
            extract_sentiments_array(
                (noun_stems, verb_stems, adj_stems),
                task.language,
                config=self.config,
            )
            or {}
        )

        task.title_deframed = sanitize_text(task.title_deframed)
        task.description_deframed = sanitize_text(task.description_deframed)
        content_deframed = f"{task.title_deframed}{' ' if task.title_deframed else ''}{task.description_deframed}"

        # we don't store the deframed stems - we only use them for creating the deframed sentiments
        noun_stems_deframed, verb_stems_deframed, adj_stems_deframed = extract_stems(
            content_deframed,
            task.language,
            stop_words=task.stop_words,
            with_ner=with_ner,
            config=self.config,
        )
        sentiments_deframed = (
            extract_sentiments_array(
                (noun_stems_deframed, verb_stems_deframed, adj_stems_deframed),
                task.language,
                config=self.config,
            )
            or {}
        )

        category_stems = []
        for c in task.categories:
            if stemmed := stem_category(
                sanitize_text(c),
                task.language,
                stop_words=task.stop_words,
                config=self.config,
            ):
                category_stems.append(stemmed)

        trend = Trend(
            item_id=task.item_id,
            feed_id=task.feed_id,
            language=task.language,
            pub_date=task.pub_date,
            category_stems=category_stems,
            noun_stems=list(noun_stems),
            verb_stems=list(verb_stems),
            adjective_stems=list(adj_stems),
            root_domain=task.root_domain,
            sentiments=sentiments,
            sentiments_deframed=sentiments_deframed,
        )
        self._repository.upsert_trends([trend])
