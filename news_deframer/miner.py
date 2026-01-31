"""Miner service responsible for handling processed items."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Optional

from news_deframer.config import Config
from news_deframer.duckdb_store import DuckDBStore, TrendDoc
from news_deframer.nlp import extract_stems, sanitize_text


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
    pub_date: datetime | None = None


class Miner:
    """Encapsulates business logic for handling mined items."""

    def __init__(self, config: Config, store: DuckDBStore | None = None):
        self.config = config
        self._logger = logger.getChild("Miner")
        self._store = store or DuckDBStore(config.duck_db_file)

    def mine_item(self, task: MiningTask) -> None:
        """Persist or otherwise process a single mined item.

        Currently this is a placeholder that simply logs the provided task.
        """

        task.title = sanitize_text(task.title)
        task.description = sanitize_text(task.description)

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
                "pub_date": task.pub_date.isoformat() if task.pub_date else None,
            },
        )

        title_text = task.title or ""
        description_text = task.description or ""
        content = f"{title_text}{' ' if title_text else ''}{description_text}"

        noun_stems, verb_stems = extract_stems(
            content,
            task.language,
            title=task.title,
            description=task.description,
        )

        doc = TrendDoc(
            item_id=task.item_id,
            feed_id=task.feed_id,
            language=task.language,
            pub_date=task.pub_date,
            categories=tuple(task.categories),
            noun_stems=noun_stems,
            verb_stems=verb_stems,
        )

        self._persist_trend_doc(doc)

    def _persist_trend_doc(self, doc: TrendDoc) -> None:
        if not self._store:
            return

        self._store.insert_trend_docs([doc])
