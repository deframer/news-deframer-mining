"""Miner service responsible for handling processed items."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
import re
from typing import Optional, Sequence

from news_deframer.config import Config
from news_deframer.duckdb_store import DuckDBStore, TrendDoc


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

        self._persist_trend_doc(task)

    def _persist_trend_doc(self, task: MiningTask) -> None:
        if not self._store:
            return

        noun_stems = _tokenize_words(task.title)
        verb_stems = _tokenize_words(task.description)

        doc = TrendDoc(
            item_id=task.item_id,
            feed_id=task.feed_id,
            language=task.language,
            pub_date=task.pub_date,
            categories=tuple(task.categories),
            # fake
            noun_stems=noun_stems if noun_stems else None,
            # fake
            verb_stems=verb_stems if verb_stems else None,
        )
        self._store.insert_trend_docs([doc])


_WORD_RE = re.compile(r"[A-Za-z]+")


def _tokenize_words(value: Optional[str]) -> Sequence[str]:
    if not value:
        return []
    return [match.group(0).lower() for match in _WORD_RE.finditer(value)]
