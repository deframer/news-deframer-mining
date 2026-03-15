"""Command line interface for sentiment updates."""

from __future__ import annotations

import logging
from typing import Optional, Sequence

from news_deframer.config import Config
from news_deframer.logger import configure_logging
from news_deframer.postgres import Postgres
from news_deframer.sentiments import extract_sentiments_array

logger = logging.getLogger(__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    _ = argv

    config = Config.load()
    configure_logging(config.log_level)
    repository = Postgres(config)
    trends, items_and_feeds = repository.fetch_trends_without_sentiments(limit=1000)
    logger.info("Fetched %d trends for sentiment updates", len(trends))
    _ = items_and_feeds
    updates = {}

    for trend in trends:
        sentiment = extract_sentiments_array(
            (trend.noun_stems, trend.verb_stems, trend.adjective_stems),
            trend.language,
        )

        if sentiment is None:
            continue

        updates[trend.item_id] = sentiment

    # repository.update_trend_sentiments(updates)

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
