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
    trends, items_and_feeds = repository.fetch_trends_without_sentiments(limit=10)
    logger.info("Fetched %d trends for sentiment updates", len(trends))
    _ = items_and_feeds
    updates_sentiment = {}
    updates_deframed_sentiment = {}

    for trend in trends:
        if trend.sentiments == {}:
            sentiment = extract_sentiments_array(
                (trend.noun_stems, trend.verb_stems, trend.adjective_stems),
                trend.language,
            )

            # Add to updates only if sentiment is not None
            if sentiment is not None:
                updates_sentiment[trend.item_id] = sentiment

        if trend.sentiments_deframed == {}:
            # We know this item_id exists in items_and_feeds because that's how fetch_trends_without_sentiments works
            item, feed = items_and_feeds[trend.item_id]
            # Log if sentiments_deframed is empty
            logger.info(
                "Trend %s has empty sentiments_deframed - feed categories length: %d, item content length: %d",
                trend.item_id,
                len(feed.categories),
                len(item.content),
            )

    #repository.update_trend_sentiments(updates_sentiment)
    #repository.update_trend_deframed_sentiments(updates_deframed_sentiment)

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
