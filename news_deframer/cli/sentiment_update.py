"""Command line interface for sentiment updates."""

from __future__ import annotations

import logging
from typing import Optional, Sequence

from news_deframer.config import Config
from news_deframer.logger import configure_logging
from news_deframer.postgres import Postgres
from news_deframer.sentiments import extract_sentiments_array
from news_deframer.netutil import get_base_domain_name
from news_deframer.nlp import extract_stems, sanitize_text

logger = logging.getLogger(__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    _ = argv

    config = Config.load()
    configure_logging(config.log_level)
    repository = Postgres(config)
    trends, items_and_feeds = repository.fetch_trends_without_sentiments(limit=100)
    logger.info("Fetched %d trends for sentiment updates", len(trends))
    _ = items_and_feeds
    updates_sentiment = {}
    updates_deframed_sentiment = {}
    with_ner = False

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
            think_result = item.think_result
            if think_result is None:
                continue

            base_domain = get_base_domain_name(feed.url)

            # we split here for apollo-news also at the -
            stop_words = (
                [w for w in base_domain.split("-") if len(w) >= 3]
                if base_domain
                else []
            )

            title_deframed = sanitize_text(think_result.title_corrected)
            description_deframed = sanitize_text(think_result.description_corrected)
            content_deframed = f"{title_deframed}{' ' if title_deframed else ''}{description_deframed}"

            noun_stems_deframed, verb_stems_deframed, adj_stems_deframed = (
                extract_stems(
                    content_deframed,
                    trend.language,
                    stop_words=stop_words,
                    with_ner=with_ner,
                )
            )
            sentiments_deframed = (
                extract_sentiments_array(
                    (noun_stems_deframed, verb_stems_deframed, adj_stems_deframed),
                    trend.language,
                )
                or {}
            )

            # Add to updates only if sentiments_deframed is not None
            if sentiments_deframed is not None:
                updates_deframed_sentiment[trend.item_id] = sentiments_deframed

    repository.update_trend_sentiments(updates_sentiment)
    repository.update_trend_deframed_sentiments(updates_deframed_sentiment)
    logger.info(
        "Updated %d sentiments and %d deframed sentiments",
        len(updates_sentiment),
        len(updates_deframed_sentiment),
    )

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
