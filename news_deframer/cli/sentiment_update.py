"""Command line interface for sentiment updates."""

from __future__ import annotations

from typing import Optional, Sequence

from news_deframer.config import Config
from news_deframer.postgres import Postgres
from news_deframer.sentiments import extract_sentiments_array


def main(argv: Optional[Sequence[str]] = None) -> int:
    _ = argv

    config = Config.load()
    repository = Postgres(config)
    trends = repository.fetch_trends_without_sentiments(limit=1000)
    updates = {}

    for trend in trends:
        sentiment = extract_sentiments_array(
            (trend.noun_stems, trend.verb_stems, trend.adjective_stems),
            trend.language,
        )

        if sentiment is None:
            continue

        updates[trend.item_id] = sentiment

    repository.update_trend_sentiments(updates)

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
