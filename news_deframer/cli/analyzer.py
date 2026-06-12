from __future__ import annotations

import logging
from typing import Any, TextIO

from news_deframer.config import Config
from news_deframer.nlp import extract_stems, sanitize_text
from news_deframer.sentiments import extract_sentiments_array

logger = logging.getLogger(__name__)

SENTIMENT_LONG_NAMES = {
    "v": "valence",
    "a": "arousal",
    "d": "dominance",
    "j": "joy",
    "a_n": "anger",
    "s": "sadness",
    "f": "fear",
    "d_g": "disgust",
}


def analyze_text(
    content: str, language: str, config: Config | None = None
) -> dict[str, Any]:
    """Analyze the provided text and return sentiment scores with long names."""
    # turn off ner (spicy is buggy)
    with_ner = False

    sanitized = sanitize_text(content) or ""

    noun_stems, verb_stems, adj_stems = extract_stems(
        sanitized,
        language,
        stop_words=None,
        with_ner=with_ner,
        config=config,
    )

    sentiments = extract_sentiments_array(
        (noun_stems, verb_stems, adj_stems),
        language,
        config=config,
    )

    if not sentiments:
        return {}

    # Map short names to long names
    return {SENTIMENT_LONG_NAMES.get(k, k): v for k, v in sentiments.items()}


def analyze_file(
    input_file: TextIO, language: str, config: Config | None = None
) -> dict[str, Any]:
    """Read content from a file-like object and analyze it."""
    content = input_file.read()
    return analyze_text(content, language, config=config)
