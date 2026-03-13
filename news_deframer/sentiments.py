"""Language-specific helpers for extracting Memolon sentiment scores."""

from __future__ import annotations

from statistics import median
from pathlib import Path
from typing import Any, Sequence, TypedDict, cast

from news_deframer.memolon_models import MEMOLON_LANGUAGE_MODELS

try:  # pragma: no cover - optional dependency behavior
    import pyarrow.parquet as pq  # type: ignore[import-untyped]
except Exception:  # pragma: no cover - optional dependency behavior
    pq = None  # type: ignore[assignment]


_MEMOLON_CACHE: dict[str, tuple[Any, list[str]]] = {}


class Sentiment(TypedDict, total=False):
    """Compact sentiment payload based on Memolon's 1-9 emotion ratings.

    Scale interpretation:
    - 1 = very low / absent
    - 5 = neutral / medium
    - 9 = very high / strong

    The first three fields follow the VAD model:
    - `v` (Valence): 1 = very negative, 9 = very positive
    - `a` (Arousal): 1 = calm / sleepy, 9 = excited / highly activated
    - `d` (Dominance): 1 = feeling controlled or powerless,
      9 = feeling in control / dominant

    The emotion intensity fields use the same 1-9 scale:
    - `j` (Joy)
    - `a_n` (Anger)
    - `s` (Sadness)
    - `f` (Fear)
    - `d_g` (Disgust)

    Background on the VAD/PAD model:
    https://en.wikipedia.org/wiki/PAD_emotional_state_model
    """

    v: float
    a: float
    d: float
    j: float
    a_n: float
    s: float
    f: float
    d_g: float


_SENTIMENT_COLUMN_MAP = {
    "valence": "v",
    "arousal": "a",
    "dominance": "d",
    "joy": "j",
    "anger": "a_n",
    "sadness": "s",
    "fear": "f",
    "disgust": "d_g",
}


def _set_sentiment_value(sentiment: Sentiment, column_name: str, value: Any) -> None:
    numeric_value = float(value)
    if column_name == "valence":
        sentiment["v"] = numeric_value
    elif column_name == "arousal":
        sentiment["a"] = numeric_value
    elif column_name == "dominance":
        sentiment["d"] = numeric_value
    elif column_name == "joy":
        sentiment["j"] = numeric_value
    elif column_name == "anger":
        sentiment["a_n"] = numeric_value
    elif column_name == "sadness":
        sentiment["s"] = numeric_value
    elif column_name == "fear":
        sentiment["f"] = numeric_value
    elif column_name == "disgust":
        sentiment["d_g"] = numeric_value


def _get_language_code(language: str) -> str:
    """Extract the base language code (e.g., 'en-US' -> 'en')."""
    return (language or "").split("-")[0].lower()


def _get_memolon_model(language: str) -> tuple[Any, list[str]]:
    """Load and cache the Memolon table and lowercase word index for a language."""
    if pq is None:
        raise RuntimeError("pyarrow is required but not installed")

    lang_code = _get_language_code(language)
    if not lang_code:
        raise RuntimeError("Language code is required for sentiment handling")

    cached = _MEMOLON_CACHE.get(lang_code)
    if cached is not None:
        return cached

    model_filename = MEMOLON_LANGUAGE_MODELS.get(lang_code)
    if not model_filename:
        raise RuntimeError(f"No Memolon model available for language '{language}'")

    model_path = Path(__file__).resolve().parent.parent / "memolon" / model_filename
    if not model_path.exists():
        raise RuntimeError(f"Memolon model file not found: {model_path}")

    try:
        table = pq.read_table(model_path)
    except Exception as exc:  # pragma: no cover - propagate failure gracefully
        raise RuntimeError(f"Failed to load Memolon model '{model_filename}'") from exc

    if "word" not in table.column_names:
        raise RuntimeError(f"Memolon model '{model_filename}' has no 'word' column")

    lowercase_word_list = [
        str(word).lower() for word in table.column("word").to_pylist()
    ]
    cached_model = (table, lowercase_word_list)
    _MEMOLON_CACHE[lang_code] = cached_model
    return cached_model


def extract_sentiments(word_to_find: str, language: str) -> Sentiment | None:
    """Return compact Memolon sentiment scores for a single word.

    Memolon stores emotional ratings on a 1-9 scale. The first three
    dimensions use the VAD model (Valence, Arousal, Dominance), and the
    remaining dimensions capture emotion intensities for Joy, Anger,
    Sadness, Fear, and Disgust on the same scale.

    Background on the VAD/PAD model:
    https://en.wikipedia.org/wiki/PAD_emotional_state_model
    """
    normalized = word_to_find.strip()
    if not normalized:
        return None

    table, lowercase_word_list = _get_memolon_model(language)
    lowercase_word_to_find = normalized.lower()

    try:
        index = lowercase_word_list.index(lowercase_word_to_find)
    except ValueError:
        return None

    sentiment: Sentiment = {}
    for column_name in _SENTIMENT_COLUMN_MAP:
        if column_name in table.column_names:
            value = table.column(column_name)[index].as_py()
            if value is not None:
                _set_sentiment_value(sentiment, column_name, value)

    return sentiment


def extract_sentiments_array(
    stems: tuple[Sequence[str], Sequence[str], Sequence[str]], language: str
) -> Sentiment | None:
    """Return median sentiment scores aggregated across matched words.

    The input typically contains noun, verb, and adjective stems. Each word is
    looked up independently, missing words are ignored, and the median is taken
    per sentiment dimension across all matched words.
    """

    collected_values: dict[str, list[float]] = {
        key: [] for key in Sentiment.__annotations__
    }

    for words in stems:
        for word in words:
            sentiment = extract_sentiments(word, language)
            if sentiment is None:
                continue
            for key, value in sentiment.items():
                collected_values[key].append(cast(float, value))

    aggregated: Sentiment = {}
    for key, values in collected_values.items():
        if not values:
            continue

        median_value = round(float(median(values)), 2)
        if key == "v":
            aggregated["v"] = median_value
        elif key == "a":
            aggregated["a"] = median_value
        elif key == "d":
            aggregated["d"] = median_value
        elif key == "j":
            aggregated["j"] = median_value
        elif key == "a_n":
            aggregated["a_n"] = median_value
        elif key == "s":
            aggregated["s"] = median_value
        elif key == "f":
            aggregated["f"] = median_value
        elif key == "d_g":
            aggregated["d_g"] = median_value

    return aggregated or None
