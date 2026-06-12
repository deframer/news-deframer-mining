from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from news_deframer.config import Config
from news_deframer.logger import _format_bytes, _get_rss_bytes
from news_deframer.model_store import (
    acquire_lock,
    download_url_to_path,
    get_model_root,
)

try:  # pragma: no cover - optional dependency behavior
    import pyarrow.parquet as pq  # type: ignore[import-untyped]
except Exception:  # pragma: no cover - optional dependency behavior
    pq = None  # type: ignore[assignment]

MEMOLON_LANGUAGE_MODELS = {
    "en": "memolon-grouped-en-<MEMOLON_VERSION>.parquet",
    "de": "memolon-grouped-de-<MEMOLON_VERSION>.parquet",
    "es": "memolon-grouped-es-<MEMOLON_VERSION>.parquet",
    "fr": "memolon-grouped-fr-<MEMOLON_VERSION>.parquet",
    "it": "memolon-grouped-it-<MEMOLON_VERSION>.parquet",
    "pt": "memolon-grouped-pt-<MEMOLON_VERSION>.parquet",
    "nl": "memolon-grouped-nl-<MEMOLON_VERSION>.parquet",
    "ru": "memolon-grouped-ru-<MEMOLON_VERSION>.parquet",
    "da": "memolon-grouped-da-<MEMOLON_VERSION>.parquet",
}


logger = logging.getLogger(__name__)

_MEMOLON_CACHE: dict[tuple[str, str], tuple[Any, list[str]]] = {}


def _get_language_code(language: str) -> str:
    return (language or "").split("-")[0].lower()


def _get_memolon_root() -> Path:
    return get_model_root() / "memolon"


def _get_memolon_lock_path(filename: str) -> Path:
    return _get_memolon_root() / ".locks" / f"{filename}.lock"


def _get_memolon_version(config: Config | None) -> str:
    if config is None:
        config = Config.load()
    return config.memolon_version


def _get_memolon_release_url(filename: str, memolon_version: str) -> str:
    return f"https://github.com/deframer/memolon-parquet/releases/download/v{memolon_version}/{filename}"


def _get_memolon_model_filename(language: str, memolon_version: str) -> str:
    lang_code = _get_language_code(language)
    template = MEMOLON_LANGUAGE_MODELS.get(lang_code)
    if not template:
        raise RuntimeError(f"No Memolon model available for language '{language}'")
    return template.replace("<MEMOLON_VERSION>", memolon_version)


def get_memolon_model_path(language: str, config: Config | None = None) -> Path:
    memolon_version = _get_memolon_version(config)
    filename = _get_memolon_model_filename(language, memolon_version)
    return _get_memolon_root() / filename


def ensure_memolon_model(language: str, config: Config | None = None) -> Path:
    memolon_version = _get_memolon_version(config)
    model_path = get_memolon_model_path(language, config=config)
    if model_path.exists():
        return model_path

    filename = model_path.name
    lock_path = _get_memolon_lock_path(filename)
    with acquire_lock(lock_path):
        if model_path.exists():
            return model_path
        started_at = time.perf_counter()
        try:
            download_url_to_path(
                _get_memolon_release_url(filename, memolon_version), model_path
            )
        except Exception as exc:  # pragma: no cover - network failure propagation
            raise RuntimeError(
                f"Failed to download Memolon model '{filename}'"
            ) from exc
        elapsed = time.perf_counter() - started_at
        logger.info(
            "Downloaded Memolon model '%s' in %.3fs",
            filename,
            elapsed,
            extra={
                "language": language,
                "version": memolon_version,
                "path": str(model_path),
            },
        )

    return model_path


def load_memolon_model(
    language: str, config: Config | None = None
) -> tuple[Any, list[str]]:
    if pq is None:
        raise RuntimeError("pyarrow is required but not installed")

    lang_code = _get_language_code(language)
    if not lang_code:
        raise RuntimeError("Language code is required for sentiment handling")

    memolon_version = _get_memolon_version(config)
    cache_key = (lang_code, memolon_version)
    cached = _MEMOLON_CACHE.get(cache_key)
    if cached is not None:
        return cached

    filename = _get_memolon_model_filename(language, memolon_version)

    model_path = ensure_memolon_model(language, config=config)

    started_at = time.perf_counter()
    rss_before = _get_rss_bytes()
    try:
        table = pq.read_table(model_path)
    except Exception as exc:  # pragma: no cover - propagate failure gracefully
        raise RuntimeError(f"Failed to load Memolon model '{filename}'") from exc

    if "word" not in table.column_names:
        raise RuntimeError(f"Memolon model '{filename}' has no 'word' column")

    lowercase_word_list = [
        str(word).lower() for word in table.column("word").to_pylist()
    ]
    cached_model = (table, lowercase_word_list)

    elapsed = time.perf_counter() - started_at
    rss_after = _get_rss_bytes()
    rss_delta = (
        rss_after - rss_before
        if rss_before is not None and rss_after is not None
        else None
    )
    disk_size_bytes = model_path.stat().st_size
    table_nbytes = getattr(table, "nbytes", None)
    logger.info(
        "Loaded Memolon model '%s' in %.3fs (disk=%s, table=%s, rss_delta=%s)",
        filename,
        elapsed,
        _format_bytes(disk_size_bytes),
        _format_bytes(table_nbytes if isinstance(table_nbytes, int) else None),
        _format_bytes(rss_delta),
        extra={
            "language": language,
            "version": memolon_version,
            "path": str(model_path),
            "disk_size": _format_bytes(disk_size_bytes),
            "table_size": _format_bytes(
                table_nbytes if isinstance(table_nbytes, int) else None
            ),
            "rss_delta": _format_bytes(rss_delta),
        },
    )

    _MEMOLON_CACHE[cache_key] = cached_model
    return cached_model


def download_all_memolon_models(config: Config | None = None) -> None:
    for language in sorted(MEMOLON_LANGUAGE_MODELS):
        ensure_memolon_model(language, config=config)
