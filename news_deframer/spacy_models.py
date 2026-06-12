from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from news_deframer.config import Config
from news_deframer.logger import _format_bytes, _get_rss_bytes
from news_deframer.model_store import (
    acquire_lock,
    download_wheel_to_directory,
    get_model_root,
)

try:  # pragma: no cover - optional dependency
    import spacy
except Exception:  # pragma: no cover - optional dependency
    spacy = None  # type: ignore[assignment]

if TYPE_CHECKING:  # pragma: no cover - typing only
    from spacy.language import Language as SpacyLanguage
else:  # pragma: no cover - runtime fallback
    SpacyLanguage = Any  # type: ignore[misc,assignment]

SPACY_LANGUAGE_MODELS = {
    "en": "en_core_web_lg",
    "de": "de_core_news_lg",
    "es": "es_core_news_lg",
    "fr": "fr_core_news_lg",
    "it": "it_core_news_lg",
    "pt": "pt_core_news_lg",
    "nl": "nl_core_news_lg",
    "pl": "pl_core_news_lg",
    "ru": "ru_core_news_lg",
    "da": "da_core_news_lg",
}


logger = logging.getLogger(__name__)

_NLP_CACHE: dict[tuple[str, str, bool], SpacyLanguage] = {}


def _get_directory_size_bytes(path: Path) -> int | None:
    total = 0
    for entry in path.rglob("*"):
        if entry.is_file():
            try:
                total += entry.stat().st_size
            except FileNotFoundError:
                continue
    return total or None


def _get_language_code(language: str) -> str:
    return (language or "").split("-")[0].lower()


def _get_spacy_root() -> Path:
    return get_model_root() / "spacy"


def _get_spacy_model_name(language: str) -> str:
    lang_code = _get_language_code(language)
    model_name = SPACY_LANGUAGE_MODELS.get(lang_code)
    if not model_name:
        raise RuntimeError(f"No spaCy model available for language '{language}'")
    return model_name


def get_spacy_model_name(language: str) -> str:
    return _get_spacy_model_name(language)


def _get_spacy_version(config: Config | None) -> str:
    if config is None:
        config = Config.load()
    return config.spacy_version


def _get_spacy_model_root(model_name: str, spacy_version: str) -> Path:
    return _get_spacy_root() / f"{model_name}-{spacy_version}"


def _get_spacy_model_lock_path(model_name: str, spacy_version: str) -> Path:
    return _get_spacy_root() / ".locks" / f"{model_name}-{spacy_version}.lock"


def _get_spacy_release_url(model_name: str, spacy_version: str) -> str:
    return (
        f"https://github.com/explosion/spacy-models/releases/download/"
        f"{model_name}-{spacy_version}/{model_name}-{spacy_version}-py3-none-any.whl"
    )


def _find_spacy_model_path(model_root: Path) -> Path:
    config_files = sorted(
        model_root.rglob("config.cfg"), key=lambda path: len(path.parts)
    )
    if not config_files:
        raise RuntimeError(
            f"Downloaded spaCy model at '{model_root}' has no config.cfg"
        )
    return config_files[0].parent


def get_spacy_model_path(language: str, config: Config | None = None) -> Path:
    model_name = _get_spacy_model_name(language)
    spacy_version = _get_spacy_version(config)
    model_root = _get_spacy_model_root(model_name, spacy_version)
    if not model_root.exists():
        raise RuntimeError(f"spaCy model file not found: {model_root}")
    return _find_spacy_model_path(model_root)


def ensure_spacy_model(language: str, config: Config | None = None) -> Path:
    model_name = _get_spacy_model_name(language)
    spacy_version = _get_spacy_version(config)
    model_root = _get_spacy_model_root(model_name, spacy_version)
    if model_root.exists():
        return _find_spacy_model_path(model_root)

    lock_path = _get_spacy_model_lock_path(model_name, spacy_version)
    with acquire_lock(lock_path):
        if model_root.exists():
            return _find_spacy_model_path(model_root)
        started_at = time.perf_counter()
        try:
            download_wheel_to_directory(
                _get_spacy_release_url(model_name, spacy_version), model_root
            )
        except Exception as exc:  # pragma: no cover - network failure propagation
            raise RuntimeError(
                f"Failed to download spaCy model '{model_name}'"
            ) from exc
        elapsed = time.perf_counter() - started_at
        logger.info(
            "Downloaded spaCy model '%s' in %.3fs",
            model_name,
            elapsed,
            extra={
                "language": language,
                "version": spacy_version,
                "path": str(model_root),
            },
        )

    return _find_spacy_model_path(model_root)


def get_spacy_model(
    language: str, config: Config | None = None, with_ner: bool = True
) -> SpacyLanguage:
    if spacy is None:
        raise RuntimeError("spaCy is required but not installed")

    model_name = _get_spacy_model_name(language)
    spacy_version = _get_spacy_version(config)
    cache_key = (model_name, spacy_version, with_ner)
    cached = _NLP_CACHE.get(cache_key)
    if cached is not None:
        return cached

    model_path = ensure_spacy_model(language, config=config)
    started_at = time.perf_counter()
    rss_before = _get_rss_bytes()
    disable = () if with_ner else ("ner",)
    try:
        model = spacy.load(str(model_path), disable=disable)
    except Exception as exc:  # pragma: no cover - propagate failure gracefully
        raise RuntimeError(f"Failed to load spaCy model '{model_name}'") from exc

    elapsed = time.perf_counter() - started_at
    rss_after = _get_rss_bytes()
    rss_delta = (
        rss_after - rss_before
        if rss_before is not None and rss_after is not None
        else None
    )
    disk_size_bytes = _get_directory_size_bytes(model_path)
    logger.info(
        "Loaded spaCy model '%s' in %.3fs (disk=%s, rss_delta=%s)",
        model_name,
        elapsed,
        _format_bytes(disk_size_bytes),
        _format_bytes(rss_delta),
        extra={
            "language": language,
            "version": spacy_version,
            "with_ner": with_ner,
            "path": str(model_path),
            "disk_size": _format_bytes(disk_size_bytes),
            "rss_delta": _format_bytes(rss_delta),
        },
    )

    _NLP_CACHE[cache_key] = model
    return model


def download_all_spacy_models(config: Config | None = None) -> None:
    for language in sorted(SPACY_LANGUAGE_MODELS):
        ensure_spacy_model(language, config=config)
