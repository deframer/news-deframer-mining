"""Item content parser for extracting metadata from HTML content."""

from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import logging
from typing import Optional
from uuid import UUID

logger = logging.getLogger(__name__)


@dataclass
class ContentParseResult:
    """Result of parsing HTML content for title and description."""

    title_original: Optional[str] = None
    title_deframed: Optional[str] = None
    description_original: Optional[str] = None
    description_deframed: Optional[str] = None


class ItemContentParser(HTMLParser):
    """Extracts title and description from HTML content using deframer metadata and standard HTML tags."""

    def __init__(self) -> None:
        super().__init__()
        self.data: dict[str, Optional[str]] = {
            "deframer:title_original": None,
            "deframer:description_original": None,
            "title": None,
            "description": None,
        }
        self._current: Optional[str] = None
        self._buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag in self.data:
            self._current = tag
            self._buffer = []

    def handle_endtag(self, tag: str) -> None:
        if tag == self._current:
            self.data[tag] = "".join(self._buffer).strip() or None
            self._current = None

    def handle_data(self, data: str) -> None:
        if self._current:
            self._buffer.append(data)


def extract_title_and_description(
    content: str, item_id: Optional[UUID] = None
) -> ContentParseResult:
    """Extract title and description from HTML content using deframer metadata.

    Args:
        content: HTML content to parse
        item_id: Optional item ID for error logging

    Returns:
        ContentParseResult with title_original, title, description_original, description
    """
    parser = ItemContentParser()
    try:
        parser.feed(content)
        parser.close()
    except Exception as exc:
        if item_id:
            logger.error(
                "Failed to parse content", extra={"item_id": str(item_id)}, exc_info=exc
            )
        return ContentParseResult()
    return ContentParseResult(
        title_original=parser.data["deframer:title_original"],
        title_deframed=parser.data["title"],
        description_original=parser.data["deframer:description_original"],
        description_deframed=parser.data["description"],
    )
