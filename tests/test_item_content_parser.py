"""Tests for the item content parser."""

from uuid import uuid4

from news_deframer.item_content_parser import (
    extract_title_and_description,
    ItemContentParser,
)


def test_extract_title_and_description_success() -> None:
    """Test successful extraction of title and description."""
    content = """
    <item>
      <deframer:title_original>
        Boost Your Productivity
      </deframer:title_original>
      <deframer:description_original>
        Simple Tips
      </deframer:description_original>
      <title>Ignored Standard Title</title>
    </item>
    """
    result = extract_title_and_description(content)
    assert result.title_original == "Boost Your Productivity"
    assert result.description_original == "Simple Tips"
    assert result.title_deframed == "Ignored Standard Title"
    assert result.description_deframed is None


def test_extract_title_and_description_ignores_standard_tags() -> None:
    """Test that standard title/description tags are ignored."""
    content = """
    <item>
      <title>Standard Title</title>
      <description>Standard Description</description>
    </item>
    """
    result = extract_title_and_description(content)
    assert result.title_original is None
    assert result.description_original is None
    assert result.title_deframed == "Standard Title"
    assert result.description_deframed == "Standard Description"


def test_extract_title_and_description_handles_malformed_xml() -> None:
    """Test handling of malformed XML content."""
    content = "<item><title>Unclosed"
    result = extract_title_and_description(content)
    assert result.title_original is None
    assert result.description_original is None
    assert result.title_deframed is None
    assert result.description_deframed is None


def test_extract_title_and_description_with_unknown_namespaces() -> None:
    """Test extraction works with other namespaces present."""
    content = """<item>
  <deframer:title_original>Extracted Title</deframer:title_original>
  <deframer:description_original>Extracted Description</deframer:description_original>
  <wfw:commentRss>http://example.com/feed</wfw:commentRss>
  <slash:comments>10</slash:comments>
</item>"""
    result = extract_title_and_description(content)
    assert result.title_original == "Extracted Title"
    assert result.description_original == "Extracted Description"
    assert result.title_deframed is None
    assert result.description_deframed is None


def test_extract_title_and_description_with_item_id() -> None:
    """Test that item_id is properly passed for error logging."""
    content = "<item><title>Unclosed"
    item_id = uuid4()
    # This should not raise an exception, just log an error
    result = extract_title_and_description(content, item_id=item_id)
    assert result.title_original is None
    assert result.description_original is None
    assert result.title_deframed is None
    assert result.description_deframed is None


def test_item_content_parser_class() -> None:
    """Test the ItemContentParser class directly."""
    parser = ItemContentParser()
    content = "<deframer:title_original>Test Title</deframer:title_original>"
    parser.feed(content)
    parser.close()
    assert parser.data["deframer:title_original"] == "Test Title"
    assert parser.data["deframer:description_original"] is None


def test_item_content_parser_multiple_fields() -> None:
    """Test parsing multiple fields."""
    parser = ItemContentParser()
    content = """
    <item>
      <deframer:title_original>Test Title</deframer:title_original>
      <deframer:description_original>Test Description</deframer:description_original>
    </item>
    """
    parser.feed(content)
    parser.close()
    assert parser.data["deframer:title_original"] == "Test Title"
    assert parser.data["deframer:description_original"] == "Test Description"


def test_item_content_parser_whitespace_handling() -> None:
    """Test that whitespace is properly handled."""
    parser = ItemContentParser()
    content = "<deframer:title_original>  Title With Spaces  </deframer:title_original>"
    parser.feed(content)
    parser.close()
    assert parser.data["deframer:title_original"] == "Title With Spaces"
