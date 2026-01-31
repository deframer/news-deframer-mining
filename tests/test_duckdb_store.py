from datetime import datetime
from pathlib import Path

import pytest

from news_deframer.duckdb_store import DuckDBStore, TrendDoc


@pytest.mark.parametrize("memory_alias", [":memory", ":memory:"])
def test_store_roundtrip_inserts_and_fetches(memory_alias):
    store = DuckDBStore(memory_alias)
    doc = TrendDoc(
        item_id="hash-1",
        feed_id="feed-1",
        language="en",
        pub_date=datetime(2024, 1, 1, 12, 0, 0),
        categories=("economy", "policy"),
        noun_stems=("economy",),
        verb_stems=("grow",),
    )

    store.insert_trend_docs([doc])
    fetched = store.fetch_trend_docs_by_language("en")

    assert len(fetched) == 1
    result = fetched[0]
    assert result.item_id == doc.item_id
    assert result.feed_id == doc.feed_id
    assert result.language == doc.language
    assert result.pub_date == doc.pub_date
    categories = list(result.categories) if result.categories else []
    noun_stems = list(result.noun_stems) if result.noun_stems else []
    verb_stems = list(result.verb_stems) if result.verb_stems else []
    expected_categories = list(doc.categories) if doc.categories else []
    expected_nouns = list(doc.noun_stems) if doc.noun_stems else []
    expected_verbs = list(doc.verb_stems) if doc.verb_stems else []
    assert categories == expected_categories
    assert noun_stems == expected_nouns
    assert verb_stems == expected_verbs

    store.close()


def test_store_creates_parent_directory_and_extension(tmp_path):
    db_root = tmp_path / "nested" / "trend_docs"
    store = DuckDBStore(str(db_root))
    store.close()

    expected_path = Path(str(db_root) + ".duckdb")
    assert expected_path.exists()


def test_memory_alias_does_not_create_path():
    store = DuckDBStore(":memory:")
    assert getattr(store, "_db_path") is None
    store.close()
