from datetime import datetime
from pathlib import Path

import duckdb

from news_deframer.duckdb_store import DuckDBStore, TrendDoc


def test_store_roundtrip_records_persist(tmp_path):
    db_file = tmp_path / "duck" / "trend_docs"
    store = DuckDBStore(str(db_file))
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
    store.close()

    db_path = Path(str(db_file) + ".duckdb")
    assert db_path.exists()

    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT item_id, feed_id, language, pub_date, categories, noun_stems, verb_stems FROM trend_docs"
        ).fetchall()

    expected_categories = list(doc.categories) if doc.categories else None
    expected_nouns = list(doc.noun_stems) if doc.noun_stems else None
    expected_verbs = list(doc.verb_stems) if doc.verb_stems else None
    assert rows == [
        (
            doc.item_id,
            doc.feed_id,
            doc.language,
            doc.pub_date,
            expected_categories,
            expected_nouns,
            expected_verbs,
        )
    ]


def test_memory_alias_does_not_create_path():
    store = DuckDBStore(":memory:")
    assert getattr(store, "_db_path") is None
    store.close()
