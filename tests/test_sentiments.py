from __future__ import annotations

from typing import Generator

import pytest

from news_deframer import sentiments
from news_deframer import memolon_models


@pytest.fixture(autouse=True)
def clear_sentiment_caches() -> Generator[None, None, None]:
    memolon_models._MEMOLON_CACHE.clear()
    yield
    memolon_models._MEMOLON_CACHE.clear()


def test_extract_sentiments_returns_matching_row(monkeypatch, tmp_path) -> None:
    class DummyScalar:
        def __init__(self, value: object):
            self._value = value

        def as_py(self) -> object:
            return self._value

    class DummyColumn:
        def __init__(self, values: list[object]):
            self._values = values

        def to_pylist(self) -> list[object]:
            return self._values

        def __getitem__(self, index: int) -> DummyScalar:
            return DummyScalar(self._values[index])

    class DummyTable:
        def __init__(self) -> None:
            self.column_names = ["word", "valence", "joy"]
            self._columns = {
                "word": DummyColumn(["Cheese", "Bread"]),
                "valence": DummyColumn([7.2, 5.1]),
                "joy": DummyColumn([6.3, 4.0]),
            }

        def column(self, name: str) -> DummyColumn:
            return self._columns[name]

    read_calls = 0

    model_path = tmp_path / "memolon.parquet"
    model_path.write_bytes(b"dummy")

    def fake_read_table(_path):
        nonlocal read_calls
        read_calls += 1
        return DummyTable()

    monkeypatch.setattr(
        memolon_models, "pq", type("DummyPQ", (), {"read_table": fake_read_table})
    )
    monkeypatch.setattr(
        memolon_models,
        "ensure_memolon_model",
        lambda _lang, **_kwargs: model_path,
    )

    row = sentiments.extract_sentiments("cHeEsE", "en")

    assert row == {"v": 7.2, "j": 6.3}
    assert read_calls == 1


def test_extract_sentiments_uses_cached_model(monkeypatch, tmp_path) -> None:
    class DummyScalar:
        def __init__(self, value: object):
            self._value = value

        def as_py(self) -> object:
            return self._value

    class DummyColumn:
        def __init__(self, values: list[object]):
            self._values = values

        def to_pylist(self) -> list[object]:
            return self._values

        def __getitem__(self, index: int) -> DummyScalar:
            return DummyScalar(self._values[index])

    class DummyTable:
        def __init__(self) -> None:
            self.column_names = ["word", "valence"]
            self._columns = {
                "word": DummyColumn(["Cheese"]),
                "valence": DummyColumn([7.2]),
            }

        def column(self, name: str) -> DummyColumn:
            return self._columns[name]

    read_calls = 0

    model_path = tmp_path / "memolon.parquet"
    model_path.write_bytes(b"dummy")

    def fake_read_table(_path):
        nonlocal read_calls
        read_calls += 1
        return DummyTable()

    monkeypatch.setattr(
        memolon_models, "pq", type("DummyPQ", (), {"read_table": fake_read_table})
    )
    monkeypatch.setattr(
        memolon_models,
        "ensure_memolon_model",
        lambda _lang, **_kwargs: model_path,
    )

    first = sentiments.extract_sentiments("Cheese", "en")
    second = sentiments.extract_sentiments("cheese", "en")

    assert first == second == {"v": 7.2}
    assert read_calls == 1


def test_extract_sentiments_returns_none_for_missing_word(
    monkeypatch, tmp_path
) -> None:
    class DummyColumn:
        def __init__(self, values: list[object]):
            self._values = values

        def to_pylist(self) -> list[object]:
            return self._values

    class DummyTable:
        column_names = ["word"]

        def column(self, _name: str) -> DummyColumn:
            return DummyColumn(["Cheese"])

    model_path = tmp_path / "memolon.parquet"
    model_path.write_bytes(b"dummy")

    monkeypatch.setattr(
        memolon_models,
        "pq",
        type("DummyPQ", (), {"read_table": lambda _path: DummyTable()}),
    )
    monkeypatch.setattr(
        memolon_models,
        "ensure_memolon_model",
        lambda _lang, **_kwargs: model_path,
    )

    assert sentiments.extract_sentiments("Butter", "en") is None


def test_extract_sentiments_array_uses_mean_for_vad_and_max_for_be5(
    monkeypatch,
) -> None:
    lookup = {
        "cheese": {"v": 7.111, "a": 3.0, "j": 2.5, "f": 1.0},
        "bread": {"v": 5.555, "a": 5.0, "j": 4.0, "f": 2.0},
        "happy": {"v": 8.888, "a": 7.0, "j": 7.0, "f": 1.0},
    }

    monkeypatch.setattr(
        sentiments,
        "extract_sentiments",
        lambda word, _language, **_kwargs: lookup.get(word.lower()),
    )

    result = sentiments.extract_sentiments_array(
        (["Cheese", "Missing"], ["Bread"], ["Happy"]),
        "en",
    )

    assert result == {"v": 7.18, "a": 5.0, "j": 7.0, "f": 2.0}


def test_extract_sentiments_array_returns_none_when_no_words_match(monkeypatch) -> None:
    monkeypatch.setattr(
        sentiments,
        "extract_sentiments",
        lambda _word, _language, **_kwargs: None,
    )

    result = sentiments.extract_sentiments_array(([], ["Unknown"], []), "en")

    assert result is None


def test_extract_sentiments_with_real_english_model() -> None:
    try:
        sentiments._get_memolon_model("en")
    except RuntimeError:
        pytest.skip("Memolon English model unavailable")

    row = sentiments.extract_sentiments("Chese", "en")

    assert row is not None
    assert "v" in row
    assert "a" in row
    assert "d" in row
    assert "j" in row


def test_extract_sentiments_with_real_german_model() -> None:
    try:
        sentiments._get_memolon_model("de")
    except RuntimeError:
        pytest.skip("Memolon German model unavailable")

    row = sentiments.extract_sentiments("Käse", "de")

    assert row is not None
    assert "v" in row
    assert "a" in row
    assert "d" in row
    assert "j" in row
