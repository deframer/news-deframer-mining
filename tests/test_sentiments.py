from __future__ import annotations

from typing import Generator

import pytest

from news_deframer import sentiments


@pytest.fixture(autouse=True)
def clear_sentiment_caches() -> Generator[None, None, None]:
    sentiments._MEMOLON_CACHE.clear()
    yield
    sentiments._MEMOLON_CACHE.clear()


def test_extract_sentiments_returns_matching_row(monkeypatch) -> None:
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

    def fake_read_table(_path):
        nonlocal read_calls
        read_calls += 1
        return DummyTable()

    monkeypatch.setattr(
        sentiments, "pq", type("DummyPQ", (), {"read_table": fake_read_table})
    )
    monkeypatch.setattr(sentiments.Path, "exists", lambda _self: True)

    row = sentiments.extract_sentiments("cHeEsE", "en")

    assert row == {"v": 7.2, "j": 6.3}
    assert read_calls == 1


def test_extract_sentiments_uses_cached_model(monkeypatch) -> None:
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

    def fake_read_table(_path):
        nonlocal read_calls
        read_calls += 1
        return DummyTable()

    monkeypatch.setattr(
        sentiments, "pq", type("DummyPQ", (), {"read_table": fake_read_table})
    )
    monkeypatch.setattr(sentiments.Path, "exists", lambda _self: True)

    first = sentiments.extract_sentiments("Cheese", "en")
    second = sentiments.extract_sentiments("cheese", "en")

    assert first == second == {"v": 7.2}
    assert read_calls == 1


def test_extract_sentiments_returns_none_for_missing_word(monkeypatch) -> None:
    class DummyColumn:
        def __init__(self, values: list[object]):
            self._values = values

        def to_pylist(self) -> list[object]:
            return self._values

    class DummyTable:
        column_names = ["word"]

        def column(self, _name: str) -> DummyColumn:
            return DummyColumn(["Cheese"])

    monkeypatch.setattr(
        sentiments,
        "pq",
        type("DummyPQ", (), {"read_table": lambda _path: DummyTable()}),
    )
    monkeypatch.setattr(sentiments.Path, "exists", lambda _self: True)

    assert sentiments.extract_sentiments("Butter", "en") is None


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
