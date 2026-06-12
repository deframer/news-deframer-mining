from __future__ import annotations

import pytest

from news_deframer import model_store


def test_ensure_model_storage_creates_directories(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(model_store, "get_project_root", lambda: tmp_path)

    model_store.ensure_model_storage()

    assert (tmp_path / "models").is_dir()
    assert (tmp_path / "models" / "spacy").is_dir()
    assert (tmp_path / "models" / "memolon").is_dir()


def test_ensure_model_storage_raises_when_models_is_file(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(model_store, "get_project_root", lambda: tmp_path)
    (tmp_path / "models").write_text("blocked", encoding="utf-8")

    with pytest.raises(RuntimeError):
        model_store.ensure_model_storage()
