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


def test_get_model_root_uses_repo_models_by_default(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(model_store, "get_project_root", lambda: tmp_path)
    monkeypatch.delenv("MODEL_ROOT", raising=False)

    assert model_store.get_model_root() == tmp_path / "models"


def test_get_model_root_honors_env_override(monkeypatch, tmp_path) -> None:
    model_root = tmp_path / "shared-models"
    monkeypatch.setenv("MODEL_ROOT", str(model_root))

    assert model_store.get_model_root() == model_root


def test_get_model_root_falls_back_when_repo_models_unwritable(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(model_store, "get_project_root", lambda: tmp_path)
    (tmp_path / "models").mkdir()
    monkeypatch.setattr(model_store.os, "access", lambda path, mode: False)

    assert model_store.get_model_root() == (
        model_store.Path.home() / ".cache" / "news-deframer-miner" / "models"
    )
