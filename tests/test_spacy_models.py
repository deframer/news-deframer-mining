from __future__ import annotations

from news_deframer.config import Config
from news_deframer import spacy_models


def test_ensure_spacy_model_uses_existing_local_path(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(spacy_models, "get_workspace_root", lambda: tmp_path)

    model_name = spacy_models.SPACY_LANGUAGE_MODELS["en"]
    model_path = tmp_path / "models" / "spacy" / f"{model_name}-{Config.load().spacy_version}"
    load_path = model_path / model_name
    load_path.mkdir(parents=True)
    (load_path / "config.cfg").write_text("[nlp]\nlang = \"en\"\n", encoding="utf-8")

    resolved = spacy_models.ensure_spacy_model("en")

    assert resolved == load_path


def test_ensure_spacy_model_downloads_missing_model(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(spacy_models, "get_workspace_root", lambda: tmp_path)

    model_name = spacy_models.SPACY_LANGUAGE_MODELS["de"]
    model_root = tmp_path / "models" / "spacy" / f"{model_name}-{Config.load().spacy_version}"
    load_path = model_root / model_name

    calls = []

    def fake_download(url: str, target_dir, timeout: int = 60) -> None:
        calls.append((url, target_dir, timeout))
        load_path.mkdir(parents=True)
        (load_path / "config.cfg").write_text("[nlp]\nlang = \"de\"\n", encoding="utf-8")

    monkeypatch.setattr(spacy_models, "download_wheel_to_directory", fake_download)

    resolved = spacy_models.ensure_spacy_model("de")

    assert resolved == load_path
    assert len(calls) == 1
