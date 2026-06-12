from __future__ import annotations

from news_deframer.config import Config
from news_deframer import memolon_models


def test_get_memolon_model_path_uses_local_filesystem(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(memolon_models, "get_workspace_root", lambda: tmp_path)

    model_path = memolon_models.get_memolon_model_path("en")

    expected = tmp_path / "models" / "memolon" / memolon_models.MEMOLON_LANGUAGE_MODELS["en"].replace(
        "<MEMOLON_VERSION>", Config.load().memolon_version
    )
    assert model_path == expected


def test_ensure_memolon_model_downloads_missing_file(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(memolon_models, "get_workspace_root", lambda: tmp_path)

    model_path = memolon_models.get_memolon_model_path("de")
    calls = []

    def fake_download(url: str, target_path, timeout: int = 60) -> None:
        calls.append((url, target_path, timeout))
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(b"parquet")

    monkeypatch.setattr(memolon_models, "download_url_to_path", fake_download)

    resolved = memolon_models.ensure_memolon_model("de")

    assert resolved == model_path
    assert len(calls) == 1
