from __future__ import annotations

import json

from news_deframer.cli import stem_stopwords_json as cli


def test_main_rewrites_stopword_lists_for_objects_with_language(
    tmp_path, monkeypatch, capsys
) -> None:
    payload = [
        {"language": "de", "stop_words": ["montag"]},
        {"language": "en", "stop_words": ["Fox News", "weather", "Fox News"]},
        {"url": "https://example.com/feed", "language": "EN", "stop_words": ["Cats"]},
        {"stop_words": ["ignored-without-language"]},
    ]
    input_path = tmp_path / "stopwords.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")

    monkeypatch.setattr(cli, "ensure_model_storage", lambda: None)
    monkeypatch.setattr(
        cli,
        "stem_noun",
        lambda language, word: {
            "montag": "montag",
            "Fox News": "fox news",
            "weather": "weather",
            "Cats": "cat",
        }[word],
    )

    exit_code = cli.main(["--input", str(input_path)])

    assert exit_code == 0
    assert str(input_path) in capsys.readouterr().out
    assert json.loads(input_path.read_text(encoding="utf-8")) == [
        {"language": "de", "stop_words": ["montag"]},
        {"language": "en", "stop_words": ["fox news", "weather"]},
        {
            "url": "https://example.com/feed",
            "language": "EN",
            "stop_words": ["cat"],
        },
        {"stop_words": ["ignored-without-language"]},
    ]


def test_main_reports_invalid_stop_words_type(tmp_path, monkeypatch, capsys) -> None:
    input_path = tmp_path / "stopwords.json"
    input_path.write_text(
        json.dumps([{"language": "en", "stop_words": ["ok", 1]}]),
        encoding="utf-8",
    )

    monkeypatch.setattr(cli, "ensure_model_storage", lambda: None)

    exit_code = cli.main(["--input", str(input_path)])

    assert exit_code == 1
    assert "stop_words[1] must be a string" in capsys.readouterr().err
