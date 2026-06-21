"""Command line interface for stemming stop words inside a JSON file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from json import JSONDecodeError
from typing import Any, Optional, Sequence

from news_deframer.model_store import ensure_model_storage
from news_deframer.nlp import stem_noun


def _stem_stop_words(language: str, stop_words: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()

    for word in stop_words:
        stemmed = stem_noun(language, word)
        if not stemmed or stemmed in seen:
            continue
        seen.add(stemmed)
        result.append(stemmed)

    return result


def _rewrite_stop_words(node: Any, path: str = "$") -> bool:
    changed = False

    if isinstance(node, list):
        for index, item in enumerate(node):
            changed = _rewrite_stop_words(item, f"{path}[{index}]") or changed
        return changed

    if not isinstance(node, dict):
        return False

    language = node.get("language")
    stop_words = node.get("stop_words")
    if isinstance(language, str) and language.strip() and stop_words is not None:
        if not isinstance(stop_words, list):
            raise ValueError(f"{path}.stop_words must be an array of strings")

        values: list[str] = []
        for index, value in enumerate(stop_words):
            if not isinstance(value, str):
                raise ValueError(f"{path}.stop_words[{index}] must be a string")
            values.append(value)

        stemmed_stop_words = _stem_stop_words(language, values)
        if stemmed_stop_words != stop_words:
            node["stop_words"] = stemmed_stop_words
            changed = True

    for key, value in node.items():
        changed = _rewrite_stop_words(value, f"{path}.{key}") or changed

    return changed


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="stem-stopwords-json",
        description="Stem stop_words in a JSON file in place",
    )
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Input JSON file",
    )

    args = parser.parse_args(argv)
    ensure_model_storage()

    try:
        with args.input.open("r", encoding="utf-8") as input_file:
            payload = json.load(input_file)
        changed = _rewrite_stop_words(payload)
    except (JSONDecodeError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if changed:
        with args.input.open("w", encoding="utf-8") as output_file:
            output_file.write(json.dumps(payload, indent=2))
            output_file.write("\n")

    print(args.input)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
