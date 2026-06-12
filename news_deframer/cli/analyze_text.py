"""Command line interface for text analysis."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Optional, Sequence

from news_deframer.config import Config
from news_deframer.cli.analyzer import analyze_file
from news_deframer.logger import configure_logging
from news_deframer.model_store import ensure_model_storage


def main(argv: Optional[Sequence[str]] = None) -> int:
    verbose = os.getenv("VERBOSE", "").lower() not in {"", "0", "false", "off"}
    if verbose:
        logging.disable(logging.NOTSET)
        configure_logging("INFO", stream=sys.stderr)
    else:
        logging.disable(logging.CRITICAL)

    config = Config.load()
    ensure_model_storage()

    parser = argparse.ArgumentParser(prog="analyze-text", description="Analyze text")
    parser.add_argument(
        "-i",
        "--input",
        type=argparse.FileType("r"),
        default=sys.stdin,
        help="Input file (default: stdin)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "-l",
        "--language",
        required=True,
        help="2-character language code (e.g., en, de)",
    )

    args = parser.parse_args(argv)

    # Validate language code length
    if len(args.language) != 2:
        print(
            f"Error: Language code must be 2 characters, got '{args.language}'",
            file=sys.stderr,
        )
        return 1

    results = analyze_file(args.input, args.language, config=config)
    json.dump(results, args.output, indent=2)
    args.output.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
