"""Command line interface for text analysis."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional, Sequence

from news_deframer.cli.analyzer import analyze_file


def main(argv: Optional[Sequence[str]] = None) -> int:
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

    results = analyze_file(args.input, args.language)
    json.dump(results, args.output, indent=2)
    args.output.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
