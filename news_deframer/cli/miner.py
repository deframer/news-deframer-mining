"""Command line interface for the News Deframer miner."""

from __future__ import annotations

import argparse
import logging
from typing import Callable, Optional, Sequence

from news_deframer.config import Config
from news_deframer.logger import configure_logging
from news_deframer import miner as miner_module

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="miner-cli", description="News Deframer Miner CLI"
    )
    subparsers = parser.add_subparsers(dest="command", metavar="command")

    poll_parser = subparsers.add_parser("poll", help="Run the poll loop (default)")
    poll_parser.set_defaults(command="poll")

    # Default command is poll to keep the CLI convenient.
    parser.set_defaults(command="poll")
    return parser


def dispatch(command: str, config: Config) -> None:
    commands: dict[str, Callable[[Config], None]] = {
        "poll": miner_module.poll,
    }

    try:
        handler = commands[command]
    except KeyError as err:  # pragma: no cover - argparse guards this
        raise ValueError(f"Unknown command: {command}") from err

    handler(config)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = Config.load()
    configure_logging(config.log_level)
    logger.debug("Executing command %s", args.command)

    dispatch(args.command, config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
