"""Command line interface for the News Deframer miner."""

from __future__ import annotations

import argparse
import logging
from typing import Optional, Sequence

from news_deframer import poller as poller_module
from news_deframer.config import Config
from news_deframer.logger import configure_logging

logger = logging.getLogger(__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="miner", description="News Deframer Miner")
    parser.parse_args(argv)

    config = Config.load()
    configure_logging(config.log_level)
    logger.debug("Starting mining poller")

    poller_module.poll(config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
