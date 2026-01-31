import argparse
import logging

from miner import miner
from miner.config import Config
from miner.logger import configure_logging

logger = logging.getLogger(__name__)


def main():
    """
    Main entry point for the CLI.
    """
    parser = argparse.ArgumentParser(description="News Deframer Mining")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # Command: mine
    subparsers.add_parser("mine", help="Run the mining worker")

    # Parse arguments
    args = parser.parse_args()

    # Load config and setup logging
    config = Config.load()
    configure_logging(config.log_level)

    if args.command == "mine":
        miner.run(config)


if __name__ == "__main__":
    main()
