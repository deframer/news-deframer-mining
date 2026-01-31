import logging
import sys
import time

from miner.config import Config
from miner.postgres import Postgres
from miner.logger import configure_logging

logger = logging.getLogger(__name__)


def main():
    """
    Main function that runs a loop until a KeyboardInterrupt is caught.
    """
    config = Config.load()
    configure_logging(config.log_level)

    logger.info("Application started. Press Ctrl+C to exit.")
    logger.debug(f"Loaded configuration with Log Level: {config.log_level}")

    # Initialize Database and fetch current time
    db = Postgres(config)
    try:
        db_time = db.get_date()
        logger.info(f"Database Time: {db_time}")
    except Exception:
        logger.warning("Could not fetch time from database.")

    try:
        while True:
            # The application's main work loop can go here.
            # For now, it just sleeps to prevent high CPU usage.
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Application shutting down gracefully.")
        sys.exit(0)


if __name__ == "__main__":
    main()
