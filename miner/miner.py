import logging
import sys
import time

from miner.config import Config, IDLE_SLEEP_TIME
from miner.postgres import Postgres

logger = logging.getLogger(__name__)


def run(config: Config) -> None:
    """
    Runs the main mining loop.
    """
    logger.info("Miner started. Press Ctrl+C to exit.")
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
            time.sleep(IDLE_SLEEP_TIME)
    except KeyboardInterrupt:
        logger.info("Application shutting down gracefully.")
        sys.exit(0)