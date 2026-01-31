"""Miner worker routines."""

from __future__ import annotations

import logging
import time

from news_deframer.config import IDLE_SLEEP_TIME, Config
from news_deframer.database.postgres import Postgres

logger = logging.getLogger(__name__)


def poll(config: Config) -> None:
    """Run the polling loop until interrupted."""
    logger.info("Miner poll started. Press Ctrl+C to exit.")
    logger.debug("Loaded configuration: log level=%s", config.log_level)

    db = Postgres(config)
    try:
        db_time = db.get_date()
        logger.info("Database time: %s", db_time)
    except Exception:
        logger.warning("Could not fetch time from database.")

    try:
        while True:
            logger.debug("Poll tick; sleeping for %s seconds", IDLE_SLEEP_TIME)
            time.sleep(IDLE_SLEEP_TIME)
    except KeyboardInterrupt:
        logger.info("Poll interrupted. Exiting.")
