import logging
import psycopg2

from miner.config import Config

logger = logging.getLogger(__name__)


class Postgres:
    def __init__(self, config: Config):
        self.config = config

    def get_date(self):
        """
        Connects to the database and retrieves the current timestamp (now()).
        """
        try:
            with psycopg2.connect(self.config.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT now();")
                    return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
