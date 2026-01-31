import os
from dataclasses import dataclass

from dotenv import load_dotenv

# PollingInterval defines how often a single feed is data mined
# If a feed was synced at T, it will be eligible again at T + PollingInterval.
POLLING_INTERVAL = 600  # 10 minutes

# IdleSleepTime defines how long the worker sleeps when no feeds are due for mining.
IDLE_SLEEP_TIME = 10  # 10 seconds

# Default lock duration for a miner poll
DEFAULT_LOCK_DURATION = 5 * 60  # 5 minutes


@dataclass
class Config:
    dsn: str
    log_level: str
    log_database: bool
    duck_db_file: str

    @classmethod
    def load(cls) -> "Config":
        """
        Load configuration from environment variables.
        This is where you would add CLI argument parsing (e.g. argparse)
        to override environment variables if needed.
        """
        # Only load .env if NOT running in Docker
        if not os.path.exists("/.dockerenv"):
            load_dotenv()

        return cls(
            # The DSN string from your env.example is compatible with psycopg2 directly.
            dsn=os.getenv("DSN", ""),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_database=os.getenv("LOG_DATABASE", "false").lower() == "true",
            duck_db_file=os.getenv("DUCK_DB_FILE", "./trend_docs.duckdb"),
        )
