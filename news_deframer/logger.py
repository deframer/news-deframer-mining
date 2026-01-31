import logging
import sys


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configures the root logger.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
        force=True,
    )
