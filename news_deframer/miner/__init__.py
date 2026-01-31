"""News Deframer miner submodule."""

from news_deframer.config import Config

from .poller import poll
from . import miner

__all__ = ["Config", "poll", "miner"]
