"""News Deframer miner submodule."""

from news_deframer.config import Config

from .poller import poll

__all__ = ["Config", "poll"]
