"""News Deframer miner submodule."""

from news_deframer.config import Config

from .miner import poll

__all__ = ["Config", "poll"]
