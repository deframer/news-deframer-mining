from __future__ import annotations

from unittest.mock import MagicMock

from news_deframer.cli import miner as miner_cli


def test_main_runs_poll(monkeypatch):
    fake_config = MagicMock()
    called = {}

    monkeypatch.setattr("news_deframer.cli.miner.Config.load", lambda: fake_config)
    monkeypatch.setattr("news_deframer.cli.miner.configure_logging", lambda level: None)

    def fake_poll(config):
        called["config"] = config

    monkeypatch.setattr("news_deframer.cli.miner.poller_module.poll", fake_poll)

    exit_code = miner_cli.main([])

    assert exit_code == 0
    assert called["config"] is fake_config
