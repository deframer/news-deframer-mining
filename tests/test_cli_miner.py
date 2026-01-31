from __future__ import annotations

from unittest.mock import MagicMock

from news_deframer.cli import miner as miner_cli


def test_build_parser_defaults_to_poll():
    parser = miner_cli.build_parser()
    args = parser.parse_args([])
    assert args.command == "poll"


def test_main_invokes_dispatch(monkeypatch):
    fake_config = MagicMock()
    called = {}

    monkeypatch.setattr("news_deframer.cli.miner.Config.load", lambda: fake_config)
    monkeypatch.setattr("news_deframer.cli.miner.configure_logging", lambda level: None)

    def fake_dispatch(command, config):
        called["command"] = command
        called["config"] = config

    monkeypatch.setattr("news_deframer.cli.miner.dispatch", fake_dispatch)

    exit_code = miner_cli.main(["poll"])

    assert exit_code == 0
    assert called["command"] == "poll"
    assert called["config"] is fake_config
