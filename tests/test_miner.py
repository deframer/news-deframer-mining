from news_deframer.cli import miner as miner_cli


def test_miner_cli_import():
    assert callable(miner_cli.main)
