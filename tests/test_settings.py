from synch.settings import Settings


def test_parse_config_file():
    Settings.init("synch.yaml")
