from synch.settings import Settings


def test_parse_config_file():
    ret = Settings.parse_config_file("../synch.yaml")
