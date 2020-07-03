from synch.settings import Settings


def test_parse_config_file():
    setting = Settings("synch.yaml")
    config = setting._config
    assert isinstance(config, dict)
