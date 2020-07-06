from synch.settings import Settings


def test_parse_config_file():
    Settings.init("synch.yaml")


def test_settings():
    assert isinstance(Settings.debug(), bool)
    assert isinstance(Settings.insert_num(), int)
    assert isinstance(Settings.insert_interval(), int)
