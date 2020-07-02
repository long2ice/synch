import json

from synch.settings import Settings


def test_parse_config_file():
    setting = Settings("../synch.yaml")
    config = setting._config
    print(json.dumps(config))
