import configparser
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Config:
    config = None
    pos_handler = None

    @classmethod
    def set_config_file(cls, config_file=None):
        if not config_file:
            config_file = os.path.join(BASE_DIR, 'config.ini')
        cls.config = configparser.ConfigParser()
        cls.config.read(config_file)

    @classmethod
    def get(cls, name):
        return cls.config[name]
