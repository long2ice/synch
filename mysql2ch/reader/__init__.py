import abc
import logging
from copy import deepcopy
from typing import Generator, Tuple, Union

from mysql2ch.broker import Broker
from mysql2ch.common import complex_decode
from mysql2ch.settings import Settings

logger = logging.getLogger("mysql2ch.reader")


class Reader:

    def __init__(self, settings: Settings):
        self.settings = settings

    def convert_values(self, values):
        cp_values = deepcopy(values)
        for k, v in values.items():
            cp_values[k] = complex_decode(v)
        return cp_values

    @abc.abstractmethod
    def get_primary_key(self, db: str, table: str) -> Union[None, str, Tuple[str, ...]]:
        raise NotImplementedError

    @abc.abstractmethod
    def start_sync(self, broker: Broker):
        raise NotImplementedError
