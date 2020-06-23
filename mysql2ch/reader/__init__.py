import abc
import logging
from copy import deepcopy
from typing import Tuple, Union

from mysql2ch.common import complex_decode

logger = logging.getLogger("mysql2ch.reader")


class Reader:
    cursor = None

    def __init__(self, host: str, port: int, user: str, password: str, **extra):
        self.password = password
        self.user = user
        self.port = port
        self.host = host
        self.extra = extra

    def convert_values(self, values):
        cp_values = deepcopy(values)
        for k, v in values.items():
            cp_values[k] = complex_decode(v)
        return cp_values

    def execute(self, sql, args=None):
        logger.debug(sql)
        self.cursor.execute(sql, args)
        return self.cursor.fetchall()

    @abc.abstractmethod
    def get_binlog_pos(self) -> Tuple[str, str]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_primary_key(self, db: str, table: str) -> Union[None, str, Tuple[str, ...]]:
        raise NotImplementedError

    @abc.abstractmethod
    def start_replication(self):
        raise NotImplementedError
