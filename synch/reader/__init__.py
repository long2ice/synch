import abc
import logging
import signal
import time
from signal import Signals
from typing import Callable, Tuple, Union

from synch.broker import Broker
from synch.common import insert_log
from synch.settings import Settings

logger = logging.getLogger("synch.reader")


class Reader:
    cursor = None
    fix_column_type = False
    last_time = 0
    count = {}

    def __init__(self, alias: str):
        self.alias = alias
        source_db = Settings.get_source_db(alias)
        self.source_db = source_db
        self.host = source_db.get("host")
        self.port = source_db.get("port")
        self.user = source_db.get("user")
        self.password = source_db.get("password")

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def execute(self, sql, args=None):
        logger.debug(sql)
        self.cursor.execute(sql, args)
        ret = self.cursor.fetchall()
        return ret

    @abc.abstractmethod
    def get_primary_key(self, db: str, table: str) -> Union[None, str, Tuple[str, ...]]:
        raise NotImplementedError

    @abc.abstractmethod
    def start_sync(self, broker: Broker):
        raise NotImplementedError

    @abc.abstractmethod
    def signal_handler(self, signum: Signals, handler: Callable):
        raise NotImplementedError

    @abc.abstractmethod
    def get_source_select_sql(self, schema: str, table: str, sign_column: str = None):
        raise NotImplementedError

    def after_send(self, schema, table):
        now = int(time.time())
        schema_table = f"{schema}.{table}"
        self.count.setdefault(schema_table, 0)
        self.count[schema_table] += 1
        if self.last_time == 0:
            self.last_time = now
        if now - self.last_time >= Settings.insert_interval():
            for schema_table, num in self.count.items():
                logger.info(f"success send {num} events for {schema_table}")
                s, t = schema_table.split(".")
                insert_log(self.alias, s, t, num, 1)
            self.last_time = 0
            self.count = {}
