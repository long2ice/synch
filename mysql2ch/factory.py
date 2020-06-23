from typing import Optional

from MySQLdb.cursors import DictCursor
from mysql2ch.reader import Reader
from mysql2ch.settings import Settings
from mysql2ch.writer import ClickHouseWriter


class Global:
    """
    global instances
    """

    settings: Optional[Settings] = None
    reader: Optional[Reader] = None
    writer: Optional[ClickHouseWriter] = None

    @classmethod
    def init(cls, config):
        cls.settings = Settings.parse(config)
        cls.writer = ClickHouseWriter(
            host=cls.settings.clickhouse_host,
            port=cls.settings.clickhouse_port,
            password=cls.settings.clickhouse_password,
            user=cls.settings.clickhouse_user,
        )
        cls.reader = Reader(
            host=cls.settings.mysql_host,
            port=cls.settings.mysql_port,
            password=cls.settings.mysql_password,
            user=cls.settings.mysql_user,
            connect_timeout=5,
            cursorclass=DictCursor,
            charset="utf8",
        )
