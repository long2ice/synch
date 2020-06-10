from typing import Optional

from mysql2ch.reader import MysqlReader
from mysql2ch.settings import Settings
from mysql2ch.writer import ClickHouseWriter


class Global:
    """
    global instances
    """

    settings: Optional[Settings] = None
    reader: Optional[MysqlReader] = None
    writer: Optional[ClickHouseWriter] = None

    @classmethod
    def init(cls, config):
        if cls.settings:
            return
        cls.settings = Settings.parse(config)
        cls.writer = ClickHouseWriter(
            host=cls.settings.clickhouse_host,
            port=cls.settings.clickhouse_port,
            password=cls.settings.clickhouse_password,
            user=cls.settings.clickhouse_user,
        )
        cls.reader = MysqlReader(
            host=cls.settings.mysql_host,
            port=cls.settings.mysql_port,
            password=cls.settings.mysql_password,
            user=cls.settings.mysql_user,
        )
