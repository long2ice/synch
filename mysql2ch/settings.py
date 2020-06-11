import configparser
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseSettings


class BrokerType(str, Enum):
    redis = "redis"
    kafka = "kafka"


class Settings(BaseSettings):
    """
    settings
    """

    environment: str = "development"
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = "123456"
    mysql_server_id: int = 1
    redis_host: str = "120.0.0.1"
    redis_port: int = 6379
    redis_password: str = None
    redis_db: int = 0
    redis_prefix: str = "mysql2ch"
    clickhouse_host: str = "127.0.0.1"
    clickhouse_port: int = 9000
    clickhouse_user: str = "default"
    clickhouse_password: str = None
    sentry_dsn: Optional[str]
    schema_table: Dict[str, List[str]]
    init_binlog_file: str
    init_binlog_pos: int
    skip_delete_tables: List[str]
    skip_update_tables: List[str]
    skip_dmls: List[str]
    insert_num: int = 20000
    insert_interval: int = 60
    queue_max_len: int = 200000

    @classmethod
    def parse(cls, path: str) -> "Settings":
        parser = configparser.ConfigParser()
        parser.read(path)
        sentry = parser["sentry"]
        redis = parser["redis"]
        mysql = parser["mysql"]
        clickhouse = parser["clickhouse"]
        core = parser["core"]

        return cls(
            environment=sentry["environment"],
            sentry_dsn=sentry["dsn"],
            mysql_host=mysql["host"],
            mysql_port=int(mysql["port"]),
            mysql_user=mysql["user"],
            mysql_password=mysql["password"],
            redis_host=redis["host"],
            redis_port=int(redis["port"]),
            redis_password=redis["password"],
            redis_db=int(redis["db"]),
            redis_prefix=redis["prefix"],
            clickhouse_host=clickhouse["host"],
            clickhouse_port=int(clickhouse["port"]),
            clickhouse_user=clickhouse["user"],
            clickhouse_password=clickhouse["password"],
            mysql_server_id=int(core["mysql_server_id"]),
            schema_table=cls._get_schema_tables(parser),
            init_binlog_file=core["init_binlog_file"],
            init_binlog_pos=int(core["init_binlog_pos"]),
            skip_delete_tables=core["skip_delete_tables"].split(","),
            skip_update_tables=core["skip_update_tables"].split(","),
            skip_dmls=core["skip_dmls"].split(","),
            insert_num=int(core["insert_num"]),
            insert_interval=int(core["insert_interval"]),
            queue_max_len=int(core["queue_max_len"]),
        )

    @classmethod
    def _get_schema_tables(cls, parser: configparser.ConfigParser) -> Dict[str, List[str]]:
        ret = {}
        for item in filter(lambda x: x.startswith("mysql."), parser.sections()):
            ret[item.split("mysql.")[-1]] = parser[item]["tables"].split(",")
        return ret
