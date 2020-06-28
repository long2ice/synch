import configparser
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from pydantic import BaseSettings


class BrokerType(str, Enum):
    redis = "redis"
    kafka = "kafka"


class SourceDatabase(str, Enum):
    mysql = "mysql"
    postgres = "postgres"


class Settings(BaseSettings):
    """
    settings
    """

    debug: bool = False
    environment: str = "development"
    broker_type: BrokerType = "redis"
    source_db: SourceDatabase = SourceDatabase.mysql

    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = "123456"
    mysql_server_id: int = 1

    postgres_host: str = "127.0.0.1"
    postgres_port: int = 5432
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"

    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_password: str = None
    redis_db: int = 0
    redis_prefix: str = "synch"
    redis_sentinel: bool = False
    redis_sentinel_master: str = "master"
    redis_sentinel_hosts: Optional[List[Tuple[str, int]]]

    clickhouse_host: str = "127.0.0.1"
    clickhouse_port: int = 9000
    clickhouse_user: str = "default"
    clickhouse_password: str = None

    kafka_servers: Set[str] = {"127.0.0.1"}
    kafka_topic: str = "synch"
    kafka_partitions: Optional[Dict[str, int]]

    sentry_dsn: Optional[str]
    schema_table: Dict[str, List[str]]
    init_binlog_file: str
    init_binlog_pos: int
    skip_delete_tables: Set[str]
    skip_update_tables: Set[str]
    skip_dmls: List[str]
    insert_num: int = 20000
    insert_interval: int = 60
    queue_max_len: int = 200000
    auto_full_etl: bool = True

    @classmethod
    def parse(cls, path: str) -> "Settings":
        parser = configparser.ConfigParser()
        parser.read(path)
        sentry = parser["sentry"]
        redis = parser["redis"]
        mysql = parser["mysql"]
        postgres = parser["postgres"]
        clickhouse = parser["clickhouse"]
        kafka = parser["kafka"]
        core = parser["core"]
        source_db = SourceDatabase(parser.get("core", "source_db"))
        schema_tables, partitions = cls._get_schemas(source_db, parser)

        return cls(
            environment=sentry.get("environment"),
            sentry_dsn=sentry.get("dsn"),
            mysql_host=mysql.get("host"),
            mysql_port=int(mysql.get("port")),
            mysql_user=mysql.get("user"),
            mysql_password=mysql.get("password"),
            init_binlog_file=mysql.get("init_binlog_file"),
            init_binlog_pos=int(mysql.get("init_binlog_pos") or 0),
            mysql_server_id=int(mysql.get("server_id")),
            postgres_host=postgres.get("host"),
            postgres_port=int(postgres.get("port")),
            postgres_user=postgres.get("user"),
            postgres_password=postgres.get("password"),
            redis_host=redis.get("host"),
            redis_port=int(redis.get("port")),
            redis_password=redis.get("password"),
            redis_db=int(redis.get("db")),
            redis_prefix=redis.get("prefix"),
            queue_max_len=int(redis.get("queue_max_len")),
            redis_sentinel=redis.get("sentinel") == "true",
            redis_sentinel_hosts=cls._get_sentinel_hosts(redis.get("sentinel_hosts")),
            redis_sentinel_master=redis.get("sentinel_master"),
            kafka_servers=set(kafka.get("servers").split(",")),
            kafka_topic=kafka.get("topic"),
            kafka_partitions=partitions,
            clickhouse_host=clickhouse.get("host"),
            clickhouse_port=int(clickhouse.get("port")),
            clickhouse_user=clickhouse.get("user"),
            clickhouse_password=clickhouse.get("password"),
            schema_table=schema_tables,
            skip_delete_tables=set(core.get("skip_delete_tables").split(",")),
            skip_update_tables=set(core.get("skip_update_tables").split(",")),
            skip_dmls=core.get("skip_dmls").split(","),
            insert_num=int(core.get("insert_num")),
            insert_interval=int(core.get("insert_interval")),
            broker_type=BrokerType(core.get("broker_type")),
            debug=core.get("debug") == "True",
            auto_full_etl=core.get("auto_full_etl") == "True",
            source_db=source_db,
        )

    @classmethod
    def _get_sentinel_hosts(cls, sentinel_hosts: str) -> List[Tuple[str, int]]:
        hosts = []
        for host in sentinel_hosts.split(","):
            host_split = host.split(":")
            hosts.append((host_split[0], int(host_split[1])))
        return hosts

    @classmethod
    def _get_schemas(
        cls, source_db, parser: configparser.ConfigParser
    ) -> Tuple[Dict[str, List[str]], Dict[str, int]]:
        schema_tables = {}
        schema_partitions = {}
        for item in filter(lambda x: x.startswith(f"{source_db}."), parser.sections()):
            schema = item.split(f"{source_db}.")[-1]
            schema_tables[schema] = parser.get(item, "tables").split(",")
            schema_partitions[schema] = parser.getint(item, "kafka_partition")
        return schema_tables, schema_partitions
