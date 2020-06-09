from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseSettings, HttpUrl, validator


class BrokerType(str, Enum):
    redis = 'redis'
    kafka = 'kafka'


class Settings(BaseSettings):
    """
    settings
    """
    debug: bool = True
    environment: str = "development"
    broker_type: BrokerType = BrokerType.redis
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
    kafka_server: str = "127.0.0.1:9092"
    kafka_topic: str = "test"
    sentry_dsn: Optional[HttpUrl]
    schema_table: Dict[str, Dict[str, Union[List[str], int]]]
    init_binlog_file: str
    init_binlog_pos: int
    skip_delete_tables: List[str]
    skip_update_tables: List[str]
    skip_dmls: List[str]
    insert_num: int = 20000
    insert_interval: int = 60

    @validator("schema_table")
    def check_kafka_partition(cls, v):
        partitions = list(map(lambda x: v.get(x).get("kafka_partition"), v))
        if len(partitions) != len(set(partitions)):
            raise ValueError("kafka_partition must be unique for schema!")
        return v
