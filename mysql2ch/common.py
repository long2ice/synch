import datetime
import json
import logging
import sys
from decimal import Decimal
from typing import Dict, List, Optional, Union

import dateutil.parser
from kafka import KafkaAdminClient
from kafka.admin import NewPartitions
from pydantic import BaseSettings, HttpUrl, validator


def partitioner(key_bytes, all_partitions, available_partitions):
    """
    custom partitioner depend on settings
    :param key_bytes:
    :param all_partitions:
    :param available_partitions:
    :return:
    """
    key = key_bytes.decode()
    partition = Global.settings.schema_table.get(key).get("kafka_partition")
    return all_partitions[partition]


def init_logging(debug):
    logger = logging.getLogger("mysql2ch")
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(sh)


class Settings(BaseSettings):
    debug: bool = True
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
    log_pos_prefix: str = "mysql2ch"
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


class Global:
    settings: Optional["Settings"] = None
    reader = None
    writer = None


logger = logging.getLogger("mysql2ch.common")

CONVERTERS = {
    "date": dateutil.parser.parse,
    "datetime": dateutil.parser.parse,
    "decimal": Decimal,
}


def complex_decode(xs):
    if isinstance(xs, dict):
        ret = {}
        for k in xs:
            ret[k.decode()] = complex_decode(xs[k])
        return ret
    elif isinstance(xs, list):
        ret = []
        for x in xs:
            ret.append(complex_decode(x))
        return ret
    elif isinstance(xs, bytes):
        return xs.decode()
    else:
        return xs


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return {"val": obj.strftime("%Y-%m-%d %H:%M:%S"), "_spec_type": "datetime"}
        elif isinstance(obj, datetime.date):
            return {"val": obj.strftime("%Y-%m-%d"), "_spec_type": "date"}
        elif isinstance(obj, Decimal):
            return {"val": str(obj), "_spec_type": "decimal"}
        else:
            return super().default(obj)


def object_hook(obj):
    _spec_type = obj.get("_spec_type")
    if not _spec_type:
        return obj

    if _spec_type in CONVERTERS:
        return CONVERTERS[_spec_type](obj["val"])
    else:
        raise TypeError("Unknown {}".format(_spec_type))


def init_partitions(settings: Settings):
    client = KafkaAdminClient(bootstrap_servers=settings.kafka_server,)
    try:
        client.create_partitions(
            topic_partitions={
                settings.kafka_topic: NewPartitions(total_count=len(settings.schema_table.keys()))
            }
        )
    except Exception as e:
        logger.warning(f"init_partitions error:{e}")
