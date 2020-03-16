import logging
import logging.handlers
import sys

import settings
from mysql2ch.pos import RedisLogPos
from mysql2ch.reader import MysqlReader
from mysql2ch.writer import ClickHouseWriter


def partitioner(key_bytes, all_partitions, available_partitions):
    """
    custom partitioner depend on settings
    :param key_bytes:
    :param all_partitions:
    :param available_partitions:
    :return:
    """
    key = key_bytes.decode()
    values = settings.PARTITIONS.values()
    assert len(set(values)) == len(values), 'partition must be unique'
    return all_partitions[settings.PARTITIONS.get(key)]


def init_logging(debug):
    logger = logging.getLogger('mysql2ch')
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(logging.Formatter(
        fmt='%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    logger.addHandler(sh)


writer = ClickHouseWriter(
    host=settings.CLICKHOUSE_HOST,
    port=settings.CLICKHOUSE_PORT,
    password=settings.CLICKHOUSE_PASSWORD,
    user=settings.CLICKHOUSE_USER
)
reader = MysqlReader(
    host=settings.MYSQL_HOST,
    port=settings.MYSQL_PORT,
    password=settings.MYSQL_PASSWORD,
    user=settings.MYSQL_USER
)

pos_handler = RedisLogPos(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    password=settings.REDIS_PASSWORD,
    db=settings.REDIS_DB,
    log_pos_prefix=settings.LOG_POS_PREFIX,
    server_id=settings.MYSQL_SERVER_ID
)

__version__ = '0.1.0'
