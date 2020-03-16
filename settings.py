import os

from dotenv import load_dotenv
import sentry_sdk


def parse_partitions(partitions):
    ret_list = partitions.split(';')
    ret = {}
    for x in ret_list:
        if x:
            a = x.split('=')
            ret[a[0]] = int(a[1])
    return ret


load_dotenv()
sentry_sdk.init(
    os.getenv('SENTRY_DSN'),
    environment=os.getenv('ENVIRONMENT', 'development')
)
DEBUG = os.getenv('DEBUG') == 'True'

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_SERVER_ID = os.getenv('MYSQL_SERVER_ID')

REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_DB = os.getenv('REDIS_DB')
LOG_POS_PREFIX = os.getenv('REDIS_DB') or 'mysql2ch'

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')

# only these schemas to replication
SCHEMAS = os.getenv('SCHEMAS').split(',')
# only these tables to replication
TABLES = os.getenv('TABLES').split(',')

# which table to skip delete or update
SKIP_DELETE_TB_NAME = (os.getenv('SKIP_DELETE_TB_NAME') or '').split(',')
SKIP_UPDATE_TB_NAME = (os.getenv('SKIP_UPDATE_TB_NAME') or '').split(',')

# skip delete or update
SKIP_TYPE = (os.getenv('SKIP_TYPE') or '').split(',')

# how many num to submit
INSERT_NUMS = int(os.getenv('INSERT_NUMS') or 20000)
# how many seconds to submit
INSERT_INTERVAL = int(os.getenv('INSERT_INTERVAL') or 60)

KAFKA_SERVER = os.getenv('KAFKA_SERVER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
# kafka partition mapping,partition must be unique
PARTITIONS = parse_partitions(os.getenv('PARTITIONS'))

# init binlog file,will read when pos in redis not exists
INIT_BINLOG_FILE = os.getenv('INIT_BINLOG_FILE')
INIT_BINLOG_POS = os.getenv('INIT_BINLOG_POS')
