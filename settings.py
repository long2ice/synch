import os

from dotenv import load_dotenv
import sentry_sdk

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

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')

# only these schemas to replication
SCHEMAS = ('test',)
# only these tables to replication
TABLES = ('test',)

# which table to skip delete or update
SKIP_DELETE_TB_NAME = ()
SKIP_UPDATE_TB_NAME = ()

# skip delete or update
SKIP_TYPE = ()

# how many num to submit
INSERT_NUMS = 1
# how many seconds to submit
INSERT_INTERVAL = 1

KAFKA_SERVER = os.getenv('KAFKA_SERVER')
KAFKA_TOPIC = 'mysql2ch'
# kafka partition mapping,partition must be unique
PARTITIONS = {
    'test.test': 0,
}
