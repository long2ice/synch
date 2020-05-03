def parse_partitions(partitions):
    ret_list = partitions.split(';')
    ret = {}
    for x in ret_list:
        if x:
            a = x.split('=')
            ret[a[0].strip()] = int(a[1].strip())
    return ret


def parse_schema_table(partitions):
    ret_list = partitions.split(';')
    schemas = set()
    tables = set()
    for x in ret_list:
        if x:
            a = x.split('=')[0].split('.')
            schemas.add(a[0].strip())
            tables.add(a[1].strip())
    return schemas, tables


DEBUG = True

MYSQL_HOST = 'mysql'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = '123456'
MYSQL_SERVER_ID = 1

REDIS_HOST = 'redis'
LOG_POS_PREFIX = 'mysql2ch'

CLICKHOUSE_HOST = 'clickhouse'
PARTITIONS = 'test.test=0'
SCHEMAS, TABLES = parse_schema_table(PARTITIONS)

# how many num to submit
INSERT_NUMS = 1
# how many seconds to submit
INSERT_INTERVAL = 60

KAFKA_SERVER = 'kafka'
KAFKA_TOPIC = 'mysql2ch'
# kafka partition mapping,partition must be unique
PARTITIONS = parse_partitions(PARTITIONS)

# init binlog file,will read when pos in redis not exists
INIT_BINLOG_FILE = '0'
INIT_BINLOG_POS = 0
