import logging

from mysql2ch.config import Config
from mysql2ch.pos import FileLogPos, RedisLogPos
from mysql2ch.reader import MysqlReader
from mysql2ch.writer import ClickHouseWriter

logger = logging.getLogger('mysql2ch.replication')


def run_replication(pos_type):
    master_server = Config.get('master_server')
    redis_server = Config.get('redis_server')
    bulk_insert_nums = Config.get('bulk_insert_nums')
    only_schemas = Config.get('only_schemas')['schemas'].split(',')
    only_tables = Config.get('only_tables')['tables'].split(',')
    server_id = master_server.pop('server_id')
    if pos_type == 'file':
        pos_handler = FileLogPos(Config.get('log_position').get('log_pos_file'))
    else:
        pos_handler = RedisLogPos(**redis_server, server_id=server_id)
    Config.pos_handler = pos_handler

    log_file, log_pos = pos_handler.get_log_pos()
    reader = MysqlReader(**master_server, )
    clickhouse_server = Config.get('clickhouse_server')
    writer = ClickHouseWriter(**clickhouse_server)

    skip_delete_tb_name = Config.get('skip_dmls_sing')['skip_delete_tb_name'].split(',')
    skip_update_tb_name = Config.get('skip_dmls_sing')['skip_update_tb_name'].split(',')
    skip_dmls_all = Config.get('skip_dmls_all')['skip_type'].split(',')

    for data, pk_dict, file, pos in reader.binlog_reading(
            only_tables=only_tables,
            only_schemas=only_schemas,
            log_file=log_file,
            log_pos=log_pos,
            insert_nums=bulk_insert_nums.get('insert_nums'),
            interval=int(bulk_insert_nums.get('interval')),
            server_id=int(server_id)
    ):
        status = writer.insert_event(data, skip_dmls_all, skip_delete_tb_name, pk_dict, only_schemas)
        if status:
            Config.pos_handler.set_log_pos_slave(file, pos)
        else:
            log_file, log_pos = Config.pos_handler.get_log_pos()
            message = 'SQL执行错误,当前binlog位置 {0}:{1}'.format(log_file, log_pos)
            logger.error(message)
            exit(1)


def etl_full(database, table):
    only_schemas = Config.get('only_schemas')['schemas'].split(',')
    only_tables = Config.get('only_tables')['tables'].split(',')
    if database not in only_schemas:
        logger.error(f'Database {database} not in only_schemas')
        exit(1)
    tables = []
    if table:
        if table not in only_tables:
            logger.error(f'Table {table} not in only_schemas')
            exit(1)
        else:
            tables.append(table)
    else:
        tables = only_tables
    master_server = Config.get('master_server')
    host = master_server.get('host')
    port = master_server.get('port')
    password = master_server.get('password')
    user = master_server.get('user')
    clickhouse_server = Config.get('clickhouse_server')
    writer = ClickHouseWriter(
        **clickhouse_server
    )
    reader = MysqlReader(
        host=host, user=user, port=port, password=password
    )
    for table in tables:
        pk = reader.get_primary_key(database, table)[0]
        sql = f"CREATE TABLE {database}.{table} ENGINE = MergeTree ORDER BY {pk} AS SELECT * FROM mysql('{host}:{port}', '{database}', '{table}', '{user}', '{password}')"
        try:
            writer.client.execute(sql)
            logger.info(f'全量迁移成功：{database}.{table}')
        except Exception as e:
            logger.error(f'全量迁移失败：{database}.{table}，{e}')
