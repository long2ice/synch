import logging

import settings
from mysql2ch import reader, writer

logger = logging.getLogger('mysql2ch.replication')


def etl_full(database, tables):
    tables = tables.split(',')
    for table in tables:
        pk = reader.get_primary_key(database, table)
        sql = f"CREATE TABLE {database}.{table} ENGINE = MergeTree ORDER BY {pk} AS SELECT * FROM mysql('{settings.MYSQL_HOST}:{settings.MYSQL_PORT}', '{database}', '{table}', '{settings.MYSQL_USER}', '{settings.MYSQL_PASSWORD}')"
        try:
            writer.execute(sql)
            writer.fix_table_column_type(reader, database, table)
            logger.info(f'etl success:{database}.{table}')
        except Exception as e:
            logger.error(f'etl error:{database}.{table},{e}')
