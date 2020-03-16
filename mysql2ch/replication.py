import logging

import settings
from mysql2ch import reader, writer

logger = logging.getLogger('mysql2ch.replication')


def etl_full(schema, tables):
    assert schema in settings.SCHEMAS, 'schema must in settings.SCHEMAS'
    tables = tables.split(',')
    for table in tables:
        assert table in settings.TABLES, 'table must in settings.TABLES'
    for table in tables:
        pk = reader.get_primary_key(schema, table)
        sql = f"CREATE TABLE {schema}.{table} ENGINE = MergeTree ORDER BY {pk} AS SELECT * FROM mysql('{settings.MYSQL_HOST}:{settings.MYSQL_PORT}', '{schema}', '{table}', '{settings.MYSQL_USER}', '{settings.MYSQL_PASSWORD}')"
        try:
            writer.execute(sql)
            writer.fix_table_column_type(reader, schema, table)
            logger.info(f'etl success:{schema}.{table}')
        except Exception as e:
            logger.error(f'etl error:{schema}.{table},{e}')
