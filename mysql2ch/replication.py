import logging

from . import reader, writer
from mysql2ch import settings

logger = logging.getLogger('mysql2ch.replication')


def etl_full(schema, tables, renew=False):
    assert schema in settings.SCHEMAS, 'schema must in settings.SCHEMAS'
    tables = tables.split(',')
    for table in tables:
        assert table in settings.TABLES, 'table must in settings.TABLES'
    for table in tables:
        pk = reader.get_primary_key(schema, table)
        if renew:
            drop_sq = f'drop table {schema}.{table}'
            try:
                writer.execute(drop_sq)
                logger.info(f'drop table success:{schema}.{table}')
            except Exception as e:
                logger.error(f'etl error with renew:{schema}.{table},{e}')
        try:
            sql = f"CREATE TABLE {schema}.{table} ENGINE = MergeTree ORDER BY {pk} AS SELECT * FROM mysql('{settings.MYSQL_HOST}:{settings.MYSQL_PORT}', '{schema}', '{table}', '{settings.MYSQL_USER}', '{settings.MYSQL_PASSWORD}')"
            writer.execute(sql)
            writer.fix_table_column_type(reader, schema, table)
            logger.info(f'etl success:{schema}.{table}')
        except Exception as e:
            logger.error(f'etl error:{schema}.{table},{e}')
