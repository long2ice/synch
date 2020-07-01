import logging
from typing import List

from synch.enums import ClickHouseEngine, SourceDatabase
from synch.factory import Global

logger = logging.getLogger("synch.replication.etl")


def get_source_select_sql(schema: str, table: str, addition_column: str = ""):
    """
    get source db select sql from different db
    """
    settings = Global.settings
    select = "*"
    if addition_column:
        select += f", toInt8(1) as {addition_column}"
    if settings.source_db == SourceDatabase.postgres:
        return f"SELECT {select} FROM jdbc('postgresql://{settings.postgres_host}:{settings.postgres_port}/{schema}?user={settings.postgres_user}&password={settings.postgres_password}', '{table}')"
    elif settings.source_db == SourceDatabase.mysql:
        return f"SELECT {select} FROM mysql('{settings.mysql_host}:{settings.mysql_port}', '{schema}', '{table}', '{settings.mysql_user}', '{settings.mysql_password}')"


def get_table_create_sql(schema: str, table: str, pk: str, partition_by: str, engine_settings: str):
    """
    get table create sql from by settings
    """
    settings = Global.settings
    partition_by_str = ""
    engine_settings_str = ""
    if partition_by:
        partition_by_str = f" PARTITION BY {partition_by} "
    if engine_settings:
        engine_settings_str = f" SETTINGS {engine_settings} "
    schema_setting = settings.schema_settings.get(schema)
    engine = schema_setting.get("clickhouse_engine")
    sign_column = schema_setting.get("sign_column")
    if engine == ClickHouseEngine.merge_tree:
        select_sql = get_source_select_sql(schema, table)
        return f"CREATE TABLE {schema}.{table} ENGINE = {engine} {partition_by_str} ORDER BY {pk} {engine_settings_str} AS {select_sql} limit 0"
    elif engine == ClickHouseEngine.collapsing_merge_tree:
        select_sql = get_source_select_sql(schema, table, sign_column)
        return f"CREATE TABLE {schema}.{table} ENGINE = {engine}({sign_column}) {partition_by_str} ORDER BY {pk} {engine_settings_str} AS {select_sql} limit 0"


def get_full_insert_sql(
    schema: str, table: str,
):
    settings = Global.settings
    schema_setting = settings.schema_settings.get(schema)
    engine = schema_setting.get("clickhouse_engine")
    if engine == ClickHouseEngine.merge_tree:
        return f"insert into {schema}.{table} {get_source_select_sql(schema, table)}"
    elif engine == ClickHouseEngine.collapsing_merge_tree:
        settings = Global.settings
        sign_column = settings.schema_settings.get(schema).get("sign_column")
        return f"insert into {schema}.{table} {get_source_select_sql(schema, table, sign_column)}"


def etl_full(
    reader,
    writer,
    schema,
    tables: List[str] = None,
    renew=False,
    partition_by=None,
    engine_settings=None,
):
    """
    full etl
    """
    settings = Global.settings
    if not tables:
        tables = settings.schema_table.get(schema)

    for table in tables:
        pk = reader.get_primary_key(schema, table)
        if not pk:
            logger.warning(f"No pk found in {schema}.{table}, skip")
            continue
        elif isinstance(pk, tuple):
            pk = f"({','.join(pk)}"
        if renew:
            drop_sq = f"drop table {schema}.{table}"
            try:
                writer.execute(drop_sq)
                logger.info(f"drop table success:{schema}.{table}")
            except Exception as e:
                logger.warning(f"Try to drop table {schema}.{table} fail")
        if not writer.table_exists(schema, table):
            writer.execute(get_table_create_sql(schema, table, pk, partition_by, engine_settings,))
            if reader.fix_column_type:
                writer.fix_table_column_type(reader, schema, table)
            writer.execute(get_full_insert_sql(schema, table,))
            logger.info(f"etl success:{schema}.{table}")


def make_etl(args):
    schema = args.schema
    tables = args.tables
    renew = args.renew
    partition_by = args.partition_by
    settings = args.settings
    etl_full(Global.reader, Global.writer, schema, tables.split(","), renew, partition_by, settings)
