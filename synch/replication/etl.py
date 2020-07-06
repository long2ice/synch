import logging
from typing import Dict

from synch.enums import ClickHouseEngine, SourceDatabase
from synch.factory import get_reader, get_writer
from synch.settings import Settings

logger = logging.getLogger("synch.replication.etl")


def get_source_select_sql(schema: str, table: str, source_db: Dict, addition_column: str = ""):
    """
    get source db select sql from different db
    """
    select = "*"
    user = source_db.get("user")
    host = source_db.get("host")
    port = source_db.get("port")
    password = source_db.get("password")
    db_type = source_db.get("db_type")
    if addition_column:
        select += f", toInt8(1) as {addition_column}"
    if db_type == SourceDatabase.postgres.value:
        return f"SELECT {select} FROM jdbc('postgresql://{host}:{port}/{schema}?user={user}&password={password}', '{table}')"
    elif db_type == SourceDatabase.mysql.value:
        return f"SELECT {select} FROM mysql('{host}:{port}', '{schema}', '{table}', '{user}', '{password}')"


def get_table_create_sql(
    schema: str,
    table: str,
    pk: str,
    partition_by: str,
    engine_settings: str,
    engine: ClickHouseEngine,
    source_db: Dict,
    **kwargs,
):
    """
    get table create sql from by settings
    """
    partition_by_str = ""
    engine_settings_str = ""
    if partition_by:
        partition_by_str = f" PARTITION BY {partition_by} "
    if engine_settings:
        engine_settings_str = f" SETTINGS {engine_settings} "
    if engine == ClickHouseEngine.merge_tree:
        select_sql = get_source_select_sql(schema, table, source_db)
        return f"CREATE TABLE {schema}.{table} ENGINE = {engine} {partition_by_str} ORDER BY {pk} {engine_settings_str} AS {select_sql} limit 0"
    elif engine == ClickHouseEngine.collapsing_merge_tree:
        sign_column = kwargs.get("sign_column")
        select_sql = get_source_select_sql(schema, table, source_db, sign_column)
        return f"CREATE TABLE {schema}.{table} ENGINE = {engine}({sign_column}) {partition_by_str} ORDER BY {pk} {engine_settings_str} AS {select_sql} limit 0"


def get_full_insert_sql(schema: str, source_db: Dict, source_db_database_table: Dict):
    engine = source_db_database_table.get("clickhouse_engine")
    table = source_db_database_table.get("table")
    if engine == ClickHouseEngine.merge_tree:
        return f"insert into {schema}.{table} {get_source_select_sql(schema, table, source_db)}"
    elif engine == ClickHouseEngine.collapsing_merge_tree:
        sign_column = source_db_database_table.get("sign_column")
        return f"insert into {schema}.{table} {get_source_select_sql(schema, table, source_db, sign_column)}"


def etl_full(
    alias: str, schema: str, tables_pk: Dict, renew=False,
):
    """
    full etl
    """
    reader = get_reader(alias)
    source_db = Settings.get_source_db(alias)
    source_db_database = Settings.get_source_db_database(alias, schema)
    schema = source_db_database.get("database")
    for table in source_db_database.get("tables"):
        if table.get("auto_full_etl") is False:
            continue
        table_name = table.get("table")
        pk = tables_pk.get(table_name)
        writer = get_writer(table.get("clickhouse_engine"))
        if not pk:
            logger.warning(f"No pk found in {schema}.{table_name}, skip")
            continue
        elif isinstance(pk, tuple):
            pk = f"({','.join(pk)}"
        if renew:
            drop_sq = f"drop table {schema}.{table_name}"
            try:
                writer.execute(drop_sq)
                logger.info(f"drop table success:{schema}.{table_name}")
            except Exception:
                logger.warning(f"Try to drop table {schema}.{table_name} fail")
        if not writer.table_exists(schema, table_name):
            writer.execute(
                get_table_create_sql(
                    schema,
                    table_name,
                    pk,
                    table.get("partition_by"),
                    table.get("engine_settings"),
                    table.get("clickhouse_engine"),
                    source_db,
                    sign_column=table.get("sign_column"),
                )
            )
            if reader.fix_column_type:
                writer.fix_table_column_type(reader, schema, table_name)
            source_db_database_table = Settings.get_source_db_database_table(
                alias, schema, table_name
            )
            writer.execute(get_full_insert_sql(schema, source_db, source_db_database_table))
            logger.info(f"full data etl for {schema}.{table_name} success")
        else:
            logger.info(
                f"{schema}.{table_name} exists, skip, or use --renew force etl with drop old tables"
            )
