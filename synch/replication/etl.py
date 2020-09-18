import logging
from typing import Dict

from synch.factory import get_reader, get_writer
from synch.settings import Settings

logger = logging.getLogger("synch.replication.etl")


def etl_full(
    alias: str, schema: str, tables_pk: Dict, renew=False,
):
    """
    full etl
    """
    reader = get_reader(alias)
    source_db_database = Settings.get_source_db_database(alias, schema)
    schema = source_db_database.get("database")
    writer = get_writer()
    if not writer.check_database_exists(schema):
        if source_db_database.get("auto_create") is not False:
            writer.create_database(schema, Settings.cluster_name())
        else:
            logger.warning(
                f"Can't etl since no database {schema} found in ClickHouse and auto_create=false"
            )
            exit(-1)
    for table in source_db_database.get("tables"):
        if table.get("auto_full_etl") is False:
            continue
        table_name = table.get("table")
        pk = tables_pk.get(table_name)
        writer = get_writer(table.get("clickhouse_engine"))
        if not pk and not renew:
            logger.warning(f"No pk found in {schema}.{table_name}, skip")
            continue
        elif isinstance(pk, tuple):
            pk = f"({','.join(pk)})"
        if renew:
            drop_sql = f"drop table if exists {schema}.{table_name}"
            writer.execute(drop_sql)
            logger.info(f"drop table success:{schema}.{table_name}")
        if not writer.check_table_exists(schema, table_name):
            sign_column = table.get("sign_column")
            version_column = table.get("version_column")
            writer.execute(
                writer.get_table_create_sql(
                    reader,
                    schema,
                    table_name,
                    pk,
                    table.get("partition_by"),
                    table.get("engine_settings"),
                    sign_column=sign_column,
                    version_column=version_column,
                )
            )
            if Settings.is_cluster():
                for w in get_writer(choice=False):
                    w.execute(
                        w.get_distributed_table_create_sql(
                            schema, table_name, Settings.get("clickhouse.distributed_suffix")
                        )
                    )
            if reader.fix_column_type and not table.get("skip_decimal"):
                writer.fix_table_column_type(reader, schema, table_name)
            full_insert_sql = writer.get_full_insert_sql(reader, schema, table_name, sign_column)
            writer.execute(full_insert_sql)
            logger.info(f"full data etl for {schema}.{table_name} success")
        else:
            logger.debug(
                f"{schema}.{table_name} exists, skip, or use --renew force etl with drop old tables"
            )
