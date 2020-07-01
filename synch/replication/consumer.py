import logging

from synch.enums import ClickHouseEngine
from synch.factory import Global
from synch.replication.etl import etl_full
from synch.writer.collapsing_merge_tree import ClickHouseCollapsingMergeTree
from synch.writer.merge_tree import ClickHouseMergeTree

logger = logging.getLogger("synch.replication.consumer")


def consume(args):
    settings = Global.settings
    reader = Global.reader
    broker = Global.broker

    schema = args.schema
    engine = settings.schema_settings.get(schema).get("clickhouse_engine")
    if engine == ClickHouseEngine.merge_tree:
        writer_cls = ClickHouseMergeTree
    elif engine == ClickHouseEngine.collapsing_merge_tree:
        writer_cls = ClickHouseCollapsingMergeTree
    else:
        raise NotImplementedError
    writer = writer_cls(settings, broker)
    tables = settings.schema_settings.get(schema).get("tables")
    # try etl full
    if settings.auto_full_etl:
        etl_full(reader, writer, schema, tables)

    tables_pk = {}
    for table in tables:
        tables_pk[table] = reader.get_primary_key(schema, table)

    writer.start_consume(schema, tables_pk, args.last_msg_id, args.skip_error)
