import logging

from synch.factory import Global

logger = logging.getLogger("synch.replication.etl")


def make_etl(args):
    schema = args.schema
    tables = args.tables
    renew = args.renew
    engine = args.engine
    partition_by = args.partition_by
    settings = args.settings
    Global.reader.etl_full(
        Global.writer, schema, tables.split(","), renew, engine, partition_by, settings
    )
