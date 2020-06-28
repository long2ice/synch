import logging

from mysql2ch.factory import Global

logger = logging.getLogger("mysql2ch.replication.etl")


def make_etl(args):
    schema = args.schema
    tables = args.tables
    renew = args.renew
    Global.reader.etl_full(Global.writer, schema, tables, renew)
