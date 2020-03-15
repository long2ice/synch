import argparse
import logging

import settings
from mysql2ch import init_logging, pos_handler
from mysql2ch.consumer import consume
from mysql2ch.producer import produce
from mysql2ch.replication import etl_full

parser = argparse.ArgumentParser()

init_logging(settings.DEBUG)
logger = logging.getLogger('mysql2ch.manage')


def make_etl(args):
    etl_database = args.etl_database
    etl_tables = args.etl_tables
    etl_full(etl_database, etl_tables)


def set_log_pos(args):
    binlog_file = args.binlog_file
    binlog_pos = args.binlog_pos
    pos_handler.set_log_pos_slave(binlog_file, binlog_pos)
    logger.info(f'set binlog in redis success: {binlog_file}:{binlog_pos}')


if __name__ == '__main__':
    subparsers = parser.add_subparsers(title='subcommands')
    parser_etl = subparsers.add_parser('etl')
    parser_etl.add_argument('--etl-database', required=True, help='Database to full etl.')
    parser_etl.add_argument('--etl-tables', required=True, help='Table to full etl,multiple tables split with comma.')
    parser_etl.add_argument('--debug', default=False, action='store_true', help='Display SQL information.')
    parser_etl.set_defaults(func=make_etl)

    parser_log_pos = subparsers.add_parser('setpos')
    parser_log_pos.add_argument('--binlog-file', required=True, help='Binlog file.')
    parser_log_pos.add_argument('--binlog-pos', required=True, help='Binlog position.')
    parser_log_pos.set_defaults(func=set_log_pos)

    parser_producer = subparsers.add_parser('produce')
    parser_producer.set_defaults(func=produce)

    parser_consumer = subparsers.add_parser('consume')
    parser_consumer.add_argument('--schema', required=True, help='Schema to consume.')
    parser_consumer.add_argument('--table', required=True, help='Table to consume.')
    parser_consumer.set_defaults(func=consume)

    parse_args = parser.parse_args()
    parse_args.func(parse_args)
