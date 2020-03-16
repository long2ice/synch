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
    schema = args.schema
    tables = args.tables
    etl_full(schema, tables)


def cli():
    subparsers = parser.add_subparsers(title='subcommands')
    parser_etl = subparsers.add_parser('etl')
    parser_etl.add_argument('--schema', required=True, help='Schema to full etl.')
    parser_etl.add_argument('--tables', required=True, help='Tables to full etl,multiple tables split with comma.')
    parser_etl.add_argument('--debug', default=False, action='store_true', help='Display SQL information.')
    parser_etl.set_defaults(func=make_etl)

    parser_producer = subparsers.add_parser('produce')
    parser_producer.set_defaults(func=produce)

    parser_consumer = subparsers.add_parser('consume')
    parser_consumer.add_argument('--schema', required=True, help='Schema to consume.')
    parser_consumer.add_argument('--table', required=True, help='Table to consume.')
    parser_consumer.set_defaults(func=consume)

    parse_args = parser.parse_args()
    parse_args.func(parse_args)


if __name__ == '__main__':
    cli()
