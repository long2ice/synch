import argparse
import logging

import sentry_sdk
from sentry_sdk.integrations.redis import RedisIntegration

import mysql2ch
from mysql2ch import Settings
from mysql2ch.consumer import consume
from mysql2ch.producer import produce
from mysql2ch.reader import MysqlReader
from mysql2ch.replication import make_etl
from mysql2ch.writer import ClickHouseWriter

logger = logging.getLogger('mysql2ch.manage')


def run(args):
    config = args.config
    settings = mysql2ch.Global.settings = Settings.parse_file(config)

    sentry_sdk.init(
        settings.sentry_dsn,
        environment=settings.environment,
        integrations=[RedisIntegration()]
    )

    mysql2ch.init_logging(settings.debug)
    mysql2ch.Global.writer = ClickHouseWriter(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        password=settings.clickhouse_password,
        user=settings.clickhouse_user
    )
    mysql2ch.Global.reader = MysqlReader(
        host=settings.mysql_host,
        port=settings.mysql_port,
        password=settings.mysql_password,
        user=settings.mysql_user
    )

    args.func(args)


def cli():
    parser = argparse.ArgumentParser(
        description='Sync data from MySQL to ClickHouse.',
    )
    parser.add_argument('-c', '--config', required=True, help='Config json file.')
    subparsers = parser.add_subparsers(title='subcommands')
    parser_etl = subparsers.add_parser('etl')
    parser_etl.add_argument('--schema', required=True, help='Schema to full etl.')
    parser_etl.add_argument('--tables', required=False,
                            help='Tables to full etl,multiple tables split with comma,default read from environment.')
    parser_etl.add_argument('--renew', default=False, action='store_true',
                            help='Etl after try to drop the target tables.')
    parser_etl.set_defaults(run=run, func=make_etl)

    parser_producer = subparsers.add_parser('produce')
    parser_producer.set_defaults(run=run, func=produce)

    parser_consumer = subparsers.add_parser('consume')
    parser_consumer.add_argument('--schema', required=True, help='Schema to consume.')
    parser_consumer.add_argument('--skip-error', action='store_true', default=False, help='Skip error rows.')
    parser_consumer.add_argument('--auto-offset-reset', required=False, default='earliest',
                                 help='Kafka auto offset reset,default earliest.')
    parser_consumer.set_defaults(run=run, func=consume)

    parse_args = parser.parse_args()
    parse_args.run(parse_args)


if __name__ == '__main__':
    cli()
