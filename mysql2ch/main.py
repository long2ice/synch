import argparse

from mysql2ch import init_logging
from mysql2ch.config import Config


def cli():
    parser = argparse.ArgumentParser(
        description='A tool replication data from MySQL to ClickHouse',
    )
    parser.add_argument('-c', '--conf', required=False, default='./config.ini',
                        help='Data synchronization config file.')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='Display SQL information.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--log-pos-to', required=False, choices=('redis', 'file'),
                       help='log position to redis or file.')
    group.add_argument('--etl', action='store_true', default=False,
                       help='Full data etl with table create,must create database first.', )
    parser.add_argument('--etl-database', required=False, help='Database to full etl.')
    parser.add_argument('--etl-table', required=False,
                        help='Table to full etl,if not set,will etl all only tables in config.')
    parser.add_argument('--log-file', required=False, default='./mysql2ch.log', help='logging file.')
    args = parser.parse_args()

    config_file = args.conf
    log_pos_to = args.log_pos_to
    etl = args.etl
    etl_database = args.etl_database
    etl_table = args.etl_table
    log_file = args.log_file
    debug = args.debug

    Config.set_config_file(config_file)

    init_logging(log_file, debug)

    from mysql2ch.replication import run_replication, etl_full

    if log_pos_to:
        run_replication(log_pos_to)
    elif etl:
        etl_full(etl_database, etl_table)


if __name__ == '__main__':
    cli()
