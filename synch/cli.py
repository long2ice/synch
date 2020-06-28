import argparse

from synch.factory import init
from synch.replication.consumer import consume
from synch.replication.etl import make_etl
from synch.replication.producer import produce


def version():
    # wait poetry fix up: https://github.com/python-poetry/poetry/issues/1338
    # with open("pyproject.toml") as f:
    #     ret = re.findall(r'version = "(\d+\.\d+\.\d+)"', f.read())
    #     return ret[0]
    return "0.6.0"


def run(args):
    init(args.config)
    args.func(args)


def cli():
    parser = argparse.ArgumentParser(description="Sync data from MySQL to ClickHouse.",)
    parser.add_argument(
        "-c", "--config", required=False, default="./synch.ini", help="Config file."
    )
    parser.add_argument(
        "--version",
        "-V",
        action="version",
        version=f"synch version, {version()}",
        help="show the version",
    )
    subparsers = parser.add_subparsers(title="subcommands")
    parser_etl = subparsers.add_parser("etl")
    parser_etl.add_argument("--schema", required=True, help="Schema to full etl.")
    parser_etl.add_argument(
        "--tables",
        required=False,
        help="Tables to full etl,multiple tables split with comma,default read from environment.",
    )
    parser_etl.add_argument(
        "--renew",
        default=False,
        action="store_true",
        help="Etl after try to drop the target tables.",
    )
    parser_etl.set_defaults(run=run, func=make_etl)

    parser_producer = subparsers.add_parser("produce")
    parser_producer.set_defaults(run=run, func=produce)

    parser_consumer = subparsers.add_parser("consume")
    parser_consumer.add_argument("--schema", required=True, help="Schema to consume.")
    parser_consumer.add_argument(
        "--skip-error", action="store_true", default=False, help="Skip error rows."
    )
    parser_consumer.add_argument(
        "--last-msg-id",
        required=False,
        help="Redis stream last msg id or kafka msg offset, depend on broker_type in config.",
    )
    parser_consumer.set_defaults(run=run, func=consume)

    parse_args = parser.parse_args()
    parse_args.run(parse_args)


if __name__ == "__main__":
    cli()
