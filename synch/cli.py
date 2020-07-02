import datetime
import logging
from typing import List

import click
from click import Context

from synch import get_reader, get_writer
from synch.factory import Global, init
from synch.replication.etl import etl_full, continuous_etl

logger = logging.getLogger("synch.cli")


def version():
    # wait poetry fix up: https://github.com/python-poetry/poetry/issues/1338
    # with open("pyproject.toml") as f:
    #     ret = re.findall(r'version = "(\d+\.\d+\.\d+)"', f.read())
    #     return ret[0]
    return "0.6.2"


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version(), "-V", "--version")
@click.option(
    "--alias", help="DB alias from config.",
)
@click.option(
    "-c", "--config", default="./synch.yaml", show_default=True, help="Config file.",
)
@click.pass_context
def cli(ctx: Context, alias: str, config: str):
    """
    Sync data from other DB to ClickHouse.
    """
    ctx.ensure_object(dict)
    ctx.obj["alias"] = alias
    init(config)


@cli.command(help="Make etl from source table to ClickHouse.")
@click.option(
    "--schema", help="Schema to full etl.",
)
@click.option(
    "--renew", help="Etl after try to drop the target tables.", is_flag=True, default=False
)
@click.option(
    "-t", "--table", help="Tables to full etl.", required=False, multiple=True,
)
@click.pass_context
def etl(ctx: Context, schema: str, renew: bool, table: List[str]):
    alias = ctx.obj["alias"]
    settings = Global.settings
    tables = table
    if tables:
        tables = settings.get_source_db_database_tables_by_tables_name(alias, schema, tables)
    else:
        tables = settings.get_source_db_database_tables(alias, schema)
    tables_pk = {}
    reader = Global.get_reader(alias)
    for table in tables:
        tables_pk[table] = reader.get_primary_key(schema, table)
    etl_full(Global.get_reader(alias), Global.writer, schema, tables, tables_pk, renew)


@cli.command(help="Consume from broker and insert into ClickHouse.")
@click.option(
    "--schema", help="Schema to consume.",
)
@click.option("--skip-error", help="Skip error rows.", is_flag=True, default=False)
@click.option(
    "--last-msg-id",
    required=False,
    help="Redis stream last msg id or kafka msg offset, depend on broker_type in config.",
)
@click.pass_context
def consume(ctx: Context, schema: str, skip_error: bool, last_msg_id: str):
    alias = ctx.obj["alias"]
    settings = Global.settings
    reader = get_reader(alias)
    tables = settings.get_source_db_database_tables_name(alias, schema)
    tables_pk = {}
    for table in tables:
        tables_pk[table] = reader.get_primary_key(schema, table)

    # try etl full
    if settings.auto_full_etl:
        etl_full(reader, settings, schema, tables_pk, alias)

    continuous_etl(schema, tables_pk, last_msg_id, skip_error, settings.insert_interval)


@cli.command(help="Listen binlog and produce to broker.")
@click.pass_context
def produce(ctx: Context):
    alias = ctx.obj["alias"]
    reader = get_reader(alias)
    broker = Global.broker
    logger.info(
        f"start producer success at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    reader.start_sync(broker, Global.settings.insert_interval)


if __name__ == '__main__':
    cli()
