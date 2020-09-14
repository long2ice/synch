import logging
from typing import List

import click
from click import Context

from synch.factory import get_broker, get_reader, get_writer, init
from synch.replication.continuous import continuous_etl
from synch.replication.etl import etl_full
from synch.settings import Settings

logger = logging.getLogger("synch.cli")


def version():
    # wait poetry fix up: https://github.com/python-poetry/poetry/issues/1338
    # with open("pyproject.toml") as f:
    #     ret = re.findall(r'version = "(\d+\.\d+\.\d+)"', f.read())
    #     return ret[0]
    return "0.7.1"


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version(), "-V", "--version")
@click.option("--alias", help="DB alias from config.", required=True)
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
@click.option("--schema", help="Schema to full etl.", required=True)
@click.option(
    "--renew", help="Etl after try to drop the target tables.", is_flag=True, default=False
)
@click.option(
    "-t", "--table", help="Tables to full etl.", required=False, multiple=True,
)
@click.pass_context
def etl(ctx: Context, schema: str, renew: bool, table: List[str]):
    alias = ctx.obj["alias"]
    tables = table
    if not tables:
        tables = Settings.get_source_db_database_tables_name(alias, schema)
    tables_pk = {}
    reader = get_reader(alias)
    for table in tables:
        tables_pk[table] = reader.get_primary_key(schema, table)
    etl_full(alias, schema, tables_pk, renew)


@cli.command(help="Consume from broker and insert into ClickHouse.")
@click.option("--schema", help="Schema to consume.", required=True)
@click.option("--skip-error", help="Skip error rows.", is_flag=True, default=False)
@click.option(
    "--last-msg-id",
    required=False,
    help="Redis stream last msg id or kafka msg offset, depend on broker_type in config.",
)
@click.pass_context
def consume(ctx: Context, schema: str, skip_error: bool, last_msg_id: str):
    alias = ctx.obj["alias"]
    reader = get_reader(alias)
    tables = Settings.get_source_db_database_tables_name(alias, schema)
    tables_pk = {}
    for table in tables:
        tables_pk[table] = reader.get_primary_key(schema, table)

    # try etl full
    etl_full(alias, schema, tables_pk)
    table_dict = Settings.get_source_db_database_tables_dict(alias, schema)

    continuous_etl(
        alias, schema, tables_pk, table_dict, last_msg_id, skip_error,
    )


@cli.command(help="Listen binlog and produce to broker.")
@click.pass_context
def produce(ctx: Context):
    alias = ctx.obj["alias"]
    reader = get_reader(alias)
    broker = get_broker(alias)
    logger.info(f"start producer for {alias} success")
    reader.start_sync(broker)


@cli.command(help="Check whether equal count of target database records and ClickHouse.")
@click.option("--schema", help="Schema to check.", required=True)
@click.pass_context
def check(ctx: Context, schema: str):
    alias = ctx.obj["alias"]
    reader = get_reader(alias)
    writer = get_writer()
    tables = Settings.get_source_db_database_tables_name(alias, schema)
    for table in tables:
        source_table_count = reader.get_count(schema, table)
        target_table_count = writer.get_count(schema, table)
        if source_table_count == target_table_count:
            logger.info(f"{schema}.{table} is equal, count={source_table_count}")
        else:
            logger.warning(
                f"{schema}.{table} is not equal, source_table_count={source_table_count}, target_table_count={target_table_count}"
            )


if __name__ == "__main__":
    cli()
