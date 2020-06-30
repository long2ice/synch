import abc
import logging
import signal
from signal import Signals
from typing import Callable, List, Tuple, Union

from synch.broker import Broker
from synch.settings import Settings

logger = logging.getLogger("synch.reader")


class Reader:
    cursor = None
    fix_column_type = False

    def __init__(self, settings: Settings):
        self.settings = settings
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def execute(self, sql, args=None):
        logger.debug(sql)
        self.cursor.execute(sql, args)
        return self.cursor.fetchall()

    def get_full_insert_sql(self, schema: str, table: str):
        return f"insert into {schema}.{table} {self.get_source_select_sql(schema, table)}"

    @abc.abstractmethod
    def get_table_create_sql(
        self, schema: str, table: str, pk: str, engine: str, partition_by: str, settings: str
    ):
        raise NotImplementedError

    def etl_full(
        self,
        writer,
        schema,
        tables: List[str] = None,
        renew=False,
        engine="MergeTree",
        partition_by=None,
        engine_settings=None,
    ):
        """
        full etl
        """
        settings = self.settings
        if not tables:
            tables = settings.schema_table.get(schema)

        for table in tables:
            pk = self.get_primary_key(schema, table)
            if not pk:
                logger.warning(f"No pk found in {schema}.{table}, skip")
                continue
            elif isinstance(pk, tuple):
                pk = f"({','.join(pk)}"
            if renew:
                drop_sq = f"drop table {schema}.{table}"
                try:
                    writer.execute(drop_sq)
                    logger.info(f"drop table success:{schema}.{table}")
                except Exception as e:
                    logger.warning(f"Try to drop table {schema}.{table} fail")
            if not writer.table_exists(schema, table):
                writer.execute(
                    self.get_table_create_sql(
                        schema, table, pk, engine, partition_by, engine_settings
                    )
                )
                if self.fix_column_type:
                    writer.fix_table_column_type(self, schema, table)
                writer.execute(self.get_full_insert_sql(schema, table))
                logger.info(f"etl success:{schema}.{table}")

    @abc.abstractmethod
    def get_primary_key(self, db: str, table: str) -> Union[None, str, Tuple[str, ...]]:
        raise NotImplementedError

    @abc.abstractmethod
    def start_sync(self, broker: Broker):
        raise NotImplementedError

    @abc.abstractmethod
    def signal_handler(self, signum: Signals, handler: Callable):
        raise NotImplementedError

    @abc.abstractmethod
    def get_source_select_sql(self, schema: str, table: str):
        raise NotImplementedError
