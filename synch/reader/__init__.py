import abc
import logging
from typing import List, Tuple, Union

from synch.broker import Broker
from synch.settings import Settings

logger = logging.getLogger("synch.reader")


class Reader:
    cursor = None
    fix_column_type = False

    def __init__(self, settings: Settings):
        self.settings = settings

    def execute(self, sql, args=None):
        logger.debug(sql)
        self.cursor.execute(sql, args)
        return self.cursor.fetchall()

    @abc.abstractmethod
    def get_full_etl_sql(self, schema: str, table: str, pk: str):
        raise NotImplementedError

    def etl_full(self, writer, schema, tables: List[str] = None, renew=False):
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
                sql = self.get_full_etl_sql(schema, table, pk)
                writer.execute(sql)
                if self.fix_column_type:
                    writer.fix_table_column_type(self, schema, table)
                logger.info(f"etl success:{schema}.{table}")

    @abc.abstractmethod
    def get_primary_key(self, db: str, table: str) -> Union[None, str, Tuple[str, ...]]:
        raise NotImplementedError

    @abc.abstractmethod
    def start_sync(self, broker: Broker):
        raise NotImplementedError
